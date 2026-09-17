from __future__ import annotations

import json
from dataclasses import asdict, dataclass, is_dataclass
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

from replay.execution import NextObservationExecution
from silver.accounting import AccountingLedger
from silver.market import CanonicalObservation
from strategy_engine import MeanReversionStrategyConfig, MeanReversionStrategyEngine, RenkoStrategyConfig, RenkoStrategyEngine
from trading_events import MarketTick


PARITY_SCHEMA_VERSION = "research.deterministic_lifecycle_parity.v1"


@dataclass(frozen=True)
class LifecycleRun:
    label: str
    steps: tuple[dict[str, Any], ...]
    final_state_hash: str
    final_state: dict[str, Any]


@dataclass(frozen=True)
class ParityReport:
    trader_id: str
    symbol: str
    observation_count: int
    observation_parity: bool
    brick_parity: bool
    signal_parity: bool
    order_intent_parity: bool
    lifecycle_parity: bool
    pnl_parity: bool
    identical_final_state: bool
    first_divergence: dict[str, Any] | None
    replay_final_state_hash: str
    production_offline_final_state_hash: str
    status: str


@dataclass
class _LifecycleRuntime:
    label: str
    trader_id: str
    strategy_config: dict[str, Any]
    strategy: Any
    accounting: AccountingLedger
    execution: NextObservationExecution


def run_deterministic_parity(
    *,
    trader_id: str,
    strategy_config: RenkoStrategyConfig | MeanReversionStrategyConfig | dict[str, Any],
    observations: tuple[CanonicalObservation, ...],
    initial_capital: Decimal | int | float | str = Decimal("10000"),
) -> ParityReport:
    config_payload = _config_payload(strategy_config)
    replay = _new_runtime(
        label="canonical_replay_lifecycle",
        trader_id=trader_id,
        strategy_config=config_payload,
        initial_capital=initial_capital,
    )
    production = _new_runtime(
        label="production_strategy_offline_sink",
        trader_id=trader_id,
        strategy_config=config_payload,
        initial_capital=initial_capital,
    )
    first = None
    observation_parity = True
    brick_parity = True
    signal_parity = True
    order_intent_parity = True
    pnl_parity = True
    for sequence, observation in enumerate(observations, start=1):
        replay_step = _advance_runtime(replay, observation=observation, sequence=sequence)
        production_step = _advance_runtime(production, observation=observation, sequence=sequence)
        if first is None and replay_step != production_step:
            first = {"sequence": sequence, "replay": replay_step, "production_offline": production_step}
        observation_parity = observation_parity and replay_step.get("observation") == production_step.get("observation")
        brick_parity = brick_parity and replay_step.get("bricks") == production_step.get("bricks")
        signal_parity = signal_parity and replay_step.get("signals") == production_step.get("signals")
        order_intent_parity = order_intent_parity and replay_step.get("order_intents") == production_step.get("order_intents")
        pnl_parity = pnl_parity and replay_step.get("accounting") == production_step.get("accounting")
    mark_price = observations[-1].close_price if observations else None
    replay_final = _final_state(replay, mark_price=mark_price)
    production_final = _final_state(production, mark_price=mark_price)
    replay_hash = _stable_hash(replay_final)
    production_hash = _stable_hash(production_final)
    final_equal = replay_hash == production_hash
    status = "PASS" if first is None and final_equal else "FAIL"
    return ParityReport(
        trader_id=trader_id,
        symbol=str(config_payload["symbol"]).upper(),
        observation_count=len(observations),
        observation_parity=observation_parity,
        brick_parity=brick_parity,
        signal_parity=signal_parity,
        order_intent_parity=order_intent_parity,
        lifecycle_parity=first is None,
        pnl_parity=pnl_parity,
        identical_final_state=final_equal,
        first_divergence=first,
        replay_final_state_hash=replay_hash,
        production_offline_final_state_hash=production_hash,
        status=status,
    )


def run_lifecycle(
    *,
    label: str,
    trader_id: str,
    strategy_config: dict[str, Any],
    observations: tuple[CanonicalObservation, ...],
    initial_capital: Decimal | int | float | str,
) -> LifecycleRun:
    runtime = _new_runtime(
        label=label,
        trader_id=trader_id,
        strategy_config=strategy_config,
        initial_capital=initial_capital,
    )
    steps: list[dict[str, Any]] = []
    for sequence, observation in enumerate(observations, start=1):
        steps.append(_advance_runtime(runtime, observation=observation, sequence=sequence))
    final_state = _final_state(runtime, mark_price=observations[-1].close_price if observations else None)
    return LifecycleRun(label=label, steps=tuple(steps), final_state_hash=_stable_hash(final_state), final_state=final_state)


def write_parity_reports(path: str | Path, reports: list[ParityReport]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps({"schema_version": PARITY_SCHEMA_VERSION, "reports": [asdict(report) for report in reports]}, indent=2, sort_keys=True, default=str)
        + "\n",
        encoding="utf-8",
    )


def _strategy_engine(config: dict[str, Any]):
    if config.get("strategy_type") == "chop_mean_reversion":
        payload = dict(config)
        payload.pop("strategy_type", None)
        return MeanReversionStrategyEngine(MeanReversionStrategyConfig(**payload))
    return RenkoStrategyEngine(RenkoStrategyConfig(**config))


def _config_payload(config: RenkoStrategyConfig | MeanReversionStrategyConfig | dict[str, Any]) -> dict[str, Any]:
    payload = asdict(config) if is_dataclass(config) else dict(config)
    if payload.get("strategy_type") is None and "window_seconds" in payload:
        payload["strategy_type"] = "chop_mean_reversion"
    return payload


def _new_runtime(
    *,
    label: str,
    trader_id: str,
    strategy_config: dict[str, Any],
    initial_capital: Decimal | int | float | str,
) -> _LifecycleRuntime:
    return _LifecycleRuntime(
        label=label,
        trader_id=trader_id,
        strategy_config=strategy_config,
        strategy=_strategy_engine(strategy_config),
        accounting=AccountingLedger(symbol=strategy_config["symbol"], starting_capital=initial_capital),
        execution=NextObservationExecution(run_id=f"parity:{trader_id}", trader_id=trader_id),
    )


def _advance_runtime(runtime: _LifecycleRuntime, *, observation: CanonicalObservation, sequence: int) -> dict[str, Any]:
    fills = []
    for bundle in runtime.execution.fill_pending(observation=observation, position=runtime.accounting.position):
        for fill in bundle.fills:
            fills.append(fill)
            runtime.accounting.process_fill(fill)
            if fill.leg == "close" and runtime.accounting.position.side is None and hasattr(runtime.strategy, "mark_close_filled"):
                runtime.strategy.mark_close_filled(timestamp=fill.timestamp, reason=fill.reason, order_id=fill.order_id, source=runtime.execution.version)
    tick = MarketTick(
        timestamp=pd.Timestamp(observation.second_bucket),
        symbol=observation.symbol,
        price=float(observation.close_price),
        size=1.0,
        exchange=observation.source_exchange,
        trade_id=observation.source_alpaca_trade_id,
        tape=observation.source_tape,
        conditions=(),
    )
    bricks, actions = runtime.strategy.on_tick(tick)
    orders = []
    for signal, intent in actions:
        order = runtime.execution.queue(signal=signal, intent=intent, observation=observation, position=runtime.accounting.position)
        if order is None:
            if intent.close_position and hasattr(runtime.strategy, "mark_close_filled"):
                runtime.strategy.mark_close_filled(timestamp=signal.timestamp, reason=intent.reason, order_id=None, source=f"{runtime.execution.version}:already_flat")
            continue
        if intent.close_position and hasattr(runtime.strategy, "mark_close_submitted"):
            runtime.strategy.mark_close_submitted(intent, response={"id": order.order_id})
        orders.append(order)
    return {
        "sequence": sequence,
        "observation": _observation_semantics(observation),
        "bricks": [_brick_semantics(item) for item in bricks],
        "signals": [_signal_semantics(signal) for signal, _ in actions],
        "order_intents": [_intent_semantics(intent) for _, intent in actions],
        "queued_orders": [_order_semantics(order) for order in orders],
        "fills": [_fill_semantics(fill) for fill in fills],
        "strategy": _strategy_step_semantics(runtime.strategy),
        "accounting": _accounting_semantics(runtime.accounting.snapshot(mark_price=observation.close_price)),
        "pending_order_count": len(runtime.execution.pending_orders),
    }


def _final_state(runtime: _LifecycleRuntime, *, mark_price: Decimal | None) -> dict[str, Any]:
    return {
        "strategy": _strategy_final_semantics(runtime.strategy),
        "accounting": _accounting_semantics(runtime.accounting.snapshot(mark_price=mark_price)),
        "pending_orders": len(runtime.execution.pending_orders),
    }


def _first_divergence(left: tuple[dict[str, Any], ...], right: tuple[dict[str, Any], ...]) -> dict[str, Any] | None:
    max_len = max(len(left), len(right))
    for index in range(max_len):
        lval = left[index] if index < len(left) else None
        rval = right[index] if index < len(right) else None
        if lval != rval:
            return {"sequence": index + 1, "replay": lval, "production_offline": rval}
    return None


def _field_parity(left: tuple[dict[str, Any], ...], right: tuple[dict[str, Any], ...], field: str) -> bool:
    if len(left) != len(right):
        return False
    return all(lrow.get(field) == rrow.get(field) for lrow, rrow in zip(left, right, strict=True))


def _observation_semantics(observation: CanonicalObservation) -> dict[str, Any]:
    return {
        "symbol": observation.symbol,
        "second_bucket": observation.second_bucket.isoformat(),
        "close_price": str(observation.close_price),
        "source_sip_timestamp": observation.source_sip_timestamp.isoformat(),
        "source_exchange": observation.source_exchange,
        "source_tape": observation.source_tape,
    }


def _brick_semantics(brick) -> dict[str, Any]:
    return {
        "timestamp": brick.timestamp.isoformat(),
        "symbol": brick.symbol,
        "open": _rounded(brick.open),
        "close": _rounded(brick.close),
        "direction": brick.direction,
        "run_len": brick.run_len,
        "brick_size": _rounded(brick.brick_size),
    }


def _signal_semantics(signal) -> dict[str, Any]:
    return {
        "signal_type": signal.signal_type,
        "timestamp": signal.timestamp.isoformat(),
        "symbol": signal.symbol,
        "side": signal.side,
        "broker_side": signal.broker_side,
        "price": _rounded(signal.price),
        "reason": signal.reason,
        "strategy": signal.strategy,
        "trader_id": signal.trader_id,
    }


def _intent_semantics(intent) -> dict[str, Any]:
    return {
        "timestamp": intent.timestamp.isoformat(),
        "symbol": intent.symbol,
        "side": intent.side,
        "signal_type": intent.signal_type,
        "order_type": intent.order_type,
        "strategy": intent.strategy,
        "reason": intent.reason,
        "qty": _rounded(intent.qty),
        "target_qty": _rounded(intent.target_qty),
        "notional": _rounded(intent.notional),
        "limit_price": _rounded(intent.limit_price),
        "close_position": intent.close_position,
        "take_profit_limit_price": _rounded(intent.take_profit_limit_price),
        "stop_loss_stop_price": _rounded(intent.stop_loss_stop_price),
        "stop_loss_limit_price": _rounded(intent.stop_loss_limit_price),
        "lifecycle_state": intent.lifecycle_state,
    }


def _order_semantics(order) -> dict[str, Any]:
    return {
        "sequence": order.sequence,
        "symbol": order.symbol,
        "trader_id": order.trader_id,
        "qty": str(order.qty),
        "source_observation_id": order.source_observation_id,
    }


def _fill_semantics(fill) -> dict[str, Any]:
    return {
        "timestamp": fill.timestamp.isoformat(),
        "symbol": fill.symbol,
        "trader_id": fill.trader_id,
        "side": fill.side,
        "qty": str(fill.qty),
        "price": str(fill.price),
        "leg": fill.leg,
        "reason": fill.reason,
        "source_sequence": fill.source_sequence,
    }


def _strategy_step_semantics(strategy) -> dict[str, Any]:
    if isinstance(strategy, MeanReversionStrategyEngine):
        return _mean_reversion_strategy_semantics(strategy, include_sequence_checksums=False)
    return _strategy_semantics(strategy.snapshot())


def _strategy_final_semantics(strategy) -> dict[str, Any]:
    if isinstance(strategy, MeanReversionStrategyEngine):
        return _mean_reversion_strategy_semantics(strategy, include_sequence_checksums=True)
    return _strategy_semantics(strategy.snapshot())


def _mean_reversion_strategy_semantics(strategy: MeanReversionStrategyEngine, *, include_sequence_checksums: bool) -> dict[str, Any]:
    return {
        "version": 1,
        "trader_id": strategy.config.trader_id,
        "symbol": strategy.config.symbol.upper(),
        "strategy_name": strategy.config.strategy_name,
        "strategy_type": strategy.config.strategy_type,
        "mean_reversion": {
            "points": _sequence_summary(strategy.points, include_checksum=include_sequence_checksums),
            "abs_moves": _sequence_summary(strategy.abs_moves, include_checksum=include_sequence_checksums),
            "sum_close": _rounded(strategy.sum_close),
            "sum_close_sq": _rounded(strategy.sum_close_sq),
            "sum_abs_moves": _rounded(strategy.sum_abs_moves),
            "last_price": _rounded(strategy.last_price),
            "entry_z": _rounded(strategy.entry_z),
            "entry_efficiency": _rounded(strategy.entry_efficiency),
        },
        "trade": _trade_state_semantics(strategy.trade_state_snapshot()),
    }


def _sequence_summary(values, *, include_checksum: bool) -> dict[str, Any]:
    summary = {
        "count": len(values),
        "first": _sequence_item(values[0]) if values else None,
        "last": _sequence_item(values[-1]) if values else None,
    }
    if include_checksum:
        digest = sha256()
        for item in values:
            digest.update((json.dumps(_sequence_item(item), sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8"))
        summary["checksum"] = digest.hexdigest()
    return summary


def _sequence_item(item) -> tuple[str, Any]:
    timestamp, value = item
    return (timestamp.isoformat(), _rounded(value))


def _strategy_semantics(snapshot: dict[str, Any]) -> dict[str, Any]:
    payload = json.loads(json.dumps(snapshot, default=str))
    if isinstance(payload.get("trade"), dict):
        payload["trade"] = _trade_state_semantics(payload["trade"])
    return payload


def _trade_state_semantics(trade: dict[str, Any]) -> dict[str, Any]:
    payload = json.loads(json.dumps(trade, default=str))
    pending = payload.get("pending_exit")
    if isinstance(pending, dict):
        pending.pop("signal_id", None)
        pending.pop("client_order_id", None)
        pending.pop("broker_order_id", None)
    last_closed = payload.get("last_closed_exit")
    if isinstance(last_closed, dict):
        last_closed.pop("order_id", None)
    protection = payload.get("protection_state")
    if isinstance(protection, dict):
        for key in (
            "parent_order_id",
            "take_profit_order_id",
            "stop_loss_order_id",
            "stop_order_id",
            "broker_order_id",
            "client_order_id",
        ):
            protection.pop(key, None)
    return payload


def _accounting_semantics(snapshot) -> dict[str, Any]:
    position = snapshot.position
    return {
        "position": {
            "symbol": position.symbol,
            "side": position.side,
            "qty": str(position.qty),
            "avg_entry_price": str(position.avg_entry_price) if position.avg_entry_price is not None else None,
            "basis_known": position.basis_known,
        },
        "realized_pnl": str(snapshot.realized_pnl),
        "unrealized_pnl": str(snapshot.unrealized_pnl),
        "net_pnl": str(snapshot.net_pnl),
        "equity": str(snapshot.equity),
        "closed_trades": len(snapshot.closed_trades),
        "position_transitions": len(snapshot.position_transitions),
    }


def _rounded(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (int, str, bool)):
        return value
    return round(float(value), 12)


def _stable_hash(payload: dict[str, Any]) -> str:
    return sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()
