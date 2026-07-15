from __future__ import annotations

import argparse
import asyncio
import json
import re
import ssl
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from datetime import date, timedelta

import certifi
import pandas as pd
import websockets
from dotenv import load_dotenv

from alpaca_trader import load_alpaca_credentials
from execution_engine import AlpacaExecutionEngine, JsonlEventLog, ShadowExecutionEngine
from strategy_engine import MeanReversionStrategyConfig, MeanReversionStrategyEngine, RenkoStrategyConfig, RenkoStrategyEngine
from trading_events import MarketTick, OrderIntent, Signal, dataclass_to_jsonable, is_price_forming_trade


DATA_STREAM_URL_TEMPLATE = "wss://stream.data.alpaca.markets/v2/{feed}"
BROKER_MANAGED_RECONCILE_INTERVAL_SECONDS = 5.0
BROKER_FORCE_FLAT_POLL_SECONDS = 0.5
_LAST_BROKER_MANAGED_RECONCILE: dict[str, float] = {}
_BROKER_FORCE_FLAT_DATES: set[tuple[str, str, str, str]] = set()


class BrokerEngineStateMismatch(RuntimeError):
    pass


@dataclass(frozen=True)
class TraderRuntime:
    trader_id: str
    symbol: str
    mode: str
    credential_profile: str | None
    feed: str
    enabled: bool
    execution_enabled: bool
    max_position_notional: float
    max_daily_loss: float
    engine: RenkoStrategyEngine | MeanReversionStrategyEngine
    execution: AlpacaExecutionEngine | ShadowExecutionEngine
    log: JsonlEventLog
    renko_source: str = "ticks"


_PENDING_1S_CLOSE_TICKS: dict[str, tuple[pd.Timestamp, MarketTick]] = {}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a config-driven fleet of live strategy traders.")
    parser.add_argument("--config", required=True, help="Fleet JSON config path.")
    parser.add_argument("--execute-real", action="store_true", help="Submit Alpaca orders. Defaults to shadow mode.")
    parser.add_argument("--force-mode", choices=["paper", "live"], help="Override trader mode for the whole fleet.")
    parser.add_argument("--force-feed", choices=["iex", "sip", "delayed_sip", "boats", "overnight"], help="Override trader feed.")
    parser.add_argument("--log-output", help="Override all trader event logs.")
    parser.add_argument("--archive-root", default="artifacts/live", help="Root directory for tick, Renko, and state archives.")
    parser.add_argument("--no-persist-ticks", action="store_true", help="Disable raw tick archive writes.")
    parser.add_argument("--reconnect-delay", type=float, default=5.0)
    parser.add_argument("--log-ticks", action="store_true")
    return parser


def load_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        config = json.load(handle)
    if not isinstance(config.get("traders"), list):
        raise ValueError("Fleet config must contain a top-level traders array.")
    return config


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def normalize_renko_source(value: Any) -> str:
    raw = str(value or "ticks").lower().strip().replace("-", "_")
    if raw in {"tick", "ticks", "raw_ticks"}:
        return "ticks"
    if raw in {"1s", "1s_close", "one_second_close", "second_close"}:
        return "1s_close"
    raise ValueError(f"Unsupported renko_source: {value}")


def tick_second_bucket(timestamp: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(timestamp).tz_convert("UTC").floor("s")


def renko_source_ticks(runtime: TraderRuntime, tick: MarketTick) -> list[MarketTick]:
    if runtime.renko_source == "ticks":
        return [tick]

    bucket = tick_second_bucket(tick.timestamp)
    pending = _PENDING_1S_CLOSE_TICKS.get(runtime.trader_id)
    if pending is None:
        _PENDING_1S_CLOSE_TICKS[runtime.trader_id] = (bucket, tick)
        return []

    pending_bucket, pending_tick = pending
    if bucket == pending_bucket:
        _PENDING_1S_CLOSE_TICKS[runtime.trader_id] = (bucket, tick)
        return []
    if bucket < pending_bucket:
        return []

    _PENDING_1S_CLOSE_TICKS[runtime.trader_id] = (bucket, tick)
    return [
        MarketTick(
            timestamp=pending_bucket,
            symbol=pending_tick.symbol,
            price=pending_tick.price,
            size=pending_tick.size,
            exchange=pending_tick.exchange,
            trade_id=pending_tick.trade_id,
            tape=pending_tick.tape,
            conditions=pending_tick.conditions,
        )
    ]


def archive_root(args: argparse.Namespace) -> Path:
    return Path(str(args.archive_root))


def slug(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_") or "unknown"


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    temp_path.replace(path)


def archive_date(timestamp: pd.Timestamp) -> str:
    return pd.Timestamp(timestamp).tz_convert("UTC").date().isoformat()


def tick_archive_path(root: Path, tick: MarketTick) -> Path:
    return root / "ticks" / archive_date(tick.timestamp) / f"{tick.symbol.upper()}.jsonl"


def renko_archive_path(root: Path, brick_symbol: str, trader_id: str, timestamp: pd.Timestamp) -> Path:
    return root / "renko" / archive_date(timestamp) / f"{slug(trader_id)}_{brick_symbol.upper()}.jsonl"


def state_snapshot_path(root: Path, trader_id: str) -> Path:
    return root / "state" / f"{slug(trader_id)}.json"


def archive_tick(root: Path, tick: MarketTick, mode: str, feed: str) -> None:
    payload = dataclass_to_jsonable(tick)
    payload["mode"] = mode
    payload["feed"] = feed
    payload["logged_at"] = pd.Timestamp.now(tz="UTC").isoformat()
    append_jsonl(tick_archive_path(root, tick), payload)


def archive_renko_brick(root: Path, runtime: TraderRuntime, brick_payload: dict[str, Any]) -> None:
    payload = dict(brick_payload)
    payload["mode"] = runtime.mode
    payload["feed"] = runtime.feed
    payload["credential_profile"] = runtime.credential_profile
    payload["logged_at"] = pd.Timestamp.now(tz="UTC").isoformat()
    append_jsonl(
        renko_archive_path(root, runtime.symbol, runtime.trader_id, pd.Timestamp(payload["timestamp"])),
        payload,
    )


def archive_engine_state(root: Path, runtime: TraderRuntime, reason: str) -> None:
    payload = {
        "saved_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "reason": reason,
        "trader_id": runtime.trader_id,
        "symbol": runtime.symbol,
        "mode": runtime.mode,
        "credential_profile": runtime.credential_profile,
        "feed": runtime.feed,
        "execution_enabled": runtime.execution_enabled,
        "engine": runtime.engine.snapshot(),
    }
    write_json_atomic(state_snapshot_path(root, runtime.trader_id), payload)


def restore_engine_state(root: Path, trader_id: str, engine: RenkoStrategyEngine) -> tuple[bool, str | None]:
    path = state_snapshot_path(root, trader_id)
    if not path.exists():
        return False, None
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    snapshot = payload.get("engine") if isinstance(payload, dict) else None
    if not isinstance(snapshot, dict):
        return False, "state file missing engine snapshot"
    engine.restore_snapshot(snapshot)
    return True, str(path)


def warm_start_runtime_from_archived_ticks(root: Path, runtime: TraderRuntime, days: int) -> int:
    if days <= 0:
        return 0

    ticks_root = root / "ticks"
    if not ticks_root.exists():
        return 0

    cutoff = date.today() - timedelta(days=days)
    tick_files: list[Path] = []
    for path in sorted(ticks_root.glob(f"*/{runtime.symbol.upper()}.jsonl")):
        try:
            path_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if path_date >= cutoff:
            tick_files.append(path)

    ticks: list[MarketTick] = []
    seen_ticks: set[tuple[str, str, float, float, str]] = set()
    raw_tick_count = 0
    for path in tick_files:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                payload = json.loads(line)
                raw_tick_count += 1
                tick = MarketTick(
                    timestamp=pd.Timestamp(payload["timestamp"]),
                    symbol=str(payload["symbol"]).upper(),
                    price=float(payload["price"]),
                    size=float(payload.get("size", 0.0) or 0.0),
                    exchange=str(payload.get("exchange", "")),
                    trade_id=str(payload.get("trade_id", "")),
                    tape=str(payload.get("tape", "")),
                )
                dedupe_key = (
                    tick.timestamp.isoformat(),
                    tick.trade_id,
                    tick.price,
                    tick.size,
                    tick.exchange,
                )
                if dedupe_key in seen_ticks:
                    continue
                seen_ticks.add(dedupe_key)
                ticks.append(tick)

    ticks.sort(key=lambda tick: tick.timestamp)
    for tick in ticks:
        runtime.engine.on_tick(tick)

    if ticks:
        runtime.log.write(
            "state_warm_started",
            {
                "trader_id": runtime.trader_id,
                "symbol": runtime.symbol,
                "source": "archived_ticks",
                "days": days,
                "tick_count": len(ticks),
                "raw_tick_count": raw_tick_count,
                "skipped_duplicate_ticks": raw_tick_count - len(ticks),
                "first_tick": ticks[0].timestamp.isoformat(),
                "last_tick": ticks[-1].timestamp.isoformat(),
            },
        )
        archive_engine_state(root, runtime, reason="warm_started_archived_ticks")

    return len(ticks)


def reconcile_engine_with_broker(root: Path, runtime: TraderRuntime) -> None:
    if not isinstance(runtime.execution, AlpacaExecutionEngine):
        return

    broker_position = runtime.execution.trader._get_position(runtime.symbol)
    engine_trade = runtime.engine.trade_state_snapshot()
    engine_position = engine_trade.get("position")

    if broker_position is None and engine_position is not None:
        runtime.engine.restore_trade_state(
            {
                "position": None,
                "psar_sar": None,
                "psar_ep": None,
                "psar_af": None,
                "force_flat_dates": engine_trade.get("force_flat_dates", []),
            }
        )
        runtime.log.write(
            "state_reconciled",
            {
                "trader_id": runtime.trader_id,
                "symbol": runtime.symbol,
                "reason": "broker_flat_engine_position_cleared",
                "previous_trade_state": engine_trade,
            },
        )
        archive_engine_state(root, runtime, reason="broker_reconciled_flat")
        return

    if broker_position is not None and engine_position is None:
        runtime.log.write(
            "state_reconcile_error",
            {
                "trader_id": runtime.trader_id,
                "symbol": runtime.symbol,
                "reason": "broker_position_exists_engine_flat",
                "broker_position": broker_position,
            },
        )
        archive_engine_state(root, runtime, reason="broker_position_exists_engine_flat")
        raise BrokerEngineStateMismatch(
            f"{runtime.trader_id} has broker {runtime.symbol} position but engine is flat"
        )


def reconcile_broker_state_if_due(root: Path, runtime: TraderRuntime) -> None:
    if not isinstance(runtime.execution, AlpacaExecutionEngine):
        return

    now = time.monotonic()
    last = _LAST_BROKER_MANAGED_RECONCILE.get(runtime.trader_id, 0.0)
    if now - last < BROKER_MANAGED_RECONCILE_INTERVAL_SECONDS:
        return
    _LAST_BROKER_MANAGED_RECONCILE[runtime.trader_id] = now

    try:
        reconcile_engine_with_broker(root, runtime)
    except BrokerEngineStateMismatch:
        raise
    except Exception as exc:
        runtime.log.write(
            "state_reconcile_error",
            {
                "trader_id": runtime.trader_id,
                "symbol": runtime.symbol,
                "reason": "broker_state_reconcile_failed",
                "error": str(exc),
            },
        )


def runtime_force_flat_date(runtime: TraderRuntime, timestamp: pd.Timestamp) -> str | None:
    force_flat = getattr(runtime.engine, "force_flat", None)
    if runtime.engine.config.allow_overnight or force_flat is None:
        return None

    local_timestamp = runtime.engine._local_timestamp(timestamp)
    current_date = local_timestamp.date().isoformat()
    active_start_date = getattr(runtime.engine, "active_start_date", None)
    active_end_date = getattr(runtime.engine, "active_end_date", None)
    if active_start_date and current_date < active_start_date:
        return None
    if active_end_date and current_date > active_end_date:
        return None
    if local_timestamp.time() < force_flat:
        return None
    return current_date


def force_flat_key(runtime: TraderRuntime, current_date: str) -> tuple[str, str, str, str]:
    return (runtime.mode, runtime.credential_profile or "", runtime.symbol, current_date)


def force_flat_client_order_id(runtime: TraderRuntime, timestamp: pd.Timestamp) -> str:
    local_timestamp = runtime.engine._local_timestamp(timestamp)
    suffix = f"ff-{local_timestamp.strftime('%Y%m%d-%H%M%S')}"
    prefix = slug(runtime.trader_id).lower()
    max_prefix_length = max(1, 48 - len(suffix) - 1)
    return f"{prefix[:max_prefix_length]}-{suffix}"[:48]


def mark_runtime_force_flat_state(root: Path, runtime: TraderRuntime, current_date: str, reason: str) -> None:
    engine_trade = runtime.engine.trade_state_snapshot()
    force_flat_dates = set(engine_trade.get("force_flat_dates", []))
    already_flat_for_date = engine_trade.get("position") is None and current_date in force_flat_dates
    force_flat_dates.add(current_date)
    runtime.engine.restore_trade_state(
        {
            "position": None,
            "psar_sar": None,
            "psar_ep": None,
            "psar_af": None,
            "force_flat_dates": sorted(force_flat_dates),
        }
    )
    if not already_flat_for_date:
        archive_engine_state(root, runtime, reason=reason)


def broker_force_flat_if_due(
    root: Path,
    runtime: TraderRuntime,
    timestamp: pd.Timestamp,
    *,
    price: float | None = None,
    source: str,
) -> bool:
    current_date = runtime_force_flat_date(runtime, timestamp)
    if current_date is None:
        return False

    key = force_flat_key(runtime, current_date)
    if key in _BROKER_FORCE_FLAT_DATES:
        mark_runtime_force_flat_state(root, runtime, current_date, reason=f"broker_force_flat_already_handled_{source}")
        return False

    broker_position = None
    if isinstance(runtime.execution, AlpacaExecutionEngine):
        try:
            broker_position = runtime.execution.trader._get_position(runtime.symbol)
        except Exception as exc:
            runtime.log.write(
                "broker_force_flat_position_error",
                {
                    "trader_id": runtime.trader_id,
                    "symbol": runtime.symbol,
                    "source": source,
                    "error": str(exc),
                },
            )

    engine_position = runtime.engine.trade_state_snapshot().get("position")
    broker_position_side = str((broker_position or {}).get("side", "")).lower().strip()
    if broker_position_side == "short":
        strategy_side = "short"
        broker_side = "buy"
    elif broker_position_side == "long":
        strategy_side = "long"
        broker_side = "sell"
    elif isinstance(engine_position, dict) and engine_position.get("side") == "short":
        strategy_side = "short"
        broker_side = "buy"
    else:
        strategy_side = "long"
        broker_side = "sell"

    signal = Signal(
        signal_id=f"{runtime.trader_id}-{runtime.symbol.lower()}-force-flat-{current_date.replace('-', '')}",
        signal_type="exit",
        timestamp=timestamp,
        symbol=runtime.symbol,
        side=strategy_side,  # type: ignore[arg-type]
        broker_side=broker_side,  # type: ignore[arg-type]
        price=float(
            price
            if price is not None
            else as_float((broker_position or {}).get("current_price"), default=0.0)
            or as_float((broker_position or {}).get("avg_entry_price"), default=0.0)
        ),
        reason="session_force_flat",
        strategy=runtime.engine.config.strategy_name,
        trader_id=runtime.trader_id,
    )
    intent = OrderIntent(
        client_order_id=force_flat_client_order_id(runtime, timestamp),
        timestamp=timestamp,
        symbol=runtime.symbol,
        side=broker_side,  # type: ignore[arg-type]
        signal_type="exit",
        order_type="market",
        strategy=runtime.engine.config.strategy_name,
        reason="session_force_flat",
        close_position=True,
        trader_id=runtime.trader_id,
    )

    runtime.log.write(
        "broker_force_flat",
        {
            "trader_id": runtime.trader_id,
            "symbol": runtime.symbol,
            "source": source,
            "current_date": current_date,
            "broker_position": broker_position,
            "engine_trade": runtime.engine.trade_state_snapshot(),
            "intent": dataclass_to_jsonable(intent),
        },
    )
    runtime.log.write("signal", dataclass_to_jsonable(signal))
    _BROKER_FORCE_FLAT_DATES.add(key)
    try:
        runtime.execution.execute(intent)
    except Exception as exc:
        _BROKER_FORCE_FLAT_DATES.discard(key)
        runtime.log.write(
            "execution_error",
            {
                "trader_id": runtime.trader_id,
                "symbol": runtime.symbol,
                "error": str(exc),
                "signal": dataclass_to_jsonable(signal),
                "intent": dataclass_to_jsonable(intent),
            },
        )
        return False

    mark_runtime_force_flat_state(root, runtime, current_date, reason=f"broker_force_flat_{source}")
    return True


def build_strategy_config(trader: dict[str, Any]) -> RenkoStrategyConfig | MeanReversionStrategyConfig:
    strategy = trader.get("strategy") if isinstance(trader.get("strategy"), dict) else {}
    session = trader.get("session") if isinstance(trader.get("session"), dict) else {}
    sizing = trader.get("sizing") if isinstance(trader.get("sizing"), dict) else {}

    strategy_type = str(strategy.get("type") or "renko_tp_sl")
    if strategy_type not in {"renko_tp_sl", "renko_psar", "renko_reversal", "chop_mean_reversion"}:
        raise ValueError(f"{trader.get('id')}: unsupported strategy type {strategy_type}.")

    qty = as_float(sizing.get("qty"), default=0.0) or None
    notional = as_float(sizing.get("notional"), default=0.0) or None
    exit_management = str(strategy.get("exit_management") or "strategy").lower().strip()
    if exit_management == "oco":
        if qty is None:
            raise ValueError(f"{trader.get('id')}: oco exit_management requires whole-share qty sizing.")
        if abs(qty - round(qty)) > 1e-9:
            raise ValueError(
                f"{trader.get('id')}: Alpaca OCO does not support fractional qty sizing ({qty:g})."
            )
    if strategy_type == "chop_mean_reversion":
        return MeanReversionStrategyConfig(
            trader_id=str(trader["id"]),
            symbol=str(trader["symbol"]).upper(),
            window_seconds=int(strategy["window_seconds"]),
            entry_z=float(strategy["entry_z"]),
            exit_z=float(strategy.get("exit_z", 0.10)),
            stop_z=float(strategy["stop_z"]),
            efficiency_max=float(strategy["efficiency_max"]),
            trend_exit_efficiency=float(strategy["trend_exit_efficiency"]),
            max_hold_seconds=int(strategy["max_hold_seconds"]),
            allow_short=bool(strategy.get("allow_short", True)),
            strategy_name=str(trader.get("strategy_name") or trader["id"]),
            order_type=str(strategy.get("order_type") or "market").lower().strip(),
            qty=qty,
            notional=notional,
            timezone=str(session.get("timezone") or "America/New_York"),
            entry_start=session.get("entry_start"),
            entry_end=session.get("entry_end"),
            force_flat=session.get("force_flat"),
            active_start_date=session.get("active_start_date"),
            active_end_date=session.get("active_end_date"),
            allow_overnight=bool(session.get("allow_overnight", False)),
        )
    return RenkoStrategyConfig(
        trader_id=str(trader["id"]),
        symbol=str(trader["symbol"]).upper(),
        strategy_type=strategy_type,  # type: ignore[arg-type]
        brick_size=float(strategy["brick_size"]),
        entry_bricks=int(strategy["entry_bricks"]),
        take_profit_bricks=int(strategy["take_profit_bricks"]) if strategy.get("take_profit_bricks") else None,
        stop_loss_bricks=int(strategy["stop_loss_bricks"]) if strategy.get("stop_loss_bricks") else None,
        psar_initial_stop_bricks=int(strategy["psar_initial_stop_bricks"])
        if strategy.get("psar_initial_stop_bricks")
        else None,
        psar_af_step=float(strategy["psar_af_step"]) if strategy.get("psar_af_step") else None,
        psar_af_max=float(strategy["psar_af_max"]) if strategy.get("psar_af_max") else None,
        allow_short=bool(strategy.get("allow_short", True)),
        strategy_name=str(trader.get("strategy_name") or trader["id"]),
        order_type=str(strategy.get("order_type") or "market").lower().strip(),
        exit_management=exit_management,
        stop_loss_order_type=str(strategy.get("stop_loss_order_type") or "stop_market").lower().strip(),
        stop_limit_offset_pct=float(strategy.get("stop_limit_offset_pct") or 0.0),
        qty=qty,
        notional=notional,
        renko_anchor_mode=str(strategy.get("renko_anchor_mode") or strategy.get("anchor_mode") or "first_price"),
        renko_anchor_price=float(strategy["renko_anchor_price"])
        if strategy.get("renko_anchor_price") is not None
        else float(strategy["anchor_price"])
        if strategy.get("anchor_price") is not None
        else None,
        timezone=str(session.get("timezone") or "America/New_York"),
        entry_start=session.get("entry_start"),
        entry_end=session.get("entry_end"),
        force_flat=session.get("force_flat"),
        active_start_date=session.get("active_start_date"),
        active_end_date=session.get("active_end_date"),
        allow_overnight=bool(session.get("allow_overnight", False)),
    )


def behavior_profile_block_reason(trader: dict[str, Any]) -> str | None:
    profile = trader.get("profile") if isinstance(trader.get("profile"), dict) else {}
    risk = trader.get("risk") if isinstance(trader.get("risk"), dict) else {}
    max_trade_cost = as_float(risk.get("max_trade_cost_proxy_bps"), default=0.0)
    max_p95_vol = as_float(risk.get("max_1m_p95_abs_return_bps"), default=0.0)

    trade_cost = as_float(profile.get("trade_cost_proxy_bps"), default=-1.0)
    if max_trade_cost > 0 and trade_cost >= 0 and trade_cost > max_trade_cost:
        return f"profile_trade_cost_proxy_{trade_cost:.4f}_exceeds_{max_trade_cost:.4f}_bps"

    p95_vol = as_float(profile.get("p95_abs_1m_return_bps"), default=-1.0)
    if max_p95_vol > 0 and p95_vol >= 0 and p95_vol > max_p95_vol:
        return f"profile_p95_abs_1m_return_{p95_vol:.4f}_exceeds_{max_p95_vol:.4f}_bps"

    return None


def build_runtimes(config: dict[str, Any], args: argparse.Namespace) -> list[TraderRuntime]:
    runtimes: list[TraderRuntime] = []
    for trader in config["traders"]:
        if not isinstance(trader, dict):
            continue
        trader_id = str(trader.get("id") or "")
        if not trader_id:
            raise ValueError("Every trader must have an id.")
        symbol = str(trader.get("symbol") or "").upper()
        if not symbol:
            raise ValueError(f"{trader_id}: missing symbol.")

        mode = args.force_mode or str(trader.get("mode") or "paper").lower()
        credential_profile = trader.get("credential_profile")
        data = trader.get("data") if isinstance(trader.get("data"), dict) else {}
        feed = args.force_feed or str(data.get("feed") or "iex").lower()
        renko_source = normalize_renko_source(data.get("renko_source"))
        log_path = args.log_output or trader.get("event_log") or "artifacts/live/fleet_events.jsonl"
        risk = trader.get("risk") if isinstance(trader.get("risk"), dict) else {}
        max_position_notional = as_float(risk.get("max_position_notional"), default=250000.0)
        max_daily_loss = as_float(risk.get("max_daily_loss"), default=0.0)
        execution_enabled = bool(trader.get("execution_enabled", True))
        behavior_block_reason = behavior_profile_block_reason(trader)
        log = JsonlEventLog(log_path)
        strategy_config = build_strategy_config(trader)
        if isinstance(strategy_config, MeanReversionStrategyConfig):
            engine = MeanReversionStrategyEngine(strategy_config)
        else:
            engine = RenkoStrategyEngine(strategy_config)
        try:
            restored, restored_path = restore_engine_state(archive_root(args), trader_id, engine)
        except Exception as exc:
            restored = False
            restored_path = None
            log.write(
                "state_restore_error",
                {
                    "trader_id": trader_id,
                    "symbol": symbol,
                    "error": str(exc),
                },
            )

        execution = (
            AlpacaExecutionEngine(
                mode=mode,
                credential_profile=credential_profile,
                max_notional=max_position_notional,
                log=log,
            )
            if args.execute_real and execution_enabled
            else ShadowExecutionEngine(log=log)
        )
        runtime = TraderRuntime(
            trader_id=trader_id,
            symbol=symbol,
            mode=mode,
            credential_profile=str(credential_profile) if credential_profile else None,
            feed=feed,
            renko_source=renko_source,
            enabled=bool(trader.get("enabled", True)) and behavior_block_reason is None,
            execution_enabled=execution_enabled,
            max_position_notional=max_position_notional,
            max_daily_loss=max_daily_loss,
            engine=engine,
            execution=execution,
            log=log,
        )
        runtime.log.write(
            "trader_configured",
            {
                "trader_id": runtime.trader_id,
                "symbol": runtime.symbol,
                "mode": runtime.mode,
                "credential_profile": runtime.credential_profile,
                "feed": runtime.feed,
                "renko_source": runtime.renko_source,
                "enabled": runtime.enabled,
                "execution_enabled": runtime.execution_enabled,
                "execution_mode": "real" if args.execute_real and runtime.execution_enabled else "shadow",
                "max_position_notional": runtime.max_position_notional,
                "max_daily_loss": runtime.max_daily_loss,
                "behavior_profile_block_reason": behavior_block_reason,
                "state_restored": restored,
                "state_path": restored_path,
            },
        )
        if behavior_block_reason:
            runtime.log.write(
                "behavior_profile_block",
                {
                    "trader_id": runtime.trader_id,
                    "symbol": runtime.symbol,
                    "reason": behavior_block_reason,
                },
            )
        warm_start = trader.get("warm_start") if isinstance(trader.get("warm_start"), dict) else {}
        if not restored and bool(warm_start.get("enabled", False)):
            warm_start_runtime_from_archived_ticks(
                archive_root(args),
                runtime,
                days=int(warm_start.get("days", 10)),
            )
        reconcile_engine_with_broker(archive_root(args), runtime)
        runtimes.append(runtime)
    return runtimes


def group_runtimes(runtimes: list[TraderRuntime]) -> dict[tuple[str, str], list[TraderRuntime]]:
    grouped: dict[tuple[str, str], list[TraderRuntime]] = {}
    for runtime in runtimes:
        if not runtime.enabled:
            continue
        grouped.setdefault((runtime.mode, runtime.feed), []).append(runtime)
    return grouped


def decode_ws_payload(raw_message: str | bytes) -> Any:
    if isinstance(raw_message, bytes):
        raw_message = raw_message.decode("utf-8")
    return json.loads(raw_message)


def should_block_for_risk(runtime: TraderRuntime, signal: Signal, intent: OrderIntent) -> str | None:
    if signal.signal_type != "entry":
        return None
    estimated_order_notional = intent.notional or ((intent.qty or 0.0) * signal.price)
    reason_text = str(signal.reason or "").lower()
    is_reversal = "reversal" in reason_text or "reverse" in reason_text
    max_order_notional = runtime.max_position_notional * (2 if is_reversal else 1)
    notional_tolerance = max(1.0, max_order_notional * 0.01)
    if (
        estimated_order_notional
        and runtime.max_position_notional
        and estimated_order_notional > max_order_notional + notional_tolerance
    ):
        return f"entry_notional_exceeds_limit_{runtime.max_position_notional:g}"
    if isinstance(runtime.execution, AlpacaExecutionEngine):
        account = runtime.execution.trader.get_account()
        available_buying_power = max(
            as_float(account.get("buying_power"), default=0.0),
            as_float(account.get("daytrading_buying_power"), default=0.0),
        )
        if not is_reversal and estimated_order_notional and available_buying_power < estimated_order_notional:
            return f"buying_power_{available_buying_power:.2f}_below_notional_{estimated_order_notional:.2f}"
        if intent.side == "sell" and not bool(account.get("shorting_enabled")):
            return "account_shorting_disabled"
        positions = runtime.execution.trader.get_positions()
        for position in positions:
            if str(position.get("symbol", "")).upper() != runtime.symbol:
                continue
            market_value = abs(as_float(position.get("market_value"), default=0.0))
            unrealized_pl = as_float(position.get("unrealized_pl"), default=0.0)
            if market_value > 0 and not is_reversal:
                return "broker_position_already_open"
            if runtime.max_daily_loss and unrealized_pl <= -abs(runtime.max_daily_loss):
                return f"symbol_unrealized_loss_exceeds_limit_{runtime.max_daily_loss:g}"
    return None


def handle_action(runtime: TraderRuntime, signal: Signal, intent: OrderIntent) -> bool:
    block_reason = should_block_for_risk(runtime, signal, intent)
    if block_reason:
        runtime.log.write(
            "risk_block",
            {
                "trader_id": runtime.trader_id,
                "symbol": runtime.symbol,
                "reason": block_reason,
                "signal": dataclass_to_jsonable(signal),
                "intent": dataclass_to_jsonable(intent),
            },
        )
        return False

    runtime.log.write("signal", dataclass_to_jsonable(signal))
    try:
        runtime.execution.execute(intent)
    except Exception as exc:
        runtime.log.write(
            "execution_error",
            {
                "trader_id": runtime.trader_id,
                "symbol": runtime.symbol,
                "error": str(exc),
                "signal": dataclass_to_jsonable(signal),
                "intent": dataclass_to_jsonable(intent),
            },
        )
        return False
    return True


async def run_market_stream(
    mode: str,
    feed: str,
    runtimes: list[TraderRuntime],
    args: argparse.Namespace,
) -> None:
    credentials = load_alpaca_credentials(mode, profile=None)
    data_url = DATA_STREAM_URL_TEMPLATE.format(feed=feed)
    root = archive_root(args)
    symbols = sorted({runtime.symbol for runtime in runtimes})
    by_symbol: dict[str, list[TraderRuntime]] = {}
    for runtime in runtimes:
        by_symbol.setdefault(runtime.symbol, []).append(runtime)
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    while True:
        try:
            async with websockets.connect(
                data_url,
                ssl=ssl_context,
                ping_interval=20,
                ping_timeout=20,
                max_queue=None,
            ) as websocket:
                await websocket.send(
                    json.dumps(
                        {
                            "action": "auth",
                            "key": credentials.key_id,
                            "secret": credentials.secret_key,
                        }
                    )
                )
                await websocket.send(json.dumps({"action": "subscribe", "trades": symbols}))
                for runtime in runtimes:
                    runtime.log.write(
                        "runner_started",
                        {
                            "trader_id": runtime.trader_id,
                            "symbol": runtime.symbol,
                            "mode": runtime.mode,
                            "credential_profile": runtime.credential_profile,
                            "feed": runtime.feed,
                            "renko_source": runtime.renko_source,
                            "execution_mode": "real" if args.execute_real and runtime.execution_enabled else "shadow",
                        },
                    )
                    archive_engine_state(root, runtime, reason="runner_started")

                async for raw_message in websocket:
                    messages = decode_ws_payload(raw_message)
                    if isinstance(messages, dict):
                        messages = [messages]

                    for message in messages:
                        message_type = message.get("T")
                        if message_type in {"success", "subscription"}:
                            for runtime in runtimes:
                                runtime.log.write(
                                    "stream_status",
                                    {
                                        "trader_id": runtime.trader_id,
                                        "mode": mode,
                                        "feed": feed,
                                        "payload": message,
                                    },
                                )
                            continue
                        if message_type == "error":
                            raise RuntimeError(f"Alpaca market stream error: {message}")
                        if message_type != "t":
                            continue

                        tick = MarketTick.from_alpaca_trade(message)
                        if not args.no_persist_ticks:
                            archive_tick(root, tick, mode=mode, feed=feed)
                        if not is_price_forming_trade(tick):
                            continue
                        for runtime in by_symbol.get(tick.symbol, []):
                            if args.log_ticks:
                                payload = dataclass_to_jsonable(tick)
                                payload["trader_id"] = runtime.trader_id
                                runtime.log.write("market_tick", payload)

                            broker_force_flat_if_due(
                                root,
                                runtime,
                                tick.timestamp,
                                price=tick.price,
                                source="tick",
                            )
                            reconcile_broker_state_if_due(root, runtime)
                            source_ticks = renko_source_ticks(runtime, tick)
                            if not source_ticks:
                                archive_engine_state(root, runtime, reason="tick")
                                continue

                            for source_tick in source_ticks:
                                trade_snapshot = runtime.engine.trade_state_snapshot()
                                renko_bricks, actions = runtime.engine.on_tick(source_tick)
                                for brick in renko_bricks:
                                    brick_payload = dataclass_to_jsonable(brick)
                                    runtime.log.write("renko_brick", brick_payload)
                                    archive_renko_brick(root, runtime, brick_payload)
                                for signal, intent in actions:
                                    if not handle_action(runtime, signal, intent):
                                        runtime.engine.restore_trade_state(trade_snapshot)
                                        reconcile_engine_with_broker(root, runtime)
                                        archive_engine_state(root, runtime, reason="action_rejected")
                                        break
                                    archive_engine_state(root, runtime, reason="action_executed")
                                else:
                                    archive_engine_state(root, runtime, reason=runtime.renko_source)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            for runtime in runtimes:
                runtime.log.write(
                    "stream_error",
                    {
                        "trader_id": runtime.trader_id,
                        "stream": "market_data",
                        "mode": mode,
                        "feed": feed,
                        "error": str(exc),
                        "timestamp": pd.Timestamp.now(tz="UTC").isoformat(),
                    },
            )
            await asyncio.sleep(args.reconnect_delay)


async def run_force_flat_scheduler(runtimes: list[TraderRuntime], args: argparse.Namespace) -> None:
    root = archive_root(args)
    while True:
        timestamp = pd.Timestamp.now(tz="UTC")
        for runtime in runtimes:
            if not runtime.enabled:
                continue
            try:
                broker_force_flat_if_due(root, runtime, timestamp, source="scheduler")
            except Exception as exc:
                runtime.log.write(
                    "broker_force_flat_scheduler_error",
                    {
                        "trader_id": runtime.trader_id,
                        "symbol": runtime.symbol,
                        "error": str(exc),
                        "timestamp": timestamp.isoformat(),
                    },
                )
        await asyncio.sleep(BROKER_FORCE_FLAT_POLL_SECONDS)


async def async_main() -> None:
    load_dotenv()
    args = build_parser().parse_args()
    config = load_config(args.config)
    runtimes = build_runtimes(config, args)
    grouped = group_runtimes(runtimes)
    if not grouped:
        raise RuntimeError("No enabled traders in fleet config.")

    tasks = [run_market_stream(mode=mode, feed=feed, runtimes=items, args=args) for (mode, feed), items in grouped.items()]
    tasks.append(run_force_flat_scheduler(runtimes, args))
    await asyncio.gather(*tasks)


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        print("Stopped fleet runner.")


if __name__ == "__main__":
    main()
