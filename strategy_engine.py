from __future__ import annotations

import uuid
from collections import deque
from dataclasses import dataclass
from datetime import time
from typing import Any, Literal
from zoneinfo import ZoneInfo

import pandas as pd

from broker_protection import verify_broker_native_protection
from exit_plan import build_renko_tp_sl_exit_plan
from renko_builder import RenkoBuildState, update_renko_build_state
from reversal_protection import PROTECTION_TYPE, verify_reversal_catastrophic_stop
from trading_events import MarketBar, MarketTick, OrderIntent, PositionState, RenkoBrick, Signal


MAX_CLIENT_ORDER_ID_LENGTH = 48
TP_SL_LIFECYCLE_FLAT = "FLAT"
TP_SL_LIFECYCLE_POSITION_OPEN = "OPEN"
TP_SL_LIFECYCLE_TRIGGERED = "EXIT_PENDING"
TP_SL_LIFECYCLE_EXIT_PENDING = "EXIT_PENDING"
TP_SL_LIFECYCLE_CLOSE_SUBMITTED = "EXIT_PENDING"
TP_SL_LIFECYCLE_CLOSE_FILLED = "FLAT"
TP_SL_LIFECYCLE_ENTRY_PLANNED = "ENTRY_PENDING"
TP_SL_LIFECYCLE_BRACKET_SUBMITTED = "ENTRY_PENDING"
TP_SL_LIFECYCLE_ENTRY_PARTIALLY_FILLED = "ENTRY_PENDING"
TP_SL_LIFECYCLE_ENTRY_FILLED_AWAITING_PROTECTION = "ENTRY_PENDING"
TP_SL_LIFECYCLE_OCO_RECOVERY_SUBMITTED = "ENTRY_PENDING"
TP_SL_LIFECYCLE_PROTECTED = "OPEN"
TP_SL_LIFECYCLE_EXIT_PARTIALLY_FILLED = "EXIT_PENDING"
TP_SL_LIFECYCLE_PROTECTION_FAILED = "ENTRY_PENDING"
TP_SL_LIFECYCLE_EMERGENCY_FLATTEN_PENDING = "EXIT_PENDING"
TP_SL_LIFECYCLE_EMERGENCY_FLATTEN_PLANNED = "EXIT_PENDING"
TP_SL_LIFECYCLE_EMERGENCY_FLATTEN_SUBMITTED = "EXIT_PENDING"
TP_SL_LIFECYCLE_EMERGENCY_FLATTEN_PARTIALLY_FILLED = "EXIT_PENDING"
TP_SL_LIFECYCLE_EMERGENCY_FLATTEN_FILLED = "FLAT"
TP_SL_LIFECYCLE_EMERGENCY_FLATTEN_FAILED = "EXIT_PENDING"
TP_SL_EXIT_LOCK_STATES = {
    TP_SL_LIFECYCLE_ENTRY_PLANNED,
    TP_SL_LIFECYCLE_EXIT_PENDING,
}
_LEGACY_RUNTIME_STATE_MAP = {
    "POSITION_OPEN": TP_SL_LIFECYCLE_POSITION_OPEN,
    "TP_OR_SL_TRIGGERED": TP_SL_LIFECYCLE_EXIT_PENDING,
    "CLOSE_SUBMITTED": TP_SL_LIFECYCLE_EXIT_PENDING,
    "CLOSE_FILLED": TP_SL_LIFECYCLE_FLAT,
    "ENTRY_PLANNED": TP_SL_LIFECYCLE_ENTRY_PLANNED,
    "BRACKET_SUBMITTED": TP_SL_LIFECYCLE_ENTRY_PLANNED,
    "ENTRY_PARTIALLY_FILLED": TP_SL_LIFECYCLE_ENTRY_PLANNED,
    "ENTRY_FILLED_AWAITING_PROTECTION": TP_SL_LIFECYCLE_ENTRY_PLANNED,
    "OCO_RECOVERY_SUBMITTED": TP_SL_LIFECYCLE_ENTRY_PLANNED,
    "PROTECTED": TP_SL_LIFECYCLE_POSITION_OPEN,
    "EXIT_PARTIALLY_FILLED": TP_SL_LIFECYCLE_EXIT_PENDING,
    "PROTECTION_FAILED": TP_SL_LIFECYCLE_ENTRY_PLANNED,
    "EMERGENCY_FLATTEN_PENDING": TP_SL_LIFECYCLE_EXIT_PENDING,
    "EMERGENCY_FLATTEN_PLANNED": TP_SL_LIFECYCLE_EXIT_PENDING,
    "EMERGENCY_FLATTEN_SUBMITTED": TP_SL_LIFECYCLE_EXIT_PENDING,
    "EMERGENCY_FLATTEN_PARTIALLY_FILLED": TP_SL_LIFECYCLE_EXIT_PENDING,
    "EMERGENCY_FLATTEN_FILLED": TP_SL_LIFECYCLE_FLAT,
    "EMERGENCY_FLATTEN_FAILED": TP_SL_LIFECYCLE_EXIT_PENDING,
    "REVERSAL_EXIT_PLANNED": TP_SL_LIFECYCLE_EXIT_PENDING,
}


@dataclass(frozen=True)
class RenkoStrategyConfig:
    symbol: str
    brick_size: float
    entry_bricks: int
    strategy_type: Literal["renko_tp_sl", "renko_psar", "renko_reversal"] = "renko_tp_sl"
    take_profit_bricks: int | None = None
    stop_loss_bricks: int | None = None
    catastrophic_stop_bricks: int | None = None
    psar_initial_stop_bricks: int | None = None
    psar_af_step: float | None = None
    psar_af_max: float | None = None
    trader_id: str | None = None
    allow_short: bool = True
    strategy_name: str = "renko_live_v1"
    account_namespace: str | None = None
    qty: float | None = None
    notional: float | None = None
    order_type: str = "market"
    exit_management: str = "strategy"
    stop_loss_order_type: str = "stop_market"
    stop_limit_offset_pct: float = 0.0
    protection_acknowledgement_timeout_seconds: float = 2.0
    protection_polling_interval_seconds: float = 0.25
    protection_max_recovery_attempts: int = 1
    emergency_flatten_enabled: bool = False
    renko_anchor_mode: str = "first_price"
    renko_anchor_price: float | None = None
    timezone: str = "America/New_York"
    entry_start: str | None = None
    entry_end: str | None = None
    force_flat: str | None = None
    active_start_date: str | None = None
    active_end_date: str | None = None
    allow_overnight: bool = False
    candidate_config_fingerprint: str | None = None
    market_contract_fingerprint: str | None = None

    def __post_init__(self) -> None:
        if self.brick_size <= 0:
            raise ValueError("brick_size must be > 0.")
        if self.entry_bricks <= 0:
            raise ValueError("entry_bricks must be > 0.")
        if self.strategy_type == "renko_tp_sl":
            if self.take_profit_bricks is None or self.take_profit_bricks <= 0:
                raise ValueError("take_profit_bricks must be > 0.")
            if self.stop_loss_bricks is None or self.stop_loss_bricks <= 0:
                raise ValueError("stop_loss_bricks must be > 0.")
        elif self.strategy_type == "renko_psar":
            if self.psar_initial_stop_bricks is None or self.psar_initial_stop_bricks <= 0:
                raise ValueError("psar_initial_stop_bricks must be > 0.")
            if self.psar_af_step is None or self.psar_af_step <= 0:
                raise ValueError("psar_af_step must be > 0.")
            if self.psar_af_max is None or self.psar_af_max <= 0:
                raise ValueError("psar_af_max must be > 0.")
            if self.psar_af_step > self.psar_af_max:
                raise ValueError("psar_af_step must be <= psar_af_max.")
        elif self.strategy_type == "renko_reversal":
            pass
        else:
            raise ValueError(f"Unsupported strategy_type: {self.strategy_type}")
        if self.qty is None and self.notional is None:
            raise ValueError("Either qty or notional is required.")
        if self.qty is not None and self.qty <= 0:
            raise ValueError("qty must be > 0.")
        if self.notional is not None and self.notional <= 0:
            raise ValueError("notional must be > 0.")
        if str(self.order_type).lower().strip() not in {"market", "limit"}:
            raise ValueError("order_type must be market or limit.")
        if str(self.exit_management).lower().strip() not in {"strategy", "oco", "bracket", "catastrophic_stop"}:
            raise ValueError("exit_management must be strategy, oco, bracket, or catastrophic_stop.")
        if str(self.exit_management).lower().strip() == "bracket":
            if not self.candidate_config_fingerprint:
                raise ValueError("bracket exit_management requires candidate_config_fingerprint.")
            if not self.market_contract_fingerprint:
                raise ValueError("bracket exit_management requires market_contract_fingerprint.")
        if str(self.exit_management).lower().strip() == "catastrophic_stop":
            if self.strategy_type != "renko_reversal":
                raise ValueError("catastrophic_stop exit_management is only supported for renko_reversal.")
            if self.catastrophic_stop_bricks is None or self.catastrophic_stop_bricks <= 0:
                raise ValueError("catastrophic_stop exit_management requires catastrophic_stop_bricks > 0.")
            if not self.candidate_config_fingerprint:
                raise ValueError("catastrophic_stop exit_management requires candidate_config_fingerprint.")
            if not self.market_contract_fingerprint:
                raise ValueError("catastrophic_stop exit_management requires market_contract_fingerprint.")
        if str(self.stop_loss_order_type).lower().strip() not in {"stop_market", "stop_limit"}:
            raise ValueError("stop_loss_order_type must be stop_market or stop_limit.")
        if self.stop_limit_offset_pct < 0:
            raise ValueError("stop_limit_offset_pct must be >= 0.")
        if self.protection_acknowledgement_timeout_seconds <= 0:
            raise ValueError("protection_acknowledgement_timeout_seconds must be > 0.")
        if self.protection_polling_interval_seconds <= 0:
            raise ValueError("protection_polling_interval_seconds must be > 0.")
        if self.protection_max_recovery_attempts < 0:
            raise ValueError("protection_max_recovery_attempts must be >= 0.")
        _parse_time(self.entry_start, "entry_start")
        _parse_time(self.entry_end, "entry_end")
        _parse_time(self.force_flat, "force_flat")
        _parse_date(self.active_start_date, "active_start_date")
        _parse_date(self.active_end_date, "active_end_date")
        if self.active_start_date and self.active_end_date:
            if self.active_start_date > self.active_end_date:
                raise ValueError("active_start_date must be <= active_end_date.")
        normalized_anchor = str(self.renko_anchor_mode or "first_price").lower().strip()
        if normalized_anchor not in {"first_price", "first", "price", "grid", "zero_grid", "anchored_grid"}:
            raise ValueError("renko_anchor_mode must be first_price or grid.")


@dataclass(frozen=True)
class MeanReversionStrategyConfig:
    symbol: str
    window_seconds: int
    entry_z: float
    exit_z: float
    stop_z: float
    efficiency_max: float
    trend_exit_efficiency: float
    max_hold_seconds: int
    trader_id: str | None = None
    allow_short: bool = True
    strategy_name: str = "chop_mean_reversion_v1"
    account_namespace: str | None = None
    qty: float | None = None
    notional: float | None = None
    order_type: str = "market"
    timezone: str = "America/New_York"
    entry_start: str | None = None
    entry_end: str | None = None
    force_flat: str | None = None
    active_start_date: str | None = None
    active_end_date: str | None = None
    allow_overnight: bool = False

    @property
    def strategy_type(self) -> str:
        return "chop_mean_reversion"

    def __post_init__(self) -> None:
        if self.window_seconds <= 0:
            raise ValueError("window_seconds must be > 0.")
        if self.entry_z <= 0:
            raise ValueError("entry_z must be > 0.")
        if self.exit_z < 0:
            raise ValueError("exit_z must be >= 0.")
        if self.stop_z <= self.entry_z:
            raise ValueError("stop_z must be > entry_z.")
        if self.efficiency_max < 0:
            raise ValueError("efficiency_max must be >= 0.")
        if self.trend_exit_efficiency <= self.efficiency_max:
            raise ValueError("trend_exit_efficiency must be > efficiency_max.")
        if self.max_hold_seconds <= 0:
            raise ValueError("max_hold_seconds must be > 0.")
        if self.qty is None and self.notional is None:
            raise ValueError("Either qty or notional is required.")
        if self.qty is not None and self.qty <= 0:
            raise ValueError("qty must be > 0.")
        if self.notional is not None and self.notional <= 0:
            raise ValueError("notional must be > 0.")
        if str(self.order_type).lower().strip() not in {"market", "limit"}:
            raise ValueError("order_type must be market or limit.")
        _parse_time(self.entry_start, "entry_start")
        _parse_time(self.entry_end, "entry_end")
        _parse_time(self.force_flat, "force_flat")
        _parse_date(self.active_start_date, "active_start_date")
        _parse_date(self.active_end_date, "active_end_date")
        if self.active_start_date and self.active_end_date and self.active_start_date > self.active_end_date:
            raise ValueError("active_start_date must be <= active_end_date.")


def _parse_time(value: str | None, field_name: str) -> time | None:
    if value in {None, ""}:
        return None
    parts = str(value).split(":")
    if len(parts) not in {2, 3}:
        raise ValueError(f"{field_name} must be HH:MM or HH:MM:SS.")
    hour = int(parts[0])
    minute = int(parts[1])
    second = int(parts[2]) if len(parts) == 3 else 0
    return time(hour=hour, minute=minute, second=second)


def _parse_date(value: str | None, field_name: str) -> str | None:
    if value in {None, ""}:
        return None
    try:
        pd.Timestamp(value).date()
    except Exception as exc:
        raise ValueError(f"{field_name} must be YYYY-MM-DD.") from exc
    return str(value)


def _canonical_runtime_state(value: object, *, has_position: bool = False) -> str:
    text = str(value or "").strip()
    if text in {TP_SL_LIFECYCLE_FLAT, TP_SL_LIFECYCLE_ENTRY_PLANNED, TP_SL_LIFECYCLE_POSITION_OPEN, TP_SL_LIFECYCLE_EXIT_PENDING}:
        return text
    if text in _LEGACY_RUNTIME_STATE_MAP:
        return _LEGACY_RUNTIME_STATE_MAP[text]
    return TP_SL_LIFECYCLE_POSITION_OPEN if has_position else TP_SL_LIFECYCLE_FLAT


def _verification_issues(verification: object) -> list[dict[str, Any]]:
    return [issue.__dict__ for issue in getattr(verification, "issues", ())]


class RenkoStrategyEngine:
    def __init__(self, config: RenkoStrategyConfig) -> None:
        self.config = config
        self.renko_state = RenkoBuildState(
            brick_size=config.brick_size,
            anchor_mode=config.renko_anchor_mode,
            anchor_price=config.renko_anchor_price,
        )
        self.position: PositionState | None = None
        self.processed_bricks = 0
        self.entry_start = _parse_time(config.entry_start, "entry_start")
        self.entry_end = _parse_time(config.entry_end, "entry_end")
        self.force_flat = _parse_time(config.force_flat, "force_flat")
        self.active_start_date = _parse_date(config.active_start_date, "active_start_date")
        self.active_end_date = _parse_date(config.active_end_date, "active_end_date")
        self.timezone = ZoneInfo(config.timezone)
        self.force_flat_dates: set[str] = set()
        self.psar_sar: float | None = None
        self.psar_ep: float | None = None
        self.psar_af: float | None = None
        self.tp_sl_lifecycle_state = TP_SL_LIFECYCLE_FLAT
        self.pending_exit: dict[str, Any] | None = None
        self.last_closed_exit: dict[str, Any] | None = None
        self.protection_state: dict[str, Any] | None = None
        self.reconciliation_unsafe = False

    def snapshot(self) -> dict[str, Any]:
        return {
            "version": 1,
            "trader_id": self.config.trader_id,
            "symbol": self.config.symbol.upper(),
            "strategy_name": self.config.strategy_name,
            "strategy_type": self.config.strategy_type,
            "renko": {
                "brick_size": self.renko_state.brick_size,
                "last_brick_close": self.renko_state.last_brick_close,
                "run_dir": self.renko_state.run_dir,
                "run_len": self.renko_state.run_len,
                "processed_bricks": self.processed_bricks,
                "anchor_mode": self.renko_state.anchor_mode,
                "anchor_price": self.renko_state.anchor_price,
            },
            "trade": self.trade_state_snapshot(),
        }

    def trade_state_snapshot(self) -> dict[str, Any]:
        position = None
        if self.position is not None:
            position = {
                "symbol": self.position.symbol,
                "side": self.position.side,
                "entry_time": self.position.entry_time.isoformat(),
                "entry_price": self.position.entry_price,
                "qty": self.position.qty,
            }
        return {
            "position": position,
            "psar_sar": self.psar_sar,
            "psar_ep": self.psar_ep,
            "psar_af": self.psar_af,
            "force_flat_dates": sorted(self.force_flat_dates),
            "tp_sl_lifecycle_state": self.tp_sl_lifecycle_state,
            "pending_exit": self.pending_exit,
            "last_closed_exit": self.last_closed_exit,
            "protection_state": self.protection_state,
            "reconciliation_unsafe": self.reconciliation_unsafe,
        }

    def restore_snapshot(self, snapshot: dict[str, Any]) -> None:
        if str(snapshot.get("symbol", "")).upper() != self.config.symbol.upper():
            raise ValueError("Snapshot symbol does not match strategy config.")
        if str(snapshot.get("strategy_type", "")) != self.config.strategy_type:
            raise ValueError("Snapshot strategy_type does not match strategy config.")

        renko = snapshot.get("renko")
        if not isinstance(renko, dict):
            raise ValueError("Snapshot missing renko state.")
        brick_size = float(renko.get("brick_size", 0.0))
        if abs(brick_size - self.config.brick_size) > 1e-9:
            raise ValueError("Snapshot brick_size does not match strategy config.")
        snapshot_anchor_mode = str(renko.get("anchor_mode") or "first_price").lower().strip()
        config_anchor_mode = str(self.config.renko_anchor_mode or "first_price").lower().strip()
        if snapshot_anchor_mode != config_anchor_mode:
            raise ValueError("Snapshot renko anchor_mode does not match strategy config.")
        snapshot_anchor_price = renko.get("anchor_price")
        config_anchor_price = self.config.renko_anchor_price
        if snapshot_anchor_price is not None or config_anchor_price is not None:
            if abs(float(snapshot_anchor_price or 0.0) - float(config_anchor_price or 0.0)) > 1e-9:
                raise ValueError("Snapshot renko anchor_price does not match strategy config.")

        self.renko_state = RenkoBuildState(
            brick_size=self.config.brick_size,
            last_brick_close=float(renko["last_brick_close"]) if renko.get("last_brick_close") is not None else None,
            run_dir=int(renko.get("run_dir") or 0),
            run_len=int(renko.get("run_len") or 0),
            anchor_mode=self.config.renko_anchor_mode,
            anchor_price=self.config.renko_anchor_price,
        )
        self.processed_bricks = 0
        trade = snapshot.get("trade")
        if isinstance(trade, dict):
            self.restore_trade_state(trade)

    def restore_trade_state(self, snapshot: dict[str, Any]) -> None:
        position = snapshot.get("position")
        if isinstance(position, dict):
            self.position = PositionState(
                symbol=str(position["symbol"]).upper(),
                side=str(position["side"]),  # type: ignore[arg-type]
                entry_time=pd.Timestamp(position["entry_time"]),
                entry_price=float(position["entry_price"]),
                qty=float(position["qty"]) if position.get("qty") is not None else None,
            )
        else:
            self.position = None

        self.psar_sar = float(snapshot["psar_sar"]) if snapshot.get("psar_sar") is not None else None
        self.psar_ep = float(snapshot["psar_ep"]) if snapshot.get("psar_ep") is not None else None
        self.psar_af = float(snapshot["psar_af"]) if snapshot.get("psar_af") is not None else None
        force_flat_dates = snapshot.get("force_flat_dates")
        self.force_flat_dates = set(force_flat_dates if isinstance(force_flat_dates, list) else [])
        self.tp_sl_lifecycle_state = _canonical_runtime_state(
            snapshot.get("tp_sl_lifecycle_state"),
            has_position=self.position is not None,
        )
        pending_exit = snapshot.get("pending_exit")
        self.pending_exit = dict(pending_exit) if isinstance(pending_exit, dict) else None
        last_closed_exit = snapshot.get("last_closed_exit")
        self.last_closed_exit = dict(last_closed_exit) if isinstance(last_closed_exit, dict) else None
        protection_state = snapshot.get("protection_state")
        self.protection_state = dict(protection_state) if isinstance(protection_state, dict) else None
        if self.protection_state is not None:
            self.protection_state["state"] = _canonical_runtime_state(
                self.protection_state.get("state"),
                has_position=self.position is not None,
            )
        self.reconciliation_unsafe = bool(snapshot.get("reconciliation_unsafe", False))

    def on_bar(self, bar: MarketBar) -> list[tuple[Signal, OrderIntent]]:
        _, emitted = self.on_bar_with_bricks(bar)
        return emitted

    def on_bar_with_bricks(self, bar: MarketBar) -> tuple[list[RenkoBrick], list[tuple[Signal, OrderIntent]]]:
        if bar.symbol.upper() != self.config.symbol.upper():
            return [], []
        return self._process_price(timestamp=bar.timestamp, price=bar.close)

    def on_tick(self, tick: MarketTick) -> tuple[list[RenkoBrick], list[tuple[Signal, OrderIntent]]]:
        if tick.symbol.upper() != self.config.symbol.upper():
            return [], []
        return self._process_price(timestamp=tick.timestamp, price=tick.price)

    def _process_price(self, timestamp: pd.Timestamp, price: float) -> tuple[list[RenkoBrick], list[tuple[Signal, OrderIntent]]]:
        force_exit = self._force_flat_action(timestamp=timestamp, price=price)
        update_renko_build_state(
            self.renko_state,
            timestamps=[timestamp],
            prices=[price],
        )

        new_brick_rows = self.renko_state.bricks[self.processed_bricks :]
        self.processed_bricks = len(self.renko_state.bricks)

        completed_bricks = [self._build_renko_brick(row) for row in new_brick_rows]
        emitted: list[tuple[Signal, OrderIntent]] = []
        if force_exit is not None:
            emitted.append(force_exit)
        for brick in new_brick_rows:
            brick_actions = self._on_brick(brick)
            emitted.extend(brick_actions)
            if self.config.strategy_type == "renko_tp_sl" and brick_actions:
                break
        return completed_bricks, emitted

    def _build_renko_brick(self, brick: dict[str, object]) -> RenkoBrick:
        close = float(brick["brick_close"])
        direction = int(brick["direction"])
        open_price = close - (direction * self.config.brick_size)
        return RenkoBrick(
            timestamp=pd.Timestamp(brick["timestamp"]),
            symbol=self.config.symbol.upper(),
            open=open_price,
            close=close,
            direction=direction,
            run_len=int(brick["run_len"]),
            brick_size=self.config.brick_size,
            trader_id=self.config.trader_id,
        )

    def _on_brick(self, brick: dict[str, object]) -> list[tuple[Signal, OrderIntent]]:
        if self.config.strategy_type == "renko_psar":
            return self._on_brick_psar(brick)
        if self.config.strategy_type == "renko_reversal":
            return self._on_brick_reversal(brick)
        return self._on_brick_tp_sl(brick)

    def _on_brick_reversal(self, brick: dict[str, object]) -> list[tuple[Signal, OrderIntent]]:
        timestamp = pd.Timestamp(brick["timestamp"])
        price = float(brick["brick_close"])
        direction = int(brick["direction"])
        run_len = int(brick["run_len"])

        if run_len != self.config.entry_bricks:
            return []
        if direction == -1 and not self.config.allow_short:
            return []

        side = "long" if direction == 1 else "short"
        broker_side = "buy" if side == "long" else "sell"
        target_qty = self._target_qty(price)

        if self.position is not None:
            if self.position.side == side:
                return []
            if not self._reversal_entry_allowed(timestamp):
                return []
            if not self._reversal_allowed(timestamp):
                return []
            existing_qty = abs(float(self.position.qty or target_qty))
            reversal_qty = existing_qty + target_qty
            reason = f"{side}_entry_e1_reversal"
            self._mark_reversal_exit_planned(timestamp=timestamp, reason=reason, broker_side=broker_side)
            self.position = PositionState(
                symbol=self.config.symbol.upper(),
                side=side,  # type: ignore[arg-type]
                entry_time=timestamp,
                entry_price=price,
                qty=target_qty,
            )
            self._mark_reversal_entry_planned(timestamp=timestamp, side=side, price=price, qty=target_qty)
            signal = self._build_signal(
                signal_type="entry",
                timestamp=timestamp,
                side=side,
                broker_side=broker_side,
                price=price,
                reason=reason,
            )
            return [
                (
                    signal,
                    self._build_order_intent(
                        signal,
                        close_position=False,
                        qty=reversal_qty,
                        target_qty=target_qty,
                    ),
                )
            ]

        if not self._entry_allowed(timestamp):
            return []

        reason = f"{side}_entry_e1"
        self.position = PositionState(
            symbol=self.config.symbol.upper(),
            side=side,  # type: ignore[arg-type]
            entry_time=timestamp,
            entry_price=price,
            qty=target_qty,
        )
        self._mark_reversal_entry_planned(timestamp=timestamp, side=side, price=price, qty=target_qty)
        signal = self._build_signal(
            signal_type="entry",
            timestamp=timestamp,
            side=side,
            broker_side=broker_side,
            price=price,
            reason=reason,
        )
        return [(signal, self._build_order_intent(signal, close_position=False, qty=target_qty))]

    def _on_brick_tp_sl(self, brick: dict[str, object]) -> list[tuple[Signal, OrderIntent]]:
        timestamp = pd.Timestamp(brick["timestamp"])
        brick_price = float(brick["brick_close"])
        price = brick_price
        direction = int(brick["direction"])
        run_len = int(brick["run_len"])

        if self._tp_sl_entries_and_exits_locked():
            return []

        if self.position is not None:
            exit_reason = self._exit_reason(brick_price)
            if exit_reason:
                if str(self.config.exit_management).lower().strip() in {"oco", "bracket"}:
                    return []
                signal = self._build_signal(
                    signal_type="exit",
                    timestamp=timestamp,
                    side=self.position.side,
                    broker_side="sell" if self.position.side == "long" else "buy",
                    price=brick_price,
                    reason=exit_reason,
                )
                intent = self._build_order_intent(signal, close_position=True)
                self._mark_tp_sl_exit_pending(signal, intent)
                return [(signal, intent)]
            return []

        if run_len != self.config.entry_bricks:
            return []
        if not self._entry_allowed(timestamp):
            return []
        if direction == -1 and not self.config.allow_short:
            return []

        side = "long" if direction == 1 else "short"
        broker_side = "buy" if side == "long" else "sell"
        signal = self._build_signal(
            signal_type="entry",
            timestamp=timestamp,
            side=side,
            broker_side=broker_side,
            price=price,
            reason=f"renko_run_{run_len}_{side}",
        )
        intent = self._build_order_intent(signal, close_position=False)
        self.position = PositionState(
            symbol=self.config.symbol.upper(),
            side=side,
            entry_time=timestamp,
            entry_price=price,
            qty=intent.qty or self.config.qty,
        )
        self._mark_tp_sl_position_open()
        return [(signal, intent)]

    def _on_brick_psar(self, brick: dict[str, object]) -> list[tuple[Signal, OrderIntent]]:
        timestamp = pd.Timestamp(brick["timestamp"])
        price = float(brick["brick_close"])
        brick_size = self.config.brick_size
        direction = int(brick["direction"])
        run_len = int(brick["run_len"])
        emitted: list[tuple[Signal, OrderIntent]] = []

        if self.position is not None:
            existing_side = self.position.side
            position_qty = abs(float(self.position.qty or self._target_qty(price)))
            if existing_side == "long":
                candidate_sar = self._psar_sar_required() + (
                    self._psar_af_required() * (self._psar_ep_required() - self._psar_sar_required())
                )
                if price <= candidate_sar:
                    previous_ep = self._psar_ep_required()
                    if self.config.allow_short and self._reversal_allowed(timestamp):
                        target_qty = self._target_qty(price)
                        reversal_qty = position_qty + target_qty
                        self.position = self._open_psar_position(
                            side="short",
                            timestamp=timestamp,
                            price=price,
                            qty=target_qty,
                            psar_sar=previous_ep,
                            psar_ep=price,
                        )
                        signal = self._build_signal(
                            signal_type="entry",
                            timestamp=timestamp,
                            side="short",
                            broker_side="sell",
                            price=price,
                            reason="short_entry_psar_reversal",
                        )
                        emitted.append(
                            (
                                signal,
                                self._build_order_intent(
                                    signal,
                                    close_position=False,
                                    qty=reversal_qty,
                                    target_qty=target_qty,
                                ),
                            )
                        )
                    else:
                        emitted.append(self._close_psar_position(timestamp, price, reason="psar_exit"))
                    return emitted

                self.psar_sar = candidate_sar
                if price > self._psar_ep_required():
                    self.psar_ep = price
                    self.psar_af = min(self._psar_af_required() + self._psar_af_step(), self._psar_af_max())
                return []

            candidate_sar = self._psar_sar_required() - (
                self._psar_af_required() * (self._psar_sar_required() - self._psar_ep_required())
            )
            if price >= candidate_sar:
                previous_ep = self._psar_ep_required()
                if self._reversal_allowed(timestamp):
                    target_qty = self._target_qty(price)
                    reversal_qty = position_qty + target_qty
                    self.position = self._open_psar_position(
                        side="long",
                        timestamp=timestamp,
                        price=price,
                        qty=target_qty,
                        psar_sar=previous_ep,
                        psar_ep=price,
                    )
                    signal = self._build_signal(
                        signal_type="entry",
                        timestamp=timestamp,
                        side="long",
                        broker_side="buy",
                        price=price,
                        reason="long_entry_psar_reversal",
                    )
                    emitted.append(
                        (
                            signal,
                            self._build_order_intent(
                                signal,
                                close_position=False,
                                qty=reversal_qty,
                                target_qty=target_qty,
                            ),
                        )
                    )
                else:
                    emitted.append(self._close_psar_position(timestamp, price, reason="psar_exit"))
                return emitted

            self.psar_sar = candidate_sar
            if price < self._psar_ep_required():
                self.psar_ep = price
                self.psar_af = min(self._psar_af_required() + self._psar_af_step(), self._psar_af_max())
            return []

        if run_len != self.config.entry_bricks or not self._entry_allowed(timestamp):
            return []
        if direction == -1 and not self.config.allow_short:
            return []

        side = "long" if direction == 1 else "short"
        broker_side = "buy" if side == "long" else "sell"
        qty = self._target_qty(price)
        psar_sar = price - (self._psar_initial_stop_bricks() * brick_size) if side == "long" else price + (
            self._psar_initial_stop_bricks() * brick_size
        )
        self.position = self._open_psar_position(
            side=side,
            timestamp=timestamp,
            price=price,
            qty=qty,
            psar_sar=psar_sar,
            psar_ep=price,
        )
        signal = self._build_signal(
            signal_type="entry",
            timestamp=timestamp,
            side=side,
            broker_side=broker_side,
            price=price,
            reason=f"{side}_entry",
        )
        return [(signal, self._build_order_intent(signal, close_position=False, qty=qty))]

    def _exit_reason(self, price: float) -> str | None:
        if self.position is None:
            return None

        entry_dir = 1 if self.position.side == "long" else -1
        take_profit = self.position.entry_price + (
            entry_dir * self._take_profit_bricks() * self.config.brick_size
        )
        stop_loss = self.position.entry_price - (
            entry_dir * self._stop_loss_bricks() * self.config.brick_size
        )

        if self.position.side == "long":
            if price >= take_profit:
                return f"take_profit_{self._take_profit_bricks()}_bricks"
            if price <= stop_loss:
                return f"stop_loss_{self._stop_loss_bricks()}_bricks"
        else:
            if price <= take_profit:
                return f"take_profit_{self._take_profit_bricks()}_bricks"
            if price >= stop_loss:
                return f"stop_loss_{self._stop_loss_bricks()}_bricks"
        return None

    def _local_timestamp(self, timestamp: pd.Timestamp) -> pd.Timestamp:
        return pd.Timestamp(timestamp).tz_convert(self.timezone)

    def _entry_allowed(self, timestamp: pd.Timestamp) -> bool:
        local_timestamp = self._local_timestamp(timestamp)
        current_date = local_timestamp.date().isoformat()
        if self.active_start_date and current_date < self.active_start_date:
            return False
        if self.active_end_date and current_date > self.active_end_date:
            return False
        current_time = local_timestamp.time()
        if self.entry_start and current_time < self.entry_start:
            return False
        if self.entry_end and current_time > self.entry_end:
            return False
        if not self.config.allow_overnight and self.force_flat and current_time >= self.force_flat:
            return False
        return True

    def _reversal_entry_allowed(self, timestamp: pd.Timestamp) -> bool:
        local_timestamp = self._local_timestamp(timestamp)
        current_date = local_timestamp.date().isoformat()
        if self.active_start_date and current_date < self.active_start_date:
            return False
        if self.active_end_date and current_date > self.active_end_date:
            return False
        current_time = local_timestamp.time()
        if current_time < time(9, 30):
            return False
        if self.entry_end and current_time > self.entry_end:
            return False
        if not self.config.allow_overnight and self.force_flat and current_time >= self.force_flat:
            return False
        return True

    def _reversal_allowed(self, timestamp: pd.Timestamp) -> bool:
        if self.config.allow_overnight or self.force_flat is None:
            return True
        return self._local_timestamp(timestamp).time() < self.force_flat

    def _force_flat_action(self, timestamp: pd.Timestamp, price: float) -> tuple[Signal, OrderIntent] | None:
        if self.position is None or self.config.allow_overnight or self.force_flat is None:
            return None
        if self.config.strategy_type == "renko_tp_sl" and self._tp_sl_entries_and_exits_locked():
            return None

        local_timestamp = self._local_timestamp(timestamp)
        current_time = local_timestamp.time()
        current_date = local_timestamp.date().isoformat()
        if current_time < self.force_flat or current_date in self.force_flat_dates:
            return None

        self.force_flat_dates.add(current_date)
        signal = self._build_signal(
            signal_type="exit",
            timestamp=timestamp,
            side=self.position.side,
            broker_side="sell" if self.position.side == "long" else "buy",
            price=price,
            reason="session_force_flat",
        )
        intent = self._build_order_intent(signal, close_position=True)
        if self.config.strategy_type == "renko_tp_sl":
            self._mark_tp_sl_exit_pending(signal, intent)
        else:
            self.position = None
            self._reset_psar()
        return signal, intent

    def _close_psar_position(self, timestamp: pd.Timestamp, price: float, reason: str) -> tuple[Signal, OrderIntent]:
        if self.position is None:
            raise RuntimeError("Cannot close PSAR position while flat.")
        signal = self._build_signal(
            signal_type="exit",
            timestamp=timestamp,
            side=self.position.side,
            broker_side="sell" if self.position.side == "long" else "buy",
            price=price,
            reason=reason,
        )
        intent = self._build_order_intent(signal, close_position=True)
        self.position = None
        self._reset_psar()
        return signal, intent

    def _open_psar_position(
        self,
        *,
        side: str,
        timestamp: pd.Timestamp,
        price: float,
        qty: float,
        psar_sar: float,
        psar_ep: float,
    ) -> PositionState:
        self.psar_sar = psar_sar
        self.psar_ep = psar_ep
        self.psar_af = self._psar_af_step()
        return PositionState(
            symbol=self.config.symbol.upper(),
            side=side,  # type: ignore[arg-type]
            entry_time=timestamp,
            entry_price=price,
            qty=qty,
        )

    def _reset_psar(self) -> None:
        self.psar_sar = None
        self.psar_ep = None
        self.psar_af = None

    def _mark_tp_sl_position_open(self) -> None:
        if self.config.strategy_type != "renko_tp_sl":
            return
        if str(self.config.exit_management).lower().strip() == "bracket":
            self.tp_sl_lifecycle_state = TP_SL_LIFECYCLE_ENTRY_PLANNED
            return
        self.tp_sl_lifecycle_state = TP_SL_LIFECYCLE_POSITION_OPEN
        self.pending_exit = None

    def _mark_tp_sl_exit_pending(self, signal: Signal, intent: OrderIntent) -> None:
        if self.config.strategy_type != "renko_tp_sl":
            return
        self.tp_sl_lifecycle_state = TP_SL_LIFECYCLE_TRIGGERED
        self.pending_exit = {
            "state": TP_SL_LIFECYCLE_EXIT_PENDING,
            "signal_id": signal.signal_id,
            "client_order_id": intent.client_order_id,
            "reason": signal.reason,
            "side": signal.side,
            "broker_side": intent.side,
            "timestamp": signal.timestamp.isoformat(),
        }
        self.tp_sl_lifecycle_state = TP_SL_LIFECYCLE_EXIT_PENDING

    def mark_close_submitted(self, intent: OrderIntent, response: object | None = None) -> None:
        if (
            self.config.strategy_type != "renko_tp_sl"
            and not self._reversal_catastrophic_stop_enabled()
        ) or not intent.close_position:
            return
        if self.pending_exit is None:
            self.pending_exit = {
                "state": TP_SL_LIFECYCLE_CLOSE_SUBMITTED,
                "client_order_id": intent.client_order_id,
                "reason": intent.reason,
                "broker_side": intent.side,
                "timestamp": intent.timestamp.isoformat(),
            }
        else:
            self.pending_exit = dict(self.pending_exit)
        self.pending_exit["state"] = TP_SL_LIFECYCLE_CLOSE_SUBMITTED
        broker_order_id = _broker_order_id(response)
        if broker_order_id:
            self.pending_exit["broker_order_id"] = broker_order_id
        self.tp_sl_lifecycle_state = TP_SL_LIFECYCLE_CLOSE_SUBMITTED

    def mark_close_filled(
        self,
        *,
        timestamp: object | None = None,
        reason: str | None = None,
        order_id: str | None = None,
        source: str = "reconciliation",
    ) -> None:
        if self.config.strategy_type != "renko_tp_sl" and not self._reversal_catastrophic_stop_enabled():
            return
        self.tp_sl_lifecycle_state = TP_SL_LIFECYCLE_CLOSE_FILLED
        self.last_closed_exit = {
            "timestamp": timestamp.isoformat() if hasattr(timestamp, "isoformat") else timestamp,
            "reason": reason or (self.pending_exit or {}).get("reason"),
            "order_id": order_id or (self.pending_exit or {}).get("broker_order_id"),
            "source": source,
        }
        self.position = None
        self.pending_exit = None
        self.protection_state = None
        self.tp_sl_lifecycle_state = TP_SL_LIFECYCLE_FLAT

    def mark_external_close_filled(
        self,
        *,
        timestamp: object | None,
        reason: str,
        order_id: str | None,
        source: str,
        fill_price: object | None = None,
        filled_qty: object | None = None,
        realized_pnl: object | None = None,
    ) -> None:
        if self.config.strategy_type != "renko_tp_sl" and not self._reversal_catastrophic_stop_enabled():
            return
        previous_position = self.position
        self.mark_close_filled(timestamp=timestamp, reason=reason, order_id=order_id, source=source)
        if self.last_closed_exit is not None:
            previous_payload = None
            if previous_position is not None:
                previous_payload = {
                    "symbol": previous_position.symbol,
                    "side": previous_position.side,
                    "entry_time": previous_position.entry_time.isoformat()
                    if hasattr(previous_position.entry_time, "isoformat")
                    else previous_position.entry_time,
                    "entry_price": previous_position.entry_price,
                    "qty": previous_position.qty,
                }
            self.last_closed_exit.update(
                {
                    "external": True,
                    "fill_price": fill_price,
                    "filled_qty": filled_qty,
                    "realized_pnl": realized_pnl,
                    "previous_position": previous_payload,
                }
            )

    def _sync_position_from_broker(self, broker_position: dict[str, Any], timestamp: object | None = None) -> None:
        side = str(broker_position.get("side") or "").lower().strip()
        if side not in {"long", "short"}:
            qty_value = float(broker_position.get("qty") or 0)
            side = "long" if qty_value >= 0 else "short"
        self.position = PositionState(
            symbol=self.config.symbol.upper(),
            side=side,  # type: ignore[arg-type]
            entry_time=pd.Timestamp(timestamp if timestamp is not None else pd.Timestamp.now(tz="UTC")),
            entry_price=float(broker_position.get("avg_entry_price") or broker_position.get("current_price") or 0),
            qty=abs(float(broker_position.get("qty") or broker_position.get("qty_available") or 0)),
        )

    def apply_tp_sl_protection_evidence(
        self,
        *,
        exit_plan: dict[str, Any] | None = None,
        verification: object,
        broker_position: dict[str, Any] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        if self.config.strategy_type != "renko_tp_sl":
            return
        if isinstance(broker_position, dict):
            self._sync_position_from_broker(broker_position)
        prior = dict(self.protection_state or {})
        plan = exit_plan if isinstance(exit_plan, dict) and exit_plan else prior.get("exit_plan") or {}
        protected = bool(getattr(verification, "protected", False))
        protection = {
            "state": TP_SL_LIFECYCLE_POSITION_OPEN if protected else TP_SL_LIFECYCLE_ENTRY_PLANNED,
            "protection_type": "broker_native_tp_sl",
            "order_class": plan.get("order_class") or prior.get("order_class") or "bracket",
            "lifecycle_id": plan.get("lifecycle_id") or prior.get("lifecycle_id"),
            "client_order_id": plan.get("client_order_id") or prior.get("client_order_id"),
            "parent_order_id": getattr(verification, "parent_order_id", None) or prior.get("parent_order_id"),
            "take_profit_order_id": getattr(verification, "take_profit_order_id", None)
            or prior.get("take_profit_order_id"),
            "stop_loss_order_id": getattr(verification, "stop_loss_order_id", None) or prior.get("stop_loss_order_id"),
            "take_profit_client_order_id": plan.get("take_profit_client_order_id")
            or prior.get("take_profit_client_order_id"),
            "stop_loss_client_order_id": plan.get("stop_loss_client_order_id") or prior.get("stop_loss_client_order_id"),
            "take_profit_limit_price": plan.get("take_profit_limit_price") or prior.get("take_profit_limit_price"),
            "stop_loss_stop_price": plan.get("stop_loss_trigger_price") or prior.get("stop_loss_stop_price"),
            "stop_loss_trigger_price": plan.get("stop_loss_trigger_price") or prior.get("stop_loss_trigger_price"),
            "protected_quantity": getattr(verification, "protected_quantity", None),
            "exit_plan": plan,
            "verification_issues": _verification_issues(verification),
        }
        if extra:
            protection.update(extra)
        self.protection_state = {key: value for key, value in protection.items() if value is not None}
        self.tp_sl_lifecycle_state = protection["state"]
        self.pending_exit = None

    def apply_reversal_stop_evidence(
        self,
        *,
        stop_plan: dict[str, Any],
        verification: object,
        broker_position: dict[str, Any] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        if not self._reversal_catastrophic_stop_enabled():
            return
        if isinstance(broker_position, dict):
            self._sync_position_from_broker(broker_position)
        prior = dict(self.protection_state or {})
        plan = stop_plan if isinstance(stop_plan, dict) else prior.get("stop_plan") or {}
        protected = bool(getattr(verification, "protected", False))
        protection = {
            "state": TP_SL_LIFECYCLE_POSITION_OPEN if protected else TP_SL_LIFECYCLE_ENTRY_PLANNED,
            "protection_type": PROTECTION_TYPE,
            "order_class": "single_stop",
            "lifecycle_id": plan.get("lifecycle_id") or prior.get("lifecycle_id"),
            "client_order_id": plan.get("client_order_id") or prior.get("client_order_id"),
            "stop_order_id": getattr(verification, "stop_order_id", None) or prior.get("stop_order_id"),
            "stop_client_order_id": getattr(verification, "stop_client_order_id", None)
            or plan.get("stop_client_order_id")
            or prior.get("stop_client_order_id"),
            "stop_price": getattr(verification, "stop_price", None) or plan.get("stop_price") or prior.get("stop_price"),
            "returned_limit_price": getattr(verification, "returned_limit_price", None),
            "protected_quantity": getattr(verification, "protected_quantity", None),
            "stop_plan": plan,
            "verification_issues": _verification_issues(verification),
        }
        if extra:
            protection.update(extra)
        self.protection_state = {key: value for key, value in protection.items() if value is not None}
        self.tp_sl_lifecycle_state = protection["state"]
        self.pending_exit = None

    def mark_entry_protection_submitted(self, intent: OrderIntent, response: object | None = None) -> None:
        if self.config.strategy_type != "renko_tp_sl":
            return
        exit_plan = intent.exit_plan or {}
        broker_order = response.get("response") if isinstance(response, dict) and isinstance(response.get("response"), dict) else response
        entry_side = str(intent.side).lower().strip()
        position_side = "long" if entry_side == "buy" else "short"
        synthetic_position = {
            "symbol": intent.symbol,
            "side": position_side,
            "qty": str(intent.qty or (exit_plan or {}).get("quantity") or 0),
            "avg_entry_price": (exit_plan or {}).get("entry_reference_price") or intent.limit_price,
        }
        verification = verify_broker_native_protection(
            symbol=intent.symbol,
            broker_position=synthetic_position,
            nested_orders=[broker_order] if isinstance(broker_order, dict) else [],
            exit_plan=exit_plan,
            expected_config_fingerprint=self.config.candidate_config_fingerprint,
            expected_market_contract_fingerprint=self.config.market_contract_fingerprint,
        )
        extra = _broker_leg_ids(response)
        broker_order_id = _broker_order_id(response)
        if broker_order_id:
            extra["parent_order_id"] = broker_order_id
        self.apply_tp_sl_protection_evidence(exit_plan=exit_plan, verification=verification, extra=extra)

    def mark_reversal_catastrophic_stop_submitted(self, intent: OrderIntent, response: object | None = None) -> None:
        if not self._reversal_catastrophic_stop_enabled():
            return
        payload = response if isinstance(response, dict) else {}
        stop_response = payload.get("catastrophic_stop_response") if isinstance(payload.get("catastrophic_stop_response"), dict) else {}
        stop_plan = stop_response.get("stop_plan") if isinstance(stop_response.get("stop_plan"), dict) else {}
        broker_position = stop_response.get("broker_position") if isinstance(stop_response.get("broker_position"), dict) else None
        stop_order = stop_response.get("response") if isinstance(stop_response.get("response"), dict) else None
        verification = verify_reversal_catastrophic_stop(
            symbol=intent.symbol,
            broker_position=broker_position,
            open_orders=[stop_order] if isinstance(stop_order, dict) else [],
            stop_plan=stop_plan,
            expected_config_fingerprint=self.config.candidate_config_fingerprint,
            expected_market_contract_fingerprint=self.config.market_contract_fingerprint,
        )
        self.apply_reversal_stop_evidence(stop_plan=stop_plan, verification=verification)

    def mark_position_unprotected(self, reason: str, payload: object | None = None) -> None:
        if self.config.strategy_type != "renko_tp_sl" and not self._reversal_catastrophic_stop_enabled():
            return
        prior_protection = self.protection_state if isinstance(self.protection_state, dict) else {}
        preserved = {
            key: value
            for key, value in prior_protection.items()
            if key
            in {
                "order_class",
                "lifecycle_id",
                "client_order_id",
                "parent_order_id",
                "take_profit_order_id",
                "take_profit_client_order_id",
                "take_profit_limit_price",
                "stop_loss_order_id",
                "stop_loss_client_order_id",
                "stop_loss_stop_price",
                "stop_loss_trigger_price",
                "exit_plan",
                "stop_plan",
                "stop_order_id",
                "stop_client_order_id",
                "stop_price",
                "protection_type",
            }
            and value is not None
        }
        broker_position = payload.get("broker_position") if isinstance(payload, dict) else None
        if isinstance(broker_position, dict):
            side = str(broker_position.get("side") or "").lower().strip()
            if side in {"long", "short"}:
                self.position = PositionState(
                    symbol=self.config.symbol.upper(),
                    side=side,  # type: ignore[arg-type]
                    entry_time=pd.Timestamp.now(tz="UTC"),
                    entry_price=float(broker_position.get("avg_entry_price") or broker_position.get("current_price") or 0),
                    qty=abs(float(broker_position.get("qty") or 0)),
                )
        self.tp_sl_lifecycle_state = TP_SL_LIFECYCLE_ENTRY_PLANNED
        self.protection_state = {
            **preserved,
            "state": TP_SL_LIFECYCLE_ENTRY_PLANNED,
            "reason": reason,
            "payload": payload,
        }

    def mark_reconciliation_unsafe(self, reason: str, evidence: dict[str, Any] | None = None) -> dict[str, Any] | None:
        previous = self.reconciliation_unsafe
        self.reconciliation_unsafe = True
        if self.config.strategy_type == "renko_tp_sl":
            pending = dict(self.pending_exit or {})
            pending["unsafe_reason"] = reason
            self.pending_exit = pending or None
        return self._reconciliation_transition_payload(
            previous=previous,
            new=True,
            reason=reason,
            evidence=evidence,
        )

    def mark_reconciliation_safe(self, reason: str, evidence: dict[str, Any] | None = None) -> dict[str, Any] | None:
        previous = self.reconciliation_unsafe
        self.reconciliation_unsafe = False
        return self._reconciliation_transition_payload(
            previous=previous,
            new=False,
            reason=reason,
            evidence=evidence,
        )

    def _reconciliation_transition_payload(
        self,
        *,
        previous: bool,
        new: bool,
        reason: str,
        evidence: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        if previous == new:
            return None
        return {
            "timestamp": pd.Timestamp.now(tz="UTC").isoformat(),
            "symbol": self.config.symbol.upper(),
            "previous_value": previous,
            "new_value": new,
            "reason": reason,
            "evidence": evidence or {},
        }

    def _reversal_catastrophic_stop_enabled(self) -> bool:
        return (
            self.config.strategy_type == "renko_reversal"
            and str(self.config.exit_management).lower().strip() == "catastrophic_stop"
        )

    def _mark_reversal_entry_planned(self, *, timestamp: pd.Timestamp, side: str, price: float, qty: float) -> None:
        if not self._reversal_catastrophic_stop_enabled():
            return
        prior_exit = dict(self.protection_state or {}) if isinstance(self.protection_state, dict) else None
        if prior_exit and prior_exit.get("state") != TP_SL_LIFECYCLE_EXIT_PENDING:
            prior_exit = None
        self.tp_sl_lifecycle_state = TP_SL_LIFECYCLE_ENTRY_PLANNED
        self.pending_exit = None
        self.protection_state = {
            "state": TP_SL_LIFECYCLE_ENTRY_PLANNED,
            "protection_type": PROTECTION_TYPE,
            "symbol": self.config.symbol.upper(),
            "side": side,
            "signal_reference_price": price,
            "qty": qty,
            "timestamp": timestamp.isoformat(),
        }
        if prior_exit is not None:
            self.protection_state["reversal_exit_planned"] = prior_exit

    def _mark_reversal_exit_planned(self, *, timestamp: pd.Timestamp, reason: str, broker_side: str) -> None:
        if not self._reversal_catastrophic_stop_enabled():
            return
        prior = dict(self.protection_state or {})
        prior.update(
            {
                "state": TP_SL_LIFECYCLE_EXIT_PENDING,
                "protection_type": PROTECTION_TYPE,
                "reason": reason,
                "broker_side": broker_side,
                "timestamp": timestamp.isoformat(),
            }
        )
        self.protection_state = prior
        self.tp_sl_lifecycle_state = TP_SL_LIFECYCLE_EXIT_PENDING

    def _tp_sl_entries_and_exits_locked(self) -> bool:
        return (
            self.config.strategy_type == "renko_tp_sl"
            and (self.reconciliation_unsafe or self.tp_sl_lifecycle_state in TP_SL_EXIT_LOCK_STATES)
        )

    def _target_qty(self, price: float) -> float:
        if self.config.qty is not None:
            return float(self.config.qty)
        if self.config.notional is None:
            raise RuntimeError("Either qty or notional is required.")
        if price <= 0:
            return 1.0
        return max(0.000001, round(float(self.config.notional) / price, 6))

    def _take_profit_bricks(self) -> int:
        if self.config.take_profit_bricks is None:
            raise RuntimeError("take_profit_bricks is required for renko_tp_sl.")
        return int(self.config.take_profit_bricks)

    def _stop_loss_bricks(self) -> int:
        if self.config.stop_loss_bricks is None:
            raise RuntimeError("stop_loss_bricks is required for renko_tp_sl.")
        return int(self.config.stop_loss_bricks)

    def _psar_initial_stop_bricks(self) -> int:
        if self.config.psar_initial_stop_bricks is None:
            raise RuntimeError("psar_initial_stop_bricks is required for renko_psar.")
        return int(self.config.psar_initial_stop_bricks)

    def _psar_af_step(self) -> float:
        if self.config.psar_af_step is None:
            raise RuntimeError("psar_af_step is required for renko_psar.")
        return float(self.config.psar_af_step)

    def _psar_af_max(self) -> float:
        if self.config.psar_af_max is None:
            raise RuntimeError("psar_af_max is required for renko_psar.")
        return float(self.config.psar_af_max)

    def _psar_sar_required(self) -> float:
        if self.psar_sar is None:
            raise RuntimeError("PSAR SAR is not initialized.")
        return self.psar_sar

    def _psar_ep_required(self) -> float:
        if self.psar_ep is None:
            raise RuntimeError("PSAR EP is not initialized.")
        return self.psar_ep

    def _psar_af_required(self) -> float:
        if self.psar_af is None:
            raise RuntimeError("PSAR AF is not initialized.")
        return self.psar_af

    def _build_signal(
        self,
        signal_type: str,
        timestamp: pd.Timestamp,
        side: str,
        broker_side: str,
        price: float,
        reason: str,
    ) -> Signal:
        signal_prefix = self.config.trader_id or self.config.strategy_name
        signal_id = f"{signal_prefix}-{self.config.symbol.lower()}-{uuid.uuid4().hex[:12]}"
        return Signal(
            signal_id=signal_id,
            signal_type=signal_type,  # type: ignore[arg-type]
            timestamp=timestamp,
            symbol=self.config.symbol.upper(),
            side=side,  # type: ignore[arg-type]
            broker_side=broker_side,  # type: ignore[arg-type]
            price=price,
            reason=reason,
            strategy=self.config.strategy_name,
            trader_id=self.config.trader_id,
        )

    def _build_order_intent(
        self,
        signal: Signal,
        close_position: bool,
        qty: float | None = None,
        target_qty: float | None = None,
    ) -> OrderIntent:
        take_profit_limit_price: float | None = None
        stop_loss_stop_price: float | None = None
        stop_loss_limit_price: float | None = None
        exit_plan: dict[str, Any] | None = None
        exit_management = str(self.config.exit_management or "strategy").lower().strip()
        stop_loss_order_type = str(self.config.stop_loss_order_type or "stop_market").lower().strip()
        order_qty = qty
        order_notional = self.config.notional
        if (
            not close_position
            and signal.signal_type == "entry"
            and self.config.strategy_type == "renko_tp_sl"
            and exit_management in {"oco", "bracket"}
        ):
            entry_dir = 1 if signal.side == "long" else -1
            take_profit_offset = self._take_profit_bricks() * self.config.brick_size
            stop_loss_offset = self._stop_loss_bricks() * self.config.brick_size
            take_profit_limit_price = signal.price + (
                entry_dir * take_profit_offset
            )
            stop_loss_stop_price = signal.price - (
                entry_dir * stop_loss_offset
            )
            if stop_loss_order_type == "stop_limit":
                offset = float(self.config.stop_limit_offset_pct) / 100.0
                if signal.side == "long":
                    stop_loss_limit_price = stop_loss_stop_price * (1.0 - offset)
                else:
                    stop_loss_limit_price = stop_loss_stop_price * (1.0 + offset)
            if order_qty is None:
                order_qty = self._target_qty(signal.price)
                order_notional = None
            if exit_management == "bracket":
                exit_plan = build_renko_tp_sl_exit_plan(
                    trader_id=self.config.trader_id or self.config.strategy_name,
                    strategy_id=self.config.strategy_name,
                    symbol=signal.symbol,
                    entry_side=signal.broker_side,
                    quantity=order_qty,
                    entry_reference_type="signal_price",
                    entry_reference_price=signal.price,
                    brick_size=self.config.brick_size,
                    take_profit_bricks=self._take_profit_bricks(),
                    stop_loss_bricks=self._stop_loss_bricks(),
                    time_in_force="day",
                    order_class="bracket",
                    client_order_id=self._client_order_id(signal),
                    candidate_config_fingerprint=self.config.candidate_config_fingerprint,
                    market_contract_fingerprint=self.config.market_contract_fingerprint,
                ).to_dict()

                # The canonical ExitPlan owns broker tick rounding.
                take_profit_limit_price = float(exit_plan["take_profit_limit_price"])
                stop_loss_stop_price = float(exit_plan["stop_loss_trigger_price"])

        intent_qty = None
        if not close_position:
            intent_qty = order_qty if order_qty is not None else self.config.qty

        return OrderIntent(
            client_order_id=self._client_order_id(signal),
            timestamp=signal.timestamp,
            symbol=signal.symbol,
            side=signal.broker_side,
            signal_type=signal.signal_type,
            order_type=self.config.order_type,
            strategy=signal.strategy,
            reason=signal.reason,
            qty=intent_qty,
            target_qty=0 if close_position else target_qty,
            notional=None if order_qty is not None or close_position else order_notional,
            limit_price=signal.price if self.config.order_type == "limit" else None,
            exit_management=exit_management,
            stop_loss_order_type=stop_loss_order_type,
            take_profit_limit_price=take_profit_limit_price,
            stop_loss_stop_price=stop_loss_stop_price,
            stop_loss_limit_price=stop_loss_limit_price,
            take_profit_offset=self._take_profit_bricks() * self.config.brick_size
            if (
                not close_position
                and signal.signal_type == "entry"
                and self.config.strategy_type == "renko_tp_sl"
                and exit_management == "oco"
            )
            else None,
            stop_loss_offset=self._stop_loss_bricks() * self.config.brick_size
            if (
                not close_position
                and signal.signal_type == "entry"
                and self.config.strategy_type == "renko_tp_sl"
                and exit_management == "oco"
            )
            else None,
            exit_plan=exit_plan,
            close_position=close_position,
            trader_id=self.config.trader_id,
            signal_id=signal.signal_id,
            lifecycle_state=TP_SL_LIFECYCLE_TRIGGERED
            if close_position and self.config.strategy_type == "renko_tp_sl"
            else None,
            account_namespace=self.config.account_namespace,
            brick_size=self.config.brick_size,
            catastrophic_stop_bricks=self.config.catastrophic_stop_bricks
            if (
                not close_position
                and signal.signal_type == "entry"
                and self._reversal_catastrophic_stop_enabled()
            )
            else None,
            protection_type=PROTECTION_TYPE
            if (
                not close_position
                and signal.signal_type == "entry"
                and self._reversal_catastrophic_stop_enabled()
            )
            else None,
            candidate_config_fingerprint=self.config.candidate_config_fingerprint,
            market_contract_fingerprint=self.config.market_contract_fingerprint,
            strategy_type=self.config.strategy_type,
        )

    def _client_order_id(self, signal: Signal) -> str:
        suffix = signal.signal_id.rsplit("-", 1)[-1]
        raw_prefix = self.config.trader_id or self.config.strategy_name or signal.symbol.lower()
        prefix = "".join(char if char.isalnum() or char in {"-", "_"} else "-" for char in raw_prefix.lower())
        max_prefix_length = MAX_CLIENT_ORDER_ID_LENGTH - len(suffix) - 1
        return f"{prefix[:max_prefix_length]}-{suffix}"[:MAX_CLIENT_ORDER_ID_LENGTH]


def _broker_order_id(response: object | None) -> str | None:
    if not isinstance(response, dict):
        return None
    direct = response.get("id")
    if direct:
        return str(direct)
    child = response.get("response")
    if isinstance(child, dict) and child.get("id"):
        return str(child["id"])
    return None


def _broker_leg_ids(response: object | None) -> dict[str, str]:
    if not isinstance(response, dict):
        return {}
    order = response.get("response") if isinstance(response.get("response"), dict) else response
    if not isinstance(order, dict):
        return {}
    result: dict[str, str] = {}
    for leg in order.get("legs") or []:
        if not isinstance(leg, dict):
            continue
        leg_id = leg.get("id")
        if not leg_id:
            continue
        if leg.get("limit_price") is not None and "take_profit_order_id" not in result:
            result["take_profit_order_id"] = str(leg_id)
        if leg.get("stop_price") is not None and "stop_loss_order_id" not in result:
            result["stop_loss_order_id"] = str(leg_id)
    return result


class MeanReversionStrategyEngine:
    def __init__(self, config: MeanReversionStrategyConfig) -> None:
        self.config = config
        self.position: PositionState | None = None
        self.entry_z: float | None = None
        self.entry_efficiency: float | None = None
        self.points: deque[tuple[pd.Timestamp, float]] = deque()
        self.abs_moves: deque[tuple[pd.Timestamp, float]] = deque()
        self.sum_close = 0.0
        self.sum_close_sq = 0.0
        self.sum_abs_moves = 0.0
        self.last_price: float | None = None
        self.entry_start = _parse_time(config.entry_start, "entry_start")
        self.entry_end = _parse_time(config.entry_end, "entry_end")
        self.force_flat = _parse_time(config.force_flat, "force_flat")
        self.active_start_date = _parse_date(config.active_start_date, "active_start_date")
        self.active_end_date = _parse_date(config.active_end_date, "active_end_date")
        self.timezone = ZoneInfo(config.timezone)
        self.force_flat_dates: set[str] = set()

    def snapshot(self) -> dict[str, Any]:
        return {
            "version": 1,
            "trader_id": self.config.trader_id,
            "symbol": self.config.symbol.upper(),
            "strategy_name": self.config.strategy_name,
            "strategy_type": self.config.strategy_type,
            "mean_reversion": {
                "points": [(ts.isoformat(), price) for ts, price in self.points],
                "abs_moves": [(ts.isoformat(), move) for ts, move in self.abs_moves],
                "sum_close": self.sum_close,
                "sum_close_sq": self.sum_close_sq,
                "sum_abs_moves": self.sum_abs_moves,
                "last_price": self.last_price,
                "entry_z": self.entry_z,
                "entry_efficiency": self.entry_efficiency,
            },
            "trade": self.trade_state_snapshot(),
        }

    def restore_snapshot(self, snapshot: dict[str, Any]) -> None:
        if str(snapshot.get("symbol", "")).upper() != self.config.symbol.upper():
            raise ValueError("Snapshot symbol does not match strategy config.")
        if str(snapshot.get("strategy_type", "")) != self.config.strategy_type:
            raise ValueError("Snapshot strategy_type does not match strategy config.")
        state = snapshot.get("mean_reversion")
        if isinstance(state, dict):
            self.points = deque((pd.Timestamp(ts), float(price)) for ts, price in state.get("points", []))
            self.abs_moves = deque((pd.Timestamp(ts), float(move)) for ts, move in state.get("abs_moves", []))
            self.sum_close = float(state.get("sum_close") or 0.0)
            self.sum_close_sq = float(state.get("sum_close_sq") or 0.0)
            self.sum_abs_moves = float(state.get("sum_abs_moves") or 0.0)
            self.last_price = float(state["last_price"]) if state.get("last_price") is not None else None
            self.entry_z = float(state["entry_z"]) if state.get("entry_z") is not None else None
            self.entry_efficiency = (
                float(state["entry_efficiency"]) if state.get("entry_efficiency") is not None else None
            )
        trade = snapshot.get("trade")
        if isinstance(trade, dict):
            self.restore_trade_state(trade)

    def trade_state_snapshot(self) -> dict[str, Any]:
        position = None
        if self.position is not None:
            position = {
                "symbol": self.position.symbol,
                "side": self.position.side,
                "entry_time": self.position.entry_time.isoformat(),
                "entry_price": self.position.entry_price,
                "qty": self.position.qty,
            }
        return {
            "position": position,
            "psar_sar": None,
            "psar_ep": None,
            "psar_af": None,
            "force_flat_dates": sorted(self.force_flat_dates),
            "entry_z": self.entry_z,
            "entry_efficiency": self.entry_efficiency,
        }

    def restore_trade_state(self, snapshot: dict[str, Any]) -> None:
        position = snapshot.get("position")
        if isinstance(position, dict):
            self.position = PositionState(
                symbol=str(position["symbol"]).upper(),
                side=str(position["side"]),  # type: ignore[arg-type]
                entry_time=pd.Timestamp(position["entry_time"]),
                entry_price=float(position["entry_price"]),
                qty=float(position["qty"]) if position.get("qty") is not None else None,
            )
        else:
            self.position = None
        force_flat_dates = snapshot.get("force_flat_dates")
        self.force_flat_dates = set(force_flat_dates if isinstance(force_flat_dates, list) else [])
        self.entry_z = float(snapshot["entry_z"]) if snapshot.get("entry_z") is not None else None
        self.entry_efficiency = (
            float(snapshot["entry_efficiency"]) if snapshot.get("entry_efficiency") is not None else None
        )

    def on_bar(self, bar: MarketBar) -> list[tuple[Signal, OrderIntent]]:
        _, actions = self._process_price(timestamp=bar.timestamp, price=bar.close)
        return actions

    def on_bar_with_bricks(self, bar: MarketBar) -> tuple[list[RenkoBrick], list[tuple[Signal, OrderIntent]]]:
        if bar.symbol.upper() != self.config.symbol.upper():
            return [], []
        return self._process_price(timestamp=bar.timestamp, price=bar.close)

    def on_tick(self, tick: MarketTick) -> tuple[list[RenkoBrick], list[tuple[Signal, OrderIntent]]]:
        if tick.symbol.upper() != self.config.symbol.upper():
            return [], []
        return self._process_price(timestamp=tick.timestamp, price=tick.price)

    def _process_price(self, timestamp: pd.Timestamp, price: float) -> tuple[list[RenkoBrick], list[tuple[Signal, OrderIntent]]]:
        self._update_window(timestamp, price)
        actions: list[tuple[Signal, OrderIntent]] = []
        force_exit = self._force_flat_action(timestamp=timestamp, price=price)
        if force_exit is not None:
            actions.append(force_exit)
            return [], actions

        z_score, efficiency, warm = self._current_stats()
        if self.position is not None:
            exit_reason = self._exit_reason(timestamp, z_score, efficiency)
            if exit_reason:
                signal = self._build_signal(
                    signal_type="exit",
                    timestamp=timestamp,
                    side=self.position.side,
                    broker_side="sell" if self.position.side == "long" else "buy",
                    price=price,
                    reason=exit_reason,
                )
                intent = self._build_order_intent(signal, close_position=True)
                self.position = None
                self.entry_z = None
                self.entry_efficiency = None
                actions.append((signal, intent))
            return [], actions

        if not warm or not self._entry_allowed(timestamp):
            return [], []
        if efficiency > self.config.efficiency_max:
            return [], []

        if z_score <= -self.config.entry_z:
            side = "long"
            broker_side = "buy"
            reason = "long_entry_chop_mean_reversion"
        elif z_score >= self.config.entry_z and self.config.allow_short:
            side = "short"
            broker_side = "sell"
            reason = "short_entry_chop_mean_reversion"
        else:
            return [], []

        qty = self._target_qty(price)
        self.position = PositionState(
            symbol=self.config.symbol.upper(),
            side=side,  # type: ignore[arg-type]
            entry_time=timestamp,
            entry_price=price,
            qty=qty,
        )
        self.entry_z = z_score
        self.entry_efficiency = efficiency
        signal = self._build_signal(
            signal_type="entry",
            timestamp=timestamp,
            side=side,
            broker_side=broker_side,
            price=price,
            reason=reason,
        )
        actions.append((signal, self._build_order_intent(signal, close_position=False, qty=qty)))
        return [], actions

    def _update_window(self, timestamp: pd.Timestamp, price: float) -> None:
        timestamp = pd.Timestamp(timestamp).tz_convert("UTC")
        self.points.append((timestamp, price))
        self.sum_close += price
        self.sum_close_sq += price * price
        if self.last_price is not None:
            move = abs(price - self.last_price)
            self.abs_moves.append((timestamp, move))
            self.sum_abs_moves += move
        self.last_price = price

        cutoff = timestamp - pd.Timedelta(seconds=self.config.window_seconds)
        while self.points and self.points[0][0] < cutoff:
            _, old_price = self.points.popleft()
            self.sum_close -= old_price
            self.sum_close_sq -= old_price * old_price
        while self.abs_moves and self.abs_moves[0][0] <= cutoff:
            _, old_move = self.abs_moves.popleft()
            self.sum_abs_moves -= old_move

    def _current_stats(self) -> tuple[float, float, bool]:
        if not self.points:
            return 0.0, 0.0, False
        count = len(self.points)
        price = self.points[-1][1]
        mean = self.sum_close / count
        variance = max(0.0, (self.sum_close_sq / count) - mean * mean)
        stdev = variance**0.5
        z_score = (price - mean) / stdev if stdev > 1e-12 else 0.0
        warm = count >= max(30, min(self.config.window_seconds // 2, self.config.window_seconds - 1))
        if len(self.points) >= 2 and self.sum_abs_moves > 0:
            displacement = abs(price - self.points[0][1])
            efficiency = displacement / self.sum_abs_moves
        else:
            efficiency = 0.0
        return z_score, efficiency, warm

    def _exit_reason(self, timestamp: pd.Timestamp, z_score: float, efficiency: float) -> str | None:
        if self.position is None:
            return None
        held_seconds = (pd.Timestamp(timestamp) - self.position.entry_time).total_seconds()
        if self.position.side == "long":
            if z_score >= -self.config.exit_z:
                return "mean_reversion_exit"
            if z_score <= -self.config.stop_z:
                return "z_stop_exit"
        else:
            if z_score <= self.config.exit_z:
                return "mean_reversion_exit"
            if z_score >= self.config.stop_z:
                return "z_stop_exit"
        if efficiency >= self.config.trend_exit_efficiency:
            return "trend_efficiency_exit"
        if held_seconds >= self.config.max_hold_seconds:
            return "max_hold_exit"
        return None

    def _local_timestamp(self, timestamp: pd.Timestamp) -> pd.Timestamp:
        return pd.Timestamp(timestamp).tz_convert(self.timezone)

    def _entry_allowed(self, timestamp: pd.Timestamp) -> bool:
        local_timestamp = self._local_timestamp(timestamp)
        current_date = local_timestamp.date().isoformat()
        if self.active_start_date and current_date < self.active_start_date:
            return False
        if self.active_end_date and current_date > self.active_end_date:
            return False
        current_time = local_timestamp.time()
        if self.entry_start and current_time < self.entry_start:
            return False
        if self.entry_end and current_time > self.entry_end:
            return False
        if not self.config.allow_overnight and self.force_flat and current_time >= self.force_flat:
            return False
        return True

    def _force_flat_action(self, timestamp: pd.Timestamp, price: float) -> tuple[Signal, OrderIntent] | None:
        if self.position is None or self.config.allow_overnight or self.force_flat is None:
            return None
        local_timestamp = self._local_timestamp(timestamp)
        current_time = local_timestamp.time()
        current_date = local_timestamp.date().isoformat()
        if current_time < self.force_flat or current_date in self.force_flat_dates:
            return None
        self.force_flat_dates.add(current_date)
        signal = self._build_signal(
            signal_type="exit",
            timestamp=timestamp,
            side=self.position.side,
            broker_side="sell" if self.position.side == "long" else "buy",
            price=price,
            reason="session_force_flat",
        )
        intent = self._build_order_intent(signal, close_position=True)
        self.position = None
        self.entry_z = None
        self.entry_efficiency = None
        return signal, intent

    def _target_qty(self, price: float) -> float:
        if self.config.qty is not None:
            return float(self.config.qty)
        if self.config.notional is None:
            raise RuntimeError("Either qty or notional is required.")
        if price <= 0:
            return 1.0
        return max(0.000001, round(float(self.config.notional) / price, 6))

    def _build_signal(
        self,
        signal_type: str,
        timestamp: pd.Timestamp,
        side: str,
        broker_side: str,
        price: float,
        reason: str,
    ) -> Signal:
        signal_prefix = self.config.trader_id or self.config.strategy_name
        signal_id = f"{signal_prefix}-{self.config.symbol.lower()}-{uuid.uuid4().hex[:12]}"
        return Signal(
            signal_id=signal_id,
            signal_type=signal_type,  # type: ignore[arg-type]
            timestamp=timestamp,
            symbol=self.config.symbol.upper(),
            side=side,  # type: ignore[arg-type]
            broker_side=broker_side,  # type: ignore[arg-type]
            price=price,
            reason=reason,
            strategy=self.config.strategy_name,
            trader_id=self.config.trader_id,
        )

    def _build_order_intent(
        self,
        signal: Signal,
        close_position: bool,
        qty: float | None = None,
    ) -> OrderIntent:
        return OrderIntent(
            client_order_id=self._client_order_id(signal),
            timestamp=signal.timestamp,
            symbol=signal.symbol,
            side=signal.broker_side,
            signal_type=signal.signal_type,
            order_type=self.config.order_type,
            strategy=signal.strategy,
            reason=signal.reason,
            qty=None if close_position else qty if qty is not None else self.config.qty,
            notional=None if qty is not None or close_position else self.config.notional,
            limit_price=signal.price if self.config.order_type == "limit" else None,
            exit_management="strategy",
            close_position=close_position,
            trader_id=self.config.trader_id,
            signal_id=signal.signal_id,
            account_namespace=self.config.account_namespace,
        )

    def _client_order_id(self, signal: Signal) -> str:
        suffix = signal.signal_id.rsplit("-", 1)[-1]
        raw_prefix = self.config.trader_id or self.config.strategy_name or signal.symbol.lower()
        prefix = "".join(char if char.isalnum() or char in {"-", "_"} else "-" for char in raw_prefix.lower())
        max_prefix_length = MAX_CLIENT_ORDER_ID_LENGTH - len(suffix) - 1
        return f"{prefix[:max_prefix_length]}-{suffix}"[:MAX_CLIENT_ORDER_ID_LENGTH]
