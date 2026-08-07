from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Literal

import pandas as pd


Direction = Literal["long", "short"]
BrokerSide = Literal["buy", "sell"]
SignalType = Literal["entry", "exit"]
ExecutionMode = Literal["shadow", "real"]

PRICE_FORMING_TRADE_CONDITIONS = {frozenset({"@"}), frozenset({"@", "F"})}


def utc_timestamp(value: Any) -> pd.Timestamp:
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        raise ValueError(f"Could not parse timestamp: {value}")
    return pd.Timestamp(timestamp)


@dataclass(frozen=True)
class MarketBar:
    timestamp: pd.Timestamp
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0

    @classmethod
    def from_mapping(cls, row: dict[str, Any], symbol: str) -> "MarketBar":
        return cls(
            timestamp=utc_timestamp(row["timestamp"]),
            symbol=symbol.upper(),
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=float(row.get("volume", 0.0) or 0.0),
        )


@dataclass(frozen=True)
class MarketTick:
    timestamp: pd.Timestamp
    symbol: str
    price: float
    size: float
    exchange: str = ""
    trade_id: str = ""
    tape: str = ""
    conditions: tuple[str, ...] = ()

    @classmethod
    def from_alpaca_trade(cls, message: dict[str, Any]) -> "MarketTick":
        return cls(
            timestamp=utc_timestamp(message["t"]),
            symbol=str(message["S"]).upper(),
            price=float(message["p"]),
            size=float(message["s"]),
            exchange=str(message.get("x", "")),
            trade_id=str(message.get("i", "")),
            tape=str(message.get("z", "")),
            conditions=tuple(str(condition) for condition in message.get("c", []) if condition is not None),
        )


def is_price_forming_trade(tick: MarketTick) -> bool:
    if not tick.conditions:
        return True
    return frozenset(tick.conditions) in PRICE_FORMING_TRADE_CONDITIONS


@dataclass(frozen=True)
class RenkoBrick:
    timestamp: pd.Timestamp
    symbol: str
    open: float
    close: float
    direction: int
    run_len: int
    brick_size: float
    trader_id: str | None = None


@dataclass(frozen=True)
class PositionState:
    symbol: str
    side: Direction
    entry_time: pd.Timestamp
    entry_price: float
    qty: float | None = None


@dataclass(frozen=True)
class Signal:
    signal_id: str
    signal_type: SignalType
    timestamp: pd.Timestamp
    symbol: str
    side: Direction
    broker_side: BrokerSide
    price: float
    reason: str
    strategy: str
    trader_id: str | None = None


@dataclass(frozen=True)
class OrderIntent:
    client_order_id: str
    timestamp: pd.Timestamp
    symbol: str
    side: BrokerSide
    signal_type: SignalType
    order_type: str
    strategy: str
    reason: str
    qty: float | None = None
    target_qty: float | None = None
    notional: float | None = None
    limit_price: float | None = None
    stop_price: float | None = None
    stop_limit_price: float | None = None
    exit_management: str | None = None
    stop_loss_order_type: str | None = None
    take_profit_limit_price: float | None = None
    stop_loss_stop_price: float | None = None
    stop_loss_limit_price: float | None = None
    take_profit_offset: float | None = None
    stop_loss_offset: float | None = None
    close_position: bool = False
    trader_id: str | None = None
    signal_id: str | None = None
    lifecycle_state: str | None = None
    account_namespace: str | None = None


@dataclass(frozen=True)
class ExecutionEvent:
    timestamp: pd.Timestamp
    mode: ExecutionMode
    broker: str
    status: str
    intent: OrderIntent
    response: dict[str, Any] | None = None


def dataclass_to_jsonable(value: Any) -> dict[str, Any]:
    payload = asdict(value)

    def normalize(item: Any) -> Any:
        if isinstance(item, pd.Timestamp):
            return item.isoformat()
        if isinstance(item, dict):
            return {key: normalize(val) for key, val in item.items()}
        if isinstance(item, list):
            return [normalize(val) for val in item]
        if isinstance(item, tuple):
            return [normalize(val) for val in item]
        return item

    return normalize(payload)
