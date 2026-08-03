from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
from typing import Literal


ZERO = Decimal("0")

FillSide = Literal["buy", "sell", "sell_short"]
FillLeg = Literal["open", "close"]
PositionSide = Literal["long", "short"]


def to_decimal(value: Decimal | int | float | str) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def normalize_timestamp(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        ts = value
    else:
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def normalize_side(value: str) -> FillSide:
    side = value.strip().lower()
    if side in {"buy", "b"}:
        return "buy"
    if side in {"sell", "s"}:
        return "sell"
    if side in {"sell_short", "sell short", "short"}:
        return "sell_short"
    raise ValueError(f"Unsupported fill side: {value!r}")


def normalize_leg(value: str | None) -> FillLeg | None:
    if value is None or value == "":
        return None
    leg = value.strip().lower()
    if leg in {"open", "close"}:
        return leg  # type: ignore[return-value]
    raise ValueError(f"Unsupported fill leg: {value!r}")


@dataclass(frozen=True)
class NormalizedFill:
    timestamp: datetime | str
    symbol: str
    trader_id: str
    side: str
    qty: Decimal | int | float | str
    price: Decimal | int | float | str
    account_id: str | None = None
    order_id: str | None = None
    activity_id: str | None = None
    fill_id: str | None = None
    leg: str | None = None
    reason: str | None = None
    source: str = "unknown"
    source_sequence: int = 0
    row_order: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "timestamp", normalize_timestamp(self.timestamp))
        object.__setattr__(self, "symbol", self.symbol.upper())
        object.__setattr__(self, "side", normalize_side(self.side))
        object.__setattr__(self, "qty", to_decimal(self.qty))
        object.__setattr__(self, "price", to_decimal(self.price))
        object.__setattr__(self, "leg", normalize_leg(self.leg))
        if self.qty <= ZERO:
            raise ValueError("Fill quantity must be positive")
        if self.price <= ZERO:
            raise ValueError("Fill price must be positive")

    @property
    def idempotency_key(self) -> str:
        """Replay-stable fill identity: activity_id, fill_id, then deterministic digest."""
        if self.activity_id:
            return f"activity:{self.activity_id}"
        if self.fill_id:
            return f"fill:{self.fill_id}"
        parts = [
            self.account_id or "",
            self.trader_id,
            self.symbol,
            self.order_id or "",
            self.leg or "",
            self.side,
            str(self.qty),
            str(self.price),
            self.timestamp.isoformat(),
            self.source,
            str(self.source_sequence),
            str(self.row_order),
        ]
        digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
        return f"composite:{digest}"

    @property
    def stable_fill_id(self) -> str:
        return self.idempotency_key

    @property
    def sort_key(self) -> tuple[datetime, int, int]:
        return (self.timestamp, self.source_sequence, self.row_order)


@dataclass(frozen=True)
class PositionState:
    symbol: str
    side: PositionSide | None = None
    qty: Decimal = ZERO
    avg_entry_price: Decimal | None = None
    basis_known: bool = True
    entry_timestamp: datetime | None = None
    entry_fill_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", self.symbol.upper())
        object.__setattr__(self, "qty", to_decimal(self.qty))
        if self.avg_entry_price is not None:
            object.__setattr__(self, "avg_entry_price", to_decimal(self.avg_entry_price))
        if self.entry_timestamp is not None:
            object.__setattr__(self, "entry_timestamp", normalize_timestamp(self.entry_timestamp))
        if self.qty == ZERO:
            object.__setattr__(self, "side", None)
            object.__setattr__(self, "avg_entry_price", None)
            object.__setattr__(self, "basis_known", True)
        elif self.side not in {"long", "short"}:
            raise ValueError("Non-flat position must have side 'long' or 'short'")
        elif self.basis_known and self.avg_entry_price is None:
            raise ValueError("Known-basis position requires avg_entry_price")

    @classmethod
    def flat(cls, symbol: str) -> "PositionState":
        return cls(symbol=symbol, side=None, qty=ZERO, avg_entry_price=None, basis_known=True)


@dataclass(frozen=True)
class PositionTransition:
    timestamp: datetime
    symbol: str
    trader_id: str
    fill_id: str
    fill: NormalizedFill
    before: PositionState
    after: PositionState
    action: str
    realized_pnl: Decimal = ZERO
    realized_basis_known: bool = True


@dataclass(frozen=True)
class ClosedTrade:
    symbol: str
    trader_id: str
    side: PositionSide
    qty: Decimal
    entry_price: Decimal | None
    exit_price: Decimal
    entry_timestamp: datetime | None
    exit_timestamp: datetime
    entry_fill_id: str | None
    exit_fill_id: str
    pnl: Decimal | None
    pnl_pct: Decimal | None
    basis_known: bool
    reason: str | None = None


@dataclass(frozen=True)
class EquityPoint:
    timestamp: datetime
    equity: Decimal
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    net_pnl: Decimal
    fill_id: str


@dataclass(frozen=True)
class AccountingSnapshot:
    position: PositionState
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    net_pnl: Decimal
    equity: Decimal
    position_transitions: tuple[PositionTransition, ...] = field(default_factory=tuple)
    closed_trades: tuple[ClosedTrade, ...] = field(default_factory=tuple)
    equity_curve: tuple[EquityPoint, ...] = field(default_factory=tuple)
    gross_profit: Decimal = ZERO
    gross_loss: Decimal = ZERO
    profit_factor: Decimal | None = None
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: Decimal | None = None
    average_trade_pnl_dollars: Decimal | None = None
    average_trade_pnl_percent: Decimal | None = None
    average_winner_dollars: Decimal | None = None
    average_winner_percent: Decimal | None = None
    average_loser_dollars: Decimal | None = None
    average_loser_percent: Decimal | None = None
    largest_winner: Decimal | None = None
    largest_loser: Decimal | None = None
    max_drawdown_dollars: Decimal = ZERO
    max_drawdown_percent: Decimal = ZERO
    cost_basis_method: str = "weighted_average"
