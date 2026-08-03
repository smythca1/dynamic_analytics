from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from hashlib import sha256
from typing import Any

from silver.events.models import utc_datetime


ZERO = Decimal("0")


def to_decimal(value: Decimal | int | float | str) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def second_bucket(value: datetime | str) -> datetime:
    ts = utc_datetime(value)
    return ts.replace(microsecond=0)


@dataclass(frozen=True)
class RawSipTrade:
    sip_timestamp: datetime | str
    symbol: str
    price: Decimal | int | float | str
    quantity: Decimal | int | float | str
    exchange: str = ""
    tape: str = ""
    trade_id: str = ""
    broker_trade_id: str | None = None
    source_record_id: str | None = None
    source_dataset_reference: str | None = None
    source_dataset_hash: str | None = None
    source_row_order: int | None = None
    economic_event_fingerprint: str | None = None
    conditions: tuple[str, ...] = ()
    source: str = "alpaca_sip"
    source_sequence: int = 0
    row_order: int = 0
    receive_timestamp: datetime | str | None = None
    raw_payload_ref: str | None = None
    trader_id: str | None = None
    account_id: str | None = None
    source_stream_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "sip_timestamp", utc_datetime(self.sip_timestamp))
        if self.receive_timestamp is not None:
            object.__setattr__(self, "receive_timestamp", utc_datetime(self.receive_timestamp))
        object.__setattr__(self, "symbol", self.symbol.upper())
        broker_trade_id = self.broker_trade_id if self.broker_trade_id is not None else self.trade_id
        object.__setattr__(self, "broker_trade_id", str(broker_trade_id or ""))
        object.__setattr__(self, "trade_id", str(self.trade_id or broker_trade_id or ""))
        object.__setattr__(self, "price", to_decimal(self.price))
        object.__setattr__(self, "quantity", to_decimal(self.quantity))
        object.__setattr__(self, "conditions", tuple(str(item) for item in self.conditions if item is not None))
        if self.source_row_order is None:
            object.__setattr__(self, "source_row_order", self.row_order)
        if self.economic_event_fingerprint is None:
            object.__setattr__(self, "economic_event_fingerprint", self._build_economic_event_fingerprint())
        if self.source_record_id is None:
            object.__setattr__(self, "source_record_id", self._build_source_record_id())
        if self.price <= ZERO:
            raise ValueError("Raw SIP trade price must be positive")
        if self.quantity <= ZERO:
            raise ValueError("Raw SIP trade quantity must be positive")

    @property
    def second_bucket(self) -> datetime:
        return second_bucket(self.sip_timestamp)

    @property
    def scope_key(self) -> tuple[str, str]:
        stream = self.trader_id or self.source_stream_id or self.source
        return (stream, self.symbol)

    @property
    def source_identity(self) -> str:
        return str(self.source_record_id)

    @property
    def ordering_key(self) -> tuple[datetime, int, int]:
        return (self.sip_timestamp, self.source_sequence, self.row_order)

    def _build_source_record_id(self) -> str:
        parts = [
            self.source_dataset_hash or "",
            self.source_dataset_reference or "",
            str(self.source_row_order if self.source_row_order is not None else self.row_order),
            self.symbol,
            self.sip_timestamp.isoformat(),
            self.broker_trade_id or "",
        ]
        return "source_record:" + sha256("|".join(parts).encode("utf-8")).hexdigest()

    def _build_economic_event_fingerprint(self) -> str:
        parts = [
            self.symbol,
            self.sip_timestamp.isoformat(),
            str(self.price),
            str(self.quantity),
            self.exchange,
            self.tape,
            ",".join(self.conditions),
            self.broker_trade_id or "",
        ]
        return "economic:" + sha256("|".join(parts).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class TradeEligibilityDecision:
    raw_trade: RawSipTrade
    is_eligible: bool
    reason: str
    event_id: str | None = None


@dataclass(frozen=True)
class EligibleTrade:
    raw_trade: RawSipTrade
    eligible_trade_id: str
    eligibility_event_id: str | None = None
    root_event_id: str | None = None

    @property
    def second_bucket(self) -> datetime:
        return self.raw_trade.second_bucket


@dataclass(frozen=True)
class CanonicalObservation:
    observation_id: str
    run_id: str
    symbol: str
    second_bucket: datetime
    emitted_sequence: int
    close_price: Decimal
    source_eligible_trade_id: str
    source_sip_timestamp: datetime
    source_alpaca_trade_id: str
    source_record_id: str | None = None
    source_broker_trade_id: str | None = None
    source_dataset_reference: str | None = None
    source_dataset_hash: str | None = None
    source_row_order: int | None = None
    source_economic_event_fingerprint: str | None = None
    source_exchange: str = ""
    source_tape: str = ""
    triggering_later_eligible_trade_id: str | None = None
    triggering_sip_timestamp: datetime | None = None
    sampler_version: str = ""
    parent_event_id: str | None = None
    root_event_id: str | None = None
    source_eligible_trade_event_id: str | None = None
    triggering_eligible_trade_event_id: str | None = None
    replay_finalized: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", self.symbol.upper())
        object.__setattr__(self, "second_bucket", utc_datetime(self.second_bucket))
        object.__setattr__(self, "source_sip_timestamp", utc_datetime(self.source_sip_timestamp))
        if self.triggering_sip_timestamp is not None:
            object.__setattr__(self, "triggering_sip_timestamp", utc_datetime(self.triggering_sip_timestamp))
        object.__setattr__(self, "close_price", to_decimal(self.close_price))
        if self.source_eligible_trade_event_id is None:
            object.__setattr__(self, "source_eligible_trade_event_id", self.source_eligible_trade_id)
        if self.triggering_eligible_trade_event_id is None:
            object.__setattr__(self, "triggering_eligible_trade_event_id", self.triggering_later_eligible_trade_id)


@dataclass(frozen=True)
class SamplerDiagnostic:
    diagnostic_type: str
    message: str
    raw_trade: RawSipTrade | None = None
    observation: CanonicalObservation | None = None
    event_id: str | None = None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SamplerState:
    scope_key: tuple[str, str]
    pending_bucket: datetime | None = None
    pending_trade: EligibleTrade | None = None
    last_ordering_key: tuple[datetime, int, int] | None = None
    emitted_sequence: int = 0
    seen_source_id_capacity: int = 0
    seen_source_id_count: int = 0
    seen_source_id_evictions: int = 0
