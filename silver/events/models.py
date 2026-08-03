from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, Literal


EventType = Literal[
    "RawSipTradeReceived",
    "TradeEligibilityEvaluated",
    "EligibleTradeAccepted",
    "TradeRejected",
    "CanonicalObservationCreated",
    "SamplerDiagnosticCreated",
]


def utc_datetime(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        ts = value
    else:
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def deterministic_event_id(*, run_id: str, event_type: str, stable_source_identity: str, sequence: int) -> str:
    raw = f"{run_id}|{event_type}|{stable_source_identity}|{sequence}"
    return sha256(raw.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class CanonicalEvent:
    event_id: str
    event_type: EventType
    event_time: datetime
    run_id: str
    source: str
    symbol: str
    sequence: int
    payload: dict[str, Any] = field(default_factory=dict)
    processing_time: datetime | None = None
    trader_id: str | None = None
    account_id: str | None = None
    parent_event_id: str | None = None
    root_event_id: str | None = None
    schema_version: str = "1"
    producer_version: str = "silver.events.v1"

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_time", utc_datetime(self.event_time))
        if self.processing_time is not None:
            object.__setattr__(self, "processing_time", utc_datetime(self.processing_time))
        object.__setattr__(self, "symbol", self.symbol.upper())
        if self.root_event_id is None:
            object.__setattr__(self, "root_event_id", self.event_id)

    @classmethod
    def build(
        cls,
        *,
        event_type: EventType,
        event_time: datetime | str,
        run_id: str,
        source: str,
        symbol: str,
        sequence: int,
        stable_source_identity: str,
        payload: dict[str, Any] | None = None,
        processing_time: datetime | str | None = None,
        trader_id: str | None = None,
        account_id: str | None = None,
        parent_event_id: str | None = None,
        root_event_id: str | None = None,
        schema_version: str = "1",
        producer_version: str = "silver.events.v1",
    ) -> "CanonicalEvent":
        event_id = deterministic_event_id(
            run_id=run_id,
            event_type=event_type,
            stable_source_identity=stable_source_identity,
            sequence=sequence,
        )
        return cls(
            event_id=event_id,
            event_type=event_type,
            event_time=event_time,
            processing_time=processing_time,
            run_id=run_id,
            source=source,
            symbol=symbol,
            trader_id=trader_id,
            account_id=account_id,
            sequence=sequence,
            parent_event_id=parent_event_id,
            root_event_id=root_event_id or event_id,
            schema_version=schema_version,
            producer_version=producer_version,
            payload=payload or {},
        )
