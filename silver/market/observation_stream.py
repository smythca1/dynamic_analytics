from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import pandas as pd

from trading_events import MarketTick

from .contract import CANONICAL_RENKO_SOURCE
from .models import CanonicalObservation, RawSipTrade
from .one_second_sampler import CanonicalOneSecondSipSampler, DEFAULT_SEEN_SOURCE_ID_CAPACITY, SamplerProcessResult


@dataclass
class CanonicalMarketObservationAdapter:
    run_id: str
    source_id_capacity: int = DEFAULT_SEEN_SOURCE_ID_CAPACITY
    producer_version: str | None = None
    sampler: CanonicalOneSecondSipSampler = field(init=False)

    def __post_init__(self) -> None:
        kwargs: dict[str, Any] = {
            "run_id": self.run_id,
            "max_seen_source_ids": self.source_id_capacity,
        }
        if self.producer_version is not None:
            kwargs["producer_version"] = self.producer_version
        self.sampler = CanonicalOneSecondSipSampler(**kwargs)

    def process_tick(
        self,
        tick: MarketTick,
        *,
        trader_id: str,
        source_event: Any | None = None,
        account_id: str | None = None,
    ) -> SamplerProcessResult:
        return self.sampler.process_trade(
            raw_sip_trade_from_market_tick(
                tick,
                trader_id=trader_id,
                source_event=source_event,
                account_id=account_id,
            )
        )

    def observation_ticks(self, result: SamplerProcessResult) -> tuple[MarketTick, ...]:
        return tuple(observation_to_market_tick(observation) for observation in result.observations)

    def snapshot(self, *, trader_id: str, symbol: str) -> dict[str, Any]:
        payload = self.sampler.snapshot(scope_key=(trader_id, symbol.upper()))
        payload["source_identifier"] = CANONICAL_RENKO_SOURCE
        return payload

    def restore_snapshot(self, payload: dict[str, Any] | None) -> None:
        if payload:
            self.sampler.restore_snapshot(payload)


def raw_sip_trade_from_market_tick(
    tick: MarketTick,
    *,
    trader_id: str,
    source_event: Any | None = None,
    account_id: str | None = None,
) -> RawSipTrade:
    source_sequence = getattr(source_event, "source_sequence", None)
    source_record_id = getattr(source_event, "source_record_id", None)
    archive_path = getattr(source_event, "archive_path", None)
    broker_trade_id = getattr(source_event, "broker_trade_id", None) or tick.trade_id
    source_dataset_reference = archive_path or "live_alpaca_sip_stream"
    source_dataset_hash = archive_path or "live_alpaca_sip_stream"
    row_order = int(source_sequence or 0)
    return RawSipTrade(
        sip_timestamp=tick.timestamp,
        receive_timestamp=tick.timestamp,
        symbol=tick.symbol,
        price=Decimal(str(tick.price)),
        quantity=Decimal(str(tick.size or 1)),
        exchange=tick.exchange,
        tape=tick.tape,
        trade_id=tick.trade_id,
        broker_trade_id=broker_trade_id,
        source_record_id=source_record_id,
        source_dataset_reference=source_dataset_reference,
        source_dataset_hash=source_dataset_hash,
        source_row_order=row_order,
        conditions=tick.conditions,
        source="alpaca_sip",
        source_sequence=row_order,
        row_order=row_order,
        raw_payload_ref=f"{source_dataset_reference}:{row_order}",
        trader_id=trader_id,
        account_id=account_id,
    )


def observation_to_market_tick(observation: CanonicalObservation) -> MarketTick:
    return MarketTick(
        timestamp=pd.Timestamp(observation.second_bucket),
        symbol=observation.symbol,
        price=float(observation.close_price),
        size=1.0,
        exchange=observation.source_exchange,
        trade_id=observation.source_alpaca_trade_id,
        tape=observation.source_tape,
        conditions=(),
    )
