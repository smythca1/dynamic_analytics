from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from hashlib import sha256
from typing import Any

from silver.market import CanonicalOneSecondSipSampler, RawSipTrade

from .models import UNKNOWN, jsonable


SHADOW_SAMPLER_VERSION = "silver.evidence.shadow_sampler.v1"


@dataclass(frozen=True)
class ShadowSamplerSettings:
    enabled: bool = False
    session_id: str = ""
    process_id: str = "default"
    source_id_capacity: int = 100_000


@dataclass
class ShadowSamplerCounters:
    trades_processed: int = 0
    observations_emitted: int = 0
    diagnostics_emitted: int = 0
    failures: int = 0


@dataclass
class ShadowSamplerResult:
    observations: list[dict[str, Any]] = field(default_factory=list)
    diagnostics: list[dict[str, Any]] = field(default_factory=list)


class EvidenceShadowSampler:
    def __init__(self, settings: ShadowSamplerSettings) -> None:
        self.settings = settings
        self.counters = ShadowSamplerCounters()
        self._sampler = CanonicalOneSecondSipSampler(
            run_id=f"shadow:{settings.session_id}",
            producer_version=SHADOW_SAMPLER_VERSION,
            max_seen_source_ids=settings.source_id_capacity,
        )
        self._source_sequences: dict[tuple[str, str, str], int] = {}

    @property
    def enabled(self) -> bool:
        return self.settings.enabled

    def process_tick(self, *, trader_id: str, tick: Any) -> ShadowSamplerResult:
        if not self.enabled:
            return ShadowSamplerResult()
        source_sequence = self._next_source_sequence(trader_id=trader_id, symbol=tick.symbol)
        raw = self._raw_trade(trader_id=trader_id, tick=tick, source_sequence=source_sequence)
        result = self._sampler.process_trade(raw)
        self.counters.trades_processed += 1
        observations: list[dict[str, Any]] = []
        for observation in result.observations:
            self.counters.observations_emitted += 1
            observations.append(
                {
                    "sequence": observation.emitted_sequence,
                    "observation_bucket_timestamp": observation.second_bucket.isoformat(),
                    "close_price": str(observation.close_price),
                    "source_record_id": observation.source_record_id,
                    "broker_trade_id": observation.source_broker_trade_id,
                    "source_trade_timestamp": observation.source_sip_timestamp.isoformat(),
                    "triggering_source_record_id": raw.source_record_id,
                    "triggering_trade_timestamp": observation.triggering_sip_timestamp.isoformat()
                    if observation.triggering_sip_timestamp
                    else UNKNOWN,
                }
            )
        diagnostics: list[dict[str, Any]] = []
        for diagnostic in result.diagnostics:
            self.counters.diagnostics_emitted += 1
            diagnostics.append(
                {
                    "diagnostic_type": diagnostic.diagnostic_type,
                    "message": diagnostic.message,
                    "source_record_id": diagnostic.raw_trade.source_record_id if diagnostic.raw_trade else None,
                }
            )
        return ShadowSamplerResult(observations=observations, diagnostics=diagnostics)

    def checkpoint(self, *, trader_id: str, symbol: str) -> dict[str, Any]:
        state = self._sampler.state((trader_id, symbol.upper()))
        if state is None or state.pending_trade is None:
            pending = {
                "pending_second_bucket": None,
                "pending_source_record_id": None,
                "pending_price": None,
                "pending_sip_timestamp": None,
            }
            emitted_sequence = 0 if state is None else state.emitted_sequence
        else:
            raw = state.pending_trade.raw_trade
            pending = {
                "pending_second_bucket": state.pending_bucket.isoformat() if state.pending_bucket else None,
                "pending_source_record_id": raw.source_record_id,
                "pending_price": str(raw.price),
                "pending_sip_timestamp": raw.sip_timestamp.isoformat(),
            }
            emitted_sequence = state.emitted_sequence
        return {
            "sampler_version": SHADOW_SAMPLER_VERSION,
            "session_id": self.settings.session_id,
            "trader_id": trader_id,
            "symbol": symbol.upper(),
            "emitted_observation_sequence": emitted_sequence,
            "seen_source_id_capacity": self.settings.source_id_capacity if state is None else state.seen_source_id_capacity,
            "seen_source_id_count": 0 if state is None else state.seen_source_id_count,
            "seen_source_id_evictions": 0 if state is None else state.seen_source_id_evictions,
            "diagnostic_counters": jsonable(self.counters),
            **pending,
        }

    def _next_source_sequence(self, *, trader_id: str, symbol: str) -> int:
        key = (self.settings.session_id, trader_id, symbol.upper())
        current = self._source_sequences.get(key, 0) + 1
        self._source_sequences[key] = current
        return current

    def _raw_trade(self, *, trader_id: str, tick: Any, source_sequence: int) -> RawSipTrade:
        source_record_id = source_record_id_for_tick(
            session_id=self.settings.session_id,
            trader_id=trader_id,
            symbol=tick.symbol,
            source_sequence=source_sequence,
            sip_timestamp=tick.timestamp,
            broker_trade_id=tick.trade_id,
        )
        return RawSipTrade(
            sip_timestamp=tick.timestamp,
            symbol=tick.symbol,
            price=Decimal(str(tick.price)),
            quantity=Decimal(str(tick.size)),
            exchange=str(getattr(tick, "exchange", "")),
            tape=str(getattr(tick, "tape", "")),
            trade_id=str(getattr(tick, "trade_id", "")),
            broker_trade_id=str(getattr(tick, "trade_id", "")),
            source_record_id=source_record_id,
            source_dataset_reference=f"live_evidence:{self.settings.session_id}",
            source_dataset_hash=self.settings.session_id,
            source_row_order=source_sequence,
            conditions=tuple(str(item) for item in getattr(tick, "conditions", ()) if item is not None),
            source="production_shadow_sip",
            source_sequence=source_sequence,
            row_order=source_sequence,
            trader_id=trader_id,
            source_stream_id=self.settings.session_id,
        )


def source_record_id_for_tick(
    *,
    session_id: str,
    trader_id: str,
    symbol: str,
    source_sequence: int,
    sip_timestamp: Any,
    broker_trade_id: str | None,
) -> str:
    parts = [
        session_id,
        trader_id,
        symbol.upper(),
        str(source_sequence),
        sip_timestamp.isoformat() if hasattr(sip_timestamp, "isoformat") else str(sip_timestamp),
        broker_trade_id or "",
    ]
    return "shadow_source_record:" + sha256("|".join(parts).encode("utf-8")).hexdigest()
