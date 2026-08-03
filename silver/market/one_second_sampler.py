from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, replace
from hashlib import sha256

from silver.events.models import CanonicalEvent

from .models import (
    CanonicalObservation,
    EligibleTrade,
    RawSipTrade,
    SamplerDiagnostic,
    SamplerState,
    TradeEligibilityDecision,
)
from .trade_filter import evaluate_trade_eligibility


SAMPLER_VERSION = "silver.one_second_sip_sampler.v1"
DEFAULT_SEEN_SOURCE_ID_CAPACITY = 100_000


@dataclass(frozen=True)
class SamplerProcessResult:
    events: tuple[CanonicalEvent, ...]
    eligibility: TradeEligibilityDecision | None
    observations: tuple[CanonicalObservation, ...]
    diagnostics: tuple[SamplerDiagnostic, ...]


class CanonicalOneSecondSipSampler:
    def __init__(
        self,
        *,
        run_id: str,
        producer_version: str = SAMPLER_VERSION,
        max_seen_source_ids: int = DEFAULT_SEEN_SOURCE_ID_CAPACITY,
    ) -> None:
        if max_seen_source_ids < 1:
            raise ValueError("max_seen_source_ids must be >= 1.")
        self.run_id = run_id
        self.producer_version = producer_version
        self.max_seen_source_ids = max_seen_source_ids
        self._states: dict[tuple[str, str], SamplerState] = {}
        self._seen_source_ids: dict[tuple[str, str], OrderedDict[str, None]] = {}
        self._event_sequence = 0

    def process_trade(self, raw_trade: RawSipTrade) -> SamplerProcessResult:
        events: list[CanonicalEvent] = []
        diagnostics: list[SamplerDiagnostic] = []
        raw_event = self._event("RawSipTradeReceived", raw_trade, raw_trade.source_identity, payload={"trade_id": raw_trade.trade_id})
        events.append(raw_event)

        state = self._state_for(raw_trade)
        seen = self._seen_for(raw_trade.scope_key)
        if raw_trade.source_identity in seen:
            diagnostic, diagnostic_event = self._diagnostic(
                "duplicate_source_trade",
                "Duplicate source trade ignored.",
                raw_trade,
                parent_event_id=raw_event.event_id,
                root_event_id=raw_event.root_event_id,
            )
            diagnostics.append(diagnostic)
            events.append(diagnostic_event)
            return SamplerProcessResult(tuple(events), None, tuple(), tuple(diagnostics))

        state, evicted_source_id = self._remember_source_identity(state, raw_trade.source_identity)
        self._states[raw_trade.scope_key] = state
        if evicted_source_id is not None:
            diagnostic, diagnostic_event = self._diagnostic(
                "source_id_cache_evicted",
                "Bounded source ID cache evicted oldest source identity.",
                raw_trade,
                parent_event_id=raw_event.event_id,
                root_event_id=raw_event.root_event_id,
                details={
                    "evicted_source_id": evicted_source_id,
                    "seen_source_id_capacity": state.seen_source_id_capacity,
                    "seen_source_id_count": state.seen_source_id_count,
                    "seen_source_id_evictions": state.seen_source_id_evictions,
                },
            )
            diagnostics.append(diagnostic)
            events.append(diagnostic_event)

        decision = evaluate_trade_eligibility(raw_trade)
        eligibility_event = self._event(
            "TradeEligibilityEvaluated",
            raw_trade,
            raw_trade.source_identity + ":eligibility",
            parent_event_id=raw_event.event_id,
            root_event_id=raw_event.root_event_id,
            payload={"eligible": decision.is_eligible, "reason": decision.reason},
        )
        decision = replace(decision, event_id=eligibility_event.event_id)
        events.append(eligibility_event)

        if not decision.is_eligible:
            rejected_event = self._event(
                "TradeRejected",
                raw_trade,
                raw_trade.source_identity + ":rejected",
                parent_event_id=eligibility_event.event_id,
                root_event_id=raw_event.root_event_id,
                payload={"reason": decision.reason},
            )
            diagnostic, diagnostic_event = self._diagnostic(
                "rejected_non_price_forming_trade",
                "Rejected non-price-forming trade.",
                raw_trade,
                parent_event_id=rejected_event.event_id,
                root_event_id=raw_event.root_event_id,
            )
            diagnostics.append(diagnostic)
            events.extend([rejected_event, diagnostic_event])
            return SamplerProcessResult(tuple(events), decision, tuple(), tuple(diagnostics))

        accepted_event = self._event(
            "EligibleTradeAccepted",
            raw_trade,
            raw_trade.source_identity + ":accepted",
            parent_event_id=eligibility_event.event_id,
            root_event_id=raw_event.root_event_id,
            payload={"second_bucket": raw_trade.second_bucket.isoformat()},
        )
        events.append(accepted_event)
        eligible = EligibleTrade(
            raw_trade=raw_trade,
            eligible_trade_id=accepted_event.event_id,
            eligibility_event_id=eligibility_event.event_id,
            root_event_id=raw_event.root_event_id,
        )

        state = self._state_for(raw_trade)
        observations: list[CanonicalObservation] = []
        if state.pending_trade is None:
            state = replace(state, pending_bucket=eligible.second_bucket, pending_trade=eligible, last_ordering_key=raw_trade.ordering_key)
            self._states[raw_trade.scope_key] = state
            return SamplerProcessResult(tuple(events), decision, tuple(), tuple(diagnostics))

        if eligible.second_bucket == state.pending_bucket:
            pending_key = state.pending_trade.raw_trade.ordering_key
            if raw_trade.ordering_key < pending_key:
                diagnostic, diagnostic_event = self._diagnostic(
                    "older_same_bucket_trade",
                    "Logically older same-bucket trade preserved existing pending close.",
                    raw_trade,
                    parent_event_id=accepted_event.event_id,
                    root_event_id=raw_event.root_event_id,
                )
                diagnostics.append(diagnostic)
                events.append(diagnostic_event)
                return SamplerProcessResult(tuple(events), decision, tuple(), tuple(diagnostics))
            if raw_trade.ordering_key == pending_key:
                diagnostic, diagnostic_event = self._diagnostic(
                    "ambiguous_equal_ordering_trade",
                    "Equal timestamp and source sequence have no deterministic ordering tie-breaker; existing pending close preserved.",
                    raw_trade,
                    parent_event_id=accepted_event.event_id,
                    root_event_id=raw_event.root_event_id,
                )
                diagnostics.append(diagnostic)
                events.append(diagnostic_event)
                return SamplerProcessResult(tuple(events), decision, tuple(), tuple(diagnostics))
            if raw_trade.sip_timestamp == pending_key[0]:
                diagnostic, diagnostic_event = self._diagnostic(
                    "equal_timestamp_stable_ordering",
                    "Equal timestamp retained in source-sequence and row-order order.",
                    raw_trade,
                    parent_event_id=accepted_event.event_id,
                    root_event_id=raw_event.root_event_id,
                )
                diagnostics.append(diagnostic)
                events.append(diagnostic_event)
            state = replace(state, pending_trade=eligible, last_ordering_key=max(state.last_ordering_key or raw_trade.ordering_key, raw_trade.ordering_key))
            self._states[raw_trade.scope_key] = state
            return SamplerProcessResult(tuple(events), decision, tuple(), tuple(diagnostics))

        if eligible.second_bucket < state.pending_bucket:
            diagnostic, diagnostic_event = self._diagnostic(
                "older_prior_bucket_trade",
                "Older prior-bucket trade preserved existing pending close.",
                raw_trade,
                parent_event_id=accepted_event.event_id,
                root_event_id=raw_event.root_event_id,
            )
            diagnostics.append(diagnostic)
            events.append(diagnostic_event)
            self._states[raw_trade.scope_key] = state
            return SamplerProcessResult(tuple(events), decision, tuple(), tuple(diagnostics))

        observation = self._build_observation(state, trigger=eligible, replay_finalized=False)
        observation_event = self._observation_event(
            observation,
            raw_trade,
            parent_event_id=observation.parent_event_id,
            root_event_id=observation.root_event_id,
        )
        observations.append(observation)
        events.append(observation_event)
        state = replace(
            state,
            pending_bucket=eligible.second_bucket,
            pending_trade=eligible,
            last_ordering_key=max(state.last_ordering_key or raw_trade.ordering_key, raw_trade.ordering_key),
            emitted_sequence=state.emitted_sequence + 1,
        )
        self._states[raw_trade.scope_key] = state
        return SamplerProcessResult(tuple(events), decision, tuple(observations), tuple(diagnostics))

    def flush_pending(self, *, scope_key: tuple[str, str]) -> SamplerProcessResult:
        state = self._states.get(scope_key)
        if state is None or state.pending_trade is None:
            return SamplerProcessResult(tuple(), None, tuple(), tuple())
        raw_trade = state.pending_trade.raw_trade
        observation = self._build_observation(state, trigger=None, replay_finalized=True)
        diagnostic, diagnostic_event = self._diagnostic(
            "explicit_replay_finalization_flush",
            "Explicit replay finalization emitted pending occupied second.",
            raw_trade,
            observation=observation,
            parent_event_id=observation.parent_event_id,
            root_event_id=observation.root_event_id,
        )
        state = replace(
            state,
            pending_bucket=None,
            pending_trade=None,
            emitted_sequence=state.emitted_sequence + 1,
        )
        self._states[scope_key] = state
        return SamplerProcessResult(tuple([diagnostic_event]), None, tuple([observation]), tuple([diagnostic]))

    def pending_diagnostics(self) -> tuple[SamplerDiagnostic, ...]:
        diagnostics: list[SamplerDiagnostic] = []
        for state in self._states.values():
            if state.pending_trade is not None:
                diagnostics.append(
                    SamplerDiagnostic(
                        diagnostic_type="pending_final_occupied_second",
                        message="Pending occupied second is retained and not emitted by default.",
                        raw_trade=state.pending_trade.raw_trade,
                    )
                )
        return tuple(diagnostics)

    def state(self, scope_key: tuple[str, str]) -> SamplerState | None:
        return self._states.get(scope_key)

    def _state_for(self, raw_trade: RawSipTrade) -> SamplerState:
        return self._states.setdefault(
            raw_trade.scope_key,
            SamplerState(
                scope_key=raw_trade.scope_key,
                seen_source_id_capacity=self.max_seen_source_ids,
            ),
        )

    def _seen_for(self, scope_key: tuple[str, str]) -> OrderedDict[str, None]:
        return self._seen_source_ids.setdefault(scope_key, OrderedDict())

    def _remember_source_identity(self, state: SamplerState, source_identity: str) -> tuple[SamplerState, str | None]:
        seen = self._seen_for(state.scope_key)
        evicted_source_id = None
        seen[source_identity] = None
        if len(seen) > self.max_seen_source_ids:
            evicted_source_id, _ = seen.popitem(last=False)
        return (
            replace(
                state,
                seen_source_id_capacity=self.max_seen_source_ids,
                seen_source_id_count=len(seen),
                seen_source_id_evictions=state.seen_source_id_evictions + (1 if evicted_source_id is not None else 0),
            ),
            evicted_source_id,
        )

    def _event(
        self,
        event_type,
        raw_trade: RawSipTrade,
        stable_source_identity: str,
        *,
        parent_event_id: str | None = None,
        root_event_id: str | None = None,
        payload: dict | None = None,
    ) -> CanonicalEvent:
        self._event_sequence += 1
        return CanonicalEvent.build(
            event_type=event_type,
            event_time=raw_trade.sip_timestamp,
            run_id=self.run_id,
            source=raw_trade.source,
            symbol=raw_trade.symbol,
            trader_id=raw_trade.trader_id,
            account_id=raw_trade.account_id,
            sequence=self._event_sequence,
            stable_source_identity=stable_source_identity,
            parent_event_id=parent_event_id,
            root_event_id=root_event_id,
            producer_version=self.producer_version,
            payload=payload,
        )

    def _diagnostic(
        self,
        diagnostic_type: str,
        message: str,
        raw_trade: RawSipTrade,
        *,
        observation: CanonicalObservation | None = None,
        parent_event_id: str | None,
        root_event_id: str | None,
        details: dict | None = None,
    ) -> tuple[SamplerDiagnostic, CanonicalEvent]:
        event = self._event(
            "SamplerDiagnosticCreated",
            raw_trade,
            raw_trade.source_identity + ":" + diagnostic_type,
            parent_event_id=parent_event_id,
            root_event_id=root_event_id,
            payload={"diagnostic_type": diagnostic_type, "message": message, "details": details or {}},
        )
        return (
            SamplerDiagnostic(
                diagnostic_type=diagnostic_type,
                message=message,
                raw_trade=raw_trade,
                observation=observation,
                event_id=event.event_id,
                details=details or {},
            ),
            event,
        )

    def _build_observation(self, state: SamplerState, *, trigger: EligibleTrade | None, replay_finalized: bool) -> CanonicalObservation:
        assert state.pending_trade is not None
        source = state.pending_trade.raw_trade
        emitted_sequence = state.emitted_sequence + 1
        stable = "|".join(
            [
                self.run_id,
                source.symbol,
                source.second_bucket.isoformat(),
                state.pending_trade.eligible_trade_id,
                str(emitted_sequence),
                "final" if replay_finalized else "live",
            ]
        )
        observation_id = sha256(stable.encode("utf-8")).hexdigest()
        return CanonicalObservation(
            observation_id=observation_id,
            run_id=self.run_id,
            symbol=source.symbol,
            second_bucket=source.second_bucket,
            emitted_sequence=emitted_sequence,
            close_price=source.price,
            source_eligible_trade_id=state.pending_trade.eligible_trade_id,
            source_sip_timestamp=source.sip_timestamp,
            source_alpaca_trade_id=source.broker_trade_id or source.trade_id,
            source_record_id=source.source_record_id,
            source_broker_trade_id=source.broker_trade_id,
            source_dataset_reference=source.source_dataset_reference,
            source_dataset_hash=source.source_dataset_hash,
            source_row_order=source.source_row_order,
            source_economic_event_fingerprint=source.economic_event_fingerprint,
            source_exchange=source.exchange,
            source_tape=source.tape,
            triggering_later_eligible_trade_id=trigger.eligible_trade_id if trigger else None,
            triggering_sip_timestamp=trigger.raw_trade.sip_timestamp if trigger else None,
            sampler_version=SAMPLER_VERSION,
            parent_event_id=trigger.eligible_trade_id if trigger else state.pending_trade.eligible_trade_id,
            root_event_id=state.pending_trade.root_event_id,
            source_eligible_trade_event_id=state.pending_trade.eligible_trade_id,
            triggering_eligible_trade_event_id=trigger.eligible_trade_id if trigger else None,
            replay_finalized=replay_finalized,
        )

    def _observation_event(
        self,
        observation: CanonicalObservation,
        raw_trade: RawSipTrade,
        *,
        parent_event_id: str | None,
        root_event_id: str | None,
    ) -> CanonicalEvent:
        self._event_sequence += 1
        return CanonicalEvent.build(
            event_type="CanonicalObservationCreated",
            event_time=observation.second_bucket,
            run_id=self.run_id,
            source=raw_trade.source,
            symbol=observation.symbol,
            trader_id=raw_trade.trader_id,
            account_id=raw_trade.account_id,
            sequence=self._event_sequence,
            stable_source_identity=observation.observation_id,
            parent_event_id=parent_event_id,
            root_event_id=root_event_id,
            producer_version=self.producer_version,
            payload={
                "observation_id": observation.observation_id,
                "second_bucket": observation.second_bucket.isoformat(),
                "close_price": str(observation.close_price),
                "source_eligible_trade_id": observation.source_eligible_trade_id,
                "source_record_id": observation.source_record_id,
                "source_broker_trade_id": observation.source_broker_trade_id,
                "triggering_later_eligible_trade_id": observation.triggering_later_eligible_trade_id,
                "source_eligible_trade_event_id": observation.source_eligible_trade_event_id,
                "triggering_eligible_trade_event_id": observation.triggering_eligible_trade_event_id,
                "replay_finalized": observation.replay_finalized,
            },
        )
