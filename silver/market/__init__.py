"""Canonical market-data models and one-second SIP sampler."""

from .models import (
    CanonicalObservation,
    EligibleTrade,
    RawSipTrade,
    SamplerDiagnostic,
    SamplerState,
    TradeEligibilityDecision,
)
from .live_event_stream import (
    LiveMarketEvent,
    LocalArchiveMarketDataTail,
    live_market_event_from_alpaca_message,
    live_market_event_from_archive_payload,
)
from .one_second_sampler import CanonicalOneSecondSipSampler, SamplerProcessResult
from .trade_filter import TRADE_FILTER_VERSION, evaluate_market_tick_eligibility, evaluate_trade_eligibility
from .contract import (
    CANONICAL_RENKO_SOURCE,
    ContractFingerprint,
    contract_snapshot,
    market_stream_contract,
    promotion_contract,
    renko_strategy_contract,
    validate_canonical_renko_source,
)
from .observation_stream import (
    CanonicalMarketObservationAdapter,
    observation_to_market_tick,
    raw_sip_trade_from_market_tick,
)
from .ipc_consumer import CanonicalStreamEvent, SharedCanonicalStreamConsumer
from .ipc_protocol import PROTOCOL_VERSION as MARKET_IPC_PROTOCOL_VERSION, canonical_event_id
from .shared_service import (
    SharedMarketDataConfig,
    SharedCanonicalMarketDataServer,
    validate_shared_market_data_config,
    validate_symbol_union,
)

__all__ = [
    "CanonicalObservation",
    "CanonicalOneSecondSipSampler",
    "CanonicalMarketObservationAdapter",
    "CanonicalStreamEvent",
    "CANONICAL_RENKO_SOURCE",
    "ContractFingerprint",
    "EligibleTrade",
    "LiveMarketEvent",
    "LocalArchiveMarketDataTail",
    "MARKET_IPC_PROTOCOL_VERSION",
    "RawSipTrade",
    "SamplerDiagnostic",
    "SamplerProcessResult",
    "SamplerState",
    "TRADE_FILTER_VERSION",
    "TradeEligibilityDecision",
    "contract_snapshot",
    "canonical_event_id",
    "evaluate_market_tick_eligibility",
    "evaluate_trade_eligibility",
    "live_market_event_from_alpaca_message",
    "live_market_event_from_archive_payload",
    "market_stream_contract",
    "observation_to_market_tick",
    "promotion_contract",
    "raw_sip_trade_from_market_tick",
    "renko_strategy_contract",
    "SharedMarketDataConfig",
    "SharedCanonicalMarketDataServer",
    "SharedCanonicalStreamConsumer",
    "validate_shared_market_data_config",
    "validate_symbol_union",
    "validate_canonical_renko_source",
]
