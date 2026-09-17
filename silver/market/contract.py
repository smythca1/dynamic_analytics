from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from hashlib import sha256
from typing import Any

from renko_builder import RENKO_EPSILON

from .one_second_sampler import DEFAULT_SEEN_SOURCE_ID_CAPACITY, SAMPLER_VERSION
from .trade_filter import TRADE_FILTER_VERSION


CANONICAL_RENKO_SOURCE = "1s_close"
MARKET_STREAM_CONTRACT_VERSION = "silver.market_stream_contract.v1"
RENKO_STRATEGY_CONTRACT_VERSION = "silver.renko_strategy_contract.v1"
PROMOTION_CONTRACT_VERSION = "silver.renko_promotion_contract.v1"
RENKO_IMPLEMENTATION_VERSION = "renko_builder.update_renko_build_state.v1"
DELAYED_EMISSION_POLICY = "emit_previous_occupied_second_after_later_eligible_second"
END_OF_STREAM_FLUSH_POLICY = "NO_FLUSH"
DUPLICATE_SOURCE_ID_POLICY = "bounded_lru_ignore_duplicate_source_record_ids"
SESSION_CALENDAR_VERSION = "us_equities_regular_session.v1"


@dataclass(frozen=True)
class ContractFingerprint:
    version: str
    fingerprint: str
    payload: dict[str, Any]


def validate_canonical_renko_source(value: Any, *, trader_id: str | None = None) -> str:
    if value in {None, ""}:
        _raise_source_error(value, trader_id=trader_id, reason="missing")
    raw = str(value).strip()
    if raw != CANONICAL_RENKO_SOURCE:
        _raise_source_error(value, trader_id=trader_id, reason="noncanonical")
    return CANONICAL_RENKO_SOURCE


def market_stream_contract_payload(
    *,
    symbol: str,
    session_timezone: str,
    session_calendar: str = SESSION_CALENDAR_VERSION,
    session_mode: str = "regular",
    source_id_capacity: int = DEFAULT_SEEN_SOURCE_ID_CAPACITY,
) -> dict[str, Any]:
    return {
        "contract_version": MARKET_STREAM_CONTRACT_VERSION,
        "source_identifier": CANONICAL_RENKO_SOURCE,
        "symbol": symbol.upper(),
        "sip_eligibility_rules_version": TRADE_FILTER_VERSION,
        "sampler_implementation": "CanonicalOneSecondSipSampler",
        "sampler_version": SAMPLER_VERSION,
        "sampler_configuration": {
            "max_seen_source_ids": int(source_id_capacity),
        },
        "delayed_emission_policy": DELAYED_EMISSION_POLICY,
        "end_of_stream_flush_policy": END_OF_STREAM_FLUSH_POLICY,
        "duplicate_source_id_policy": DUPLICATE_SOURCE_ID_POLICY,
        "session_calendar": session_calendar,
        "session_mode": session_mode,
        "timezone": session_timezone,
    }


def market_stream_contract(
    *,
    symbol: str,
    session_timezone: str,
    session_calendar: str = SESSION_CALENDAR_VERSION,
    session_mode: str = "regular",
    source_id_capacity: int = DEFAULT_SEEN_SOURCE_ID_CAPACITY,
) -> ContractFingerprint:
    payload = market_stream_contract_payload(
        symbol=symbol,
        session_timezone=session_timezone,
        session_calendar=session_calendar,
        session_mode=session_mode,
        source_id_capacity=source_id_capacity,
    )
    return ContractFingerprint(MARKET_STREAM_CONTRACT_VERSION, stable_hash(payload), payload)


def renko_strategy_contract_payload(strategy_config: Any) -> dict[str, Any]:
    raw = asdict(strategy_config) if hasattr(strategy_config, "__dataclass_fields__") else dict(strategy_config)
    signal_parameters = {
        "strategy_type": raw.get("strategy_type"),
        "entry_bricks": raw.get("entry_bricks"),
        "take_profit_bricks": raw.get("take_profit_bricks"),
        "stop_loss_bricks": raw.get("stop_loss_bricks"),
        "psar_initial_stop_bricks": raw.get("psar_initial_stop_bricks"),
        "psar_af_step": raw.get("psar_af_step"),
        "psar_af_max": raw.get("psar_af_max"),
        "allow_short": raw.get("allow_short"),
        "renko_anchor_mode": raw.get("renko_anchor_mode"),
        "renko_anchor_price": raw.get("renko_anchor_price"),
        "entry_start": raw.get("entry_start"),
        "entry_end": raw.get("entry_end"),
        "force_flat": raw.get("force_flat"),
        "active_start_date": raw.get("active_start_date"),
        "active_end_date": raw.get("active_end_date"),
        "allow_overnight": raw.get("allow_overnight"),
        "timezone": raw.get("timezone"),
    }
    return {
        "contract_version": RENKO_STRATEGY_CONTRACT_VERSION,
        "renko_implementation": RENKO_IMPLEMENTATION_VERSION,
        "renko_epsilon": RENKO_EPSILON,
        "symbol": str(raw.get("symbol", "")).upper(),
        "brick_size": str(raw.get("brick_size")),
        "strategy_name": raw.get("strategy_name"),
        "signal_parameters": signal_parameters,
    }


def renko_strategy_contract(strategy_config: Any) -> ContractFingerprint:
    payload = renko_strategy_contract_payload(strategy_config)
    return ContractFingerprint(RENKO_STRATEGY_CONTRACT_VERSION, stable_hash(payload), payload)


def promotion_contract(
    *,
    market_stream_fingerprint: str,
    renko_strategy_fingerprint: str,
    source_dataset_hash: str | None = None,
    canonical_path: str = "canonical_silver_observations",
) -> ContractFingerprint:
    payload = {
        "contract_version": PROMOTION_CONTRACT_VERSION,
        "market_stream_fingerprint": market_stream_fingerprint,
        "renko_strategy_fingerprint": renko_strategy_fingerprint,
        "source_dataset_hash": source_dataset_hash,
        "canonical_path": canonical_path,
    }
    return ContractFingerprint(PROMOTION_CONTRACT_VERSION, stable_hash(payload), payload)


def contract_snapshot(
    *,
    symbol: str,
    strategy_config: Any,
    session_timezone: str,
    source_id_capacity: int = DEFAULT_SEEN_SOURCE_ID_CAPACITY,
    source_dataset_hash: str | None = None,
) -> dict[str, Any]:
    market = market_stream_contract(
        symbol=symbol,
        session_timezone=session_timezone,
        source_id_capacity=source_id_capacity,
    )
    strategy = renko_strategy_contract(strategy_config)
    combined = promotion_contract(
        market_stream_fingerprint=market.fingerprint,
        renko_strategy_fingerprint=strategy.fingerprint,
        source_dataset_hash=source_dataset_hash,
    )
    return {
        "market_stream": asdict(market),
        "renko_strategy": asdict(strategy),
        "promotion": asdict(combined),
    }


def stable_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return sha256(raw.encode("utf-8")).hexdigest()


def _raise_source_error(value: Any, *, trader_id: str | None, reason: str) -> None:
    prefix = f"{trader_id}: " if trader_id else ""
    raise ValueError(
        f"{prefix}Renko strategies must use renko_source={CANONICAL_RENKO_SOURCE!r}; "
        f"got {value!r} ({reason})."
    )
