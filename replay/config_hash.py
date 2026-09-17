from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from decimal import Decimal
from hashlib import sha256
from typing import Any

from silver.market import market_stream_contract, promotion_contract, renko_strategy_contract

from .models import ReplayConfig


def stable_config_hash(config: ReplayConfig) -> str:
    payload = semantic_configuration_payload(config)
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return sha256(raw.encode("utf-8")).hexdigest()


def replay_fingerprint(*, configuration_hash: str, source_dataset_hash: str, config: ReplayConfig) -> str:
    market = market_stream_contract(
        symbol=config.symbol,
        session_timezone=str(config.strategy_config.get("timezone") or "America/New_York"),
    )
    strategy = renko_strategy_contract(config.strategy_config)
    payload = {
        "configuration_hash": configuration_hash,
        "source_dataset_hash": source_dataset_hash,
        "replay_version": config.replay_version,
        "sampler_version": config.sampler_version,
        "renko_version": config.renko_version,
        "accounting_version": config.accounting_version,
        "metrics_version": config.metrics_version,
        "market_stream_fingerprint": market.fingerprint,
        "renko_strategy_fingerprint": strategy.fingerprint,
        "promotion_fingerprint": promotion_contract(
            market_stream_fingerprint=market.fingerprint,
            renko_strategy_fingerprint=strategy.fingerprint,
            source_dataset_hash=source_dataset_hash,
        ).fingerprint,
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return sha256(raw.encode("utf-8")).hexdigest()


def semantic_configuration_payload(config: ReplayConfig) -> dict[str, Any]:
    market = market_stream_contract(
        symbol=config.symbol,
        session_timezone=str(config.strategy_config.get("timezone") or "America/New_York"),
    )
    strategy = renko_strategy_contract(config.strategy_config)
    return _jsonable(
        {
            "trader_id": config.trader_id,
            "symbol": config.symbol,
            "start": config.start,
            "end": config.end,
            "timezone": config.timezone,
            "session_mode": config.session_mode,
            "comparison_start": config.comparison_start,
            "comparison_end": config.comparison_end,
            "warmup_start": config.warmup_start,
            "strategy_config": config.strategy_config,
            "brick_configuration": {
                "brick_mode": config.brick_mode,
                "brick_size_dollars": config.brick_size_dollars,
                "brick_size_ltp_percent": config.brick_size_ltp_percent,
                "derived_brick_size_ltp_percent": config.derived_brick_size_ltp_percent,
                "brick_reference_price": config.brick_reference_price,
                "brick_reference_timestamp": config.brick_reference_timestamp,
                "brick_reference_source": config.brick_reference_source,
                "brick_rounding_policy": config.brick_rounding_policy,
                "effective_brick_size_dollars": config.effective_brick_size_dollars,
            },
            "execution_model": config.execution_model,
            "execution_version": config.execution_version,
            "sampler_final_flush_policy": config.sampler_final_flush_policy,
            "initial_capital": config.initial_capital,
            "initial_renko_state": config.initial_renko_state,
            "initial_strategy_state": config.initial_strategy_state,
            "initial_accounting_position": config.initial_accounting_position,
            "initial_accounting_basis": config.initial_accounting_basis,
            "force_flat_replay_policy": config.force_flat_replay_policy,
            "source_semantic_mode": config.source_semantic_mode,
            "replay_version": config.replay_version,
            "sampler_version": config.sampler_version,
            "renko_version": config.renko_version,
            "accounting_version": config.accounting_version,
            "metrics_version": config.metrics_version,
            "production_config_hash": config.production_config.get("production_config_hash"),
            "market_stream_contract": asdict(market),
            "renko_strategy_contract": asdict(strategy),
        }
    )


def _jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return _jsonable(asdict(value))
    if isinstance(value, dict):
        return {str(key): _jsonable(val) for key, val in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value
