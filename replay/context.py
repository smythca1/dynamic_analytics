from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .models import ReplayConfig
from .config_hash import replay_fingerprint, stable_config_hash


@dataclass(frozen=True)
class ReplayContext:
    run_id: str
    replay_version: str
    replay_timestamp: datetime
    trader_id: str
    strategy_id: str
    strategy_version: str
    symbol: str
    execution_model: str
    sampler_version: str
    renko_version: str
    accounting_version: str
    metrics_version: str
    initial_state: dict[str, Any]
    configuration_hash: str
    replay_fingerprint: str
    git_commit: str | None
    source_dataset_hash: str
    source_dataset_reference: str
    output_level: str
    diagnostics_summary: dict[str, int] = field(default_factory=dict)

def current_git_commit() -> str | None:
    try:
        result = subprocess.run(["git", "rev-parse", "--short", "HEAD"], check=True, capture_output=True, text=True)
    except Exception:
        return None
    return result.stdout.strip() or None


def build_context(config: ReplayConfig, *, source_dataset_hash: str) -> ReplayContext:
    strategy_id = str(config.strategy_config.get("strategy_name") or config.strategy_config.get("strategy_type") or "unknown")
    config_hash = stable_config_hash(config)
    return ReplayContext(
        run_id=config.run_id,
        replay_version=config.replay_version,
        replay_timestamp=datetime.now(timezone.utc),
        trader_id=config.trader_id,
        strategy_id=strategy_id,
        strategy_version=str(config.strategy_config.get("strategy_type") or "unknown"),
        symbol=config.symbol,
        execution_model=config.execution_model,
        sampler_version=config.sampler_version,
        renko_version=config.renko_version,
        accounting_version=config.accounting_version,
        metrics_version=config.metrics_version,
        initial_state={
            "accounting_position": str(config.initial_accounting_position),
            "renko_state_supplied": config.initial_renko_state is not None,
            "strategy_state_supplied": config.initial_strategy_state is not None,
        },
        configuration_hash=config_hash,
        replay_fingerprint=replay_fingerprint(
            configuration_hash=config_hash,
            source_dataset_hash=source_dataset_hash,
            config=config,
        ),
        git_commit=current_git_commit(),
        source_dataset_hash=source_dataset_hash,
        source_dataset_reference=config.source_data_path,
        output_level=config.output_level,
    )
