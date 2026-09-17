from __future__ import annotations

import copy
import csv
import gc
import json
import math
import multiprocessing
import os
import socket
import subprocess
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass, is_dataclass, replace
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_FLOOR
from hashlib import sha256
from statistics import median, pstdev
from pathlib import Path
from typing import Any

import pandas as pd

from replay.config_hash import stable_config_hash
from replay.execution import NextObservationExecution
from replay.metrics import build_metrics
from replay.models import ReplayConfig
from renko_builder import RenkoBuildState, update_renko_build_state
from shared.config.trader_config import build_strategy_config, load_production_renko_config
from silver.accounting import AccountingLedger
from silver.accounting.models import EquityPoint
from silver.market import CanonicalObservation, promotion_contract, renko_strategy_contract
from strategy_engine import RenkoStrategyConfig, RenkoStrategyEngine
from trading_events import MarketTick, OrderIntent, RenkoBrick, Signal

from .models import (
    OPTIMIZER_VERSION,
    PREPARATION_VERSION,
    SENSITIVITY_RULES_VERSION,
    CandidateResult,
    CandidateSpec,
    OptimizationConfig,
    OptimizationResult,
    PreparedSource,
    StrategyOptimizationParameters,
    TraderSelection,
)
from .output import write_optimization_outputs
from .recommendations import build_recommendations
from .robustness import annotate_robustness
from .source_preparation import load_prepared_observations, prepare_source


_WORKER_CONFIG: OptimizationConfig | None = None
_WORKER_PREPARED: PreparedSource | None = None
_WORKER_OBSERVATIONS: tuple[CanonicalObservation, ...] | None = None
_WORKER_RENKO_STREAM_CACHE: dict[str, "CachedRenkoStream"] = {}
_WORKER_GC_THRESHOLDS = (1_000_000, 50, 50)


@dataclass(frozen=True)
class CachedRenkoObservation:
    observation: CanonicalObservation
    raw_bricks: tuple[dict[str, Any], ...]
    renko_bricks: tuple[RenkoBrick, ...]


@dataclass(frozen=True)
class CachedRenkoStream:
    cache_key: str
    symbol: str
    brick_size_dollars: Decimal
    brick_count: int
    renko_version: str
    events: tuple[CachedRenkoObservation, ...]


def brick_grid(start: Decimal, end: Decimal, step: Decimal) -> tuple[Decimal, ...]:
    values: list[Decimal] = []
    current = start
    while current <= end:
        values.append(current.quantize(Decimal("0.01")))
        current += step
    return tuple(values)


def strategy_parameter_grid_for(config: OptimizationConfig, trader: TraderSelection | None = None) -> tuple[StrategyOptimizationParameters, ...]:
    if config.parameter_grid_mode == "RENKO_TP_SL_ENTRY_TP_SL_GRID_V1":
        entries = config.selected_entry_confirmations or ("E1", "E2")
        take_profits = config.selected_take_profit_bricks or (1, 2, 3)
        stop_losses = config.selected_stop_loss_bricks or (1, 2, 3)
        return tuple(
            StrategyOptimizationParameters(
                strategy_family="FLATTENING_TP_SL_STRATEGY",
                strategy_type="renko_tp_sl",
                entry_confirmation=entry,
                take_profit_bricks=tp,
                stop_loss_bricks=sl,
            )
            for entry in entries
            for tp in take_profits
            for sl in stop_losses
        )
    if trader is None:
        return (
            StrategyOptimizationParameters(
                strategy_family="DIRECT_REVERSAL_BASELINE",
                strategy_type="renko_reversal",
                entry_confirmation="E1",
            ),
        )
    return (_production_strategy_parameters(trader),)


def load_enabled_renko_traders(
    config_path: str,
    selected: tuple[str, ...],
    *,
    include_disabled_research_traders: bool = False,
    included_strategy_types: tuple[str, ...] = ("renko_reversal",),
) -> tuple[TraderSelection, ...]:
    data = json.loads(Path(config_path).read_text(encoding="utf-8"))
    requested_all = selected == ("all-enabled-renko",) or not selected
    traders: list[TraderSelection] = []
    for raw in data.get("traders", []):
        if not isinstance(raw, dict):
            continue
        if not raw.get("enabled", True) and not include_disabled_research_traders:
            continue
        if not requested_all and raw.get("id") not in selected:
            continue
        strategy = build_strategy_config(raw)
        if not isinstance(strategy, RenkoStrategyConfig):
            continue
        if strategy.strategy_type not in set(included_strategy_types):
            continue
        loaded = load_production_renko_config(config_path, str(raw["id"]))
        traders.append(
            TraderSelection(
                trader_id=str(raw["id"]),
                symbol=strategy.symbol.upper(),
                strategy_name=strategy.strategy_name,
                strategy_type=strategy.strategy_type,
                strategy_config=dict(strategy.__dict__),
                production_metadata={**loaded.metadata, "production_config_hash": loaded.config_hash},
                production_config_hash=loaded.config_hash,
            )
        )
    return tuple(traders)


def _load_configured_renko_traders(config: OptimizationConfig) -> tuple[TraderSelection, ...]:
    strategy_types = ("renko_reversal", "renko_tp_sl") if config.parameter_grid_mode == "RENKO_TP_SL_ENTRY_TP_SL_GRID_V1" else ("renko_reversal",)
    return load_enabled_renko_traders(
        config.production_config_path,
        config.selected_trader_ids,
        include_disabled_research_traders=config.include_disabled_research_traders,
        included_strategy_types=strategy_types,
    )


def preflight(config: OptimizationConfig) -> dict[str, Any]:
    traders = _load_configured_renko_traders(config)
    grid = config.candidate_pcts or brick_grid(config.brick_start_pct, config.brick_end_pct, config.brick_step_pct_points)
    strategy_parameter_grid = strategy_parameter_grid_for(config)
    grid_candidate_count = len(grid) * len(strategy_parameter_grid)
    resource_plan = resolve_process_plan(config)
    source_inventory = []
    for trader in traders:
        for session in config.session_dates:
            path = Path(config.source_root) / trader.symbol / session.isoformat() / "sip_trades.jsonl"
            source_inventory.append({"symbol": trader.symbol, "session": session.isoformat(), "path": str(path), "exists": path.exists()})
    return {
        "optimization_id": config.optimization_id,
        "optimizer_version": OPTIMIZER_VERSION,
        "included_traders": [asdict(item) for item in traders],
        "session_dates": [item.isoformat() for item in config.session_dates],
        "source_inventory": source_inventory,
        "missing_files": [item for item in source_inventory if not item["exists"]],
        "candidate_count_per_trader": grid_candidate_count + (1 if config.include_current_production else 0),
        "grid_candidate_count_per_trader": grid_candidate_count,
        "strategy_parameter_count_per_brick": len(strategy_parameter_grid),
        "total_grid_candidate_count": len(traders) * grid_candidate_count,
        "total_candidate_count": len(traders) * (grid_candidate_count + (1 if config.include_current_production else 0)),
        "position_sizing_mode": config.position_sizing_mode,
        "parameter_grid_mode": config.parameter_grid_mode,
        "logical_cpu_count": resource_plan["logical_cpu_count"],
        "selected_process_count": resource_plan["selected_process_count"],
        "candidate_batch_size": config.candidate_batch_size,
        "estimated_memory_per_worker_mb": resource_plan["estimated_memory_per_worker_mb"],
        "estimated_runtime": "UNKNOWN; depends on raw source size and host IO",
        "estimated_output_size": "compact CSV/JSON summaries; selected TRADES outputs only",
        "output_path": str(config.output_path),
        "warmup_policy": config.warmup_policy,
        "limitations": [
            "research-only",
            "NEXT_CANONICAL_OBSERVATION_V1 is not calibrated live execution",
            "starting accounting state is flat at score start",
        ],
    }


def run_optimization(config: OptimizationConfig) -> OptimizationResult:
    start_time = time.monotonic()
    traders = _load_configured_renko_traders(config)
    if config.preflight_only:
        payload = preflight(config)
        output_dir = write_optimization_outputs(
            config=config,
            traders=traders,
            prepared_sources={},
            rows=[],
            recommendations={},
            cross_asset_summary={},
            diagnostics_summary={},
            preflight=payload,
        )
        return OptimizationResult(config, traders, {}, tuple(), {}, {}, str(output_dir), preflight=payload)
    resource_plan = resolve_process_plan(config)
    process_count = int(resource_plan["selected_process_count"])
    prepared: dict[str, PreparedSource] = {}
    observations_by_symbol: dict[str, tuple[CanonicalObservation, ...]] = {}
    diagnostics_summary: dict[str, int] = {}
    for trader in traders:
        if trader.symbol in prepared:
            continue
        source = prepare_source(
            symbol=trader.symbol,
            trader_id=trader.trader_id,
            session_dates=config.session_dates,
            source_root=config.source_root,
            output_root=config.prepared_output_root,
            run_id=f"{config.optimization_id}:{trader.symbol}:prep",
        )
        prepared[trader.symbol] = source
        observations_by_symbol[trader.symbol] = load_prepared_observations(source.observations_path)
        for key, value in source.diagnostics_summary.items():
            diagnostics_summary[key] = diagnostics_summary.get(key, 0) + value
    rows: list[dict[str, Any]] = []
    all_specs: list[CandidateSpec] = []
    for trader in traders:
        source = prepared[trader.symbol]
        observations = observations_by_symbol[trader.symbol]
        all_specs.extend(build_candidate_specs(config=config, trader=trader, prepared=source, observations=observations))
    config_hash = _optimization_config_hash(config)
    grid_hash = _candidate_grid_hash(all_specs)
    completed = _load_completed_candidates(config=config, config_hash=config_hash, grid_hash=grid_hash, prepared=prepared)
    rows_by_id: dict[str, dict[str, Any]] = dict(completed)
    progress = _ProgressWriter(
        config=config,
        total_candidates=len(all_specs),
        process_count=process_count,
        batch_size=config.candidate_batch_size,
        config_hash=config_hash,
        grid_hash=grid_hash,
        prepared=prepared,
    )
    progress.write(current_asset=None, status="RUNNING", rows_by_id=rows_by_id)
    for trader in traders:
        source = prepared[trader.symbol]
        observations = observations_by_symbol[trader.symbol]
        specs = [
            spec
            for spec in build_candidate_specs(config=config, trader=trader, prepared=source, observations=observations)
            if _should_run_spec(spec, rows_by_id, retry_failed=config.retry_failed)
        ]
        if process_count == 1:
            for batch in _candidate_batches(config=config, specs=specs):
                batch_rows = _run_candidate_batch(config=config, specs=batch, prepared=source, observations=observations)
                for row in batch_rows:
                    rows_by_id[str(row["candidate_id"])] = row
                    progress.record(row, current_asset=trader.symbol, rows_by_id=rows_by_id)
        else:
            batches = tuple(_candidate_batches(config=config, specs=specs))
            try:
                context = multiprocessing.get_context("spawn")
                with ProcessPoolExecutor(
                    max_workers=process_count,
                    mp_context=context,
                    initializer=_initialize_candidate_worker,
                    initargs=(config, source, source.observations_path),
                ) as pool:
                    for batch_rows in pool.map(_run_candidate_batch_job, batches):
                        for row in batch_rows:
                            rows_by_id[str(row["candidate_id"])] = row
                            progress.record(row, current_asset=trader.symbol, rows_by_id=rows_by_id)
            except KeyboardInterrupt:
                progress.write(current_asset=trader.symbol, status="INTERRUPTED", rows_by_id=rows_by_id, force=True)
                raise
            except (OSError, PermissionError) as exc:
                diagnostics_summary["process_pool_unavailable_fallback_serial"] = diagnostics_summary.get(
                    "process_pool_unavailable_fallback_serial", 0
                ) + 1
                diagnostics_summary["process_pool_unavailable_error"] = f"{type(exc).__name__}: {exc}"
                for spec in specs:
                    row = run_candidate(config=config, spec=spec, prepared=source, observations=observations).row
                    rows_by_id[spec.candidate_id] = row
                    progress.record(row, current_asset=trader.symbol, rows_by_id=rows_by_id)
    rows = annotate_robustness(
        [rows_by_id[spec.candidate_id] for spec in all_specs if spec.candidate_id in rows_by_id],
        minimum_closed_trades=config.minimum_closed_trades,
        max_drawdown_pct_threshold=config.max_drawdown_pct_threshold,
        local_profitable_pct_threshold=config.local_profitable_pct_threshold,
    )
    rows = _annotate_sensitivity(rows)
    recommendations = build_recommendations(rows)
    specs_by_id = {
        spec.candidate_id: (spec, prepared[spec.trader.symbol], observations_by_symbol[spec.trader.symbol])
        for trader in traders
        for spec in build_candidate_specs(config=config, trader=trader, prepared=prepared[trader.symbol], observations=observations_by_symbol[trader.symbol])
    }
    _write_selected_outputs(config=config, rows=rows, recommendations=recommendations, specs_by_id=specs_by_id, diagnostics_summary=diagnostics_summary)
    cross_asset_summary = _cross_asset_summary(rows, recommendations)
    diagnostics_summary["elapsed_seconds"] = int(time.monotonic() - start_time)
    diagnostics_summary["candidate_rows"] = len(rows)
    diagnostics_summary["logical_cpu_count"] = int(resource_plan["logical_cpu_count"])
    diagnostics_summary["selected_process_count"] = process_count
    diagnostics_summary["candidate_batch_size"] = config.candidate_batch_size
    progress.write(current_asset=None, status="COMPLETED", rows_by_id={str(row["candidate_id"]): row for row in rows}, force=True)
    output_dir = write_optimization_outputs(
        config=config,
        traders=traders,
        prepared_sources=prepared,
        rows=rows,
        recommendations=recommendations,
        cross_asset_summary=cross_asset_summary,
        diagnostics_summary=diagnostics_summary,
    )
    return OptimizationResult(config, traders, prepared, tuple(rows), recommendations, cross_asset_summary, str(output_dir), diagnostics_summary)


def build_candidate_specs(
    *,
    config: OptimizationConfig,
    trader: TraderSelection,
    prepared: PreparedSource,
    observations: tuple[CanonicalObservation, ...],
) -> tuple[CandidateSpec, ...]:
    if not observations:
        raise ValueError(f"No prepared observations for {trader.symbol}.")
    reference = observations[0]
    reference_price = reference.close_price
    production_qty = Decimal(str(trader.strategy_config.get("qty", 0)))
    research_qty, allocated_capital, unused_cash = _position_allocation(
        mode=config.position_sizing_mode,
        production_quantity=production_qty,
        initial_capital=config.initial_capital,
        reference_price=reference_price,
    )
    grid = config.candidate_pcts or brick_grid(config.brick_start_pct, config.brick_end_pct, config.brick_step_pct_points)
    specs: list[CandidateSpec] = []
    parameter_grid = strategy_parameter_grid_for(config, trader)
    for pct in grid:
        for parameters in parameter_grid:
            brick_dollars = reference_price * pct / Decimal("100")
            candidate_id = _candidate_id(
                config.optimization_id,
                trader.trader_id,
                "grid",
                pct,
                *parameters.identity_parts(),
                config.position_sizing_mode,
                config.execution_model,
                config.warmup_policy,
                prepared.source_combined_hash,
            )
            specs.append(
                CandidateSpec(
                    candidate_id,
                    "BRICK_PERCENT_GRID",
                    trader,
                    pct,
                    brick_dollars,
                    reference_price,
                    reference.second_bucket,
                    production_qty,
                    research_qty,
                    allocated_capital,
                    unused_cash,
                    config.position_sizing_mode,
                    parameters,
                )
            )
    if config.include_current_production:
        current = Decimal(str(trader.strategy_config["brick_size"]))
        derived_pct = current / reference_price * Decimal("100")
        production_parameters = _production_strategy_parameters(trader)
        candidate_id = _candidate_id(
            config.optimization_id,
            trader.trader_id,
            "current",
            current,
            *production_parameters.identity_parts(),
            config.position_sizing_mode,
            config.execution_model,
            config.warmup_policy,
            prepared.source_combined_hash,
        )
        if config.position_sizing_mode == "CAPITAL_ALLOCATED_WHOLE_SHARES_V1" and config.parameter_grid_mode == "RENKO_TP_SL_ENTRY_TP_SL_GRID_V1":
            current_type = "CURRENT_PRODUCTION_REVERSAL_CAPITAL_ALLOCATED"
        else:
            current_type = "CURRENT_PRODUCTION_CAPITAL_ALLOCATED" if config.position_sizing_mode == "CAPITAL_ALLOCATED_WHOLE_SHARES_V1" else "CURRENT_PRODUCTION"
        specs.append(
            CandidateSpec(
                candidate_id,
                current_type,
                trader,
                derived_pct,
                current,
                reference_price,
                reference.second_bucket,
                production_qty,
                research_qty,
                allocated_capital,
                unused_cash,
                config.position_sizing_mode,
                production_parameters,
                reference_price_policy="CURRENT_PRODUCTION_FIXED_DOLLARS",
            )
        )
    return tuple(specs)


def _initialize_candidate_worker(config: OptimizationConfig, prepared: PreparedSource, observations_path: str) -> None:
    global _WORKER_CONFIG, _WORKER_PREPARED, _WORKER_OBSERVATIONS, _WORKER_RENKO_STREAM_CACHE
    gc.set_threshold(*_WORKER_GC_THRESHOLDS)
    if config.nice_level:
        try:
            os.nice(config.nice_level)
        except OSError:
            pass
    _WORKER_CONFIG = config
    _WORKER_PREPARED = prepared
    _WORKER_OBSERVATIONS = load_prepared_observations(observations_path)
    _WORKER_RENKO_STREAM_CACHE = {}


def _run_candidate_batch_job(specs: tuple[CandidateSpec, ...]) -> list[dict[str, Any]]:
    if _WORKER_CONFIG is None or _WORKER_PREPARED is None or _WORKER_OBSERVATIONS is None:
        raise RuntimeError("Candidate worker was not initialized.")
    return _run_candidate_batch(config=_WORKER_CONFIG, specs=specs, prepared=_WORKER_PREPARED, observations=_WORKER_OBSERVATIONS)


def _run_candidate_batch(
    *,
    config: OptimizationConfig,
    specs: tuple[CandidateSpec, ...],
    prepared: PreparedSource,
    observations: tuple[CanonicalObservation, ...],
) -> list[dict[str, Any]]:
    stream_cache: dict[str, CachedRenkoStream] = {}
    rows: list[dict[str, Any]] = []
    for spec in specs:
        stream = None
        if _cached_renko_stream_supported(spec):
            cache_key = _renko_stream_cache_key(config=config, spec=spec, prepared=prepared)
            stream = stream_cache.get(cache_key)
            if stream is None:
                stream = _build_cached_renko_stream(config=config, spec=spec, prepared=prepared, observations=observations)
                stream_cache[cache_key] = stream
        rows.append(run_candidate(config=config, spec=spec, prepared=prepared, observations=observations, renko_stream=stream).row)
    return rows


def resolve_process_plan(config: OptimizationConfig) -> dict[str, Any]:
    logical = os.cpu_count() or 1
    if config.process_count == "auto":
        selected = max(1, min(logical - 2, 12))
    else:
        selected = int(config.process_count)
    estimated_mb = 256
    if config.max_memory_gb is not None:
        memory_limited = int((Decimal(str(config.max_memory_gb)) * Decimal("1024")) // Decimal(estimated_mb))
        selected = max(1, min(selected, memory_limited))
    live_service_active = _live_trading_service_active() if config.host_role == "live-trading" else None
    if config.host_role == "live-trading" and (selected > 1 or live_service_active):
        raise ValueError("live-trading host role permits only explicit low-impact one-process checks while fleet service is inactive.")
    return {
        "logical_cpu_count": logical,
        "selected_process_count": selected,
        "estimated_memory_per_worker_mb": estimated_mb,
        "host": socket.gethostname(),
        "host_role": config.host_role,
        "trading_fleet_live_active": live_service_active,
    }


def _live_trading_service_active() -> bool:
    try:
        result = subprocess.run(
            ["systemctl", "is-active", "trading-fleet-live.service"],
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return result.stdout.strip() == "active"


class _ProgressWriter:
    def __init__(
        self,
        *,
        config: OptimizationConfig,
        total_candidates: int,
        process_count: int,
        batch_size: int,
        config_hash: str,
        grid_hash: str,
        prepared: dict[str, PreparedSource],
    ) -> None:
        self.config = config
        self.total_candidates = total_candidates
        self.process_count = process_count
        self.batch_size = batch_size
        self.config_hash = config_hash
        self.grid_hash = grid_hash
        self.prepared = prepared
        self.started = time.monotonic()
        self.started_at = datetime.now(timezone.utc)
        self.completed_path = config.output_path / "completed_candidates.jsonl"
        self.progress_path = config.output_path / "progress.json"
        self._records_since_progress = 0
        self._last_progress = 0.0
        self.completed_path.parent.mkdir(parents=True, exist_ok=True)
        if not config.resume:
            if self.completed_path.exists():
                self.completed_path.write_text("", encoding="utf-8")
            if self.progress_path.exists():
                self.progress_path.write_text("", encoding="utf-8")

    def record(self, row: dict[str, Any], *, current_asset: str | None, rows_by_id: dict[str, dict[str, Any]]) -> None:
        with self.completed_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(_jsonable(row), sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        self._records_since_progress += 1
        elapsed_since = time.monotonic() - self._last_progress
        if (
            self._records_since_progress >= self.config.checkpoint_every_candidates
            or elapsed_since >= self.config.progress_every_seconds
        ):
            self.write(current_asset=current_asset, status="RUNNING", rows_by_id=rows_by_id)

    def write(
        self,
        *,
        current_asset: str | None,
        status: str,
        rows_by_id: dict[str, dict[str, Any]],
        force: bool = False,
    ) -> None:
        if not force and time.monotonic() - self._last_progress < 1 and self._records_since_progress == 0:
            return
        completed = len(rows_by_id)
        failed = sum(1 for row in rows_by_id.values() if row.get("candidate_status") == "FAILED")
        successful = sum(1 for row in rows_by_id.values() if row.get("candidate_status") == "COMPLETED")
        elapsed = max(time.monotonic() - self.started, 0.000001)
        rate = Decimal(str(completed)) / Decimal(str(elapsed)) if completed else Decimal("0")
        remaining = self.total_candidates - completed
        payload = {
            "optimization_id": self.config.optimization_id,
            "total_candidates": self.total_candidates,
            "completed_candidates": completed,
            "successful_candidates": successful,
            "failed_candidates": failed,
            "pending_candidates": remaining,
            "current_asset": current_asset,
            "current_strategy_family": self.config.parameter_grid_mode,
            "started_at": self.started_at.isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "elapsed_seconds": elapsed,
            "candidates_per_second": _num(rate),
            "estimated_remaining_seconds": _num(Decimal(str(remaining)) / rate) if rate > 0 else "UNKNOWN",
            "process_count": self.process_count,
            "batch_size": self.batch_size,
            "host": socket.gethostname(),
            "source_preparation_ids": {symbol: source.preparation_id for symbol, source in self.prepared.items()},
            "config_hash": self.config_hash,
            "candidate_grid_hash": self.grid_hash,
            "status": status,
        }
        tmp = self.progress_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(_jsonable(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
        tmp.replace(self.progress_path)
        self._records_since_progress = 0
        self._last_progress = time.monotonic()


def _load_completed_candidates(
    *,
    config: OptimizationConfig,
    config_hash: str,
    grid_hash: str,
    prepared: dict[str, PreparedSource],
) -> dict[str, dict[str, Any]]:
    if not config.resume:
        return {}
    progress_path = config.output_path / "progress.json"
    completed_path = config.output_path / "completed_candidates.jsonl"
    if not progress_path.exists() or not completed_path.exists():
        return {}
    progress = json.loads(progress_path.read_text(encoding="utf-8"))
    if progress.get("config_hash") != config_hash:
        raise ValueError("Cannot resume optimization: config hash does not match.")
    if progress.get("candidate_grid_hash") != grid_hash:
        raise ValueError("Cannot resume optimization: candidate grid hash does not match.")
    expected_preps = {symbol: source.preparation_id for symbol, source in prepared.items()}
    if progress.get("source_preparation_ids") != expected_preps:
        raise ValueError("Cannot resume optimization: source preparation IDs do not match.")
    rows: dict[str, dict[str, Any]] = {}
    with completed_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            candidate_id = str(row.get("candidate_id"))
            if candidate_id:
                rows[candidate_id] = row
    return rows


def _should_run_spec(spec: CandidateSpec, rows_by_id: dict[str, dict[str, Any]], *, retry_failed: bool) -> bool:
    row = rows_by_id.get(spec.candidate_id)
    if row is None:
        return True
    return retry_failed and row.get("candidate_status") == "FAILED"


def _candidate_batches(*, config: OptimizationConfig, specs: list[CandidateSpec]) -> list[tuple[CandidateSpec, ...]]:
    if config.parameter_grid_mode != "RENKO_TP_SL_ENTRY_TP_SL_GRID_V1":
        return _chunks(specs, config.candidate_batch_size)
    brick_groups: list[list[CandidateSpec]] = []
    current_key: tuple[str, str, str] | None = None
    for spec in specs:
        key = (spec.trader.trader_id, str(spec.brick_size_dollars), spec.strategy_parameters.strategy_type)
        if key != current_key:
            brick_groups.append([])
            current_key = key
        brick_groups[-1].append(spec)

    batches: list[tuple[CandidateSpec, ...]] = []
    current: list[CandidateSpec] = []
    for group in brick_groups:
        max_batch_size = max(config.candidate_batch_size, len(group))
        if current and len(current) + len(group) > max_batch_size:
            batches.append(tuple(current))
            current = []
        current.extend(group)
    if current:
        batches.append(tuple(current))
    return batches


def _chunks(specs: list[CandidateSpec], size: int) -> list[tuple[CandidateSpec, ...]]:
    return [tuple(specs[index : index + size]) for index in range(0, len(specs), size)]


def _optimization_config_hash(config: OptimizationConfig) -> str:
    payload = {
        "optimization_id": config.optimization_id,
        "production_config_path": config.production_config_path,
        "session_dates": [item.isoformat() for item in config.session_dates],
        "source_root": config.source_root,
        "selected_trader_ids": config.selected_trader_ids,
        "warmup_policy": config.warmup_policy,
        "initial_capital": str(config.initial_capital),
        "execution_model": config.execution_model,
        "position_sizing_mode": config.position_sizing_mode,
        "parameter_grid_mode": config.parameter_grid_mode,
        "selected_entry_confirmations": config.selected_entry_confirmations,
        "selected_take_profit_bricks": config.selected_take_profit_bricks,
        "selected_stop_loss_bricks": config.selected_stop_loss_bricks,
        "include_current_production": config.include_current_production,
        "include_disabled_research_traders": config.include_disabled_research_traders,
        "minimum_closed_trades": config.minimum_closed_trades,
    }
    return sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _candidate_grid_hash(specs: list[CandidateSpec]) -> str:
    payload = [
        {
            "candidate_id": spec.candidate_id,
            "trader_id": spec.trader.trader_id,
            "brick_pct": str(spec.brick_pct),
            "brick_size": str(spec.brick_size_dollars),
            "strategy": spec.strategy_parameters.identity_parts(),
            "quantity": str(spec.research_quantity),
        }
        for spec in specs
    ]
    return sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def run_candidate(
    *,
    config: OptimizationConfig,
    spec: CandidateSpec,
    prepared: PreparedSource,
    observations: tuple[CanonicalObservation, ...],
    renko_stream: CachedRenkoStream | None = None,
    use_cached_renko_stream: bool = True,
) -> CandidateResult:
    try:
        row = _execute_candidate(
            config=config,
            spec=spec,
            prepared=prepared,
            observations=observations,
            renko_stream=renko_stream,
            use_cached_renko_stream=use_cached_renko_stream,
        )
        return CandidateResult(spec=spec, status="COMPLETED", row=row)
    except Exception as exc:
        row = _base_row(config=config, spec=spec, prepared=prepared)
        row.update({"candidate_status": "FAILED", "error_type": type(exc).__name__, "error_message": str(exc), "result_quality": "FAILED"})
        return CandidateResult(spec=spec, status="FAILED", row=row, error_type=type(exc).__name__, error_message=str(exc))


def promotion_contract_guard(
    *,
    candidate_row: dict[str, Any],
    target_contract_fingerprints: dict[str, Any],
    session_coverage_complete: bool,
) -> dict[str, Any]:
    reasons: list[str] = []
    if not session_coverage_complete:
        reasons.append("incomplete_session_coverage")
    if candidate_row.get("candidate_status") != "COMPLETED":
        reasons.append("candidate_not_completed")
    if candidate_row.get("approved_canonical_path") is not True:
        reasons.append("candidate_not_from_approved_canonical_path")
    if candidate_row.get("promotable_contract_metadata") is not True:
        reasons.append("missing_promotable_contract_metadata")

    target_market = target_contract_fingerprints.get("market_stream") if isinstance(target_contract_fingerprints, dict) else None
    target_strategy = target_contract_fingerprints.get("renko_strategy") if isinstance(target_contract_fingerprints, dict) else None
    target_market_fingerprint = target_market.get("fingerprint") if isinstance(target_market, dict) else None
    target_strategy_fingerprint = target_strategy.get("fingerprint") if isinstance(target_strategy, dict) else None
    if not target_market_fingerprint:
        reasons.append("target_missing_market_stream_fingerprint")
    elif candidate_row.get("market_stream_fingerprint") != target_market_fingerprint:
        reasons.append("market_stream_fingerprint_mismatch")
    if not target_strategy_fingerprint:
        reasons.append("target_missing_renko_strategy_fingerprint")
    elif candidate_row.get("renko_strategy_fingerprint") != target_strategy_fingerprint:
        reasons.append("renko_strategy_fingerprint_mismatch")
    return {
        "promotable": not reasons,
        "reasons": reasons,
        "candidate_market_stream_fingerprint": candidate_row.get("market_stream_fingerprint"),
        "target_market_stream_fingerprint": target_market_fingerprint,
        "candidate_renko_strategy_fingerprint": candidate_row.get("renko_strategy_fingerprint"),
        "target_renko_strategy_fingerprint": target_strategy_fingerprint,
    }


def _execute_candidate(
    *,
    config: OptimizationConfig,
    spec: CandidateSpec,
    prepared: PreparedSource,
    observations: tuple[CanonicalObservation, ...],
    selected_output_dir: Path | None = None,
    renko_stream: CachedRenkoStream | None = None,
    use_cached_renko_stream: bool = True,
) -> dict[str, Any]:
    strategy_config = copy.deepcopy(spec.trader.strategy_config)
    strategy_config["strategy_type"] = spec.strategy_parameters.strategy_type
    strategy_config["brick_size"] = float(spec.brick_size_dollars)
    strategy_config["qty"] = float(spec.research_quantity)
    strategy_config["entry_bricks"] = spec.strategy_parameters.entry_bricks
    strategy_config["take_profit_bricks"] = spec.strategy_parameters.take_profit_bricks
    strategy_config["stop_loss_bricks"] = spec.strategy_parameters.stop_loss_bricks
    strategy = RenkoStrategyEngine(RenkoStrategyConfig(**strategy_config))
    if renko_stream is not None and not (hasattr(strategy, "_on_brick") and hasattr(strategy, "_force_flat_action")):
        renko_stream = None
    if (
        renko_stream is None
        and use_cached_renko_stream
        and _cached_renko_stream_supported(spec)
        and hasattr(strategy, "_on_brick")
        and hasattr(strategy, "_force_flat_action")
    ):
        renko_stream = _build_cached_renko_stream(config=config, spec=spec, prepared=prepared, observations=observations)
    accounting = AccountingLedger(symbol=spec.trader.symbol, starting_capital=config.initial_capital)
    execution = NextObservationExecution(run_id=spec.candidate_id, trader_id=spec.trader.trader_id)
    order_latencies: list[Decimal] = []
    renko_bricks = []
    signals = []
    orders = []
    fills = []
    equity_curve: list[EquityPoint] = []
    position_samples: list[dict[str, Any]] = []
    observation_events: Any = renko_stream.events if renko_stream is not None else observations
    for event_or_observation in observation_events:
        if renko_stream is not None:
            event = event_or_observation
            observation = event.observation
        else:
            event = None
            observation = event_or_observation
        for bundle in execution.fill_pending(observation=observation, position=accounting.position):
            order_latencies.append(bundle.latency_seconds)
            for fill in bundle.fills:
                fills.append(fill)
                accounting.process_fill(fill)
                if fill.leg == "close" and accounting.position.side is None and hasattr(strategy, "mark_close_filled"):
                    strategy.mark_close_filled(
                        timestamp=fill.timestamp,
                        reason=fill.reason,
                        order_id=fill.order_id,
                        source=execution.version,
                    )
        snapshot = accounting.snapshot(mark_price=observation.close_price)
        _validate_tp_sl_position_invariant(spec=spec, position=accounting.position)
        position_samples.append(
            {
                "timestamp": observation.second_bucket,
                "side": accounting.position.side or "flat",
                "qty": accounting.position.qty,
                "mark_price": observation.close_price,
                "equity": snapshot.equity,
            }
        )
        equity_curve.append(
            EquityPoint(
                timestamp=observation.second_bucket,
                equity=snapshot.equity,
                realized_pnl=snapshot.realized_pnl,
                unrealized_pnl=snapshot.unrealized_pnl,
                net_pnl=snapshot.net_pnl,
                fill_id=f"mark:{spec.candidate_id}:{len(equity_curve)+1}",
            )
        )
        tick = MarketTick(
            timestamp=pd.Timestamp(observation.second_bucket),
            symbol=observation.symbol,
            price=float(observation.close_price),
            size=1.0,
            exchange=observation.source_exchange,
            trade_id=observation.source_alpaca_trade_id,
            tape=observation.source_tape,
            conditions=(),
        )
        if event is not None:
            new_bricks, actions = _actions_from_cached_renko_event(strategy=strategy, tick=tick, event=event)
        else:
            new_bricks, actions = strategy.on_tick(tick)
        renko_bricks.extend(new_bricks)
        for signal, intent in actions:
            signal, intent = _stable_optimizer_action_identifiers(
                spec=spec,
                observation=observation,
                action_sequence=len(signals) + 1,
                signal=signal,
                intent=intent,
            )
            signals.append(signal)
            order = execution.queue(signal=signal, intent=intent, observation=observation, position=accounting.position)
            if order is None:
                if intent.close_position and hasattr(strategy, "mark_close_filled"):
                    strategy.mark_close_filled(
                        timestamp=signal.timestamp,
                        reason=intent.reason,
                        order_id=None,
                        source=f"{execution.version}:already_flat",
                    )
                continue
            if intent.close_position and hasattr(strategy, "mark_close_submitted"):
                strategy.mark_close_submitted(intent, response={"id": order.order_id})
            orders.append(order)
    final_snapshot = accounting.snapshot(mark_price=observations[-1].close_price if observations else None)
    _validate_tp_sl_position_invariant(spec=spec, position=final_snapshot.position)
    metrics = build_metrics(
        snapshot=final_snapshot,
        initial_capital=config.initial_capital,
        observations=list(observations),
        order_latencies=order_latencies,
        equity_curve=equity_curve,
        benchmark_quantity=spec.research_quantity,
        allocated_capital=spec.allocated_capital,
        unused_cash=spec.unused_cash,
        position_samples=position_samples,
        fills=fills,
    )
    replay_config = ReplayConfig(
        run_id=spec.candidate_id,
        trader_id=spec.trader.trader_id,
        symbol=spec.trader.symbol,
        start=observations[0].second_bucket,
        end=observations[-1].second_bucket,
        timezone="UTC",
        session_mode="regular",
        strategy_config=strategy_config,
        initial_capital=config.initial_capital,
        brick_mode="BRICK_DOLLARS",
        brick_size_dollars=spec.brick_size_dollars,
        source_data_path=prepared.observations_path,
        source_semantic_mode="prepared_canonical_observations",
    )
    configuration_hash = stable_config_hash(replay_config)
    market_contract = prepared.market_stream_contract if isinstance(prepared.market_stream_contract, dict) else {}
    market_fingerprint = market_contract.get("fingerprint")
    strategy_contract = renko_strategy_contract(strategy_config)
    combined_contract = (
        promotion_contract(
            market_stream_fingerprint=str(market_fingerprint),
            renko_strategy_fingerprint=strategy_contract.fingerprint,
            source_dataset_hash=prepared.source_combined_hash,
        )
        if market_fingerprint
        else None
    )
    replay_fingerprint = sha256(
        f"{configuration_hash}|{prepared.source_combined_hash}|{PREPARATION_VERSION}|{config.execution_model}|"
        f"{market_fingerprint}|{strategy_contract.fingerprint}".encode("utf-8")
    ).hexdigest()
    row = _base_row(config=config, spec=spec, prepared=prepared)
    row.update(
        {
            "configuration_hash": configuration_hash,
            "replay_fingerprint": replay_fingerprint,
            "market_stream_fingerprint": market_fingerprint,
            "market_stream_contract_version": market_contract.get("version"),
            "renko_strategy_fingerprint": strategy_contract.fingerprint,
            "renko_strategy_contract_version": strategy_contract.version,
            "promotion_fingerprint": combined_contract.fingerprint if combined_contract else None,
            "promotion_contract_version": combined_contract.version if combined_contract else None,
            "approved_canonical_path": bool(prepared.approved_canonical_path and market_fingerprint),
            "promotable_contract_metadata": bool(prepared.promotable and market_fingerprint and combined_contract is not None),
            "candidate_status": "COMPLETED",
            "result_quality": "FULL_SESSION_RESEARCH",
            "canonical_observations": len(observations),
            "renko_bricks": len(renko_bricks),
            "strategy_signals": len(signals),
            "queued_orders": len(orders),
            "simulated_fills": len(fills),
            "pending_orders_at_end": len(execution.pending_orders),
            "closed_trades": metrics["closed_trades"],
            **_metrics_row(metrics),
            **_annualized_session_row(equity_curve=equity_curve, initial_capital=config.initial_capital),
            **_trade_audit_metrics_row(closed_trades=list(final_snapshot.closed_trades), position_samples=position_samples),
            **_capital_row(config=config, spec=spec, metrics=metrics),
        }
    )
    if selected_output_dir is not None:
        _write_selected_candidate_outputs(
            selected_output_dir,
            row=row,
            fills=fills,
            signals=signals,
            orders=orders,
            renko_bricks=renko_bricks,
            position_transitions=list(final_snapshot.position_transitions),
            closed_trades=list(final_snapshot.closed_trades),
            equity_curve=equity_curve,
        )
    return row


def _stable_optimizer_action_identifiers(
    *,
    spec: CandidateSpec,
    observation: CanonicalObservation,
    action_sequence: int,
    signal: Signal,
    intent: OrderIntent,
) -> tuple[Signal, OrderIntent]:
    stable_suffix = sha256(
        "|".join(
            str(item)
            for item in (
                spec.candidate_id,
                action_sequence,
                observation.observation_id,
                signal.timestamp.isoformat(),
                signal.symbol,
                signal.signal_type,
                signal.side,
                signal.broker_side,
                signal.price,
                signal.reason,
            )
        ).encode("utf-8")
    ).hexdigest()[:16]
    stable_signal_id = f"{_safe_path_name(spec.trader.trader_id)}-{signal.symbol.lower()}-{stable_suffix}"
    stable_prefix = _safe_path_name(spec.trader.trader_id.lower())
    stable_client_order_id = f"{stable_prefix[:31]}-{stable_suffix}"[:48]
    exit_plan = intent.exit_plan
    if isinstance(exit_plan, dict):
        exit_plan = dict(exit_plan)
        exit_plan["client_order_id"] = stable_client_order_id
        exit_plan["take_profit_client_order_id"] = f"{stable_client_order_id[:45]}-tp"[:48]
        exit_plan["stop_loss_client_order_id"] = f"{stable_client_order_id[:45]}-sl"[:48]
    return (
        replace(signal, signal_id=stable_signal_id),
        replace(intent, signal_id=stable_signal_id, client_order_id=stable_client_order_id, exit_plan=exit_plan),
    )


def _cached_renko_stream_supported(spec: CandidateSpec) -> bool:
    return spec.strategy_parameters.strategy_type == "renko_tp_sl"


def _renko_stream_cache_key(*, config: OptimizationConfig, spec: CandidateSpec, prepared: PreparedSource) -> str:
    payload = {
        "cache_contract": "RENKO_STREAM_CACHE_V1",
        "prepared_source_hash": prepared.source_combined_hash,
        "source_preparation_id": prepared.preparation_id,
        "symbol": spec.trader.symbol,
        "sessions": [item.isoformat() for item in prepared.session_dates],
        "brick_size_dollars": str(spec.brick_size_dollars),
        "renko_anchor_mode": spec.trader.strategy_config.get("renko_anchor_mode"),
        "renko_anchor_price": spec.trader.strategy_config.get("renko_anchor_price"),
        "renko_implementation": "renko_builder.update_renko_build_state",
        "renko_cache_version": "RENKO_STREAM_CACHE_V1",
        "source_semantics": "prepared_canonical_one_second_observations",
        "preparation_version": PREPARATION_VERSION,
        "execution_model": config.execution_model,
    }
    return sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _build_cached_renko_stream(
    *,
    config: OptimizationConfig,
    spec: CandidateSpec,
    prepared: PreparedSource,
    observations: tuple[CanonicalObservation, ...],
) -> CachedRenkoStream:
    state = RenkoBuildState(
        brick_size=float(spec.brick_size_dollars),
        anchor_mode=str(spec.trader.strategy_config.get("renko_anchor_mode") or "first_price"),
        anchor_price=spec.trader.strategy_config.get("renko_anchor_price"),
    )
    processed = 0
    events: list[CachedRenkoObservation] = []
    brick_count = 0
    for observation in observations:
        update_renko_build_state(
            state,
            timestamps=[pd.Timestamp(observation.second_bucket)],
            prices=[float(observation.close_price)],
        )
        raw_bricks = tuple(dict(row) for row in state.bricks[processed:])
        processed = len(state.bricks)
        renko_bricks = tuple(_renko_brick_from_row(spec=spec, row=row) for row in raw_bricks)
        brick_count += len(renko_bricks)
        events.append(CachedRenkoObservation(observation=observation, raw_bricks=raw_bricks, renko_bricks=renko_bricks))
    return CachedRenkoStream(
        cache_key=_renko_stream_cache_key(config=config, spec=spec, prepared=prepared),
        symbol=spec.trader.symbol,
        brick_size_dollars=spec.brick_size_dollars,
        brick_count=brick_count,
        renko_version="renko_builder.update_renko_build_state",
        events=tuple(events),
    )


def _renko_brick_from_row(*, spec: CandidateSpec, row: dict[str, Any]) -> RenkoBrick:
    close = float(row["brick_close"])
    direction = int(row["direction"])
    open_price = close - (direction * float(spec.brick_size_dollars))
    return RenkoBrick(
        timestamp=pd.Timestamp(row["timestamp"]),
        symbol=spec.trader.symbol.upper(),
        open=open_price,
        close=close,
        direction=direction,
        run_len=int(row["run_len"]),
        brick_size=float(spec.brick_size_dollars),
        trader_id=spec.trader.trader_id,
    )


def _actions_from_cached_renko_event(
    *,
    strategy: Any,
    tick: MarketTick,
    event: CachedRenkoObservation,
) -> tuple[tuple[RenkoBrick, ...], list[tuple[Signal, OrderIntent]]]:
    emitted: list[tuple[Signal, OrderIntent]] = []
    force_exit = strategy._force_flat_action(timestamp=tick.timestamp, price=tick.price)
    if force_exit is not None:
        emitted.append(force_exit)
    strategy.processed_bricks += len(event.raw_bricks)
    for raw_brick in event.raw_bricks:
        brick_actions = strategy._on_brick(raw_brick)
        emitted.extend(brick_actions)
        if strategy.config.strategy_type == "renko_tp_sl" and brick_actions:
            break
    return event.renko_bricks, emitted


def _base_row(*, config: OptimizationConfig, spec: CandidateSpec, prepared: PreparedSource) -> dict[str, Any]:
    return {
        "optimization_id": config.optimization_id,
        "candidate_id": spec.candidate_id,
        "candidate_type": spec.candidate_type,
        "trader_id": spec.trader.trader_id,
        "strategy_name": spec.trader.strategy_name,
        "strategy_family": spec.strategy_parameters.strategy_family,
        "strategy_type": spec.strategy_parameters.strategy_type,
        "production_strategy_type": spec.trader.strategy_type,
        "symbol": spec.trader.symbol,
        "entry_confirmation": spec.strategy_parameters.entry_confirmation,
        "entry_bricks": spec.strategy_parameters.entry_bricks,
        "take_profit_confirmation": spec.strategy_parameters.take_profit_confirmation,
        "take_profit_bricks": spec.strategy_parameters.take_profit_bricks if spec.strategy_parameters.take_profit_bricks is not None else "UNKNOWN",
        "stop_loss_confirmation": spec.strategy_parameters.stop_loss_confirmation,
        "stop_loss_bricks": spec.strategy_parameters.stop_loss_bricks if spec.strategy_parameters.stop_loss_bricks is not None else "UNKNOWN",
        "direct_reversal_enabled": spec.strategy_parameters.direct_reversal_enabled,
        "exits_flat_before_reentry": spec.strategy_parameters.exits_flat_before_reentry,
        "parameter_grid_mode": config.parameter_grid_mode,
        "brick_pct": _num(spec.brick_pct),
        "brick_size_dollars": _num(spec.brick_size_dollars),
        "reference_price": _num(spec.reference_price),
        "reference_price_timestamp": spec.reference_price_timestamp.isoformat(),
        "reference_price_policy": spec.reference_price_policy,
        "rounding_policy": spec.rounding_policy,
        "source_preparation_id": prepared.preparation_id,
        "source_combined_hash": prepared.source_combined_hash,
        "configuration_hash": None,
        "replay_fingerprint": None,
        "market_stream_fingerprint": (prepared.market_stream_contract or {}).get("fingerprint")
        if isinstance(prepared.market_stream_contract, dict)
        else None,
        "market_stream_contract_version": (prepared.market_stream_contract or {}).get("version")
        if isinstance(prepared.market_stream_contract, dict)
        else None,
        "renko_strategy_fingerprint": None,
        "renko_strategy_contract_version": None,
        "promotion_fingerprint": None,
        "promotion_contract_version": None,
        "approved_canonical_path": bool(prepared.approved_canonical_path),
        "promotable_contract_metadata": False,
        "candidate_status": "SKIPPED",
        "result_quality": "UNKNOWN",
        "execution_model": config.execution_model,
        "partial_fill_policy": "NOT_MODELED",
        "pending_order_policy": "Pending at end and not force-filled",
        "signal_to_order_latency_ms": "UNKNOWN",
        "spread": "UNKNOWN",
        "slippage_per_share_dollars": "UNKNOWN",
        "slippage_bps": "UNKNOWN",
        "fees_dollars": "UNKNOWN",
        "total_execution_drag_dollars": "UNKNOWN",
        "total_execution_drag_percentage_points": "UNKNOWN",
        "position_sizing_mode": spec.position_sizing_mode,
        "production_config_quantity": _num(spec.production_config_quantity),
        "research_quantity": _num(spec.research_quantity),
        "quantity": _num(spec.research_quantity),
        "allocated_capital_dollars": _num(spec.allocated_capital),
        "unused_cash_dollars": _num(spec.unused_cash),
        "execution_cost_sensitive": "UNKNOWN",
        "execution_sensitivity_warning": "UNKNOWN",
        "sensitivity_rules_version": SENSITIVITY_RULES_VERSION,
        "sensitivity_index": "UNKNOWN",
        "sensitivity_classification": "UNKNOWN",
        "brick_sensitivity_index": "UNKNOWN",
        "brick_sensitivity_classification": "UNKNOWN",
        "tp_sensitivity_index": "UNKNOWN",
        "tp_sensitivity_classification": "UNKNOWN",
        "sl_sensitivity_index": "UNKNOWN",
        "sl_sensitivity_classification": "UNKNOWN",
        "entry_sensitivity_index": "UNKNOWN",
        "entry_sensitivity_classification": "UNKNOWN",
        "average_trade_pnl_per_share_dollars": "UNKNOWN",
        "pnl_per_closed_trade_dollars": "UNKNOWN",
        "execution_cost_sensitivity_reason": "UNKNOWN",
        "selected_output_directory": None,
        "error_type": None,
        "error_message": None,
    }


def _validate_tp_sl_position_invariant(*, spec: CandidateSpec, position: Any) -> None:
    if spec.strategy_parameters.strategy_type != "renko_tp_sl":
        return
    qty = Decimal(str(getattr(position, "qty", 0) or 0))
    target = Decimal(str(spec.research_quantity))
    if qty not in {Decimal("0"), target}:
        raise RuntimeError(f"renko_tp_sl position invariant failed: qty={qty} target={target}")


def _write_selected_outputs(
    *,
    config: OptimizationConfig,
    rows: list[dict[str, Any]],
    recommendations: dict[str, Any],
    specs_by_id: dict[str, tuple[CandidateSpec, PreparedSource, tuple[CanonicalObservation, ...]]],
    diagnostics_summary: dict[str, int],
) -> None:
    selected: dict[str, str] = {}
    for trader_id, payload in recommendations.items():
        labels = {
            "current_production": payload.get("current_production"),
            "best_overall": payload.get("best_overall"),
            "best_e1": payload.get("best_e1"),
            "best_e2": payload.get("best_e2"),
            "highest_net_pnl": payload.get("highest_net_pnl"),
            "best_pf": payload.get("best_pf"),
            "best_sharpe": payload.get("best_sharpe"),
            "best_sortino": payload.get("best_sortino"),
            "recommended_robust": payload.get("recommended_robust"),
        }
        for label, row in labels.items():
            if isinstance(row, dict) and row.get("candidate_id"):
                selected[str(row["candidate_id"])] = f"{_safe_path_name(trader_id)}/{label}"
    rows_by_id = {str(row.get("candidate_id")): row for row in rows}
    for candidate_id, relative_dir in sorted(selected.items(), key=lambda item: item[1]):
        if candidate_id not in specs_by_id or candidate_id not in rows_by_id:
            continue
        spec, prepared, observations = specs_by_id[candidate_id]
        target = config.output_path / "selected_runs" / relative_dir
        try:
            _execute_candidate(config=config, spec=spec, prepared=prepared, observations=observations, selected_output_dir=target)
            rows_by_id[candidate_id]["selected_output_directory"] = str(target)
        except Exception as exc:
            diagnostics_summary["selected_output_failures"] = diagnostics_summary.get("selected_output_failures", 0) + 1
            rows_by_id[candidate_id]["selected_output_error"] = f"{type(exc).__name__}: {exc}"


def _write_selected_candidate_outputs(
    output_dir: Path,
    *,
    row: dict[str, Any],
    fills: list[Any],
    signals: list[Any],
    orders: list[Any],
    renko_bricks: list[Any],
    position_transitions: list[Any],
    closed_trades: list[Any],
    equity_curve: list[Any],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "summary.json").write_text(json.dumps(_jsonable(row), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_jsonl(output_dir / "fills.jsonl", fills)
    _write_jsonl(output_dir / "signals.jsonl", signals)
    _write_jsonl(output_dir / "orders.jsonl", orders)
    _write_jsonl(output_dir / "renko_bricks.jsonl", renko_bricks)
    _write_jsonl(output_dir / "position_transitions.jsonl", position_transitions)
    _write_jsonl(output_dir / "closed_trades.jsonl", closed_trades)
    _write_csv(output_dir / "equity_curve.csv", [_jsonable(point) for point in equity_curve])


def _write_jsonl(path: Path, rows: list[Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(_jsonable(row), sort_keys=True) + "\n")


def _annotate_sensitivity(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_trader: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_trader.setdefault(str(row.get("trader_id")), []).append(row)
    annotated_by_id: dict[str, dict[str, Any]] = {}
    for trader_rows in by_trader.values():
        grid = [row for row in trader_rows if row.get("candidate_type") == "BRICK_PERCENT_GRID" and row.get("candidate_status") == "COMPLETED"]
        raw_by_dimension = {
            "brick": _raw_sensitivity_by_id(grid, "brick"),
            "tp": _raw_sensitivity_by_id(grid, "tp"),
            "sl": _raw_sensitivity_by_id(grid, "sl"),
            "entry": _raw_sensitivity_by_id(grid, "entry"),
        }
        max_by_dimension = {
            dimension: max(raw.values(), default=Decimal("1")) or Decimal("1")
            for dimension, raw in raw_by_dimension.items()
        }
        for row in trader_rows:
            enriched = dict(row)
            if row.get("candidate_type") == "BRICK_PERCENT_GRID" and row.get("candidate_status") == "COMPLETED":
                indexes = {
                    dimension: raw_by_dimension[dimension].get(str(row.get("candidate_id")), Decimal("0")) / max_by_dimension[dimension]
                    for dimension in raw_by_dimension
                }
                index = max(indexes.values(), default=Decimal("0"))
                enriched["sensitivity_rules_version"] = SENSITIVITY_RULES_VERSION
                enriched["sensitivity_index"] = _num(index)
                enriched["sensitivity_classification"] = _sensitivity_classification(
                    sensitivity_index=index,
                    local_pnl_stddev=Decimal(str(row.get("local_pnl_stddev") or 0)),
                    local_profitable_pct=Decimal(str(row.get("local_profitable_pct") or 0)),
                    isolated=row.get("isolated_spike_flag") is True,
                    plateau=row.get("stable_plateau_flag") is True,
                )
                for dimension, dimension_index in indexes.items():
                    enriched[f"{dimension}_sensitivity_index"] = _num(dimension_index)
                    enriched[f"{dimension}_sensitivity_classification"] = _basic_sensitivity_classification(dimension_index)
            annotated_by_id[str(row.get("candidate_id"))] = enriched
    return [annotated_by_id.get(str(row.get("candidate_id")), row) for row in rows]


def _raw_sensitivity_by_id(grid: list[dict[str, Any]], dimension: str) -> dict[str, Decimal]:
    result: dict[str, Decimal] = {}
    for row in grid:
        pct = Decimal(str(row.get("brick_pct")))
        center_pnl = Decimal(str(row.get("net_pnl_dollars") or 0))
        slopes = []
        for candidate in grid:
            if not _same_sensitivity_slice(row, candidate, dimension):
                continue
            if dimension == "brick":
                other_pct = Decimal(str(candidate.get("brick_pct")))
                distance = abs(other_pct - pct)
                if Decimal("0") < distance <= Decimal("0.05"):
                    slopes.append(abs(Decimal(str(candidate.get("net_pnl_dollars") or 0)) - center_pnl) / distance)
            else:
                if str(candidate.get("candidate_id")) != str(row.get("candidate_id")):
                    slopes.append(abs(Decimal(str(candidate.get("net_pnl_dollars") or 0)) - center_pnl))
        result[str(row.get("candidate_id"))] = sum(slopes, Decimal("0")) / Decimal(len(slopes)) if slopes else Decimal("0")
    return result


def _same_sensitivity_slice(row: dict[str, Any], candidate: dict[str, Any], dimension: str) -> bool:
    row_entry, row_tp, row_sl = _row_parameter_key(row)
    candidate_entry, candidate_tp, candidate_sl = _row_parameter_key(candidate)
    same_pct = Decimal(str(row.get("brick_pct"))) == Decimal(str(candidate.get("brick_pct")))
    if dimension == "brick":
        return (row_entry, row_tp, row_sl) == (candidate_entry, candidate_tp, candidate_sl)
    if dimension == "tp":
        return row_entry == candidate_entry and row_sl == candidate_sl and same_pct
    if dimension == "sl":
        return row_entry == candidate_entry and row_tp == candidate_tp and same_pct
    if dimension == "entry":
        return row_tp == candidate_tp and row_sl == candidate_sl and same_pct
    raise ValueError(f"Unsupported sensitivity dimension: {dimension}")


def _sensitivity_classification(
    *,
    sensitivity_index: Decimal,
    local_pnl_stddev: Decimal,
    local_profitable_pct: Decimal,
    isolated: bool,
    plateau: bool,
) -> str:
    if isolated or sensitivity_index >= Decimal("0.75"):
        return "VERY_HIGH"
    if sensitivity_index >= Decimal("0.50"):
        return "HIGH"
    if sensitivity_index >= Decimal("0.25") or (not plateau and local_profitable_pct < Decimal("60")) or local_pnl_stddev > Decimal("0"):
        return "MODERATE"
    return "LOW"


def _basic_sensitivity_classification(sensitivity_index: Decimal) -> str:
    if sensitivity_index >= Decimal("0.75"):
        return "VERY_HIGH"
    if sensitivity_index >= Decimal("0.50"):
        return "HIGH"
    if sensitivity_index >= Decimal("0.25"):
        return "MODERATE"
    return "LOW"


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    keys = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def _jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return _jsonable(asdict(value))
    if isinstance(value, dict):
        return {key: _jsonable(val) for key, val in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _safe_path_name(value: str) -> str:
    return "".join(char if char.isalnum() or char in "._-" else "_" for char in value)[:120]


def _metrics_row(metrics: dict[str, Any]) -> dict[str, Any]:
    return {
        "initial_capital_dollars": metrics["initial_capital_dollars"],
        "ending_equity_dollars": metrics["ending_equity_dollars"],
        "realized_pnl_dollars": metrics["realized_pnl_dollars"],
        "unrealized_pnl_dollars": metrics["unrealized_pnl_dollars"],
        "net_pnl_dollars": metrics["net_pnl_dollars"],
        "gross_profit_dollars": metrics["gross_profit_dollars"],
        "gross_loss_dollars": metrics["gross_loss_dollars"],
        "strategy_return_pct": metrics["strategy_return_pct"],
        "winning_trades": metrics["winning_trades"],
        "losing_trades": metrics["losing_trades"],
        "win_rate_pct": metrics["win_rate_pct"],
        "profit_factor": metrics["profit_factor"],
        "average_trade_pnl_dollars": metrics["average_trade_pnl_dollars"],
        "average_trade_return_pct": metrics["average_trade_return_pct"],
        "average_winner_dollars": metrics["average_winner_dollars"],
        "average_winner_pct": metrics["average_winner_pct"],
        "average_loser_dollars": metrics["average_loser_dollars"],
        "average_loser_pct": metrics["average_loser_pct"],
        "largest_winner_dollars": metrics["largest_winner_dollars"],
        "largest_loser_dollars": metrics["largest_loser_dollars"],
        "breakeven_trades": "UNKNOWN",
        "median_trade_pnl_dollars": "UNKNOWN",
        "median_trade_return_pct": "UNKNOWN",
        "average_holding_seconds": "UNKNOWN",
        "median_holding_seconds": "UNKNOWN",
        "max_holding_seconds": "UNKNOWN",
        "max_drawdown_dollars": metrics["max_drawdown_dollars"],
        "max_drawdown_pct": metrics["max_drawdown_pct"],
        "drawdown_peak_timestamp": metrics["drawdown_peak_timestamp"],
        "drawdown_trough_timestamp": metrics["drawdown_trough_timestamp"],
        "drawdown_recovery_timestamp": metrics["drawdown_recovery_timestamp"],
        "sharpe_ratio": metrics["sharpe_ratio"],
        "sortino_ratio": metrics["sortino_ratio"],
        "downside_deviation": "UNKNOWN",
        "return_volatility": "UNKNOWN",
        "exposure_pct": metrics["average_exposure_pct"],
        "long_exposure_pct": metrics["long_exposure_pct"],
        "short_exposure_pct": metrics["short_exposure_pct"],
        "flat_exposure_pct": metrics["flat_exposure_pct"],
        "benchmark_entry_price": metrics["benchmark_entry_price"],
        "benchmark_exit_price": metrics["benchmark_exit_price"],
        "benchmark_quantity": metrics["benchmark_quantity"],
        "benchmark_entry_value_dollars": metrics["benchmark_entry_value_dollars"],
        "benchmark_exit_value_dollars": metrics["benchmark_exit_value_dollars"],
        "buy_hold_pnl_dollars": metrics["buy_hold_pnl_dollars"],
        "buy_hold_return_pct": metrics["buy_hold_return_pct"],
        "buy_hold_ending_equity_dollars": metrics["buy_hold_ending_equity_dollars"],
        "buy_hold_return_on_total_equity_pct": metrics["buy_hold_return_on_total_equity_pct"],
        "buy_hold_return_on_allocated_capital_pct": metrics["buy_hold_return_on_allocated_capital_pct"],
        "strategy_outperformance_dollars": metrics["strategy_outperformance_dollars"],
        "strategy_outperformance_percentage_points": metrics["strategy_outperformance_percentage_points"],
        "cagr": metrics["cagr"],
        "cagr_pct": metrics["cagr"],
        "cagr_label": metrics["cagr_label"],
        "annualized_equivalent_pct": "UNKNOWN",
        "annualized_volatility_pct": "UNKNOWN",
        "order_to_fill_latency_mean_ms": _seconds_to_ms(metrics["order_to_fill_latency_mean_seconds"]),
        "order_to_fill_latency_median_ms": _seconds_to_ms(metrics["order_to_fill_latency_median_seconds"]),
        "order_to_fill_latency_p95_ms": _seconds_to_ms(metrics["order_to_fill_latency_p95_seconds"]),
        "average_deployed_capital_dollars": metrics["average_deployed_capital_dollars"],
        "maximum_deployed_capital_dollars": metrics["maximum_deployed_capital_dollars"],
        "minimum_deployed_capital_dollars": metrics["minimum_deployed_capital_dollars"],
        "average_cash_balance_dollars": metrics["average_cash_balance_dollars"],
        "minimum_cash_balance_dollars": metrics["minimum_cash_balance_dollars"],
        "average_position_value_dollars": metrics["average_position_value_dollars"],
        "maximum_position_value_dollars": metrics["maximum_position_value_dollars"],
        "average_exposure_pct": metrics["average_exposure_pct"],
        "maximum_exposure_pct": metrics["maximum_exposure_pct"],
        "capital_utilization_pct": metrics["capital_utilization_pct"],
        "pnl_per_share_dollars": metrics["pnl_per_share_dollars"],
        "pnl_on_allocated_capital_dollars": metrics["pnl_on_allocated_capital_dollars"],
        "return_on_allocated_capital_pct": metrics["return_on_allocated_capital_pct"],
        "return_on_total_account_equity_pct": metrics["return_on_total_account_equity_pct"],
        "turnover_dollars": metrics["turnover_dollars"],
        "turnover_multiple": metrics["turnover_multiple"],
        "average_trade_size_dollars": metrics["average_trade_size_dollars"],
    }


def _annualized_session_row(*, equity_curve: list[EquityPoint], initial_capital: Decimal) -> dict[str, Any]:
    base = {
        "session_count": 0,
        "profitable_sessions": 0,
        "losing_sessions": 0,
        "cagr": "UNKNOWN",
        "cagr_pct": "UNKNOWN",
        "cagr_label": "ANNUALIZED EQUIVALENT FROM SESSION RETURNS - NOT A FORECAST",
        "annualized_equivalent_pct": "UNKNOWN",
        "sharpe_ratio": "UNKNOWN",
        "sortino_ratio": "UNKNOWN",
        "annualized_volatility_pct": "UNKNOWN",
        "return_volatility": "UNKNOWN",
        "downside_deviation": "UNKNOWN",
    }
    if not equity_curve or initial_capital <= 0:
        return base

    last_equity_by_session: dict[str, Decimal] = {}
    for point in equity_curve:
        last_equity_by_session[point.timestamp.date().isoformat()] = Decimal(str(point.equity))
    if not last_equity_by_session:
        return base

    returns: list[float] = []
    previous_equity = Decimal(str(initial_capital))
    profitable = 0
    losing = 0
    for session in sorted(last_equity_by_session):
        ending_equity = last_equity_by_session[session]
        pnl = ending_equity - previous_equity
        if pnl > 0:
            profitable += 1
        elif pnl < 0:
            losing += 1
        returns.append(float(pnl / previous_equity) if previous_equity else 0.0)
        previous_equity = ending_equity

    session_count = len(returns)
    total_factor = float(previous_equity / initial_capital) if initial_capital else 0.0
    if total_factor > 0 and session_count > 0:
        cagr_pct = (math.pow(total_factor, 252.0 / session_count) - 1.0) * 100.0
    else:
        cagr_pct = None

    mean_return = sum(returns) / len(returns) if returns else 0.0
    volatility = pstdev(returns) if len(returns) > 1 else 0.0
    annualized_volatility_pct = volatility * math.sqrt(252.0) * 100.0 if volatility > 0 else None
    sharpe = (mean_return / volatility * math.sqrt(252.0)) if volatility > 0 else None
    downside_returns = [min(item, 0.0) for item in returns]
    downside_deviation = math.sqrt(sum(item * item for item in downside_returns) / len(downside_returns)) if downside_returns else 0.0
    sortino = (mean_return / downside_deviation * math.sqrt(252.0)) if downside_deviation > 0 else None
    base.update(
        {
            "session_count": session_count,
            "profitable_sessions": profitable,
            "losing_sessions": losing,
            "cagr": _num_or_unknown_float(cagr_pct),
            "cagr_pct": _num_or_unknown_float(cagr_pct),
            "annualized_equivalent_pct": _num_or_unknown_float(cagr_pct),
            "sharpe_ratio": _num_or_unknown_float(sharpe),
            "sortino_ratio": _num_or_unknown_float(sortino),
            "annualized_volatility_pct": _num_or_unknown_float(annualized_volatility_pct),
            "return_volatility": _num_or_unknown_float(annualized_volatility_pct),
            "downside_deviation": _num_or_unknown_float(downside_deviation * 100.0 if downside_deviation > 0 else None),
        }
    )
    return base


def _trade_audit_metrics_row(*, closed_trades: list[Any], position_samples: list[dict[str, Any]]) -> dict[str, Any]:
    pnl_values = [Decimal(str(trade.pnl)) for trade in closed_trades if getattr(trade, "pnl", None) is not None]
    return_values = [Decimal(str(trade.pnl_pct)) for trade in closed_trades if getattr(trade, "pnl_pct", None) is not None]
    holding_seconds = [
        Decimal(str((trade.exit_timestamp - trade.entry_timestamp).total_seconds()))
        for trade in closed_trades
        if getattr(trade, "entry_timestamp", None) is not None and getattr(trade, "exit_timestamp", None) is not None
    ]
    long_trades = [trade for trade in closed_trades if getattr(trade, "side", None) == "long"]
    short_trades = [trade for trade in closed_trades if getattr(trade, "side", None) == "short"]
    reason_counts: dict[str, int] = {}
    for trade in closed_trades:
        reason = str(getattr(trade, "reason", None) or "UNKNOWN")
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
    max_position_qty = max((abs(Decimal(str(sample.get("qty", 0) or 0))) for sample in position_samples), default=Decimal("0"))
    tp_exit_count = sum(count for reason, count in reason_counts.items() if reason.startswith("take_profit_"))
    sl_exit_count = sum(count for reason, count in reason_counts.items() if reason.startswith("stop_loss_"))
    return {
        "breakeven_trades": sum(1 for value in pnl_values if value == 0),
        "median_trade_pnl_dollars": _num_or_unknown_decimal(_median_decimal(pnl_values)),
        "median_trade_return_pct": _num_or_unknown_decimal(_median_decimal(return_values)),
        "average_holding_seconds": _num_or_unknown_decimal(_average_decimal(holding_seconds)),
        "median_holding_seconds": _num_or_unknown_decimal(_median_decimal(holding_seconds)),
        "max_holding_seconds": _num_or_unknown_decimal(max(holding_seconds) if holding_seconds else None),
        "long_trade_count": len(long_trades),
        "short_trade_count": len(short_trades),
        "long_pnl_dollars": _num(sum((Decimal(str(trade.pnl)) for trade in long_trades if getattr(trade, "pnl", None) is not None), Decimal("0"))),
        "short_pnl_dollars": _num(sum((Decimal(str(trade.pnl)) for trade in short_trades if getattr(trade, "pnl", None) is not None), Decimal("0"))),
        "max_position_quantity": _num(max_position_qty),
        "take_profit_exit_count": tp_exit_count,
        "stop_loss_exit_count": sl_exit_count,
        "tp_sl_exit_reason_counts": json.dumps(reason_counts, sort_keys=True),
    }


def _cross_asset_summary(rows: list[dict[str, Any]], recommendations: dict[str, Any]) -> dict[str, Any]:
    recommended_ids = {
        payload["recommended_robust"]["candidate_id"]
        for payload in recommendations.values()
        if isinstance(payload.get("recommended_robust"), dict) and payload["recommended_robust"]
    }
    selected = [row for row in rows if row.get("candidate_id") in recommended_ids]
    total_initial = sum(Decimal(str(row.get("initial_capital_dollars") or 0)) for row in selected)
    combined_allocated = sum(Decimal(str(row.get("allocated_capital_dollars") or 0)) for row in selected)
    combined_unused = sum(Decimal(str(row.get("unused_cash_dollars") or 0)) for row in selected)
    combined_net = sum(Decimal(str(row.get("net_pnl_dollars") or 0)) for row in selected)
    combined_buy_hold = sum(Decimal(str(row.get("buy_hold_pnl_dollars") or 0)) for row in selected)
    weighted_exposure = sum(Decimal(str(row.get("average_exposure_pct") or 0)) * Decimal(str(row.get("initial_capital_dollars") or 0)) for row in selected)
    return {
        "combined_allocated_capital_dollars": _num(combined_allocated),
        "combined_unused_cash_dollars": _num(combined_unused),
        "combined_net_pnl_dollars": _num(combined_net),
        "combined_ending_equity_dollars": _num(total_initial + combined_net),
        "combined_strategy_return_on_total_equity_pct": _num(combined_net / total_initial * Decimal("100")) if total_initial else "UNKNOWN",
        "combined_return_on_allocated_capital_pct": _num(combined_net / combined_allocated * Decimal("100")) if combined_allocated else "UNKNOWN",
        "combined_buy_hold_pnl_dollars": _num(combined_buy_hold),
        "combined_outperformance_dollars": _num(sum(Decimal(str(row.get("strategy_outperformance_dollars") or 0)) for row in selected)),
        "equal_weight_portfolio_return_pct": _num(sum(Decimal(str(row.get("strategy_return_pct") or 0)) for row in selected) / Decimal(len(selected))) if selected else "UNKNOWN",
        "weighted_average_exposure_pct": _num(weighted_exposure / total_initial) if total_initial else "UNKNOWN",
        "aggregate_turnover_dollars": _num(sum(Decimal(str(row.get("turnover_dollars") or 0)) for row in selected)),
        "aggregate_drawdown": "UNKNOWN",
        "asset_count": len(selected),
        "note": "Percent returns are averaged equal-weight; percentages are not summed.",
    }


def _candidate_id(*parts: Any) -> str:
    return "cand_" + sha256("|".join(str(item) for item in parts).encode("utf-8")).hexdigest()[:24]


def _production_strategy_parameters(trader: TraderSelection) -> StrategyOptimizationParameters:
    entry_bricks = int(trader.strategy_config.get("entry_bricks") or 1)
    entry_confirmation = "E1" if entry_bricks == 1 else "E2"
    if trader.strategy_type == "renko_tp_sl":
        return StrategyOptimizationParameters(
            strategy_family="FLATTENING_TP_SL_STRATEGY",
            strategy_type="renko_tp_sl",
            entry_confirmation=entry_confirmation,
            take_profit_bricks=int(trader.strategy_config.get("take_profit_bricks") or 1),
            stop_loss_bricks=int(trader.strategy_config.get("stop_loss_bricks") or 1),
        )
    return StrategyOptimizationParameters(
        strategy_family="DIRECT_REVERSAL_BASELINE",
        strategy_type="renko_reversal",
        entry_confirmation=entry_confirmation,
    )


def _row_parameter_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("entry_confirmation") or "UNKNOWN"),
        str(row.get("take_profit_bricks") or "UNKNOWN"),
        str(row.get("stop_loss_bricks") or "UNKNOWN"),
    )


def _seconds_to_ms(value: Any) -> Any:
    if value == "UNKNOWN":
        return "UNKNOWN"
    return float(Decimal(str(value)) * Decimal("1000"))


def _position_allocation(
    *,
    mode: str,
    production_quantity: Decimal,
    initial_capital: Decimal,
    reference_price: Decimal,
) -> tuple[Decimal, Decimal, Decimal]:
    if reference_price <= 0:
        raise ValueError("Reference price must be positive for candidate sizing.")
    if mode == "CAPITAL_ALLOCATED_WHOLE_SHARES_V1":
        quantity = (initial_capital / reference_price).to_integral_value(rounding=ROUND_FLOOR)
    elif mode == "PRODUCTION_CONFIG_QUANTITY_V1":
        quantity = production_quantity
    else:
        raise ValueError(f"Unsupported position sizing mode: {mode}")
    if quantity < 1:
        raise ValueError("Research quantity must be at least one whole share.")
    allocated = quantity * reference_price
    if allocated > initial_capital and mode == "CAPITAL_ALLOCATED_WHOLE_SHARES_V1":
        raise ValueError("Allocated capital exceeds initial capital.")
    return quantity, allocated, initial_capital - allocated


def _capital_row(*, config: OptimizationConfig, spec: CandidateSpec, metrics: dict[str, Any]) -> dict[str, Any]:
    avg_trade = _dec_or_zero(metrics.get("average_trade_pnl_dollars"))
    per_share_avg = avg_trade / spec.research_quantity if spec.research_quantity else Decimal("0")
    closed_trades = _dec_or_zero(metrics.get("closed_trades"))
    net_per_trade = _dec_or_zero(metrics.get("net_pnl_dollars")) / closed_trades if closed_trades else Decimal("0")
    execution_sensitive = bool(
        closed_trades
        and (abs(per_share_avg) < config.minimum_average_trade_pnl_per_share or abs(net_per_trade) < config.minimum_average_trade_pnl_per_share)
    )
    warning = (
        f"average_trade_pnl_per_share_or_net_per_trade_below_{config.minimum_average_trade_pnl_per_share}"
        if execution_sensitive
        else None
    )
    return {
        "position_sizing_mode": spec.position_sizing_mode,
        "production_config_quantity": _num(spec.production_config_quantity),
        "research_quantity": _num(spec.research_quantity),
        "quantity": _num(spec.research_quantity),
        "allocated_capital_dollars": _num(spec.allocated_capital),
        "unused_cash_dollars": _num(spec.unused_cash),
        "execution_cost_sensitive": execution_sensitive,
        "execution_sensitivity_warning": warning,
        "execution_cost_sensitivity_reason": warning,
        "average_trade_pnl_per_share_dollars": _num(per_share_avg),
        "pnl_per_closed_trade_dollars": _num(net_per_trade) if closed_trades else "UNKNOWN",
        "sensitivity_rules_version": SENSITIVITY_RULES_VERSION,
        "sensitivity_index": "UNKNOWN",
        "sensitivity_classification": "UNKNOWN",
        "brick_sensitivity_index": "UNKNOWN",
        "brick_sensitivity_classification": "UNKNOWN",
        "tp_sensitivity_index": "UNKNOWN",
        "tp_sensitivity_classification": "UNKNOWN",
        "sl_sensitivity_index": "UNKNOWN",
        "sl_sensitivity_classification": "UNKNOWN",
        "entry_sensitivity_index": "UNKNOWN",
        "entry_sensitivity_classification": "UNKNOWN",
    }


def _num(value: Any) -> Any:
    if value in {None, "UNKNOWN"}:
        return "UNKNOWN"
    dec = Decimal(str(value))
    if dec == dec.to_integral_value():
        return int(dec)
    return float(dec)


def _num_or_unknown_float(value: float | None) -> Any:
    if value is None or not math.isfinite(value):
        return "UNKNOWN"
    return _num(value)


def _num_or_unknown_decimal(value: Decimal | None) -> Any:
    if value is None:
        return "UNKNOWN"
    return _num(value)


def _average_decimal(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, Decimal("0")) / Decimal(len(values))


def _median_decimal(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return Decimal(str(median(values)))


def _dec_or_zero(value: Any) -> Decimal:
    if value in {None, "", "UNKNOWN"}:
        return Decimal("0")
    return Decimal(str(value))
