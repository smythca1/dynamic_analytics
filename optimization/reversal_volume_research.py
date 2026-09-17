from __future__ import annotations

import copy
import csv
import math
import json
from dataclasses import asdict, dataclass, is_dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_FLOOR
from hashlib import sha256
from pathlib import Path
from statistics import median, pstdev
from typing import Any

import pandas as pd

from renko_builder import update_renko_build_state
from replay.execution import NextObservationExecution
from replay.historical_source import stream_historical_source
from replay.metrics import build_metrics
from replay.models import ReplayConfig
from shared.config.trader_config import build_strategy_config, load_production_renko_config
from silver.accounting import AccountingLedger, AccountingSnapshot
from silver.accounting.models import EquityPoint
from silver.market import CanonicalObservation, evaluate_trade_eligibility, renko_strategy_contract
from strategy_engine import RenkoStrategyConfig, RenkoStrategyEngine
from trading_events import MarketTick, OrderIntent, RenkoBrick, Signal

from .coverage import CoverageReport, validate_coverage, write_coverage_reports
from .market_calendar import EASTERN, latest_completed_nyse_sessions, nyse_session_window
from .models import PreparedSource, TraderSelection
from .source_inventory import resolve_local_source_path
from .source_preparation import load_prepared_observations, prepare_source


REVERSAL_VOLUME_RESEARCH_VERSION = "research.reversal_volume_zscore.v1"
NO_FILTER_LABEL = "NO_FILTER"
VOLUME_STATUS_OK = "OK"
VOLUME_STATUS_INSUFFICIENT_BASELINE = "INSUFFICIENT_BASELINE_HISTORY"
VOLUME_STATUS_ZERO_STD = "ZERO_BASELINE_STD"
VOLUME_STATUS_MISSING = "MISSING_VOLUME_SNAPSHOT"


DEFAULT_VOLUME_WINDOWS_SECONDS = (5, 15, 30, 60)
DEFAULT_VOLUME_THRESHOLDS: tuple[Decimal | None, ...] = (
    None,
    Decimal("0.00"),
    Decimal("0.25"),
    Decimal("0.50"),
    Decimal("0.75"),
    Decimal("1.00"),
    Decimal("1.25"),
    Decimal("1.50"),
    Decimal("1.75"),
    Decimal("2.00"),
    Decimal("2.50"),
    Decimal("3.00"),
)


@dataclass(frozen=True)
class ReversalVolumeResearchConfig:
    research_id: str
    config_path: str
    session_dates: tuple[date, ...] = ()
    source_root: str = "artifacts/research/raw-data/raw/alpaca/sip"
    output_directory: str = "artifacts/research/reversal_volume_zscore"
    prepared_output_root: str = "artifacts/research/prepared_market"
    symbols: tuple[str, ...] = ()
    baseline_session_dates: tuple[date, ...] = ()
    initial_capital: Decimal | int | float | str = Decimal("10000")
    position_sizing_mode: str = "PRODUCTION_CONFIG_QUANTITY_V1"
    brick_start_pct: Decimal | int | float | str = Decimal("0.10")
    brick_end_pct: Decimal | int | float | str = Decimal("2.00")
    brick_step_pct_points: Decimal | int | float | str = Decimal("0.02")
    candidate_pcts: tuple[Decimal | int | float | str, ...] | None = None
    volume_windows_seconds: tuple[int, ...] = DEFAULT_VOLUME_WINDOWS_SECONDS
    volume_thresholds: tuple[Decimal | int | float | str | None, ...] = DEFAULT_VOLUME_THRESHOLDS
    minimum_volume_baseline_sessions: int = 5
    minimum_closed_trades: int = 20
    validate_market_data_coverage: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbols", tuple(symbol.upper() for symbol in self.symbols))
        object.__setattr__(self, "baseline_session_dates", tuple(sorted(set(self.baseline_session_dates))))
        object.__setattr__(self, "session_dates", tuple(sorted(set(self.session_dates))))
        object.__setattr__(self, "initial_capital", Decimal(str(self.initial_capital)))
        object.__setattr__(self, "brick_start_pct", Decimal(str(self.brick_start_pct)))
        object.__setattr__(self, "brick_end_pct", Decimal(str(self.brick_end_pct)))
        object.__setattr__(self, "brick_step_pct_points", Decimal(str(self.brick_step_pct_points)))
        if self.candidate_pcts is not None:
            object.__setattr__(self, "candidate_pcts", tuple(Decimal(str(item)) for item in self.candidate_pcts))
        thresholds = tuple(None if item is None else Decimal(str(item)) for item in self.volume_thresholds)
        if not thresholds or thresholds[0] is not None or any(item is None for item in thresholds[1:]):
            raise ValueError("volume_thresholds must start with None/NO_FILTER and then numeric thresholds.")
        object.__setattr__(self, "volume_thresholds", thresholds)
        windows = tuple(int(item) for item in self.volume_windows_seconds)
        if not windows or any(item <= 0 for item in windows):
            raise ValueError("volume_windows_seconds must contain positive integer windows.")
        object.__setattr__(self, "volume_windows_seconds", windows)
        if self.minimum_volume_baseline_sessions < 1:
            raise ValueError("minimum_volume_baseline_sessions must be >= 1.")
        if self.minimum_closed_trades < 0:
            raise ValueError("minimum_closed_trades must be >= 0.")
        if self.brick_step_pct_points <= 0:
            raise ValueError("brick_step_pct_points must be > 0.")
        if self.position_sizing_mode not in {"PRODUCTION_CONFIG_QUANTITY_V1", "CAPITAL_ALLOCATED_WHOLE_SHARES_V1"}:
            raise ValueError(f"Unsupported position_sizing_mode: {self.position_sizing_mode}")

    @property
    def output_path(self) -> Path:
        root = Path(self.output_directory)
        return root if root.name == self.research_id else root / self.research_id


@dataclass(frozen=True)
class VolumeZSnapshot:
    timestamp: datetime
    symbol: str
    window_seconds: int
    observed_volume: Decimal
    baseline_sample_count: int
    baseline_mean_volume: Decimal | None
    baseline_std_volume: Decimal | None
    z_score: Decimal | None
    status: str


@dataclass(frozen=True)
class ReversalVolumeCandidateSpec:
    candidate_id: str
    trader: TraderSelection
    symbol: str
    brick_pct: Decimal
    brick_size_dollars: Decimal
    reference_price: Decimal
    reference_price_timestamp: datetime
    research_quantity: Decimal
    allocated_capital: Decimal
    unused_cash: Decimal
    volume_window_seconds: int
    volume_threshold_z: Decimal | None
    research_id: str

    @property
    def volume_threshold_label(self) -> str:
        return volume_threshold_label(self.volume_threshold_z)


@dataclass(frozen=True)
class ReversalVolumeCandidateRun:
    row: dict[str, Any]
    opportunities: tuple[dict[str, Any], ...]
    session_rows: tuple[dict[str, Any], ...]
    fills: tuple[Any, ...]
    signals: tuple[Signal, ...]
    orders: tuple[Any, ...]
    closed_trades: tuple[Any, ...]
    position_transitions: tuple[Any, ...]
    final_strategy_snapshot: dict[str, Any]
    final_accounting_snapshot: AccountingSnapshot


@dataclass(frozen=True)
class ReversalVolumeResearchResult:
    output_directory: str
    symbols: tuple[str, ...]
    session_dates: tuple[date, ...]
    baseline_session_dates: tuple[date, ...]
    prepared_sources: dict[str, PreparedSource]
    rows: tuple[dict[str, Any], ...]
    opportunity_count: int


def brick_grid(start: Decimal, end: Decimal, step: Decimal) -> tuple[Decimal, ...]:
    values: list[Decimal] = []
    current = start
    while current <= end:
        values.append(current.quantize(Decimal("0.01")))
        current += step
    return tuple(values)


def volume_threshold_label(threshold: Decimal | None) -> str:
    if threshold is None:
        return NO_FILTER_LABEL
    text = str(threshold.quantize(Decimal("0.01"))).replace(".", "p")
    return f"Z_GTE_{text}"


def run_reversal_volume_research(config: ReversalVolumeResearchConfig) -> ReversalVolumeResearchResult:
    session_dates = config.session_dates or latest_completed_nyse_sessions(now=datetime.now(timezone.utc), count=20)
    symbols = config.symbols or supported_production_renko_symbols(config.config_path)
    if not symbols:
        raise ValueError("No Renko symbols were selected for reversal volume research.")

    coverage_reports: list[CoverageReport] = []
    if config.validate_market_data_coverage:
        coverage_reports = _validate_source_coverage(
            symbols=symbols,
            sessions=tuple(sorted(set(session_dates) | set(config.baseline_session_dates))),
            source_root=config.source_root,
        )
        failed = [report for report in coverage_reports if report.status != "PASS"]
        if failed:
            first = failed[0]
            raise ValueError(
                "Market-data coverage validation failed before research sweep: "
                f"{first.symbol} {first.session_date.isoformat()} {','.join(first.failure_reasons)}"
            )

    traders = load_reversal_research_traders(config.config_path, symbols)
    traders_by_symbol = {trader.symbol: trader for trader in traders}
    missing_symbols = [symbol for symbol in symbols if symbol not in traders_by_symbol]
    if missing_symbols:
        raise ValueError("No Renko trader config found for symbols: " + ",".join(sorted(missing_symbols)))

    prepared_sources: dict[str, PreparedSource] = {}
    rows: list[dict[str, Any]] = []
    opportunity_rows: list[dict[str, Any]] = []
    session_rows: list[dict[str, Any]] = []
    for symbol in symbols:
        trader = traders_by_symbol[symbol]
        prepared = prepare_source(
            symbol=symbol,
            trader_id=trader.trader_id,
            session_dates=session_dates,
            source_root=config.source_root,
            output_root=config.prepared_output_root,
            run_id=f"{config.research_id}:{symbol}:prep",
        )
        prepared_sources[symbol] = prepared
        observations = load_prepared_observations(prepared.observations_path)
        volume_lookup = build_volume_z_lookup(
            symbol=symbol,
            evaluation_sessions=session_dates,
            baseline_sessions=config.baseline_session_dates,
            observations=observations,
            source_root=config.source_root,
            volume_windows_seconds=config.volume_windows_seconds,
            minimum_baseline_sessions=config.minimum_volume_baseline_sessions,
        )
        for spec in build_candidate_specs(config=config, trader=trader, prepared=prepared, observations=observations):
            candidate = run_candidate(
                config=config,
                spec=spec,
                prepared=prepared,
                observations=observations,
                volume_lookup=volume_lookup,
            )
            rows.append(candidate.row)
            opportunity_rows.extend(candidate.opportunities)
            session_rows.extend(candidate.session_rows)

    rows = annotate_candidate_rows(rows, minimum_closed_trades=config.minimum_closed_trades)
    _write_outputs(
        config=config,
        session_dates=session_dates,
        symbols=symbols,
        prepared_sources=prepared_sources,
        coverage_reports=coverage_reports,
        rows=rows,
        opportunities=opportunity_rows,
        session_rows=session_rows,
    )
    return ReversalVolumeResearchResult(
        output_directory=str(config.output_path),
        symbols=tuple(symbols),
        session_dates=tuple(session_dates),
        baseline_session_dates=config.baseline_session_dates,
        prepared_sources=prepared_sources,
        rows=tuple(rows),
        opportunity_count=len(opportunity_rows),
    )


def supported_production_renko_symbols(config_path: str | Path) -> tuple[str, ...]:
    data = json.loads(Path(config_path).read_text(encoding="utf-8"))
    symbols: list[str] = []
    fallback: list[str] = []
    for trader in data.get("traders", []):
        if not isinstance(trader, dict):
            continue
        try:
            strategy = build_strategy_config(trader)
        except Exception:
            continue
        if not isinstance(strategy, RenkoStrategyConfig):
            continue
        symbol = strategy.symbol.upper()
        if trader.get("enabled", True):
            symbols.append(symbol)
        fallback.append(symbol)
    selected = symbols or fallback
    return tuple(sorted(set(selected)))


def load_reversal_research_traders(config_path: str | Path, symbols: tuple[str, ...]) -> tuple[TraderSelection, ...]:
    data = json.loads(Path(config_path).read_text(encoding="utf-8"))
    requested = {symbol.upper() for symbol in symbols}
    by_symbol: dict[str, list[tuple[int, int, dict[str, Any], RenkoStrategyConfig]]] = {}
    for index, raw in enumerate(data.get("traders", [])):
        if not isinstance(raw, dict):
            continue
        try:
            strategy = build_strategy_config(raw)
        except Exception:
            continue
        if not isinstance(strategy, RenkoStrategyConfig):
            continue
        symbol = strategy.symbol.upper()
        if requested and symbol not in requested:
            continue
        priority = _research_trader_priority(raw, strategy)
        by_symbol.setdefault(symbol, []).append((priority, index, raw, strategy))

    selections: list[TraderSelection] = []
    for symbol in sorted(requested or by_symbol.keys()):
        candidates = by_symbol.get(symbol, [])
        if not candidates:
            continue
        _priority, _index, raw, base_strategy = sorted(candidates, key=lambda item: (item[0], item[1]))[0]
        research_strategy = _research_reversal_strategy_config(base_strategy)
        loaded = load_production_renko_config(config_path, str(raw["id"]))
        selections.append(
            TraderSelection(
                trader_id=str(raw["id"]),
                symbol=symbol,
                strategy_name=research_strategy.strategy_name,
                strategy_type="renko_reversal",
                strategy_config=dict(research_strategy.__dict__),
                production_metadata={
                    **loaded.metadata,
                    "production_config_hash": loaded.config_hash,
                    "research_source_strategy_type": base_strategy.strategy_type,
                    "active_date_gate_policy": "preserved_for_reversal_sources_converted_sources_clear_active_dates",
                },
                production_config_hash=loaded.config_hash,
            )
        )
    return tuple(selections)


def build_candidate_specs(
    *,
    config: ReversalVolumeResearchConfig,
    trader: TraderSelection,
    prepared: PreparedSource,
    observations: tuple[CanonicalObservation, ...],
) -> tuple[ReversalVolumeCandidateSpec, ...]:
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
    pcts = config.candidate_pcts or brick_grid(config.brick_start_pct, config.brick_end_pct, config.brick_step_pct_points)
    specs: list[ReversalVolumeCandidateSpec] = []
    for pct in pcts:
        brick_size = reference_price * pct / Decimal("100")
        for window_seconds in config.volume_windows_seconds:
            for threshold in config.volume_thresholds:
                candidate_id = _candidate_id(
                    config.research_id,
                    REVERSAL_VOLUME_RESEARCH_VERSION,
                    trader.trader_id,
                    trader.symbol,
                    str(pct),
                    str(window_seconds),
                    volume_threshold_label(threshold),
                    config.position_sizing_mode,
                    prepared.source_combined_hash,
                )
                specs.append(
                    ReversalVolumeCandidateSpec(
                        candidate_id=candidate_id,
                        trader=trader,
                        symbol=trader.symbol,
                        brick_pct=pct,
                        brick_size_dollars=brick_size,
                        reference_price=reference_price,
                        reference_price_timestamp=reference.second_bucket,
                        research_quantity=research_qty,
                        allocated_capital=allocated_capital,
                        unused_cash=unused_cash,
                        volume_window_seconds=window_seconds,
                        volume_threshold_z=threshold,
                        research_id=config.research_id,
                    )
                )
    return tuple(specs)


def build_volume_z_lookup(
    *,
    symbol: str,
    evaluation_sessions: tuple[date, ...],
    baseline_sessions: tuple[date, ...],
    observations: tuple[CanonicalObservation, ...],
    source_root: str,
    volume_windows_seconds: tuple[int, ...],
    minimum_baseline_sessions: int,
) -> dict[int, dict[datetime, VolumeZSnapshot]]:
    if minimum_baseline_sessions < 1:
        raise ValueError("minimum_baseline_sessions must be >= 1.")
    eval_set = set(evaluation_sessions)
    all_sessions = tuple(sorted(set(evaluation_sessions) | set(baseline_sessions)))
    observation_offsets: dict[date, dict[datetime, int]] = {}
    for observation in observations:
        session = observation.second_bucket.astimezone(EASTERN).date()
        if session not in eval_set:
            continue
        window = nyse_session_window(session)
        offset = int((observation.second_bucket - window.start_utc).total_seconds())
        observation_offsets.setdefault(session, {})[observation.second_bucket] = offset

    history: dict[int, dict[int, list[Decimal]]] = {
        window_seconds: {} for window_seconds in volume_windows_seconds
    }
    lookup: dict[int, dict[datetime, VolumeZSnapshot]] = {
        window_seconds: {} for window_seconds in volume_windows_seconds
    }
    for session in all_sessions:
        second_volumes = _eligible_second_volumes(symbol=symbol, session=session, source_root=source_root)
        rolling_by_window = {
            window_seconds: _rolling_session_volume_by_offset(
                session=session,
                second_volumes=second_volumes,
                window_seconds=window_seconds,
            )
            for window_seconds in volume_windows_seconds
        }
        if session in eval_set:
            for timestamp, offset in observation_offsets.get(session, {}).items():
                for window_seconds in volume_windows_seconds:
                    observed = rolling_by_window[window_seconds].get(offset, Decimal("0"))
                    prior_values = history[window_seconds].get(offset, [])
                    lookup[window_seconds][timestamp] = _volume_z_snapshot(
                        symbol=symbol,
                        timestamp=timestamp,
                        window_seconds=window_seconds,
                        observed_volume=observed,
                        history=prior_values,
                        minimum_baseline_sessions=minimum_baseline_sessions,
                    )
        for window_seconds, values_by_offset in rolling_by_window.items():
            for offset, value in values_by_offset.items():
                history[window_seconds].setdefault(offset, []).append(value)
    return lookup


def run_candidate(
    *,
    config: ReversalVolumeResearchConfig,
    spec: ReversalVolumeCandidateSpec,
    prepared: PreparedSource,
    observations: tuple[CanonicalObservation, ...],
    volume_lookup: dict[int, dict[datetime, VolumeZSnapshot]],
) -> ReversalVolumeCandidateRun:
    if not observations:
        raise ValueError(f"No observations for {spec.symbol}.")
    strategy_config = _candidate_strategy_config(spec)
    strategy = RenkoStrategyEngine(RenkoStrategyConfig(**strategy_config))
    accounting = AccountingLedger(symbol=spec.symbol, starting_capital=config.initial_capital)
    execution = NextObservationExecution(run_id=spec.candidate_id, trader_id=spec.trader.trader_id)
    order_latencies: list[Decimal] = []
    renko_bricks: list[RenkoBrick] = []
    signals: list[Signal] = []
    orders: list[Any] = []
    fills: list[Any] = []
    equity_curve: list[EquityPoint] = []
    position_samples: list[dict[str, Any]] = []
    opportunity_rows: list[dict[str, Any]] = []
    open_opportunity_by_entry_fill_id: dict[str, int] = {}

    for observation in observations:
        for bundle in execution.fill_pending(observation=observation, position=accounting.position):
            order_latencies.append(bundle.latency_seconds)
            for fill in bundle.fills:
                fills.append(fill)
                before_closed_count = len(accounting.closed_trades)
                accounting.process_fill(fill)
                if fill.leg == "open":
                    opportunity_index = _open_opportunity_index_for_fill(fill, opportunity_rows)
                    if opportunity_index is not None:
                        open_opportunity_by_entry_fill_id[fill.stable_fill_id] = opportunity_index
                        opportunity_rows[opportunity_index]["actual_entry_timestamp"] = fill.timestamp.isoformat()
                        opportunity_rows[opportunity_index]["actual_entry_price"] = _num(fill.price)
                        opportunity_rows[opportunity_index]["entry_fill_id"] = fill.stable_fill_id
                if fill.leg == "close" and len(accounting.closed_trades) > before_closed_count:
                    closed_trade = accounting.closed_trades[-1]
                    _attach_closed_trade_to_opportunity(
                        closed_trade=closed_trade,
                        opportunities=opportunity_rows,
                        open_opportunity_by_entry_fill_id=open_opportunity_by_entry_fill_id,
                    )
                if fill.leg == "close" and accounting.position.side is None and hasattr(strategy, "mark_close_filled"):
                    strategy.mark_close_filled(
                        timestamp=fill.timestamp,
                        reason=fill.reason,
                        order_id=fill.order_id,
                        source=execution.version,
                    )

        snapshot = accounting.snapshot(mark_price=observation.close_price)
        _update_open_opportunity_excursion(
            position=accounting.position,
            mark_price=observation.close_price,
            opportunities=opportunity_rows,
            open_opportunity_by_entry_fill_id=open_opportunity_by_entry_fill_id,
        )
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
        processed = _process_research_tick(
            strategy=strategy,
            tick=tick,
            observation=observation,
            spec=spec,
            volume_lookup=volume_lookup,
            action_sequence_start=len(signals),
            opportunity_sequence_start=len(opportunity_rows),
        )
        renko_bricks.extend(processed["renko_bricks"])
        for rejected in processed["rejected_opportunities"]:
            opportunity_rows.append(rejected)
        for signal, intent, opportunity in processed["accepted_actions"]:
            signal, intent = _stable_research_action_identifiers(
                spec=spec,
                observation=observation,
                action_sequence=len(signals) + 1,
                signal=signal,
                intent=intent,
            )
            opportunity["signal_id"] = signal.signal_id
            signals.append(signal)
            opportunity_rows.append(opportunity)
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
            opportunity_rows[-1]["order_id"] = order.order_id
            orders.append(order)
            if intent.close_position and hasattr(strategy, "mark_close_submitted"):
                strategy.mark_close_submitted(intent, response={"id": order.order_id})

    final_snapshot = accounting.snapshot(mark_price=observations[-1].close_price)
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
    row = _candidate_row(
        config=config,
        spec=spec,
        prepared=prepared,
        strategy_config=strategy_config,
        metrics=metrics,
        renko_bricks=renko_bricks,
        signals=signals,
        orders=orders,
        fills=fills,
        final_snapshot=final_snapshot,
        opportunities=opportunity_rows,
        canonical_observation_count=len(observations),
        pending_orders_at_end=len(execution.pending_orders),
    )
    sessions = _candidate_session_rows(spec=spec, closed_trades=list(final_snapshot.closed_trades), equity_curve=equity_curve)
    return ReversalVolumeCandidateRun(
        row=row,
        opportunities=tuple(opportunity_rows),
        session_rows=tuple(sessions),
        fills=tuple(fills),
        signals=tuple(signals),
        orders=tuple(orders),
        closed_trades=tuple(final_snapshot.closed_trades),
        position_transitions=tuple(final_snapshot.position_transitions),
        final_strategy_snapshot=strategy.snapshot(),
        final_accounting_snapshot=final_snapshot,
    )


def annotate_candidate_rows(rows: list[dict[str, Any]], *, minimum_closed_trades: int) -> list[dict[str, Any]]:
    threshold_order = {volume_threshold_label(threshold): index for index, threshold in enumerate(DEFAULT_VOLUME_THRESHOLDS)}
    by_control: dict[tuple[Any, Any, Any], dict[str, Any]] = {}
    by_key: dict[tuple[Any, Any, Any, Any], dict[str, Any]] = {}
    for row in rows:
        control_key = (row.get("symbol"), row.get("volume_window_seconds"), _dec(row.get("brick_pct")))
        if row.get("volume_threshold_label") == NO_FILTER_LABEL:
            by_control[control_key] = row
        by_key[(*control_key, row.get("volume_threshold_label"))] = row

    annotated: list[dict[str, Any]] = []
    for row in rows:
        enriched = dict(row)
        closed_trades = int(row.get("closed_trades") or 0)
        enriched["sample_quality"] = "DISCOVERY_QUALITY" if closed_trades >= minimum_closed_trades else "INSUFFICIENT_SAMPLE"
        control = by_control.get((row.get("symbol"), row.get("volume_window_seconds"), _dec(row.get("brick_pct"))))
        if control is not None and row.get("volume_threshold_label") != NO_FILTER_LABEL:
            enriched.update(_control_delta_row(control=control, candidate=row))
        else:
            enriched.update(_empty_control_delta_row())
        enriched.update(_neighbor_row(row, rows=rows, threshold_order=threshold_order))
        annotated.append(enriched)
    return annotated


def _process_research_tick(
    *,
    strategy: RenkoStrategyEngine,
    tick: MarketTick,
    observation: CanonicalObservation,
    spec: ReversalVolumeCandidateSpec,
    volume_lookup: dict[int, dict[datetime, VolumeZSnapshot]],
    action_sequence_start: int,
    opportunity_sequence_start: int,
) -> dict[str, Any]:
    accepted: list[tuple[Signal, OrderIntent, dict[str, Any]]] = []
    rejected: list[dict[str, Any]] = []
    renko_bricks: list[RenkoBrick] = []

    force_exit = strategy._force_flat_action(timestamp=tick.timestamp, price=tick.price)
    if force_exit is not None:
        signal, intent = force_exit
        signal, intent = _stable_research_action_identifiers(
            spec=spec,
            observation=observation,
            action_sequence=action_sequence_start + len(accepted) + 1,
            signal=signal,
            intent=intent,
        )
        accepted.append((signal, intent, _exit_opportunity_placeholder(spec=spec, signal=signal)))

    update_renko_build_state(
        strategy.renko_state,
        timestamps=[tick.timestamp],
        prices=[tick.price],
    )
    new_brick_rows = strategy.renko_state.bricks[strategy.processed_bricks :]
    strategy.processed_bricks = len(strategy.renko_state.bricks)
    renko_bricks.extend(strategy._build_renko_brick(row) for row in new_brick_rows)

    for raw_brick in new_brick_rows:
        trade_state_before = strategy.trade_state_snapshot()
        renko_state_identity = id(strategy.renko_state)
        processed_bricks_before_restore = strategy.processed_bricks
        brick_actions = strategy._on_brick(raw_brick)
        entry_actions = [action for action in brick_actions if action[0].signal_type == "entry"]
        if len(entry_actions) > 1:
            raise RuntimeError("Reversal volume research expected at most one entry action per Renko brick.")
        for signal, intent in brick_actions:
            if signal.signal_type != "entry":
                accepted.append((signal, intent, _exit_opportunity_placeholder(spec=spec, signal=signal)))
                continue
            volume = volume_lookup.get(spec.volume_window_seconds, {}).get(observation.second_bucket)
            decision = _volume_entry_decision(spec=spec, volume=volume)
            opportunity = _entry_opportunity_row(
                spec=spec,
                signal=signal,
                raw_brick=raw_brick,
                volume=volume,
                accepted=decision["accepted"],
                rejection_reason=decision["rejection_reason"],
                opportunity_sequence=opportunity_sequence_start + len(accepted) + len(rejected) + 1,
            )
            if decision["accepted"]:
                accepted.append((signal, intent, opportunity))
                continue
            _restore_rejected_entry_trade_state(
                strategy=strategy,
                trade_state_snapshot=trade_state_before,
                renko_state_identity=renko_state_identity,
                processed_bricks=processed_bricks_before_restore,
            )
            rejected.append(opportunity)
    return {
        "renko_bricks": tuple(renko_bricks),
        "accepted_actions": tuple(accepted),
        "rejected_opportunities": tuple(rejected),
    }


def _restore_rejected_entry_trade_state(
    *,
    strategy: RenkoStrategyEngine,
    trade_state_snapshot: dict[str, Any],
    renko_state_identity: int,
    processed_bricks: int,
) -> None:
    strategy.restore_trade_state(trade_state_snapshot)
    if id(strategy.renko_state) != renko_state_identity:
        raise RuntimeError("Rejected-entry restoration changed Renko builder state identity.")
    if strategy.processed_bricks != processed_bricks:
        raise RuntimeError("Rejected-entry restoration changed processed Renko brick progress.")


def _volume_entry_decision(*, spec: ReversalVolumeCandidateSpec, volume: VolumeZSnapshot | None) -> dict[str, Any]:
    if spec.volume_threshold_z is None:
        return {"accepted": True, "rejection_reason": None}
    if volume is None:
        return {"accepted": False, "rejection_reason": VOLUME_STATUS_MISSING}
    if volume.status != VOLUME_STATUS_OK:
        return {"accepted": False, "rejection_reason": volume.status}
    if volume.z_score is None:
        return {"accepted": False, "rejection_reason": VOLUME_STATUS_MISSING}
    if volume.z_score >= spec.volume_threshold_z:
        return {"accepted": True, "rejection_reason": None}
    return {"accepted": False, "rejection_reason": "VOLUME_Z_BELOW_THRESHOLD"}


def _entry_opportunity_row(
    *,
    spec: ReversalVolumeCandidateSpec,
    signal: Signal,
    raw_brick: dict[str, Any],
    volume: VolumeZSnapshot | None,
    accepted: bool,
    rejection_reason: str | None,
    opportunity_sequence: int,
) -> dict[str, Any]:
    brick_close = Decimal(str(raw_brick["brick_close"]))
    direction = int(raw_brick["direction"])
    brick_formed_at = pd.Timestamp(raw_brick["timestamp"]).to_pydatetime().astimezone(timezone.utc)
    previous_close = brick_close - (Decimal(direction) * spec.brick_size_dollars)
    formation_seconds = None
    if signal.timestamp is not None:
        formation_seconds = 0
    return {
        "research_id": spec.research_id,
        "candidate_id": spec.candidate_id,
        "opportunity_sequence": opportunity_sequence,
        "symbol": spec.symbol,
        "session": signal.timestamp.tz_convert(EASTERN).date().isoformat(),
        "timestamp": signal.timestamp.isoformat(),
        "direction": direction,
        "side": signal.side,
        "broker_side": signal.broker_side,
        "brick_pct": _num(spec.brick_pct),
        "brick_size_dollars": _num(spec.brick_size_dollars),
        "brick_open": _num(previous_close),
        "brick_close": _num(brick_close),
        "brick_formed_at": brick_formed_at.isoformat(),
        "brick_formation_seconds": formation_seconds,
        "brick_velocity_dollars_per_second": "UNKNOWN" if not formation_seconds else _num(spec.brick_size_dollars / Decimal(formation_seconds)),
        "volume_window_seconds": spec.volume_window_seconds,
        "volume_threshold_label": spec.volume_threshold_label,
        "volume_threshold_z": _num_or_none(spec.volume_threshold_z),
        "volume_status": volume.status if volume is not None else VOLUME_STATUS_MISSING,
        "volume_z": _num_or_none(volume.z_score if volume is not None else None),
        "observed_volume": _num_or_none(volume.observed_volume if volume is not None else None),
        "baseline_sample_count": volume.baseline_sample_count if volume is not None else 0,
        "baseline_mean_volume": _num_or_none(volume.baseline_mean_volume if volume is not None else None),
        "baseline_std_volume": _num_or_none(volume.baseline_std_volume if volume is not None else None),
        "entry_signal_price": signal.price,
        "accepted": accepted,
        "rejection_reason": rejection_reason,
        "signal_id": signal.signal_id,
        "order_id": None,
        "actual_entry_timestamp": None,
        "actual_entry_price": "UNKNOWN",
        "entry_fill_id": None,
        "exit_timestamp": None,
        "exit_price": "UNKNOWN",
        "exit_fill_id": None,
        "realized_pnl_dollars": "UNKNOWN",
        "trade_duration_seconds": "UNKNOWN",
        "mfe_dollars_per_share": "UNKNOWN",
        "mae_dollars_per_share": "UNKNOWN",
        "mae_abs_dollars_per_share": "UNKNOWN",
        "spread_dollars": "UNKNOWN",
        "spread_over_brick_dollars": "UNKNOWN",
        "expected_transaction_cost_dollars": "UNKNOWN",
        "transaction_cost_over_brick_dollars": "UNKNOWN",
        "transaction_cost_over_expected_captured_move": "UNKNOWN",
    }


def _exit_opportunity_placeholder(*, spec: ReversalVolumeCandidateSpec, signal: Signal) -> dict[str, Any]:
    return {
        "candidate_id": spec.candidate_id,
        "symbol": spec.symbol,
        "timestamp": signal.timestamp.isoformat(),
        "signal_type": signal.signal_type,
        "accepted": True,
    }


def _candidate_row(
    *,
    config: ReversalVolumeResearchConfig,
    spec: ReversalVolumeCandidateSpec,
    prepared: PreparedSource,
    strategy_config: dict[str, Any],
    metrics: dict[str, Any],
    renko_bricks: list[RenkoBrick],
    signals: list[Signal],
    orders: list[Any],
    fills: list[Any],
    final_snapshot: AccountingSnapshot,
    opportunities: list[dict[str, Any]],
    canonical_observation_count: int,
    pending_orders_at_end: int,
) -> dict[str, Any]:
    raw_entries = [item for item in opportunities if item.get("volume_threshold_label")]
    accepted_entries = [item for item in raw_entries if item.get("accepted") is True]
    rejected_entries = [item for item in raw_entries if item.get("accepted") is False]
    closed_opportunities = [item for item in accepted_entries if item.get("realized_pnl_dollars") not in {None, "UNKNOWN"}]
    mfe_values = [_dec(item.get("mfe_dollars_per_share")) for item in closed_opportunities if item.get("mfe_dollars_per_share") not in {None, "UNKNOWN"}]
    mae_values = [_dec(item.get("mae_dollars_per_share")) for item in closed_opportunities if item.get("mae_dollars_per_share") not in {None, "UNKNOWN"}]
    mae_abs_values = [abs(value) for value in mae_values]
    closed_trades = list(final_snapshot.closed_trades)
    long_trades = [trade for trade in closed_trades if getattr(trade, "side", None) == "long"]
    short_trades = [trade for trade in closed_trades if getattr(trade, "side", None) == "short"]
    holding_seconds = [
        Decimal(str((trade.exit_timestamp - trade.entry_timestamp).total_seconds()))
        for trade in closed_trades
        if getattr(trade, "entry_timestamp", None) is not None and getattr(trade, "exit_timestamp", None) is not None
    ]
    session_pnl = _session_pnl_by_exit(closed_trades)
    dominant_share = _dominant_session_share(session_pnl, _dec(metrics.get("net_pnl_dollars")))
    strategy_contract = renko_strategy_contract(strategy_config)
    row = {
        "research_id": config.research_id,
        "research_version": REVERSAL_VOLUME_RESEARCH_VERSION,
        "candidate_id": spec.candidate_id,
        "candidate_status": "COMPLETED",
        "result_quality": "FULL_SESSION_RESEARCH",
        "trader_id": spec.trader.trader_id,
        "symbol": spec.symbol,
        "strategy_type": "renko_reversal",
        "entry_confirmation": "E1",
        "entry_bricks": 1,
        "brick_pct": _num(spec.brick_pct),
        "brick_size_dollars": _num(spec.brick_size_dollars),
        "reference_price": _num(spec.reference_price),
        "reference_price_timestamp": spec.reference_price_timestamp.isoformat(),
        "volume_window_seconds": spec.volume_window_seconds,
        "volume_threshold_label": spec.volume_threshold_label,
        "volume_threshold_z": _num_or_none(spec.volume_threshold_z),
        "minimum_volume_baseline_sessions": config.minimum_volume_baseline_sessions,
        "source_preparation_id": prepared.preparation_id,
        "source_combined_hash": prepared.source_combined_hash,
        "market_stream_fingerprint": (prepared.market_stream_contract or {}).get("fingerprint")
        if isinstance(prepared.market_stream_contract, dict)
        else None,
        "renko_strategy_fingerprint": strategy_contract.fingerprint,
        "renko_strategy_contract_version": strategy_contract.version,
        "approved_canonical_path": bool(prepared.approved_canonical_path),
        "execution_model": NextObservationExecution.version,
        "canonical_observations": canonical_observation_count,
        "renko_bricks": len(renko_bricks),
        "raw_e1_opportunities": len(raw_entries),
        "accepted_e1_opportunities": len(accepted_entries),
        "rejected_e1_opportunities": len(rejected_entries),
        "acceptance_rate_pct": _num(Decimal(len(accepted_entries)) / Decimal(len(raw_entries)) * Decimal("100")) if raw_entries else "UNKNOWN",
        "strategy_signals": len(signals),
        "queued_orders": len(orders),
        "simulated_fills": len(fills),
        "pending_orders_at_end": pending_orders_at_end,
        "closed_trades": metrics["closed_trades"],
        "winning_trades": metrics["winning_trades"],
        "losing_trades": metrics["losing_trades"],
        "win_rate_pct": metrics["win_rate_pct"],
        "gross_profit_dollars": metrics["gross_profit_dollars"],
        "gross_loss_dollars": metrics["gross_loss_dollars"],
        "realized_pnl_dollars": metrics["realized_pnl_dollars"],
        "unrealized_pnl_dollars": metrics["unrealized_pnl_dollars"],
        "net_pnl_dollars": metrics["net_pnl_dollars"],
        "strategy_return_pct": metrics["strategy_return_pct"],
        "return_pct": metrics["strategy_return_pct"],
        "return_on_allocated_capital_pct": metrics["return_on_allocated_capital_pct"],
        "max_drawdown_dollars": metrics["max_drawdown_dollars"],
        "max_drawdown_pct": metrics["max_drawdown_pct"],
        "profit_factor": metrics["profit_factor"],
        "expectancy_dollars": metrics["average_trade_pnl_dollars"],
        "average_trade_pnl_dollars": metrics["average_trade_pnl_dollars"],
        "average_winner_dollars": metrics["average_winner_dollars"],
        "average_loser_dollars": metrics["average_loser_dollars"],
        "avg_mfe_dollars_per_share": _num_or_unknown(_average_decimal(mfe_values)),
        "avg_mae_dollars_per_share": _num_or_unknown(_average_decimal(mae_values)),
        "avg_mae_abs_dollars_per_share": _num_or_unknown(_average_decimal(mae_abs_values)),
        "median_mfe_dollars_per_share": _num_or_unknown(_median_decimal(mfe_values)),
        "median_mae_dollars_per_share": _num_or_unknown(_median_decimal(mae_values)),
        "median_mae_abs_dollars_per_share": _num_or_unknown(_median_decimal(mae_abs_values)),
        "long_trade_count": len(long_trades),
        "short_trade_count": len(short_trades),
        "long_pnl_dollars": _num(sum((Decimal(str(trade.pnl)) for trade in long_trades if trade.pnl is not None), Decimal("0"))),
        "short_pnl_dollars": _num(sum((Decimal(str(trade.pnl)) for trade in short_trades if trade.pnl is not None), Decimal("0"))),
        "average_trade_duration_seconds": _num_or_unknown(_average_decimal(holding_seconds)),
        "median_trade_duration_seconds": _num_or_unknown(_median_decimal(holding_seconds)),
        "position_sizing_mode": config.position_sizing_mode,
        "research_quantity": _num(spec.research_quantity),
        "allocated_capital_dollars": _num(spec.allocated_capital),
        "unused_cash_dollars": _num(spec.unused_cash),
        "spread_source": "UNAVAILABLE_FROM_TRADE_ONLY_SIP_SOURCE",
        "spread_dollars": "UNKNOWN",
        "spread_over_brick_dollars": "UNKNOWN",
        "expected_transaction_cost_dollars": "UNKNOWN",
        "transaction_cost_over_brick_dollars": "UNKNOWN",
        "transaction_cost_over_expected_captured_move": "UNKNOWN",
        "dominant_session_pnl_share_pct": _num_or_unknown(dominant_share),
        "dominant_session_flag": bool(dominant_share is not None and dominant_share >= Decimal("50")),
    }
    return row


def _candidate_session_rows(
    *,
    spec: ReversalVolumeCandidateSpec,
    closed_trades: list[Any],
    equity_curve: list[EquityPoint],
) -> list[dict[str, Any]]:
    pnl_by_session = _session_pnl_by_exit(closed_trades)
    trades_by_session: dict[str, int] = {}
    for trade in closed_trades:
        session = trade.exit_timestamp.astimezone(EASTERN).date().isoformat()
        trades_by_session[session] = trades_by_session.get(session, 0) + 1
    ending_equity_by_session: dict[str, Decimal] = {}
    for point in equity_curve:
        session = point.timestamp.astimezone(EASTERN).date().isoformat()
        ending_equity_by_session[session] = Decimal(str(point.equity))
    sessions = sorted(set(pnl_by_session) | set(trades_by_session) | set(ending_equity_by_session))
    return [
        {
            "candidate_id": spec.candidate_id,
            "symbol": spec.symbol,
            "brick_pct": _num(spec.brick_pct),
            "volume_window_seconds": spec.volume_window_seconds,
            "volume_threshold_label": spec.volume_threshold_label,
            "session": session,
            "closed_trades": trades_by_session.get(session, 0),
            "realized_pnl_dollars": _num(pnl_by_session.get(session, Decimal("0"))),
            "ending_equity_dollars": _num(ending_equity_by_session[session]) if session in ending_equity_by_session else "UNKNOWN",
        }
        for session in sessions
    ]


def _candidate_strategy_config(spec: ReversalVolumeCandidateSpec) -> dict[str, Any]:
    strategy_config = copy.deepcopy(spec.trader.strategy_config)
    strategy_config["strategy_type"] = "renko_reversal"
    strategy_config["brick_size"] = float(spec.brick_size_dollars)
    strategy_config["entry_bricks"] = 1
    strategy_config["qty"] = float(spec.research_quantity)
    strategy_config["notional"] = None
    strategy_config["take_profit_bricks"] = None
    strategy_config["stop_loss_bricks"] = None
    strategy_config["psar_initial_stop_bricks"] = None
    strategy_config["psar_af_step"] = None
    strategy_config["psar_af_max"] = None
    return strategy_config


def _research_trader_priority(raw: dict[str, Any], strategy: RenkoStrategyConfig) -> int:
    if strategy.strategy_type == "renko_reversal" and strategy.entry_bricks == 1:
        return 0
    if strategy.strategy_type == "renko_reversal":
        return 1
    if raw.get("enabled", True):
        return 2
    return 3


def _research_reversal_strategy_config(base: RenkoStrategyConfig) -> RenkoStrategyConfig:
    payload = dict(base.__dict__)
    source_is_reversal = base.strategy_type == "renko_reversal"
    payload.update(
        {
            "strategy_type": "renko_reversal",
            "entry_bricks": 1,
            "take_profit_bricks": None,
            "stop_loss_bricks": None,
            "psar_initial_stop_bricks": None,
            "psar_af_step": None,
            "psar_af_max": None,
            "notional": None,
            "exit_management": base.exit_management if source_is_reversal else "strategy",
            "candidate_config_fingerprint": base.candidate_config_fingerprint if source_is_reversal else None,
            "market_contract_fingerprint": base.market_contract_fingerprint if source_is_reversal else None,
            "active_start_date": base.active_start_date if source_is_reversal else None,
            "active_end_date": base.active_end_date if source_is_reversal else None,
        }
    )
    return RenkoStrategyConfig(**payload)


def _validate_source_coverage(*, symbols: tuple[str, ...], sessions: tuple[date, ...], source_root: str) -> list[CoverageReport]:
    reports: list[CoverageReport] = []
    for symbol in symbols:
        for session in sessions:
            path = resolve_local_source_path(source_root, symbol, session)
            reports.append(validate_coverage(symbol=symbol, session_date=session, source_path=path))
    return reports


def _eligible_second_volumes(*, symbol: str, session: date, source_root: str) -> dict[datetime, Decimal]:
    source_path = resolve_local_source_path(source_root, symbol, session)
    window = nyse_session_window(session)
    config = ReplayConfig(
        run_id=f"volume:{symbol}:{session.isoformat()}",
        trader_id=f"volume:{symbol}",
        symbol=symbol.upper(),
        start=window.start_utc,
        end=window.end_utc,
        timezone="UTC",
        session_mode="regular",
        strategy_config={"symbol": symbol.upper(), "brick_size": 1, "entry_bricks": 1, "qty": 1},
        initial_capital="10000",
        brick_mode="BRICK_DOLLARS",
        brick_size_dollars="1",
        source_data_path=str(source_path),
    )
    volumes: dict[datetime, Decimal] = {}
    for item in stream_historical_source(config):
        if item.trade is None:
            continue
        decision = evaluate_trade_eligibility(item.trade)
        if not decision.is_eligible:
            continue
        second = item.trade.second_bucket
        volumes[second] = volumes.get(second, Decimal("0")) + item.trade.quantity
    return volumes


def _rolling_session_volume_by_offset(
    *,
    session: date,
    second_volumes: dict[datetime, Decimal],
    window_seconds: int,
) -> dict[int, Decimal]:
    window = nyse_session_window(session)
    values: dict[int, Decimal] = {}
    queue: list[tuple[datetime, Decimal]] = []
    rolling = Decimal("0")
    timestamp = window.start_utc
    offset = 0
    while timestamp <= window.end_utc:
        volume = second_volumes.get(timestamp, Decimal("0"))
        queue.append((timestamp, volume))
        rolling += volume
        cutoff = timestamp - timedelta(seconds=window_seconds - 1)
        while queue and queue[0][0] < cutoff:
            _old_ts, old_volume = queue.pop(0)
            rolling -= old_volume
        values[offset] = rolling
        timestamp += timedelta(seconds=1)
        offset += 1
    return values


def _volume_z_snapshot(
    *,
    symbol: str,
    timestamp: datetime,
    window_seconds: int,
    observed_volume: Decimal,
    history: list[Decimal],
    minimum_baseline_sessions: int,
) -> VolumeZSnapshot:
    if len(history) < minimum_baseline_sessions:
        return VolumeZSnapshot(
            timestamp=timestamp,
            symbol=symbol.upper(),
            window_seconds=window_seconds,
            observed_volume=observed_volume,
            baseline_sample_count=len(history),
            baseline_mean_volume=None,
            baseline_std_volume=None,
            z_score=None,
            status=VOLUME_STATUS_INSUFFICIENT_BASELINE,
        )
    mean_value = sum(history, Decimal("0")) / Decimal(len(history))
    std_value = Decimal(str(pstdev([float(value) for value in history])))
    if std_value <= 0:
        return VolumeZSnapshot(
            timestamp=timestamp,
            symbol=symbol.upper(),
            window_seconds=window_seconds,
            observed_volume=observed_volume,
            baseline_sample_count=len(history),
            baseline_mean_volume=mean_value,
            baseline_std_volume=std_value,
            z_score=None,
            status=VOLUME_STATUS_ZERO_STD,
        )
    return VolumeZSnapshot(
        timestamp=timestamp,
        symbol=symbol.upper(),
        window_seconds=window_seconds,
        observed_volume=observed_volume,
        baseline_sample_count=len(history),
        baseline_mean_volume=mean_value,
        baseline_std_volume=std_value,
        z_score=(observed_volume - mean_value) / std_value,
        status=VOLUME_STATUS_OK,
    )


def _attach_closed_trade_to_opportunity(
    *,
    closed_trade: Any,
    opportunities: list[dict[str, Any]],
    open_opportunity_by_entry_fill_id: dict[str, int],
) -> None:
    entry_fill_id = closed_trade.entry_fill_id
    if entry_fill_id is None:
        return
    opportunity_index = open_opportunity_by_entry_fill_id.pop(entry_fill_id, None)
    if opportunity_index is None:
        return
    opportunity = opportunities[opportunity_index]
    opportunity["exit_timestamp"] = closed_trade.exit_timestamp.isoformat()
    opportunity["exit_price"] = _num(closed_trade.exit_price)
    opportunity["exit_fill_id"] = closed_trade.exit_fill_id
    opportunity["realized_pnl_dollars"] = _num_or_none(closed_trade.pnl)
    if closed_trade.entry_timestamp is not None:
        opportunity["trade_duration_seconds"] = (closed_trade.exit_timestamp - closed_trade.entry_timestamp).total_seconds()


def _open_opportunity_index_for_fill(fill: Any, opportunities: list[dict[str, Any]]) -> int | None:
    for index in range(len(opportunities) - 1, -1, -1):
        opportunity = opportunities[index]
        if opportunity.get("accepted") is not True:
            continue
        if not opportunity.get("volume_threshold_label"):
            continue
        if opportunity.get("entry_fill_id"):
            continue
        if opportunity.get("symbol") != fill.symbol:
            continue
        return index
    return None


def _update_open_opportunity_excursion(
    *,
    position: Any,
    mark_price: Decimal,
    opportunities: list[dict[str, Any]],
    open_opportunity_by_entry_fill_id: dict[str, int],
) -> None:
    entry_fill_id = getattr(position, "entry_fill_id", None)
    if not entry_fill_id or entry_fill_id not in open_opportunity_by_entry_fill_id:
        return
    index = open_opportunity_by_entry_fill_id[entry_fill_id]
    opportunity = opportunities[index]
    entry_price = Decimal(str(getattr(position, "avg_entry_price", 0) or 0))
    if entry_price <= 0:
        return
    side = getattr(position, "side", None)
    direction = Decimal("1") if side == "long" else Decimal("-1")
    excursion = (Decimal(str(mark_price)) - entry_price) * direction
    current_mfe = _dec(opportunity.get("mfe_dollars_per_share")) if opportunity.get("mfe_dollars_per_share") not in {"UNKNOWN", None} else excursion
    current_mae = _dec(opportunity.get("mae_dollars_per_share")) if opportunity.get("mae_dollars_per_share") not in {"UNKNOWN", None} else excursion
    opportunity["mfe_dollars_per_share"] = _num(max(current_mfe, excursion))
    opportunity["mae_dollars_per_share"] = _num(min(current_mae, excursion))
    opportunity["mae_abs_dollars_per_share"] = _num(abs(min(current_mae, excursion)))


def _stable_research_action_identifiers(
    *,
    spec: ReversalVolumeCandidateSpec,
    observation: CanonicalObservation,
    action_sequence: int,
    signal: Signal,
    intent: OrderIntent,
) -> tuple[Signal, OrderIntent]:
    suffix = sha256(
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
    safe_trader = _safe_path_name(spec.trader.trader_id.lower())
    stable_signal_id = f"{safe_trader}-{signal.symbol.lower()}-{suffix}"
    stable_client_order_id = f"{safe_trader[:31]}-{suffix}"[:48]
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


def _control_delta_row(*, control: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    fields = {
        "control_closed_trades": "closed_trades",
        "control_net_pnl_dollars": "net_pnl_dollars",
        "control_expectancy_dollars": "expectancy_dollars",
        "control_profit_factor": "profit_factor",
        "control_max_drawdown_dollars": "max_drawdown_dollars",
        "control_win_rate_pct": "win_rate_pct",
        "control_avg_mfe_dollars_per_share": "avg_mfe_dollars_per_share",
        "control_avg_mae_dollars_per_share": "avg_mae_dollars_per_share",
    }
    result = {target: control.get(source) for target, source in fields.items()}
    for metric in (
        "closed_trades",
        "net_pnl_dollars",
        "expectancy_dollars",
        "profit_factor",
        "max_drawdown_dollars",
        "win_rate_pct",
        "avg_mfe_dollars_per_share",
        "avg_mae_dollars_per_share",
    ):
        result[f"delta_{metric}"] = _delta(candidate.get(metric), control.get(metric))
    return result


def _empty_control_delta_row() -> dict[str, Any]:
    keys = [
        "control_closed_trades",
        "control_net_pnl_dollars",
        "control_expectancy_dollars",
        "control_profit_factor",
        "control_max_drawdown_dollars",
        "control_win_rate_pct",
        "control_avg_mfe_dollars_per_share",
        "control_avg_mae_dollars_per_share",
        "delta_closed_trades",
        "delta_net_pnl_dollars",
        "delta_expectancy_dollars",
        "delta_profit_factor",
        "delta_max_drawdown_dollars",
        "delta_win_rate_pct",
        "delta_avg_mfe_dollars_per_share",
        "delta_avg_mae_dollars_per_share",
    ]
    return {key: "NOT_APPLICABLE" for key in keys}


def _neighbor_row(
    row: dict[str, Any],
    *,
    rows: list[dict[str, Any]],
    threshold_order: dict[str, int],
) -> dict[str, Any]:
    symbol = row.get("symbol")
    window = row.get("volume_window_seconds")
    threshold = row.get("volume_threshold_label")
    brick = _dec(row.get("brick_pct"))
    brick_neighbors = [
        other
        for other in rows
        if other.get("symbol") == symbol
        and other.get("volume_window_seconds") == window
        and other.get("volume_threshold_label") == threshold
        and Decimal("0") < abs(_dec(other.get("brick_pct")) - brick) <= Decimal("0.04")
    ]
    threshold_idx = threshold_order.get(str(threshold), -1)
    z_neighbors = [
        other
        for other in rows
        if other.get("symbol") == symbol
        and other.get("volume_window_seconds") == window
        and _dec(other.get("brick_pct")) == brick
        and abs(threshold_order.get(str(other.get("volume_threshold_label")), -99) - threshold_idx) == 1
    ]
    return {
        "brick_neighbor_count": len(brick_neighbors),
        "brick_neighbor_min_expectancy_dollars": _num_or_unknown(min((_dec(item.get("expectancy_dollars")) for item in brick_neighbors), default=None)),
        "brick_neighbor_min_profit_factor": _num_or_unknown(min((_dec(item.get("profit_factor")) for item in brick_neighbors), default=None)),
        "volume_z_neighbor_count": len(z_neighbors),
        "volume_z_neighbor_min_expectancy_dollars": _num_or_unknown(min((_dec(item.get("expectancy_dollars")) for item in z_neighbors), default=None)),
        "volume_z_neighbor_min_profit_factor": _num_or_unknown(min((_dec(item.get("profit_factor")) for item in z_neighbors), default=None)),
    }


def _write_outputs(
    *,
    config: ReversalVolumeResearchConfig,
    session_dates: tuple[date, ...],
    symbols: tuple[str, ...],
    prepared_sources: dict[str, PreparedSource],
    coverage_reports: list[CoverageReport],
    rows: list[dict[str, Any]],
    opportunities: list[dict[str, Any]],
    session_rows: list[dict[str, Any]],
) -> None:
    out = config.output_path
    out.mkdir(parents=True, exist_ok=True)
    manifest = {
        "research_id": config.research_id,
        "version": REVERSAL_VOLUME_RESEARCH_VERSION,
        "config_path": config.config_path,
        "symbols": list(symbols),
        "session_dates": [item.isoformat() for item in session_dates],
        "baseline_session_dates": [item.isoformat() for item in config.baseline_session_dates],
        "brick_grid": {
            "start_pct": str(config.brick_start_pct),
            "end_pct": str(config.brick_end_pct),
            "step_pct_points": str(config.brick_step_pct_points),
            "candidate_pcts": [str(item) for item in config.candidate_pcts] if config.candidate_pcts else None,
        },
        "volume_windows_seconds": list(config.volume_windows_seconds),
        "volume_thresholds": [volume_threshold_label(item) for item in config.volume_thresholds],
        "minimum_volume_baseline_sessions": config.minimum_volume_baseline_sessions,
        "minimum_closed_trades": config.minimum_closed_trades,
        "prepared_sources": {symbol: asdict(source) for symbol, source in prepared_sources.items()},
        "coverage_validation": {
            "enabled": config.validate_market_data_coverage,
            "reports": [asdict(report) for report in coverage_reports],
        },
        "production_live_behavior_modified": False,
        "broker_order_endpoints_enabled": False,
        "production_config_mutation": False,
        "deployment_action": False,
        "research_backfill_action": False,
    }
    _write_json(out / "manifest.json", manifest)
    if coverage_reports:
        write_coverage_reports(out / "coverage_reports.json", coverage_reports)
    _write_csv(out / "candidates.csv", rows)
    _write_csv(out / "heatmap_data.csv", _heatmap_rows(rows))
    _write_csv(out / "control_comparison.csv", _control_comparison_rows(rows))
    _write_csv(out / "session_performance.csv", session_rows)
    _write_jsonl(out / "opportunities.jsonl", opportunities)
    _write_summary(out / "SUMMARY.md", rows=rows, config=config, session_dates=session_dates, symbols=symbols)
    _write_heatmaps(out / "heatmaps", rows)


def _heatmap_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fields = [
        "symbol",
        "volume_window_seconds",
        "volume_threshold_label",
        "volume_threshold_z",
        "brick_pct",
        "net_pnl_dollars",
        "expectancy_dollars",
        "profit_factor",
        "max_drawdown_dollars",
        "closed_trades",
        "avg_mfe_dollars_per_share",
        "avg_mae_dollars_per_share",
        "sample_quality",
    ]
    return [{field: row.get(field) for field in fields} for row in rows]


def _control_comparison_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "symbol": row.get("symbol"),
            "volume_window_seconds": row.get("volume_window_seconds"),
            "brick_pct": row.get("brick_pct"),
            "volume_threshold_label": row.get("volume_threshold_label"),
            "control_closed_trades": row.get("control_closed_trades"),
            "filtered_closed_trades": row.get("closed_trades"),
            "delta_closed_trades": row.get("delta_closed_trades"),
            "control_net_pnl_dollars": row.get("control_net_pnl_dollars"),
            "filtered_net_pnl_dollars": row.get("net_pnl_dollars"),
            "delta_net_pnl_dollars": row.get("delta_net_pnl_dollars"),
            "control_expectancy_dollars": row.get("control_expectancy_dollars"),
            "filtered_expectancy_dollars": row.get("expectancy_dollars"),
            "delta_expectancy_dollars": row.get("delta_expectancy_dollars"),
            "control_profit_factor": row.get("control_profit_factor"),
            "filtered_profit_factor": row.get("profit_factor"),
            "delta_profit_factor": row.get("delta_profit_factor"),
            "control_max_drawdown_dollars": row.get("control_max_drawdown_dollars"),
            "filtered_max_drawdown_dollars": row.get("max_drawdown_dollars"),
            "delta_max_drawdown_dollars": row.get("delta_max_drawdown_dollars"),
            "control_win_rate_pct": row.get("control_win_rate_pct"),
            "filtered_win_rate_pct": row.get("win_rate_pct"),
            "delta_win_rate_pct": row.get("delta_win_rate_pct"),
            "control_avg_mfe_dollars_per_share": row.get("control_avg_mfe_dollars_per_share"),
            "filtered_avg_mfe_dollars_per_share": row.get("avg_mfe_dollars_per_share"),
            "delta_avg_mfe_dollars_per_share": row.get("delta_avg_mfe_dollars_per_share"),
            "control_avg_mae_dollars_per_share": row.get("control_avg_mae_dollars_per_share"),
            "filtered_avg_mae_dollars_per_share": row.get("avg_mae_dollars_per_share"),
            "delta_avg_mae_dollars_per_share": row.get("delta_avg_mae_dollars_per_share"),
        }
        for row in rows
        if row.get("volume_threshold_label") != NO_FILTER_LABEL
    ]


def _write_summary(path: Path, *, rows: list[dict[str, Any]], config: ReversalVolumeResearchConfig, session_dates: tuple[date, ...], symbols: tuple[str, ...]) -> None:
    eligible = [row for row in rows if row.get("sample_quality") == "DISCOVERY_QUALITY" and row.get("volume_threshold_label") != NO_FILTER_LABEL]
    ranked = sorted(eligible, key=lambda row: (_dec(row.get("expectancy_dollars")), _dec(row.get("profit_factor")), _dec(row.get("net_pnl_dollars"))), reverse=True)
    lines = [
        "# Reversal Volume Z-Score Research",
        "",
        f"Research ID: `{config.research_id}`",
        f"Symbols: `{','.join(symbols)}`",
        f"Evaluation sessions: `{','.join(item.isoformat() for item in session_dates)}`",
        f"Minimum baseline sessions: `{config.minimum_volume_baseline_sessions}`",
        "",
        "This is research-only output. No production/live behavior is modified by this runner.",
        "",
        "## Top Discovery-Quality Filtered Candidates",
        "",
        "| Symbol | Window | Brick % | Threshold | Trades | Net PnL | Expectancy | Profit Factor | Max DD | Acceptance |",
        "|---|---:|---:|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in ranked[:25]:
        lines.append(
            f"| {row.get('symbol')} | {row.get('volume_window_seconds')} | {row.get('brick_pct')} | "
            f"{row.get('volume_threshold_label')} | {row.get('closed_trades')} | {row.get('net_pnl_dollars')} | "
            f"{row.get('expectancy_dollars')} | {row.get('profit_factor')} | {row.get('max_drawdown_dollars')} | "
            f"{row.get('acceptance_rate_pct')} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_heatmaps(root: Path, rows: list[dict[str, Any]]) -> None:
    metrics = {
        "expectancy": "expectancy_dollars",
        "profit_factor": "profit_factor",
        "net_pnl": "net_pnl_dollars",
        "max_drawdown": "max_drawdown_dollars",
        "trade_count": "closed_trades",
        "average_mfe": "avg_mfe_dollars_per_share",
        "average_mae": "avg_mae_dollars_per_share",
    }
    groups: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault((str(row.get("symbol")), int(row.get("volume_window_seconds"))), []).append(row)
    for (symbol, window), grouped in groups.items():
        for name, field_name in metrics.items():
            _write_heatmap_svg(
                root / symbol.lower() / f"{window}s" / f"{name}.svg",
                rows=grouped,
                field_name=field_name,
                title=f"{symbol} {window}s {name}",
            )


def _write_heatmap_svg(path: Path, *, rows: list[dict[str, Any]], field_name: str, title: str) -> None:
    if not rows:
        return
    thresholds = sorted({str(row.get("volume_threshold_label")) for row in rows}, key=_threshold_sort_key)
    bricks = sorted({_dec(row.get("brick_pct")) for row in rows})
    by_cell = {(_dec(row.get("brick_pct")), str(row.get("volume_threshold_label"))): row for row in rows}
    values = [_float_or_none(row.get(field_name)) for row in rows]
    values = [value for value in values if value is not None and math.isfinite(value)]
    if not values:
        return
    min_value = min(values)
    max_value = max(values)
    cell_w = 72
    cell_h = 18
    left = 80
    top = 58
    width = left + len(thresholds) * cell_w + 24
    height = top + len(bricks) * cell_h + 36
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        handle.write(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">\n')
        handle.write('<rect width="100%" height="100%" fill="#f8fafc"/>\n')
        handle.write("<style>text{font-family:Arial,sans-serif;font-size:10px;fill:#0f172a}.title{font-size:16px;font-weight:700}</style>\n")
        handle.write(f'<text class="title" x="16" y="26">{title}</text>\n')
        for index, threshold in enumerate(thresholds):
            x = left + index * cell_w + cell_w / 2
            handle.write(f'<text x="{x:.1f}" y="48" text-anchor="middle">{threshold}</text>\n')
        for row_index, brick in enumerate(bricks):
            y = top + row_index * cell_h
            handle.write(f'<text x="72" y="{y + 12}" text-anchor="end">{brick}%</text>\n')
            for col_index, threshold in enumerate(thresholds):
                row = by_cell.get((brick, threshold))
                value = _float_or_none(row.get(field_name)) if row is not None else None
                color = _heatmap_color(value, min_value=min_value, max_value=max_value)
                x = left + col_index * cell_w
                handle.write(f'<rect x="{x}" y="{y}" width="{cell_w - 1}" height="{cell_h - 1}" fill="{color}"><title>{value}</title></rect>\n')
        handle.write("</svg>\n")


def _threshold_sort_key(label: str) -> tuple[int, Decimal]:
    if label == NO_FILTER_LABEL:
        return (0, Decimal("-999"))
    return (1, Decimal(label.replace("Z_GTE_", "").replace("p", ".")))


def _heatmap_color(value: float | None, *, min_value: float, max_value: float) -> str:
    if value is None or not math.isfinite(value):
        return "#e2e8f0"
    if max_value == min_value:
        ratio = 0.5
    else:
        ratio = (value - min_value) / (max_value - min_value)
    ratio = min(1.0, max(0.0, ratio))
    red = int(220 - ratio * 150)
    green = int(80 + ratio * 130)
    blue = int(78 + ratio * 30)
    return f"#{red:02x}{green:02x}{blue:02x}"


def _session_pnl_by_exit(closed_trades: list[Any]) -> dict[str, Decimal]:
    result: dict[str, Decimal] = {}
    for trade in closed_trades:
        if trade.pnl is None:
            continue
        session = trade.exit_timestamp.astimezone(EASTERN).date().isoformat()
        result[session] = result.get(session, Decimal("0")) + Decimal(str(trade.pnl))
    return result


def _dominant_session_share(session_pnl: dict[str, Decimal], net_pnl: Decimal) -> Decimal | None:
    if not session_pnl or net_pnl == 0:
        return None
    largest = max(abs(value) for value in session_pnl.values())
    return largest / max(abs(net_pnl), Decimal("0.0000001")) * Decimal("100")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(_jsonable(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(_jsonable(row), sort_keys=True) + "\n")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    keys = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(_jsonable(row) for row in rows)


def _jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return _jsonable(asdict(value))
    if isinstance(value, dict):
        return {key: _jsonable(val) for key, val in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, Decimal):
        if not value.is_finite():
            return str(value)
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _candidate_id(*parts: Any) -> str:
    return "rvz_" + sha256("|".join(str(item) for item in parts).encode("utf-8")).hexdigest()[:24]


def _safe_path_name(value: str) -> str:
    return "".join(char if char.isalnum() or char in "._-" else "_" for char in value)[:120]


def _num(value: Any) -> Any:
    if value in {None, "UNKNOWN"}:
        return "UNKNOWN"
    dec = Decimal(str(value))
    if not dec.is_finite():
        return str(dec)
    if dec == dec.to_integral_value():
        return int(dec)
    return float(dec)


def _num_or_none(value: Any) -> Any:
    if value is None:
        return None
    return _num(value)


def _num_or_unknown(value: Any) -> Any:
    if value is None:
        return "UNKNOWN"
    return _num(value)


def _dec(value: Any) -> Decimal:
    if value in {None, "", "UNKNOWN", "NOT_APPLICABLE"}:
        return Decimal("0")
    return Decimal(str(value))


def _delta(left: Any, right: Any) -> Any:
    if left in {None, "", "UNKNOWN", "NOT_APPLICABLE"} or right in {None, "", "UNKNOWN", "NOT_APPLICABLE"}:
        return "UNKNOWN"
    return _num(_dec(left) - _dec(right))


def _float_or_none(value: Any) -> float | None:
    if value in {None, "", "UNKNOWN", "NOT_APPLICABLE"}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _average_decimal(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, Decimal("0")) / Decimal(len(values))


def _median_decimal(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return Decimal(str(median(values)))
