from __future__ import annotations

from dataclasses import asdict, is_dataclass, replace
from decimal import Decimal
from typing import Any

import pandas as pd

from silver.accounting import AccountingLedger, AccountingSnapshot
from silver.accounting.models import EquityPoint
from silver.events.models import CanonicalEvent
from silver.market import CanonicalOneSecondSipSampler, CanonicalObservation, market_stream_contract, promotion_contract, renko_strategy_contract
from strategy_engine import RenkoStrategyConfig, RenkoStrategyEngine
from trading_events import MarketTick

from .context import ReplayContext, build_context
from .execution import NextObservationExecution
from .historical_source import read_historical_source
from .ledgers import ReplayLedgers
from .metrics import build_metrics
from .models import ReplayConfig, ReplayDiagnostic, ReplayResult
from .output import write_outputs


def run_replay(config: ReplayConfig) -> ReplayResult:
    source = read_historical_source(config)
    config = _resolve_brick_configuration(config, source.trades)
    context = build_context(config, source_dataset_hash=source.source_hash)
    ledgers = ReplayLedgers()
    ledgers.diagnostics.extend(source.diagnostics)
    ledgers.raw_source_trades.extend(source.trades)

    strategy = RenkoStrategyEngine(RenkoStrategyConfig(**config.strategy_config))
    if config.initial_strategy_state is not None:
        strategy.restore_snapshot(config.initial_strategy_state)
    elif config.initial_renko_state is not None:
        strategy.restore_snapshot(
            {
                "symbol": config.symbol,
                "strategy_type": config.strategy_config.get("strategy_type", "renko_reversal"),
                "renko": config.initial_renko_state,
                "trade": {},
            }
        )

    accounting = AccountingLedger(
        symbol=config.symbol,
        starting_capital=config.initial_capital,
        starting_position=config.initial_accounting_position,
    )
    sampler = CanonicalOneSecondSipSampler(run_id=config.run_id)
    execution = NextObservationExecution(run_id=config.run_id, trader_id=config.trader_id)
    order_latencies: list[Decimal] = []
    sequence = 0

    for raw_trade in source.trades:
        result = sampler.process_trade(raw_trade)
        ledgers.events.extend(result.events)
        if result.eligibility is not None:
            ledgers.eligibility_decisions.append(result.eligibility)
            if result.eligibility.is_eligible:
                ledgers.eligible_trades.append(raw_trade)
        ledgers.diagnostics.extend(_sampler_diagnostics(result.diagnostics))
        for observation in result.observations:
            sequence += 1
            ledgers.canonical_observations.append(observation)
            _process_observation(
                config=config,
                observation=observation,
                sequence=sequence,
                strategy=strategy,
                accounting=accounting,
                execution=execution,
                ledgers=ledgers,
                order_latencies=order_latencies,
            )

    if config.sampler_final_flush_policy != "NO_FLUSH":
        ledgers.diagnostics.append(
            ReplayDiagnostic("unsupported_sampler_flush_policy", "Replay V1 default path does not flush final sampler state.", "warning")
        )
    ledgers.diagnostics.extend(_pending_sampler_diagnostics(sampler.pending_diagnostics()))
    ledgers.diagnostics.extend(_sanity_warnings(config=config, ledgers=ledgers, pending_orders=execution.pending_orders, source=source))

    final_mark = ledgers.canonical_observations[-1].close_price if ledgers.canonical_observations else None
    snapshot = accounting.snapshot(mark_price=final_mark)
    ledgers.accounting_snapshots.append(snapshot)
    if ledgers.canonical_observations:
        ledgers.equity_curve.append(
            EquityPoint(
                timestamp=ledgers.canonical_observations[-1].second_bucket,
                equity=snapshot.equity,
                realized_pnl=snapshot.realized_pnl,
                unrealized_pnl=snapshot.unrealized_pnl,
                net_pnl=snapshot.net_pnl,
                fill_id="final_mark_to_market",
            )
        )
    metrics = build_metrics(
        snapshot=snapshot,
        initial_capital=config.initial_capital,
        observations=ledgers.canonical_observations,
        order_latencies=order_latencies,
        equity_curve=ledgers.equity_curve,
    )
    diagnostics_summary = _diagnostics_summary(ledgers.diagnostics)
    context = replace(context, diagnostics_summary=diagnostics_summary)
    manifest = _manifest(config, context, ledgers, metrics, execution.pending_orders, source)
    output_dir = write_outputs(config=config, context=context, ledgers=ledgers, metrics=metrics, manifest=manifest)
    return ReplayResult(
        context=context,
        metrics=metrics,
        manifest=manifest,
        output_directory=str(output_dir),
        ledgers=ledgers,
        pending_orders=execution.pending_orders,
        diagnostics=tuple(ledgers.diagnostics),
    )


def _process_observation(
    *,
    config: ReplayConfig,
    observation: CanonicalObservation,
    sequence: int,
    strategy: RenkoStrategyEngine,
    accounting: AccountingLedger,
    execution: NextObservationExecution,
    ledgers: ReplayLedgers,
    order_latencies: list[Decimal],
) -> None:
    fill_bundles = execution.fill_pending(observation=observation, position=accounting.position)
    for bundle in fill_bundles:
        order_latencies.append(bundle.latency_seconds)
        for fill in bundle.fills:
            ledgers.simulated_fills.append(fill)
            before_transition_count = len(accounting.position_transitions)
            before_closed_count = len(accounting.closed_trades)
            snapshot = accounting.process_fill(fill)
            ledgers.position_transitions.extend(accounting.position_transitions[before_transition_count:])
            ledgers.closed_trades.extend(accounting.closed_trades[before_closed_count:])
            ledgers.accounting_snapshots.append(snapshot)
            ledgers.equity_curve.extend(snapshot.equity_curve[len(ledgers.equity_curve) :])
            ledgers.events.append(_compact_event(config.run_id, "SimulatedFillCreated", fill.timestamp, len(ledgers.events) + 1, fill.stable_fill_id, {}))
            if fill.leg == "close" and accounting.position.side is None and hasattr(strategy, "mark_close_filled"):
                strategy.mark_close_filled(
                    timestamp=fill.timestamp,
                    reason=fill.reason,
                    order_id=fill.order_id,
                    source=execution.version,
                )
            for transition in accounting.position_transitions[before_transition_count:]:
                ledgers.events.append(
                    _compact_event(config.run_id, "PositionTransitionCreated", transition.timestamp, len(ledgers.events) + 1, transition.fill_id, {})
                )
            for trade in accounting.closed_trades[before_closed_count:]:
                ledgers.events.append(
                    _compact_event(config.run_id, "ClosedTradeCreated", trade.exit_timestamp, len(ledgers.events) + 1, trade.exit_fill_id, {})
                )
            ledgers.events.append(
                _compact_event(config.run_id, "AccountingSnapshotCreated", fill.timestamp, len(ledgers.events) + 1, fill.stable_fill_id, {})
            )

    accounting_snapshot = accounting.snapshot(mark_price=observation.close_price)
    ledgers.accounting_snapshots.append(accounting_snapshot)

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
    bricks, actions = strategy.on_tick(tick)
    ledgers.renko_bricks.extend(bricks)
    for brick in bricks:
        ledgers.events.append(_compact_event(config.run_id, "RenkoBrickCreated", brick.timestamp, len(ledgers.events) + 1, observation.observation_id, _payload(brick)))
    for signal, intent in actions:
        ledgers.strategy_signals.append(signal)
        ledgers.events.append(_compact_event(config.run_id, "StrategySignalGenerated", signal.timestamp, len(ledgers.events) + 1, observation.observation_id, _payload(signal)))
        order = execution.queue(signal=signal, intent=intent, observation=observation, position=accounting.position)
        if order is None:
            ledgers.events.append(
                _compact_event(
                    config.run_id,
                    "SimulatedOrderSkipped",
                    signal.timestamp,
                    len(ledgers.events) + 1,
                    signal.signal_id,
                    {"reason": "close_position_already_flat", "intent": _payload(intent)},
                )
            )
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
        ledgers.simulated_orders.append(order)
        ledgers.events.append(_compact_event(config.run_id, "SimulatedOrderQueued", signal.timestamp, len(ledgers.events) + 1, signal.signal_id, _payload(order)))


def _resolve_brick_configuration(config: ReplayConfig, trades) -> ReplayConfig:
    reference = _first_canonical_observation(config, trades)
    strategy_config = dict(config.strategy_config)
    ref_price = reference.close_price if reference is not None else None
    ref_ts = reference.second_bucket if reference is not None else None
    ref_source = "first_canonical_observation" if reference is not None else "UNKNOWN"
    if config.brick_mode == "BRICK_DOLLARS":
        effective = config.brick_size_dollars
        pct = (effective / ref_price * Decimal("100")) if effective is not None and ref_price not in {None, Decimal("0")} else None
    elif config.brick_mode == "BRICK_PERCENT":
        if ref_price is None:
            raise ValueError("BRICK_PERCENT requires at least one canonical observation for reference price.")
        pct = config.brick_size_ltp_percent
        effective = ref_price * pct / Decimal("100")
    else:
        effective = Decimal(str(strategy_config["brick_size"]))
        pct = (effective / ref_price * Decimal("100")) if ref_price not in {None, Decimal("0")} else None
    strategy_config["brick_size"] = float(effective)
    return replace(
        config,
        strategy_config=strategy_config,
        brick_size_dollars=config.brick_size_dollars,
        brick_size_ltp_percent=config.brick_size_ltp_percent,
        derived_brick_size_ltp_percent=pct,
        brick_reference_price=ref_price,
        brick_reference_timestamp=ref_ts,
        brick_reference_source=ref_source,
        effective_brick_size_dollars=effective,
    )


def _first_canonical_observation(config: ReplayConfig, trades):
    sampler = CanonicalOneSecondSipSampler(run_id=config.run_id)
    for trade in trades:
        result = sampler.process_trade(trade)
        if result.observations:
            return result.observations[0]
    return None


def _manifest(config: ReplayConfig, context: ReplayContext, ledgers: ReplayLedgers, metrics: dict[str, Any], pending_orders: tuple[Any, ...], source) -> dict[str, Any]:
    warnings = sum(1 for diagnostic in ledgers.diagnostics if diagnostic.severity == "warning")
    errors = sum(1 for diagnostic in ledgers.diagnostics if diagnostic.severity == "error")
    source_integrity = asdict(source.integrity)
    market_contract = market_stream_contract(
        symbol=config.symbol,
        session_timezone=str(config.strategy_config.get("timezone") or "America/New_York"),
    )
    strategy_contract = renko_strategy_contract(config.strategy_config)
    combined_contract = promotion_contract(
        market_stream_fingerprint=market_contract.fingerprint,
        renko_strategy_fingerprint=strategy_contract.fingerprint,
        source_dataset_hash=source.source_hash,
    )
    force_flat = config.strategy_config.get("force_flat")
    force_flat_status = "NOT_CONFIGURED" if not force_flat else "NOT_MODELED"
    result_quality = _result_quality_classification(config=config, ledgers=ledgers, source=source)
    return {
        "run_id": config.run_id,
        "replay_version": config.replay_version,
        "replay_timestamp": context.replay_timestamp.isoformat(),
        "trader_id": config.trader_id,
        "strategy_id": context.strategy_id,
        "strategy_version": context.strategy_version,
        "symbol": config.symbol,
        "start": config.start.isoformat(),
        "end": config.end.isoformat(),
        "execution_model": config.execution_model,
        "result_quality_classification": result_quality,
        "brick_configuration": {
            "brick_mode": config.brick_mode,
            "brick_size_dollars": _num_or_none(config.brick_size_dollars),
            "brick_size_ltp_percent": _num_or_none(config.brick_size_ltp_percent),
            "derived_brick_size_ltp_percent": _num_or_none(config.derived_brick_size_ltp_percent),
            "reference_price": _num_or_none(config.brick_reference_price),
            "reference_timestamp": config.brick_reference_timestamp.isoformat() if config.brick_reference_timestamp else None,
            "reference_source": config.brick_reference_source,
            "rounding_policy": config.brick_rounding_policy,
            "calculated_brick_dollars": _num_or_none(config.effective_brick_size_dollars),
            "effective_brick_size_dollars": _num_or_none(config.effective_brick_size_dollars),
        },
        "strategy_configuration": {
            "source_config_path": config.source_config_path,
            "selected_trader_id": config.production_config.get("selected_trader_id") or config.trader_id,
            "strategy_type": config.strategy_config.get("strategy_type"),
            "strategy_name": config.strategy_config.get("strategy_name"),
            "configured_renko_source": config.production_config.get("configured_renko_source"),
            "entry_bricks": config.strategy_config.get("entry_bricks"),
            "reversal_behavior": "enabled for renko_reversal" if config.strategy_config.get("strategy_type") == "renko_reversal" else "strategy-specific",
            "renko_anchor_mode": config.strategy_config.get("renko_anchor_mode"),
            "renko_anchor_price": config.strategy_config.get("renko_anchor_price"),
            "configured_brick_pct": config.production_config.get("configured_brick_pct"),
            "brick_sizing_mode": config.production_config.get("brick_sizing_mode"),
            "authoritative_brick_field": config.production_config.get("authoritative_brick_field"),
            "reference_price_policy": config.production_config.get("reference_price_policy"),
            "brick_audit": config.production_config.get("brick_audit"),
            "profile_ltp_reference": config.production_config.get("profile_ltp_reference"),
            "qty": config.strategy_config.get("qty"),
            "entry_start": config.strategy_config.get("entry_start"),
            "entry_end": config.strategy_config.get("entry_end"),
            "force_flat": force_flat,
            "force_flat_parity": force_flat_status,
            "allow_overnight": config.strategy_config.get("allow_overnight"),
            "session_mode": config.session_mode,
            "session_timezone": config.strategy_config.get("timezone"),
            "production_config_hash": config.production_config.get("production_config_hash"),
        },
        "starting_state": {
            "position": str(config.initial_accounting_position),
            "basis_status": _basis_status(config),
            "parity_verifiable": config.initial_renko_state is not None and config.initial_strategy_state is not None,
            "start_state_path": config.start_state_path,
            "start_state_completeness": config.start_state_completeness,
        },
        "comparison_window": {
            "warmup_start": config.warmup_start.isoformat() if config.warmup_start else None,
            "comparison_start": config.comparison_start.isoformat() if config.comparison_start else config.start.isoformat(),
            "comparison_end": config.comparison_end.isoformat() if config.comparison_end else config.end.isoformat(),
        },
        "source_dataset_reference": context.source_dataset_reference,
        "source_dataset_hash": context.source_dataset_hash,
        "source_semantic_mode": config.source_semantic_mode,
        "configuration_hash": context.configuration_hash,
        "replay_fingerprint": context.replay_fingerprint,
        "contract_fingerprints": {
            "market_stream": asdict(market_contract),
            "renko_strategy": asdict(strategy_contract),
            "promotion": asdict(combined_contract),
        },
        "git_commit": context.git_commit,
        "sampler_version": config.sampler_version,
        "renko_version": config.renko_version,
        "accounting_version": config.accounting_version,
        "metrics_version": config.metrics_version,
        "output_level": config.output_level,
        "completion_status": "completed_with_errors" if errors else "completed",
        "warning_count": warnings,
        "error_count": errors,
        "pending_order_count": len(pending_orders),
        "production_parity_status": "NOT YET CALIBRATED",
        "source_integrity": source_integrity,
        "counts": {
            "source_rows_scanned": source.raw_rows_read,
            "raw_rows_read": source.raw_rows_read,
            "malformed_rows": source.malformed_rows,
            "raw_trades_in_window": source.raw_trades_in_window,
            "exact_duplicate_rows": source.integrity.exact_duplicate_rows,
            "duplicate_source_id_records": source.integrity.duplicate_source_id_records,
            "duplicate_broker_trade_id_records": source.integrity.duplicate_trade_ids,
            "conflicting_duplicate_broker_trade_ids": source.integrity.duplicate_ids_with_conflicting_values,
            "conflicting_duplicate_source_ids": source.integrity.duplicate_ids_with_conflicting_values,
            "eligibility_decisions": len(ledgers.eligibility_decisions),
            "eligible_trades": len(ledgers.eligible_trades),
            "rejected_trades": len(ledgers.eligibility_decisions) - len(ledgers.eligible_trades),
            "canonical_observations": len(ledgers.canonical_observations),
            "pending_final_sampler_trade": sum(1 for diag in ledgers.diagnostics if diag.diagnostic_type == "pending_final_occupied_second"),
            "renko_bricks": len(ledgers.renko_bricks),
            "strategy_signals": len(ledgers.strategy_signals),
            "queued_orders": len(ledgers.simulated_orders),
            "simulated_orders": len(ledgers.simulated_orders),
            "simulated_fills": len(ledgers.simulated_fills),
            "closed_trades": metrics["closed_trades"],
            "pending_orders_at_end": len(pending_orders),
        },
    }


def _sanity_warnings(*, config: ReplayConfig, ledgers: ReplayLedgers, pending_orders: tuple[Any, ...], source) -> list[ReplayDiagnostic]:
    warnings: list[ReplayDiagnostic] = []
    elapsed_seconds = (config.end - config.start).total_seconds()
    if not ledgers.canonical_observations:
        warnings.append(ReplayDiagnostic("zero_canonical_observations", "Replay emitted zero canonical observations."))
        warnings.append(ReplayDiagnostic("benchmark_missing_observation", "Benchmark requires valid first and final canonical observations."))
    if not ledgers.renko_bricks:
        warnings.append(ReplayDiagnostic("zero_renko_bricks", "Replay emitted zero Renko bricks."))
    if not ledgers.strategy_signals:
        warnings.append(ReplayDiagnostic("zero_signals", "Replay emitted zero strategy signals."))
    if elapsed_seconds < 3600:
        warnings.append(ReplayDiagnostic("short_window", "Replay period is too short for meaningful performance analysis."))
    if not ledgers.closed_trades:
        warnings.append(ReplayDiagnostic("no_completed_trades", "Replay closed zero trades."))
    if config.initial_renko_state is None or config.initial_strategy_state is None:
        warnings.append(ReplayDiagnostic("missing_starting_state_for_parity", "Production parity is not verifiable without matching initial Renko and strategy state."))
    if config.initial_accounting_position is None:
        warnings.append(ReplayDiagnostic("missing_starting_accounting_state", "Production parity is not verifiable without matching initial accounting state."))
    if config.start_state_path and config.start_state_completeness != "complete":
        warnings.append(
            ReplayDiagnostic(
                "incomplete_starting_state_snapshot",
                "Supplied starting-state snapshot is incomplete for full logic parity.",
                details={"start_state_path": config.start_state_path, "completeness": config.start_state_completeness},
            )
        )
    if config.strategy_config.get("force_flat"):
        warnings.append(ReplayDiagnostic("force_flat_not_modeled", "Configured force-flat behavior is not modeled in replay V1."))
    if pending_orders:
        warnings.append(ReplayDiagnostic("pending_orders_at_end", "Replay ended with pending orders that were not force-filled."))
    if source.integrity.window_timestamp_start is None or source.integrity.window_timestamp_end is None:
        warnings.append(ReplayDiagnostic("incomplete_session_coverage", "No source records were found inside the requested session."))
    else:
        if (source.integrity.window_timestamp_start - config.start).total_seconds() > 60:
            warnings.append(ReplayDiagnostic("incomplete_session_coverage", "Source starts materially after requested session start."))
        if (config.end - source.integrity.window_timestamp_end).total_seconds() > 60:
            warnings.append(ReplayDiagnostic("incomplete_session_coverage", "Source ends materially before requested session end."))
    if source.integrity.maximum_timestamp_gap_seconds is not None and source.integrity.maximum_timestamp_gap_seconds > Decimal("60"):
        warnings.append(
            ReplayDiagnostic(
                "large_market_data_gap",
                "Source contains a timestamp gap larger than 60 seconds in the requested session.",
                details={"maximum_timestamp_gap_seconds": str(source.integrity.maximum_timestamp_gap_seconds)},
            )
        )
    if source.integrity.duplicate_rate_pct > Decimal("5"):
        warnings.append(
            ReplayDiagnostic(
                "high_duplicate_source_rate",
                "Historical source duplicate source-identity rate is materially high.",
                details={"duplicate_rate_pct": str(source.integrity.duplicate_rate_pct)},
            )
        )
    if source.integrity.duplicate_ids_with_conflicting_values:
        warnings.append(
            ReplayDiagnostic(
                "conflicting_duplicate_source_ids",
                "Historical source contains conflicting repeated source IDs.",
                "error",
                details={"count": source.integrity.duplicate_ids_with_conflicting_values},
            )
        )
    return warnings


def _result_quality_classification(*, config: ReplayConfig, ledgers: ReplayLedgers, source) -> str:
    elapsed_seconds = (config.end - config.start).total_seconds()
    if not ledgers.canonical_observations or elapsed_seconds < 3600:
        return "SMOKE_VALIDATION"
    if source.integrity.source_kind != "raw_sip_trades":
        return "FULL_SESSION_RESEARCH"
    return "FULL_SESSION_RESEARCH"


def _diagnostics_summary(diagnostics: list[ReplayDiagnostic]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for diagnostic in diagnostics:
        counts[diagnostic.diagnostic_type] = counts.get(diagnostic.diagnostic_type, 0) + 1
    return counts


def _basis_status(config: ReplayConfig) -> str:
    if config.initial_accounting_position is None:
        return "flat_known_zero_basis"
    return "known" if config.initial_accounting_position.basis_known else "UNKNOWN"


def _num_or_none(value: Any) -> int | float | None:
    if value is None:
        return None
    dec = Decimal(str(value))
    if dec == dec.to_integral_value():
        return int(dec)
    return float(dec)


def _sampler_diagnostics(items) -> list[ReplayDiagnostic]:
    return [ReplayDiagnostic(item.diagnostic_type, item.message, "warning", item.raw_trade.sip_timestamp if item.raw_trade else None) for item in items]


def _pending_sampler_diagnostics(items) -> list[ReplayDiagnostic]:
    return [ReplayDiagnostic(item.diagnostic_type, item.message, "warning", item.raw_trade.sip_timestamp if item.raw_trade else None) for item in items]


def _compact_event(run_id: str, event_type: str, timestamp: Any, sequence: int, parent: str | None, payload: dict[str, Any]) -> CanonicalEvent:
    return CanonicalEvent.build(
        event_type=event_type,  # type: ignore[arg-type]
        event_time=timestamp,
        run_id=run_id,
        source="replay",
        symbol=str(payload.get("symbol") or ""),
        sequence=sequence,
        stable_source_identity=f"{event_type}:{parent}:{sequence}",
        parent_event_id=parent,
        payload=payload,
    )


def _payload(value: Any) -> dict[str, Any]:
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, dict):
        return value
    return {"value": str(value)}
