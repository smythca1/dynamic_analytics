from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from .context import ReplayContext
from .ledgers import ReplayLedgers
from .metrics import metric_definitions
from .models import ReplayConfig


def write_outputs(*, config: ReplayConfig, context: ReplayContext, ledgers: ReplayLedgers, metrics: dict[str, Any], manifest: dict[str, Any]) -> Path:
    output_dir = Path(config.output_directory)
    if output_dir.name != config.run_id:
        output_dir = output_dir / config.run_id
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_json(output_dir / "manifest.json", manifest)
    _write_json(output_dir / "metrics.json", {"metrics": metrics, "definitions": metric_definitions()})
    _write_json(output_dir / "diagnostics_summary.json", context.diagnostics_summary)
    (output_dir / "report.txt").write_text(_report_text(metrics, manifest), encoding="utf-8")
    if config.output_level in {"TRADES", "AUDIT"}:
        _write_jsonl(output_dir / "fills.jsonl", ledgers.simulated_fills)
        _write_jsonl(output_dir / "position_transitions.jsonl", ledgers.position_transitions)
        _write_jsonl(output_dir / "closed_trades.jsonl", ledgers.closed_trades)
        _write_csv(output_dir / "equity_curve.csv", ledgers.equity_curve)
    if config.output_level == "AUDIT":
        _write_jsonl(output_dir / "raw_trade_references.jsonl", ledgers.raw_source_trades)
        _write_jsonl(output_dir / "eligibility_decisions.jsonl", ledgers.eligibility_decisions)
        _write_jsonl(output_dir / "eligible_trades.jsonl", ledgers.eligible_trades)
        _write_jsonl(output_dir / "observations_1s.jsonl", ledgers.canonical_observations)
        _write_jsonl(output_dir / "renko_bricks.jsonl", ledgers.renko_bricks)
        _write_jsonl(output_dir / "signals.jsonl", ledgers.strategy_signals)
        _write_jsonl(output_dir / "orders.jsonl", ledgers.simulated_orders)
        _write_jsonl(output_dir / "accounting_snapshots.jsonl", ledgers.accounting_snapshots)
        _write_jsonl(output_dir / "diagnostics.jsonl", ledgers.diagnostics)
    return output_dir


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(_jsonable(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(_jsonable(row), sort_keys=True) + "\n")


def _write_csv(path: Path, rows: list[Any]) -> None:
    serial = [_jsonable(row) for row in rows]
    keys = sorted({key for row in serial if isinstance(row, dict) for key in row})
    with path.open("w", encoding="utf-8") as handle:
        handle.write(",".join(keys) + "\n")
        for row in serial:
            handle.write(",".join(str(row.get(key, "")) for key in keys) + "\n")


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


def _report_text(metrics: dict[str, Any], manifest: dict[str, Any]) -> str:
    brick = manifest["brick_configuration"]
    strategy = manifest["strategy_configuration"]
    start = manifest["starting_state"]
    counts = manifest["counts"]
    source = manifest["source_integrity"]
    lines = [
        "Canonical Replay V1",
        f"Run ID: {manifest['run_id']}",
        f"Result quality: {manifest['result_quality_classification']}",
        f"Source file: {manifest['source_dataset_reference']}",
        f"Source hash: {manifest['source_dataset_hash']}",
        f"Source semantic mode: {manifest.get('source_semantic_mode')}",
        f"Configuration hash: {manifest.get('configuration_hash')}",
        f"Replay fingerprint: {manifest.get('replay_fingerprint')}",
        f"Source kind: {source['source_kind']}",
        f"Symbol: {manifest['symbol']}",
        f"Actual start: {manifest['start']}",
        f"Actual end: {manifest['end']}",
        f"Source rows scanned: {counts['source_rows_scanned']}",
        f"Raw trades in window: {counts['raw_trades_in_window']}",
        f"Malformed rows: {counts['malformed_rows']}",
        f"Exact duplicate rows: {counts['exact_duplicate_rows']}",
        f"Duplicate source ID records: {counts['duplicate_source_id_records']}",
        f"Duplicate broker trade ID records: {counts['duplicate_broker_trade_id_records']}",
        f"Conflicting duplicate broker trade IDs: {counts['conflicting_duplicate_broker_trade_ids']}",
        f"Conflicting duplicate source IDs: {counts['conflicting_duplicate_source_ids']}",
        f"Eligible trades: {counts['eligible_trades']}",
        f"Rejected trades: {counts['rejected_trades']}",
        f"Canonical observations: {counts['canonical_observations']}",
        f"Pending final sampler trade: {counts['pending_final_sampler_trade']}",
        f"Renko bricks: {counts['renko_bricks']}",
        f"Strategy signals: {counts['strategy_signals']}",
        f"Queued orders: {counts['queued_orders']}",
        f"Simulated fills: {counts['simulated_fills']}",
        f"Closed trades: {metrics['closed_trades']}",
        f"Pending orders: {manifest['pending_order_count']}",
        f"Source config: {strategy['source_config_path']}",
        f"Selected trader ID: {strategy['selected_trader_id']}",
        f"Strategy type: {strategy['strategy_type']}",
        f"Strategy name: {strategy['strategy_name']}",
        f"Configured Renko source: {strategy['configured_renko_source']}",
        f"Entry bricks: {strategy['entry_bricks']}",
        f"Reversal behavior: {strategy['reversal_behavior']}",
        f"Renko anchor: {strategy['renko_anchor_mode']} @ {strategy['renko_anchor_price']}",
        f"Effective brick size: ${brick['effective_brick_size_dollars']}",
        f"Brick sizing mode: {strategy['brick_sizing_mode']}",
        f"Authoritative brick field: {strategy['authoritative_brick_field']}",
        f"Reference price policy: {strategy['reference_price_policy']}",
        f"Configured brick percent: {strategy['configured_brick_pct'] if strategy['configured_brick_pct'] is not None else brick['brick_size_ltp_percent']}%",
        f"Derived brick percent: {brick['derived_brick_size_ltp_percent']}%",
        f"Brick reference: ${brick['reference_price']} at {brick['reference_timestamp']} ({brick['reference_source']})",
        f"Quantity: {strategy['qty']}",
        f"Session/overnight: {strategy['session_mode']} / allow_overnight={strategy['allow_overnight']} / entry={strategy['entry_start']}-{strategy['entry_end']}",
        f"Force-flat parity: {strategy['force_flat_parity']} ({strategy['force_flat']})",
        f"Starting position: {start['position']}",
        f"Starting basis status: {start['basis_status']}",
        "Execution model: NEXT_CANONICAL_OBSERVATION_V1",
        "Calibration period: NOT APPLICABLE",
        "Matched production fills: 0",
        "Signal-to-order latency: UNKNOWN - not modeled",
        f"Order-to-fill latency mean: {metrics['order_to_fill_latency_mean_seconds']}",
        f"Order-to-fill latency median: {metrics['order_to_fill_latency_median_seconds']}",
        f"Order-to-fill latency P95: {metrics['order_to_fill_latency_p95_seconds']}",
        "Total signal-to-fill latency: Same as order-to-fill latency in V1",
        "Average adverse slippage: UNKNOWN - production calibration not implemented",
        "Average slippage per closed trade: UNKNOWN - production calibration not implemented",
        "Slippage dollars per share: UNKNOWN",
        "Slippage basis points: UNKNOWN",
        "Spread: UNKNOWN",
        "Fees and commissions: UNKNOWN unless explicitly configured",
        "Partial fills: NOT MODELED",
        "Unfilled-order behavior: Pending at end of replay and not force-filled",
        "Gross signal-model PnL: UNKNOWN unless an explicit idealized comparison is run",
        f"Net simulated PnL: ${metrics['net_pnl_dollars']}",
        f"Ending equity: ${metrics['ending_equity_dollars']}",
        f"Buy-and-hold PnL: ${metrics['buy_hold_pnl_dollars']}",
        f"Buy-and-hold return: {metrics['buy_hold_return_pct']}%",
        f"Strategy return: {metrics['strategy_return_pct']}%",
        "Production parity: NOT YET CALIBRATED",
    ]
    return "\n".join(lines) + "\n"
