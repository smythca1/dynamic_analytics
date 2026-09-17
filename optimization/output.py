from __future__ import annotations

import csv
import json
from dataclasses import asdict, is_dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from .models import OPTIMIZER_VERSION, OptimizationConfig, PreparedSource, TraderSelection
from .recommendations import recommendation_labels


def write_optimization_outputs(
    *,
    config: OptimizationConfig,
    traders: tuple[TraderSelection, ...],
    prepared_sources: dict[str, PreparedSource],
    rows: list[dict[str, Any]],
    recommendations: dict[str, Any],
    cross_asset_summary: dict[str, Any],
    diagnostics_summary: dict[str, int],
    preflight: dict[str, Any] | None = None,
) -> Path:
    out = config.output_path
    out.mkdir(parents=True, exist_ok=True)
    manifest = {
        "optimization_id": config.optimization_id,
        "optimizer_version": OPTIMIZER_VERSION,
        "production_config_path": config.production_config_path,
        "session_dates": [item.isoformat() for item in config.session_dates],
        "included_traders": [asdict(item) for item in traders],
        "candidate_count": len(rows),
        "preflight_only": config.preflight_only,
        "warmup_policy": config.warmup_policy,
        "execution_model": config.execution_model,
        "position_sizing_mode": config.position_sizing_mode,
        "parameter_grid_mode": config.parameter_grid_mode,
        "initial_capital_dollars": str(config.initial_capital),
        "capital_metric_conventions": {
            "quantity": "floor(initial_capital / reference_price) whole shares when CAPITAL_ALLOCATED_WHOLE_SHARES_V1 is selected",
            "average_deployed_capital_dollars": "equal-weighted over scored canonical observations",
            "average_trade_size_dollars": "average absolute simulated fill notional",
            "turnover_dollars": "sum of absolute simulated fill notional values",
        },
        "brick_grid": {
            "start_pct": str(config.brick_start_pct),
            "end_pct": str(config.brick_end_pct),
            "step_pct_points": str(config.brick_step_pct_points),
        },
        "strategy_parameter_grid": {
            "mode": config.parameter_grid_mode,
            "baseline_strategy_family": "DIRECT_REVERSAL_BASELINE",
            "grid_strategy_family": "FLATTENING_TP_SL_STRATEGY" if config.parameter_grid_mode == "RENKO_TP_SL_ENTRY_TP_SL_GRID_V1" else "production",
            "entry_confirmation": ["E1", "E2"] if config.parameter_grid_mode == "RENKO_TP_SL_ENTRY_TP_SL_GRID_V1" else ["production"],
            "take_profit_bricks": [1, 2, 3] if config.parameter_grid_mode == "RENKO_TP_SL_ENTRY_TP_SL_GRID_V1" else ["production"],
            "stop_loss_bricks": [1, 2, 3] if config.parameter_grid_mode == "RENKO_TP_SL_ENTRY_TP_SL_GRID_V1" else ["production"],
            "selected_entry_confirmations": list(config.selected_entry_confirmations) if config.selected_entry_confirmations else None,
            "selected_take_profit_bricks": list(config.selected_take_profit_bricks) if config.selected_take_profit_bricks else None,
            "selected_stop_loss_bricks": list(config.selected_stop_loss_bricks) if config.selected_stop_loss_bricks else None,
            "include_current_production": config.include_current_production,
        },
        "output_level": config.output_level,
        "process_control": {
            "process_count": config.process_count,
            "candidate_batch_size": config.candidate_batch_size,
            "host_role": config.host_role,
            "max_memory_gb": str(config.max_memory_gb) if config.max_memory_gb is not None else None,
            "checkpoint_every_candidates": config.checkpoint_every_candidates,
            "progress_every_seconds": config.progress_every_seconds,
            "resume": config.resume,
            "retry_failed": config.retry_failed,
        },
        "result_quality": "FULL_SESSION_RESEARCH" if rows else "PREFLIGHT_ONLY",
        "production_parity": "NOT_CLAIMED",
    }
    _write_json(out / "optimization_manifest.json", manifest)
    _write_json(out / "prepared_sources.json", {symbol: asdict(source) for symbol, source in prepared_sources.items()})
    _write_json(out / "recommendations.json", recommendations)
    _write_json(out / "cross_asset_summary.json", cross_asset_summary)
    _write_json(out / "diagnostics_summary.json", diagnostics_summary)
    if preflight is not None:
        _write_json(out / "preflight.json", preflight)
    labels = recommendation_labels(recommendations)
    rows_with_labels = []
    for row in rows:
        enriched = dict(row)
        enriched["recommendation_labels"] = ";".join(sorted(labels.get(str(row.get("candidate_id")), set())))
        rows_with_labels.append(enriched)
    _write_csv(out / "all_candidates.csv", rows_with_labels)
    _write_csv(out / "heatmap_data.csv", _heatmap_rows(rows_with_labels))
    _write_csv(out / "asset_summary.csv", _asset_summary_rows(rows_with_labels, recommendations))
    _write_csv(out / "top_three_by_asset.csv", _top_three_rows(recommendations))
    _write_csv(out / "strategy_family_summary.csv", _strategy_family_summary_rows(rows_with_labels))
    _write_csv(out / "parameter_combination_summary.csv", _parameter_combination_summary_rows(rows_with_labels))
    _write_json(out / "top_100_rankings.json", _top_100_rankings(rows_with_labels))
    report = _report_text(manifest, recommendations, cross_asset_summary, diagnostics_summary, preflight)
    (out / "report.txt").write_text(report, encoding="utf-8")
    (out / "report.html").write_text("<pre>" + report.replace("&", "&amp;").replace("<", "&lt;") + "</pre>\n", encoding="utf-8")
    return out


def _asset_summary_rows(rows: list[dict[str, Any]], recommendations: dict[str, Any]) -> list[dict[str, Any]]:
    result = []
    for trader_id, payload in sorted(recommendations.items()):
        recommended = payload.get("recommended_robust") or {}
        highest = payload.get("highest_net_pnl") or {}
        current = payload.get("current_production") or {}
        matching = [row for row in rows if row.get("trader_id") == trader_id]
        first = matching[0] if matching else {}
        result.append(
            {
                "asset": first.get("symbol"),
                "trader_id": trader_id,
                "current_production_brick_size_dollars": current.get("brick_size_dollars"),
                "recommended_robust_brick_pct": recommended.get("brick_pct"),
                "recommended_entry_confirmation": recommended.get("entry_confirmation"),
                "recommended_take_profit_bricks": recommended.get("take_profit_bricks"),
                "recommended_stop_loss_bricks": recommended.get("stop_loss_bricks"),
                "recommended_brick_dollars": recommended.get("brick_size_dollars"),
                "highest_pnl_brick_pct": highest.get("brick_pct"),
                "highest_pnl_entry_confirmation": highest.get("entry_confirmation"),
                "highest_pnl_take_profit_bricks": highest.get("take_profit_bricks"),
                "highest_pnl_stop_loss_bricks": highest.get("stop_loss_bricks"),
                "net_pnl_dollars": recommended.get("net_pnl_dollars"),
                "return_pct": recommended.get("return_on_total_account_equity_pct") or recommended.get("strategy_return_pct"),
                "return_on_allocated_capital_pct": recommended.get("return_on_allocated_capital_pct"),
                "buy_hold_return_pct": _lookup(rows, recommended.get("candidate_id"), "buy_hold_return_pct"),
                "outperformance_percentage_points": _lookup(rows, recommended.get("candidate_id"), "strategy_outperformance_percentage_points"),
                "profit_factor": recommended.get("profit_factor"),
                "win_rate_pct": _lookup(rows, recommended.get("candidate_id"), "win_rate_pct"),
                "closed_trades": recommended.get("closed_trades"),
                "max_drawdown_pct": recommended.get("max_drawdown_pct"),
                "sharpe_ratio": _lookup(rows, recommended.get("candidate_id"), "sharpe_ratio"),
                "sortino_ratio": _lookup(rows, recommended.get("candidate_id"), "sortino_ratio"),
                "robustness_score": recommended.get("robustness_score"),
                "sensitivity_classification": _lookup(rows, recommended.get("candidate_id"), "sensitivity_classification"),
                "allocated_capital_dollars": _lookup(rows, recommended.get("candidate_id"), "allocated_capital_dollars"),
                "average_deployed_capital_dollars": _lookup(rows, recommended.get("candidate_id"), "average_deployed_capital_dollars"),
                "average_exposure_pct": _lookup(rows, recommended.get("candidate_id"), "average_exposure_pct"),
                "data_quality_classification": "SEE_PREPARED_SOURCE",
                "result_quality_classification": _lookup(rows, recommended.get("candidate_id"), "result_quality"),
            }
        )
    return result


def _heatmap_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fields = [
        "symbol",
        "trader_id",
        "brick_pct",
        "strategy_family",
        "strategy_type",
        "entry_confirmation",
        "entry_bricks",
        "take_profit_confirmation",
        "take_profit_bricks",
        "stop_loss_confirmation",
        "stop_loss_bricks",
        "net_pnl_dollars",
        "strategy_return_pct",
        "max_drawdown_pct",
        "profit_factor",
        "closed_trades",
        "local_profitable_pct",
        "robustness_score",
        "brick_neighbor_robustness",
        "tp_parameter_stability",
        "sl_parameter_stability",
        "entry_parameter_stability",
        "overall_strategy_robustness",
        "robustness_eligible",
        "sensitivity_index",
        "sensitivity_classification",
        "brick_sensitivity_index",
        "brick_sensitivity_classification",
        "tp_sensitivity_index",
        "tp_sensitivity_classification",
        "sl_sensitivity_index",
        "sl_sensitivity_classification",
        "entry_sensitivity_index",
        "entry_sensitivity_classification",
        "execution_cost_sensitive",
        "allocated_capital_dollars",
        "return_on_allocated_capital_pct",
        "return_on_total_account_equity_pct",
        "capital_utilization_pct",
        "recommendation_labels",
    ]
    return [{field: row.get(field) for field in fields} for row in rows if row.get("candidate_type") == "BRICK_PERCENT_GRID"]


def _strategy_family_summary_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, Any], list[dict[str, Any]]] = {}
    for row in rows:
        if row.get("candidate_status") == "COMPLETED":
            groups.setdefault((row.get("symbol"), row.get("strategy_family")), []).append(row)
    return [_summary_row({"symbol": symbol, "strategy_family": family}, grouped) for (symbol, family), grouped in sorted(groups.items())]


def _parameter_combination_summary_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, Any, Any, Any, Any], list[dict[str, Any]]] = {}
    for row in rows:
        if row.get("candidate_status") == "COMPLETED" and row.get("candidate_type") == "BRICK_PERCENT_GRID":
            key = (
                row.get("symbol"),
                row.get("strategy_family"),
                row.get("entry_confirmation"),
                row.get("take_profit_confirmation"),
                row.get("stop_loss_confirmation"),
            )
            groups.setdefault(key, []).append(row)
    return [
        _summary_row(
            {
                "symbol": symbol,
                "strategy_family": family,
                "entry_confirmation": entry,
                "take_profit_confirmation": tp,
                "stop_loss_confirmation": sl,
            },
            grouped,
        )
        for (symbol, family, entry, tp, sl), grouped in sorted(groups.items())
    ]


def _summary_row(identity: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    best = max(rows, key=lambda row: (Decimal(str(row.get("net_pnl_dollars") or 0)), str(row.get("candidate_id"))))
    eligible = [row for row in rows if row.get("robustness_eligible") is True]
    return {
        **identity,
        "candidate_count": len(rows),
        "eligible_count": len(eligible),
        "best_candidate_id": best.get("candidate_id"),
        "best_brick_pct": best.get("brick_pct"),
        "best_net_pnl_dollars": best.get("net_pnl_dollars"),
        "best_profit_factor": best.get("profit_factor"),
        "best_max_drawdown_pct": best.get("max_drawdown_pct"),
        "average_net_pnl_dollars": _average(rows, "net_pnl_dollars"),
        "profitable_count": sum(1 for row in rows if Decimal(str(row.get("net_pnl_dollars") or 0)) > 0),
    }


def _average(rows: list[dict[str, Any]], key: str) -> Any:
    values = [Decimal(str(row.get(key))) for row in rows if row.get(key) not in {None, "", "UNKNOWN"}]
    if not values:
        return "UNKNOWN"
    value = sum(values, Decimal("0")) / Decimal(len(values))
    return int(value) if value == value.to_integral_value() else float(value)


def _lookup(rows: list[dict[str, Any]], candidate_id: Any, key: str) -> Any:
    for row in rows:
        if row.get("candidate_id") == candidate_id:
            return row.get(key)
    return None


def _top_three_rows(recommendations: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for trader_id, payload in sorted(recommendations.items()):
        for index, item in enumerate(payload.get("top_three_primary") or [], start=1):
            if isinstance(item, dict):
                rows.append({"trader_id": trader_id, "rank": index, **item})
    return rows


def _top_100_rankings(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    completed = [row for row in rows if row.get("candidate_status") == "COMPLETED"]
    return {
        "robustness": [_ranking_identity(row) for row in _rank(completed, "robustness_score")[:100]],
        "net_pnl": [_ranking_identity(row) for row in _rank(completed, "net_pnl_dollars")[:100]],
        "profit_factor": [_ranking_identity(row) for row in _rank(completed, "profit_factor")[:100]],
        "return_pct": [_ranking_identity(row) for row in _rank(completed, "strategy_return_pct")[:100]],
        "sharpe": [_ranking_identity(row) for row in _rank(completed, "sharpe_ratio")[:100]],
        "sortino": [_ranking_identity(row) for row in _rank(completed, "sortino_ratio")[:100]],
    }


def _rank(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    values = [row for row in rows if row.get(key) not in {None, "", "UNKNOWN"}]
    return sorted(values, key=lambda row: (Decimal(str(row.get(key))), str(row.get("candidate_id"))), reverse=True)


def _ranking_identity(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": row.get("candidate_id"),
        "candidate_type": row.get("candidate_type"),
        "symbol": row.get("symbol"),
        "trader_id": row.get("trader_id"),
        "strategy_family": row.get("strategy_family"),
        "strategy_type": row.get("strategy_type"),
        "brick_pct": row.get("brick_pct"),
        "brick_size_dollars": row.get("brick_size_dollars"),
        "entry_confirmation": row.get("entry_confirmation"),
        "take_profit_bricks": row.get("take_profit_bricks"),
        "stop_loss_bricks": row.get("stop_loss_bricks"),
        "net_pnl_dollars": row.get("net_pnl_dollars"),
        "strategy_return_pct": row.get("strategy_return_pct"),
        "profit_factor": row.get("profit_factor"),
        "max_drawdown_pct": row.get("max_drawdown_pct"),
        "win_rate_pct": row.get("win_rate_pct"),
        "closed_trades": row.get("closed_trades"),
        "sharpe_ratio": row.get("sharpe_ratio"),
        "sortino_ratio": row.get("sortino_ratio"),
        "robustness_score": row.get("robustness_score"),
    }


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(_jsonable(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


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


def _report_text(
    manifest: dict[str, Any],
    recommendations: dict[str, Any],
    cross_asset_summary: dict[str, Any],
    diagnostics_summary: dict[str, int],
    preflight: dict[str, Any] | None,
) -> str:
    lines = [
        "Canonical Multi-Asset Renko Optimizer",
        f"Optimization ID: {manifest['optimization_id']}",
        f"Optimizer version: {manifest['optimizer_version']}",
        f"Result quality: {manifest['result_quality']}",
        "Execution model: NEXT_CANONICAL_OBSERVATION_V1",
        "Execution disclosure: same-observation fills prohibited; partial fills not modeled; slippage/spread/fees UNKNOWN; not CALIBRATED_LIVE_V1.",
        "Production parity: NOT CLAIMED",
        "Strategy-family disclosure: CURRENT_PRODUCTION_REVERSAL_CAPITAL_ALLOCATED is a direct reversal baseline; BRICK_PERCENT_GRID candidates use canonical renko_tp_sl and flatten before later re-entry.",
        f"Candidate count: {manifest['candidate_count']}",
        f"Warm-up policy: {manifest['warmup_policy']}",
        f"Position sizing: {manifest['position_sizing_mode']}",
        "Classification: RESEARCH ONLY; NEXT_CANONICAL_OBSERVATION_V1; NOT CALIBRATED.",
        "Deployment Impact: HYPOTHETICAL CAPITAL-ALLOCATED RESEARCH IMPACT only; live production quantity may differ.",
        f"Diagnostics: {diagnostics_summary}",
        f"Cross-asset summary: {cross_asset_summary}",
        "Recommendations:",
        json.dumps(recommendations, indent=2, sort_keys=True),
    ]
    if preflight is not None:
        lines.extend(["Preflight:", json.dumps(preflight, indent=2, sort_keys=True, default=str)])
    return "\n".join(lines) + "\n"
