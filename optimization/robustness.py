from __future__ import annotations

from decimal import Decimal
from statistics import median, pstdev
from typing import Any

from .models import ROBUSTNESS_RULES_VERSION


def annotate_robustness(
    rows: list[dict[str, Any]],
    *,
    minimum_closed_trades: int,
    max_drawdown_pct_threshold: Decimal,
    local_profitable_pct_threshold: Decimal,
) -> list[dict[str, Any]]:
    by_trader: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_trader.setdefault(str(row["trader_id"]), []).append(row)
    annotated: list[dict[str, Any]] = []
    for trader_rows in by_trader.values():
        grid = [row for row in trader_rows if row.get("candidate_type") == "BRICK_PERCENT_GRID" and row.get("candidate_status") == "COMPLETED"]
        by_parameter_pct = {(_parameter_key(row), Decimal(str(row["brick_pct"]))): row for row in grid if row.get("brick_pct") not in {None, ""}}
        by_entry_sl_pct: dict[tuple[str, str, Decimal], list[dict[str, Any]]] = {}
        by_entry_tp_pct: dict[tuple[str, str, Decimal], list[dict[str, Any]]] = {}
        by_tp_sl_pct: dict[tuple[str, str, Decimal], list[dict[str, Any]]] = {}
        for candidate in grid:
            pct_key = Decimal(str(candidate["brick_pct"]))
            entry, tp, sl = _parameter_key(candidate)
            by_entry_sl_pct.setdefault((entry, sl, pct_key), []).append(candidate)
            by_entry_tp_pct.setdefault((entry, tp, pct_key), []).append(candidate)
            by_tp_sl_pct.setdefault((tp, sl, pct_key), []).append(candidate)
        max_pnl = max((_dec(row.get("net_pnl_dollars")) for row in grid), default=Decimal("0"))
        min_pnl = min((_dec(row.get("net_pnl_dollars")) for row in grid), default=Decimal("0"))
        pnl_span = max(max_pnl - min_pnl, Decimal("1"))
        for row in trader_rows:
            enriched = dict(row)
            if row.get("candidate_type") == "BRICK_PERCENT_GRID" and row.get("candidate_status") == "COMPLETED":
                pct = Decimal(str(row["brick_pct"]))
                parameters = _parameter_key(row)
                lower = by_parameter_pct.get((parameters, pct - Decimal("0.01")))
                upper = by_parameter_pct.get((parameters, pct + Decimal("0.01")))
                neighbors = [
                    candidate
                    for candidate in grid
                    if abs(Decimal(str(candidate["brick_pct"])) - pct) <= Decimal("0.05")
                    and _parameter_key(candidate) == parameters
                ]
                tp_stability = _stability(row, by_entry_sl_pct.get((parameters[0], parameters[2], pct), []))
                sl_stability = _stability(row, by_entry_tp_pct.get((parameters[0], parameters[1], pct), []))
                entry_stability = _stability(row, by_tp_sl_pct.get((parameters[1], parameters[2], pct), []))
                pnls = [_dec(candidate.get("net_pnl_dollars")) for candidate in neighbors]
                returns = [_dec(candidate.get("strategy_return_pct")) for candidate in neighbors]
                profitable = [value for value in pnls if value > 0]
                local_profitable_pct = Decimal(len(profitable)) / Decimal(len(neighbors)) * Decimal("100") if neighbors else Decimal("0")
                local_avg_pnl = sum(pnls, Decimal("0")) / Decimal(len(pnls)) if pnls else None
                local_median_pnl = Decimal(str(median(pnls))) if pnls else None
                local_pnl_stddev = Decimal(str(pstdev([float(value) for value in pnls]))) if len(pnls) > 1 else Decimal("0")
                local_return_stddev = Decimal(str(pstdev([float(value) for value in returns]))) if len(returns) > 1 else Decimal("0")
                isolated = bool(pnls and _dec(row.get("net_pnl_dollars")) > 0 and local_median_pnl is not None and local_median_pnl <= 0)
                plateau = bool(local_profitable_pct >= local_profitable_pct_threshold and not isolated)
                eligibility, reasons = _eligibility(
                    row,
                    local_profitable_pct=local_profitable_pct,
                    isolated=isolated,
                    minimum_closed_trades=minimum_closed_trades,
                    max_drawdown_pct_threshold=max_drawdown_pct_threshold,
                    local_profitable_pct_threshold=local_profitable_pct_threshold,
                )
                normalized_pnl = (_dec(row.get("net_pnl_dollars")) - min_pnl) / pnl_span
                drawdown_penalty = max(Decimal("0"), _dec(row.get("max_drawdown_pct")) / max_drawdown_pct_threshold) if max_drawdown_pct_threshold else Decimal("0")
                stability = max(Decimal("0"), Decimal("1") - (local_pnl_stddev / pnl_span))
                outperformance = Decimal("1") if _dec(row.get("strategy_outperformance_dollars")) > 0 else Decimal("0")
                score = (
                    normalized_pnl * Decimal("0.35")
                    + (local_profitable_pct / Decimal("100")) * Decimal("0.25")
                    + stability * Decimal("0.20")
                    + outperformance * Decimal("0.10")
                    + max(Decimal("0"), Decimal("1") - drawdown_penalty) * Decimal("0.10")
                )
                strategy_stability = _average_known(tp_stability["score"], sl_stability["score"], entry_stability["score"])
                overall_score = (score * Decimal("0.70")) + (strategy_stability * Decimal("0.30"))
                enriched.update(
                    {
                        "robustness_rules_version": ROBUSTNESS_RULES_VERSION,
                        "robustness_parameter_scope": "exact_entry_tp_sl_brick_neighbors_v1",
                        "lower_neighbor_candidate_id": lower.get("candidate_id") if lower else None,
                        "upper_neighbor_candidate_id": upper.get("candidate_id") if upper else None,
                        "lower_neighbor_net_pnl_dollars": lower.get("net_pnl_dollars") if lower else "UNKNOWN",
                        "upper_neighbor_net_pnl_dollars": upper.get("net_pnl_dollars") if upper else "UNKNOWN",
                        "lower_neighbor_return_pct": lower.get("strategy_return_pct") if lower else "UNKNOWN",
                        "upper_neighbor_return_pct": upper.get("strategy_return_pct") if upper else "UNKNOWN",
                        "lower_neighbor_profit_factor": lower.get("profit_factor") if lower else "UNKNOWN",
                        "upper_neighbor_profit_factor": upper.get("profit_factor") if upper else "UNKNOWN",
                        "local_neighbor_count": len(neighbors),
                        "local_profitable_count": len(profitable),
                        "local_profitable_pct": _num(local_profitable_pct),
                        "local_average_net_pnl_dollars": _num(local_avg_pnl),
                        "local_median_net_pnl_dollars": _num(local_median_pnl),
                        "local_min_net_pnl_dollars": _num(min(pnls)) if pnls else "UNKNOWN",
                        "local_max_net_pnl_dollars": _num(max(pnls)) if pnls else "UNKNOWN",
                        "local_average_return_pct": _num(sum(returns, Decimal("0")) / Decimal(len(returns))) if returns else "UNKNOWN",
                        "local_return_stddev": _num(local_return_stddev),
                        "local_pnl_stddev": _num(local_pnl_stddev),
                        "local_max_drawdown_pct": _num(max(_dec(candidate.get("max_drawdown_pct")) for candidate in neighbors)) if neighbors else "UNKNOWN",
                        "brick_neighbor_robustness": _num(score),
                        "tp_parameter_stability": _num(tp_stability["score"]),
                        "tp_parameter_neighbor_count": tp_stability["count"],
                        "sl_parameter_stability": _num(sl_stability["score"]),
                        "sl_parameter_neighbor_count": sl_stability["count"],
                        "entry_parameter_stability": _num(entry_stability["score"]),
                        "entry_parameter_neighbor_count": entry_stability["count"],
                        "overall_strategy_robustness": _num(overall_score),
                        "isolated_spike_flag": isolated,
                        "stable_plateau_flag": plateau,
                        "robustness_score": _num(overall_score),
                        "robustness_eligible": eligibility,
                        "eligibility_reasons": ";".join(reasons),
                    }
                )
            else:
                enriched.update(_empty_robustness())
            annotated.append(enriched)
    return annotated


def _eligibility(
    row: dict[str, Any],
    *,
    local_profitable_pct: Decimal,
    isolated: bool,
    minimum_closed_trades: int,
    max_drawdown_pct_threshold: Decimal,
    local_profitable_pct_threshold: Decimal,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if int(row.get("closed_trades") or 0) < minimum_closed_trades:
        reasons.append("below_minimum_closed_trades")
    if _dec(row.get("net_pnl_dollars")) <= 0:
        reasons.append("non_positive_net_pnl")
    if _dec(row.get("profit_factor")) <= 1:
        reasons.append("profit_factor_not_above_one")
    if _dec(row.get("max_drawdown_pct")) > max_drawdown_pct_threshold:
        reasons.append("max_drawdown_exceeds_threshold")
    if local_profitable_pct < local_profitable_pct_threshold:
        reasons.append("local_profitability_below_threshold")
    if isolated:
        reasons.append("isolated_spike")
    if row.get("candidate_status") != "COMPLETED":
        reasons.append("candidate_not_completed")
    return not reasons, reasons or ["eligible"]


def _empty_robustness() -> dict[str, Any]:
    return {
        "robustness_rules_version": ROBUSTNESS_RULES_VERSION,
        "robustness_parameter_scope": "exact_entry_tp_sl_brick_neighbors_v1",
        "lower_neighbor_candidate_id": None,
        "upper_neighbor_candidate_id": None,
        "lower_neighbor_net_pnl_dollars": "UNKNOWN",
        "upper_neighbor_net_pnl_dollars": "UNKNOWN",
        "lower_neighbor_return_pct": "UNKNOWN",
        "upper_neighbor_return_pct": "UNKNOWN",
        "lower_neighbor_profit_factor": "UNKNOWN",
        "upper_neighbor_profit_factor": "UNKNOWN",
        "local_neighbor_count": 0,
        "local_profitable_count": 0,
        "local_profitable_pct": "UNKNOWN",
        "local_average_net_pnl_dollars": "UNKNOWN",
        "local_median_net_pnl_dollars": "UNKNOWN",
        "local_min_net_pnl_dollars": "UNKNOWN",
        "local_max_net_pnl_dollars": "UNKNOWN",
        "local_average_return_pct": "UNKNOWN",
        "local_return_stddev": "UNKNOWN",
        "local_pnl_stddev": "UNKNOWN",
        "local_max_drawdown_pct": "UNKNOWN",
        "brick_neighbor_robustness": "UNKNOWN",
        "tp_parameter_stability": "UNKNOWN",
        "tp_parameter_neighbor_count": 0,
        "sl_parameter_stability": "UNKNOWN",
        "sl_parameter_neighbor_count": 0,
        "entry_parameter_stability": "UNKNOWN",
        "entry_parameter_neighbor_count": 0,
        "overall_strategy_robustness": "UNKNOWN",
        "isolated_spike_flag": False,
        "stable_plateau_flag": False,
        "robustness_score": "UNKNOWN",
        "robustness_eligible": False,
        "eligibility_reasons": "not_grid_candidate",
    }


def _dec(value: Any) -> Decimal:
    if value in {None, "", "UNKNOWN"}:
        return Decimal("0")
    return Decimal(str(value))


def _parameter_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("entry_confirmation") or "UNKNOWN"),
        str(row.get("take_profit_bricks") or "UNKNOWN"),
        str(row.get("stop_loss_bricks") or "UNKNOWN"),
    )


def _stability(center: dict[str, Any], neighbors: list[dict[str, Any]]) -> dict[str, Any]:
    comparable = [row for row in neighbors if row.get("candidate_id") != center.get("candidate_id")]
    if not comparable:
        return {"score": Decimal("0"), "count": 0}
    center_pnl = _dec(center.get("net_pnl_dollars"))
    values = [_dec(row.get("net_pnl_dollars")) for row in comparable]
    avg_abs_delta = sum((abs(value - center_pnl) for value in values), Decimal("0")) / Decimal(len(values))
    scale = max(abs(center_pnl), max((abs(value) for value in values), default=Decimal("0")), Decimal("1"))
    score = max(Decimal("0"), Decimal("1") - (avg_abs_delta / scale))
    return {"score": score, "count": len(comparable)}


def _average_known(*values: Decimal) -> Decimal:
    return sum(values, Decimal("0")) / Decimal(len(values)) if values else Decimal("0")


def _num(value: Decimal | None) -> float | str:
    if value is None:
        return "UNKNOWN"
    if value == value.to_integral_value():
        return int(value)
    return float(value)
