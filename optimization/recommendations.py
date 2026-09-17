from __future__ import annotations

from decimal import Decimal
from typing import Any


def build_recommendations(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_trader: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_trader.setdefault(str(row["trader_id"]), []).append(row)
    result: dict[str, Any] = {}
    for trader_id, trader_rows in sorted(by_trader.items()):
        completed = [row for row in trader_rows if row.get("candidate_status") == "COMPLETED"]
        research_completed = [row for row in completed if row.get("candidate_type") == "BRICK_PERCENT_GRID"]
        eligible = [row for row in research_completed if row.get("robustness_eligible") is True and row.get("execution_cost_sensitive") is not True]
        fallback_eligible = [row for row in research_completed if row.get("robustness_eligible") is True]
        eligible_pool = eligible or fallback_eligible
        min_trade_completed = [row for row in research_completed if int(row.get("closed_trades") or 0) >= 10]
        profitable_min_trade = [row for row in min_trade_completed if _dec(row.get("net_pnl_dollars")) > 0]
        current = _first([row for row in completed if row.get("candidate_type") in {"CURRENT_PRODUCTION", "CURRENT_PRODUCTION_CAPITAL_ALLOCATED", "CURRENT_PRODUCTION_REVERSAL_CAPITAL_ALLOCATED"}])
        best_overall = _max(eligible_pool or research_completed, "net_pnl_dollars")
        best_e1 = _max([row for row in eligible_pool or research_completed if row.get("entry_confirmation") == "E1"], "net_pnl_dollars")
        best_e2 = _max([row for row in eligible_pool or research_completed if row.get("entry_confirmation") == "E2"], "net_pnl_dollars")
        best_pf = _max(min_trade_completed, "profit_factor")
        lowest_drawdown = _min(profitable_min_trade, "max_drawdown_pct")
        best_sharpe = _max(eligible_pool or research_completed, "sharpe_ratio")
        best_sortino = _max(eligible_pool or research_completed, "sortino_ratio")
        recommended = _max(eligible_pool, "robustness_score")
        best_risk_adjusted = _best_risk_adjusted(eligible_pool)
        primary = _distinct(
            [best_overall, recommended, best_risk_adjusted]
            + sorted(eligible_pool, key=lambda row: (_dec(row.get("robustness_score")), _dec(row.get("return_on_allocated_capital_pct")), str(row.get("candidate_id"))), reverse=True)
            + sorted(research_completed, key=lambda row: (_dec(row.get("net_pnl_dollars")), str(row.get("candidate_id"))), reverse=True),
            limit=3,
        )
        result[trader_id] = {
            "current_production": _identity(current),
            "current_production_capital_allocated": _identity(current) if current and current.get("candidate_type") == "CURRENT_PRODUCTION_CAPITAL_ALLOCATED" else None,
            "best_overall": _identity(best_overall),
            "best_e1": _identity(best_e1),
            "best_e2": _identity(best_e2),
            "best_pf": _identity(best_pf),
            "best_sharpe": _identity(best_sharpe),
            "best_sortino": _identity(best_sortino),
            "lowest_drawdown": _identity(lowest_drawdown),
            "highest_net_pnl": _identity(best_overall),
            "highest_return": _identity(_max(eligible or completed, "strategy_return_pct")),
            "highest_profit_factor_min_trades": _identity(best_pf),
            "lowest_drawdown_profitable_min_trades": _identity(lowest_drawdown),
            "highest_sharpe": _identity(best_sharpe),
            "highest_sortino": _identity(best_sortino),
            "recommended_robust": _identity(recommended),
            "best_risk_adjusted_deployable": _identity(best_risk_adjusted),
            "top_three_primary": [_identity(row) for row in primary],
            "lowest_execution_cost_sensitivity": _identity(_min([row for row in research_completed if row.get("execution_cost_sensitive") is not True], "turnover_multiple")),
            "recommendation_note": "Robust recommendation ranks eligible renko_tp_sl research candidates by combined brick and parameter robustness; current production reversal is a separate baseline.",
        }
    return result


def recommendation_labels(recommendations: dict[str, Any]) -> dict[str, set[str]]:
    labels: dict[str, set[str]] = {}
    for trader_id, payload in recommendations.items():
        for label, identity in payload.items():
            if isinstance(identity, dict) and identity.get("candidate_id"):
                labels.setdefault(identity["candidate_id"], set()).add(label)
    return labels


def _identity(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "candidate_id": row.get("candidate_id"),
        "candidate_type": row.get("candidate_type"),
        "symbol": row.get("symbol"),
        "trader_id": row.get("trader_id"),
        "strategy_family": row.get("strategy_family"),
        "strategy_type": row.get("strategy_type"),
        "brick_pct": row.get("brick_pct"),
        "brick_size_dollars": row.get("brick_size_dollars"),
        "net_pnl_dollars": row.get("net_pnl_dollars"),
        "strategy_return_pct": row.get("strategy_return_pct"),
        "entry_confirmation": row.get("entry_confirmation"),
        "take_profit_bricks": row.get("take_profit_bricks"),
        "stop_loss_bricks": row.get("stop_loss_bricks"),
        "profit_factor": row.get("profit_factor"),
        "closed_trades": row.get("closed_trades"),
        "max_drawdown_pct": row.get("max_drawdown_pct"),
        "robustness_score": row.get("robustness_score"),
        "return_on_allocated_capital_pct": row.get("return_on_allocated_capital_pct"),
        "return_on_total_account_equity_pct": row.get("return_on_total_account_equity_pct"),
        "allocated_capital_dollars": row.get("allocated_capital_dollars"),
        "unused_cash_dollars": row.get("unused_cash_dollars"),
        "research_quantity": row.get("research_quantity"),
        "average_exposure_pct": row.get("average_exposure_pct"),
        "turnover_multiple": row.get("turnover_multiple"),
        "sensitivity_index": row.get("sensitivity_index"),
        "sensitivity_classification": row.get("sensitivity_classification"),
    }


def _first(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    return rows[0] if rows else None


def _max(rows: list[dict[str, Any]], key: str) -> dict[str, Any] | None:
    values = [row for row in rows if row.get(key) not in {None, "UNKNOWN", ""}]
    return max(values, key=lambda row: (_dec(row.get(key)), str(row.get("candidate_id"))), default=None)


def _min(rows: list[dict[str, Any]], key: str) -> dict[str, Any] | None:
    values = [row for row in rows if row.get(key) not in {None, "UNKNOWN", ""}]
    return min(values, key=lambda row: (_dec(row.get(key)), str(row.get("candidate_id"))), default=None)


def _best_risk_adjusted(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    values = [row for row in rows if _dec(row.get("max_drawdown_pct")) > 0]
    if not values:
        return None
    return max(
        values,
        key=lambda row: (
            _dec(row.get("sharpe_ratio")) if row.get("sharpe_ratio") not in {None, "UNKNOWN", ""} else _dec(row.get("return_on_allocated_capital_pct")) / _dec(row.get("max_drawdown_pct")),
            _dec(row.get("robustness_score")),
            str(row.get("candidate_id")),
        ),
        default=None,
    )


def _distinct(rows: list[dict[str, Any] | None], *, limit: int) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        if row is None:
            continue
        candidate_id = str(row.get("candidate_id"))
        if candidate_id in seen:
            continue
        seen.add(candidate_id)
        result.append(row)
        if len(result) >= limit:
            break
    return result


def _dec(value: Any) -> Decimal:
    if value in {None, "", "UNKNOWN"}:
        return Decimal("0")
    return Decimal(str(value))
