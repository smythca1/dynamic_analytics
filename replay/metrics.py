from __future__ import annotations

from decimal import Decimal
from statistics import median
from typing import Any

from silver.accounting import AccountingSnapshot
from silver.accounting import metrics as accounting_metrics
from silver.accounting.models import EquityPoint
from silver.market import CanonicalObservation


METRICS_VERSION = "replay.metrics.v1"


def build_metrics(
    *,
    snapshot: AccountingSnapshot,
    initial_capital: Decimal,
    observations: list[CanonicalObservation],
    order_latencies: list[Decimal],
    equity_curve: list[EquityPoint] | None = None,
) -> dict[str, Any]:
    first_price = observations[0].close_price if observations else None
    final_price = observations[-1].close_price if observations else None
    buy_hold_dollars = None
    buy_hold_percent = None
    if first_price is not None and final_price is not None and first_price != 0:
        shares = initial_capital / first_price
        buy_hold_dollars = (final_price - first_price) * shares
        buy_hold_percent = (final_price / first_price - Decimal("1")) * Decimal("100")
    strategy_return_percent = (snapshot.net_pnl / initial_capital * Decimal("100")) if initial_capital else None
    latency_values = [float(value) for value in order_latencies]
    drawdown_source = tuple(equity_curve) if equity_curve is not None else snapshot.equity_curve
    max_dd, max_dd_pct = accounting_metrics.drawdown_metrics(drawdown_source)
    return {
        "metrics_version": METRICS_VERSION,
        "initial_capital_dollars": _num(initial_capital),
        "ending_equity_dollars": _num(snapshot.equity),
        "realized_pnl_dollars": _num(snapshot.realized_pnl),
        "unrealized_pnl_dollars": _num(snapshot.unrealized_pnl),
        "net_pnl_dollars": _num(snapshot.net_pnl),
        "gross_profit_dollars": _num(snapshot.gross_profit),
        "gross_loss_dollars": _num(abs(snapshot.gross_loss)),
        "profit_factor": _num_or_unknown(snapshot.profit_factor),
        "closed_trades": len(snapshot.closed_trades),
        "winning_trades": snapshot.winning_trades,
        "losing_trades": snapshot.losing_trades,
        "win_rate_pct": _num_or_unknown(snapshot.win_rate),
        "avg_trade_pnl_dollars": _num_or_unknown(snapshot.average_trade_pnl_dollars),
        "average_trade_pnl_dollars": _num_or_unknown(snapshot.average_trade_pnl_dollars),
        "avg_trade_return_pct": _num_or_unknown(snapshot.average_trade_pnl_percent),
        "average_trade_return_pct": _num_or_unknown(snapshot.average_trade_pnl_percent),
        "avg_winner_pnl_dollars": _num_or_unknown(snapshot.average_winner_dollars),
        "average_winner_dollars": _num_or_unknown(snapshot.average_winner_dollars),
        "avg_winner_return_pct": _num_or_unknown(snapshot.average_winner_percent),
        "average_winner_pct": _num_or_unknown(snapshot.average_winner_percent),
        "avg_loser_pnl_dollars": _num_or_unknown(snapshot.average_loser_dollars),
        "average_loser_dollars": _num_or_unknown(snapshot.average_loser_dollars),
        "avg_loser_return_pct": _num_or_unknown(snapshot.average_loser_percent),
        "average_loser_pct": _num_or_unknown(snapshot.average_loser_percent),
        "largest_winner_dollars": _num_or_unknown(snapshot.largest_winner),
        "largest_loser_dollars": _num_or_unknown(snapshot.largest_loser),
        "max_drawdown_dollars": _num(max_dd),
        "max_drawdown_pct": _num(max_dd_pct),
        "drawdown_peak_timestamp": "UNKNOWN",
        "drawdown_trough_timestamp": "UNKNOWN",
        "drawdown_recovery_timestamp": "UNKNOWN",
        "benchmark_entry_price": _num_or_unknown(first_price),
        "benchmark_exit_price": _num_or_unknown(final_price),
        "benchmark_quantity": _num_or_unknown(initial_capital / first_price if first_price is not None and first_price != 0 else None),
        "buy_hold_pnl_dollars": _num_or_unknown(buy_hold_dollars),
        "buy_hold_return_pct": _num_or_unknown(buy_hold_percent),
        "strategy_outperformance_dollars": _num_or_unknown(snapshot.net_pnl - buy_hold_dollars if buy_hold_dollars is not None else None),
        "strategy_outperformance_percentage_points": _num_or_unknown(
            strategy_return_percent - buy_hold_percent if strategy_return_percent is not None and buy_hold_percent is not None else None
        ),
        "strategy_return_dollars": _num(snapshot.net_pnl),
        "strategy_return_pct": _num_or_unknown(strategy_return_percent),
        "cagr": "UNKNOWN",
        "cagr_label": "ANNUALIZED EQUIVALENT - NOT A FORECAST",
        "sharpe_ratio": "UNKNOWN",
        "sortino_ratio": "UNKNOWN",
        "order_to_fill_latency_mean_seconds": sum(latency_values) / len(latency_values) if latency_values else "UNKNOWN",
        "order_to_fill_latency_median_seconds": median(latency_values) if latency_values else "UNKNOWN",
        "order_to_fill_latency_p95_seconds": _p95(latency_values) if latency_values else "UNKNOWN",
    }


def metric_definitions() -> dict[str, str]:
    return {
        "net_pnl_dollars": "realized PnL + unrealized PnL - fees - commissions",
        "gross_profit_dollars": "sum of positive closed-trade PnL before fees",
        "gross_loss_dollars": "absolute value of sum of negative closed-trade PnL before fees",
        "profit_factor": "gross profit divided by gross loss",
        "win_rate": "winning closed trades divided by total closed trades",
        "maximum_drawdown_dollars": "largest peak-to-trough decline in marked-to-market equity",
        "buy_hold_pnl_dollars": "same initial capital, first and final canonical observations, fractional shares",
    }


def _num(value: Decimal | int | float) -> int | float:
    if isinstance(value, Decimal):
        if not value.is_finite():
            return str(value)
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    return value


def _num_or_unknown(value: Any) -> int | float | str:
    return "UNKNOWN" if value is None else _num(value)


def _p95(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(round((len(ordered) - 1) * 0.95)))
    return ordered[index]
