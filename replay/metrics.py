from __future__ import annotations

from decimal import Decimal
from statistics import median
from typing import Any

from silver.accounting import AccountingSnapshot
from silver.accounting.models import NormalizedFill
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
    benchmark_quantity: Decimal | None = None,
    allocated_capital: Decimal | None = None,
    unused_cash: Decimal | None = None,
    position_samples: list[dict[str, Any]] | None = None,
    fills: list[NormalizedFill] | None = None,
) -> dict[str, Any]:
    first_price = observations[0].close_price if observations else None
    final_price = observations[-1].close_price if observations else None
    buy_hold_dollars = None
    buy_hold_percent = None
    benchmark_entry_value = None
    benchmark_exit_value = None
    buy_hold_ending_equity = None
    buy_hold_return_on_total_equity = None
    buy_hold_return_on_allocated_capital = None
    if first_price is not None and final_price is not None and first_price != 0:
        shares = benchmark_quantity if benchmark_quantity is not None else initial_capital / first_price
        benchmark_entry_value = shares * first_price
        benchmark_exit_value = shares * final_price
        buy_hold_dollars = (final_price - first_price) * shares
        buy_hold_percent = (final_price / first_price - Decimal("1")) * Decimal("100")
        if unused_cash is not None:
            buy_hold_ending_equity = unused_cash + benchmark_exit_value
        buy_hold_return_on_total_equity = (buy_hold_dollars / initial_capital * Decimal("100")) if initial_capital else None
        if allocated_capital is not None and allocated_capital != 0:
            buy_hold_return_on_allocated_capital = buy_hold_dollars / allocated_capital * Decimal("100")
    strategy_return_percent = (snapshot.net_pnl / initial_capital * Decimal("100")) if initial_capital else None
    latency_values = [float(value) for value in order_latencies]
    drawdown_source = tuple(equity_curve) if equity_curve is not None else snapshot.equity_curve
    max_dd, max_dd_pct = accounting_metrics.drawdown_metrics(drawdown_source)
    capital_metrics = build_capital_metrics(
        initial_capital=initial_capital,
        allocated_capital=allocated_capital,
        unused_cash=unused_cash,
        quantity=benchmark_quantity,
        net_pnl=snapshot.net_pnl,
        position_samples=position_samples or [],
        fills=fills or [],
    )
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
        "benchmark_quantity": _num_or_unknown(benchmark_quantity if benchmark_quantity is not None else initial_capital / first_price if first_price is not None and first_price != 0 else None),
        "benchmark_entry_value_dollars": _num_or_unknown(benchmark_entry_value),
        "benchmark_exit_value_dollars": _num_or_unknown(benchmark_exit_value),
        "buy_hold_pnl_dollars": _num_or_unknown(buy_hold_dollars),
        "buy_hold_return_pct": _num_or_unknown(buy_hold_percent),
        "buy_hold_ending_equity_dollars": _num_or_unknown(buy_hold_ending_equity),
        "buy_hold_return_on_total_equity_pct": _num_or_unknown(buy_hold_return_on_total_equity),
        "buy_hold_return_on_allocated_capital_pct": _num_or_unknown(buy_hold_return_on_allocated_capital),
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
        **capital_metrics,
    }


def metric_definitions() -> dict[str, str]:
    return {
        "net_pnl_dollars": "realized PnL + unrealized PnL - fees - commissions",
        "gross_profit_dollars": "sum of positive closed-trade PnL before fees",
        "gross_loss_dollars": "absolute value of sum of negative closed-trade PnL before fees",
        "profit_factor": "gross profit divided by gross loss",
        "win_rate": "winning closed trades divided by total closed trades",
        "maximum_drawdown_dollars": "largest peak-to-trough decline in marked-to-market equity",
        "buy_hold_pnl_dollars": "benchmark quantity times first-to-final canonical observation price change; fractional shares only when no benchmark_quantity is supplied",
        "average_deployed_capital_dollars": "equal-weighted average over canonical observations of abs(position quantity) times mark price",
        "turnover_dollars": "sum of absolute simulated fill notional values",
    }


def build_capital_metrics(
    *,
    initial_capital: Decimal,
    allocated_capital: Decimal | None,
    unused_cash: Decimal | None,
    quantity: Decimal | None,
    net_pnl: Decimal,
    position_samples: list[dict[str, Any]],
    fills: list[NormalizedFill],
) -> dict[str, Any]:
    deployed_total = Decimal("0")
    deployed_min: Decimal | None = None
    deployed_max: Decimal | None = None
    cash_total = Decimal("0")
    cash_min: Decimal | None = None
    position_total = Decimal("0")
    position_max: Decimal | None = None
    exposure_total = Decimal("0")
    exposure_max: Decimal | None = None
    long_count = 0
    short_count = 0
    flat_count = 0
    for sample in position_samples:
        qty = Decimal(str(sample.get("qty", "0")))
        mark_price = Decimal(str(sample.get("mark_price", "0")))
        equity = Decimal(str(sample.get("equity", initial_capital)))
        side = sample.get("side")
        position_value = abs(qty) * mark_price
        cash_value = equity - position_value
        exposure_value = position_value / equity * Decimal("100") if equity else Decimal("0")
        deployed_total += position_value
        deployed_min = position_value if deployed_min is None or position_value < deployed_min else deployed_min
        deployed_max = position_value if deployed_max is None or position_value > deployed_max else deployed_max
        cash_total += cash_value
        cash_min = cash_value if cash_min is None or cash_value < cash_min else cash_min
        position_total += position_value
        position_max = position_value if position_max is None or position_value > position_max else position_max
        exposure_total += exposure_value
        exposure_max = exposure_value if exposure_max is None or exposure_value > exposure_max else exposure_max
        if qty == 0 or side in {None, "", "flat"}:
            flat_count += 1
        elif side == "long":
            long_count += 1
        elif side == "short":
            short_count += 1
    sample_count = Decimal(len(position_samples)) if position_samples else Decimal("0")
    average_deployed = deployed_total / sample_count if sample_count else None
    average_cash = cash_total / sample_count if sample_count else None
    average_position = position_total / sample_count if sample_count else None
    average_exposure = exposure_total / sample_count if sample_count else None
    turnover = sum((fill.qty * fill.price for fill in fills), Decimal("0"))
    average_trade_size = turnover / Decimal(len(fills)) if fills else None
    return {
        "average_deployed_capital_dollars": _num_or_unknown(average_deployed),
        "maximum_deployed_capital_dollars": _num_or_unknown(deployed_max),
        "minimum_deployed_capital_dollars": _num_or_unknown(deployed_min),
        "average_cash_balance_dollars": _num_or_unknown(average_cash),
        "minimum_cash_balance_dollars": _num_or_unknown(cash_min),
        "average_position_value_dollars": _num_or_unknown(average_position),
        "maximum_position_value_dollars": _num_or_unknown(position_max),
        "average_exposure_pct": _num_or_unknown(average_exposure),
        "maximum_exposure_pct": _num_or_unknown(exposure_max),
        "long_exposure_pct": _num_or_unknown(Decimal(long_count) / sample_count * Decimal("100") if sample_count else None),
        "short_exposure_pct": _num_or_unknown(Decimal(short_count) / sample_count * Decimal("100") if sample_count else None),
        "flat_exposure_pct": _num_or_unknown(Decimal(flat_count) / sample_count * Decimal("100") if sample_count else None),
        "capital_utilization_pct": _num_or_unknown(allocated_capital / initial_capital * Decimal("100") if allocated_capital is not None and initial_capital else None),
        "pnl_per_share_dollars": _num_or_unknown(net_pnl / quantity if quantity else None),
        "pnl_on_allocated_capital_dollars": _num(net_pnl),
        "return_on_allocated_capital_pct": _num_or_unknown(net_pnl / allocated_capital * Decimal("100") if allocated_capital is not None and allocated_capital else None),
        "return_on_total_account_equity_pct": _num_or_unknown(net_pnl / initial_capital * Decimal("100") if initial_capital else None),
        "turnover_dollars": _num(turnover),
        "turnover_multiple": _num_or_unknown(turnover / allocated_capital if allocated_capital is not None and allocated_capital else None),
        "average_trade_size_dollars": _num_or_unknown(average_trade_size),
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
