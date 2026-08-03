from __future__ import annotations

from decimal import Decimal

from .models import ClosedTrade, EquityPoint, ZERO


def pct(numerator: Decimal, denominator: Decimal) -> Decimal | None:
    if denominator == ZERO:
        return None
    return (numerator / denominator) * Decimal("100")


def mean(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, ZERO) / Decimal(len(values))


def profit_factor(gross_profit: Decimal, gross_loss: Decimal) -> Decimal | None:
    if gross_loss == ZERO:
        if gross_profit > ZERO:
            return Decimal("Infinity")
        return None
    return gross_profit / abs(gross_loss)


def trade_metrics(closed_trades: list[ClosedTrade]) -> dict[str, Decimal | int | None]:
    known = [trade for trade in closed_trades if trade.pnl is not None]
    pnls = [trade.pnl for trade in known if trade.pnl is not None]
    pnl_pcts = [trade.pnl_pct for trade in known if trade.pnl_pct is not None]
    winners = [trade for trade in known if trade.pnl is not None and trade.pnl > ZERO]
    losers = [trade for trade in known if trade.pnl is not None and trade.pnl < ZERO]
    winner_pnls = [trade.pnl for trade in winners if trade.pnl is not None]
    loser_pnls = [trade.pnl for trade in losers if trade.pnl is not None]
    winner_pcts = [trade.pnl_pct for trade in winners if trade.pnl_pct is not None]
    loser_pcts = [trade.pnl_pct for trade in losers if trade.pnl_pct is not None]
    gross_profit = sum(winner_pnls, ZERO)
    gross_loss = sum(loser_pnls, ZERO)
    known_count = len(known)

    return {
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "profit_factor": profit_factor(gross_profit, gross_loss),
        "winning_trades": len(winners),
        "losing_trades": len(losers),
        "win_rate": (Decimal(len(winners)) / Decimal(known_count) * Decimal("100")) if known_count else None,
        "average_trade_pnl_dollars": mean(pnls),
        "average_trade_pnl_percent": mean(pnl_pcts),
        "average_winner_dollars": mean(winner_pnls),
        "average_winner_percent": mean(winner_pcts),
        "average_loser_dollars": mean(loser_pnls),
        "average_loser_percent": mean(loser_pcts),
        "largest_winner": max(winner_pnls) if winner_pnls else None,
        "largest_loser": min(loser_pnls) if loser_pnls else None,
    }


def drawdown_metrics(equity_curve: list[EquityPoint]) -> tuple[Decimal, Decimal]:
    peak: Decimal | None = None
    max_drawdown = ZERO
    max_drawdown_pct = ZERO
    for point in equity_curve:
        if peak is None or point.equity > peak:
            peak = point.equity
        drawdown = peak - point.equity
        if drawdown > max_drawdown:
            max_drawdown = drawdown
            drawdown_pct = pct(drawdown, peak)
            max_drawdown_pct = drawdown_pct if drawdown_pct is not None else ZERO
    return max_drawdown, max_drawdown_pct
