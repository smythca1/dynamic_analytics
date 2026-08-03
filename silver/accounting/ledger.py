from __future__ import annotations

from decimal import Decimal
from typing import Iterable

from . import metrics
from .models import (
    AccountingSnapshot,
    ClosedTrade,
    EquityPoint,
    NormalizedFill,
    PositionSide,
    PositionState,
    PositionTransition,
    ZERO,
    to_decimal,
)


class AccountingError(Exception):
    """Base class for canonical accounting errors."""


class DuplicateFillError(AccountingError):
    """Raised when the same fill/activity is processed more than once."""


class AccountingLedger:
    cost_basis_method = "weighted_average"

    def __init__(
        self,
        *,
        symbol: str,
        starting_capital: Decimal | int | float | str = ZERO,
        starting_position: PositionState | None = None,
    ) -> None:
        self.symbol = symbol.upper()
        self.starting_capital = to_decimal(starting_capital)
        self.position = starting_position or PositionState.flat(self.symbol)
        if self.position.symbol != self.symbol:
            raise AccountingError("Starting position symbol does not match ledger symbol")
        self.realized_pnl = ZERO
        self._processed_fill_ids: set[str] = set()
        self.position_transitions: list[PositionTransition] = []
        self.closed_trades: list[ClosedTrade] = []
        self.equity_curve: list[EquityPoint] = []
        self._last_mark_price: Decimal | None = None

    def process_fills(self, fills: Iterable[NormalizedFill]) -> AccountingSnapshot:
        for fill in sorted(fills, key=lambda item: item.sort_key):
            self.process_fill(fill)
        return self.snapshot()

    def process_fill(self, fill: NormalizedFill) -> AccountingSnapshot:
        if fill.symbol != self.symbol:
            raise AccountingError(f"Fill symbol {fill.symbol} does not match ledger symbol {self.symbol}")
        fill_id = fill.idempotency_key
        if fill_id in self._processed_fill_ids:
            raise DuplicateFillError(f"Duplicate fill: {fill_id}")

        before = self.position
        transition_realized = ZERO
        basis_known = True
        actions: list[str]

        if fill.leg == "open":
            actions = [self._open(fill, self._opening_side(fill))]
        elif fill.leg == "close":
            action, transition_realized, basis_known = self._close(fill, fill.qty)
            actions = [action]
        else:
            actions, transition_realized, basis_known = self._apply_unlegged(fill)

        self.realized_pnl += transition_realized
        self._processed_fill_ids.add(fill_id)
        self._last_mark_price = fill.price
        self.position_transitions.append(
            PositionTransition(
                timestamp=fill.timestamp,
                symbol=fill.symbol,
                trader_id=fill.trader_id,
                fill_id=fill_id,
                fill=fill,
                before=before,
                after=self.position,
                action="+".join(actions),
                realized_pnl=transition_realized,
                realized_basis_known=basis_known,
            )
        )
        self._append_equity_point(fill)
        return self.snapshot()

    def snapshot(self, mark_price: Decimal | int | float | str | None = None) -> AccountingSnapshot:
        if mark_price is not None:
            mark = to_decimal(mark_price)
        else:
            mark = self._last_mark_price
        unrealized = self._unrealized_pnl(mark)
        net = self.realized_pnl + unrealized
        trade_stats = metrics.trade_metrics(self.closed_trades)
        max_dd, max_dd_pct = metrics.drawdown_metrics(self.equity_curve)
        return AccountingSnapshot(
            position=self.position,
            realized_pnl=self.realized_pnl,
            unrealized_pnl=unrealized,
            net_pnl=net,
            equity=self.starting_capital + net,
            position_transitions=tuple(self.position_transitions),
            closed_trades=tuple(self.closed_trades),
            equity_curve=tuple(self.equity_curve),
            max_drawdown_dollars=max_dd,
            max_drawdown_percent=max_dd_pct,
            cost_basis_method=self.cost_basis_method,
            **trade_stats,
        )

    def _apply_unlegged(self, fill: NormalizedFill) -> tuple[list[str], Decimal, bool]:
        opening_side = self._opening_side(fill)
        if self.position.side is None:
            return [self._open(fill, opening_side)], ZERO, True
        if self.position.side == opening_side:
            return [self._open(fill, opening_side)], ZERO, True

        close_qty = min(self.position.qty, fill.qty)
        action, realized, known = self._close(fill, close_qty)
        actions = [action]
        remainder = fill.qty - close_qty
        if remainder > ZERO:
            synthetic_open = self._fill_with_qty(fill, remainder)
            actions.append(self._open(synthetic_open, opening_side))
        return actions, realized, known

    def _open(self, fill: NormalizedFill, side: PositionSide) -> str:
        if self.position.side is not None and self.position.side != side:
            raise AccountingError("Open fill would cross an existing opposite position")
        if self.position.side is None:
            self.position = PositionState(
                symbol=self.symbol,
                side=side,
                qty=fill.qty,
                avg_entry_price=fill.price,
                basis_known=True,
                entry_timestamp=fill.timestamp,
                entry_fill_id=fill.stable_fill_id,
            )
            return "open"

        old_notional = self.position.qty * (self.position.avg_entry_price or ZERO)
        added_notional = fill.qty * fill.price
        new_qty = self.position.qty + fill.qty
        new_basis = (old_notional + added_notional) / new_qty
        self.position = PositionState(
            symbol=self.symbol,
            side=side,
            qty=new_qty,
            avg_entry_price=new_basis,
            basis_known=self.position.basis_known,
            entry_timestamp=self.position.entry_timestamp,
            entry_fill_id=self.position.entry_fill_id,
        )
        return "add"

    def _close(self, fill: NormalizedFill, qty: Decimal) -> tuple[str, Decimal, bool]:
        if self.position.side is None:
            raise AccountingError("Close fill received while flat")
        if qty > self.position.qty:
            raise AccountingError("Close quantity exceeds open position quantity")
        if self.position.side == "long" and fill.side not in {"sell", "sell_short"}:
            raise AccountingError("Long positions close with SELL fills")
        if self.position.side == "short" and fill.side != "buy":
            raise AccountingError("Short positions close with BUY fills")

        closed_side = self.position.side
        basis_known = self.position.basis_known and self.position.avg_entry_price is not None
        realized = ZERO
        pnl: Decimal | None = None
        pnl_pct: Decimal | None = None
        entry_price = self.position.avg_entry_price if basis_known else None
        if basis_known and entry_price is not None:
            if closed_side == "long":
                pnl = (fill.price - entry_price) * qty
            else:
                pnl = (entry_price - fill.price) * qty
            realized = pnl
            pnl_pct = metrics.pct(pnl, entry_price * qty)

        self.closed_trades.append(
            ClosedTrade(
                symbol=self.symbol,
                trader_id=fill.trader_id,
                side=closed_side,
                qty=qty,
                entry_price=entry_price,
                exit_price=fill.price,
                entry_timestamp=self.position.entry_timestamp,
                exit_timestamp=fill.timestamp,
                entry_fill_id=self.position.entry_fill_id,
                exit_fill_id=fill.stable_fill_id,
                pnl=pnl,
                pnl_pct=pnl_pct,
                basis_known=basis_known,
                reason=fill.reason,
            )
        )

        remaining_qty = self.position.qty - qty
        if remaining_qty == ZERO:
            self.position = PositionState.flat(self.symbol)
            return "close", realized, basis_known

        self.position = PositionState(
            symbol=self.symbol,
            side=closed_side,
            qty=remaining_qty,
            avg_entry_price=self.position.avg_entry_price,
            basis_known=self.position.basis_known,
            entry_timestamp=self.position.entry_timestamp,
            entry_fill_id=self.position.entry_fill_id,
        )
        return "reduce", realized, basis_known

    def _opening_side(self, fill: NormalizedFill) -> PositionSide:
        if fill.side == "buy":
            return "long"
        return "short"

    def _unrealized_pnl(self, mark_price: Decimal | None) -> Decimal:
        if self.position.side is None or mark_price is None:
            return ZERO
        if not self.position.basis_known or self.position.avg_entry_price is None:
            return ZERO
        if self.position.side == "long":
            return (mark_price - self.position.avg_entry_price) * self.position.qty
        return (self.position.avg_entry_price - mark_price) * self.position.qty

    def _append_equity_point(self, fill: NormalizedFill) -> None:
        unrealized = self._unrealized_pnl(fill.price)
        net = self.realized_pnl + unrealized
        self.equity_curve.append(
            EquityPoint(
                timestamp=fill.timestamp,
                equity=self.starting_capital + net,
                realized_pnl=self.realized_pnl,
                unrealized_pnl=unrealized,
                net_pnl=net,
                fill_id=fill.stable_fill_id,
            )
        )

    def _fill_with_qty(self, fill: NormalizedFill, qty: Decimal) -> NormalizedFill:
        return NormalizedFill(
            timestamp=fill.timestamp,
            symbol=fill.symbol,
            trader_id=fill.trader_id,
            account_id=fill.account_id,
            order_id=fill.order_id,
            activity_id=fill.activity_id,
            fill_id=fill.fill_id,
            side=fill.side,
            qty=qty,
            price=fill.price,
            leg="open",
            reason=fill.reason,
            source=fill.source,
            source_sequence=fill.source_sequence,
            row_order=fill.row_order,
        )
