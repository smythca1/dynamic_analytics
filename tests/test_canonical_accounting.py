from __future__ import annotations

import unittest
from decimal import Decimal

from silver.accounting import AccountingLedger, DuplicateFillError, NormalizedFill, PositionState


D = Decimal


def make_fill(
    fill_id: str | None,
    side: str,
    qty: str,
    price: str,
    *,
    activity_id: str | None = None,
    leg: str | None = None,
    timestamp: str = "2026-07-20T13:30:00Z",
    source_sequence: int = 0,
    row_order: int = 0,
    order_id: str | None = None,
) -> NormalizedFill:
    return NormalizedFill(
        timestamp=timestamp,
        symbol="TSLA",
        trader_id="unit-trader",
        account_id="paper",
        order_id=order_id,
        activity_id=activity_id,
        fill_id=fill_id,
        side=side,
        qty=qty,
        price=price,
        leg=leg,
        source="unit",
        source_sequence=source_sequence,
        row_order=row_order,
    )


def assert_snapshot_unchanged(test: unittest.TestCase, before, after) -> None:
    test.assertEqual(after.position.qty, before.position.qty)
    test.assertEqual(after.position.side, before.position.side)
    test.assertEqual(after.position.avg_entry_price, before.position.avg_entry_price)
    test.assertEqual(after.realized_pnl, before.realized_pnl)
    test.assertEqual(after.unrealized_pnl, before.unrealized_pnl)
    test.assertEqual(after.net_pnl, before.net_pnl)
    test.assertEqual(after.equity, before.equity)
    test.assertEqual(len(after.closed_trades), len(before.closed_trades))
    test.assertEqual(len(after.position_transitions), len(before.position_transitions))
    test.assertEqual(after.winning_trades, before.winning_trades)
    test.assertEqual(after.losing_trades, before.losing_trades)
    test.assertEqual(after.gross_profit, before.gross_profit)
    test.assertEqual(after.gross_loss, before.gross_loss)


class CanonicalAccountingTests(unittest.TestCase):
    def test_flat_to_long(self) -> None:
        ledger = AccountingLedger(symbol="TSLA")
        snapshot = ledger.process_fill(make_fill("f1", "buy", "2", "100", leg="open"))
        self.assertEqual(snapshot.position.side, "long")
        self.assertEqual(snapshot.position.qty, D("2"))
        self.assertEqual(snapshot.position.avg_entry_price, D("100"))
        self.assertEqual(snapshot.realized_pnl, D("0"))

    def test_flat_to_short(self) -> None:
        ledger = AccountingLedger(symbol="TSLA")
        snapshot = ledger.process_fill(make_fill("f1", "sell_short", "3", "50", leg="open"))
        self.assertEqual(snapshot.position.side, "short")
        self.assertEqual(snapshot.position.qty, D("3"))
        self.assertEqual(snapshot.position.avg_entry_price, D("50"))

    def test_long_to_flat_realizes_only_closing_fill(self) -> None:
        ledger = AccountingLedger(symbol="TSLA")
        ledger.process_fill(make_fill("f1", "buy", "2", "100", leg="open"))
        snapshot = ledger.process_fill(make_fill("f2", "sell", "2", "110", leg="close"))
        self.assertIsNone(snapshot.position.side)
        self.assertEqual(snapshot.realized_pnl, D("20"))
        self.assertEqual(snapshot.closed_trades[0].pnl, D("20"))
        self.assertEqual(snapshot.gross_profit, D("20"))

    def test_short_to_flat_realizes_only_closing_fill(self) -> None:
        ledger = AccountingLedger(symbol="TSLA")
        ledger.process_fill(make_fill("f1", "sell_short", "2", "100", leg="open"))
        snapshot = ledger.process_fill(make_fill("f2", "buy", "2", "90", leg="close"))
        self.assertIsNone(snapshot.position.side)
        self.assertEqual(snapshot.realized_pnl, D("20"))
        self.assertEqual(snapshot.closed_trades[0].pnl, D("20"))

    def test_long_to_short_reversal_uses_sell_close_then_sell_short_open(self) -> None:
        ledger = AccountingLedger(symbol="TSLA")
        ledger.process_fill(make_fill("f1", "buy", "1", "100", leg="open"))
        ledger.process_fill(make_fill("f2", "sell", "1", "110", leg="close"))
        snapshot = ledger.process_fill(make_fill("f3", "sell_short", "1", "111", leg="open"))
        self.assertEqual(snapshot.realized_pnl, D("10"))
        self.assertEqual(len(snapshot.closed_trades), 1)
        self.assertEqual(snapshot.position.side, "short")
        self.assertEqual(snapshot.position.avg_entry_price, D("111"))
        self.assertEqual(snapshot.position_transitions[-1].realized_pnl, D("0"))

    def test_short_to_long_reversal_uses_buy_close_then_buy_open(self) -> None:
        ledger = AccountingLedger(symbol="TSLA")
        ledger.process_fill(make_fill("f1", "sell_short", "1", "100", leg="open"))
        ledger.process_fill(make_fill("f2", "buy", "1", "90", leg="close"))
        snapshot = ledger.process_fill(make_fill("f3", "buy", "1", "91", leg="open"))
        self.assertEqual(snapshot.realized_pnl, D("10"))
        self.assertEqual(len(snapshot.closed_trades), 1)
        self.assertEqual(snapshot.position.side, "long")
        self.assertEqual(snapshot.position.avg_entry_price, D("91"))
        self.assertEqual(snapshot.position_transitions[-1].realized_pnl, D("0"))

    def test_partial_long_close(self) -> None:
        ledger = AccountingLedger(symbol="TSLA")
        ledger.process_fill(make_fill("f1", "buy", "2", "100", leg="open"))
        snapshot = ledger.process_fill(make_fill("f2", "sell", "0.5", "110", leg="close"))
        self.assertEqual(snapshot.position.side, "long")
        self.assertEqual(snapshot.position.qty, D("1.5"))
        self.assertEqual(snapshot.position.avg_entry_price, D("100"))
        self.assertEqual(snapshot.realized_pnl, D("5.0"))

    def test_partial_short_close(self) -> None:
        ledger = AccountingLedger(symbol="TSLA")
        ledger.process_fill(make_fill("f1", "sell_short", "2", "100", leg="open"))
        snapshot = ledger.process_fill(make_fill("f2", "buy", "0.5", "90", leg="close"))
        self.assertEqual(snapshot.position.side, "short")
        self.assertEqual(snapshot.position.qty, D("1.5"))
        self.assertEqual(snapshot.position.avg_entry_price, D("100"))
        self.assertEqual(snapshot.realized_pnl, D("5.0"))

    def test_add_to_long_uses_weighted_average_basis(self) -> None:
        ledger = AccountingLedger(symbol="TSLA")
        ledger.process_fill(make_fill("f1", "buy", "1", "100", leg="open"))
        snapshot = ledger.process_fill(make_fill("f2", "buy", "3", "120", leg="open"))
        self.assertEqual(snapshot.position.qty, D("4"))
        self.assertEqual(snapshot.position.avg_entry_price, D("115"))
        self.assertEqual(snapshot.cost_basis_method, "weighted_average")

    def test_add_to_short_uses_weighted_average_basis(self) -> None:
        ledger = AccountingLedger(symbol="TSLA")
        ledger.process_fill(make_fill("f1", "sell_short", "1", "100", leg="open"))
        snapshot = ledger.process_fill(make_fill("f2", "sell_short", "3", "80", leg="open"))
        self.assertEqual(snapshot.position.qty, D("4"))
        self.assertEqual(snapshot.position.avg_entry_price, D("85"))

    def test_duplicate_fill_does_not_alter_state(self) -> None:
        ledger = AccountingLedger(symbol="TSLA")
        fill = make_fill("f1", "buy", "1", "100", leg="open")
        first = ledger.process_fill(fill)
        with self.assertRaises(DuplicateFillError):
            ledger.process_fill(fill)
        after = ledger.snapshot()
        assert_snapshot_unchanged(self, first, after)

    def test_unknown_starting_basis_does_not_fabricate_pnl(self) -> None:
        starting = PositionState(symbol="TSLA", side="short", qty=D("1"), avg_entry_price=None, basis_known=False)
        ledger = AccountingLedger(symbol="TSLA", starting_position=starting)
        ledger.process_fill(make_fill("f1", "buy", "1", "90", leg="close"))
        snapshot = ledger.process_fill(make_fill("f2", "buy", "1", "91", leg="open"))
        self.assertEqual(snapshot.realized_pnl, D("0"))
        self.assertFalse(snapshot.closed_trades[0].basis_known)
        self.assertIsNone(snapshot.closed_trades[0].pnl)
        self.assertEqual(snapshot.position.side, "long")
        self.assertEqual(snapshot.position.avg_entry_price, D("91"))

    def test_equal_timestamps_use_source_sequence_then_original_row_order(self) -> None:
        ledger = AccountingLedger(symbol="TSLA")
        later = make_fill("seq2", "buy", "1", "102", leg="open", source_sequence=2, row_order=0)
        earlier = make_fill("seq1", "buy", "1", "100", leg="open", source_sequence=1, row_order=0)
        snapshot = ledger.process_fills([later, earlier])
        self.assertEqual(snapshot.position_transitions[0].fill_id, "fill:seq1")
        self.assertEqual(snapshot.position_transitions[1].fill_id, "fill:seq2")
        self.assertEqual(snapshot.position.avg_entry_price, D("101"))

    def test_multiple_partial_fills_for_one_order(self) -> None:
        ledger = AccountingLedger(symbol="TSLA", starting_capital="100")
        ledger.process_fill(make_fill("f1", "buy", "0.4", "100", leg="open", order_id="o1"))
        ledger.process_fill(make_fill("f2", "buy", "0.6", "102", leg="open", order_id="o1"))
        ledger.process_fill(make_fill("f3", "sell", "0.5", "103", leg="close", order_id="o2"))
        snapshot = ledger.process_fill(make_fill("f4", "sell", "0.5", "99", leg="close", order_id="o2"))
        self.assertEqual(snapshot.realized_pnl, D("-0.20"))
        self.assertEqual(snapshot.closed_trades[0].pnl, D("0.90"))
        self.assertEqual(snapshot.closed_trades[1].pnl, D("-1.10"))
        self.assertEqual(snapshot.gross_profit, D("0.90"))
        self.assertEqual(snapshot.gross_loss, D("-1.10"))
        self.assertEqual(snapshot.largest_winner, D("0.90"))
        self.assertEqual(snapshot.largest_loser, D("-1.10"))
        self.assertEqual(snapshot.max_drawdown_dollars, D("2.00"))

    def test_repeated_processing_is_deterministic(self) -> None:
        fills = [
            make_fill("f2", "sell", "1", "110", leg="close", timestamp="2026-07-20T13:30:02Z", source_sequence=2),
            make_fill("f1", "buy", "1", "100", leg="open", timestamp="2026-07-20T13:30:01Z", source_sequence=1),
            make_fill("f3", "sell_short", "1", "111", leg="open", timestamp="2026-07-20T13:30:02Z", source_sequence=3),
        ]
        first = AccountingLedger(symbol="TSLA").process_fills(fills)
        second = AccountingLedger(symbol="TSLA").process_fills(fills)
        self.assertEqual(first.position, second.position)
        self.assertEqual(first.realized_pnl, second.realized_pnl)
        self.assertEqual(first.closed_trades, second.closed_trades)
        self.assertEqual(first.position_transitions, second.position_transitions)

    def test_duplicate_same_activity_id_and_same_fill_id(self) -> None:
        ledger = AccountingLedger(symbol="TSLA")
        fill = make_fill("fill-1", "buy", "1", "100", activity_id="activity-1", leg="open")
        before = ledger.process_fill(fill)
        with self.assertRaises(DuplicateFillError):
            ledger.process_fill(make_fill("fill-1", "buy", "1", "100", activity_id="activity-1", leg="open"))
        assert_snapshot_unchanged(self, before, ledger.snapshot())

    def test_duplicate_same_activity_id_and_different_fill_id(self) -> None:
        ledger = AccountingLedger(symbol="TSLA")
        before = ledger.process_fill(make_fill("fill-1", "buy", "1", "100", activity_id="activity-1", leg="open"))
        with self.assertRaises(DuplicateFillError):
            ledger.process_fill(make_fill("fill-2", "buy", "1", "100", activity_id="activity-1", leg="open"))
        assert_snapshot_unchanged(self, before, ledger.snapshot())

    def test_duplicate_same_activity_id_when_one_record_has_no_fill_id(self) -> None:
        ledger = AccountingLedger(symbol="TSLA")
        before = ledger.process_fill(make_fill("fill-1", "buy", "1", "100", activity_id="activity-1", leg="open"))
        with self.assertRaises(DuplicateFillError):
            ledger.process_fill(make_fill(None, "buy", "1", "100", activity_id="activity-1", leg="open"))
        assert_snapshot_unchanged(self, before, ledger.snapshot())

    def test_different_activity_id_with_same_fill_id_processes_as_distinct_activity(self) -> None:
        ledger = AccountingLedger(symbol="TSLA")
        ledger.process_fill(make_fill("fill-1", "buy", "1", "100", activity_id="activity-1", leg="open"))
        snapshot = ledger.process_fill(make_fill("fill-1", "buy", "1", "102", activity_id="activity-2", leg="open"))
        self.assertEqual(snapshot.position.qty, D("2"))
        self.assertEqual(snapshot.position.avg_entry_price, D("101"))
        self.assertEqual(len(snapshot.position_transitions), 2)

    def test_duplicate_both_ids_absent_with_identical_composite_identity(self) -> None:
        ledger = AccountingLedger(symbol="TSLA")
        fill = make_fill(None, "buy", "1", "100", leg="open", order_id="order-1", source_sequence=10, row_order=3)
        before = ledger.process_fill(fill)
        with self.assertRaises(DuplicateFillError):
            ledger.process_fill(make_fill(None, "buy", "1", "100", leg="open", order_id="order-1", source_sequence=10, row_order=3))
        assert_snapshot_unchanged(self, before, ledger.snapshot())

    def test_both_ids_absent_with_materially_different_composite_identity(self) -> None:
        ledger = AccountingLedger(symbol="TSLA")
        ledger.process_fill(make_fill(None, "buy", "1", "100", leg="open", order_id="order-1", source_sequence=10))
        snapshot = ledger.process_fill(make_fill(None, "buy", "1", "101", leg="open", order_id="order-1", source_sequence=11))
        self.assertEqual(snapshot.position.qty, D("2"))
        self.assertEqual(snapshot.position.avg_entry_price, D("100.5"))
        self.assertEqual(len(snapshot.position_transitions), 2)

    def test_duplicate_close_fill_cannot_realize_pnl_twice(self) -> None:
        ledger = AccountingLedger(symbol="TSLA")
        ledger.process_fill(make_fill("open-1", "buy", "1", "100", activity_id="activity-open", leg="open"))
        before = ledger.process_fill(make_fill("close-1", "sell", "1", "110", activity_id="activity-close", leg="close"))
        with self.assertRaises(DuplicateFillError):
            ledger.process_fill(make_fill("close-2", "sell", "1", "110", activity_id="activity-close", leg="close"))
        assert_snapshot_unchanged(self, before, ledger.snapshot())

    def test_duplicate_open_fill_cannot_add_quantity_twice(self) -> None:
        ledger = AccountingLedger(symbol="TSLA")
        before = ledger.process_fill(make_fill("open-1", "buy", "1", "100", activity_id="activity-open", leg="open"))
        with self.assertRaises(DuplicateFillError):
            ledger.process_fill(make_fill("open-2", "buy", "1", "100", activity_id="activity-open", leg="open"))
        assert_snapshot_unchanged(self, before, ledger.snapshot())

    def test_activity_id_duplicate_leaves_full_accounting_snapshot_unchanged(self) -> None:
        ledger = AccountingLedger(symbol="TSLA", starting_capital="1000")
        ledger.process_fill(make_fill("open-1", "buy", "2", "100", activity_id="activity-open", leg="open"))
        before = ledger.process_fill(make_fill("close-1", "sell", "1", "110", activity_id="activity-close", leg="close"))
        with self.assertRaises(DuplicateFillError):
            ledger.process_fill(make_fill(None, "sell", "1", "110", activity_id="activity-close", leg="close"))
        assert_snapshot_unchanged(self, before, ledger.snapshot())

    def test_activity_id_idempotency_is_deterministic_across_repeated_execution(self) -> None:
        fills = [
            make_fill("open-1", "buy", "1", "100", activity_id="activity-open", leg="open", source_sequence=1),
            make_fill("close-1", "sell", "1", "110", activity_id="activity-close", leg="close", source_sequence=2),
        ]
        first = AccountingLedger(symbol="TSLA").process_fills(fills)
        second = AccountingLedger(symbol="TSLA").process_fills(fills)
        self.assertEqual(first.position, second.position)
        self.assertEqual(first.realized_pnl, second.realized_pnl)
        self.assertEqual(first.closed_trades, second.closed_trades)
        self.assertEqual(first.equity_curve, second.equity_curve)


if __name__ == "__main__":
    unittest.main()
