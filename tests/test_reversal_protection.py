import unittest

from reversal_protection import (
    build_reversal_catastrophic_stop_plan,
    catastrophic_stop_order_payload,
    verify_reversal_catastrophic_stop,
)


def plan(symbol="AAPL", entry_side="buy", entry="300.00", qty="1", brick="2.218981"):
    return build_reversal_catastrophic_stop_plan(
        trader_id=f"camatron_{symbol.lower()}",
        strategy_id=f"{symbol.lower()}_reversal",
        symbol=symbol,
        entry_side=entry_side,
        quantity=qty,
        entry_reference_price=entry,
        brick_size=brick,
        catastrophic_stop_bricks=3,
        client_order_id=f"{symbol.lower()}-entry-1",
        candidate_config_fingerprint=f"config-{symbol.lower()}",
        market_contract_fingerprint=f"market-{symbol.lower()}",
    )


def position(symbol="AAPL", side="long", qty="1", avg="300.00"):
    return {
        "symbol": symbol,
        "side": side,
        "qty": qty if side == "long" else f"-{qty}",
        "qty_available": qty if side == "long" else f"-{qty}",
        "avg_entry_price": avg,
    }


def stop_order(stop_plan, **overrides):
    order = catastrophic_stop_order_payload(stop_plan)
    order.update(
        {
            "id": "stop-1",
            "status": "accepted",
            "filled_qty": "0",
        }
    )
    order.update(overrides)
    return order


class ReversalProtectionTests(unittest.TestCase):
    def test_long_aapl_stop_calculation_rounds_down(self):
        stop_plan = plan("AAPL", "buy", "300.00", brick="2.218981")
        self.assertEqual(stop_plan["stop_distance"], "6.656943")
        self.assertEqual(stop_plan["stop_price"], "293.34")
        self.assertEqual(stop_plan["closing_side"], "sell")

    def test_short_aapl_stop_calculation_rounds_up(self):
        stop_plan = plan("AAPL", "sell", "300.00", brick="2.218981")
        self.assertEqual(stop_plan["stop_distance"], "6.656943")
        self.assertEqual(stop_plan["stop_price"], "306.66")
        self.assertEqual(stop_plan["closing_side"], "buy")

    def test_long_tsla_stop_calculation_rounds_down(self):
        stop_plan = plan("TSLA", "buy", "250.00", brick="2.32525")
        self.assertEqual(stop_plan["stop_distance"], "6.97575")
        self.assertEqual(stop_plan["stop_price"], "243.02")

    def test_short_tsla_stop_calculation_rounds_up(self):
        stop_plan = plan("TSLA", "sell", "250.00", brick="2.32525")
        self.assertEqual(stop_plan["stop_distance"], "6.97575")
        self.assertEqual(stop_plan["stop_price"], "256.98")

    def test_valid_increment_rounding_below_one_uses_four_decimals(self):
        stop_plan = plan("AAPL", "buy", "0.95555", brick="0.01001")
        self.assertEqual(stop_plan["price_rounding"]["tick_size"], "0.0001")
        self.assertEqual(stop_plan["stop_price"], "0.9255")

    def test_complete_fill_stop_acknowledgement_is_protected(self):
        stop_plan = plan()
        result = verify_reversal_catastrophic_stop(
            symbol="AAPL",
            broker_position=position(),
            open_orders=[stop_order(stop_plan)],
            stop_plan=stop_plan,
            expected_config_fingerprint="config-aapl",
            expected_market_contract_fingerprint="market-aapl",
        )
        self.assertTrue(result.protected)
        self.assertEqual(result.stop_order_id, "stop-1")
        self.assertEqual(result.protected_quantity, "1")

    def test_rejection_is_not_protected(self):
        stop_plan = plan()
        result = verify_reversal_catastrophic_stop(
            symbol="AAPL",
            broker_position=position(),
            open_orders=[stop_order(stop_plan, status="rejected")],
            stop_plan=stop_plan,
        )
        self.assertFalse(result.protected)
        self.assertIn("missing_catastrophic_stop", {issue.code for issue in result.issues})

    def test_duplicate_prevention_fails_closed(self):
        stop_plan = plan()
        duplicate = stop_order(stop_plan, id="stop-2")
        result = verify_reversal_catastrophic_stop(
            symbol="AAPL",
            broker_position=position(),
            open_orders=[stop_order(stop_plan), duplicate],
            stop_plan=stop_plan,
        )
        self.assertFalse(result.protected)
        self.assertIn("duplicate_or_conflicting_catastrophic_stop", {issue.code for issue in result.issues})

    def test_manual_cancellation_detection(self):
        stop_plan = plan()
        result = verify_reversal_catastrophic_stop(
            symbol="AAPL",
            broker_position=position(),
            open_orders=[stop_order(stop_plan, status="canceled")],
            stop_plan=stop_plan,
        )
        self.assertFalse(result.protected)
        self.assertIn("missing_catastrophic_stop", {issue.code for issue in result.issues})

    def test_partial_quantity_mismatch_is_not_protected(self):
        stop_plan = plan(qty="2")
        result = verify_reversal_catastrophic_stop(
            symbol="AAPL",
            broker_position=position(qty="2"),
            open_orders=[stop_order(stop_plan, qty="1")],
            stop_plan=stop_plan,
        )
        self.assertFalse(result.protected)
        self.assertIn("unprotected_reversal_partial_quantity", {issue.code for issue in result.issues})

    def test_short_buy_stop_limit_acknowledged_when_limit_is_protective(self):
        stop_plan = plan("AAPL", "sell")
        result = verify_reversal_catastrophic_stop(
            symbol="AAPL",
            broker_position=position(side="short"),
            open_orders=[stop_order(stop_plan, type="stop_limit", limit_price="306.70")],
            stop_plan=stop_plan,
        )
        self.assertTrue(result.protected)
        self.assertEqual(result.returned_limit_price, "306.70")

    def test_short_buy_stop_limit_rejects_bad_limit_policy(self):
        stop_plan = plan("AAPL", "sell")
        result = verify_reversal_catastrophic_stop(
            symbol="AAPL",
            broker_position=position(side="short"),
            open_orders=[stop_order(stop_plan, type="stop_limit", limit_price="306.00")],
            stop_plan=stop_plan,
        )
        self.assertFalse(result.protected)
        self.assertIn("invalid_short_buy_stop_limit_price", {issue.code for issue in result.issues})

if __name__ == "__main__":
    unittest.main()
