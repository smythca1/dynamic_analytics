import unittest

from alpaca_trader import AlpacaCredentials, AlpacaTrader, MAX_CLIENT_ORDER_ID_LENGTH, child_client_order_id


class StubAlpacaTrader(AlpacaTrader):
    def __init__(self, position: dict | None, cancelled_order_ids: list[str] | None = None):
        super().__init__(
            AlpacaCredentials(
                mode="live",
                key_id="test-key",
                secret_key="test-secret",
                base_url="https://example.test",
            ),
            max_notional=250000,
        )
        self.position = position
        self.cancelled_order_ids = cancelled_order_ids or []
        self.cancelled_symbols: list[str] = []
        self.orders: list[tuple[dict, str, object, bool]] = []

    def _get_position(self, symbol: str) -> dict | None:
        return self.position

    def _cancel_open_orders(self, symbol: str, exclude_order_ids: set[str] | None = None) -> list[str]:
        self.cancelled_symbols.append(symbol)
        return self.cancelled_order_ids.copy()

    def _submit_normalized_order(self, order: dict, action: str, reason: object, close_position: bool) -> dict:
        self.orders.append((order.copy(), action, reason, close_position))
        if close_position:
            self.position = None
        return {"id": f"order-{len(self.orders)}", "status": "accepted"}


class AlpacaTraderReversalTests(unittest.TestCase):
    def test_child_client_order_id_preserves_unique_tail_for_long_parent_ids(self):
        first = "megatron_nvda_renko_tp_sl_0p40_e1_t-309ee939f30b"
        second = "megatron_nvda_renko_tp_sl_0p40_e1_t-5af244da69fa"

        first_oco = child_client_order_id(first, "oco")
        second_oco = child_client_order_id(second, "oco")

        self.assertLessEqual(len(first_oco), MAX_CLIENT_ORDER_ID_LENGTH)
        self.assertLessEqual(len(second_oco), MAX_CLIENT_ORDER_ID_LENGTH)
        self.assertNotEqual(first_oco, second_oco)
        self.assertTrue(first_oco.endswith("309ee939f30b-oco"))
        self.assertTrue(second_oco.endswith("5af244da69fa-oco"))

    def test_reversal_qty_smaller_than_broker_short_closes_full_short_then_opens_long(self):
        trader = StubAlpacaTrader({"symbol": "QQQ", "side": "short", "qty": "-27", "qty_available": "-27"})

        result = trader.submit_order(
            {
                "symbol": "QQQ",
                "side": "buy",
                "qty": 26,
                "reason": "pivot_reverse_to_long",
                "order_type": "market",
            },
            "buy",
            "buy",
        )

        self.assertEqual(result["status"], "split reversal sent")
        self.assertEqual(trader.orders[0][0], {"symbol": "QQQ", "side": "buy", "time_in_force": "day", "type": "market", "qty": "27"})
        self.assertEqual(trader.orders[0][3], True)
        self.assertEqual(trader.orders[1][0], {"symbol": "QQQ", "side": "buy", "time_in_force": "day", "qty": "26", "type": "market"})
        self.assertEqual(trader.orders[1][3], False)

    def test_reversal_qty_smaller_than_broker_long_closes_full_long_then_opens_short(self):
        trader = StubAlpacaTrader({"symbol": "TSLA", "side": "long", "qty": "40", "qty_available": "40"})

        result = trader.submit_order(
            {
                "symbol": "TSLA",
                "side": "sell",
                "qty": 35,
                "reason": "session_open_reverse_to_short",
                "order_type": "market",
            },
            "sell",
            "sell",
        )

        self.assertEqual(result["status"], "split reversal sent")
        self.assertEqual(trader.orders[0][0]["qty"], "40")
        self.assertEqual(trader.orders[0][0]["side"], "sell")
        self.assertEqual(trader.orders[0][3], True)
        self.assertEqual(trader.orders[1][0]["qty"], "35")
        self.assertEqual(trader.orders[1][0]["side"], "sell")
        self.assertEqual(trader.orders[1][3], False)

    def test_explicit_target_qty_wins_over_net_qty(self):
        trader = StubAlpacaTrader({"symbol": "QQQ", "side": "short", "qty": "-27", "qty_available": "-27"})

        result = trader.submit_order(
            {
                "symbol": "QQQ",
                "side": "buy",
                "qty": 53,
                "target_qty": 26,
                "reversal": True,
                "reason": "pivot_reverse_to_long",
                "order_type": "market",
            },
            "buy",
            "buy",
        )

        self.assertEqual(result["broker_position_qty_closed"], "27")
        self.assertEqual(result["target_opening_qty"], "26")
        self.assertEqual(trader.orders[0][0]["qty"], "27")
        self.assertEqual(trader.orders[1][0]["qty"], "26")

    def test_reversal_uses_target_qty_when_broker_is_flat(self):
        trader = StubAlpacaTrader(None)

        result = trader.submit_order(
            {
                "symbol": "TSLA",
                "side": "sell",
                "qty": 1.235849,
                "target_qty": 1,
                "reversal": True,
                "reason": "short_entry_psar_reversal",
                "order_type": "market",
            },
            "sell",
            "sell",
        )

        self.assertEqual(result["status"], "order sent")
        self.assertEqual(len(trader.orders), 1)
        self.assertEqual(trader.orders[0][0]["qty"], "1")
        self.assertEqual(trader.orders[0][0]["side"], "sell")
        self.assertEqual(trader.orders[0][3], False)

    def test_non_reversal_order_can_reduce_opposite_position_without_forcing_reopen(self):
        trader = StubAlpacaTrader({"symbol": "QQQ", "side": "short", "qty": "-27", "qty_available": "-27"})

        result = trader.submit_order(
            {
                "symbol": "QQQ",
                "side": "buy",
                "qty": 26,
                "reason": "manual_reduce",
                "order_type": "market",
            },
            "buy",
            "buy",
        )

        self.assertEqual(result["status"], "order sent")
        self.assertEqual(len(trader.orders), 1)
        self.assertEqual(trader.orders[0][0]["qty"], "26")
        self.assertEqual(trader.orders[0][3], False)

    def test_close_position_uses_total_qty_when_oco_reserves_position(self):
        trader = StubAlpacaTrader({"symbol": "TSLA", "side": "long", "qty": "25", "qty_available": "0"})

        result = trader.submit_order(
            {
                "symbol": "TSLA",
                "side": "sell",
                "reason": "session_force_flat",
                "close_position": True,
                "order_type": "market",
            },
            "close",
            "sell",
        )

        self.assertEqual(result["status"], "order sent")
        self.assertEqual(trader.cancelled_symbols, ["TSLA"])
        self.assertEqual(trader.orders[0][0], {"symbol": "TSLA", "side": "sell", "time_in_force": "day", "qty": "25", "type": "market"})
        self.assertEqual(trader.orders[0][3], True)

    def test_close_position_cancels_resting_orders_even_when_broker_is_flat(self):
        trader = StubAlpacaTrader(None, cancelled_order_ids=["oco-parent", "oco-stop"])

        result = trader.submit_order(
            {
                "symbol": "TSLA",
                "side": "sell",
                "reason": "session_force_flat",
                "close_position": True,
                "order_type": "market",
            },
            "close",
            "sell",
        )

        self.assertEqual(result["status"], "no open position")
        self.assertEqual(result["cancelled_order_ids"], ["oco-parent", "oco-stop"])
        self.assertEqual(trader.cancelled_symbols, ["TSLA"])
        self.assertEqual(trader.orders, [])


if __name__ == "__main__":
    unittest.main()
