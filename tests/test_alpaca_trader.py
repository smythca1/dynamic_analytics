import unittest
from unittest.mock import patch

from fastapi import HTTPException

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
        self.replaced: list[tuple[str, dict]] = []

    def _get_position(self, symbol: str) -> dict | None:
        return self.position

    def _cancel_open_orders(
        self,
        symbol: str,
        exclude_order_ids: set[str] | None = None,
        *,
        confirm_terminal: bool = False,
    ) -> list[str]:
        self.cancelled_symbols.append(symbol)
        if confirm_terminal:
            for order_id in self.cancelled_order_ids:
                self._wait_until_order_terminal(order_id)
        return self.cancelled_order_ids.copy()

    def _submit_normalized_order(self, order: dict, action: str, reason: object, close_position: bool) -> dict:
        self.orders.append((order.copy(), action, reason, close_position))
        if close_position:
            self.position = None
        return {"id": f"order-{len(self.orders)}", "status": "accepted"}

    def get_order(self, order_id: str) -> dict:
        return {"id": order_id, "status": "filled", "filled_qty": self.position.get("qty", "0") if self.position else "0", "qty": self.position.get("qty", "0") if self.position else "0"}

    def replace_order(self, order_id: str, patch: dict) -> dict:
        self.replaced.append((order_id, patch))
        return {"id": order_id, "status": "accepted", **patch}


class HeldProtectionAlpacaTrader(StubAlpacaTrader):
    def __init__(self, position: dict):
        super().__init__(position, cancelled_order_ids=["protective-stop-1"])

    def _cancel_open_orders(
        self,
        symbol: str,
        exclude_order_ids: set[str] | None = None,
        *,
        confirm_terminal: bool = False,
    ) -> list[str]:
        cancelled = super()._cancel_open_orders(
            symbol,
            exclude_order_ids=exclude_order_ids,
            confirm_terminal=confirm_terminal,
        )
        if confirm_terminal and self.position is not None:
            self.position["qty_available"] = self.position.get("qty", "0")
        return cancelled

    def _submit_normalized_order(self, order: dict, action: str, reason: object, close_position: bool) -> dict:
        if not close_position and self.position is not None:
            position_side = str(self.position.get("side", "")).lower()
            order_side = str(order.get("side", "")).lower()
            is_opposite = (position_side == "long" and order_side == "sell") or (
                position_side == "short" and order_side == "buy"
            )
            qty_available = abs(float(self.position.get("qty_available") or 0))
            order_qty = abs(float(order.get("qty") or 0))
            if is_opposite and order_qty > qty_available:
                raise HTTPException(
                    status_code=403,
                    detail="insufficient qty available for order: requested quantity exceeds available quantity",
                )
        return super()._submit_normalized_order(order, action, reason, close_position)


class FaultingProtectedReversalTrader(HeldProtectionAlpacaTrader):
    def __init__(
        self,
        position: dict,
        *,
        open_fault: str | None = None,
        close_leaves_opposite_position: bool = False,
    ):
        super().__init__(position)
        self.open_fault = open_fault
        self.close_leaves_opposite_position = close_leaves_opposite_position

    def _submit_normalized_order(self, order: dict, action: str, reason: object, close_position: bool) -> dict:
        self.orders.append((order.copy(), action, reason, close_position))
        if close_position:
            if not self.close_leaves_opposite_position:
                self.position = None
            return {"id": f"order-{len(self.orders)}", "status": "accepted"}
        if self.open_fault == "reject":
            raise HTTPException(status_code=403, detail="reversal opening order rejected")
        if self.open_fault == "timeout":
            raise TimeoutError("timed out submitting reversal opening order")
        return {"id": f"order-{len(self.orders)}", "status": "accepted"}

    def _wait_until_not_opposite_position(self, symbol: str, entry_side: str, timeout_seconds: float = 8.0) -> None:
        if self.close_leaves_opposite_position:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "opposite_position_still_open_after_reversal_close",
                    "symbol": symbol,
                    "broker_position": self.position,
                },
            )
        return None


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

    def test_latest_trade_price_uses_read_only_data_endpoint(self):
        trader = StubAlpacaTrader(None)

        class Response:
            status_code = 200

            @staticmethod
            def json():
                return {"trade": {"p": 311.34, "t": "2026-08-05T14:00:00Z"}}

        with patch("alpaca_trader.requests.get", return_value=Response()) as get:
            price, timestamp = trader.get_latest_trade_price("AAPL", feed="sip")

        self.assertEqual(price, 311.34)
        self.assertEqual(timestamp, "2026-08-05T14:00:00Z")
        self.assertEqual(get.call_args.kwargs["params"], {"feed": "sip"})
        self.assertIn("/v2/stocks/AAPL/trades/latest", get.call_args.kwargs["url"])

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

    def test_catastrophic_stop_reversal_confirms_protective_cancel_before_close(self):
        trader = StubAlpacaTrader(
            {"symbol": "AAPL", "side": "long", "qty": "8", "qty_available": "8"},
            cancelled_order_ids=["protective-stop-1"],
        )

        result = trader.submit_order(
            {
                "symbol": "AAPL",
                "side": "sell",
                "qty": 8,
                "target_qty": 8,
                "reason": "short_entry_e1_reversal",
                "order_type": "market",
                "exit_management": "catastrophic_stop",
                "protection_type": "reversal_catastrophic_stop",
            },
            "sell",
            "sell",
        )

        self.assertEqual(result["status"], "split reversal sent")
        self.assertTrue(result["protective_stop_cancel_confirmed"])
        self.assertEqual(result["cancelled_order_ids"], ["protective-stop-1"])
        self.assertEqual(trader.orders[0][3], True)
        self.assertEqual(trader.orders[1][3], False)
        self.assertEqual(trader.orders[1][3], False)

    def test_protected_long_reversal_does_not_submit_net_sell_while_stop_holds_quantity(self):
        trader = HeldProtectionAlpacaTrader(
            {"symbol": "AAPL", "side": "long", "qty": "1", "qty_available": "0"}
        )

        result = trader.submit_order(
            {
                "symbol": "AAPL",
                "side": "sell",
                "qty": 2,
                "reason": "short_entry_e1_reversal",
                "order_type": "market",
                "exit_management": "catastrophic_stop",
                "protection_type": "reversal_catastrophic_stop",
            },
            "sell",
            "sell",
        )

        self.assertEqual(result["status"], "split reversal sent")
        self.assertTrue(result["protective_stop_cancel_confirmed"])
        self.assertEqual(result["cancelled_order_ids"], ["protective-stop-1"])
        self.assertEqual(trader.orders[0][0]["qty"], "1")
        self.assertEqual(trader.orders[0][0]["side"], "sell")
        self.assertEqual(trader.orders[0][3], True)
        self.assertEqual(trader.orders[1][0]["qty"], "1")
        self.assertEqual(trader.orders[1][0]["side"], "sell")
        self.assertEqual(trader.orders[1][3], False)

    def test_protected_short_reversal_does_not_submit_net_buy_while_stop_holds_quantity(self):
        trader = HeldProtectionAlpacaTrader(
            {"symbol": "TSLA", "side": "short", "qty": "-1", "qty_available": "0"}
        )

        result = trader.submit_order(
            {
                "symbol": "TSLA",
                "side": "buy",
                "qty": 2,
                "reason": "long_entry_e1_reversal",
                "order_type": "market",
                "exit_management": "catastrophic_stop",
                "protection_type": "reversal_catastrophic_stop",
            },
            "buy",
            "buy",
        )

        self.assertEqual(result["status"], "split reversal sent")
        self.assertTrue(result["protective_stop_cancel_confirmed"])
        self.assertEqual(result["cancelled_order_ids"], ["protective-stop-1"])
        self.assertEqual(trader.orders[0][0]["qty"], "1")
        self.assertEqual(trader.orders[0][0]["side"], "buy")
        self.assertEqual(trader.orders[0][3], True)
        self.assertEqual(trader.orders[1][0]["qty"], "1")
        self.assertEqual(trader.orders[1][0]["side"], "buy")
        self.assertEqual(trader.orders[1][3], False)

    def test_protected_reversal_open_rejection_after_cancel_surfaces_without_silent_success(self):
        trader = FaultingProtectedReversalTrader(
            {"symbol": "AAPL", "side": "long", "qty": "1", "qty_available": "0"},
            open_fault="reject",
        )

        with self.assertRaises(HTTPException) as raised:
            trader.submit_order(
                {
                    "symbol": "AAPL",
                    "side": "sell",
                    "qty": 2,
                    "reason": "short_entry_e1_reversal",
                    "order_type": "market",
                    "exit_management": "catastrophic_stop",
                    "protection_type": "reversal_catastrophic_stop",
                },
                "sell",
                "sell",
            )

        self.assertEqual(raised.exception.status_code, 403)
        self.assertEqual(trader.cancelled_symbols, ["AAPL"])
        self.assertEqual(len(trader.orders), 2)
        self.assertEqual(trader.orders[0][3], True)
        self.assertEqual(trader.orders[1][3], False)

    def test_protected_reversal_open_timeout_after_cancel_surfaces_without_silent_success(self):
        trader = FaultingProtectedReversalTrader(
            {"symbol": "TSLA", "side": "short", "qty": "-1", "qty_available": "0"},
            open_fault="timeout",
        )

        with self.assertRaises(TimeoutError):
            trader.submit_order(
                {
                    "symbol": "TSLA",
                    "side": "buy",
                    "qty": 2,
                    "reason": "long_entry_e1_reversal",
                    "order_type": "market",
                    "exit_management": "catastrophic_stop",
                    "protection_type": "reversal_catastrophic_stop",
                },
                "buy",
                "buy",
            )

        self.assertEqual(trader.cancelled_symbols, ["TSLA"])
        self.assertEqual(len(trader.orders), 2)
        self.assertEqual(trader.orders[0][3], True)
        self.assertEqual(trader.orders[1][3], False)

    def test_protected_reversal_partial_close_blocks_open_leg(self):
        trader = FaultingProtectedReversalTrader(
            {"symbol": "AAPL", "side": "long", "qty": "1", "qty_available": "0"},
            close_leaves_opposite_position=True,
        )

        with self.assertRaises(HTTPException) as raised:
            trader.submit_order(
                {
                    "symbol": "AAPL",
                    "side": "sell",
                    "qty": 2,
                    "reason": "short_entry_e1_reversal",
                    "order_type": "market",
                    "exit_management": "catastrophic_stop",
                    "protection_type": "reversal_catastrophic_stop",
                },
                "sell",
                "sell",
            )

        self.assertEqual(raised.exception.status_code, 409)
        self.assertEqual(trader.cancelled_symbols, ["AAPL"])
        self.assertEqual(len(trader.orders), 1)
        self.assertEqual(trader.orders[0][3], True)

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

    def test_close_position_short_take_profit_buys_actual_broker_quantity(self):
        trader = StubAlpacaTrader({"symbol": "TSLA", "side": "short", "qty": "-9", "qty_available": "-9"})

        result = trader.submit_order(
            {
                "symbol": "TSLA",
                "side": "buy",
                "qty": 25,
                "target_qty": 25,
                "reason": "take_profit_1_bricks",
                "close_position": True,
                "order_type": "market",
            },
            "close",
            "buy",
        )

        self.assertEqual(result["status"], "order sent")
        self.assertEqual(result["close_resolution"]["status"], "resolved")
        self.assertEqual(trader.orders[0][0]["side"], "buy")
        self.assertEqual(trader.orders[0][0]["qty"], "9")
        self.assertEqual(trader.orders[0][3], True)

    def test_close_position_rejects_side_that_would_open_opposite_position(self):
        trader = StubAlpacaTrader({"symbol": "TSLA", "side": "long", "qty": "25", "qty_available": "25"})

        with self.assertRaises(HTTPException) as raised:
            trader.submit_order(
                {
                    "symbol": "TSLA",
                    "side": "buy",
                    "reason": "take_profit_1_bricks",
                    "close_position": True,
                    "order_type": "market",
                },
                "close",
                "buy",
            )

        self.assertEqual(raised.exception.status_code, 409)
        self.assertEqual(trader.orders, [])

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

    def test_bracket_entry_payload_contains_take_profit_and_stop_market(self):
        trader = StubAlpacaTrader(None)

        result = trader.submit_order(
            {
                "symbol": "AAPL",
                "side": "buy",
                "qty": 4,
                "reason": "renko_run_1_long",
                "order_type": "market",
                "client_order_id": "megatron-aapl-entry",
                "exit_management": "bracket",
                "take_profit_limit_price": 311.50,
                "stop_loss_stop_price": 306.40,
                "exit_plan": {
                    "order_class": "bracket",
                    "take_profit_limit_price": "311.50",
                    "stop_loss_trigger_price": "306.40",
                },
            },
            "buy",
            "buy",
        )

        order = trader.orders[0][0]
        self.assertEqual(result["status"], "order sent")
        self.assertEqual(order["order_class"], "bracket")
        self.assertEqual(order["extended_hours"], False)
        self.assertEqual(order["take_profit"], {"limit_price": "311.50"})
        self.assertEqual(order["stop_loss"], {"stop_price": "306.40"})
        self.assertNotIn("limit_price", order["stop_loss"])
        self.assertEqual(order["qty"], "4")
        self.assertEqual(trader.orders[0][3], False)

    def test_bracket_entry_requires_explicit_quantity(self):
        trader = StubAlpacaTrader(None)

        with self.assertRaises(HTTPException) as raised:
            trader.submit_order(
                {
                    "symbol": "AAPL",
                    "side": "buy",
                    "notional": 1000,
                    "reason": "renko_run_1_long",
                    "order_type": "market",
                    "exit_management": "bracket",
                    "take_profit_limit_price": 311.50,
                    "stop_loss_stop_price": 306.40,
                },
                "buy",
                "buy",
            )

        self.assertEqual(raised.exception.status_code, 400)
        self.assertEqual(trader.orders, [])

    def test_get_order_nested_uses_nested_read_only_order_lookup(self):
        trader = StubAlpacaTrader(None)

        class Response:
            status_code = 200
            text = "{}"

            @staticmethod
            def json():
                return {"id": "parent-1", "legs": []}

        with patch("alpaca_trader.requests.request", return_value=Response()) as request:
            result = trader.get_order_nested("parent-1")

        self.assertEqual(result, {"id": "parent-1", "legs": []})
        self.assertEqual(request.call_args.kwargs["method"], "GET")
        self.assertIn("/v2/orders/parent-1", request.call_args.kwargs["url"])
        self.assertEqual(request.call_args.kwargs["params"], {"nested": "true"})

    def test_cancel_order_accepts_success_and_not_found_as_terminal(self):
        trader = StubAlpacaTrader(None)

        class Response:
            status_code = 204
            text = ""

        with patch("alpaca_trader.requests.request", return_value=Response()) as request:
            result = trader.cancel_order("parent-1")

        self.assertEqual(result, {"id": "parent-1", "status_code": 204})
        self.assertEqual(request.call_args.kwargs["method"], "DELETE")
        self.assertIn("/v2/orders/parent-1", request.call_args.kwargs["url"])

    def test_submit_recovery_oco_marks_order_as_close_position(self):
        trader = StubAlpacaTrader(None)

        result = trader.submit_recovery_oco(
            {
                "symbol": "AAPL",
                "side": "sell",
                "type": "limit",
                "time_in_force": "gtc",
                "qty": "2",
                "order_class": "oco",
                "client_order_id": "aapl-entry-r1",
                "take_profit": {"limit_price": "103.00"},
                "stop_loss": {"stop_price": "98.00"},
            }
        )

        self.assertEqual(result["status"], "accepted")
        self.assertEqual(trader.orders[0][1], "recovery_oco")
        self.assertEqual(trader.orders[0][2], "broker_native_protection_recovery")
        self.assertEqual(trader.orders[0][3], True)

    def test_submit_emergency_flatten_marks_order_as_close_position(self):
        trader = StubAlpacaTrader({"symbol": "TSLA", "side": "long", "qty": "4"})

        result = trader.submit_emergency_flatten(
            {
                "symbol": "TSLA",
                "side": "sell",
                "type": "market",
                "time_in_force": "day",
                "qty": "4",
                "client_order_id": "tsla-entry-emg",
                "close_position": True,
            }
        )

        self.assertEqual(result["status"], "accepted")
        self.assertEqual(trader.orders[0][1], "emergency_flatten")
        self.assertEqual(trader.orders[0][2], "broker_native_unprotected_position_containment")
        self.assertEqual(trader.orders[0][3], True)

    def test_submit_reversal_catastrophic_stop_uses_actual_broker_average_entry(self):
        trader = StubAlpacaTrader({"symbol": "AAPL", "side": "long", "qty": "1", "qty_available": "1", "avg_entry_price": "300.00"})

        result = trader.submit_reversal_catastrophic_stop(
            symbol="AAPL",
            entry_side="buy",
            brick_size=2.218981,
            catastrophic_stop_bricks=3,
            client_order_id="aapl-entry-1",
            entry_order_id="entry-1",
            trader_id="camatron_aapl",
            strategy_id="aapl_reversal",
            candidate_config_fingerprint="config-aapl",
            market_contract_fingerprint="market-aapl",
        )

        order = trader.orders[0][0]
        self.assertEqual(result["status"], "catastrophic stop sent")
        self.assertEqual(order["symbol"], "AAPL")
        self.assertEqual(order["side"], "sell")
        self.assertEqual(order["type"], "stop")
        self.assertEqual(order["time_in_force"], "gtc")
        self.assertEqual(order["stop_price"], "293.34")
        self.assertEqual(order["extended_hours"], False)
        self.assertTrue(order["client_order_id"].endswith("-cstop"))
        self.assertEqual(trader.orders[0][1], "catastrophic_stop")
        self.assertEqual(trader.orders[0][3], True)


if __name__ == "__main__":
    unittest.main()
