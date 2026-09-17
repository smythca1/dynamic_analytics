import unittest

from broker_protection import verify_broker_native_protection
from exit_plan import build_renko_tp_sl_exit_plan


def long_plan():
    return build_renko_tp_sl_exit_plan(
        trader_id="megatron_aapl",
        strategy_id="aapl_tp_sl",
        symbol="AAPL",
        entry_side="buy",
        quantity=4,
        entry_reference_type="signal_price",
        entry_reference_price="308.9493",
        brick_size="0.8511",
        take_profit_bricks=3,
        stop_loss_bricks=3,
        time_in_force="day",
        order_class="bracket",
        client_order_id="megatron-aapl-entry",
        candidate_config_fingerprint="config-aapl",
        market_contract_fingerprint="market-aapl",
    ).to_dict()


def short_plan():
    return build_renko_tp_sl_exit_plan(
        trader_id="megatron_tsla",
        strategy_id="tsla_tp_sl",
        symbol="TSLA",
        entry_side="sell",
        quantity=1,
        entry_reference_type="signal_price",
        entry_reference_price="380.175",
        brick_size="1.5392",
        take_profit_bricks=3,
        stop_loss_bricks=1,
        time_in_force="day",
        order_class="bracket",
        client_order_id="megatron_tsla_1s_renko_0p47_e1_tp3_-e1962d231531",
        candidate_config_fingerprint="config-tsla",
        market_contract_fingerprint="market-tsla",
    ).to_dict()


def position(qty="4", side="long"):
    return {"symbol": "AAPL", "side": side, "qty": qty, "avg_entry_price": "308.9493"}


def short_position():
    return {"symbol": "TSLA", "side": "short", "qty": "-1", "avg_entry_price": "380.175"}


def bracket_order(**overrides):
    plan = long_plan()
    order = {
        "id": "parent-1",
        "symbol": "AAPL",
        "side": "buy",
        "qty": "4",
        "filled_qty": "4",
        "status": "filled",
        "order_class": "bracket",
        "client_order_id": plan["client_order_id"],
        "legs": [
            {
                "id": "tp-1",
                "parent_order_id": "parent-1",
                "client_order_id": plan["take_profit_client_order_id"],
                "symbol": "AAPL",
                "side": "sell",
                "qty": "4",
                "filled_qty": "0",
                "status": "accepted",
                "limit_price": plan["take_profit_limit_price"],
            },
            {
                "id": "sl-1",
                "parent_order_id": "parent-1",
                "client_order_id": plan["stop_loss_client_order_id"],
                "symbol": "AAPL",
                "side": "sell",
                "qty": "4",
                "filled_qty": "0",
                "status": "accepted",
                "stop_price": plan["stop_loss_trigger_price"],
            },
        ],
    }
    order.update(overrides)
    return order


def recovery_oco_order(plan=None, **overrides):
    plan = plan or long_plan()
    order = {
        "id": "oco-parent-1",
        "symbol": plan["symbol"],
        "side": "sell",
        "qty": plan["quantity"],
        "filled_qty": "0",
        "status": "new",
        "order_class": "oco",
        "type": "limit",
        "order_type": "limit",
        "position_intent": "sell_to_close",
        "client_order_id": f"{plan['client_order_id'][:31]}-r1-deadbeef",
        "limit_price": plan["take_profit_limit_price"],
        "legs": [
            {
                "id": "oco-sl-1",
                "parent_order_id": "oco-parent-1",
                "symbol": plan["symbol"],
                "side": "sell",
                "qty": plan["quantity"],
                "filled_qty": "0",
                "status": "held",
                "order_class": "oco",
                "type": "stop",
                "order_type": "stop",
                "position_intent": "sell_to_close",
                "stop_price": plan["stop_loss_trigger_price"],
            }
        ],
    }
    order.update(overrides)
    return order


def short_bracket_order_with_broker_child_client_ids(**overrides):
    plan = short_plan()
    order = {
        "id": "e5f03802-d8fb-4521-a56d-cc0540389165",
        "symbol": "TSLA",
        "side": "sell",
        "qty": "1",
        "filled_qty": "1",
        "status": "filled",
        "order_class": "bracket",
        "client_order_id": plan["client_order_id"],
        "legs": [
            {
                "id": "d9dbb786-0ba6-4c6e-a8c3-6ae3b141033c",
                "parent_order_id": "e5f03802-d8fb-4521-a56d-cc0540389165",
                "client_order_id": "broker-generated-tp-client-id",
                "symbol": "TSLA",
                "side": "buy",
                "qty": "1",
                "filled_qty": "0",
                "status": "held",
                "limit_price": plan["take_profit_limit_price"],
            },
            {
                "id": "cfd80ede-3923-44b8-bc82-a76955e758f8",
                "parent_order_id": "e5f03802-d8fb-4521-a56d-cc0540389165",
                "client_order_id": "broker-generated-sl-client-id",
                "symbol": "TSLA",
                "side": "buy",
                "qty": "1",
                "filled_qty": "0",
                "status": "held",
                "stop_price": plan["stop_loss_trigger_price"],
            },
        ],
    }
    order.update(overrides)
    return order


def camatron_tsla_live_plan():
    return build_renko_tp_sl_exit_plan(
        trader_id="camatron_tsla_1s_renko_0p42_e2_tp1_sl3_qty1_20260917_live",
        strategy_id="tsla_tp_sl",
        symbol="TSLA",
        entry_side="sell",
        quantity=1,
        entry_reference_type="signal_price",
        entry_reference_price="353.045",
        brick_size="1.3755",
        take_profit_bricks=1,
        stop_loss_bricks=3,
        time_in_force="day",
        order_class="bracket",
        client_order_id="camatron_tsla_1s_renko_0p42_e2_tp1_-dcd8fbdb2f70",
        candidate_config_fingerprint="config-tsla",
        market_contract_fingerprint="market-tsla",
    ).to_dict()


def camatron_tsla_live_bracket_order(**overrides):
    plan = camatron_tsla_live_plan()
    order = {
        "id": "71172341-d91f-49da-9896-97076cf48ae4",
        "symbol": "TSLA",
        "side": "sell",
        "qty": "1",
        "filled_qty": "1",
        "status": "filled",
        "order_class": "bracket",
        "client_order_id": plan["client_order_id"],
        "legs": [
            {
                "id": "d0770042-74fa-4408-bd74-2389c037f1f8",
                "client_order_id": "4bf9afdc-d195-4ae7-9f99-78abe5361091",
                "symbol": "TSLA",
                "side": "buy",
                "qty": "1",
                "filled_qty": "0",
                "status": "new",
                "limit_price": plan["take_profit_limit_price"],
            },
            {
                "id": "bbcc4ac5-712d-48b9-8697-d71d63c5191d",
                "client_order_id": "d47bd4da-4ad5-42aa-9bf5-8d1e61130f72",
                "symbol": "TSLA",
                "side": "buy",
                "qty": "1",
                "filled_qty": "0",
                "status": "held",
                "stop_price": plan["stop_loss_trigger_price"],
            },
        ],
    }
    order.update(overrides)
    return order


def camatron_tsla_flat_child_order():
    parent = camatron_tsla_live_bracket_order()
    child = dict(parent["legs"][0])
    child.update(
        {
            "order_class": "bracket",
            "order_type": "limit",
            "type": "limit",
            "position_intent": "buy_to_close",
            "legs": None,
        }
    )
    return child


class BrokerProtectionVerifierTests(unittest.TestCase):
    def verify(self, orders, broker_position=None, plan=None, protection_state=None):
        return verify_broker_native_protection(
            symbol="AAPL",
            broker_position=broker_position or position(),
            nested_orders=orders,
            exit_plan=plan or long_plan(),
            protection_state=protection_state,
            expected_config_fingerprint="config-aapl",
            expected_market_contract_fingerprint="market-aapl",
        )

    def test_valid_long_bracket_is_protected(self):
        result = self.verify([bracket_order()])

        self.assertTrue(result.protected)
        self.assertEqual(result.parent_order_id, "parent-1")
        self.assertEqual(result.take_profit_order_id, "tp-1")
        self.assertEqual(result.stop_loss_order_id, "sl-1")
        self.assertEqual(result.protected_quantity, "4")

    def test_valid_recovery_oco_parent_take_profit_is_protected(self):
        result = self.verify(
            [recovery_oco_order()],
            protection_state={
                "parent_order_id": "stale-bracket-parent",
                "take_profit_order_id": "stale-tp",
                "stop_loss_order_id": "stale-sl",
            },
        )

        self.assertTrue(result.protected)
        self.assertEqual(result.parent_order_id, "oco-parent-1")
        self.assertEqual(result.take_profit_order_id, "oco-parent-1")
        self.assertEqual(result.stop_loss_order_id, "oco-sl-1")
        self.assertEqual(result.protected_quantity, "4")

    def test_recovery_oco_wrong_take_profit_fails_closed(self):
        result = self.verify([recovery_oco_order(limit_price="400.00")])

        self.assertFalse(result.protected)
        self.assertIn("take_profit_price_mismatch", {issue.code for issue in result.issues})

    def test_recovery_oco_wrong_stop_loss_fails_closed(self):
        order = recovery_oco_order()
        order["legs"][0]["stop_price"] = "400.00"

        result = self.verify([order])

        self.assertFalse(result.protected)
        self.assertIn("invalid_long_stop_loss_price", {issue.code for issue in result.issues})
        self.assertIn("stop_loss_price_mismatch", {issue.code for issue in result.issues})

    def test_recovery_oco_wrong_quantity_fails_closed(self):
        order = recovery_oco_order(qty="3")

        result = self.verify([order])

        self.assertFalse(result.protected)
        self.assertIn("unprotected_partial_quantity", {issue.code for issue in result.issues})

    def test_recovery_oco_wrong_side_fails_closed(self):
        order = recovery_oco_order(side="buy")

        result = self.verify([order])

        self.assertFalse(result.protected)
        self.assertIn("invalid_parent_side", {issue.code for issue in result.issues})
        self.assertIn("invalid_take_profit_side", {issue.code for issue in result.issues})

    def test_duplicate_recovery_oco_fails_closed(self):
        duplicate = recovery_oco_order(id="oco-parent-2")

        result = self.verify([recovery_oco_order(), duplicate])

        self.assertFalse(result.protected)
        self.assertIn("duplicate_protection_group", {issue.code for issue in result.issues})

    def test_broker_flat_with_active_recovery_oco_fails_closed(self):
        result = verify_broker_native_protection(
            symbol="AAPL",
            broker_position=None,
            nested_orders=[recovery_oco_order()],
            exit_plan=long_plan(),
            expected_config_fingerprint="config-aapl",
            expected_market_contract_fingerprint="market-aapl",
        )

        self.assertFalse(result.protected)
        self.assertIn("orphan_protection_without_position", {issue.code for issue in result.issues})

    def test_malformed_recovery_oco_without_stop_fails_closed(self):
        order = recovery_oco_order(legs=[])

        result = self.verify([order])

        self.assertFalse(result.protected)
        self.assertIn("missing_stop_loss_leg", {issue.code for issue in result.issues})

    def test_unrelated_oco_fails_closed_with_stale_lineage(self):
        order = recovery_oco_order(client_order_id="unrelated-oco")

        result = self.verify(
            [order],
            protection_state={
                "parent_order_id": "stale-bracket-parent",
                "take_profit_order_id": "stale-tp",
                "stop_loss_order_id": "stale-sl",
            },
        )

        self.assertFalse(result.protected)
        self.assertIn("protection_lineage_mismatch", {issue.code for issue in result.issues})

    def test_captured_live_nvda_recovery_oco_shape_is_protected(self):
        plan = {
            "symbol": "NVDA",
            "intended_entry_side": "buy",
            "quantity": "1.0",
            "client_order_id": "camatron_nvda_1s_renko_0p22_e1_tp3_-045f570f8276",
            "take_profit_client_order_id": "camatron_nvda_1s_renko_0p22_e1_tp3_-045f570f8-tp",
            "stop_loss_client_order_id": "camatron_nvda_1s_renko_0p22_e1_tp3_-045f570f8-sl",
            "take_profit_limit_price": "220.09",
            "stop_loss_trigger_price": "217.12",
            "candidate_config_fingerprint": "config-nvda",
            "market_contract_fingerprint": "market-nvda",
        }
        result = verify_broker_native_protection(
            symbol="NVDA",
            broker_position={"symbol": "NVDA", "side": "long", "qty": "1", "avg_entry_price": "218.605"},
            nested_orders=[
                {
                    "id": "87b09536-e3cd-42b6-af31-23fd88b235b9",
                    "client_order_id": "camatron_nvda_1s_renko_0p22_e1_-r1-9e7505742a",
                    "symbol": "NVDA",
                    "side": "sell",
                    "qty": "1",
                    "filled_qty": "0",
                    "status": "new",
                    "order_class": "oco",
                    "type": "limit",
                    "limit_price": "220.09",
                    "legs": [
                        {
                            "id": "73c0a752-8bdc-40d0-b49f-d75ab307649e",
                            "symbol": "NVDA",
                            "side": "sell",
                            "qty": "1",
                            "filled_qty": "0",
                            "status": "held",
                            "order_class": "oco",
                            "type": "stop",
                            "stop_price": "217.12",
                        }
                    ],
                }
            ],
            exit_plan=plan,
            protection_state={
                "parent_order_id": "3361f6f6-282d-460c-a67b-6c47cdb137ab",
                "take_profit_order_id": "fa47708f-75ed-4f9e-a9b6-10404c927e80",
                "stop_loss_order_id": "9868d00b-3ef3-48f9-bc51-3a9bb64a3513",
            },
            expected_config_fingerprint="config-nvda",
            expected_market_contract_fingerprint="market-nvda",
        )

        self.assertTrue(result.protected)
        self.assertEqual(result.state, "PROTECTED")
        self.assertEqual(result.parent_order_id, "87b09536-e3cd-42b6-af31-23fd88b235b9")
        self.assertEqual(result.take_profit_order_id, "87b09536-e3cd-42b6-af31-23fd88b235b9")
        self.assertEqual(result.stop_loss_order_id, "73c0a752-8bdc-40d0-b49f-d75ab307649e")
        self.assertEqual(result.protected_quantity, "1")

    def test_valid_short_bracket_with_broker_generated_child_client_ids_is_protected_by_parent_order_id(self):
        result = verify_broker_native_protection(
            symbol="TSLA",
            broker_position=short_position(),
            nested_orders=[short_bracket_order_with_broker_child_client_ids()],
            exit_plan=short_plan(),
            protection_state={"parent_order_id": "e5f03802-d8fb-4521-a56d-cc0540389165"},
            expected_config_fingerprint="config-tsla",
            expected_market_contract_fingerprint="market-tsla",
        )

        self.assertTrue(result.protected)
        self.assertEqual(result.parent_order_id, "e5f03802-d8fb-4521-a56d-cc0540389165")
        self.assertEqual(result.take_profit_order_id, "d9dbb786-0ba6-4c6e-a8c3-6ae3b141033c")
        self.assertEqual(result.stop_loss_order_id, "cfd80ede-3923-44b8-bc82-a76955e758f8")

    def test_duplicate_flat_child_representation_merges_with_nested_parent(self):
        plan = camatron_tsla_live_plan()
        result = verify_broker_native_protection(
            symbol="TSLA",
            broker_position={"symbol": "TSLA", "side": "short", "qty": "-1", "avg_entry_price": "352.96"},
            nested_orders=[camatron_tsla_flat_child_order(), camatron_tsla_live_bracket_order()],
            exit_plan=plan,
            protection_state={
                "parent_order_id": "71172341-d91f-49da-9896-97076cf48ae4",
                "take_profit_order_id": "d0770042-74fa-4408-bd74-2389c037f1f8",
                "stop_loss_order_id": "bbcc4ac5-712d-48b9-8697-d71d63c5191d",
            },
            expected_config_fingerprint="config-tsla",
            expected_market_contract_fingerprint="market-tsla",
        )

        self.assertTrue(result.protected)
        self.assertEqual(result.parent_order_id, "71172341-d91f-49da-9896-97076cf48ae4")
        self.assertEqual(result.take_profit_order_id, "d0770042-74fa-4408-bd74-2389c037f1f8")
        self.assertEqual(result.stop_loss_order_id, "bbcc4ac5-712d-48b9-8697-d71d63c5191d")
        self.assertNotIn("duplicate_protection_group", {issue.code for issue in result.issues})

    def test_lineage_state_without_exit_plan_does_not_match_unrelated_group(self):
        result = verify_broker_native_protection(
            symbol="TSLA",
            broker_position=short_position(),
            nested_orders=[short_bracket_order_with_broker_child_client_ids()],
            exit_plan=None,
            protection_state={"parent_order_id": "other-parent"},
            expected_config_fingerprint="config-tsla",
            expected_market_contract_fingerprint="market-tsla",
        )

        self.assertFalse(result.protected)
        self.assertIn("missing_exit_plan", {issue.code for issue in result.issues})
        self.assertEqual(result.parent_order_id, "e5f03802-d8fb-4521-a56d-cc0540389165")

    def test_wrong_broker_parent_order_id_fails_lineage_match(self):
        result = verify_broker_native_protection(
            symbol="TSLA",
            broker_position=short_position(),
            nested_orders=[short_bracket_order_with_broker_child_client_ids(client_order_id="unexpected-parent-client-id")],
            exit_plan=short_plan(),
            protection_state={"parent_order_id": "other-parent"},
            expected_config_fingerprint="config-tsla",
            expected_market_contract_fingerprint="market-tsla",
        )

        self.assertFalse(result.protected)
        self.assertIn("protection_lineage_mismatch", {issue.code for issue in result.issues})

    def test_missing_take_profit_leg_fails_closed(self):
        order = bracket_order()
        order["legs"] = [order["legs"][1]]

        result = self.verify([order])

        self.assertFalse(result.protected)
        self.assertIn("missing_take_profit_leg", {issue.code for issue in result.issues})

    def test_incorrect_leg_side_fails_closed(self):
        order = bracket_order()
        order["legs"][0]["side"] = "buy"

        result = self.verify([order])

        self.assertFalse(result.protected)
        self.assertIn("invalid_take_profit_side", {issue.code for issue in result.issues})

    def test_incorrect_quantity_fails_closed(self):
        order = bracket_order()
        order["legs"][0]["qty"] = "5"

        result = self.verify([order])

        self.assertFalse(result.protected)
        self.assertIn("oversized_take_profit_quantity", {issue.code for issue in result.issues})

    def test_incorrect_price_fails_closed(self):
        order = bracket_order()
        order["legs"][1]["stop_price"] = "400.00"

        result = self.verify([order])

        self.assertFalse(result.protected)
        self.assertIn("invalid_long_stop_loss_price", {issue.code for issue in result.issues})
        self.assertIn("stop_loss_price_mismatch", {issue.code for issue in result.issues})

    def test_incorrect_parent_relationship_fails_closed(self):
        order = bracket_order()
        order["legs"][0]["parent_order_id"] = "other-parent"

        result = self.verify([order])

        self.assertFalse(result.protected)
        self.assertIn("invalid_take_profit_parent", {issue.code for issue in result.issues})

    def test_duplicate_protection_group_fails_closed(self):
        duplicate = bracket_order(id="parent-2")

        result = self.verify([bracket_order(), duplicate])

        self.assertFalse(result.protected)
        self.assertIn("duplicate_protection_group", {issue.code for issue in result.issues})

    def test_orphan_protection_without_position_fails_closed(self):
        result = self.verify([bracket_order()], broker_position=None, plan=long_plan())
        result = verify_broker_native_protection(
            symbol="AAPL",
            broker_position=None,
            nested_orders=[bracket_order()],
            exit_plan=long_plan(),
            expected_config_fingerprint="config-aapl",
            expected_market_contract_fingerprint="market-aapl",
        )

        self.assertFalse(result.protected)
        self.assertIn("orphan_protection_without_position", {issue.code for issue in result.issues})

    def test_partial_tp_fill_with_adjusted_sibling_quantity_is_protected(self):
        order = bracket_order()
        order["legs"][0]["status"] = "partially_filled"
        order["legs"][0]["filled_qty"] = "2"
        order["legs"][1]["qty"] = "2"

        result = self.verify([order], broker_position=position(qty="2"))

        self.assertTrue(result.protected)
        self.assertEqual(result.protected_quantity, "2")

    def test_partial_exit_without_adjusted_sibling_quantity_fails_closed(self):
        order = bracket_order()
        order["legs"][0]["status"] = "partially_filled"
        order["legs"][0]["filled_qty"] = "2"

        result = self.verify([order], broker_position=position(qty="2"))

        self.assertFalse(result.protected)
        self.assertIn("protective_quantity_mismatch", {issue.code for issue in result.issues})

    def test_manual_leg_cancellation_fails_closed(self):
        order = bracket_order()
        order["legs"][1]["status"] = "canceled"

        result = self.verify([order])

        self.assertFalse(result.protected)
        self.assertIn("invalid_stop_loss_status", {issue.code for issue in result.issues})

    def test_both_legs_filled_flags_reversal_risk(self):
        order = bracket_order()
        for leg in order["legs"]:
            leg["status"] = "filled"
            leg["filled_qty"] = "4"

        result = self.verify([order])

        self.assertFalse(result.protected)
        self.assertIn("both_protective_legs_filled", {issue.code for issue in result.issues})

    def test_missing_fingerprint_fails_closed(self):
        plan = dict(long_plan())
        plan["market_contract_fingerprint"] = None

        result = self.verify([bracket_order()], plan=plan)

        self.assertFalse(result.protected)
        self.assertIn("missing_market_contract_fingerprint", {issue.code for issue in result.issues})


if __name__ == "__main__":
    unittest.main()
