import unittest

from exit_plan import build_renko_tp_sl_exit_plan


class ExitPlanTests(unittest.TestCase):
    def test_long_bracket_exit_plan_prices_and_lineage(self):
        plan = build_renko_tp_sl_exit_plan(
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
            client_order_id="megatron-aapl-entry-abcdef",
            candidate_config_fingerprint="config-aapl",
            market_contract_fingerprint="market-aapl",
        )

        self.assertEqual(plan.closing_side, "sell")
        self.assertEqual(plan.take_profit_limit_price, "311.50")
        self.assertEqual(plan.stop_loss_trigger_price, "306.40")
        self.assertEqual(plan.entry_reference_type, "signal_price")
        self.assertTrue(plan.take_profit_client_order_id.endswith("-tp"))
        self.assertTrue(plan.stop_loss_client_order_id.endswith("-sl"))
        self.assertEqual(len(plan.fingerprint), 64)

    def test_short_bracket_exit_plan_prices(self):
        plan = build_renko_tp_sl_exit_plan(
            trader_id="megatron_tsla",
            strategy_id="tsla_tp_sl",
            symbol="TSLA",
            entry_side="sell",
            quantity=4,
            entry_reference_type="signal_price",
            entry_reference_price="361.712",
            brick_size="1.5392",
            take_profit_bricks=3,
            stop_loss_bricks=1,
            time_in_force="day",
            order_class="bracket",
            client_order_id="megatron-tsla-entry-abcdef",
            candidate_config_fingerprint="config-tsla",
            market_contract_fingerprint="market-tsla",
        )

        self.assertEqual(plan.closing_side, "buy")
        self.assertEqual(plan.take_profit_limit_price, "357.09")
        self.assertEqual(plan.stop_loss_trigger_price, "363.25")

    def test_invalid_zero_stop_bricks_rejects(self):
        with self.assertRaisesRegex(ValueError, "TP and SL bricks must be positive"):
            build_renko_tp_sl_exit_plan(
                trader_id="bad",
                strategy_id="bad",
                symbol="AAPL",
                entry_side="buy",
                quantity=1,
                entry_reference_type="signal_price",
                entry_reference_price="1.00",
                brick_size="0.001",
                take_profit_bricks=1,
                stop_loss_bricks=0,
                time_in_force="day",
                order_class="bracket",
                client_order_id="bad-entry",
                candidate_config_fingerprint="config-bad",
                market_contract_fingerprint="market-bad",
            )

    def test_missing_fingerprints_rejects(self):
        with self.assertRaisesRegex(ValueError, "fingerprint"):
            build_renko_tp_sl_exit_plan(
                trader_id="bad",
                strategy_id="bad",
                symbol="AAPL",
                entry_side="buy",
                quantity=1,
                entry_reference_type="signal_price",
                entry_reference_price="100",
                brick_size="1",
                take_profit_bricks=1,
                stop_loss_bricks=1,
                time_in_force="day",
                order_class="bracket",
                client_order_id="bad-entry",
            )


if __name__ == "__main__":
    unittest.main()
