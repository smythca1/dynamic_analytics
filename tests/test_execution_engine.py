import unittest

import pandas as pd

from execution_engine import AlpacaExecutionEngine
from trading_events import OrderIntent


class RecordingTrader:
    def __init__(self):
        self.oco_client_order_ids = []
        self.payloads = []
        self.account = {"id": "acct-ok"}

    def submit_order(self, payload, action, side):
        self.payloads.append(payload)
        return {"status": "order sent", "response": {"id": f"entry-{len(self.oco_client_order_ids)}"}}

    def submit_oco_exit_order(self, **kwargs):
        self.oco_client_order_ids.append(kwargs["client_order_id"])
        return {"status": "oco exit sent"}

    def get_account(self):
        return self.account


class AlpacaExecutionEngineTests(unittest.TestCase):
    def test_oco_child_order_id_keeps_unique_suffix_for_long_entry_ids(self):
        engine = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        engine.trader = RecordingTrader()
        engine.log = None

        for client_order_id in (
            "megatron_nvda_renko_tp_sl_0p40_e1_t-309ee939f30b",
            "megatron_nvda_renko_tp_sl_0p40_e1_t-5af244da69fa",
        ):
            engine.execute(
                OrderIntent(
                    client_order_id=client_order_id,
                    timestamp=pd.Timestamp("2026-06-09T14:00:00Z"),
                    symbol="NVDA",
                    side="buy",
                    signal_type="entry",
                    order_type="market",
                    strategy="nvda_test",
                    reason="renko_run_1_long",
                    qty=10,
                    exit_management="oco",
                    stop_loss_order_type="stop_market",
                    take_profit_limit_price=210.97,
                    stop_loss_stop_price=205.99,
                    take_profit_offset=1.6612,
                    stop_loss_offset=3.3224,
                    close_position=False,
                    trader_id="megatron_nvda_renko_tp_sl_0p40_e1_tp2_sl4_10sh_live",
                )
            )

        self.assertEqual(len(engine.trader.oco_client_order_ids), 2)
        self.assertNotEqual(*engine.trader.oco_client_order_ids)
        self.assertTrue(engine.trader.oco_client_order_ids[0].endswith("309ee939f30b-oco"))
        self.assertTrue(engine.trader.oco_client_order_ids[1].endswith("5af244da69fa-oco"))

    def test_close_payload_preserves_signal_lineage_and_lifecycle(self):
        engine = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        engine.trader = RecordingTrader()
        engine.log = None

        engine.execute(
            OrderIntent(
                client_order_id="tp-sl-close",
                timestamp=pd.Timestamp("2026-06-09T14:00:00Z"),
                symbol="NVDA",
                side="sell",
                signal_type="exit",
                order_type="market",
                strategy="nvda_test",
                reason="take_profit_2_bricks",
                target_qty=0,
                close_position=True,
                trader_id="megatron_nvda",
                signal_id="signal-tp",
                lifecycle_state="TP_OR_SL_TRIGGERED",
                account_namespace="megatron",
            )
        )

        payload = engine.trader.payloads[0]
        self.assertEqual(payload["reason"], "take_profit_2_bricks")
        self.assertEqual(payload["target_qty"], 0)
        self.assertEqual(payload["signal_id"], "signal-tp")
        self.assertEqual(payload["lifecycle_state"], "TP_OR_SL_TRIGGERED")
        self.assertEqual(payload["signal_type"], "exit")
        self.assertEqual(payload["account_namespace"], "megatron")

    def test_execution_engine_rejects_wrong_account_namespace_before_submit(self):
        engine = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        engine.trader = RecordingTrader()
        engine.log = None
        engine.account_namespace = "megatron"
        engine.expected_account_id = None
        engine._verified_account_id = None

        with self.assertRaises(RuntimeError):
            engine.execute(
                OrderIntent(
                    client_order_id="wrong-account",
                    timestamp=pd.Timestamp("2026-06-09T14:00:00Z"),
                    symbol="NVDA",
                    side="buy",
                    signal_type="entry",
                    order_type="market",
                    strategy="nvda_test",
                    reason="renko_run_1_long",
                    qty=1,
                    close_position=False,
                    trader_id="camatron_nvda",
                    account_namespace="camatron",
                )
            )

        self.assertEqual(engine.trader.payloads, [])

    def test_execution_engine_verifies_expected_account_id_before_submit(self):
        engine = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        engine.trader = RecordingTrader()
        engine.log = None
        engine.account_namespace = "megatron"
        engine.expected_account_id = "acct-ok"
        engine._verified_account_id = None

        engine.execute(
            OrderIntent(
                client_order_id="right-account",
                timestamp=pd.Timestamp("2026-06-09T14:00:00Z"),
                symbol="NVDA",
                side="buy",
                signal_type="entry",
                order_type="market",
                strategy="nvda_test",
                reason="renko_run_1_long",
                qty=1,
                close_position=False,
                trader_id="megatron_nvda",
                account_namespace="megatron",
            )
        )

        self.assertEqual(engine._verified_account_id, "acct-ok")
        self.assertEqual(len(engine.trader.payloads), 1)


if __name__ == "__main__":
    unittest.main()
