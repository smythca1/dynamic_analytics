import unittest

import pandas as pd

from execution_engine import AlpacaExecutionEngine
from trading_events import OrderIntent


class RecordingTrader:
    def __init__(self):
        self.oco_client_order_ids = []

    def submit_order(self, payload, action, side):
        return {"status": "order sent", "response": {"id": f"entry-{len(self.oco_client_order_ids)}"}}

    def submit_oco_exit_order(self, **kwargs):
        self.oco_client_order_ids.append(kwargs["client_order_id"])
        return {"status": "oco exit sent"}


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


if __name__ == "__main__":
    unittest.main()
