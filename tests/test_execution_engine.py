import unittest
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch
from fastapi import HTTPException

import pandas as pd

from execution_engine import AlpacaExecutionEngine
from trading_events import OrderIntent
from trading_events import MarketTick
from alpaca_trader import AlpacaTrader, AlpacaCredentials
from strategy_engine import RenkoStrategyEngine, RenkoStrategyConfig
from broker_protection import verify_broker_native_protection


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
    @staticmethod
    def bracket_intent(side="buy", symbol="TQQQ"):
        strategy = RenkoStrategyEngine(RenkoStrategyConfig(
            trader_id="generic_tp_sl", symbol=symbol, strategy_type="renko_tp_sl",
            strategy_name="generic_tp_sl", brick_size=0.700920, entry_bricks=2,
            take_profit_bricks=1, stop_loss_bricks=3, qty=20,
            exit_management="bracket", candidate_config_fingerprint="candidate",
            market_contract_fingerprint="market", entry_start="09:30:15", entry_end="15:50:15",
        ))
        prices = [100,100.8,101.5] if side=="buy" else [100,99.2,98.5]
        actions=[]
        for i, price in enumerate(prices):
            _, emitted = strategy.on_tick(MarketTick(pd.Timestamp("2026-11-11T15:00:00Z")+pd.Timedelta(seconds=i),symbol,price,1))
            actions.extend(emitted)
        return strategy, actions[0][1]

    @staticmethod
    def live_engine():
        engine=AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        engine.trader=AlpacaTrader(AlpacaCredentials(mode="live",key_id="test",secret_key="test",base_url="https://example.test"),max_notional=250000)
        engine.log=None
        return engine

    def test_real_adapter_posts_one_atomic_long_and_short_bracket(self):
        for side in ("buy","sell"):
            for symbol in ("TQQQ","AMD"):
                with self.subTest(side=side,symbol=symbol):
                    strategy,intent=self.bracket_intent(side,symbol)
                    engine=self.live_engine()
                    posts=[]
                    def request(**kwargs):
                        if kwargs["method"]=="GET":
                            return SimpleNamespace(status_code=404,text="{}",json=lambda: {})
                        body=kwargs["json"]; posts.append(body)
                        closing="sell" if side=="buy" else "buy"
                        parent=dict(body,id="parent",status="filled",filled_qty="20",legs=[
                            {"id":"tp","symbol":symbol,"side":closing,"qty":"20","filled_qty":"0","status":"new","limit_price":body["take_profit"]["limit_price"]},
                            {"id":"sl","symbol":symbol,"side":closing,"qty":"20","filled_qty":"0","status":"new","stop_price":body["stop_loss"]["stop_price"]},
                        ])
                        return SimpleNamespace(status_code=200,text=json.dumps(parent),json=lambda:parent)
                    with patch("alpaca_trader.requests.request",side_effect=request):
                        event=engine.execute(intent)
                    self.assertEqual(len(posts),1)
                    self.assertEqual(posts[0]["order_class"],"bracket")
                    self.assertEqual(posts[0]["qty"],"20")
                    self.assertFalse(posts[0]["extended_hours"])
                    self.assertEqual(posts[0]["take_profit"]["limit_price"],intent.exit_plan["take_profit_limit_price"])
                    self.assertEqual(posts[0]["stop_loss"]["stop_price"],intent.exit_plan["stop_loss_trigger_price"])
                    verified=verify_broker_native_protection(
                        symbol=symbol,broker_position={"symbol":symbol,"side":"long" if side=="buy" else "short","qty":"20","avg_entry_price":intent.exit_plan["entry_reference_price"]},
                        nested_orders=[event.response["response"]],exit_plan=intent.exit_plan,
                        expected_config_fingerprint="candidate",expected_market_contract_fingerprint="market",
                    )
                    self.assertTrue(verified.protected,verified.issues)
                    strategy.mark_entry_protection_submitted(intent,event.response)
                    self.assertEqual(strategy.protection_state["take_profit_order_id"],"tp")
                    self.assertEqual(strategy.protection_state["stop_loss_order_id"],"sl")

    def test_live_tp_sl_cannot_reach_adapter_without_canonical_bracket(self):
        _,intent=self.bracket_intent()
        for changes in ({"exit_management":"strategy"},{"exit_management":"oco"},{"exit_plan":None},
                        {"qty":21},{"take_profit_limit_price":123},{"candidate_config_fingerprint":"other"}):
            with self.subTest(changes=changes), patch("alpaca_trader.requests.request") as request:
                with self.assertRaises(HTTPException):
                    self.live_engine().execute(replace(intent,**changes))
                request.assert_not_called()

    def test_rejected_or_unacknowledged_bracket_never_falls_back_to_naked_order(self):
        _,intent=self.bracket_intent()
        for status,order_class in (("rejected","bracket"),("accepted",""),("","bracket")):
            with self.subTest(status=status,order_class=order_class):
                engine=self.live_engine()
                posted=[]
                def request(**kwargs):
                    if kwargs["method"]=="GET":
                        return SimpleNamespace(status_code=404,text="{}",json=lambda:{})
                    posted.append(kwargs["json"])
                    body={"id":"parent","status":status,"order_class":order_class}
                    return SimpleNamespace(status_code=200,text=json.dumps(body),json=lambda:body)
                with patch("alpaca_trader.requests.request",side_effect=request):
                    with self.assertRaises(HTTPException):
                        engine.execute(intent)
                self.assertEqual(len(posted),1)
                self.assertEqual(posted[0]["order_class"],"bracket")

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
