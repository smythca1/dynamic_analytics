import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from fleet_runner import (
    _BROKER_FORCE_FLAT_DATES,
    _PENDING_1S_CLOSE_TICKS,
    BrokerEngineStateMismatch,
    TraderRuntime,
    behavior_profile_block_reason,
    broker_force_flat_if_due,
    build_strategy_config,
    group_runtimes,
    renko_source_ticks,
    reconcile_engine_with_broker,
)
from execution_engine import AlpacaExecutionEngine
from scripts.live_signal_parity import iter_source_ticks
from strategy_engine import (
    MAX_CLIENT_ORDER_ID_LENGTH,
    MeanReversionStrategyConfig,
    MeanReversionStrategyEngine,
    RenkoStrategyConfig,
    RenkoStrategyEngine,
)
from trading_events import MarketTick, is_price_forming_trade


class MemoryLog:
    def __init__(self):
        self.events = []

    def write(self, event_type, payload):
        self.events.append((event_type, payload))


class RecordingExecution:
    def __init__(self):
        self.intents = []

    def execute(self, intent):
        self.intents.append(intent)
        return None


class FakePositionTrader:
    def _get_position(self, symbol):
        return {
            "symbol": symbol,
            "qty": "10",
            "side": "long",
            "avg_entry_price": "75.00",
        }


def live_config_trader(trader_id: str) -> dict:
    config = json.loads(Path("configs/traders.live.example.json").read_text(encoding="utf-8"))
    return next(trader for trader in config["traders"] if trader["id"] == trader_id)


class StrategyEngineOrderIntentTests(unittest.TestCase):
    def setUp(self):
        _BROKER_FORCE_FLAT_DATES.clear()
        _PENDING_1S_CLOSE_TICKS.clear()

    def test_one_second_renko_source_emits_completed_second_close(self):
        runtime = TraderRuntime(
            trader_id="unit_1s_source",
            symbol="TSLA",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=False,
            max_position_notional=1000,
            max_daily_loss=250,
            engine=object(),
            execution=object(),
            log=object(),
            renko_source="1s_close",
        )
        first = MarketTick(
            timestamp=pd.Timestamp("2026-06-23T13:30:00.100Z"),
            symbol="TSLA",
            price=400.0,
            size=1.0,
        )
        second = MarketTick(
            timestamp=pd.Timestamp("2026-06-23T13:30:00.900Z"),
            symbol="TSLA",
            price=401.0,
            size=1.0,
        )
        third = MarketTick(
            timestamp=pd.Timestamp("2026-06-23T13:30:01.050Z"),
            symbol="TSLA",
            price=402.0,
            size=1.0,
        )

        self.assertEqual(renko_source_ticks(runtime, first), [])
        self.assertEqual(renko_source_ticks(runtime, second), [])
        emitted = renko_source_ticks(runtime, third)

        self.assertEqual(len(emitted), 1)
        self.assertEqual(emitted[0].timestamp, pd.Timestamp("2026-06-23T13:30:00Z"))
        self.assertEqual(emitted[0].price, 401.0)

    def test_price_forming_trade_filter_rejects_non_price_forming_sip_conditions(self):
        self.assertTrue(is_price_forming_trade(MarketTick(pd.Timestamp("2026-07-01T14:00:00Z"), "AAPL", 295.0, 100)))
        self.assertTrue(
            is_price_forming_trade(
                MarketTick(pd.Timestamp("2026-07-01T14:00:00Z"), "AAPL", 295.0, 100, conditions=("@",))
            )
        )
        self.assertTrue(
            is_price_forming_trade(
                MarketTick(pd.Timestamp("2026-07-01T14:00:00Z"), "AAPL", 295.0, 100, conditions=("@", "F"))
            )
        )
        self.assertFalse(
            is_price_forming_trade(
                MarketTick(pd.Timestamp("2026-07-01T14:00:00Z"), "AAPL", 308.33, 3, conditions=("@", "I"))
            )
        )
        self.assertFalse(
            is_price_forming_trade(
                MarketTick(pd.Timestamp("2026-07-01T14:00:00Z"), "AAPL", 275.6151, 50, conditions=("@", "T", "W"))
            )
        )

    def test_replay_one_second_close_uses_last_price_forming_tick(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tick_dir = Path(tmpdir) / "ticks" / "2026-07-01"
            tick_dir.mkdir(parents=True)
            tick_path = tick_dir / "AAPL.jsonl"
            rows = [
                {
                    "timestamp": "2026-07-01T14:00:00.100000000+00:00",
                    "symbol": "AAPL",
                    "price": 295.0,
                    "size": 100,
                    "conditions": ["@"],
                },
                {
                    "timestamp": "2026-07-01T14:00:00.900000000+00:00",
                    "symbol": "AAPL",
                    "price": 308.33,
                    "size": 3,
                    "conditions": ["@", "I"],
                },
                {
                    "timestamp": "2026-07-01T14:00:01.050000000+00:00",
                    "symbol": "AAPL",
                    "price": 295.2,
                    "size": 100,
                    "conditions": ["@"],
                },
            ]
            tick_path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

            ticks, stats = iter_source_ticks(Path(tmpdir), "AAPL", ["2026-07-01"], "1s_close")
            emitted = list(ticks)

        self.assertEqual(stats["raw_tick_count"], 3)
        self.assertEqual(stats["source_tick_count"], 1)
        self.assertEqual(len(emitted), 1)
        self.assertEqual(emitted[0].timestamp, pd.Timestamp("2026-07-01T14:00:00Z"))
        self.assertEqual(emitted[0].price, 295.0)

    def test_client_order_id_preserves_unique_signal_suffix_for_long_trader_ids(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="megatron_tsla_renko_psar_2p0_e2_i3_a0p04_m0p6_overnight",
                symbol="TSLA",
                strategy_type="renko_psar",
                brick_size=2.0,
                entry_bricks=2,
                psar_initial_stop_bricks=3,
                psar_af_step=0.04,
                psar_af_max=0.6,
                allow_short=False,
                notional=100,
            )
        )
        first = engine._build_signal(
            signal_type="entry",
            timestamp=pd.Timestamp("2026-05-28T14:00:00Z"),
            side="long",
            broker_side="buy",
            price=440,
            reason="long_entry",
        )
        second = engine._build_signal(
            signal_type="entry",
            timestamp=pd.Timestamp("2026-05-28T14:01:00Z"),
            side="long",
            broker_side="buy",
            price=441,
            reason="long_entry",
        )

        first_intent = engine._build_order_intent(first, close_position=False)
        second_intent = engine._build_order_intent(second, close_position=False)

        self.assertLessEqual(len(first_intent.client_order_id), MAX_CLIENT_ORDER_ID_LENGTH)
        self.assertLessEqual(len(second_intent.client_order_id), MAX_CLIENT_ORDER_ID_LENGTH)
        self.assertNotEqual(first_intent.client_order_id, second_intent.client_order_id)
        self.assertTrue(first_intent.client_order_id.endswith(first.signal_id.rsplit("-", 1)[-1]))
        self.assertTrue(second_intent.client_order_id.endswith(second.signal_id.rsplit("-", 1)[-1]))

    def test_reversal_intent_carries_target_qty(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="megatron_tsla",
                symbol="TSLA",
                strategy_type="renko_psar",
                brick_size=2.0,
                entry_bricks=2,
                psar_initial_stop_bricks=3,
                psar_af_step=0.04,
                psar_af_max=0.6,
                qty=1,
            )
        )
        signal = engine._build_signal(
            signal_type="entry",
            timestamp=pd.Timestamp("2026-06-02T14:11:00Z"),
            side="short",
            broker_side="sell",
            price=418,
            reason="short_entry_psar_reversal",
        )

        intent = engine._build_order_intent(signal, close_position=False, qty=1.235849, target_qty=1)

        self.assertEqual(intent.qty, 1.235849)
        self.assertEqual(intent.target_qty, 1)

    def test_live_config_psar_candidate_is_disabled_and_parseable(self):
        trader = live_config_trader("candidate_tsla_renko_psar_1p90_e2_i3_a0p04_m0p6_disabled")

        self.assertFalse(trader["enabled"])
        self.assertFalse(trader["execution_enabled"])
        self.assertFalse(trader["promotion"]["production_eligible"])
        self.assertEqual(trader["profile"]["trust_status"], "unvalidated_candidate")

        config = build_strategy_config(trader)

        self.assertEqual(config.strategy_type, "renko_psar")
        self.assertEqual(config.symbol, "TSLA")
        self.assertEqual(config.brick_size, 1.9)
        self.assertEqual(config.entry_bricks, 2)
        self.assertEqual(config.psar_initial_stop_bricks, 3)
        self.assertEqual(config.psar_af_step, 0.04)
        self.assertEqual(config.psar_af_max, 0.6)
        self.assertEqual(config.exit_management, "strategy")
        self.assertFalse(config.allow_overnight)

    def test_psar_candidate_can_emit_reversal_intent_from_synthetic_ticks(self):
        trader = live_config_trader("candidate_tsla_renko_psar_1p90_e2_i3_a0p04_m0p6_disabled")
        engine = RenkoStrategyEngine(build_strategy_config(trader))

        emitted = []
        for offset, price in enumerate([100.0, 100.7, 102.6, 104.5, 96.9, 95.0]):
            _bricks, actions = engine.on_tick(
                MarketTick(
                    timestamp=pd.Timestamp("2026-06-18T14:00:00Z") + pd.Timedelta(seconds=offset),
                    symbol="TSLA",
                    price=price,
                    size=1.0,
                    exchange="unit",
                    trade_id=f"unit-{offset}",
                )
            )
            emitted.extend(actions)

        self.assertGreaterEqual(len(emitted), 2)
        entry_signal, entry_intent = emitted[0]
        reversal_signal, reversal_intent = emitted[1]

        self.assertEqual(entry_signal.signal_type, "entry")
        self.assertEqual(entry_signal.side, "long")
        self.assertEqual(entry_intent.side, "buy")
        self.assertEqual(entry_intent.qty, 1)

        self.assertEqual(reversal_signal.signal_type, "entry")
        self.assertEqual(reversal_signal.side, "short")
        self.assertEqual(reversal_signal.reason, "short_entry_psar_reversal")
        self.assertEqual(reversal_intent.side, "sell")
        self.assertEqual(reversal_intent.qty, 2)
        self.assertEqual(reversal_intent.target_qty, 1)

    def test_live_config_aapl_reversal_is_enabled_and_parseable(self):
        trader = live_config_trader("camatron_aapl_1s_renko_1p3443_e1_reversal_1sh_20260701_live")

        self.assertTrue(trader["enabled"])
        self.assertTrue(trader["execution_enabled"])
        self.assertEqual(trader["symbol"], "AAPL")
        self.assertEqual(trader["sizing"]["qty"], 1)
        self.assertTrue(trader["session"]["allow_overnight"])

        config = build_strategy_config(trader)

        self.assertEqual(config.strategy_type, "renko_reversal")
        self.assertEqual(config.symbol, "AAPL")
        self.assertEqual(config.brick_size, 1.3443)
        self.assertEqual(config.entry_bricks, 1)
        self.assertEqual(config.qty, 1)
        self.assertTrue(config.allow_overnight)

    def test_live_config_tsla_reversal_is_enabled_and_parseable(self):
        trader = live_config_trader("camatron_tsla_1s_renko_2p31165_e1_reversal_1sh_20260710_live")

        self.assertTrue(trader["enabled"])
        self.assertTrue(trader["execution_enabled"])
        self.assertEqual(trader["symbol"], "TSLA")
        self.assertEqual(trader["sizing"]["qty"], 1)
        self.assertTrue(trader["session"]["allow_overnight"])

        config = build_strategy_config(trader)

        self.assertEqual(config.strategy_type, "renko_reversal")
        self.assertEqual(config.symbol, "TSLA")
        self.assertEqual(config.brick_size, 2.31165)
        self.assertEqual(config.entry_bricks, 1)
        self.assertEqual(config.qty, 1)
        self.assertTrue(config.allow_overnight)

    def test_renko_reversal_emits_one_share_reversal_intent(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_aapl_reversal_test",
                symbol="AAPL",
                strategy_type="renko_reversal",
                brick_size=1.0,
                entry_bricks=1,
                qty=1,
                renko_anchor_mode="grid",
                renko_anchor_price=0.0,
                entry_start="09:31:00",
                entry_end="15:59:59",
                allow_overnight=True,
            )
        )

        emitted = []
        for offset, price in enumerate([100.0, 101.0, 99.0]):
            _bricks, actions = engine.on_tick(
                MarketTick(
                    timestamp=pd.Timestamp("2026-07-01T14:00:00Z") + pd.Timedelta(seconds=offset),
                    symbol="AAPL",
                    price=price,
                    size=1.0,
                    exchange="unit",
                    trade_id=f"unit-{offset}",
                )
            )
            emitted.extend(actions)

        self.assertEqual(len(emitted), 2)
        entry_signal, entry_intent = emitted[0]
        reversal_signal, reversal_intent = emitted[1]

        self.assertEqual(entry_signal.reason, "long_entry_e1")
        self.assertEqual(entry_signal.side, "long")
        self.assertEqual(entry_intent.side, "buy")
        self.assertEqual(entry_intent.qty, 1)
        self.assertIsNone(entry_intent.target_qty)

        self.assertEqual(reversal_signal.reason, "short_entry_e1_reversal")
        self.assertEqual(reversal_signal.side, "short")
        self.assertEqual(reversal_intent.side, "sell")
        self.assertEqual(reversal_intent.qty, 2)
        self.assertEqual(reversal_intent.target_qty, 1)

    def test_fleet_strategy_config_reads_order_type(self):
        config = build_strategy_config(
            {
                "id": "test_limit_order_profile",
                "symbol": "QQQ",
                "strategy": {
                    "type": "renko_tp_sl",
                    "brick_size": 1.0,
                    "entry_bricks": 1,
                    "take_profit_bricks": 2,
                    "stop_loss_bricks": 3,
                    "order_type": "limit",
                },
                "sizing": {
                    "notional": 100,
                },
            }
        )

        self.assertEqual(config.order_type, "limit")

    def test_oco_strategy_rejects_fractional_qty_sizing(self):
        with self.assertRaisesRegex(ValueError, "does not support fractional qty"):
            build_strategy_config(
                {
                    "id": "collatron_fractional_oco",
                    "symbol": "SOXL",
                    "strategy": {
                        "type": "renko_tp_sl",
                        "brick_size": 1.0,
                        "entry_bricks": 1,
                        "take_profit_bricks": 2,
                        "stop_loss_bricks": 2,
                        "exit_management": "oco",
                    },
                    "sizing": {
                        "qty": 0.25,
                    },
                }
            )

    def test_behavior_profile_block_reason_checks_generated_profile_caps(self):
        reason = behavior_profile_block_reason(
            {
                "profile": {
                    "trade_cost_proxy_bps": 6.5,
                    "p95_abs_1m_return_bps": 20.0,
                },
                "risk": {
                    "max_trade_cost_proxy_bps": 5.0,
                    "max_1m_p95_abs_return_bps": 80.0,
                },
            }
        )

        self.assertEqual(reason, "profile_trade_cost_proxy_6.5000_exceeds_5.0000_bps")

    def test_fleet_market_data_grouping_ignores_execution_credential_profile(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="default_tsla",
                symbol="TSLA",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=1,
                stop_loss_bricks=1,
                qty=1,
            )
        )
        runtimes = [
            TraderRuntime(
                trader_id="default_tsla",
                symbol="TSLA",
                mode="live",
                credential_profile=None,
                feed="iex",
                enabled=True,
                execution_enabled=True,
                max_position_notional=1000,
                max_daily_loss=0,
                engine=engine,
                execution=object(),
                log=object(),
            ),
            TraderRuntime(
                trader_id="megatron_qqq",
                symbol="QQQ",
                mode="live",
                credential_profile="megatron",
                feed="iex",
                enabled=True,
                execution_enabled=True,
                max_position_notional=1000,
                max_daily_loss=0,
                engine=engine,
                execution=object(),
                log=object(),
            ),
            TraderRuntime(
                trader_id="collatron_qqq",
                symbol="QQQ",
                mode="live",
                credential_profile="collatron",
                feed="iex",
                enabled=True,
                execution_enabled=True,
                max_position_notional=1000,
                max_daily_loss=0,
                engine=engine,
                execution=object(),
                log=object(),
            ),
        ]

        grouped = group_runtimes(runtimes)

        self.assertEqual(list(grouped), [("live", "iex")])
        self.assertEqual(len(grouped[("live", "iex")]), 3)

    def test_broker_force_flat_runs_when_engine_is_flat(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_tsla_test",
                symbol="TSLA",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=1,
                stop_loss_bricks=1,
                qty=25,
                force_flat="15:59:30",
                timezone="America/New_York",
                allow_overnight=False,
            )
        )
        execution = RecordingExecution()
        log = MemoryLog()
        runtime = TraderRuntime(
            trader_id="camatron_tsla_test",
            symbol="TSLA",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=1000,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=log,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            did_force_flat = broker_force_flat_if_due(
                Path(temp_dir),
                runtime,
                pd.Timestamp("2026-06-08T19:59:30Z"),
                price=408.8,
                source="test",
            )
            did_repeat = broker_force_flat_if_due(
                Path(temp_dir),
                runtime,
                pd.Timestamp("2026-06-08T19:59:31Z"),
                price=408.7,
                source="test",
            )

        self.assertTrue(did_force_flat)
        self.assertFalse(did_repeat)
        self.assertEqual(len(execution.intents), 1)
        intent = execution.intents[0]
        self.assertTrue(intent.close_position)
        self.assertEqual(intent.order_type, "market")
        self.assertEqual(intent.reason, "session_force_flat")
        self.assertEqual(intent.side, "sell")
        self.assertIn("2026-06-08", engine.trade_state_snapshot()["force_flat_dates"])
        self.assertIn("broker_force_flat", [event_type for event_type, _payload in log.events])
        self.assertIn("signal", [event_type for event_type, _payload in log.events])

    def test_broker_position_engine_flat_reconcile_is_hard_failure(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_tqqq_test",
                symbol="TQQQ",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=1,
                stop_loss_bricks=1,
                qty=10,
            )
        )
        execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        execution.trader = FakePositionTrader()
        execution.log = None
        log = MemoryLog()
        runtime = TraderRuntime(
            trader_id="camatron_tqqq_test",
            symbol="TQQQ",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=1000,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=log,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(BrokerEngineStateMismatch):
                reconcile_engine_with_broker(Path(temp_dir), runtime)

        self.assertEqual(log.events[0][0], "state_reconcile_error")
        self.assertEqual(log.events[0][1]["reason"], "broker_position_exists_engine_flat")

    def test_mean_reversion_enters_long_on_low_efficiency_down_stretch(self):
        engine = MeanReversionStrategyEngine(
            MeanReversionStrategyConfig(
                trader_id="camatron_qqq_chop_test",
                symbol="QQQ",
                window_seconds=180,
                entry_z=2.0,
                exit_z=0.10,
                stop_z=3.0,
                efficiency_max=0.35,
                trend_exit_efficiency=0.65,
                max_hold_seconds=300,
                qty=1,
                entry_start="09:35:00",
                entry_end="15:45:00",
                force_flat="15:58:00",
                active_start_date="2026-07-15",
                allow_overnight=False,
            )
        )

        actions = []
        start = pd.Timestamp("2026-07-15T13:35:00Z")
        prices = ([100.0, 100.2] * 75) + [98.0]
        for idx, price in enumerate(prices):
            _bricks, emitted = engine.on_tick(
                MarketTick(timestamp=start + pd.Timedelta(seconds=idx), symbol="QQQ", price=price, size=1)
            )
            actions.extend(emitted)

        self.assertEqual(len(actions), 1)
        signal, intent = actions[0]
        self.assertEqual(signal.signal_type, "entry")
        self.assertEqual(signal.side, "long")
        self.assertEqual(intent.side, "buy")
        self.assertEqual(intent.qty, 1)
        self.assertEqual(signal.reason, "long_entry_chop_mean_reversion")

    def test_mean_reversion_exits_on_mean_reversion(self):
        engine = MeanReversionStrategyEngine(
            MeanReversionStrategyConfig(
                trader_id="camatron_qqq_chop_test",
                symbol="QQQ",
                window_seconds=180,
                entry_z=2.0,
                exit_z=0.10,
                stop_z=3.0,
                efficiency_max=0.35,
                trend_exit_efficiency=0.65,
                max_hold_seconds=300,
                qty=1,
                entry_start="09:35:00",
                entry_end="15:45:00",
                force_flat="15:58:00",
                active_start_date="2026-07-15",
                allow_overnight=False,
            )
        )

        actions = []
        start = pd.Timestamp("2026-07-15T13:35:00Z")
        prices = ([100.0, 100.2] * 75) + [98.0] + [100.2]
        for idx, price in enumerate(prices):
            _bricks, emitted = engine.on_tick(
                MarketTick(timestamp=start + pd.Timedelta(seconds=idx), symbol="QQQ", price=price, size=1)
            )
            actions.extend(emitted)

        self.assertEqual(len(actions), 2)
        exit_signal, exit_intent = actions[1]
        self.assertEqual(exit_signal.signal_type, "exit")
        self.assertEqual(exit_signal.reason, "mean_reversion_exit")
        self.assertTrue(exit_intent.close_position)

    def test_mean_reversion_blocks_before_active_start_date(self):
        engine = MeanReversionStrategyEngine(
            MeanReversionStrategyConfig(
                trader_id="camatron_qqq_chop_test",
                symbol="QQQ",
                window_seconds=180,
                entry_z=2.0,
                exit_z=0.10,
                stop_z=3.0,
                efficiency_max=0.35,
                trend_exit_efficiency=0.65,
                max_hold_seconds=300,
                qty=1,
                entry_start="09:35:00",
                entry_end="15:45:00",
                force_flat="15:58:00",
                active_start_date="2026-07-15",
                allow_overnight=False,
            )
        )

        actions = []
        start = pd.Timestamp("2026-07-14T13:35:00Z")
        prices = ([100.0, 100.2] * 75) + [98.0]
        for idx, price in enumerate(prices):
            _bricks, emitted = engine.on_tick(
                MarketTick(timestamp=start + pd.Timedelta(seconds=idx), symbol="QQQ", price=price, size=1)
            )
            actions.extend(emitted)

        self.assertEqual(actions, [])


if __name__ == "__main__":
    unittest.main()
