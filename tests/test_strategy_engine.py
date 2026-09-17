import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from fleet_runner import (
    _BROKER_FORCE_FLAT_DATES,
    BrokerEngineStateMismatch,
    TraderRuntime,
    archive_tick,
    behavior_profile_block_reason,
    broker_force_flat_if_due,
    build_strategy_config,
    group_runtimes,
    handle_action,
    market_archive_root,
    migrate_renko_brick_change_state,
    process_market_tick_for_runtime,
    close_stream_after_pending_read,
    reconcile_broker_state_if_due_or_halt,
    reconcile_engine_with_broker,
    should_block_for_risk,
)
from execution_engine import AlpacaExecutionEngine
from exit_plan import build_renko_tp_sl_exit_plan
from reversal_protection import build_reversal_catastrophic_stop_plan, catastrophic_stop_order_payload
from silver.market import (
    CanonicalMarketObservationAdapter,
    LocalArchiveMarketDataTail,
    live_market_event_from_alpaca_message,
    live_market_event_from_archive_payload,
)
from shared.config.trader_config import TraderConfigError, validate_single_strategy_kernel_config
from scripts.live_signal_parity import iter_source_ticks
from strategy_engine import (
    MAX_CLIENT_ORDER_ID_LENGTH,
    TP_SL_LIFECYCLE_BRACKET_SUBMITTED,
    TP_SL_LIFECYCLE_CLOSE_SUBMITTED,
    TP_SL_LIFECYCLE_ENTRY_FILLED_AWAITING_PROTECTION,
    TP_SL_LIFECYCLE_ENTRY_PLANNED,
    TP_SL_LIFECYCLE_EXIT_PENDING,
    TP_SL_LIFECYCLE_FLAT,
    TP_SL_LIFECYCLE_PROTECTION_FAILED,
    TP_SL_LIFECYCLE_PROTECTED,
    TP_SL_LIFECYCLE_TRIGGERED,
    MeanReversionStrategyConfig,
    MeanReversionStrategyEngine,
    RenkoStrategyConfig,
    RenkoStrategyEngine,
)
from trading_events import ExecutionEvent, MarketTick, OrderIntent, PositionState, Signal, is_price_forming_trade


class MemoryLog:
    def __init__(self):
        self.events = []

    def write(self, event_type, payload):
        self.events.append((event_type, payload))


class RecordingExecution:
    def __init__(self):
        self.intents = []
        self.event = None

    def execute(self, intent):
        self.intents.append(intent)
        return self.event


class FakePositionTrader:
    def _get_position(self, symbol):
        return {
            "symbol": symbol,
            "qty": "10",
            "side": "long",
            "avg_entry_price": "75.00",
        }


class FakeMigrationTrader:
    def __init__(self, position=None, latest_price=None, latest_error=None):
        self.position = position
        self.latest_price = latest_price
        self.latest_error = latest_error
        self.orders = []
        self.fills = []

    def _get_position(self, symbol):
        return self.position

    def get_latest_trade_price(self, symbol, feed=None):
        if self.latest_error is not None:
            raise self.latest_error
        if self.latest_price is None:
            raise RuntimeError("missing latest price")
        return self.latest_price, "2026-08-05T14:00:00Z"


class FakeRiskTrader:
    def __init__(self, *, shorting_enabled=True, shortable=True, positions=None):
        self.shorting_enabled = shorting_enabled
        self.shortable = shortable
        self.positions = positions or []

    def get_account(self):
        return {
            "id": "acct-risk",
            "buying_power": "100000",
            "daytrading_buying_power": "0",
            "shorting_enabled": self.shorting_enabled,
        }

    def get_positions(self):
        return self.positions

    def get_asset(self, symbol):
        return {
            "symbol": symbol,
            "status": "active",
            "tradable": True,
            "marginable": True,
            "shortable": self.shortable,
            "easy_to_borrow": self.shortable,
        }


class FakeProtectionTrader:
    def __init__(self, *, position=None, orders=None, fills=None, parent=None):
        self.position = position
        self.orders = orders or []
        self.fills = fills or []
        self.parent = parent
        self.cancelled = []
        self.recovery_orders = []

    def _get_position(self, symbol):
        return self.position

    def get_position(self, symbol):
        return self.position

    def get_open_orders_nested(self):
        return self.orders

    def get_order_nested(self, order_id):
        return self.parent or (self.orders[0] if self.orders else {"id": order_id, "status": "filled", "filled_qty": "0", "qty": "0"})

    def get_order(self, order_id):
        if self.parent and self.parent.get("id") == order_id:
            return self.parent
        for order in self.orders:
            if order.get("id") == order_id:
                return order
        return {"id": order_id, "status": "filled", "filled_qty": "0", "qty": "0"}

    def cancel_order(self, order_id):
        self.cancelled.append(order_id)
        return {"id": order_id, "status": "pending_cancel"}

    def submit_recovery_oco(self, order):
        self.recovery_orders.append(order)
        self.orders = [test_oco_order_from_payload(order)]
        return {"id": "recovery-oco-1", "status": "accepted", "client_order_id": order["client_order_id"]}

    def submit_catastrophic_stop(self, order):
        self.recovery_orders.append(order)
        acknowledged = dict(order)
        acknowledged.update({"id": "recovery-stop-1", "status": "accepted", "filled_qty": "0"})
        self.orders = [acknowledged]
        return {"id": "recovery-stop-1", "status": "accepted", "client_order_id": order["client_order_id"]}

    def replace_order(self, order_id, patch):
        self.cancelled.append(f"replace:{order_id}")
        for order in self.orders:
            if order.get("id") == order_id:
                order.update(patch)
        return {"id": order_id, "status": "accepted", **patch}

    def get_recent_fills(self, symbol):
        return self.fills


class FailingCatastrophicStopTrader(FakeProtectionTrader):
    def submit_catastrophic_stop(self, order):
        self.recovery_orders.append(order)
        raise RuntimeError("catastrophic stop rejected")


def test_exit_plan(symbol="AAPL"):
    return build_renko_tp_sl_exit_plan(
        trader_id=f"megatron_{symbol.lower()}",
        strategy_id=f"{symbol.lower()}_tp_sl",
        symbol=symbol,
        entry_side="buy",
        quantity=4,
        entry_reference_type="signal_price",
        entry_reference_price="100",
        brick_size="1",
        take_profit_bricks=2,
        stop_loss_bricks=1,
        time_in_force="day",
        order_class="bracket",
        client_order_id=f"{symbol.lower()}-entry",
        candidate_config_fingerprint=f"config-{symbol.lower()}",
        market_contract_fingerprint=f"market-{symbol.lower()}",
    ).to_dict()


def test_bracket_order(symbol="AAPL"):
    plan = test_exit_plan(symbol)
    return {
        "id": "parent-1",
        "symbol": symbol,
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
                "symbol": symbol,
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
                "symbol": symbol,
                "side": "sell",
                "qty": "4",
                "filled_qty": "0",
                "status": "accepted",
                "stop_price": plan["stop_loss_trigger_price"],
            },
        ],
    }


def test_oco_order_from_payload(order):
    return {
        "id": "recovery-oco-1",
        "symbol": order["symbol"],
        "side": order["side"],
        "qty": order["qty"],
        "filled_qty": "0",
        "status": "accepted",
        "order_class": "oco",
        "client_order_id": order["client_order_id"],
        "legs": [
            {
                "id": "recovery-tp-1",
                "parent_order_id": "recovery-oco-1",
                "symbol": order["symbol"],
                "side": order["side"],
                "qty": order["qty"],
                "filled_qty": "0",
                "status": "accepted",
                "limit_price": order["take_profit"]["limit_price"],
            },
            {
                "id": "recovery-sl-1",
                "parent_order_id": "recovery-oco-1",
                "symbol": order["symbol"],
                "side": order["side"],
                "qty": order["qty"],
                "filled_qty": "0",
                "status": "accepted",
                "stop_price": order["stop_loss"]["stop_price"],
            },
        ],
    }


def test_reversal_stop_plan(symbol="TSLA"):
    return build_reversal_catastrophic_stop_plan(
        trader_id=f"camatron_{symbol.lower()}",
        strategy_id=f"{symbol.lower()}_reversal",
        symbol=symbol,
        entry_side="sell",
        quantity="1",
        entry_reference_price="353.2751",
        brick_size="2.32525",
        catastrophic_stop_bricks=3,
        client_order_id=f"{symbol.lower()}-entry",
        candidate_config_fingerprint=f"config-{symbol.lower()}",
        market_contract_fingerprint=f"market-{symbol.lower()}",
    )


def test_reversal_stop_order(symbol="TSLA", *, order_id="stop-1", status="filled", filled_qty="1"):
    plan = test_reversal_stop_plan(symbol)
    order = catastrophic_stop_order_payload(plan)
    order.update(
        {
            "id": order_id,
            "client_order_id": plan["stop_client_order_id"],
            "status": status,
            "filled_qty": filled_qty,
            "filled_avg_price": "366.50" if status == "filled" else None,
            "filled_at": "2026-09-03T13:30:00.568Z" if status == "filled" else None,
        }
    )
    return plan, order


def live_config_trader(trader_id: str) -> dict:
    config = json.loads(Path("configs/traders.live.example.json").read_text(encoding="utf-8"))
    return next(trader for trader in config["traders"] if trader["id"] == trader_id)


class StrategyEngineOrderIntentTests(unittest.TestCase):
    def setUp(self):
        _BROKER_FORCE_FLAT_DATES.clear()

    def migration_runtime(
        self,
        *,
        symbol="AAPL",
        target_qty=8,
        new_brick=2.161965,
        position=None,
        latest_price=None,
        latest_error=None,
    ):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id=f"camatron_{symbol.lower()}_migration_test",
                symbol=symbol,
                strategy_type="renko_reversal",
                brick_size=new_brick,
                entry_bricks=1,
                qty=target_qty,
                renko_anchor_mode="grid",
                renko_anchor_price=0.0,
                allow_overnight=True,
            )
        )
        execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        execution.trader = FakeMigrationTrader(position=position, latest_price=latest_price, latest_error=latest_error)
        execution.log = None
        return TraderRuntime(
            trader_id=engine.config.trader_id,
            symbol=symbol,
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=3500,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=MemoryLog(),
            config_hash=f"new-{symbol.lower()}",
        )

    def incompatible_state_payload(self, *, symbol="AAPL", old_brick=1.3443, position_qty=1):
        return {
            "configuration_hash": f"old-{symbol.lower()}",
            "configured_quantity": position_qty,
            "engine": {
                "trader_id": f"camatron_{symbol.lower()}_migration_test",
                "symbol": symbol,
                "strategy_type": "renko_reversal",
                "strategy_name": f"{symbol.lower()}_test",
                "renko": {
                    "brick_size": old_brick,
                    "last_brick_close": 100,
                    "run_dir": 1,
                    "run_len": 1,
                    "processed_bricks": 1,
                    "anchor_mode": "grid",
                    "anchor_price": 0.0,
                },
                "trade": {
                    "position": {
                        "symbol": symbol,
                        "side": "long",
                        "entry_time": "2026-08-04T14:00:00Z",
                        "entry_price": 100,
                        "qty": position_qty,
                    },
                    "force_flat_dates": [],
                },
            },
        }

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

        adapter = CanonicalMarketObservationAdapter(run_id="unit_1s_source")
        self.assertEqual(adapter.observation_ticks(adapter.process_tick(first, trader_id=runtime.trader_id)), ())
        self.assertEqual(adapter.observation_ticks(adapter.process_tick(second, trader_id=runtime.trader_id)), ())
        emitted = adapter.observation_ticks(adapter.process_tick(third, trader_id=runtime.trader_id))

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

    def test_single_strategy_kernel_config_has_exact_enabled_topology_and_effective_runtime_values(self):
        camatron = json.loads(Path("configs/traders.live.example.json").read_text(encoding="utf-8"))
        megatron = json.loads(Path("configs/accounts/megatron.live.json").read_text(encoding="utf-8"))
        enabled_camatron = {trader["id"]: trader for trader in camatron["traders"] if trader.get("enabled")}
        enabled_megatron = {trader["id"]: trader for trader in megatron["traders"] if trader.get("enabled")}

        expected_camatron = {
            "camatron_tsla_1s_renko_0p42_e2_tp1_sl3_qty1_20260917_live": ("TSLA", "renko_tp_sl", 1.3755, 2, 1, 3),
        }
        self.assertEqual(set(enabled_camatron), set(expected_camatron))
        self.assertEqual(enabled_megatron, {})

        for trader_id, (symbol, strategy_type, brick_size, entry, tp, sl) in expected_camatron.items():
            trader = enabled_camatron[trader_id]
            config = build_strategy_config(trader)
            self.assertTrue(trader["execution_enabled"])
            self.assertEqual(trader["account_namespace"], "camatron")
            self.assertEqual(trader["data"]["market_data_source"], "shared_canonical_stream")
            self.assertEqual(trader["data"]["renko_source"], "1s_close")
            self.assertEqual(trader["sizing"]["qty"], 1)
            self.assertEqual(config.symbol, symbol)
            self.assertEqual(config.strategy_type, strategy_type)
            self.assertAlmostEqual(config.brick_size, brick_size)
            self.assertEqual(config.entry_bricks, entry)
            self.assertEqual(config.take_profit_bricks, tp)
            self.assertEqual(config.stop_loss_bricks, sl)
            self.assertEqual(config.qty, 1)
            if strategy_type == "renko_tp_sl":
                self.assertEqual(config.entry_start, "09:30:15")
                self.assertEqual(config.entry_end, "15:50:15")
                self.assertEqual(config.force_flat, "15:59:30")
                self.assertFalse(config.allow_overnight)

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

    def test_renko_reversal_allows_existing_position_reversal_before_entry_start(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_aapl_reversal_open_test",
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
        engine.position = PositionState(
            symbol="AAPL",
            side="short",
            entry_time=pd.Timestamp("2026-07-14T15:55:58Z"),
            entry_price=313.22,
            qty=1,
        )

        actions = engine._on_brick_reversal(
            {
                "timestamp": pd.Timestamp("2026-07-15T13:30:00Z"),
                "brick_close": 317.25,
                "direction": 1,
                "run_len": 1,
            }
        )

        self.assertEqual(len(actions), 1)
        signal, intent = actions[0]
        self.assertEqual(signal.reason, "long_entry_e1_reversal")
        self.assertEqual(signal.side, "long")
        self.assertEqual(intent.side, "buy")
        self.assertEqual(intent.qty, 2)
        self.assertEqual(intent.target_qty, 1)

    def test_renko_reversal_blocks_flat_entry_before_entry_start(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_aapl_reversal_open_test",
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

        actions = engine._on_brick_reversal(
            {
                "timestamp": pd.Timestamp("2026-07-15T13:30:00Z"),
                "brick_close": 317.25,
                "direction": 1,
                "run_len": 1,
            }
        )

        self.assertEqual(actions, [])

    def test_renko_tp_sl_exit_blocks_same_observation_reentry(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="tp_sl_pending_test",
                symbol="AAPL",
                strategy_type="renko_tp_sl",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=1,
                stop_loss_bricks=1,
                qty=1,
                renko_anchor_mode="grid",
                renko_anchor_price=0.0,
                allow_overnight=True,
            )
        )

        actions = []
        start = pd.Timestamp("2026-07-20T14:00:00Z")
        for offset, price in enumerate([100.0, 101.0]):
            _bricks, emitted = engine.on_tick(MarketTick(start + pd.Timedelta(seconds=offset), "AAPL", price, 1))
            actions.extend(emitted)
        self.assertEqual(actions[-1][0].signal_type, "entry")

        _bricks, emitted = engine.on_tick(MarketTick(start + pd.Timedelta(seconds=2), "AAPL", 98.0, 1))
        self.assertEqual(len(emitted), 1)
        self.assertEqual(emitted[0][0].signal_type, "exit")
        self.assertEqual(emitted[0][0].reason, "stop_loss_1_bricks")
        self.assertTrue(emitted[0][1].close_position)
        self.assertIsNotNone(engine.position)
        self.assertEqual(engine.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_EXIT_PENDING)
        self.assertEqual(engine.pending_exit["reason"], "stop_loss_1_bricks")

        _bricks, duplicate = engine.on_tick(MarketTick(start + pd.Timedelta(seconds=3), "AAPL", 97.0, 1))
        self.assertEqual(duplicate, [])

        _bricks, blocked_entry = engine.on_tick(MarketTick(start + pd.Timedelta(seconds=4), "AAPL", 101.0, 1))
        self.assertEqual(blocked_entry, [])

        engine.mark_close_submitted(emitted[0][1], response={"id": "broker-close-1"})
        self.assertEqual(engine.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_CLOSE_SUBMITTED)
        _bricks, still_blocked = engine.on_tick(MarketTick(start + pd.Timedelta(seconds=5), "AAPL", 101.0, 1))
        self.assertEqual(still_blocked, [])

        engine.mark_close_filled(timestamp=start + pd.Timedelta(seconds=6), reason="stop_loss_1_bricks", order_id="broker-close-1")
        self.assertIsNone(engine.position)
        self.assertEqual(engine.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_FLAT)

    def test_renko_tp_sl_close_intents_have_canonical_semantics(self):
        cases = [
            ("long", 101.0, "sell", "take_profit_1_bricks"),
            ("short", 99.0, "buy", "take_profit_1_bricks"),
            ("long", 99.0, "sell", "stop_loss_1_bricks"),
            ("short", 101.0, "buy", "stop_loss_1_bricks"),
        ]
        for side, brick_close, broker_side, reason in cases:
            with self.subTest(side=side, reason=reason):
                engine = RenkoStrategyEngine(
                    RenkoStrategyConfig(
                        trader_id=f"tp_sl_{side}_{reason}",
                        symbol="AAPL",
                        strategy_type="renko_tp_sl",
                        brick_size=1.0,
                        entry_bricks=1,
                        take_profit_bricks=1,
                        stop_loss_bricks=1,
                        qty=10,
                        renko_anchor_mode="grid",
                        renko_anchor_price=0.0,
                        allow_overnight=True,
                    )
                )
                engine.position = PositionState(
                    symbol="AAPL",
                    side=side,
                    entry_time=pd.Timestamp("2026-07-20T14:00:00Z"),
                    entry_price=100.0,
                    qty=10,
                )
                engine._mark_tp_sl_position_open()

                emitted = engine._on_brick_tp_sl(
                    {
                        "timestamp": pd.Timestamp("2026-07-20T14:00:01Z"),
                        "brick_close": brick_close,
                        "direction": 1 if brick_close >= 100 else -1,
                        "run_len": 1,
                    }
                )

                self.assertEqual(len(emitted), 1)
                signal, intent = emitted[0]
                self.assertEqual(signal.reason, reason)
                self.assertEqual(intent.reason, reason)
                self.assertTrue(intent.close_position)
                self.assertIsNone(intent.qty)
                self.assertEqual(intent.target_qty, 0)
                self.assertEqual(intent.side, broker_side)
                self.assertEqual(intent.signal_id, signal.signal_id)
                self.assertEqual(intent.lifecycle_state, TP_SL_LIFECYCLE_TRIGGERED)
                self.assertEqual(engine.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_EXIT_PENDING)

    def test_renko_tp_sl_same_side_entry_does_not_pyramid(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="tp_sl_no_pyramid",
                symbol="AAPL",
                strategy_type="renko_tp_sl",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=3,
                stop_loss_bricks=3,
                qty=10,
                renko_anchor_mode="grid",
                renko_anchor_price=0.0,
                allow_overnight=True,
            )
        )
        engine.position = PositionState(
            symbol="AAPL",
            side="long",
            entry_time=pd.Timestamp("2026-07-20T14:00:00Z"),
            entry_price=100.0,
            qty=10,
        )
        engine._mark_tp_sl_position_open()

        emitted = engine._on_brick_tp_sl(
            {
                "timestamp": pd.Timestamp("2026-07-20T14:00:01Z"),
                "brick_close": 101.0,
                "direction": 1,
                "run_len": 1,
            }
        )

        self.assertEqual(emitted, [])

    def test_renko_tp_sl_entry_blocks_same_observation_exit(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="tp_sl_entry_pending_test",
                symbol="AAPL",
                strategy_type="renko_tp_sl",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=1,
                stop_loss_bricks=1,
                qty=1,
                renko_anchor_mode="grid",
                renko_anchor_price=0.0,
                allow_overnight=True,
            )
        )

        engine.on_tick(MarketTick(pd.Timestamp("2026-07-20T14:00:00Z"), "AAPL", 100.0, 1))
        _bricks, emitted = engine.on_tick(MarketTick(pd.Timestamp("2026-07-20T14:00:01Z"), "AAPL", 103.0, 1))
        self.assertEqual(len(emitted), 1)
        self.assertEqual(emitted[0][0].signal_type, "entry")
        self.assertFalse(emitted[0][1].close_position)

    def test_brick_size_migration_rejects_old_renko_state_and_preserves_broker_position(self):
        broker_position = {
            "symbol": "AAPL",
            "side": "long",
            "qty": "1",
            "qty_available": "1",
            "avg_entry_price": "305.00",
            "current_price": "311.34",
        }
        runtime = self.migration_runtime(position=broker_position)
        payload = self.incompatible_state_payload()

        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state" / f"{runtime.trader_id}.json"
            evidence_path = migrate_renko_brick_change_state(
                Path(temp_dir),
                runtime,
                state_payload=payload,
                state_snapshot=payload["engine"],
                state_path=state_path,
                restore_error="Snapshot brick_size does not match strategy config.",
            )

            evidence = json.loads(Path(evidence_path).read_text(encoding="utf-8"))

        self.assertEqual(evidence["policy"], "RESET_RENKO_ON_BRICK_CHANGE_PRESERVE_POSITION_V1")
        self.assertEqual(evidence["previous_brick_size"], 1.3443)
        self.assertEqual(evidence["new_brick_size"], 2.161965)
        self.assertEqual(evidence["previous_configured_quantity"], 1)
        self.assertEqual(evidence["new_configured_quantity"], 8)
        self.assertTrue(Path(evidence["rejected_renko_state_identifier"]).name.startswith(runtime.trader_id))
        self.assertEqual(runtime.engine.renko_state.bricks, [])
        self.assertEqual(runtime.engine.processed_bricks, 0)
        self.assertEqual(runtime.engine.renko_state.run_dir, 0)
        self.assertEqual(runtime.engine.renko_state.run_len, 0)
        self.assertEqual(runtime.engine.trade_state_snapshot()["position"]["side"], "long")
        self.assertEqual(runtime.engine.trade_state_snapshot()["position"]["qty"], 1.0)
        self.assertEqual(runtime.execution.trader.orders, [])
        self.assertEqual(runtime.execution.trader.fills, [])

    def test_migration_requires_fresh_movement_after_bootstrap(self):
        broker_position = {
            "symbol": "AAPL",
            "side": "long",
            "qty": "1",
            "avg_entry_price": "305.00",
            "current_price": "100.00",
        }
        runtime = self.migration_runtime(position=broker_position, new_brick=2.0)
        payload = self.incompatible_state_payload(old_brick=1.0)

        with tempfile.TemporaryDirectory() as temp_dir:
            migrate_renko_brick_change_state(
                Path(temp_dir),
                runtime,
                state_payload=payload,
                state_snapshot=payload["engine"],
                state_path=Path(temp_dir) / "state.json",
                restore_error="Snapshot brick_size does not match strategy config.",
            )

        bricks, actions = runtime.engine.on_tick(MarketTick(pd.Timestamp("2026-08-05T14:00:01Z"), "AAPL", 101.0, 1))
        self.assertEqual(bricks, [])
        self.assertEqual(actions, [])

        bricks, actions = runtime.engine.on_tick(MarketTick(pd.Timestamp("2026-08-05T14:00:02Z"), "AAPL", 102.0, 1))
        self.assertEqual(len(bricks), 1)
        self.assertEqual(actions, [])
        self.assertEqual(runtime.engine.trade_state_snapshot()["position"]["qty"], 1.0)

    def test_missing_bootstrap_price_fails_without_order_or_resize(self):
        runtime = self.migration_runtime(position=None, latest_error=RuntimeError("no trade"))
        payload = self.incompatible_state_payload()

        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(RuntimeError, "no trade"):
                migrate_renko_brick_change_state(
                    Path(temp_dir),
                    runtime,
                    state_payload=payload,
                    state_snapshot=payload["engine"],
                    state_path=Path(temp_dir) / "state.json",
                    restore_error="Snapshot brick_size does not match strategy config.",
                )

        self.assertIsNone(runtime.engine.trade_state_snapshot()["position"])
        self.assertEqual(runtime.execution.trader.orders, [])
        self.assertEqual(runtime.execution.trader.fills, [])

    def test_same_side_e1_signal_while_positioned_does_not_pyramid(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_aapl_same_side_test",
                symbol="AAPL",
                strategy_type="renko_reversal",
                brick_size=2.161965,
                entry_bricks=1,
                qty=8,
                allow_overnight=True,
            )
        )
        engine.position = PositionState("AAPL", "long", pd.Timestamp("2026-08-05T14:00:00Z"), 311.34, 1)

        actions = engine._on_brick_reversal(
            {
                "timestamp": pd.Timestamp("2026-08-05T14:01:00Z"),
                "brick_close": 313.50,
                "direction": 1,
                "run_len": 1,
            }
        )

        self.assertEqual(actions, [])
        self.assertEqual(engine.trade_state_snapshot()["position"]["qty"], 1)

    def test_approved_first_reversal_quantities_and_resulting_positions(self):
        cases = [
            ("AAPL", 8, 2.161965, 9, 16),
            ("NVDA", 11, 0.8841015, 12, 22),
            ("TSLA", 7, 1.62183, 8, 14),
        ]
        for symbol, target_qty, brick, first_sell_qty, buy_back_qty in cases:
            with self.subTest(symbol=symbol):
                engine = RenkoStrategyEngine(
                    RenkoStrategyConfig(
                        trader_id=f"camatron_{symbol.lower()}_reversal_test",
                        symbol=symbol,
                        strategy_type="renko_reversal",
                        brick_size=brick,
                        entry_bricks=1,
                        qty=target_qty,
                        allow_overnight=True,
                    )
                )
                engine.position = PositionState(symbol, "long", pd.Timestamp("2026-08-05T14:00:00Z"), 100, 1)
                actions = engine._on_brick_reversal(
                    {
                        "timestamp": pd.Timestamp("2026-08-05T14:01:00Z"),
                        "brick_close": 100 - brick,
                        "direction": -1,
                        "run_len": 1,
                    }
                )
                self.assertEqual(len(actions), 1)
                signal, intent = actions[0]
                self.assertEqual(signal.side, "short")
                self.assertEqual(intent.side, "sell")
                self.assertEqual(intent.qty, first_sell_qty)
                self.assertEqual(intent.target_qty, target_qty)
                self.assertEqual(engine.trade_state_snapshot()["position"]["side"], "short")
                self.assertEqual(engine.trade_state_snapshot()["position"]["qty"], target_qty)

                actions = engine._on_brick_reversal(
                    {
                        "timestamp": pd.Timestamp("2026-08-05T14:02:00Z"),
                        "brick_close": 100,
                        "direction": 1,
                        "run_len": 1,
                    }
                )
                self.assertEqual(len(actions), 1)
                signal, intent = actions[0]
                self.assertEqual(signal.side, "long")
                self.assertEqual(intent.side, "buy")
                self.assertEqual(intent.qty, buy_back_qty)
                self.assertEqual(intent.target_qty, target_qty)
                self.assertEqual(engine.trade_state_snapshot()["position"]["side"], "long")
                self.assertEqual(engine.trade_state_snapshot()["position"]["qty"], target_qty)

    def test_old_risk_caps_reject_and_updated_caps_allow_approved_quantities(self):
        signal = Signal(
            signal_id="risk-aapl",
            signal_type="entry",
            timestamp=pd.Timestamp("2026-08-05T14:00:00Z"),
            symbol="AAPL",
            side="long",
            broker_side="buy",
            price=311.34,
            reason="long_entry_e1",
            strategy="risk",
            trader_id="risk",
        )
        target_intent = OrderIntent(
            client_order_id="risk-target",
            timestamp=signal.timestamp,
            symbol="AAPL",
            side="buy",
            signal_type="entry",
            order_type="market",
            strategy="risk",
            reason="long_entry_e1",
            qty=8,
            trader_id="risk",
        )
        reversal_intent = OrderIntent(
            client_order_id="risk-reversal",
            timestamp=signal.timestamp,
            symbol="AAPL",
            side="sell",
            signal_type="entry",
            order_type="market",
            strategy="risk",
            reason="short_entry_e1_reversal",
            qty=9,
            target_qty=8,
            trader_id="risk",
        )
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="risk",
                symbol="AAPL",
                strategy_type="renko_reversal",
                brick_size=2.161965,
                entry_bricks=1,
                qty=8,
            )
        )
        runtime_old = TraderRuntime(
            trader_id="risk",
            symbol="AAPL",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=False,
            max_position_notional=1000,
            max_daily_loss=0,
            engine=engine,
            execution=object(),
            log=MemoryLog(),
        )
        runtime_new = TraderRuntime(**{**runtime_old.__dict__, "max_position_notional": 3500})

        self.assertEqual(should_block_for_risk(runtime_old, signal, target_intent), "entry_notional_exceeds_limit_1000")
        self.assertEqual(should_block_for_risk(runtime_old, signal, reversal_intent), "entry_notional_exceeds_limit_1000")
        self.assertIsNone(should_block_for_risk(runtime_new, signal, target_intent))
        self.assertIsNone(should_block_for_risk(runtime_new, signal, reversal_intent))

    def test_short_entry_requires_symbol_shortable_metadata(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="megatron_shortable_guard",
                symbol="NVDA",
                strategy_type="renko_tp_sl",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=2,
                stop_loss_bricks=2,
                qty=1,
                account_namespace="megatron",
            )
        )
        execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        execution.trader = FakeRiskTrader(shorting_enabled=True, shortable=False)
        execution.log = None
        log = MemoryLog()
        runtime = TraderRuntime(
            trader_id="megatron_shortable_guard",
            symbol="NVDA",
            mode="live",
            credential_profile="megatron",
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=1000,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=log,
            account_namespace="megatron",
        )
        signal = Signal(
            signal_id="short-signal",
            signal_type="entry",
            timestamp=pd.Timestamp("2026-08-07T14:00:00Z"),
            symbol="NVDA",
            side="short",
            broker_side="sell",
            price=220.0,
            reason="short_entry_e1",
            strategy="unit",
            trader_id="megatron_shortable_guard",
        )
        intent = OrderIntent(
            client_order_id="short-order",
            timestamp=signal.timestamp,
            symbol="NVDA",
            side="sell",
            signal_type="entry",
            order_type="market",
            strategy="unit",
            reason="short_entry_e1",
            qty=1,
            trader_id="megatron_shortable_guard",
            account_namespace="megatron",
        )

        self.assertEqual(should_block_for_risk(runtime, signal, intent), "asset_not_shortable")
        self.assertEqual(log.events[-1][0], "short_entry_rejected")
        self.assertEqual(log.events[-1][1]["asset"]["shortable"], False)

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

    def test_bracket_strategy_rejects_fractional_qty_sizing(self):
        with self.assertRaisesRegex(ValueError, "does not support fractional qty"):
            build_strategy_config(
                {
                    "id": "megatron_fractional_bracket",
                    "symbol": "AAPL",
                    "strategy": {
                        "type": "renko_tp_sl",
                        "brick_size": 1.0,
                        "entry_bricks": 1,
                        "take_profit_bricks": 2,
                        "stop_loss_bricks": 2,
                        "exit_management": "bracket",
                    },
                    "sizing": {
                        "qty": 0.25,
                    },
                }
            )

    def test_tp_sl_config_inherits_intraday_family_session_contract(self):
        config = build_strategy_config(
            {
                "id": "stale_tp_sl_session",
                "symbol": "TSLA",
                "strategy": {
                    "type": "renko_tp_sl",
                    "brick_size": 1.0,
                    "entry_bricks": 1,
                    "take_profit_bricks": 3,
                    "stop_loss_bricks": 1,
                    "exit_management": "bracket",
                    "candidate_config_fingerprint": "config-tsla",
                    "market_contract_fingerprint": "market-tsla",
                },
                "session": {
                    "timezone": "America/Chicago",
                    "entry_start": "09:31:00",
                    "entry_end": "15:59:59",
                    "force_flat": None,
                    "allow_overnight": True,
                },
                "sizing": {
                    "qty": 1,
                },
            }
        )

        self.assertEqual(config.timezone, "America/New_York")
        self.assertEqual(config.entry_start, "09:30:15")
        self.assertEqual(config.entry_end, "15:50:15")
        self.assertEqual(config.force_flat, "15:59:30")
        self.assertFalse(config.allow_overnight)

    def test_tp_sl_config_without_session_inherits_intraday_family_session_contract(self):
        config = build_strategy_config(
            {
                "id": "missing_tp_sl_session",
                "symbol": "AAPL",
                "strategy": {
                    "type": "renko_tp_sl",
                    "brick_size": 1.0,
                    "entry_bricks": 1,
                    "take_profit_bricks": 3,
                    "stop_loss_bricks": 3,
                    "exit_management": "bracket",
                    "candidate_config_fingerprint": "config-aapl",
                    "market_contract_fingerprint": "market-aapl",
                },
                "sizing": {
                    "qty": 1,
                },
            }
        )

        self.assertEqual(config.timezone, "America/New_York")
        self.assertEqual(config.entry_start, "09:30:15")
        self.assertEqual(config.entry_end, "15:50:15")
        self.assertEqual(config.force_flat, "15:59:30")
        self.assertFalse(config.allow_overnight)

    def test_reversal_config_keeps_profile_session_contract(self):
        config = build_strategy_config(
            {
                "id": "overnight_reversal",
                "symbol": "AAPL",
                "strategy": {
                    "type": "renko_reversal",
                    "brick_size": 1.0,
                    "entry_bricks": 1,
                    "exit_management": "catastrophic_stop",
                    "catastrophic_stop_bricks": 3,
                    "candidate_config_fingerprint": "config-aapl",
                    "market_contract_fingerprint": "market-aapl",
                },
                "session": {
                    "timezone": "America/New_York",
                    "entry_start": "09:31:00",
                    "entry_end": "15:59:59",
                    "force_flat": None,
                    "allow_overnight": True,
                },
                "sizing": {
                    "qty": 1,
                },
            }
        )

        self.assertEqual(config.strategy_type, "renko_reversal")
        self.assertEqual(config.entry_start, "09:31:00")
        self.assertEqual(config.entry_end, "15:59:59")
        self.assertIsNone(config.force_flat)
        self.assertTrue(config.allow_overnight)

    def test_bracket_strategy_builds_signal_price_exit_plan(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="megatron_aapl",
                symbol="AAPL",
                brick_size=0.8511,
                entry_bricks=1,
                take_profit_bricks=3,
                stop_loss_bricks=3,
                qty=4,
                strategy_name="aapl_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-aapl",
                market_contract_fingerprint="market-aapl",
                renko_anchor_mode="grid",
                renko_anchor_price=0,
            )
        )

        signal = Signal(
            signal_id="signal-1",
            signal_type="entry",
            timestamp=pd.Timestamp("2026-08-21T14:54:15Z"),
            symbol="AAPL",
            side="long",
            broker_side="buy",
            price=308.9493,
            reason="renko_run_1_long",
            strategy="aapl_tp_sl",
            trader_id="megatron_aapl",
        )
        intent = engine._build_order_intent(signal, close_position=False)
        self.assertEqual(intent.exit_management, "bracket")
        self.assertEqual(intent.qty, 4)
        self.assertEqual(intent.take_profit_limit_price, 311.5026)
        self.assertEqual(intent.stop_loss_stop_price, 306.396)
        self.assertIsNotNone(intent.exit_plan)
        self.assertEqual(intent.exit_plan["entry_reference_type"], "signal_price")
        self.assertEqual(intent.exit_plan["take_profit_limit_price"], "311.50")
        self.assertEqual(intent.exit_plan["stop_loss_trigger_price"], "306.40")

    def test_intraday_tp_sl_entry_window_boundaries(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="intraday_tp_sl",
                symbol="TSLA",
                strategy_type="renko_tp_sl",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=3,
                stop_loss_bricks=1,
                qty=1,
                entry_start="09:30:15",
                entry_end="15:50:15",
                force_flat="15:59:30",
                timezone="America/New_York",
                allow_overnight=False,
            )
        )

        self.assertFalse(engine._entry_allowed(pd.Timestamp("2026-09-03T13:30:14Z")))
        self.assertTrue(engine._entry_allowed(pd.Timestamp("2026-09-03T13:30:15Z")))
        self.assertTrue(engine._entry_allowed(pd.Timestamp("2026-09-03T19:50:15Z")))
        self.assertFalse(engine._entry_allowed(pd.Timestamp("2026-09-03T19:50:16Z")))

    def test_intraday_tp_sl_position_after_entry_cutoff_remains_protected_until_force_flat(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="intraday_tp_sl_protected",
                symbol="TSLA",
                strategy_type="renko_tp_sl",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=3,
                stop_loss_bricks=1,
                qty=1,
                exit_management="bracket",
                candidate_config_fingerprint="config-tsla",
                market_contract_fingerprint="market-tsla",
                entry_start="09:30:15",
                entry_end="15:50:15",
                force_flat="15:59:30",
                timezone="America/New_York",
                allow_overnight=False,
            )
        )
        engine.position = PositionState(
            symbol="TSLA",
            side="short",
            entry_time=pd.Timestamp("2026-09-03T19:49:00Z"),
            entry_price=380.0,
            qty=1,
        )
        engine.tp_sl_lifecycle_state = TP_SL_LIFECYCLE_PROTECTED
        engine.protection_state = {"state": "PROTECTED", "stop_loss_order_id": "sl-1", "take_profit_order_id": "tp-1"}

        action = engine._force_flat_action(timestamp=pd.Timestamp("2026-09-03T19:50:16Z"), price=379.0)

        self.assertIsNone(action)
        self.assertEqual(engine.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_PROTECTED)
        self.assertEqual(engine.protection_state["stop_loss_order_id"], "sl-1")

    def test_reversal_catastrophic_stop_entry_intent_preserves_reversal_signal_logic(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                symbol="AAPL",
                brick_size=2.218981,
                entry_bricks=1,
                strategy_type="renko_reversal",
                qty=1,
                exit_management="catastrophic_stop",
                catastrophic_stop_bricks=3,
                trader_id="camatron_aapl",
                strategy_name="aapl_reversal",
                candidate_config_fingerprint="config-aapl",
                market_contract_fingerprint="market-aapl",
            )
        )

        actions = engine._on_brick_reversal(
            {
                "timestamp": pd.Timestamp("2026-08-24T13:31:00Z"),
                "brick_close": 300.0,
                "direction": 1,
                "run_len": 1,
            }
        )

        self.assertEqual(len(actions), 1)
        signal, intent = actions[0]
        self.assertEqual(signal.reason, "long_entry_e1")
        self.assertEqual(intent.exit_management, "catastrophic_stop")
        self.assertEqual(intent.catastrophic_stop_bricks, 3)
        self.assertEqual(intent.protection_type, "reversal_catastrophic_stop")
        self.assertEqual(intent.brick_size, 2.218981)
        self.assertEqual(engine.protection_state["state"], TP_SL_LIFECYCLE_ENTRY_PLANNED)

    def test_reversal_catastrophic_stop_records_exit_planned_before_reversal(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                symbol="TSLA",
                brick_size=2.32525,
                entry_bricks=1,
                strategy_type="renko_reversal",
                qty=1,
                exit_management="catastrophic_stop",
                catastrophic_stop_bricks=3,
                trader_id="camatron_tsla",
                strategy_name="tsla_reversal",
                candidate_config_fingerprint="config-tsla",
                market_contract_fingerprint="market-tsla",
            )
        )
        engine.position = PositionState(
            symbol="TSLA",
            side="long",
            entry_time=pd.Timestamp("2026-08-24T13:31:00Z"),
            entry_price=250.0,
            qty=1,
        )

        actions = engine._on_brick_reversal(
            {
                "timestamp": pd.Timestamp("2026-08-24T13:32:00Z"),
                "brick_close": 247.67475,
                "direction": -1,
                "run_len": 1,
            }
        )

        self.assertEqual(len(actions), 1)
        _, intent = actions[0]
        self.assertEqual(intent.reason, "short_entry_e1_reversal")
        self.assertEqual(intent.target_qty, 1)
        self.assertEqual(intent.qty, 2)
        self.assertEqual(engine.protection_state["state"], TP_SL_LIFECYCLE_ENTRY_PLANNED)
        self.assertEqual(engine.protection_state["reversal_exit_planned"]["state"], TP_SL_LIFECYCLE_EXIT_PENDING)

    def test_mark_reversal_catastrophic_stop_submitted_promotes_only_after_stop_verified(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                symbol="AAPL",
                brick_size=2.218981,
                entry_bricks=1,
                strategy_type="renko_reversal",
                qty=1,
                exit_management="catastrophic_stop",
                catastrophic_stop_bricks=3,
                trader_id="camatron_aapl",
                strategy_name="aapl_reversal",
                candidate_config_fingerprint="config-aapl",
                market_contract_fingerprint="market-aapl",
            )
        )
        _, intent = engine._on_brick_reversal(
            {
                "timestamp": pd.Timestamp("2026-08-24T13:31:00Z"),
                "brick_close": 300.0,
                "direction": 1,
                "run_len": 1,
            }
        )[0]
        stop_plan = build_reversal_catastrophic_stop_plan(
            trader_id="camatron_aapl",
            strategy_id="aapl_reversal",
            symbol="AAPL",
            entry_side="buy",
            quantity="1",
            entry_reference_price="300.00",
            brick_size="2.218981",
            catastrophic_stop_bricks=3,
            client_order_id=intent.client_order_id,
            candidate_config_fingerprint="config-aapl",
            market_contract_fingerprint="market-aapl",
        )
        stop_order = catastrophic_stop_order_payload(stop_plan)
        stop_order.update({"id": "stop-1", "status": "accepted", "filled_qty": "0"})

        engine.mark_reversal_catastrophic_stop_submitted(
            intent,
            response={
                "catastrophic_stop_response": {
                    "broker_position": {
                        "symbol": "AAPL",
                        "side": "long",
                        "qty": "1",
                        "avg_entry_price": "300.00",
                    },
                    "stop_plan": stop_plan,
                    "response": stop_order,
                }
            },
        )

        self.assertEqual(engine.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_PROTECTED)
        self.assertEqual(engine.protection_state["stop_order_id"], "stop-1")
        self.assertEqual(engine.protection_state["stop_price"], "293.34")

    def test_mark_entry_protection_submitted_promotes_to_protected_with_child_legs(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="megatron_tsla",
                symbol="TSLA",
                brick_size=1.5392,
                entry_bricks=1,
                take_profit_bricks=3,
                stop_loss_bricks=1,
                qty=4,
                strategy_name="tsla_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-tsla",
                market_contract_fingerprint="market-tsla",
            )
        )
        signal = Signal(
            signal_id="signal-1",
            signal_type="entry",
            timestamp=pd.Timestamp("2026-08-21T14:55:15Z"),
            symbol="TSLA",
            side="long",
            broker_side="buy",
            price=361.712,
            reason="renko_run_1_long",
            strategy="tsla_tp_sl",
            trader_id="megatron_tsla",
        )
        intent = engine._build_order_intent(signal, close_position=False)

        engine.mark_entry_protection_submitted(
            intent,
            response={
                "response": {
                    "id": "parent-1",
                    "symbol": "TSLA",
                    "side": "buy",
                    "qty": "4",
                    "filled_qty": "4",
                    "status": "filled",
                    "order_class": "bracket",
                    "client_order_id": intent.client_order_id,
                    "legs": [
                        {
                            "id": "tp-1",
                            "parent_order_id": "parent-1",
                            "client_order_id": intent.exit_plan["take_profit_client_order_id"],
                            "symbol": "TSLA",
                            "side": "sell",
                            "qty": "4",
                            "filled_qty": "0",
                            "status": "accepted",
                            "limit_price": intent.exit_plan["take_profit_limit_price"],
                        },
                        {
                            "id": "sl-1",
                            "parent_order_id": "parent-1",
                            "client_order_id": intent.exit_plan["stop_loss_client_order_id"],
                            "symbol": "TSLA",
                            "side": "sell",
                            "qty": "4",
                            "filled_qty": "0",
                            "status": "accepted",
                            "stop_price": intent.exit_plan["stop_loss_trigger_price"],
                        },
                    ],
                }
            },
        )

        self.assertEqual(engine.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_PROTECTED)
        self.assertEqual(engine.protection_state["parent_order_id"], "parent-1")
        self.assertEqual(engine.protection_state["take_profit_order_id"], "tp-1")
        self.assertEqual(engine.protection_state["stop_loss_order_id"], "sl-1")

    def test_mark_entry_protection_submitted_without_child_legs_stays_submitted(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="megatron_tsla",
                symbol="TSLA",
                brick_size=1.5392,
                entry_bricks=1,
                take_profit_bricks=3,
                stop_loss_bricks=1,
                qty=4,
                strategy_name="tsla_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-tsla",
                market_contract_fingerprint="market-tsla",
            )
        )
        signal = Signal(
            signal_id="signal-1",
            signal_type="entry",
            timestamp=pd.Timestamp("2026-08-21T14:55:15Z"),
            symbol="TSLA",
            side="long",
            broker_side="buy",
            price=361.712,
            reason="renko_run_1_long",
            strategy="tsla_tp_sl",
            trader_id="megatron_tsla",
        )
        intent = engine._build_order_intent(signal, close_position=False)

        engine.mark_entry_protection_submitted(intent, response={"response": {"id": "parent-1"}})

        self.assertEqual(engine.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_BRACKET_SUBMITTED)
        self.assertEqual(engine.protection_state["parent_order_id"], "parent-1")

    def test_mark_position_unprotected_preserves_bracket_reverification_lineage(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_nvda",
                symbol="NVDA",
                brick_size=0.5123,
                entry_bricks=1,
                take_profit_bricks=3,
                stop_loss_bricks=3,
                qty=1,
                strategy_name="nvda_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-nvda",
                market_contract_fingerprint="market-nvda",
            )
        )
        signal = Signal(
            signal_id="signal-1",
            signal_type="entry",
            timestamp=pd.Timestamp("2026-09-04T14:11:09Z"),
            symbol="NVDA",
            side="short",
            broker_side="sell",
            price=233.4703,
            reason="renko_run_1_short",
            strategy="nvda_tp_sl",
            trader_id="camatron_nvda",
        )
        intent = engine._build_order_intent(signal, close_position=False)
        engine.mark_entry_protection_submitted(
            intent,
            response={
                "response": {
                    "id": "parent-1",
                    "symbol": "NVDA",
                    "side": "sell",
                    "qty": "1",
                    "filled_qty": "1",
                    "status": "filled",
                    "order_class": "bracket",
                    "client_order_id": intent.client_order_id,
                    "legs": [
                        {
                            "id": "tp-1",
                            "parent_order_id": "parent-1",
                            "client_order_id": intent.exit_plan["take_profit_client_order_id"],
                            "symbol": "NVDA",
                            "side": "buy",
                            "qty": "1",
                            "filled_qty": "0",
                            "status": "accepted",
                            "limit_price": intent.exit_plan["take_profit_limit_price"],
                        },
                        {
                            "id": "sl-1",
                            "parent_order_id": "parent-1",
                            "client_order_id": intent.exit_plan["stop_loss_client_order_id"],
                            "symbol": "NVDA",
                            "side": "buy",
                            "qty": "1",
                            "filled_qty": "0",
                            "status": "accepted",
                            "stop_price": intent.exit_plan["stop_loss_trigger_price"],
                        },
                    ],
                }
            },
        )

        engine.mark_position_unprotected(
            "broker_native_protection_verification_failed",
            payload={
                "broker_position": {
                    "symbol": "NVDA",
                    "side": "short",
                    "qty": "1",
                    "avg_entry_price": "233.4703",
                },
                "issues": ["transient_missing_leg"],
            },
        )

        self.assertEqual(engine.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_ENTRY_FILLED_AWAITING_PROTECTION)
        self.assertEqual(engine.protection_state["state"], TP_SL_LIFECYCLE_ENTRY_FILLED_AWAITING_PROTECTION)
        self.assertEqual(engine.protection_state["reason"], "broker_native_protection_verification_failed")
        self.assertEqual(engine.protection_state["parent_order_id"], "parent-1")
        self.assertEqual(engine.protection_state["take_profit_order_id"], "tp-1")
        self.assertEqual(engine.protection_state["stop_loss_order_id"], "sl-1")
        self.assertEqual(engine.protection_state["exit_plan"], intent.exit_plan)

    def test_bracket_managed_position_does_not_emit_engine_close_order(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="megatron_aapl",
                symbol="AAPL",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=1,
                stop_loss_bricks=1,
                qty=4,
                strategy_name="aapl_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-aapl",
                market_contract_fingerprint="market-aapl",
            )
        )
        engine.position = PositionState(
            symbol="AAPL",
            side="long",
            entry_time=pd.Timestamp("2026-08-21T14:54:15Z"),
            entry_price=100.0,
            qty=4,
        )

        emitted = engine._on_brick_tp_sl(
            {
                "timestamp": pd.Timestamp("2026-08-21T14:55:15Z"),
                "brick_close": 101.0,
                "direction": 1,
                "run_len": 2,
            }
        )

        self.assertEqual(emitted, [])

    def test_handle_action_records_bracket_protection_state(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="megatron_aapl",
                symbol="AAPL",
                brick_size=0.8511,
                entry_bricks=1,
                take_profit_bricks=3,
                stop_loss_bricks=3,
                qty=4,
                strategy_name="aapl_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-aapl",
                market_contract_fingerprint="market-aapl",
            )
        )
        signal = Signal(
            signal_id="signal-1",
            signal_type="entry",
            timestamp=pd.Timestamp("2026-08-21T14:54:15Z"),
            symbol="AAPL",
            side="long",
            broker_side="buy",
            price=308.9493,
            reason="renko_run_1_long",
            strategy="aapl_tp_sl",
            trader_id="megatron_aapl",
        )
        intent = engine._build_order_intent(signal, close_position=False)
        execution = RecordingExecution()
        execution.event = ExecutionEvent(
            timestamp=pd.Timestamp("2026-08-21T14:54:16Z"),
            mode="real",
            broker="alpaca",
            status="order sent",
            intent=intent,
            response={
                "response": {
                    "id": "parent-1",
                    "symbol": "AAPL",
                    "side": "buy",
                    "qty": "4",
                    "filled_qty": "4",
                    "status": "filled",
                    "order_class": "bracket",
                    "client_order_id": intent.client_order_id,
                    "legs": [
                        {
                            "id": "tp-1",
                            "parent_order_id": "parent-1",
                            "client_order_id": intent.exit_plan["take_profit_client_order_id"],
                            "symbol": "AAPL",
                            "side": "sell",
                            "qty": "4",
                            "filled_qty": "0",
                            "status": "accepted",
                            "limit_price": intent.exit_plan["take_profit_limit_price"],
                        },
                        {
                            "id": "sl-1",
                            "parent_order_id": "parent-1",
                            "client_order_id": intent.exit_plan["stop_loss_client_order_id"],
                            "symbol": "AAPL",
                            "side": "sell",
                            "qty": "4",
                            "filled_qty": "0",
                            "status": "accepted",
                            "stop_price": intent.exit_plan["stop_loss_trigger_price"],
                        },
                    ],
                }
            },
        )
        runtime = TraderRuntime(
            trader_id="megatron_aapl",
            symbol="AAPL",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=MemoryLog(),
        )

        self.assertTrue(handle_action(runtime, signal, intent))

        self.assertEqual(engine.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_PROTECTED)
        self.assertEqual(engine.protection_state["parent_order_id"], "parent-1")
        self.assertEqual(runtime.log.events[-1][0], "broker_native_protection_submitted")

    def test_current_bracket_entry_plan_does_not_block_itself(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="megatron_aapl",
                symbol="AAPL",
                brick_size=0.8511,
                entry_bricks=1,
                take_profit_bricks=3,
                stop_loss_bricks=3,
                qty=1,
                strategy_name="aapl_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-aapl",
                market_contract_fingerprint="market-aapl",
                renko_anchor_mode="grid",
                renko_anchor_price=0.0,
            )
        )
        runtime = TraderRuntime(
            trader_id="megatron_aapl",
            symbol="AAPL",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=engine,
            execution=RecordingExecution(),
            log=MemoryLog(),
        )

        actions = engine._on_brick_tp_sl(
            {
                "timestamp": pd.Timestamp("2026-09-01T15:04:02Z"),
                "brick_close": 323.418,
                "direction": -1,
                "run_len": 1,
            }
        )

        self.assertEqual(len(actions), 1)
        signal, intent = actions[0]
        self.assertEqual(engine.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_ENTRY_PLANNED)
        self.assertIsNone(should_block_for_risk(runtime, signal, intent))
        self.assertTrue(handle_action(runtime, signal, intent))
        self.assertEqual(runtime.execution.intents, [intent])

    def test_stale_bracket_entry_plan_still_blocks_new_entry(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="megatron_aapl",
                symbol="AAPL",
                brick_size=0.8511,
                entry_bricks=1,
                take_profit_bricks=3,
                stop_loss_bricks=3,
                qty=1,
                strategy_name="aapl_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-aapl",
                market_contract_fingerprint="market-aapl",
            )
        )
        engine.tp_sl_lifecycle_state = TP_SL_LIFECYCLE_ENTRY_PLANNED
        engine.position = PositionState(
            symbol="AAPL",
            side="long",
            entry_time=pd.Timestamp("2026-09-01T15:04:02Z"),
            entry_price=323.418,
            qty=1,
        )
        signal = Signal(
            signal_id="signal-2",
            signal_type="entry",
            timestamp=pd.Timestamp("2026-09-01T15:20:17Z"),
            symbol="AAPL",
            side="long",
            broker_side="buy",
            price=325.1202,
            reason="renko_run_1_long",
            strategy="aapl_tp_sl",
            trader_id="megatron_aapl",
        )
        intent = engine._build_order_intent(signal, close_position=False)
        runtime = TraderRuntime(
            trader_id="megatron_aapl",
            symbol="AAPL",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=engine,
            execution=RecordingExecution(),
            log=MemoryLog(),
        )

        self.assertEqual(should_block_for_risk(runtime, signal, intent), "tp_sl_exit_pending")

    def test_unresolved_bracket_lifecycle_still_blocks_entry(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="megatron_tsla",
                symbol="TSLA",
                brick_size=1.5392,
                entry_bricks=1,
                take_profit_bricks=3,
                stop_loss_bricks=1,
                qty=1,
                strategy_name="tsla_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-tsla",
                market_contract_fingerprint="market-tsla",
            )
        )
        engine.tp_sl_lifecycle_state = TP_SL_LIFECYCLE_BRACKET_SUBMITTED
        engine.protection_state = {"state": TP_SL_LIFECYCLE_BRACKET_SUBMITTED, "parent_order_id": "parent-1"}
        signal = Signal(
            signal_id="signal-1",
            signal_type="entry",
            timestamp=pd.Timestamp("2026-09-02T13:31:53Z"),
            symbol="TSLA",
            side="short",
            broker_side="sell",
            price=355.5552,
            reason="renko_run_1_short",
            strategy="tsla_tp_sl",
            trader_id="megatron_tsla",
        )
        intent = engine._build_order_intent(signal, close_position=False)
        runtime = TraderRuntime(
            trader_id="megatron_tsla",
            symbol="TSLA",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=engine,
            execution=RecordingExecution(),
            log=MemoryLog(),
        )

        self.assertEqual(should_block_for_risk(runtime, signal, intent), "tp_sl_exit_pending")

    def test_stale_market_data_block_prevents_new_entry_order(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="megatron_aapl",
                symbol="AAPL",
                brick_size=0.8511,
                entry_bricks=1,
                take_profit_bricks=3,
                stop_loss_bricks=3,
                qty=1,
                strategy_name="aapl_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-aapl",
                market_contract_fingerprint="market-aapl",
            )
        )
        signal = Signal(
            signal_id="signal-stale",
            signal_type="entry",
            timestamp=pd.Timestamp("2026-08-27T14:00:00Z"),
            symbol="AAPL",
            side="long",
            broker_side="buy",
            price=308.9493,
            reason="renko_run_1_long",
            strategy="aapl_tp_sl",
            trader_id="megatron_aapl",
        )
        intent = engine._build_order_intent(signal, close_position=False)
        execution = RecordingExecution()
        runtime = TraderRuntime(
            trader_id="megatron_aapl",
            symbol="AAPL",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=MemoryLog(),
            market_data_signal_block_reason="shared_market_data_stale_6.000s",
        )

        self.assertEqual(should_block_for_risk(runtime, signal, intent), "shared_market_data_stale_6.000s")
        self.assertFalse(handle_action(runtime, signal, intent))
        self.assertEqual(execution.intents, [])
        self.assertEqual(runtime.log.events[-1][0], "risk_block")

    def test_replay_catchup_block_prevents_live_entry_order(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="megatron_aapl",
                symbol="AAPL",
                brick_size=0.8511,
                entry_bricks=1,
                take_profit_bricks=3,
                stop_loss_bricks=3,
                qty=1,
                strategy_name="aapl_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-aapl",
                market_contract_fingerprint="market-aapl",
            )
        )
        signal = Signal(
            signal_id="signal-replay",
            signal_type="entry",
            timestamp=pd.Timestamp("2026-08-27T14:00:00Z"),
            symbol="AAPL",
            side="long",
            broker_side="buy",
            price=308.9493,
            reason="renko_run_1_long",
            strategy="aapl_tp_sl",
            trader_id="megatron_aapl",
        )
        intent = engine._build_order_intent(signal, close_position=False)
        execution = RecordingExecution()
        runtime = TraderRuntime(
            trader_id="megatron_aapl",
            symbol="AAPL",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=MemoryLog(),
            market_data_signal_block_reason="shared_market_data_not_caught_up",
        )

        self.assertEqual(should_block_for_risk(runtime, signal, intent), "shared_market_data_not_caught_up")
        self.assertFalse(handle_action(runtime, signal, intent))
        self.assertEqual(execution.intents, [])

    def test_existing_broker_protection_state_survives_stale_feed_block(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="megatron_aapl",
                symbol="AAPL",
                brick_size=0.8511,
                entry_bricks=1,
                take_profit_bricks=3,
                stop_loss_bricks=3,
                qty=1,
                strategy_name="aapl_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-aapl",
                market_contract_fingerprint="market-aapl",
            )
        )
        engine.position = PositionState(
            symbol="AAPL",
            side="long",
            entry_time=pd.Timestamp("2026-08-27T14:00:00Z"),
            entry_price=308.9493,
            qty=1,
        )
        engine.protection_state = {
            "state": TP_SL_LIFECYCLE_PROTECTED,
            "parent_order_id": "parent-1",
            "take_profit_order_id": "tp-1",
            "stop_loss_order_id": "sl-1",
        }
        signal = Signal(
            signal_id="signal-stale-second-entry",
            signal_type="entry",
            timestamp=pd.Timestamp("2026-08-27T14:01:00Z"),
            symbol="AAPL",
            side="long",
            broker_side="buy",
            price=309.0,
            reason="renko_run_1_long",
            strategy="aapl_tp_sl",
            trader_id="megatron_aapl",
        )
        intent = engine._build_order_intent(signal, close_position=False)
        execution = RecordingExecution()
        runtime = TraderRuntime(
            trader_id="megatron_aapl",
            symbol="AAPL",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=MemoryLog(),
            market_data_signal_block_reason="shared_market_data_stale_6.000s",
        )

        self.assertFalse(handle_action(runtime, signal, intent))
        self.assertEqual(execution.intents, [])
        self.assertEqual(engine.protection_state["state"], TP_SL_LIFECYCLE_PROTECTED)
        self.assertEqual(engine.protection_state["take_profit_order_id"], "tp-1")
        self.assertEqual(engine.protection_state["stop_loss_order_id"], "sl-1")

    def test_external_manual_close_lifecycle_clears_tp_sl_state_without_unsafe(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="megatron_aapl",
                symbol="AAPL",
                brick_size=0.8511,
                entry_bricks=1,
                take_profit_bricks=3,
                stop_loss_bricks=3,
                qty=4,
                strategy_name="aapl_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-aapl",
                market_contract_fingerprint="market-aapl",
            )
        )
        engine.position = PositionState(
            symbol="AAPL",
            side="long",
            entry_time=pd.Timestamp("2026-08-21T14:54:15Z"),
            entry_price=308.9493,
            qty=4,
        )
        engine.protection_state = {"state": TP_SL_LIFECYCLE_PROTECTED, "parent_order_id": "parent-1"}

        engine.mark_external_close_filled(
            timestamp=pd.Timestamp("2026-08-21T15:07:41.936Z"),
            reason="manual_external_close",
            order_id="66a377bd-6f80-4157-8dfc-2f278d65a5d4",
            source="alpaca_manual_close_reconciliation",
            fill_price="309.7875",
            filled_qty="4",
            realized_pnl="3.47",
        )

        self.assertIsNone(engine.position)
        self.assertIsNone(engine.pending_exit)
        self.assertIsNone(engine.protection_state)
        self.assertEqual(engine.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_FLAT)
        self.assertFalse(engine.reconciliation_unsafe)
        self.assertEqual(engine.last_closed_exit["order_id"], "66a377bd-6f80-4157-8dfc-2f278d65a5d4")
        self.assertEqual(engine.last_closed_exit["realized_pnl"], "3.47")

    def test_reconcile_adopts_broker_position_with_valid_bracket_protection(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="megatron_aapl",
                symbol="AAPL",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=2,
                stop_loss_bricks=1,
                qty=4,
                strategy_name="aapl_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-aapl",
                market_contract_fingerprint="market-aapl",
            )
        )
        engine.protection_state = {"exit_plan": test_exit_plan("AAPL")}
        execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        execution.trader = FakeProtectionTrader(
            position={"symbol": "AAPL", "side": "long", "qty": "4", "avg_entry_price": "100"},
            orders=[test_bracket_order("AAPL")],
        )
        execution.log = None
        runtime = TraderRuntime(
            trader_id="megatron_aapl",
            symbol="AAPL",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=MemoryLog(),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            reconcile_engine_with_broker(Path(temp_dir), runtime)

        self.assertIsNotNone(engine.position)
        self.assertEqual(engine.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_PROTECTED)
        self.assertEqual(engine.protection_state["parent_order_id"], "parent-1")
        self.assertEqual(runtime.log.events[0][1]["reason"], "broker_position_valid_protection_adopted")

    def test_reconcile_clears_unsafe_when_broker_and_local_are_flat_with_no_owned_orders(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_tsla",
                symbol="TSLA",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=2,
                stop_loss_bricks=1,
                qty=1,
                strategy_name="tsla_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-tsla",
                market_contract_fingerprint="market-tsla",
            )
        )
        engine.mark_reconciliation_unsafe("previous_orphan_protection")
        execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        execution.trader = FakeProtectionTrader(position=None, orders=[])
        execution.log = None
        log = MemoryLog()
        runtime = TraderRuntime(
            trader_id="camatron_tsla",
            symbol="TSLA",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=log,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            reconcile_engine_with_broker(Path(temp_dir), runtime)

        self.assertFalse(engine.reconciliation_unsafe)
        self.assertIn("reconciliation_safety_transition", [event_type for event_type, _payload in log.events])
        transition = next(payload for event_type, payload in log.events if event_type == "reconciliation_safety_transition")
        self.assertEqual(transition["previous_value"], True)
        self.assertEqual(transition["new_value"], False)
        self.assertEqual(transition["reason"], "broker_local_flat_no_owned_orders")

    def test_reconcile_clears_unsafe_when_matching_open_position_has_valid_protection(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_tsla",
                symbol="TSLA",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=2,
                stop_loss_bricks=1,
                qty=4,
                strategy_name="tsla_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-tsla",
                market_contract_fingerprint="market-tsla",
            )
        )
        engine.position = PositionState(
            symbol="TSLA",
            side="long",
            entry_time=pd.Timestamp("2026-09-11T16:00:00Z"),
            entry_price=100.0,
            qty=4,
        )
        engine.protection_state = {"exit_plan": test_exit_plan("TSLA"), "parent_order_id": "parent-1"}
        engine.mark_reconciliation_unsafe("previous_transient_broker_error")
        execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        execution.trader = FakeProtectionTrader(
            position={"symbol": "TSLA", "side": "long", "qty": "4", "avg_entry_price": "100"},
            orders=[test_bracket_order("TSLA")],
        )
        execution.log = None
        log = MemoryLog()
        runtime = TraderRuntime(
            trader_id="camatron_tsla",
            symbol="TSLA",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=log,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            reconcile_engine_with_broker(Path(temp_dir), runtime)

        self.assertFalse(engine.reconciliation_unsafe)
        transition = next(payload for event_type, payload in log.events if event_type == "reconciliation_safety_transition")
        self.assertEqual(transition["previous_value"], True)
        self.assertEqual(transition["new_value"], False)
        self.assertEqual(transition["reason"], "broker_local_open_valid_protection")

    def test_reconcile_keeps_unsafe_for_orphan_owned_protection(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_tsla",
                symbol="TSLA",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=2,
                stop_loss_bricks=1,
                qty=4,
                strategy_name="tsla_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-tsla",
                market_contract_fingerprint="market-tsla",
            )
        )
        engine.protection_state = {"exit_plan": test_exit_plan("TSLA"), "parent_order_id": "parent-1"}
        engine.mark_reconciliation_unsafe("previous_orphan_protection")
        execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        execution.trader = FakeProtectionTrader(position=None, orders=[test_bracket_order("TSLA")])
        execution.log = None
        runtime = TraderRuntime(
            trader_id="camatron_tsla",
            symbol="TSLA",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=MemoryLog(),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(BrokerEngineStateMismatch):
                reconcile_engine_with_broker(Path(temp_dir), runtime)

        self.assertTrue(engine.reconciliation_unsafe)

    def test_reconcile_keeps_unsafe_for_local_flat_broker_open_even_with_valid_protection(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_tsla",
                symbol="TSLA",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=2,
                stop_loss_bricks=1,
                qty=4,
                strategy_name="tsla_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-tsla",
                market_contract_fingerprint="market-tsla",
            )
        )
        engine.protection_state = {"exit_plan": test_exit_plan("TSLA"), "parent_order_id": "parent-1"}
        engine.mark_reconciliation_unsafe("broker_position_exists_engine_flat")
        execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        execution.trader = FakeProtectionTrader(
            position={"symbol": "TSLA", "side": "long", "qty": "4", "avg_entry_price": "100"},
            orders=[test_bracket_order("TSLA")],
        )
        execution.log = None
        runtime = TraderRuntime(
            trader_id="camatron_tsla",
            symbol="TSLA",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=MemoryLog(),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            reconcile_engine_with_broker(Path(temp_dir), runtime)

        self.assertTrue(engine.reconciliation_unsafe)
        self.assertIsNotNone(engine.position)

    def test_reconcile_keeps_unsafe_for_local_open_broker_flat(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_tsla",
                symbol="TSLA",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=2,
                stop_loss_bricks=1,
                qty=4,
                strategy_name="tsla_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-tsla",
                market_contract_fingerprint="market-tsla",
            )
        )
        engine.position = PositionState(
            symbol="TSLA",
            side="long",
            entry_time=pd.Timestamp("2026-09-11T16:00:00Z"),
            entry_price=100.0,
            qty=4,
        )
        engine.mark_reconciliation_unsafe("broker_flat_engine_position_without_pending_close")
        execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        execution.trader = FakeProtectionTrader(position=None, orders=[], fills=[])
        execution.log = None
        runtime = TraderRuntime(
            trader_id="camatron_tsla",
            symbol="TSLA",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=MemoryLog(),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            reconcile_engine_with_broker(Path(temp_dir), runtime)

        self.assertTrue(engine.reconciliation_unsafe)
        self.assertIsNone(engine.position)

    def test_reconcile_keeps_unsafe_for_unexpected_symbol_open_order(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_tsla",
                symbol="TSLA",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=2,
                stop_loss_bricks=1,
                qty=1,
                strategy_name="tsla_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-tsla",
                market_contract_fingerprint="market-tsla",
            )
        )
        engine.mark_reconciliation_unsafe("unexpected_order")
        execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        execution.trader = FakeProtectionTrader(
            position=None,
            orders=[
                {
                    "id": "unexpected-1",
                    "symbol": "TSLA",
                    "side": "buy",
                    "type": "market",
                    "qty": "1",
                    "filled_qty": "0",
                    "status": "accepted",
                    "client_order_id": "unknown-tsla-order",
                }
            ],
        )
        execution.log = None
        runtime = TraderRuntime(
            trader_id="camatron_tsla",
            symbol="TSLA",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=MemoryLog(),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(BrokerEngineStateMismatch):
                reconcile_engine_with_broker(Path(temp_dir), runtime)

        self.assertTrue(engine.reconciliation_unsafe)

    def test_reconciliation_unsafe_persists_across_restart_until_authoritative_safe_reconcile(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_tsla",
                symbol="TSLA",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=2,
                stop_loss_bricks=1,
                qty=1,
                strategy_name="tsla_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-tsla",
                market_contract_fingerprint="market-tsla",
            )
        )
        engine.mark_reconciliation_unsafe("previous_orphan_protection")
        restored = RenkoStrategyEngine(engine.config)
        restored.restore_snapshot(engine.snapshot())
        self.assertTrue(restored.reconciliation_unsafe)

        execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        execution.trader = FakeProtectionTrader(position=None, orders=[])
        execution.log = None
        runtime = TraderRuntime(
            trader_id="camatron_tsla",
            symbol="TSLA",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=restored,
            execution=execution,
            log=MemoryLog(),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            reconcile_engine_with_broker(Path(temp_dir), runtime)

        safe_restored = RenkoStrategyEngine(restored.config)
        safe_restored.restore_snapshot(restored.snapshot())
        self.assertFalse(safe_restored.reconciliation_unsafe)

    def test_reconcile_verifies_nested_parent_protection_with_broker_generated_child_client_ids(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="megatron_tsla",
                symbol="TSLA",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=2,
                stop_loss_bricks=1,
                qty=4,
                strategy_name="tsla_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-tsla",
                market_contract_fingerprint="market-tsla",
            )
        )
        plan = test_exit_plan("TSLA")
        parent = test_bracket_order("TSLA")
        parent["legs"][0]["client_order_id"] = "broker-generated-tp-client-id"
        parent["legs"][0]["status"] = "held"
        parent["legs"][1]["client_order_id"] = "broker-generated-sl-client-id"
        parent["legs"][1]["status"] = "held"
        engine.protection_state = {
            "state": TP_SL_LIFECYCLE_ENTRY_FILLED_AWAITING_PROTECTION,
            "parent_order_id": "parent-1",
            "take_profit_order_id": "tp-1",
            "stop_loss_order_id": "sl-1",
            "exit_plan": plan,
        }
        execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        execution.trader = FakeProtectionTrader(
            position={"symbol": "TSLA", "side": "long", "qty": "4", "avg_entry_price": "100"},
            orders=[],
            parent=parent,
        )
        execution.log = None
        runtime = TraderRuntime(
            trader_id="megatron_tsla",
            symbol="TSLA",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=MemoryLog(),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            reconcile_engine_with_broker(Path(temp_dir), runtime)

        self.assertEqual(engine.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_PROTECTED)
        self.assertEqual(engine.protection_state["parent_order_id"], "parent-1")
        self.assertEqual(engine.protection_state["take_profit_order_id"], "tp-1")
        self.assertEqual(engine.protection_state["stop_loss_order_id"], "sl-1")
        self.assertEqual(runtime.log.events[0][1]["reason"], "broker_position_valid_protection_adopted")

    def test_repeated_tp_sl_reverify_preserves_exit_plan(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_aapl",
                symbol="AAPL",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=2,
                stop_loss_bricks=1,
                qty=4,
                strategy_name="aapl_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-aapl",
                market_contract_fingerprint="market-aapl",
            )
        )
        plan = test_exit_plan("AAPL")
        engine.position = PositionState(
            symbol="AAPL",
            side="long",
            entry_time=pd.Timestamp("2026-09-09T13:30:00Z"),
            entry_price=100.0,
            qty=4,
        )
        engine.protection_state = {"state": TP_SL_LIFECYCLE_PROTECTED, "parent_order_id": "parent-1", "exit_plan": plan}
        execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        execution.trader = FakeProtectionTrader(
            position={"symbol": "AAPL", "side": "long", "qty": "4", "avg_entry_price": "100"},
            orders=[test_bracket_order("AAPL")],
        )
        execution.log = None
        runtime = TraderRuntime(
            trader_id="camatron_aapl",
            symbol="AAPL",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=MemoryLog(),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            reconcile_engine_with_broker(Path(temp_dir), runtime)
            reconcile_engine_with_broker(Path(temp_dir), runtime)

        self.assertEqual(engine.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_PROTECTED)
        self.assertEqual(engine.protection_state["exit_plan"], plan)
        self.assertNotIn(
            "missing_exit_plan",
            {
                issue["code"]
                for _event_type, payload in runtime.log.events
                for issue in payload.get("issues", [])
                if isinstance(issue, dict)
            },
        )

    def test_apply_tp_sl_evidence_omitted_exit_plan_preserves_prior_plan(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_aapl",
                symbol="AAPL",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=2,
                stop_loss_bricks=1,
                qty=4,
                strategy_name="aapl_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-aapl",
                market_contract_fingerprint="market-aapl",
            )
        )
        plan = test_exit_plan("AAPL")
        engine.protection_state = {"state": TP_SL_LIFECYCLE_PROTECTED, "exit_plan": plan}
        verification = SimpleNamespace(
            protected=True,
            parent_order_id="parent-2",
            take_profit_order_id="tp-2",
            stop_loss_order_id="sl-2",
            protected_quantity="4",
            issues=(),
        )

        engine.apply_tp_sl_protection_evidence(verification=verification)

        self.assertEqual(engine.protection_state["exit_plan"], plan)

    def test_apply_tp_sl_evidence_empty_exit_plan_preserves_prior_plan(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_aapl",
                symbol="AAPL",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=2,
                stop_loss_bricks=1,
                qty=4,
                strategy_name="aapl_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-aapl",
                market_contract_fingerprint="market-aapl",
            )
        )
        plan = test_exit_plan("AAPL")
        engine.protection_state = {"state": TP_SL_LIFECYCLE_PROTECTED, "exit_plan": plan}
        verification = SimpleNamespace(
            protected=True,
            parent_order_id="parent-2",
            take_profit_order_id="tp-2",
            stop_loss_order_id="sl-2",
            protected_quantity="4",
            issues=(),
        )

        engine.apply_tp_sl_protection_evidence(exit_plan={}, verification=verification)

        self.assertEqual(engine.protection_state["exit_plan"], plan)

    def test_apply_tp_sl_evidence_none_exit_plan_preserves_prior_plan(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_aapl",
                symbol="AAPL",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=2,
                stop_loss_bricks=1,
                qty=4,
                strategy_name="aapl_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-aapl",
                market_contract_fingerprint="market-aapl",
            )
        )
        plan = test_exit_plan("AAPL")
        engine.protection_state = {"state": TP_SL_LIFECYCLE_PROTECTED, "exit_plan": plan}
        verification = SimpleNamespace(
            protected=True,
            parent_order_id="parent-2",
            take_profit_order_id="tp-2",
            stop_loss_order_id="sl-2",
            protected_quantity="4",
            issues=(),
        )

        engine.apply_tp_sl_protection_evidence(exit_plan=None, verification=verification)

        self.assertEqual(engine.protection_state["exit_plan"], plan)

    def test_apply_tp_sl_evidence_repeated_empty_plan_is_idempotent_for_exit_plan(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_aapl",
                symbol="AAPL",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=2,
                stop_loss_bricks=1,
                qty=4,
                strategy_name="aapl_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-aapl",
                market_contract_fingerprint="market-aapl",
            )
        )
        plan = test_exit_plan("AAPL")
        engine.protection_state = {"state": TP_SL_LIFECYCLE_PROTECTED, "exit_plan": plan}
        verification = SimpleNamespace(
            protected=True,
            parent_order_id="parent-2",
            take_profit_order_id="tp-2",
            stop_loss_order_id="sl-2",
            protected_quantity="4",
            issues=(),
        )

        for _ in range(10):
            engine.apply_tp_sl_protection_evidence(exit_plan={}, verification=verification)

        self.assertEqual(engine.protection_state["exit_plan"], plan)

    def test_apply_tp_sl_evidence_preserves_plan_while_updating_broker_evidence(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_aapl",
                symbol="AAPL",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=2,
                stop_loss_bricks=1,
                qty=4,
                strategy_name="aapl_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-aapl",
                market_contract_fingerprint="market-aapl",
            )
        )
        plan = test_exit_plan("AAPL")
        engine.protection_state = {
            "state": TP_SL_LIFECYCLE_PROTECTED,
            "exit_plan": plan,
            "parent_order_id": "parent-1",
            "take_profit_order_id": "tp-1",
            "stop_loss_order_id": "sl-1",
        }
        verification = SimpleNamespace(
            protected=True,
            parent_order_id="parent-2",
            take_profit_order_id="tp-2",
            stop_loss_order_id="sl-2",
            protected_quantity="4",
            issues=(),
        )

        engine.apply_tp_sl_protection_evidence(exit_plan={}, verification=verification)

        self.assertEqual(engine.protection_state["exit_plan"], plan)
        self.assertEqual(engine.protection_state["parent_order_id"], "parent-2")
        self.assertEqual(engine.protection_state["take_profit_order_id"], "tp-2")
        self.assertEqual(engine.protection_state["stop_loss_order_id"], "sl-2")

    def test_reconcile_reverifies_bracket_after_unprotected_marker_preserves_lineage(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_nvda",
                symbol="NVDA",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=3,
                stop_loss_bricks=3,
                qty=1,
                strategy_name="nvda_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-nvda",
                market_contract_fingerprint="market-nvda",
            )
        )
        plan = build_renko_tp_sl_exit_plan(
            trader_id="camatron_nvda",
            strategy_id="nvda_tp_sl",
            symbol="NVDA",
            entry_side="sell",
            quantity=1,
            entry_reference_type="signal_price",
            entry_reference_price="100",
            brick_size="1",
            take_profit_bricks=3,
            stop_loss_bricks=3,
            time_in_force="day",
            order_class="bracket",
            client_order_id="nvda-entry",
            candidate_config_fingerprint="config-nvda",
            market_contract_fingerprint="market-nvda",
        ).to_dict()
        parent = test_bracket_order("NVDA")
        parent["qty"] = "1"
        parent["filled_qty"] = "1"
        parent["side"] = "sell"
        parent["client_order_id"] = plan["client_order_id"]
        for leg in parent["legs"]:
            leg["qty"] = "1"
            leg["side"] = "buy"
            leg["status"] = "held"
        parent["legs"][0]["client_order_id"] = "broker-generated-tp-client-id"
        parent["legs"][0]["limit_price"] = plan["take_profit_limit_price"]
        parent["legs"][1]["client_order_id"] = "broker-generated-sl-client-id"
        parent["legs"][1]["stop_price"] = plan["stop_loss_trigger_price"]
        engine.protection_state = {
            "state": TP_SL_LIFECYCLE_BRACKET_SUBMITTED,
            "parent_order_id": "parent-1",
            "exit_plan": plan,
        }
        engine.mark_position_unprotected(
            "broker_native_protection_verification_failed",
            payload={
                "broker_position": {
                    "symbol": "NVDA",
                    "side": "short",
                    "qty": "1",
                    "avg_entry_price": "100",
                },
                "issues": ["transient_missing_leg"],
            },
        )
        execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        execution.trader = FakeProtectionTrader(
            position={"symbol": "NVDA", "side": "short", "qty": "1", "avg_entry_price": "100"},
            orders=[],
            parent=parent,
        )
        execution.log = None
        runtime = TraderRuntime(
            trader_id="camatron_nvda",
            symbol="NVDA",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=MemoryLog(),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            reconcile_engine_with_broker(Path(temp_dir), runtime)

        self.assertEqual(engine.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_PROTECTED)
        self.assertEqual(engine.protection_state["parent_order_id"], "parent-1")
        self.assertEqual(engine.protection_state["take_profit_order_id"], "tp-1")
        self.assertEqual(engine.protection_state["stop_loss_order_id"], "sl-1")
        self.assertEqual(runtime.log.events[0][0], "broker_native_protection_verified")

    def test_reconcile_broker_position_without_valid_protection_fails_closed(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="megatron_aapl",
                symbol="AAPL",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=2,
                stop_loss_bricks=1,
                qty=4,
                strategy_name="aapl_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-aapl",
                market_contract_fingerprint="market-aapl",
            )
        )
        engine.protection_state = {"exit_plan": test_exit_plan("AAPL")}
        execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        execution.trader = FakeProtectionTrader(
            position={"symbol": "AAPL", "side": "long", "qty": "4", "avg_entry_price": "100"},
            orders=[],
        )
        execution.log = None
        runtime = TraderRuntime(
            trader_id="megatron_aapl",
            symbol="AAPL",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=MemoryLog(),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(BrokerEngineStateMismatch):
                reconcile_engine_with_broker(Path(temp_dir), runtime)

        self.assertEqual(engine.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_ENTRY_PLANNED)
        self.assertIn("critical_protection_invariant_violation", [event_type for event_type, _ in runtime.log.events])

    def test_reconcile_broker_position_without_protection_submits_recovery_oco_when_parent_terminal(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="megatron_aapl",
                symbol="AAPL",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=2,
                stop_loss_bricks=1,
                qty=4,
                strategy_name="aapl_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-aapl",
                market_contract_fingerprint="market-aapl",
            )
        )
        plan = test_exit_plan("AAPL")
        engine.protection_state = {
            "state": "ENTRY_FILLED_AWAITING_PROTECTION",
            "parent_order_id": "parent-1",
            "lifecycle_id": plan["lifecycle_id"],
            "exit_plan": plan,
            "recovery_state": {
                "lifecycle_id": plan["lifecycle_id"],
                "state": "ENTRY_FILLED_AWAITING_PROTECTION",
                "parent_order_id": "parent-1",
                "recovery_attempt": 0,
                "deadline_epoch_seconds": "1",
                "audit": [],
            },
        }
        execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        trader = FakeProtectionTrader(
            position={
                "symbol": "AAPL",
                "side": "long",
                "qty": "4",
                "qty_available": "4",
                "avg_entry_price": "100",
                "current_price": "100",
            },
            orders=[],
            parent={
                "id": "parent-1",
                "symbol": "AAPL",
                "side": "buy",
                "qty": "4",
                "filled_qty": "4",
                "status": "filled",
                "order_class": "bracket",
                "client_order_id": plan["client_order_id"],
                "legs": [],
            },
        )
        execution.trader = trader
        execution.log = None
        runtime = TraderRuntime(
            trader_id="megatron_aapl",
            symbol="AAPL",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=MemoryLog(),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            reconcile_engine_with_broker(Path(temp_dir), runtime)

        self.assertEqual(engine.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_PROTECTED)
        self.assertEqual(len(trader.recovery_orders), 1)
        self.assertEqual(trader.recovery_orders[0]["order_class"], "oco")
        self.assertEqual(trader.recovery_orders[0]["qty"], "4")
        self.assertEqual(engine.protection_state["state"], TP_SL_LIFECYCLE_PROTECTED)
        self.assertEqual(engine.protection_state["recovery_attempt"], 1)
        self.assertIn("broker_native_protection_recovery", [event_type for event_type, _ in runtime.log.events])

    def test_reconcile_broker_flat_stale_position_uses_manual_close_fill(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="megatron_aapl",
                symbol="AAPL",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=2,
                stop_loss_bricks=1,
                qty=4,
                strategy_name="aapl_tp_sl",
            )
        )
        engine.position = PositionState(
            symbol="AAPL",
            side="long",
            entry_time=pd.Timestamp("2026-08-21T14:54:15Z"),
            entry_price=100.0,
            qty=4,
        )
        execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        execution.trader = FakeProtectionTrader(
            position=None,
            fills=[
                {
                    "id": "fill-1",
                    "order_id": "manual-close-1",
                    "symbol": "AAPL",
                    "side": "sell",
                    "qty": "4",
                    "price": "101.25",
                    "transaction_time": "2026-08-21T15:07:41.936Z",
                }
            ],
        )
        execution.log = None
        runtime = TraderRuntime(
            trader_id="megatron_aapl",
            symbol="AAPL",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=MemoryLog(),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            reconcile_engine_with_broker(Path(temp_dir), runtime)

        self.assertIsNone(engine.position)
        self.assertEqual(engine.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_FLAT)
        self.assertEqual(engine.last_closed_exit["order_id"], "manual-close-1")
        self.assertEqual(engine.last_closed_exit["realized_pnl"], "5.00")

    def test_reconcile_broker_flat_orphan_protection_fails_closed(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="megatron_aapl",
                symbol="AAPL",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=2,
                stop_loss_bricks=1,
                qty=4,
                strategy_name="aapl_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-aapl",
                market_contract_fingerprint="market-aapl",
            )
        )
        engine.protection_state = {"exit_plan": test_exit_plan("AAPL")}
        execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        execution.trader = FakeProtectionTrader(position=None, orders=[test_bracket_order("AAPL")])
        execution.log = None
        runtime = TraderRuntime(
            trader_id="megatron_aapl",
            symbol="AAPL",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=MemoryLog(),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(BrokerEngineStateMismatch):
                reconcile_engine_with_broker(Path(temp_dir), runtime)

        self.assertIn("critical_protection_invariant_violation", [event_type for event_type, _ in runtime.log.events])

    def test_reconcile_broker_flat_reversal_catastrophic_stop_fill_clears_state(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_tsla",
                symbol="TSLA",
                brick_size=2.32525,
                entry_bricks=1,
                strategy_type="renko_reversal",
                take_profit_bricks=1,
                stop_loss_bricks=1,
                qty=1,
                strategy_name="tsla_reversal",
                exit_management="catastrophic_stop",
                catastrophic_stop_bricks=3,
                candidate_config_fingerprint="config-tsla",
                market_contract_fingerprint="market-tsla",
            )
        )
        engine.position = PositionState(
            symbol="TSLA",
            side="short",
            entry_time=pd.Timestamp("2026-09-02T13:37:14Z"),
            entry_price=353.438,
            qty=1,
        )
        plan, order = test_reversal_stop_order("TSLA", order_id="stop-1", status="filled", filled_qty="1")
        engine.protection_state = {
            "state": TP_SL_LIFECYCLE_PROTECTED,
            "protection_type": "reversal_catastrophic_stop",
            "stop_order_id": "stop-1",
            "stop_client_order_id": plan["stop_client_order_id"],
            "stop_plan": plan,
        }
        engine.tp_sl_lifecycle_state = TP_SL_LIFECYCLE_PROTECTED
        execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        execution.trader = FakeProtectionTrader(
            position=None,
            orders=[order],
            fills=[
                {
                    "id": "fill-1",
                    "order_id": "stop-1",
                    "symbol": "TSLA",
                    "side": "buy",
                    "qty": "1",
                    "price": "366.50",
                    "transaction_time": "2026-09-03T13:30:00.568Z",
                }
            ],
        )
        execution.log = None
        runtime = TraderRuntime(
            trader_id="camatron_tsla",
            symbol="TSLA",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=MemoryLog(),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            reconcile_engine_with_broker(Path(temp_dir), runtime)
            reconcile_engine_with_broker(Path(temp_dir), runtime)

        self.assertIsNone(engine.position)
        self.assertIsNone(engine.pending_exit)
        self.assertIsNone(engine.protection_state)
        self.assertEqual(engine.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_FLAT)
        self.assertFalse(engine.reconciliation_unsafe)
        self.assertEqual(engine.last_closed_exit["reason"], "reversal_catastrophic_stop_broker_native")
        self.assertEqual(engine.last_closed_exit["order_id"], "stop-1")
        self.assertEqual(engine.last_closed_exit["source"], "alpaca_reconciliation_reversal_catastrophic_stop_fill")
        self.assertEqual(engine.last_closed_exit["fill_price"], "366.50")
        self.assertEqual(engine.last_closed_exit["filled_qty"], "1")
        self.assertEqual(engine.last_closed_exit["realized_pnl"], "-13.2249")
        self.assertEqual(runtime.log.events[0][0], "state_reconciled")
        self.assertEqual(runtime.log.events[0][1]["reason"], "broker_flat_reversal_catastrophic_stop_fill_reconciled")

    def test_stale_reversal_stop_visibility_after_terminal_fill_does_not_orphan(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_tsla",
                symbol="TSLA",
                brick_size=2.32525,
                entry_bricks=1,
                strategy_type="renko_reversal",
                qty=1,
                strategy_name="tsla_reversal",
                exit_management="catastrophic_stop",
                catastrophic_stop_bricks=3,
                candidate_config_fingerprint="config-tsla",
                market_contract_fingerprint="market-tsla",
            )
        )
        plan, active_order = test_reversal_stop_order("TSLA", order_id="stop-1", status="accepted", filled_qty="0")
        _, filled_order = test_reversal_stop_order("TSLA", order_id="stop-1", status="filled", filled_qty="1")
        engine.last_closed_exit = {
            "reason": "reversal_catastrophic_stop_broker_native",
            "order_id": "stop-1",
            "source": "alpaca_reconciliation_reversal_catastrophic_stop_fill",
        }
        execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        execution.trader = FakeProtectionTrader(position=None, orders=[active_order], parent=filled_order)
        execution.log = None
        runtime = TraderRuntime(
            trader_id="camatron_tsla",
            symbol="TSLA",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=MemoryLog(),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            reconcile_engine_with_broker(Path(temp_dir), runtime)

        self.assertIsNone(engine.position)
        self.assertNotIn("critical_protection_invariant_violation", [event_type for event_type, _ in runtime.log.events])

    def test_reversal_active_stop_while_flat_still_fails_closed(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_tsla",
                symbol="TSLA",
                brick_size=2.32525,
                entry_bricks=1,
                strategy_type="renko_reversal",
                qty=1,
                strategy_name="tsla_reversal",
                exit_management="catastrophic_stop",
                catastrophic_stop_bricks=3,
                candidate_config_fingerprint="config-tsla",
                market_contract_fingerprint="market-tsla",
            )
        )
        plan, active_order = test_reversal_stop_order("TSLA", order_id="stop-1", status="accepted", filled_qty="0")
        engine.protection_state = {"stop_plan": plan}
        execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        execution.trader = FakeProtectionTrader(position=None, orders=[active_order])
        execution.log = None
        runtime = TraderRuntime(
            trader_id="camatron_tsla",
            symbol="TSLA",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=MemoryLog(),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(BrokerEngineStateMismatch):
                reconcile_engine_with_broker(Path(temp_dir), runtime)

        self.assertIn("critical_protection_invariant_violation", [event_type for event_type, _ in runtime.log.events])

    def test_reconcile_after_protected_reversal_crash_windows_recovers_catastrophic_stop_from_broker_truth(self):
        cases = [
            (
                "after_protection_cancel_before_reversal_submission",
                "long",
                {
                    "state": TP_SL_LIFECYCLE_PROTECTED,
                    "stop_plan": build_reversal_catastrophic_stop_plan(
                        trader_id="megatron_aapl",
                        strategy_id="aapl_reversal",
                        symbol="AAPL",
                        entry_side="buy",
                        quantity="1",
                        entry_reference_price="300.00",
                        brick_size="2.218981",
                        catastrophic_stop_bricks=3,
                        client_order_id="aapl-entry",
                        candidate_config_fingerprint="config-aapl",
                        market_contract_fingerprint="market-aapl",
                    ),
                },
            ),
            (
                "after_reversal_submission_before_new_protection",
                "short",
                {"state": TP_SL_LIFECYCLE_ENTRY_PLANNED},
            ),
            (
                "after_reversal_fill_before_new_protection",
                "short",
                None,
            ),
        ]
        for name, side, protection_state in cases:
            with self.subTest(name=name):
                engine = RenkoStrategyEngine(
                    RenkoStrategyConfig(
                        trader_id="megatron_aapl",
                        symbol="AAPL",
                        brick_size=2.218981,
                        entry_bricks=1,
                        strategy_type="renko_reversal",
                        qty=1,
                        strategy_name="aapl_reversal",
                        exit_management="catastrophic_stop",
                        catastrophic_stop_bricks=3,
                        candidate_config_fingerprint="config-aapl",
                        market_contract_fingerprint="market-aapl",
                    )
                )
                engine.position = PositionState(
                    symbol="AAPL",
                    side=side,
                    entry_time=pd.Timestamp("2026-09-16T15:00:00Z"),
                    entry_price=300.0,
                    qty=1,
                )
                engine.protection_state = protection_state
                engine.tp_sl_lifecycle_state = (
                    TP_SL_LIFECYCLE_PROTECTED
                    if protection_state and protection_state.get("state") == TP_SL_LIFECYCLE_PROTECTED
                    else TP_SL_LIFECYCLE_ENTRY_FILLED_AWAITING_PROTECTION
                )
                execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
                broker_side = "long" if side == "long" else "short"
                broker_qty = "1" if broker_side == "long" else "-1"
                execution.trader = FakeProtectionTrader(
                    position={
                        "symbol": "AAPL",
                        "side": broker_side,
                        "qty": broker_qty,
                        "qty_available": broker_qty,
                        "avg_entry_price": "300.00",
                    },
                    orders=[],
                )
                execution.log = None
                runtime = TraderRuntime(
                    trader_id="megatron_aapl",
                    symbol="AAPL",
                    mode="live",
                    credential_profile=None,
                    feed="sip",
                    enabled=True,
                    execution_enabled=True,
                    max_position_notional=2500,
                    max_daily_loss=0,
                    engine=engine,
                    execution=execution,
                    log=MemoryLog(),
                )

                with tempfile.TemporaryDirectory() as temp_dir:
                    reconcile_engine_with_broker(Path(temp_dir), runtime)

                self.assertEqual(engine.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_PROTECTED)
                self.assertEqual(len(execution.trader.recovery_orders), 1)
                self.assertEqual(execution.trader.recovery_orders[0]["type"], "stop")
                self.assertEqual(execution.trader.recovery_orders[0]["side"], "sell" if side == "long" else "buy")
                self.assertEqual(runtime.log.events[-1][0], "reversal_catastrophic_stop_recovery")

    def test_reconcile_after_reversal_fill_fails_closed_when_new_catastrophic_stop_cannot_be_created(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="megatron_tsla",
                symbol="TSLA",
                brick_size=1.7685,
                entry_bricks=1,
                strategy_type="renko_reversal",
                qty=1,
                strategy_name="tsla_reversal",
                exit_management="catastrophic_stop",
                catastrophic_stop_bricks=3,
                candidate_config_fingerprint="config-tsla",
                market_contract_fingerprint="market-tsla",
            )
        )
        engine.position = PositionState(
            symbol="TSLA",
            side="short",
            entry_time=pd.Timestamp("2026-09-16T15:00:00Z"),
            entry_price=330.0,
            qty=1,
        )
        engine.tp_sl_lifecycle_state = TP_SL_LIFECYCLE_ENTRY_FILLED_AWAITING_PROTECTION
        execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        execution.trader = FailingCatastrophicStopTrader(
            position={
                "symbol": "TSLA",
                "side": "short",
                "qty": "-1",
                "qty_available": "-1",
                "avg_entry_price": "330.00",
            },
            orders=[],
        )
        execution.log = None
        runtime = TraderRuntime(
            trader_id="megatron_tsla",
            symbol="TSLA",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=MemoryLog(),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(BrokerEngineStateMismatch):
                reconcile_engine_with_broker(Path(temp_dir), runtime)

        self.assertEqual(len(execution.trader.recovery_orders), 1)
        self.assertEqual(engine.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_PROTECTION_FAILED)
        self.assertIn(
            "critical_reversal_catastrophic_stop_recovery_error",
            [event_type for event_type, _ in runtime.log.events],
        )

    def test_symbol_broker_mismatch_halts_only_that_runtime(self):
        aapl_engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_aapl_isolation",
                symbol="AAPL",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=2,
                stop_loss_bricks=1,
                qty=4,
                strategy_name="aapl_tp_sl",
                exit_management="bracket",
                candidate_config_fingerprint="config-aapl",
                market_contract_fingerprint="market-aapl",
            )
        )
        aapl_execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        aapl_execution.trader = FakeProtectionTrader(
            position={"symbol": "AAPL", "side": "long", "qty": "4", "avg_entry_price": "100"},
            orders=[],
        )
        aapl_execution.log = None
        aapl = TraderRuntime(
            trader_id="camatron_aapl_isolation",
            symbol="AAPL",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=aapl_engine,
            execution=aapl_execution,
            log=MemoryLog(),
        )
        nvda_engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_nvda_isolation",
                symbol="NVDA",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=2,
                stop_loss_bricks=1,
                qty=4,
            )
        )
        nvda = TraderRuntime(
            trader_id="camatron_nvda_isolation",
            symbol="NVDA",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=nvda_engine,
            execution=RecordingExecution(),
            log=MemoryLog(),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            reconcile_broker_state_if_due_or_halt(Path(temp_dir), aapl)

        self.assertFalse(aapl.enabled)
        self.assertTrue(aapl_engine.reconciliation_unsafe)
        self.assertTrue(nvda.enabled)

    def test_stream_cleanup_awaits_pending_read_before_close(self):
        closed = False

        async def scenario():
            started = asyncio.Event()

            async def stream():
                nonlocal closed
                try:
                    started.set()
                    await asyncio.sleep(10)
                    yield "event"
                finally:
                    closed = True

            iterator = stream().__aiter__()
            pending = asyncio.create_task(iterator.__anext__())
            await started.wait()
            await close_stream_after_pending_read(iterator, pending)

        asyncio.run(scenario())
        self.assertTrue(closed)

    def test_reconcile_broker_flat_reversal_stop_unknown_flatness_still_fails_closed(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_tsla",
                symbol="TSLA",
                brick_size=2.32525,
                entry_bricks=1,
                strategy_type="renko_reversal",
                take_profit_bricks=1,
                stop_loss_bricks=1,
                qty=1,
                strategy_name="tsla_reversal",
                exit_management="catastrophic_stop",
                catastrophic_stop_bricks=3,
                candidate_config_fingerprint="config-tsla",
                market_contract_fingerprint="market-tsla",
            )
        )
        engine.position = PositionState(
            symbol="TSLA",
            side="short",
            entry_time=pd.Timestamp("2026-09-02T13:37:14Z"),
            entry_price=353.438,
            qty=1,
        )
        execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        execution.trader = FakeProtectionTrader(position=None, orders=[], fills=[])
        execution.log = None
        runtime = TraderRuntime(
            trader_id="camatron_tsla",
            symbol="TSLA",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=MemoryLog(),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(BrokerEngineStateMismatch):
                reconcile_engine_with_broker(Path(temp_dir), runtime)

        self.assertIsNotNone(engine.position)
        self.assertEqual(runtime.log.events[0][1]["reason"], "broker_flat_engine_position_without_pending_close")

    def test_reconcile_broker_flat_reversal_stop_wrong_order_id_fails_closed(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_tsla",
                symbol="TSLA",
                brick_size=2.32525,
                entry_bricks=1,
                strategy_type="renko_reversal",
                take_profit_bricks=1,
                stop_loss_bricks=1,
                qty=1,
                strategy_name="tsla_reversal",
                exit_management="catastrophic_stop",
                catastrophic_stop_bricks=3,
                candidate_config_fingerprint="config-tsla",
                market_contract_fingerprint="market-tsla",
            )
        )
        engine.position = PositionState(
            symbol="TSLA",
            side="short",
            entry_time=pd.Timestamp("2026-09-02T13:37:14Z"),
            entry_price=353.438,
            qty=1,
        )
        plan, order = test_reversal_stop_order("TSLA", order_id="stop-1", status="filled", filled_qty="1")
        engine.protection_state = {
            "state": TP_SL_LIFECYCLE_PROTECTED,
            "protection_type": "reversal_catastrophic_stop",
            "stop_order_id": "different-stop",
            "stop_client_order_id": plan["stop_client_order_id"],
            "stop_plan": plan,
        }
        execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        execution.trader = FakeProtectionTrader(position=None, orders=[order], fills=[])
        execution.log = None
        runtime = TraderRuntime(
            trader_id="camatron_tsla",
            symbol="TSLA",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=2500,
            max_daily_loss=0,
            engine=engine,
            execution=execution,
            log=MemoryLog(),
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaises(BrokerEngineStateMismatch):
                reconcile_engine_with_broker(Path(temp_dir), runtime)

        self.assertIsNotNone(engine.position)

    def test_reconcile_broker_flat_reversal_stop_partial_or_canceled_fill_fails_closed(self):
        for status, filled_qty in [("filled", "0.5"), ("canceled", "0")]:
            with self.subTest(status=status, filled_qty=filled_qty):
                engine = RenkoStrategyEngine(
                    RenkoStrategyConfig(
                        trader_id="camatron_tsla",
                        symbol="TSLA",
                        brick_size=2.32525,
                        entry_bricks=1,
                        strategy_type="renko_reversal",
                        take_profit_bricks=1,
                        stop_loss_bricks=1,
                        qty=1,
                        strategy_name="tsla_reversal",
                        exit_management="catastrophic_stop",
                        catastrophic_stop_bricks=3,
                        candidate_config_fingerprint="config-tsla",
                        market_contract_fingerprint="market-tsla",
                    )
                )
                engine.position = PositionState(
                    symbol="TSLA",
                    side="short",
                    entry_time=pd.Timestamp("2026-09-02T13:37:14Z"),
                    entry_price=353.438,
                    qty=1,
                )
                plan, order = test_reversal_stop_order("TSLA", order_id="stop-1", status=status, filled_qty=filled_qty)
                engine.protection_state = {
                    "state": TP_SL_LIFECYCLE_PROTECTED,
                    "protection_type": "reversal_catastrophic_stop",
                    "stop_order_id": "stop-1",
                    "stop_client_order_id": plan["stop_client_order_id"],
                    "stop_plan": plan,
                }
                execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
                execution.trader = FakeProtectionTrader(position=None, orders=[order], fills=[])
                execution.log = None
                runtime = TraderRuntime(
                    trader_id="camatron_tsla",
                    symbol="TSLA",
                    mode="live",
                    credential_profile=None,
                    feed="sip",
                    enabled=True,
                    execution_enabled=True,
                    max_position_notional=2500,
                    max_daily_loss=0,
                    engine=engine,
                    execution=execution,
                    log=MemoryLog(),
                )

                with tempfile.TemporaryDirectory() as temp_dir:
                    with self.assertRaises(BrokerEngineStateMismatch):
                        reconcile_engine_with_broker(Path(temp_dir), runtime)

                self.assertIsNotNone(engine.position)

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

        self.assertEqual(list(grouped), [("alpaca", "", "live", "iex")])
        self.assertEqual(len(grouped[("alpaca", "", "live", "iex")]), 3)

    def test_shared_sip_grouping_uses_camatron_market_data_profile_once(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="shared_nvda",
                symbol="NVDA",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=1,
                stop_loss_bricks=1,
                qty=1,
            )
        )
        camatron = TraderRuntime(
            trader_id="camatron_nvda",
            symbol="NVDA",
            mode="live",
            credential_profile="camatron",
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=1000,
            max_daily_loss=0,
            engine=engine,
            execution=object(),
            log=object(),
            account_namespace="camatron",
            market_data_source="alpaca",
            market_data_profile="camatron",
        )
        megatron = TraderRuntime(
            trader_id="megatron_nvda",
            symbol="NVDA",
            mode="live",
            credential_profile="megatron",
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=1000,
            max_daily_loss=0,
            engine=engine,
            execution=object(),
            log=object(),
            account_namespace="megatron",
            market_data_source="alpaca",
            market_data_profile="camatron",
        )

        grouped = group_runtimes([camatron, megatron])

        self.assertEqual(list(grouped), [("alpaca", "camatron", "live", "sip")])
        self.assertEqual(len(grouped[("alpaca", "camatron", "live", "sip")]), 2)

    def test_camatron_config_is_single_tsla_tp_sl_kernel(self):
        config = json.loads(Path("configs/traders.live.example.json").read_text(encoding="utf-8"))
        validate_single_strategy_kernel_config(config, config_path="configs/traders.live.example.json")
        active = [
            trader
            for trader in config["traders"]
            if trader.get("enabled") and trader.get("execution_enabled")
        ]

        self.assertEqual(len(active), 1)
        trader = active[0]
        strategy = build_strategy_config(trader)
        self.assertEqual(trader["id"], "camatron_tsla_1s_renko_0p42_e2_tp1_sl3_qty1_20260917_live")
        self.assertEqual(trader["symbol"], "TSLA")
        self.assertEqual(trader["account_namespace"], "camatron")
        self.assertEqual(trader["data"]["market_data_source"], "shared_canonical_stream")
        self.assertEqual(trader["data"]["feed"], "sip")
        self.assertEqual(trader["data"]["renko_source"], "1s_close")
        self.assertEqual(strategy.strategy_type, "renko_tp_sl")
        self.assertEqual(strategy.qty, 1)
        self.assertEqual(strategy.exit_management, "bracket")
        self.assertEqual(strategy.take_profit_bricks, 1)
        self.assertEqual(strategy.stop_loss_bricks, 3)

    def test_single_strategy_kernel_validator_rejects_extra_active_renko_trader(self):
        config = json.loads(Path("configs/traders.live.example.json").read_text(encoding="utf-8"))
        for trader in config["traders"]:
            if trader["id"] == "camatron_aapl_1s_renko_0p10_e2_tp3_sl3_qty1_20260904_live":
                trader["enabled"] = True
                trader["execution_enabled"] = True

        with self.assertRaisesRegex(TraderConfigError, "exactly one active execution trader"):
            validate_single_strategy_kernel_config(config)

    def test_single_strategy_kernel_validator_rejects_reversal_or_megatron_authority(self):
        config = json.loads(Path("configs/traders.live.example.json").read_text(encoding="utf-8"))
        active = next(trader for trader in config["traders"] if trader.get("enabled") and trader.get("execution_enabled"))
        active["account_namespace"] = "megatron"
        active["credential_profile"] = "megatron"
        active["strategy"]["type"] = "renko_reversal"

        with self.assertRaisesRegex(TraderConfigError, "single-strategy kernel violation"):
            validate_single_strategy_kernel_config(config)

    def test_megatron_config_preserved_but_has_no_active_renko_execution(self):
        config = json.loads(Path("configs/accounts/megatron.live.json").read_text(encoding="utf-8"))
        active = [trader for trader in config["traders"] if trader.get("enabled") or trader.get("execution_enabled")]
        preserved_reversal = [
            trader
            for trader in config["traders"]
            if trader.get("strategy", {}).get("type") == "renko_reversal"
        ]

        self.assertEqual(active, [])
        self.assertEqual({trader["symbol"] for trader in preserved_reversal}, {"AAPL", "NVDA", "TSLA"})
        for trader in preserved_reversal:
            strategy = build_strategy_config(trader)
            self.assertFalse(trader["enabled"])
            self.assertFalse(trader["execution_enabled"])
            self.assertEqual(trader["credential_profile"], "megatron")
            self.assertEqual(trader["account_namespace"], "megatron")
            self.assertEqual(trader["expected_account_id"], "71d38ca0-82d3-44a8-b79f-d48f72211c6c")
            self.assertEqual(trader["data"]["market_data_source"], "shared_canonical_stream")
            self.assertIsNone(trader["data"]["market_data_profile"])
            self.assertEqual(trader["data"]["feed"], "sip")
            self.assertEqual(trader["data"]["renko_source"], "1s_close")
            self.assertEqual(strategy.strategy_type, "renko_reversal")
            self.assertEqual(strategy.qty, 1)
            self.assertEqual(strategy.entry_bricks, 1)
            self.assertEqual(strategy.take_profit_bricks, None)
            self.assertEqual(strategy.stop_loss_bricks, None)
            self.assertEqual(strategy.exit_management, "catastrophic_stop")
            self.assertEqual(strategy.catastrophic_stop_bricks, 3)
            self.assertTrue(strategy.allow_short)
            self.assertTrue(strategy.allow_overnight)

        service = Path("trading-fleet-megatron-live.service").read_text(encoding="utf-8")
        self.assertIn("--config /root/trading-server/configs/accounts/megatron.live.json", service)
        self.assertIn("--market-data-source shared_canonical_stream", service)
        self.assertIn("--shared-market-data-config /root/trading-server/configs/market-data.live.example.json", service)
        self.assertIn("--archive-root /root/trading-server/artifacts/live/megatron", service)
        self.assertNotIn("--market-archive-root /root/trading-server/artifacts/live", service)
        self.assertNotIn("--evidence-enabled", service)
        self.assertNotIn("--data-credential-profile", service)

    def test_market_archive_root_can_be_separate_from_runtime_archive_root(self):
        class Args:
            archive_root = "/root/trading-server/artifacts/live/megatron"
            market_archive_root = "/root/trading-server/artifacts/live"

        self.assertEqual(
            market_archive_root(Args()),
            Path("/root/trading-server/artifacts/live"),
        )

    def test_shared_canonical_duplicate_after_checkpoint_is_not_processed_again(self):
        class Args:
            log_ticks = False

        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="shared_aapl",
                symbol="AAPL",
                brick_size=1.0,
                entry_bricks=1,
                strategy_type="renko_reversal",
                qty=1,
                strategy_name="shared_aapl",
            )
        )
        runtime = TraderRuntime(
            trader_id="shared_aapl",
            symbol="AAPL",
            mode="live",
            credential_profile="camatron",
            feed="sip",
            enabled=True,
            execution_enabled=False,
            max_position_notional=1000,
            max_daily_loss=0,
            engine=engine,
            execution=RecordingExecution(),
            log=MemoryLog(),
            account_namespace="camatron",
            market_data_source="shared_canonical_stream",
            market_data_profile=None,
        )
        tick = MarketTick(
            timestamp=pd.Timestamp("2026-08-21T14:00:00Z"),
            symbol="AAPL",
            price=100.0,
            size=1,
            trade_id="canonical-event-1",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            process_market_tick_for_runtime(root=root, args=Args(), runtime=runtime, tick=tick)
            snapshot = json.loads((root / "state" / "shared_aapl.json").read_text(encoding="utf-8"))
            self.assertEqual(
                snapshot["canonical_market_checkpoints"]["processed_event_ids"]["AAPL"],
                "canonical-event-1",
            )
            process_market_tick_for_runtime(root=root, args=Args(), runtime=runtime, tick=tick)

        event_types = [event_type for event_type, _payload in runtime.log.events]
        self.assertIn("duplicate_shared_canonical_event_skipped", event_types)

    def test_alpaca_market_event_identity_is_stable_and_archived(self):
        message = {
            "T": "t",
            "t": "2026-08-07T13:30:00.001Z",
            "S": "NVDA",
            "p": 219.42,
            "s": 100,
            "x": "V",
            "i": "sip-trade-1",
            "z": "C",
            "c": ["@"],
        }
        first = live_market_event_from_alpaca_message(message, source_sequence=1)
        duplicate_after_reconnect = live_market_event_from_alpaca_message(message, source_sequence=2)

        self.assertEqual(first.source_record_id, duplicate_after_reconnect.source_record_id)
        self.assertEqual(first.source_sequence, 1)
        self.assertTrue(first.eligible)

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            archive_tick(root, first.tick, mode="live", feed="sip", source_event=first)
            archived = json.loads((root / "ticks" / "2026-08-07" / "NVDA.jsonl").read_text(encoding="utf-8"))

        self.assertEqual(archived["source_record_id"], first.source_record_id)
        self.assertEqual(archived["broker_trade_id"], "sip-trade-1")
        self.assertEqual(archived["source_sequence"], 1)
        self.assertTrue(archived["eligible"])

    def test_local_archive_tail_preserves_source_identity_and_dedupes(self):
        payload = {
            "timestamp": "2026-08-07T13:30:00.001Z",
            "symbol": "NVDA",
            "price": 219.42,
            "size": 100,
            "exchange": "V",
            "trade_id": "sip-trade-1",
            "tape": "C",
            "conditions": ["@"],
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "ticks" / "2026-08-07" / "NVDA.jsonl"
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps(payload) + "\n" + json.dumps(payload) + "\n", encoding="utf-8")
            tail = LocalArchiveMarketDataTail(root=temp_dir, symbols=["NVDA"], start_at_end=False)

            events = tail.poll_path(path)

        self.assertEqual(len(events), 1)
        self.assertTrue(events[0].eligible)
        self.assertEqual(events[0].broker_trade_id, "sip-trade-1")
        self.assertEqual(events[0].tick.symbol, "NVDA")

    def test_local_archive_tail_reads_new_file_created_after_startup(self):
        first = {
            "timestamp": "2026-08-07T13:30:00.001Z",
            "symbol": "TSLA",
            "price": 320.47,
            "size": 100,
            "trade_id": "sip-trade-1",
            "conditions": ["@"],
        }
        second = {
            **first,
            "timestamp": "2026-08-07T13:30:00.002Z",
            "price": 320.49,
            "trade_id": "sip-trade-2",
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "ticks" / "2026-08-07" / "TSLA.jsonl"
            tail = LocalArchiveMarketDataTail(root=temp_dir, symbols=["TSLA"], start_at_end=True)

            self.assertEqual(tail.poll_path(path), tuple())
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps(first) + "\n" + json.dumps(second) + "\n", encoding="utf-8")
            events_after_start = tail.poll_path(path)

            fresh_tail = LocalArchiveMarketDataTail(root=temp_dir, symbols=["TSLA"], start_at_end=True)
            events_before_start = fresh_tail.poll_path(path)

        self.assertEqual([event.broker_trade_id for event in events_after_start], ["sip-trade-1", "sip-trade-2"])
        self.assertEqual(events_before_start, tuple())

    def test_shared_trade_fanout_keeps_independent_renko_state(self):
        class Args:
            log_ticks = True
            execute_real = False

        camatron_engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_nvda",
                symbol="NVDA",
                strategy_type="renko_reversal",
                brick_size=1.0,
                entry_bricks=1,
                qty=1,
                allow_overnight=True,
            )
        )
        megatron_engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="megatron_nvda",
                symbol="NVDA",
                strategy_type="renko_tp_sl",
                brick_size=0.5,
                entry_bricks=1,
                take_profit_bricks=2,
                stop_loss_bricks=2,
                qty=1,
                allow_overnight=True,
            )
        )
        camatron_log = MemoryLog()
        megatron_log = MemoryLog()
        camatron = TraderRuntime(
            trader_id="camatron_nvda",
            symbol="NVDA",
            mode="live",
            credential_profile="camatron",
            feed="sip",
            enabled=True,
            execution_enabled=False,
            max_position_notional=1000,
            max_daily_loss=0,
            engine=camatron_engine,
            execution=RecordingExecution(),
            log=camatron_log,
            account_namespace="camatron",
            market_data_source="alpaca",
            market_data_profile="camatron",
            market_adapter=CanonicalMarketObservationAdapter(run_id="camatron_nvda"),
        )
        megatron = TraderRuntime(
            trader_id="megatron_nvda",
            symbol="NVDA",
            mode="live",
            credential_profile="megatron",
            feed="sip",
            enabled=True,
            execution_enabled=False,
            max_position_notional=1000,
            max_daily_loss=0,
            engine=megatron_engine,
            execution=RecordingExecution(),
            log=megatron_log,
            account_namespace="megatron",
            market_data_source="local_archive_tail",
            market_data_profile=None,
            market_adapter=CanonicalMarketObservationAdapter(run_id="megatron_nvda"),
        )
        first = live_market_event_from_archive_payload(
            {
                "timestamp": "2026-08-07T13:30:00.001Z",
                "symbol": "NVDA",
                "price": 100.0,
                "size": 100,
                "exchange": "V",
                "trade_id": "sip-trade-1",
                "tape": "C",
                "conditions": ["@"],
            },
            source_sequence=1,
        )
        second = live_market_event_from_archive_payload(
            {
                "timestamp": "2026-08-07T13:30:01.001Z",
                "symbol": "NVDA",
                "price": 101.0,
                "size": 100,
                "exchange": "V",
                "trade_id": "sip-trade-2",
                "tape": "C",
                "conditions": ["@"],
            },
            source_sequence=2,
        )
        third = live_market_event_from_archive_payload(
            {
                "timestamp": "2026-08-07T13:30:02.001Z",
                "symbol": "NVDA",
                "price": 102.0,
                "size": 100,
                "exchange": "V",
                "trade_id": "sip-trade-3",
                "tape": "C",
                "conditions": ["@"],
            },
            source_sequence=3,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            for event in (first, second, third):
                for runtime in (camatron, megatron):
                    process_market_tick_for_runtime(
                        root=Path(temp_dir),
                        args=Args(),
                        runtime=runtime,
                        tick=event.tick,
                        source_event=event,
                    )

        camatron_market = [payload for event_type, payload in camatron_log.events if event_type == "market_tick"]
        megatron_market = [payload for event_type, payload in megatron_log.events if event_type == "market_tick"]
        self.assertEqual(camatron_market[-1]["source_record_id"], third.source_record_id)
        self.assertEqual(megatron_market[-1]["source_record_id"], third.source_record_id)
        self.assertEqual(len(camatron_engine.renko_state.bricks), 1)
        self.assertEqual(len(megatron_engine.renko_state.bricks), 2)
        self.assertIsNot(camatron_engine.renko_state, megatron_engine.renko_state)

    def test_megatron_strategy_failure_does_not_stop_camatron_runtime(self):
        class Args:
            log_ticks = False
            execute_real = False

        class FailingEngine:
            def __init__(self):
                self.config = RenkoStrategyConfig(
                    trader_id="megatron_nvda",
                    symbol="NVDA",
                    strategy_type="renko_tp_sl",
                    brick_size=1.0,
                    entry_bricks=1,
                    take_profit_bricks=1,
                    stop_loss_bricks=1,
                    qty=1,
                )

            def snapshot(self):
                return {"renko": {}, "trade": self.trade_state_snapshot()}

            def trade_state_snapshot(self):
                return {"position": None}

            def on_tick(self, tick):
                raise RuntimeError("strategy failed")

        camatron_engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_nvda",
                symbol="NVDA",
                strategy_type="renko_reversal",
                brick_size=1.0,
                entry_bricks=1,
                qty=1,
                allow_overnight=True,
            )
        )
        camatron = TraderRuntime(
            trader_id="camatron_nvda",
            symbol="NVDA",
            mode="live",
            credential_profile="camatron",
            feed="sip",
            enabled=True,
            execution_enabled=False,
            max_position_notional=1000,
            max_daily_loss=0,
            engine=camatron_engine,
            execution=RecordingExecution(),
            log=MemoryLog(),
            account_namespace="camatron",
            market_data_source="alpaca",
            market_data_profile="camatron",
            market_adapter=CanonicalMarketObservationAdapter(run_id="camatron_nvda"),
        )
        megatron = TraderRuntime(
            trader_id="megatron_nvda",
            symbol="NVDA",
            mode="live",
            credential_profile="megatron",
            feed="sip",
            enabled=True,
            execution_enabled=False,
            max_position_notional=1000,
            max_daily_loss=0,
            engine=FailingEngine(),
            execution=RecordingExecution(),
            log=MemoryLog(),
            account_namespace="megatron",
            market_data_source="local_archive_tail",
            market_data_profile=None,
            market_adapter=CanonicalMarketObservationAdapter(run_id="megatron_nvda"),
        )
        first_tick = MarketTick(pd.Timestamp("2026-08-07T13:30:00.001Z"), "NVDA", 100.0, 100, trade_id="sip-trade-1")
        second_tick = MarketTick(pd.Timestamp("2026-08-07T13:30:01.001Z"), "NVDA", 101.0, 100, trade_id="sip-trade-2")

        with tempfile.TemporaryDirectory() as temp_dir:
            for tick in (first_tick, second_tick):
                process_market_tick_for_runtime(root=Path(temp_dir), args=Args(), runtime=megatron, tick=tick)
                process_market_tick_for_runtime(root=Path(temp_dir), args=Args(), runtime=camatron, tick=tick)

        self.assertFalse(megatron.enabled)
        self.assertTrue(camatron.enabled)
        self.assertIn("critical_runtime_halt", [event_type for event_type, _payload in megatron.log.events])

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

    def test_broker_force_flat_runs_for_locked_tp_sl_lifecycle_states(self):
        for lifecycle_state in [
            TP_SL_LIFECYCLE_PROTECTED,
            TP_SL_LIFECYCLE_ENTRY_FILLED_AWAITING_PROTECTION,
            TP_SL_LIFECYCLE_PROTECTION_FAILED,
        ]:
            with self.subTest(lifecycle_state=lifecycle_state):
                _BROKER_FORCE_FLAT_DATES.clear()
                engine = RenkoStrategyEngine(
                    RenkoStrategyConfig(
                        trader_id=f"tp_sl_force_flat_{lifecycle_state.lower()}",
                        symbol="TSLA",
                        brick_size=1.0,
                        entry_bricks=1,
                        take_profit_bricks=1,
                        stop_loss_bricks=1,
                        qty=1,
                        exit_management="bracket",
                        candidate_config_fingerprint="config-tsla",
                        market_contract_fingerprint="market-tsla",
                        force_flat="15:59:30",
                        timezone="America/New_York",
                        allow_overnight=False,
                    )
                )
                engine.position = PositionState(
                    symbol="TSLA",
                    side="short",
                    entry_time=pd.Timestamp("2026-06-08T19:26:16Z"),
                    entry_price=380.175,
                    qty=1,
                )
                engine.tp_sl_lifecycle_state = lifecycle_state
                engine.protection_state = {"state": lifecycle_state, "stop_loss_order_id": "sl-1"}
                execution = RecordingExecution()
                log = MemoryLog()
                runtime = TraderRuntime(
                    trader_id=engine.config.trader_id,
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
                        price=377.3,
                        source="test",
                    )
                    did_repeat = broker_force_flat_if_due(
                        Path(temp_dir),
                        runtime,
                        pd.Timestamp("2026-06-08T19:59:31Z"),
                        price=377.2,
                        source="test",
                    )

                self.assertTrue(did_force_flat)
                self.assertFalse(did_repeat)
                self.assertEqual(len(execution.intents), 1)
                intent = execution.intents[0]
                self.assertTrue(intent.close_position)
                self.assertEqual(intent.side, "buy")
                self.assertEqual(intent.reason, "session_force_flat")
                self.assertEqual(engine.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_CLOSE_SUBMITTED)
                self.assertIn("broker_force_flat", [event_type for event_type, _payload in log.events])

    def test_handle_action_reject_status_is_not_accepted(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="camatron_tsla_test",
                symbol="TSLA",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=1,
                stop_loss_bricks=1,
                qty=1,
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
        signal = Signal(
            signal_id="unit-signal",
            signal_type="entry",
            timestamp=pd.Timestamp("2026-07-01T14:00:00Z"),
            symbol="TSLA",
            side="long",
            broker_side="buy",
            price=100.0,
            reason="unit",
            strategy="unit",
            trader_id="camatron_tsla_test",
        )
        intent = OrderIntent(
            client_order_id="unit-order",
            timestamp=signal.timestamp,
            symbol="TSLA",
            side="buy",
            signal_type="entry",
            order_type="market",
            strategy="unit",
            reason="unit",
            qty=1,
            trader_id="camatron_tsla_test",
        )
        execution.event = ExecutionEvent(
            timestamp=signal.timestamp,
            mode="real",
            broker="alpaca",
            status="rejected",
            intent=intent,
            response={"status": "rejected"},
        )

        self.assertFalse(handle_action(runtime, signal, intent))
        self.assertEqual(len(execution.intents), 1)
        self.assertEqual(log.events[-1][0], "execution_error")

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

    def test_handle_action_marks_tp_sl_close_submitted(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="tp_sl_submit_test",
                symbol="TSLA",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=1,
                stop_loss_bricks=1,
                qty=10,
            )
        )
        engine.position = PositionState(
            symbol="TSLA",
            side="long",
            entry_time=pd.Timestamp("2026-07-01T14:00:00Z"),
            entry_price=100.0,
            qty=10,
        )
        signal = Signal(
            signal_id="tp-signal",
            signal_type="exit",
            timestamp=pd.Timestamp("2026-07-01T14:00:01Z"),
            symbol="TSLA",
            side="long",
            broker_side="sell",
            price=101.0,
            reason="take_profit_1_bricks",
            strategy="unit",
            trader_id="tp_sl_submit_test",
        )
        intent = OrderIntent(
            client_order_id="tp-order",
            timestamp=signal.timestamp,
            symbol="TSLA",
            side="sell",
            signal_type="exit",
            order_type="market",
            strategy="unit",
            reason="take_profit_1_bricks",
            target_qty=0,
            close_position=True,
            trader_id="tp_sl_submit_test",
        )
        engine._mark_tp_sl_exit_pending(signal, intent)
        execution = RecordingExecution()
        execution.event = ExecutionEvent(
            timestamp=signal.timestamp,
            mode="real",
            broker="alpaca",
            status="order sent",
            intent=intent,
            response={"response": {"id": "alpaca-close-1"}},
        )
        runtime = TraderRuntime(
            trader_id="tp_sl_submit_test",
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
            log=MemoryLog(),
        )

        self.assertTrue(handle_action(runtime, signal, intent))

        self.assertEqual(engine.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_CLOSE_SUBMITTED)
        self.assertEqual(engine.pending_exit["broker_order_id"], "alpaca-close-1")

    def test_reconcile_marks_tp_sl_flat_after_broker_flat_confirmation(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="tp_sl_fill_test",
                symbol="TSLA",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=1,
                stop_loss_bricks=1,
                qty=10,
            )
        )
        engine.position = PositionState(
            symbol="TSLA",
            side="long",
            entry_time=pd.Timestamp("2026-07-01T14:00:00Z"),
            entry_price=100.0,
            qty=10,
        )
        signal = Signal(
            signal_id="tp-signal",
            signal_type="exit",
            timestamp=pd.Timestamp("2026-07-01T14:00:01Z"),
            symbol="TSLA",
            side="long",
            broker_side="sell",
            price=101.0,
            reason="take_profit_1_bricks",
            strategy="unit",
            trader_id="tp_sl_fill_test",
        )
        intent = OrderIntent(
            client_order_id="tp-order",
            timestamp=signal.timestamp,
            symbol="TSLA",
            side="sell",
            signal_type="exit",
            order_type="market",
            strategy="unit",
            reason="take_profit_1_bricks",
            target_qty=0,
            close_position=True,
            trader_id="tp_sl_fill_test",
        )
        engine._mark_tp_sl_exit_pending(signal, intent)
        engine.mark_close_submitted(intent, response={"id": "alpaca-close-1"})
        execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        execution.trader = FakeMigrationTrader(position=None)
        execution.log = None
        log = MemoryLog()
        runtime = TraderRuntime(
            trader_id="tp_sl_fill_test",
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
            reconcile_engine_with_broker(Path(temp_dir), runtime)

        self.assertIsNone(engine.position)
        self.assertEqual(engine.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_FLAT)
        self.assertEqual(engine.last_closed_exit["reason"], "take_profit_1_bricks")
        self.assertEqual(log.events[0][0], "state_reconciled")
        self.assertEqual(log.events[0][1]["reason"], "broker_flat_tp_sl_close_confirmed")

    def test_reconcile_marks_tp_sl_flat_after_stop_loss_broker_flat_confirmation(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="tp_sl_sl_fill_test",
                symbol="TSLA",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=1,
                stop_loss_bricks=1,
                qty=10,
            )
        )
        engine.position = PositionState(
            symbol="TSLA",
            side="long",
            entry_time=pd.Timestamp("2026-07-01T14:00:00Z"),
            entry_price=100.0,
            qty=10,
        )
        signal = Signal(
            signal_id="sl-signal",
            signal_type="exit",
            timestamp=pd.Timestamp("2026-07-01T14:00:01Z"),
            symbol="TSLA",
            side="long",
            broker_side="sell",
            price=99.0,
            reason="stop_loss_1_bricks",
            strategy="unit",
            trader_id="tp_sl_sl_fill_test",
        )
        intent = OrderIntent(
            client_order_id="sl-order",
            timestamp=signal.timestamp,
            symbol="TSLA",
            side="sell",
            signal_type="exit",
            order_type="market",
            strategy="unit",
            reason="stop_loss_1_bricks",
            target_qty=0,
            close_position=True,
            trader_id="tp_sl_sl_fill_test",
        )
        engine._mark_tp_sl_exit_pending(signal, intent)
        engine.mark_close_submitted(intent, response={"id": "alpaca-close-sl-1"})
        execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        execution.trader = FakeMigrationTrader(position=None)
        execution.log = None
        log = MemoryLog()
        runtime = TraderRuntime(
            trader_id="tp_sl_sl_fill_test",
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
            reconcile_engine_with_broker(Path(temp_dir), runtime)

        self.assertIsNone(engine.position)
        self.assertEqual(engine.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_FLAT)
        self.assertEqual(engine.last_closed_exit["reason"], "stop_loss_1_bricks")
        self.assertEqual(log.events[0][0], "state_reconciled")
        self.assertEqual(log.events[0][1]["reason"], "broker_flat_tp_sl_close_confirmed")

    def test_renko_tp_sl_quantity_reconciliation_violation_halts_entries(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="tp_sl_qty_halt_test",
                symbol="TSLA",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=1,
                stop_loss_bricks=1,
                qty=10,
            )
        )
        engine.position = PositionState(
            symbol="TSLA",
            side="long",
            entry_time=pd.Timestamp("2026-07-01T14:00:00Z"),
            entry_price=100.0,
            qty=10,
        )
        execution = AlpacaExecutionEngine.__new__(AlpacaExecutionEngine)
        execution.trader = FakeMigrationTrader(position={"symbol": "TSLA", "side": "long", "qty": "20"})
        execution.log = None
        log = MemoryLog()
        runtime = TraderRuntime(
            trader_id="tp_sl_qty_halt_test",
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
            with self.assertRaises(BrokerEngineStateMismatch):
                reconcile_engine_with_broker(Path(temp_dir), runtime)

        self.assertFalse(runtime.enabled)
        self.assertTrue(engine.reconciliation_unsafe)
        self.assertIn("critical_runtime_halt", [event_type for event_type, _payload in log.events])

        entry_signal = Signal(
            signal_id="entry-signal",
            signal_type="entry",
            timestamp=pd.Timestamp("2026-07-01T14:00:02Z"),
            symbol="TSLA",
            side="long",
            broker_side="buy",
            price=101.0,
            reason="renko_run_1_long",
            strategy="unit",
            trader_id="tp_sl_qty_halt_test",
        )
        entry_intent = OrderIntent(
            client_order_id="entry-order",
            timestamp=entry_signal.timestamp,
            symbol="TSLA",
            side="buy",
            signal_type="entry",
            order_type="market",
            strategy="unit",
            reason="renko_run_1_long",
            qty=10,
            trader_id="tp_sl_qty_halt_test",
        )
        self.assertEqual(should_block_for_risk(runtime, entry_signal, entry_intent), "runtime_disabled")

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
