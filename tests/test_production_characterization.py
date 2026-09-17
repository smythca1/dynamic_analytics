import unittest

import pandas as pd

from fleet_runner import tick_second_bucket
from renko_builder import RenkoBuildState, update_renko_build_state
from silver.market import CanonicalMarketObservationAdapter
from strategy_engine import RenkoStrategyConfig, RenkoStrategyEngine
from trading_events import MarketTick, is_price_forming_trade


def tick(timestamp: str, symbol: str = "TSLA", price: float = 100.0, conditions=("@",)) -> MarketTick:
    return MarketTick(
        timestamp=pd.Timestamp(timestamp),
        symbol=symbol,
        price=price,
        size=1.0,
        exchange="unit",
        trade_id=f"{symbol}-{timestamp}-{price}",
        tape="C",
        conditions=tuple(conditions),
    )


def adapter_ticks(adapter: CanonicalMarketObservationAdapter, trader_id: str, *ticks: MarketTick):
    emitted = []
    for item in ticks:
        result = adapter.process_tick(item, trader_id=trader_id)
        emitted.extend(adapter.observation_ticks(result))
    return emitted


def logical_bricks(state: RenkoBuildState):
    return [
        (
            pd.Timestamp(row["timestamp"]).isoformat(),
            float(row["brick_close"]),
            int(row["direction"]),
            int(row["run_len"]),
        )
        for row in state.bricks
    ]


class ProductionMarketDataCharacterizationTests(unittest.TestCase):
    def test_price_forming_trade_eligibility_current_contract(self):
        self.assertTrue(is_price_forming_trade(tick("2026-07-20T14:00:00Z", conditions=())))
        self.assertTrue(is_price_forming_trade(tick("2026-07-20T14:00:00Z", conditions=("@",))))
        self.assertTrue(is_price_forming_trade(tick("2026-07-20T14:00:00Z", conditions=("@", "F"))))
        self.assertFalse(is_price_forming_trade(tick("2026-07-20T14:00:00Z", conditions=("@", "I"))))
        self.assertFalse(is_price_forming_trade(tick("2026-07-20T14:00:00Z", conditions=("@", "T", "I"))))

    def test_utc_second_bucketing(self):
        bucket = tick_second_bucket(pd.Timestamp("2026-07-20T09:30:00.987654321-04:00"))

        self.assertEqual(bucket, pd.Timestamp("2026-07-20T13:30:00Z"))

    def test_last_eligible_trade_per_occupied_second_and_delayed_emission(self):
        adapter = CanonicalMarketObservationAdapter(run_id="sampler_tsla")
        emitted = adapter_ticks(
            adapter,
            "sampler_tsla",
            tick("2026-07-20T13:30:00.100Z", price=100.0),
            tick("2026-07-20T13:30:00.900Z", price=101.0),
            tick("2026-07-20T13:30:01.001Z", price=102.0),
        )

        self.assertEqual(len(emitted), 1)
        self.assertEqual(emitted[0].timestamp, pd.Timestamp("2026-07-20T13:30:00Z"))
        self.assertEqual(emitted[0].price, 101.0)

    def test_no_synthetic_empty_seconds(self):
        adapter = CanonicalMarketObservationAdapter(run_id="sampler_gap_tsla")
        emitted = adapter_ticks(
            adapter,
            "sampler_gap_tsla",
            tick("2026-07-20T13:30:00.100Z", price=100.0),
            tick("2026-07-20T13:30:05.001Z", price=105.0),
        )

        self.assertEqual(len(emitted), 1)
        self.assertEqual(emitted[0].timestamp, pd.Timestamp("2026-07-20T13:30:00Z"))
        self.assertEqual(emitted[0].price, 100.0)

    def test_out_of_order_tick_behavior(self):
        adapter = CanonicalMarketObservationAdapter(run_id="sampler_ooo_tsla")
        emitted = adapter_ticks(
            adapter,
            "sampler_ooo_tsla",
            tick("2026-07-20T13:30:02.100Z", price=102.0),
            tick("2026-07-20T13:30:01.900Z", price=101.0),
            tick("2026-07-20T13:30:03.001Z", price=103.0),
        )

        self.assertEqual(len(emitted), 1)
        self.assertEqual(emitted[0].timestamp, pd.Timestamp("2026-07-20T13:30:02Z"))
        self.assertEqual(emitted[0].price, 102.0)

    def test_sampler_state_is_isolated_across_traders(self):
        adapter = CanonicalMarketObservationAdapter(run_id="multi")

        adapter_ticks(adapter, "sampler_aapl", tick("2026-07-20T13:30:00.100Z", "AAPL", 200.0))
        adapter_ticks(adapter, "sampler_nvda", tick("2026-07-20T13:30:00.100Z", "NVDA", 300.0))

        first_emitted = adapter_ticks(adapter, "sampler_aapl", tick("2026-07-20T13:30:01.100Z", "AAPL", 201.0))
        second_emitted = adapter_ticks(adapter, "sampler_nvda", tick("2026-07-20T13:30:01.100Z", "NVDA", 301.0))

        self.assertEqual([(row.symbol, row.price) for row in first_emitted], [("AAPL", 200.0)])
        self.assertEqual([(row.symbol, row.price) for row in second_emitted], [("NVDA", 300.0)])


class ProductionRenkoCharacterizationTests(unittest.TestCase):
    def build(self, prices):
        timestamps = [pd.Timestamp("2026-07-20T13:30:00Z") + pd.Timedelta(seconds=i) for i in range(len(prices))]
        state = RenkoBuildState(brick_size=1.0)
        update_renko_build_state(state, timestamps, prices)
        return state

    def test_renko_continuation(self):
        self.assertEqual(
            logical_bricks(self.build([100.0, 101.0, 102.0, 103.0])),
            [
                ("2026-07-20T13:30:01+00:00", 101.0, 1, 1),
                ("2026-07-20T13:30:02+00:00", 102.0, 1, 2),
                ("2026-07-20T13:30:03+00:00", 103.0, 1, 3),
            ],
        )

    def test_renko_two_box_reversal(self):
        self.assertEqual(
            logical_bricks(self.build([100.0, 101.0, 102.0, 100.0])),
            [
                ("2026-07-20T13:30:01+00:00", 101.0, 1, 1),
                ("2026-07-20T13:30:02+00:00", 102.0, 1, 2),
                ("2026-07-20T13:30:03+00:00", 100.0, -1, 1),
            ],
        )

    def test_renko_multiple_bricks_from_one_observation(self):
        self.assertEqual(
            logical_bricks(self.build([100.0, 103.0])),
            [
                ("2026-07-20T13:30:01+00:00", 101.0, 1, 1),
                ("2026-07-20T13:30:01+00:00", 102.0, 1, 2),
                ("2026-07-20T13:30:01+00:00", 103.0, 1, 3),
            ],
        )


class ProductionStrategySignalCharacterizationTests(unittest.TestCase):
    def run_strategy(self):
        engine = RenkoStrategyEngine(
            RenkoStrategyConfig(
                trader_id="characterization_e1",
                symbol="TSLA",
                strategy_type="renko_reversal",
                brick_size=1.0,
                entry_bricks=1,
                qty=1.0,
                renko_anchor_mode="grid",
                renko_anchor_price=0.0,
                entry_start="09:31:00",
                entry_end="15:59:59",
                allow_overnight=True,
            )
        )
        bricks = []
        signals = []
        start = pd.Timestamp("2026-07-20T13:31:00Z")
        for offset, price in enumerate([100.0, 101.0, 99.0, 101.0]):
            emitted_bricks, actions = engine.on_tick(
                MarketTick(
                    timestamp=start + pd.Timedelta(seconds=offset),
                    symbol="TSLA",
                    price=price,
                    size=1.0,
                    exchange="unit",
                    trade_id=f"strategy-{offset}",
                    tape="C",
                    conditions=("@",),
                )
            )
            bricks.extend(
                (
                    brick.timestamp.isoformat(),
                    brick.open,
                    brick.close,
                    brick.direction,
                    brick.run_len,
                )
                for brick in emitted_bricks
            )
            signals.extend(
                (
                    signal.timestamp.isoformat(),
                    signal.signal_type,
                    signal.side,
                    signal.broker_side,
                    signal.price,
                    signal.reason,
                    intent.side,
                    intent.qty,
                    intent.target_qty,
                    intent.close_position,
                )
                for signal, intent in actions
            )
        return bricks, signals

    def test_strategy_signal_behavior_and_deterministic_repeated_output(self):
        first_bricks, first_signals = self.run_strategy()
        second_bricks, second_signals = self.run_strategy()

        self.assertEqual(first_bricks, second_bricks)
        self.assertEqual(first_signals, second_signals)
        self.assertEqual(
            first_signals,
            [
                (
                    "2026-07-20T13:31:01+00:00",
                    "entry",
                    "long",
                    "buy",
                    101.0,
                    "long_entry_e1",
                    "buy",
                    1.0,
                    None,
                    False,
                ),
                (
                    "2026-07-20T13:31:02+00:00",
                    "entry",
                    "short",
                    "sell",
                    99.0,
                    "short_entry_e1_reversal",
                    "sell",
                    2.0,
                    1.0,
                    False,
                ),
                (
                    "2026-07-20T13:31:03+00:00",
                    "entry",
                    "long",
                    "buy",
                    101.0,
                    "long_entry_e1_reversal",
                    "buy",
                    2.0,
                    1.0,
                    False,
                ),
            ],
        )


if __name__ == "__main__":
    unittest.main()
