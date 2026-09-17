import unittest

import pandas as pd

from scripts.e1_renko_production_parity_sweep import PricePoint, dedupe_points, quality_stats, simulate


def unit_trader() -> dict:
    return {
        "id": "unit_e1_reversal",
        "symbol": "TSLA",
        "strategy": {
            "type": "renko_reversal",
            "brick_size": 1.0,
            "entry_bricks": 1,
            "renko_anchor_mode": "grid",
            "renko_anchor_price": 0.0,
        },
        "sizing": {"qty": 1},
        "session": {"allow_overnight": True},
    }


def point(offset: int, price: float) -> PricePoint:
    return PricePoint(
        timestamp=pd.Timestamp("2026-07-01T14:00:00Z") + pd.Timedelta(seconds=offset),
        close=price,
    )


class E1SweepExecutionModelTests(unittest.TestCase):
    def test_next_event_model_never_fills_on_signal_event(self):
        row, _bricks, fills = simulate(
            unit_trader(),
            [point(0, 100.0), point(1, 101.0), point(2, 99.0), point(3, 100.0)],
            brick_size=1.0,
            execution_model="next_event",
            slippage=0.03,
        )

        self.assertEqual(row.execution_model, "next_event")
        self.assertEqual(row.signals, 2)
        self.assertEqual(row.simulated_fills, 3)
        self.assertEqual(row.pending_fills, 0)
        self.assertEqual(
            [(fill.signal_time, fill.fill_time, fill.fill_price) for fill in fills],
            [
                ("2026-07-01T14:00:01Z", "2026-07-01T14:00:02Z", 99.0),
                ("2026-07-01T14:00:02Z", "2026-07-01T14:00:03Z", 100.0),
                ("2026-07-01T14:00:02Z", "2026-07-01T14:00:03Z", 100.0),
            ],
        )

    def test_slippage_model_fills_at_signal_event_with_adverse_prices(self):
        row, _bricks, fills = simulate(
            unit_trader(),
            [point(0, 100.0), point(1, 101.0), point(2, 99.0)],
            brick_size=1.0,
            execution_model="slippage",
            slippage=0.03,
        )

        self.assertEqual(row.simulated_fills, 3)
        self.assertEqual(
            [(fill.action, fill.signal_time, fill.fill_time, round(fill.fill_price, 2)) for fill in fills],
            [
                ("open", "2026-07-01T14:00:01Z", "2026-07-01T14:00:01Z", 101.03),
                ("close", "2026-07-01T14:00:02Z", "2026-07-01T14:00:02Z", 98.97),
                ("open", "2026-07-01T14:00:02Z", "2026-07-01T14:00:02Z", 98.97),
            ],
        )

    def test_data_quality_records_sort_and_dedupe_repairs(self):
        raw = [point(2, 101.0), point(1, 101.0), point(1, 102.0)]
        output = dedupe_points(raw)
        stats = quality_stats(raw, output)

        self.assertEqual(stats.raw_rows, 3)
        self.assertEqual(stats.valid_rows, 3)
        self.assertEqual(stats.out_of_order_rows, 1)
        self.assertEqual(stats.duplicate_timestamps, 1)
        self.assertEqual(stats.unchanged_prices, 1)
        self.assertTrue(stats.sorted_or_deduped)
        self.assertEqual([(item.timestamp, item.close) for item in output], [(point(1, 102.0).timestamp, 102.0), (point(2, 101.0).timestamp, 101.0)])


if __name__ == "__main__":
    unittest.main()
