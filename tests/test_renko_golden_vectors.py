import unittest

import pandas as pd

from renko_builder import RenkoBuildState, update_renko_build_state


def build(prices, *, brick_size=1.0, anchor_mode="first_price", split_at=None):
    timestamps = [pd.Timestamp("2026-01-01T14:00:00Z") + pd.Timedelta(seconds=i) for i in range(len(prices))]
    state = RenkoBuildState(brick_size=brick_size, anchor_mode=anchor_mode, anchor_price=0.0)
    if split_at is None:
        update_renko_build_state(state, timestamps, prices)
        return state
    update_renko_build_state(state, timestamps[:split_at], prices[:split_at])
    restored = RenkoBuildState(
        brick_size=state.brick_size,
        last_brick_close=state.last_brick_close,
        run_dir=state.run_dir,
        run_len=state.run_len,
        anchor_mode=state.anchor_mode,
        anchor_price=state.anchor_price,
    )
    update_renko_build_state(restored, timestamps[split_at:], prices[split_at:])
    state.bricks.extend(restored.bricks)
    state.last_brick_close = restored.last_brick_close
    state.run_dir = restored.run_dir
    state.run_len = restored.run_len
    return state


def logical(bricks):
    return [
        (
            brick["timestamp"].isoformat().replace("+00:00", "Z"),
            brick["brick_close"],
            brick["direction"],
            brick["run_len"],
        )
        for brick in bricks
    ]


class RenkoGoldenVectorTests(unittest.TestCase):
    def assert_vector(self, prices, expected):
        state = build(prices)
        self.assertEqual(logical(state.bricks), expected)

    def test_continuation_up(self):
        self.assert_vector(
            [100, 101, 102, 103],
            [
                ("2026-01-01T14:00:01Z", 101.0, 1, 1),
                ("2026-01-01T14:00:02Z", 102.0, 1, 2),
                ("2026-01-01T14:00:03Z", 103.0, 1, 3),
            ],
        )

    def test_two_box_reversal_exact_threshold(self):
        self.assert_vector(
            [100, 101, 102, 100],
            [
                ("2026-01-01T14:00:01Z", 101.0, 1, 1),
                ("2026-01-01T14:00:02Z", 102.0, 1, 2),
                ("2026-01-01T14:00:03Z", 100.0, -1, 1),
            ],
        )

    def test_large_reversal_then_continuation_same_tick(self):
        self.assert_vector(
            [100, 101, 102, 99],
            [
                ("2026-01-01T14:00:01Z", 101.0, 1, 1),
                ("2026-01-01T14:00:02Z", 102.0, 1, 2),
                ("2026-01-01T14:00:03Z", 100.0, -1, 1),
                ("2026-01-01T14:00:03Z", 99.0, -1, 2),
            ],
        )

    def test_multiple_continuation_bricks_from_one_tick(self):
        self.assert_vector(
            [100, 103],
            [
                ("2026-01-01T14:00:01Z", 101.0, 1, 1),
                ("2026-01-01T14:00:01Z", 102.0, 1, 2),
                ("2026-01-01T14:00:01Z", 103.0, 1, 3),
            ],
        )

    def test_large_up_then_large_down(self):
        self.assert_vector(
            [100, 103, 99],
            [
                ("2026-01-01T14:00:01Z", 101.0, 1, 1),
                ("2026-01-01T14:00:01Z", 102.0, 1, 2),
                ("2026-01-01T14:00:01Z", 103.0, 1, 3),
                ("2026-01-01T14:00:02Z", 101.0, -1, 1),
                ("2026-01-01T14:00:02Z", 100.0, -1, 2),
                ("2026-01-01T14:00:02Z", 99.0, -1, 3),
            ],
        )

    def test_down_reversal_exact_threshold(self):
        self.assert_vector(
            [100, 99, 98, 100],
            [
                ("2026-01-01T14:00:01Z", 99.0, -1, 1),
                ("2026-01-01T14:00:02Z", 98.0, -1, 2),
                ("2026-01-01T14:00:03Z", 100.0, 1, 1),
            ],
        )

    def test_one_box_pullback_does_not_reverse(self):
        self.assert_vector(
            [100, 101, 100, 99],
            [
                ("2026-01-01T14:00:01Z", 101.0, 1, 1),
                ("2026-01-01T14:00:03Z", 99.0, -1, 1),
            ],
        )

    def test_boundary_values_around_threshold(self):
        below = build([100, 101 - 1e-9])
        exact = build([100, 101])
        above = build([100, 101 + 1e-9])

        self.assertEqual(logical(below.bricks), [])
        self.assertEqual(logical(exact.bricks), [("2026-01-01T14:00:01Z", 101.0, 1, 1)])
        self.assertEqual(logical(above.bricks), [("2026-01-01T14:00:01Z", 101.0, 1, 1)])

    def test_grid_anchor(self):
        state = build([100.6, 101.0, 101.6], anchor_mode="grid")
        self.assertEqual(state.last_brick_close, 101.0)
        self.assertEqual(logical(state.bricks), [("2026-01-01T14:00:01Z", 101.0, 1, 1)])

    def test_restart_determinism(self):
        prices = [100, 103, 99, 102, 98, 101]
        uninterrupted = build(prices)
        for split_at in range(1, len(prices)):
            restarted = build(prices, split_at=split_at)
            self.assertEqual(logical(restarted.bricks), logical(uninterrupted.bricks))
            self.assertEqual(restarted.last_brick_close, uninterrupted.last_brick_close)
            self.assertEqual(restarted.run_dir, uninterrupted.run_dir)
            self.assertEqual(restarted.run_len, uninterrupted.run_len)


if __name__ == "__main__":
    unittest.main()
