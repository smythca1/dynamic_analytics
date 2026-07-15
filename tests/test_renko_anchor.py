import unittest

import pandas as pd

from renko_builder import RenkoBuildState, update_renko_build_state


class RenkoAnchorTests(unittest.TestCase):
    def test_default_anchor_uses_first_price(self):
        state = RenkoBuildState(brick_size=2.0)
        update_renko_build_state(
            state,
            [pd.Timestamp("2026-05-28T14:00:00Z"), pd.Timestamp("2026-05-28T14:01:00Z")],
            [442.6, 444.6],
        )

        self.assertEqual(state.last_brick_close, 444.6)
        self.assertEqual(state.bricks[0]["brick_close"], 444.6)

    def test_grid_anchor_uses_zero_based_brick_lattice(self):
        state = RenkoBuildState(brick_size=2.0, anchor_mode="grid", anchor_price=0.0)
        update_renko_build_state(
            state,
            [pd.Timestamp("2026-05-28T14:00:00Z"), pd.Timestamp("2026-05-28T14:01:00Z")],
            [442.6, 444.6],
        )

        self.assertEqual(state.last_brick_close, 444.0)
        self.assertEqual(state.bricks[0]["brick_close"], 444.0)

    def test_reversal_requires_two_bricks_like_tradingview(self):
        state = RenkoBuildState(brick_size=2.0, anchor_mode="grid", anchor_price=0.0)
        update_renko_build_state(
            state,
            [
                pd.Timestamp("2026-05-28T14:00:00Z"),
                pd.Timestamp("2026-05-28T14:00:01Z"),
                pd.Timestamp("2026-05-28T14:00:02Z"),
                pd.Timestamp("2026-05-28T14:00:03Z"),
                pd.Timestamp("2026-05-28T14:00:04Z"),
            ],
            [460.0, 462.0, 464.0, 462.0, 460.0],
        )

        self.assertEqual(
            [(brick["brick_close"], brick["direction"], brick["run_len"]) for brick in state.bricks],
            [(462.0, 1, 1), (464.0, 1, 2), (460.0, -1, 1)],
        )


if __name__ == "__main__":
    unittest.main()
