import csv
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from renko_builder import RenkoBuildState, update_renko_build_state


ROOT = Path(__file__).resolve().parents[1]


def python_bricks(prices, *, brick_size=1.0, anchor_mode="first_price"):
    timestamps = [pd.Timestamp("2026-01-01T14:00:00Z") + pd.Timedelta(seconds=i) for i in range(len(prices))]
    state = RenkoBuildState(brick_size=brick_size, anchor_mode=anchor_mode, anchor_price=0.0)
    update_renko_build_state(state, timestamps, prices)
    return [
        (
            brick["timestamp"].isoformat().replace("+00:00", "Z"),
            f"{float(brick['brick_close']):.12f}",
            str(brick["direction"]),
            str(brick["run_len"]),
        )
        for brick in state.bricks
    ]


class RenkoCppParityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if shutil.which("g++") is None:
            raise unittest.SkipTest("g++ is not installed")
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.binary = Path(cls.temp_dir.name) / "validated_renko_backtest"
        subprocess.run(
            [
                "g++",
                "-std=c++17",
                "-Wall",
                "-Wextra",
                "-Werror",
                str(ROOT / "validated_renko_backtest.cpp"),
                "-o",
                str(cls.binary),
            ],
            check=True,
            cwd=ROOT,
        )

    @classmethod
    def tearDownClass(cls):
        cls.temp_dir.cleanup()

    def run_cpp(self, prices, *, brick_size=1.0, anchor_mode="first_price"):
        path = Path(self.temp_dir.name) / "prices.csv"
        timestamps = [pd.Timestamp("2026-01-01T14:00:00Z") + pd.Timedelta(seconds=i) for i in range(len(prices))]
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["timestamp", "price"])
            for ts, price in zip(timestamps, prices):
                writer.writerow([ts.isoformat().replace("+00:00", "Z"), price])
        completed = subprocess.run(
            [
                str(self.binary),
                "--renko-parity-csv",
                str(path),
                "--brick-size",
                str(brick_size),
                "--anchor-mode",
                anchor_mode,
                "--anchor-price",
                "0",
            ],
            check=True,
            text=True,
            capture_output=True,
            cwd=ROOT,
        )
        rows = list(csv.reader(completed.stdout.splitlines()))
        self.assertEqual(rows[0], ["timestamp", "brick_close", "direction", "run_len"])
        return [tuple(row) for row in rows[1:]]

    def assert_parity(self, prices, *, brick_size=1.0, anchor_mode="first_price"):
        self.assertEqual(
            self.run_cpp(prices, brick_size=brick_size, anchor_mode=anchor_mode),
            python_bricks(prices, brick_size=brick_size, anchor_mode=anchor_mode),
        )

    def test_golden_vectors_match_cpp(self):
        for prices in (
            [100, 101, 102, 103],
            [100, 101, 102, 100],
            [100, 101, 102, 99],
            [100, 103],
            [100, 103, 99],
            [100, 99, 98, 100],
            [100, 101, 100, 99],
            [100, 101 - 1e-9, 101, 99],
        ):
            with self.subTest(prices=prices):
                self.assert_parity(prices)

    def test_grid_anchor_matches_cpp(self):
        self.assert_parity([100.6, 101.0, 101.6, 99.0], anchor_mode="grid")


if __name__ == "__main__":
    unittest.main()
