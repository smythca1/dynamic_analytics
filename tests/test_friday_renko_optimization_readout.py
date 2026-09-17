from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.friday_renko_optimization_readout import FLOOR, decision, distinct_regimes, main


def row(
    candidate_id: str,
    brick_pct: float,
    *,
    entry: str = "E1",
    tp: int | str = "UNKNOWN",
    sl: int | str = "UNKNOWN",
    net: float = 100.0,
    robustness: float = 0.9,
    local_profitable_pct: float = 100.0,
    trades: int = 20,
    pf: float = 2.0,
    profitable_sessions: int = 4,
) -> dict:
    return {
        "candidate_id": candidate_id,
        "candidate_type": "BRICK_PERCENT_GRID",
        "candidate_status": "COMPLETED",
        "brick_pct": brick_pct,
        "entry_confirmation": entry,
        "take_profit_bricks": tp,
        "stop_loss_bricks": sl,
        "net_pnl_dollars": net,
        "strategy_return_pct": net / 100.0,
        "return_on_allocated_capital_pct": net / 100.0,
        "max_drawdown_dollars": 10.0,
        "max_drawdown_pct": 1.0,
        "profit_factor": pf,
        "closed_trades": trades,
        "win_rate_pct": 55.0,
        "average_trade_pnl_dollars": net / trades if trades else 0.0,
        "average_winner_dollars": 10.0,
        "average_loser_dollars": -5.0,
        "long_pnl_dollars": net / 2.0,
        "short_pnl_dollars": net / 2.0,
        "robustness_score": robustness,
        "local_profitable_pct": local_profitable_pct,
        "session_count": 5,
        "profitable_sessions": profitable_sessions,
        "losing_sessions": 5 - profitable_sessions,
    }


class FridayRenkoOptimizationReadoutTests(unittest.TestCase):
    def test_contiguous_profitable_bricks_are_one_regime_not_three_rows(self) -> None:
        regimes = distinct_regimes(
            [
                row("plateau-040", 0.40, net=90),
                row("plateau-042", 0.42, net=120),
                row("plateau-044", 0.44, net=110),
                row("separate-070", 0.70, net=80, robustness=0.8),
            ],
            strategy="Reversal",
        )

        self.assertEqual(len(regimes), 2)
        self.assertEqual(regimes[0]["member_count"], 3)
        self.assertEqual(regimes[0]["leader"]["candidate_id"], "plateau-042")

    def test_floor_boundary_reduces_confidence_and_marks_truncated_neighbors(self) -> None:
        regimes = distinct_regimes(
            [
                row("floor", FLOOR, net=120),
                row("neighbor", 0.18, net=90),
            ],
            strategy="Reversal",
        )

        self.assertTrue(regimes[0]["boundary_truncated"])
        self.assertIn("below floor", regimes[0]["neighbor_summary"])
        self.assertNotEqual(regimes[0]["confidence"], "High")

    def test_tp_sl_regime_identity_includes_entry_tp_and_sl(self) -> None:
        regimes = distinct_regimes(
            [
                row("e1-tp1-sl1", 0.40, entry="E1", tp=1, sl=1, net=100),
                row("e1-tp2-sl1", 0.40, entry="E1", tp=2, sl=1, net=95),
                row("e2-tp1-sl1", 0.40, entry="E2", tp=1, sl=1, net=90),
            ],
            strategy="TP/SL",
        )

        identities = {regime["family"] for regime in regimes}
        self.assertEqual(identities, {("E1", 1, 1), ("E1", 2, 1), ("E2", 1, 1)})

    def test_no_proposed_regime_with_profitable_current_keeps_current(self) -> None:
        current = row("current", 0.12, net=5.0, trades=7, pf=2.0)

        self.assertEqual(decision(current, None), "KEEP CURRENT")

    def test_min_trade_floor_is_configurable_for_six_trade_rerun(self) -> None:
        candidate = row("six-trade-candidate", 0.40, trades=6, net=80, pf=1.8)

        self.assertEqual(distinct_regimes([candidate], strategy="Reversal"), [])
        self.assertEqual(len(distinct_regimes([candidate], strategy="Reversal", min_trades=6)), 1)
        self.assertEqual(decision(candidate, None, min_trades=6), "KEEP CURRENT")

    def test_cli_report_uses_configured_min_trade_floor(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            reversal = root / "optimizations" / "rev"
            tp_sl = root / "optimizations" / "tp"
            reversal.mkdir(parents=True)
            tp_sl.mkdir(parents=True)
            fields = sorted(row("sample", 0.40).keys() | {"symbol"})
            reversal_rows = [
                {**row("rev-current", 0.36, trades=5, net=40, pf=1.4), "symbol": "TSLA", "candidate_type": "CURRENT_PRODUCTION"},
                {**row("rev-grid", 0.40, trades=6, net=80, pf=1.8), "symbol": "TSLA"},
            ]
            tp_sl_rows = [
                {**row("tp-current", 0.30, entry="E1", tp=1, sl=1, trades=5, net=40, pf=1.4), "symbol": "TSLA", "candidate_type": "CURRENT_PRODUCTION"},
                {**row("tp-grid", 0.42, entry="E1", tp=1, sl=1, trades=6, net=75, pf=1.7), "symbol": "TSLA"},
            ]
            write_fixture_csv(reversal / "all_candidates.csv", fields, reversal_rows)
            write_fixture_csv(tp_sl / "all_candidates.csv", fields, tp_sl_rows)
            (reversal / "optimization_manifest.json").write_text('{"session_dates":["2026-08-24","2026-09-04"]}', encoding="utf-8")
            (tp_sl / "optimization_manifest.json").write_text('{"session_dates":["2026-08-24","2026-09-04"]}', encoding="utf-8")

            self.assertEqual(
                main(
                    [
                        "--root",
                        str(root),
                        "--reversal-id",
                        "rev",
                        "--tp-sl-id",
                        "tp",
                        "--output",
                        "readout",
                        "--min-trades",
                        "6",
                    ]
                ),
                0,
            )

            report = (root / "readout" / "report.md").read_text(encoding="utf-8")
            summary = (root / "readout" / "run_summary.json").read_text(encoding="utf-8")
            self.assertIn("at least `6` closed trades", report)
            self.assertIn("below 6 closed trades", report)
            self.assertNotIn("below 10 closed trades", report)
            self.assertIn("2-Session Return", report)
            self.assertNotIn("5-Day Return", report)
            self.assertIn('"minimum_closed_trades": 6', summary)

    def test_cli_accepts_lower_floor_and_writes_three_way_artifacts(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp) / "floor012"
            previous_root = Path(tmp) / "floor016"
            reversal = root / "optimizations" / "rev012"
            tp_sl = root / "optimizations" / "tp012"
            previous_reversal = previous_root / "optimizations" / "rev016"
            previous_tp_sl = previous_root / "optimizations" / "tp016"
            for directory in (reversal, tp_sl, previous_reversal, previous_tp_sl):
                directory.mkdir(parents=True)
            fields = sorted(row("sample", 0.40).keys() | {"symbol"})
            current = {**row("current", 0.10, trades=12, net=10, pf=1.1), "symbol": "AAPL", "candidate_type": "CURRENT_PRODUCTION"}
            floor012 = {**row("floor012", 0.12, trades=14, net=80, pf=1.8), "symbol": "AAPL"}
            floor014 = {**row("floor014", 0.14, trades=13, net=75, pf=1.7), "symbol": "AAPL"}
            floor016 = {**row("floor016", 0.16, trades=11, net=20, pf=1.2), "symbol": "AAPL"}
            write_fixture_csv(reversal / "all_candidates.csv", fields, [current, floor012, floor014, floor016])
            write_fixture_csv(tp_sl / "all_candidates.csv", fields, [current, floor012, floor014, floor016])
            write_fixture_csv(previous_reversal / "all_candidates.csv", fields, [current, floor016])
            write_fixture_csv(previous_tp_sl / "all_candidates.csv", fields, [current, floor016])
            for directory in (reversal, tp_sl, previous_reversal, previous_tp_sl):
                (directory / "optimization_manifest.json").write_text('{"session_dates":["2026-08-31"]}', encoding="utf-8")

            self.assertEqual(
                main(
                    [
                        "--root",
                        str(root),
                        "--reversal-id",
                        "rev012",
                        "--tp-sl-id",
                        "tp012",
                        "--previous-root",
                        str(previous_root),
                        "--previous-reversal-id",
                        "rev016",
                        "--previous-tp-sl-id",
                        "tp016",
                        "--floor",
                        "0.12",
                    ]
                ),
                0,
            )

            report = (root / "readout" / "report.md").read_text(encoding="utf-8")
            summary = (root / "readout" / "run_summary.json").read_text(encoding="utf-8")
            self.assertIn("Controlled 0.12% Floor Rerun", report)
            self.assertIn("Best >=0.16%", report)
            self.assertIn("Reopened 0.12-0.16% region", report)
            self.assertTrue((root / "readout" / "three_way_comparison.csv").exists())
            self.assertTrue((root / "readout" / "reopened_region_candidates.csv").exists())
            self.assertIn('"candidate_floor_pct": 0.12', summary)
            self.assertIn('"reopened_region_grid_candidates": 6', summary)


def write_fixture_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    import csv

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    unittest.main()
