from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path

from replay.engine import run_replay
from replay.models import ReplayConfig
from silver.market import CanonicalObservation
from strategy_engine import RenkoStrategyConfig


class CanonicalReplayIntegrationTests(unittest.TestCase):
    def _source(self, directory: Path) -> Path:
        path = directory / "observations_1s.jsonl"
        rows = []
        for sequence, price in enumerate(("100", "101", "100", "102"), start=1):
            rows.append(asdict(CanonicalObservation(
                observation_id=f"obs-{sequence}",
                run_id="canonical-test",
                symbol="TSLA",
                second_bucket=f"2026-07-20T13:3{sequence}:00+00:00",
                emitted_sequence=sequence,
                close_price=Decimal(price),
                source_eligible_trade_id=f"eligible-{sequence}",
                source_sip_timestamp=f"2026-07-20T13:3{sequence}:00+00:00",
                source_alpaca_trade_id=f"trade-{sequence}",
            )))
        path.write_text("\n".join(json.dumps(row, default=str) for row in rows) + "\n", encoding="utf-8")
        return path

    def _config(self, source: Path, output: Path, run_id: str) -> ReplayConfig:
        strategy = RenkoStrategyConfig(
            trader_id="canonical-test",
            symbol="TSLA",
            brick_size=1.0,
            entry_bricks=1,
            strategy_type="renko_reversal",
            strategy_name="canonical_test",
            qty=1.0,
            renko_anchor_mode="grid",
            renko_anchor_price=0.0,
            entry_start="00:00:00",
            entry_end="23:59:59",
            allow_overnight=True,
        )
        return ReplayConfig(
            run_id=run_id,
            trader_id="canonical-test",
            symbol="TSLA",
            start="2026-07-20T13:30:00Z",
            end="2026-07-20T13:40:00Z",
            timezone="UTC",
            session_mode="configured",
            strategy_config=asdict(strategy),
            initial_capital="10000",
            brick_mode="BRICK_DOLLARS",
            brick_size_dollars="1",
            source_data_path=str(source),
            source_semantic_mode="prepared_canonical_observations",
            output_directory=str(output),
        )

    def test_canonical_observations_are_consumed_without_resampling(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = run_replay(self._config(self._source(root), root / "out", "canonical-run"))
            self.assertEqual(len(result.ledgers.canonical_observations), 4)
            self.assertEqual(result.manifest["source_semantic_mode"], "prepared_canonical_observations")
            self.assertEqual(result.manifest["source_integrity"]["malformed_rows"], 0)

    def test_canonical_replay_is_deterministic(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self._source(root)
            first = run_replay(self._config(source, root / "first", "canonical-repeat"))
            second = run_replay(self._config(source, root / "second", "canonical-repeat"))
            self.assertEqual(first.context.replay_fingerprint, second.context.replay_fingerprint)
            self.assertEqual(first.metrics, second.metrics)

    def test_cli_invalid_canonical_source_returns_nonzero(self):
        with tempfile.TemporaryDirectory() as tmp:
            command = [
                sys.executable, "scripts/run_replay.py", "run",
                "--source-semantic-mode", "prepared_canonical_observations",
                "--trader-id", "bad", "--source", str(Path(tmp) / "missing.jsonl"),
                "--start", "2026-07-20T13:30:00Z", "--end", "2026-07-20T13:40:00Z",
                "--symbol", "TSLA", "--initial-capital", "10000", "--run-id", "bad",
                "--brick-mode", "BRICK_DOLLARS", "--brick-size", "1",
            ]
            completed = subprocess.run(command, capture_output=True, text=True, env={**__import__("os").environ, "PYTHONPATH": "."})
            self.assertNotEqual(completed.returncode, 0)


if __name__ == "__main__":
    unittest.main()
