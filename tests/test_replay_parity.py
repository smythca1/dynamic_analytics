from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from replay.parity import compare_logic_parity, compare_sampler_observation_parity
from replay.parity import _compare_bricks
from tests.test_replay_engine import replay_config, row, run_replay, write_jsonl


class ReplayParityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def sampler_observation(
        self,
        *,
        sequence: int,
        bucket: str = "2026-07-20T13:31:00+00:00",
        price: str = "100",
        symbol: str = "AAPL",
    ) -> dict[str, object]:
        return {
            "evidence_type": "production_observation",
            "symbol": symbol,
            "payload": {
                "sequence": sequence,
                "observation_bucket_timestamp": bucket,
                "close_price": price,
            },
        }

    def test_report_writes_required_compact_outputs(self) -> None:
        source = self.root / "source.jsonl"
        write_jsonl(
            source,
            [
                row("2026-07-20T13:31:00.100Z", 100, "t0", 0),
                row("2026-07-20T13:31:01.100Z", 101, "t1", 1),
                row("2026-07-20T13:31:02.100Z", 99, "t2", 2),
            ],
        )
        result = run_replay(replay_config(source, self.root / "out", run_id="parity-required-files"))
        report = compare_logic_parity(parity_run_id="required-files", replay=result, output_directory=self.root / "parity")
        output = self.root / "parity" / "required-files"
        self.assertEqual(report.classification, "FULL_SESSION_RESEARCH")
        self.assertTrue((output / "manifest.json").exists())
        self.assertTrue((output / "parity_summary.json").exists())
        self.assertTrue((output / "mismatch_summary.json").exists())
        self.assertTrue((output / "report.txt").exists())
        manifest = json.loads((output / "manifest.json").read_text())
        self.assertEqual(manifest["configuration_hash"], result.context.configuration_hash)

    def test_controlled_parity_fixture_has_100_percent_observation_match(self) -> None:
        source = self.root / "source.jsonl"
        write_jsonl(
            source,
            [
                row("2026-07-20T13:31:00.100Z", 100, "t0", 0),
                row("2026-07-20T13:31:01.100Z", 101, "t1", 1),
                row("2026-07-20T13:31:02.100Z", 102, "t2", 2),
            ],
        )
        result = run_replay(replay_config(source, self.root / "out", run_id="obs-fixture"))
        production_observations = self.root / "production_observations.jsonl"
        write_jsonl(
            production_observations,
            [
                {
                    "symbol": observation.symbol,
                    "timestamp": observation.second_bucket.isoformat(),
                    "close_price": str(observation.close_price),
                    "source_record_id": observation.source_record_id,
                }
                for observation in result.ledgers.canonical_observations
            ],
        )

        report = compare_logic_parity(
            parity_run_id="obs-match",
            replay=result,
            output_directory=self.root / "parity",
            production_observations_path=production_observations,
        )

        self.assertEqual(report.observations.component_classification, "VALIDATED")
        self.assertEqual(report.observations.exact_match_rate, 100.0)
        self.assertEqual(report.classification, "LOGIC_PARITY_PARTIAL")

    def test_shadow_sampler_exact_fixture_reaches_100_percent_observation_parity(self) -> None:
        production = [
            self.sampler_observation(sequence=1, bucket="2026-07-20T13:31:00+00:00", price="100"),
            self.sampler_observation(sequence=2, bucket="2026-07-20T13:31:01+00:00", price="101"),
        ]
        shadow = [
            self.sampler_observation(sequence=1, bucket="2026-07-20T13:31:00+00:00", price="100.000000000"),
            self.sampler_observation(sequence=2, bucket="2026-07-20T13:31:01+00:00", price="101"),
        ]

        comparison, mismatches = compare_sampler_observation_parity(production, shadow)

        self.assertEqual(mismatches, [])
        self.assertEqual(comparison.component_classification, "VALIDATED")
        self.assertEqual(comparison.exact_match_rate, 100.0)
        self.assertEqual(comparison.maximum_consecutive_matching_sequence, 2)

    def test_shadow_sampler_price_mismatch_is_detected_offline(self) -> None:
        production = [self.sampler_observation(sequence=1, price="100")]
        shadow = [self.sampler_observation(sequence=1, price="100.02")]

        comparison, mismatches = compare_sampler_observation_parity(production, shadow)

        self.assertEqual(comparison.component_classification, "FAILED")
        self.assertEqual(mismatches[0]["category"], "price_mismatch")
        self.assertEqual(comparison.first_mismatch_sequence, 1)

    def test_shadow_sampler_bucket_mismatch_is_detected_offline(self) -> None:
        production = [self.sampler_observation(sequence=1, bucket="2026-07-20T13:31:00+00:00")]
        shadow = [self.sampler_observation(sequence=1, bucket="2026-07-20T13:31:01+00:00")]

        comparison, mismatches = compare_sampler_observation_parity(production, shadow)

        self.assertEqual(comparison.component_classification, "FAILED")
        self.assertEqual(mismatches[0]["category"], "bucket_mismatch")
        self.assertEqual(comparison.first_mismatch_timestamp, "2026-07-20T13:31:00+00:00")

    def test_shadow_sampler_sequence_mismatch_is_detected_offline(self) -> None:
        production = [self.sampler_observation(sequence=1)]
        shadow = [self.sampler_observation(sequence=2)]

        _, mismatches = compare_sampler_observation_parity(production, shadow)

        self.assertEqual(mismatches[0]["category"], "sequence_mismatch")

    def test_shadow_sampler_production_only_observation_is_detected_offline(self) -> None:
        production = [self.sampler_observation(sequence=1)]
        shadow: list[dict[str, object]] = []

        comparison, mismatches = compare_sampler_observation_parity(production, shadow)

        self.assertEqual(mismatches[0]["category"], "production_only_observation")
        self.assertEqual(comparison.mismatch_categories["production_only_observation"], 1)

    def test_shadow_sampler_shadow_only_observation_is_detected_offline(self) -> None:
        production: list[dict[str, object]] = []
        shadow = [self.sampler_observation(sequence=1)]

        comparison, mismatches = compare_sampler_observation_parity(production, shadow)

        self.assertEqual(mismatches[0]["category"], "shadow_only_observation")
        self.assertEqual(comparison.mismatch_categories["shadow_only_observation"], 1)

    def test_shadow_sampler_first_divergence_is_deterministic(self) -> None:
        production = [
            self.sampler_observation(sequence=1, bucket="2026-07-20T13:31:00+00:00", price="100"),
            self.sampler_observation(sequence=2, bucket="2026-07-20T13:31:01+00:00", price="101"),
        ]
        shadow = [
            self.sampler_observation(sequence=1, bucket="2026-07-20T13:31:00+00:00", price="100"),
            self.sampler_observation(sequence=2, bucket="2026-07-20T13:31:01+00:00", price="102"),
        ]

        first, first_mismatches = compare_sampler_observation_parity(production, shadow)
        second, second_mismatches = compare_sampler_observation_parity(production, shadow)

        self.assertEqual(first_mismatches, second_mismatches)
        self.assertEqual(first.first_mismatch_sequence, second.first_mismatch_sequence)
        self.assertEqual(first.first_mismatch_timestamp, "2026-07-20T13:31:01+00:00")

    def test_controlled_parity_fixture_has_100_percent_brick_match(self) -> None:
        source = self.root / "source.jsonl"
        write_jsonl(
            source,
            [
                row("2026-07-20T13:31:00.100Z", 100, "t0", 0),
                row("2026-07-20T13:31:01.100Z", 101, "t1", 1),
                row("2026-07-20T13:31:02.100Z", 102, "t2", 2),
            ],
        )
        result = run_replay(replay_config(source, self.root / "out", run_id="brick-fixture"))
        production_bricks = self.root / "production_bricks.jsonl"
        write_jsonl(
            production_bricks,
            [
                {
                    "timestamp": brick.timestamp.isoformat(),
                    "symbol": brick.symbol,
                    "open": brick.open,
                    "close": brick.close,
                    "direction": brick.direction,
                    "run_len": brick.run_len,
                    "brick_size": brick.brick_size,
                }
                for brick in result.ledgers.renko_bricks
            ],
        )

        report = compare_logic_parity(
            parity_run_id="brick-match",
            replay=result,
            output_directory=self.root / "parity",
            production_renko_path=production_bricks,
        )

        self.assertEqual(report.renko_bricks.component_classification, "VALIDATED")
        self.assertEqual(report.renko_bricks.economic_match_rate, 100.0)
        self.assertEqual(report.renko_bricks.logical_timestamp_match_rate, 100.0)
        self.assertEqual(report.classification, "LOGIC_PARITY_PARTIAL")

    def test_economic_renko_match_with_processing_time_difference_is_not_construction_mismatch(self) -> None:
        production = [
            {
                "symbol": "AAPL",
                "brick_logical_timestamp": "2026-07-10T14:24:26+00:00",
                "source_observation_bucket_timestamp": "2026-07-10T14:24:26+00:00",
                "production_processing_timestamp": "2026-07-10T14:24:31+00:00",
                "open": "314.56619999999947",
                "close": "313.2218999999995",
                "direction": -1,
                "run_len": 1,
                "brick_size": "1.3443",
            }
        ]
        replay = [
            {
                "symbol": "AAPL",
                "brick_logical_timestamp": "2026-07-10T14:24:26+00:00",
                "source_observation_bucket_timestamp": "2026-07-10T14:24:26+00:00",
                "production_processing_timestamp": "2026-07-10T14:24:35+00:00",
                "open": "314.5662",
                "close": "313.2219",
                "direction": -1,
                "run_len": 1,
                "brick_size": "1.3443",
            }
        ]

        comparison, mismatches = _compare_bricks(production, replay)

        self.assertEqual(mismatches, [])
        self.assertEqual(comparison.component_classification, "VALIDATED")
        self.assertEqual(comparison.economic_match_rate, 100.0)
        self.assertEqual(comparison.processing_timestamp_difference_count, 1)

    def test_logical_timestamp_mismatch_remains_real_renko_mismatch(self) -> None:
        production = [
            {
                "symbol": "AAPL",
                "brick_logical_timestamp": "2026-07-10T14:24:26+00:00",
                "source_observation_bucket_timestamp": "2026-07-10T14:24:26+00:00",
                "open": "314.56619999999947",
                "close": "313.2218999999995",
                "direction": -1,
                "run_len": 1,
                "brick_size": "1.3443",
            }
        ]
        replay = [
            {
                "symbol": "AAPL",
                "brick_logical_timestamp": "2026-07-10T14:23:58+00:00",
                "source_observation_bucket_timestamp": "2026-07-10T14:23:58+00:00",
                "open": "314.5662",
                "close": "313.2219",
                "direction": -1,
                "run_len": 1,
                "brick_size": "1.3443",
            }
        ]

        comparison, mismatches = _compare_bricks(production, replay)

        self.assertEqual(comparison.component_classification, "FAILED")
        self.assertEqual(comparison.economic_match_rate, 100.0)
        self.assertEqual(comparison.logical_timestamp_match_rate, 0.0)
        self.assertEqual(mismatches[0]["category"], "logical_timestamp_mismatch")

    def test_overall_classification_cannot_exceed_evidence_completeness(self) -> None:
        source = self.root / "source.jsonl"
        write_jsonl(source, [row("2026-07-20T13:31:00.100Z", 100, "t0", 0), row("2026-07-20T13:31:01.100Z", 101, "t1", 1)])
        result = run_replay(replay_config(source, self.root / "out", run_id="limited-evidence"))
        production_observations = self.root / "production_observations.jsonl"
        write_jsonl(
            production_observations,
            [
                {
                    "symbol": observation.symbol,
                    "timestamp": observation.second_bucket.isoformat(),
                    "close_price": str(observation.close_price),
                    "source_record_id": observation.source_record_id,
                }
                for observation in result.ledgers.canonical_observations
            ],
        )

        report = compare_logic_parity(
            parity_run_id="limited",
            replay=result,
            output_directory=self.root / "parity",
            production_observations_path=production_observations,
        )

        self.assertEqual(report.observations.component_classification, "VALIDATED")
        self.assertEqual(report.strategy_signals.component_classification, "NOT_VERIFIABLE")
        self.assertNotEqual(report.classification, "LOGIC_PARITY_VALIDATED")


if __name__ == "__main__":
    unittest.main()
