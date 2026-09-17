from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest import mock

import pandas as pd

from optimization.models import OptimizationConfig, PreparedSource, TraderSelection
from optimization.reversal_volume_research import (
    NO_FILTER_LABEL,
    ReversalVolumeCandidateSpec,
    ReversalVolumeResearchConfig,
    VolumeZSnapshot,
    _volume_z_snapshot,
    brick_grid,
    build_candidate_specs as build_research_candidate_specs,
    build_volume_z_lookup,
    run_candidate as run_research_candidate,
    run_reversal_volume_research,
    volume_threshold_label,
)
from optimization.runner import _execute_candidate, build_candidate_specs as build_optimizer_candidate_specs, load_enabled_renko_traders
from optimization.source_preparation import load_prepared_observations, prepare_source
from silver.market import CanonicalObservation
from strategy_engine import RenkoStrategyConfig, RenkoStrategyEngine


SESSIONS = (date(2026, 8, 10), date(2026, 8, 11), date(2026, 8, 12))


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def config_payload() -> dict:
    return {
        "traders": [
            {
                "id": "aapl_reversal",
                "enabled": True,
                "symbol": "AAPL",
                "strategy_name": "aapl_reversal_test",
                "strategy": {
                    "type": "renko_reversal",
                    "brick_size": 1.0,
                    "entry_bricks": 1,
                    "allow_short": True,
                    "order_type": "market",
                    "exit_management": "strategy",
                    "renko_anchor_mode": "grid",
                    "renko_anchor_price": 0.0,
                },
                "sizing": {"qty": 1},
                "session": {
                    "timezone": "America/New_York",
                    "entry_start": "09:30:00",
                    "entry_end": "15:59:59",
                    "allow_overnight": True,
                },
                "data": {"renko_source": "1s_close"},
            }
        ]
    }


def source_rows(session: date, *, prices: list[float], sizes: list[int] | None = None) -> list[dict]:
    start = datetime(session.year, session.month, session.day, 13, 30, tzinfo=timezone.utc)
    sizes = sizes or [1] * len(prices)
    return [
        {
            "timestamp": (start + timedelta(seconds=index)).isoformat().replace("+00:00", "Z"),
            "symbol": "AAPL",
            "price": price,
            "size": sizes[index],
            "exchange": "unit",
            "tape": "C",
            "trade_id": f"{session.isoformat()}-{index}",
            "conditions": ["@"],
            "source_sequence": index,
            "row_order": index,
        }
        for index, price in enumerate(prices)
    ]


def observations(prices: list[float]) -> tuple[CanonicalObservation, ...]:
    start = datetime(2026, 8, 10, 13, 30, tzinfo=timezone.utc)
    return tuple(
        CanonicalObservation(
            observation_id=f"obs-{index}",
            run_id="research-test",
            symbol="AAPL",
            second_bucket=start + timedelta(seconds=index),
            emitted_sequence=index,
            close_price=Decimal(str(price)),
            source_eligible_trade_id=f"eligible-{index}",
            source_sip_timestamp=start + timedelta(seconds=index),
            source_alpaca_trade_id=f"trade-{index}",
            source_exchange="unit",
            source_tape="C",
        )
        for index, price in enumerate(prices)
    )


def prepared_stub(root: Path) -> PreparedSource:
    return PreparedSource(
        preparation_id="prep-test",
        symbol="AAPL",
        session_dates=(SESSIONS[0],),
        source_files=tuple(),
        source_combined_hash="source-hash",
        observation_count=0,
        output_directory=str(root),
        observations_path=str(root / "observations.jsonl"),
        diagnostics_summary={},
        data_quality_status="OK",
        market_stream_contract={"fingerprint": "market-fingerprint", "version": "unit"},
        approved_canonical_path=True,
        promotable=True,
    )


def trader_selection() -> TraderSelection:
    strategy = RenkoStrategyConfig(
        trader_id="aapl_reversal",
        symbol="AAPL",
        strategy_type="renko_reversal",
        brick_size=1.0,
        entry_bricks=1,
        allow_short=True,
        strategy_name="aapl_reversal_test",
        qty=1,
        order_type="market",
        exit_management="strategy",
        renko_anchor_mode="grid",
        renko_anchor_price=0.0,
        timezone="America/New_York",
        entry_start="09:30:00",
        entry_end="15:59:59",
        allow_overnight=True,
    )
    return TraderSelection(
        trader_id="aapl_reversal",
        symbol="AAPL",
        strategy_name="aapl_reversal_test",
        strategy_type="renko_reversal",
        strategy_config=dict(strategy.__dict__),
        production_metadata={},
        production_config_hash="config-hash",
    )


def research_config(root: Path, *, thresholds=(None, Decimal("0.00"))) -> ReversalVolumeResearchConfig:
    return ReversalVolumeResearchConfig(
        research_id="rvz-test",
        config_path=str(root / "traders.json"),
        session_dates=(SESSIONS[0],),
        source_root=str(root / "bronze"),
        output_directory=str(root / "out"),
        prepared_output_root=str(root / "prepared"),
        candidate_pcts=(Decimal("1.00"),),
        volume_windows_seconds=(5,),
        volume_thresholds=thresholds,
        minimum_volume_baseline_sessions=2,
        minimum_closed_trades=1,
        validate_market_data_coverage=False,
    )


def spec_for(root: Path, *, threshold: Decimal | None) -> ReversalVolumeCandidateSpec:
    config = research_config(root, thresholds=(None,) if threshold is None else (None, threshold))
    base = build_research_candidate_specs(
        config=config,
        trader=trader_selection(),
        prepared=prepared_stub(root),
        observations=observations([100, 101, 99]),
    )
    label = volume_threshold_label(threshold)
    return next(item for item in base if item.volume_threshold_label == label)


def volume_lookup_for(obs: tuple[CanonicalObservation, ...], z_by_index: dict[int, Decimal]) -> dict[int, dict[datetime, VolumeZSnapshot]]:
    return {
        5: {
            item.second_bucket: VolumeZSnapshot(
                timestamp=item.second_bucket,
                symbol="AAPL",
                window_seconds=5,
                observed_volume=Decimal("100"),
                baseline_sample_count=2,
                baseline_mean_volume=Decimal("50"),
                baseline_std_volume=Decimal("25"),
                z_score=z_by_index.get(index, Decimal("1")),
                status="OK",
            )
            for index, item in enumerate(obs)
        }
    }


def semantic_fills(fills: list[dict]) -> list[tuple]:
    return [
        (
            fill["timestamp"],
            fill["side"],
            fill["leg"],
            Decimal(str(fill["qty"])),
            Decimal(str(fill["price"])),
            fill["reason"],
        )
        for fill in fills
    ]


def semantic_fill_objects(fills) -> list[tuple]:
    return [
        (
            fill.timestamp.isoformat(),
            fill.side,
            fill.leg,
            fill.qty,
            fill.price,
            fill.reason,
        )
        for fill in fills
    ]


class ReversalVolumeResearchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.config_path = self.root / "traders.json"
        self.config_path.write_text(json.dumps(config_payload()), encoding="utf-8")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_reversal_volume_brick_grid_has_requested_96_values(self) -> None:
        values = brick_grid(Decimal("0.10"), Decimal("2.00"), Decimal("0.02"))
        self.assertEqual(len(values), 96)
        self.assertEqual(values[0], Decimal("0.10"))
        self.assertEqual(values[-1], Decimal("2.00"))
        self.assertEqual(values[1], Decimal("0.12"))

    def test_volume_z_requires_minimum_prior_history_without_default_substitution(self) -> None:
        timestamp = datetime(2026, 8, 12, 13, 30, tzinfo=timezone.utc)
        insufficient = _volume_z_snapshot(
            symbol="AAPL",
            timestamp=timestamp,
            window_seconds=5,
            observed_volume=Decimal("100"),
            history=[Decimal("50")],
            minimum_baseline_sessions=2,
        )
        self.assertEqual(insufficient.status, "INSUFFICIENT_BASELINE_HISTORY")
        self.assertIsNone(insufficient.z_score)
        self.assertIsNone(insufficient.baseline_mean_volume)
        self.assertIsNone(insufficient.baseline_std_volume)

        computed = _volume_z_snapshot(
            symbol="AAPL",
            timestamp=timestamp,
            window_seconds=5,
            observed_volume=Decimal("25"),
            history=[Decimal("10"), Decimal("20")],
            minimum_baseline_sessions=2,
        )
        self.assertEqual(computed.status, "OK")
        self.assertEqual(computed.z_score, Decimal("2"))

    def test_volume_z_lookup_is_causal_and_marks_early_sessions_insufficient(self) -> None:
        for index, session in enumerate(SESSIONS):
            prices = [100, 101, 99, 101]
            sizes = [10 + index * 10, 20 + index * 10, 30 + index * 10, 40 + index * 10]
            write_jsonl(self.root / "bronze" / "AAPL" / session.isoformat() / "sip_trades.jsonl", source_rows(session, prices=prices, sizes=sizes))
        prepared = prepare_source(
            symbol="AAPL",
            trader_id="aapl_reversal",
            session_dates=SESSIONS,
            source_root=str(self.root / "bronze"),
            output_root=str(self.root / "prepared"),
            run_id="prep",
        )
        obs = load_prepared_observations(prepared.observations_path)
        lookup = build_volume_z_lookup(
            symbol="AAPL",
            evaluation_sessions=SESSIONS,
            baseline_sessions=tuple(),
            observations=obs,
            source_root=str(self.root / "bronze"),
            volume_windows_seconds=(5,),
            minimum_baseline_sessions=2,
        )
        by_session = {}
        for timestamp, snapshot in lookup[5].items():
            by_session.setdefault(timestamp.astimezone(pd.Timestamp.now(tz="America/New_York").tz).date(), []).append(snapshot)
        first_two = [snapshot for timestamp, snapshot in lookup[5].items() if timestamp.astimezone(pd.Timestamp.now(tz="America/New_York").tz).date() in SESSIONS[:2]]
        third = [snapshot for timestamp, snapshot in lookup[5].items() if timestamp.astimezone(pd.Timestamp.now(tz="America/New_York").tz).date() == SESSIONS[2]]
        self.assertTrue(first_two)
        self.assertTrue(all(snapshot.status == "INSUFFICIENT_BASELINE_HISTORY" for snapshot in first_two))
        self.assertTrue(third)
        self.assertTrue(all(snapshot.baseline_sample_count == 2 for snapshot in third))

    def test_no_filter_accepts_all_raw_e1_opportunities(self) -> None:
        obs = observations([100, 101, 99, 101, 99, 101])
        config = research_config(self.root, thresholds=(None,))
        result = run_research_candidate(
            config=config,
            spec=spec_for(self.root, threshold=None),
            prepared=prepared_stub(self.root),
            observations=obs,
            volume_lookup={},
        )
        self.assertEqual(result.row["volume_threshold_label"], NO_FILTER_LABEL)
        self.assertEqual(result.row["raw_e1_opportunities"], result.row["accepted_e1_opportunities"])
        self.assertEqual(result.row["rejected_e1_opportunities"], 0)
        self.assertEqual(result.row["strategy_signals"], result.row["raw_e1_opportunities"])

    def test_rejected_entry_restoration_matches_pre_mutation_gate_for_later_signals(self) -> None:
        obs = observations([100, 101, 99, 100, 101, 99, 101, 99])
        reject_timestamp = obs[2].second_bucket
        config = research_config(self.root, thresholds=(None, Decimal("0.00")))
        filtered = run_research_candidate(
            config=config,
            spec=spec_for(self.root, threshold=Decimal("0.00")),
            prepared=prepared_stub(self.root),
            observations=obs,
            volume_lookup=volume_lookup_for(obs, {2: Decimal("-1")}),
        )

        class GateBeforeMutationStrategy(RenkoStrategyEngine):
            def _on_brick_reversal(self, brick):
                if pd.Timestamp(brick["timestamp"]).to_pydatetime().astimezone(timezone.utc) == reject_timestamp:
                    return []
                return super()._on_brick_reversal(brick)

        with mock.patch("optimization.reversal_volume_research.RenkoStrategyEngine", GateBeforeMutationStrategy):
            reference = run_research_candidate(
                config=config,
                spec=spec_for(self.root, threshold=None),
                prepared=prepared_stub(self.root),
                observations=obs,
                volume_lookup={},
            )

        rejected = [item for item in filtered.opportunities if item.get("accepted") is False]
        self.assertEqual(len(rejected), 1)
        self.assertEqual(rejected[0]["timestamp"], pd.Timestamp(reject_timestamp).isoformat())
        self.assertEqual(filtered.final_strategy_snapshot["renko"], reference.final_strategy_snapshot["renko"])
        self.assertEqual(
            [(signal.timestamp.isoformat(), signal.side, signal.broker_side, signal.reason) for signal in filtered.signals],
            [(signal.timestamp.isoformat(), signal.side, signal.broker_side, signal.reason) for signal in reference.signals],
        )
        self.assertEqual(semantic_fill_objects(filtered.fills), semantic_fill_objects(reference.fills))
        self.assertEqual(filtered.final_accounting_snapshot.position.side, reference.final_accounting_snapshot.position.side)
        self.assertEqual(filtered.final_accounting_snapshot.position.qty, reference.final_accounting_snapshot.position.qty)

    def test_no_filter_control_matches_canonical_reversal_optimizer(self) -> None:
        session = SESSIONS[0]
        write_jsonl(
            self.root / "bronze" / "AAPL" / session.isoformat() / "sip_trades.jsonl",
            source_rows(session, prices=[100, 101, 99, 101, 99, 101, 99, 101]),
        )
        prepared = prepare_source(
            symbol="AAPL",
            trader_id="aapl_reversal",
            session_dates=(session,),
            source_root=str(self.root / "bronze"),
            output_root=str(self.root / "prepared-canonical"),
            run_id="prep",
        )
        obs = load_prepared_observations(prepared.observations_path)

        optimizer_config = OptimizationConfig(
            optimization_id="canonical-parity",
            production_config_path=str(self.config_path),
            session_dates=(session,),
            source_root=str(self.root / "bronze"),
            prepared_output_root=str(self.root / "prepared-canonical"),
            output_directory=str(self.root / "canonical-out"),
            selected_trader_ids=("aapl_reversal",),
            candidate_pcts=(Decimal("1.00"),),
            include_current_production=False,
            minimum_closed_trades=1,
        )
        optimizer_trader = load_enabled_renko_traders(str(self.config_path), ("aapl_reversal",))[0]
        optimizer_spec = build_optimizer_candidate_specs(
            config=optimizer_config,
            trader=optimizer_trader,
            prepared=prepared,
            observations=obs,
        )[0]
        canonical_dir = self.root / "canonical-selected"
        canonical_row = _execute_candidate(
            config=optimizer_config,
            spec=optimizer_spec,
            prepared=prepared,
            observations=obs,
            selected_output_dir=canonical_dir,
        )

        research = run_research_candidate(
            config=research_config(self.root, thresholds=(None,)),
            spec=spec_for(self.root, threshold=None),
            prepared=prepared,
            observations=obs,
            volume_lookup={},
        )
        self.assertEqual(research.row["raw_e1_opportunities"], canonical_row["strategy_signals"])
        self.assertEqual(research.row["accepted_e1_opportunities"], canonical_row["strategy_signals"])
        self.assertEqual(research.row["simulated_fills"], canonical_row["simulated_fills"])
        self.assertEqual(research.row["closed_trades"], canonical_row["closed_trades"])
        self.assertEqual(Decimal(str(research.row["realized_pnl_dollars"])), Decimal(str(canonical_row["realized_pnl_dollars"])))
        self.assertEqual(semantic_fill_objects(research.fills), semantic_fills(read_jsonl(canonical_dir / "fills.jsonl")))

        transitions = read_jsonl(canonical_dir / "position_transitions.jsonl")
        self.assertEqual(
            [(item["action"], item["fill"]["leg"], item["fill"]["side"]) for item in transitions],
            [(item.action, item.fill.leg, item.fill.side) for item in research.position_transitions],
        )
        canonical_final_position = transitions[-1]["after"]
        research_final_position = research.final_accounting_snapshot.position
        self.assertEqual(canonical_final_position["side"], research_final_position.side)
        self.assertEqual(Decimal(str(canonical_final_position["qty"])), research_final_position.qty)
        self.assertEqual(
            Decimal(str(canonical_final_position["avg_entry_price"])),
            research_final_position.avg_entry_price,
        )

    def test_full_runner_writes_research_outputs_without_broker_or_production_mutation(self) -> None:
        session = SESSIONS[0]
        write_jsonl(
            self.root / "bronze" / "AAPL" / session.isoformat() / "sip_trades.jsonl",
            source_rows(session, prices=[100, 101, 99, 101, 99, 101]),
        )
        result = run_reversal_volume_research(
            research_config(self.root, thresholds=(None, Decimal("0.00")))
        )
        out = Path(result.output_directory)
        self.assertTrue((out / "manifest.json").exists())
        self.assertTrue((out / "candidates.csv").exists())
        self.assertTrue((out / "opportunities.jsonl").exists())
        manifest = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
        self.assertFalse(manifest["production_live_behavior_modified"])
        self.assertFalse(manifest["broker_order_endpoints_enabled"])
        self.assertFalse(manifest["production_config_mutation"])

        text = Path("optimization/reversal_volume_research.py").read_text(encoding="utf-8")
        for forbidden in ("TradingClient", "submit_order", "/v2/orders", "AlpacaExecutionEngine", "execution_engine", "alpaca_trader"):
            self.assertNotIn(forbidden, text)


if __name__ == "__main__":
    unittest.main()
