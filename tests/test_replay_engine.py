from __future__ import annotations

import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

import pandas as pd

from replay.bronze import build_bronze_metadata, write_bronze_metadata
from replay.config_hash import replay_fingerprint, stable_config_hash
from replay.engine import run_replay
from replay.execution import NextObservationExecution
from replay.historical_source import read_historical_source
from replay.models import ReplayConfig
from replay.parity import compare_logic_parity
from replay.production_config import load_production_renko_config
from replay.start_state import load_replay_start_state
from shared.config.trader_config import (
    TraderConfigError,
    audit_renko_brick_config,
    build_strategy_config as canonical_build_strategy_config,
)
from silver.accounting import PositionState
from silver.market import CanonicalObservation
from silver.market import CanonicalOneSecondSipSampler
from strategy_engine import RenkoStrategyConfig, RenkoStrategyEngine
from trading_events import OrderIntent, Signal


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")


def write_csv(path: Path, rows: list[dict]) -> None:
    keys = list(rows[0].keys())
    with path.open("w", encoding="utf-8") as handle:
        handle.write(",".join(keys) + "\n")
        for item in rows:
            handle.write(",".join(str(item.get(key, "")) for key in keys) + "\n")


def row(timestamp: str, price: float, trade_id: str, sequence: int, symbol: str = "TSLA") -> dict:
    return {
        "timestamp": timestamp,
        "symbol": symbol,
        "price": price,
        "size": 1,
        "exchange": "unit",
        "tape": "C",
        "trade_id": trade_id,
        "conditions": ["@"],
        "source_sequence": sequence,
    }


def strategy_dict(**overrides):
    config = RenkoStrategyConfig(
        trader_id="replay_trader",
        symbol="TSLA",
        brick_size=1.0,
        entry_bricks=1,
        strategy_type="renko_reversal",
        strategy_name="replay_test_e1",
        qty=1.0,
        renko_anchor_mode="grid",
        renko_anchor_price=0.0,
        entry_start="09:31:00",
        entry_end="15:59:59",
        allow_overnight=True,
    )
    payload = dict(config.__dict__)
    payload.update(overrides)
    return payload


def observation(second: str, price: str = "100", sequence: int = 1) -> CanonicalObservation:
    return CanonicalObservation(
        observation_id=f"obs-{sequence}",
        run_id="test-run",
        symbol="TSLA",
        second_bucket=second,
        emitted_sequence=sequence,
        close_price=Decimal(price),
        source_eligible_trade_id=f"eligible-{sequence}",
        source_sip_timestamp=second,
        source_alpaca_trade_id=f"trade-{sequence}",
    )


def signal_and_intent(*, side: str, broker_side: str, reason: str) -> tuple[Signal, OrderIntent]:
    signal = Signal(
        signal_id=f"signal-{reason}",
        signal_type="exit",
        timestamp=pd.Timestamp("2026-07-20T13:31:00Z"),
        symbol="TSLA",
        side=side,
        broker_side=broker_side,
        price=100,
        reason=reason,
        strategy="test",
        trader_id="replay_trader",
    )
    intent = OrderIntent(
        client_order_id=f"intent-{reason}",
        timestamp=signal.timestamp,
        symbol="TSLA",
        side=broker_side,
        signal_type="exit",
        order_type="market",
        strategy="test",
        reason=reason,
        close_position=True,
        trader_id="replay_trader",
    )
    return signal, intent


def replay_config(source: Path, output: Path, *, run_id: str = "test-replay", output_level: str = "SUMMARY", **kwargs) -> ReplayConfig:
    strategy = kwargs.pop("strategy_config", strategy_dict())
    brick_mode = kwargs.pop("brick_mode", "BRICK_DOLLARS")
    brick_size_dollars = kwargs.pop("brick_size_dollars", strategy["brick_size"] if brick_mode == "BRICK_DOLLARS" else None)
    brick_pct = kwargs.pop("brick_size_ltp_percent", None)
    start = kwargs.pop("start", "2026-07-20T13:30:00Z")
    end = kwargs.pop("end", "2026-07-20T13:31:10Z")
    return ReplayConfig(
        run_id=run_id,
        trader_id="replay_trader",
        symbol="TSLA",
        start=start,
        end=end,
        timezone="UTC",
        session_mode="configured",
        strategy_config=strategy,
        initial_capital="10000",
        brick_mode=brick_mode,
        brick_size_dollars=brick_size_dollars,
        brick_size_ltp_percent=brick_pct,
        execution_model="NEXT_CANONICAL_OBSERVATION_V1",
        source_data_path=str(source),
        output_directory=str(output),
        output_level=output_level,
        **kwargs,
    )


class ReplayEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def source(self, rows: list[dict]) -> Path:
        path = self.root / "source.jsonl"
        write_jsonl(path, rows)
        return path

    def signal_rows(self) -> list[dict]:
        return [
            row("2026-07-20T13:31:00.100Z", 100, "t0", 0),
            row("2026-07-20T13:31:01.100Z", 101, "t1", 1),
            row("2026-07-20T13:31:02.100Z", 99, "t2", 2),
            row("2026-07-20T13:31:03.100Z", 101, "t3", 3),
            row("2026-07-20T13:31:04.100Z", 102, "t4", 4),
        ]

    def run_signal_replay(self, *, output_level: str = "SUMMARY", run_id: str = "test-replay"):
        source = self.source(self.signal_rows())
        return run_replay(replay_config(source, self.root / "out", output_level=output_level, run_id=run_id))

    def test_deterministic_replay_of_same_source_twice(self) -> None:
        first = self.run_signal_replay(run_id="deterministic")
        second = self.run_signal_replay(run_id="deterministic")
        self.assertEqual(first.metrics, second.metrics)
        self.assertEqual(first.manifest["configuration_hash"], second.manifest["configuration_hash"])
        self.assertEqual([obs.observation_id for obs in first.ledgers.canonical_observations], [obs.observation_id for obs in second.ledgers.canonical_observations])

    def test_configuration_hash_excludes_run_id(self) -> None:
        source = self.source(self.signal_rows())
        first = replay_config(source, self.root / "out-a", run_id="run-a")
        second = replay_config(source, self.root / "out-b", run_id="run-b")
        self.assertEqual(stable_config_hash(first), stable_config_hash(second))

    def test_same_semantic_configuration_yields_identical_hash(self) -> None:
        source = self.source(self.signal_rows())
        first = run_replay(replay_config(source, self.root / "out-a", run_id="run-a"))
        second = run_replay(replay_config(source, self.root / "out-b", run_id="run-b"))
        self.assertEqual(first.manifest["configuration_hash"], second.manifest["configuration_hash"])

    def test_different_strategy_affecting_config_changes_hash(self) -> None:
        source = self.source(self.signal_rows())
        first = replay_config(source, self.root / "out-a")
        second = replay_config(source, self.root / "out-b", strategy_config=strategy_dict(entry_bricks=2))
        self.assertNotEqual(stable_config_hash(first), stable_config_hash(second))

    def test_source_hash_changes_when_source_content_changes(self) -> None:
        first_source = self.source(self.signal_rows())
        second_source = self.root / "source2.jsonl"
        write_jsonl(second_source, self.signal_rows() + [row("2026-07-20T13:31:05.100Z", 103, "extra", 5)])
        first = run_replay(replay_config(first_source, self.root / "out-a"))
        second = run_replay(replay_config(second_source, self.root / "out-b"))
        self.assertNotEqual(first.manifest["source_dataset_hash"], second.manifest["source_dataset_hash"])

    def test_replay_fingerprint_is_deterministic(self) -> None:
        source = self.source(self.signal_rows())
        config = replay_config(source, self.root / "out")
        first = replay_fingerprint(configuration_hash=stable_config_hash(config), source_dataset_hash="abc", config=config)
        second = replay_fingerprint(configuration_hash=stable_config_hash(config), source_dataset_hash="abc", config=config)
        self.assertEqual(first, second)

    def test_bronze_metadata_preserves_raw_sip_dataset_facts(self) -> None:
        source = self.source(self.signal_rows())
        config = replay_config(source, self.root / "out")
        metadata = build_bronze_metadata(config=config, source_path=source, date="2026-07-20")
        path = write_bronze_metadata(metadata, self.root / "source.metadata.json")
        payload = json.loads(path.read_text())
        self.assertEqual(payload["feed"], "sip")
        self.assertEqual(payload["row_count"], len(self.signal_rows()))
        self.assertEqual(payload["completion_status"], "complete")

    def test_starting_state_snapshot_loading(self) -> None:
        state_path = self.root / "state.json"
        state_path.write_text(
            json.dumps(
                {
                    "saved_at": "2026-07-20T13:29:59Z",
                    "engine": {
                        "trader_id": "replay_trader",
                        "symbol": "TSLA",
                        "strategy_type": "renko_reversal",
                        "strategy_name": "replay_test_e1",
                        "renko": {
                            "brick_size": 1.0,
                            "last_brick_close": 100,
                            "run_dir": 1,
                            "run_len": 1,
                            "processed_bricks": 1,
                            "anchor_mode": "grid",
                            "anchor_price": 0.0,
                        },
                        "trade": {
                            "position": {
                                "symbol": "TSLA",
                                "side": "long",
                                "entry_time": "2026-07-20T13:29:00Z",
                                "entry_price": 100,
                                "qty": 1,
                            },
                            "force_flat_dates": [],
                        },
                    },
                }
            ),
            encoding="utf-8",
        )
        state = load_replay_start_state(state_path)
        self.assertEqual(state.symbol, "TSLA")
        self.assertEqual(state.renko_state["last_brick_close"], 100)
        self.assertEqual(state.accounting_position.side, "long")
        self.assertEqual(state.completeness, "complete")

    def test_unknown_starting_state_disclosure(self) -> None:
        result = self.run_signal_replay()
        self.assertFalse(result.manifest["starting_state"]["parity_verifiable"])
        self.assertEqual(result.manifest["starting_state"]["start_state_completeness"], "not_supplied")

    def test_observation_parity_exact_match(self) -> None:
        result = self.run_signal_replay(run_id="parity")
        prod = self.root / "observations.jsonl"
        write_jsonl(
            prod,
            [
                {
                    "symbol": obs.symbol,
                    "second_bucket": obs.second_bucket.isoformat(),
                    "close_price": str(obs.close_price),
                    "source_record_id": obs.source_record_id,
                }
                for obs in result.ledgers.canonical_observations
            ],
        )
        report = compare_logic_parity(
            parity_run_id="obs-parity",
            replay=result,
            output_directory=self.root / "parity",
            production_observations_path=prod,
        )
        self.assertEqual(report.observations.exact_match_rate, 100.0)

    def test_observation_missing_extra_mismatch(self) -> None:
        result = self.run_signal_replay(run_id="parity")
        prod = self.root / "observations.jsonl"
        write_jsonl(prod, [])
        report = compare_logic_parity(
            parity_run_id="obs-mismatch",
            replay=result,
            output_directory=self.root / "parity",
            production_observations_path=prod,
        )
        self.assertEqual(report.observations.extra_replay_count, len(result.ledgers.canonical_observations))

    def test_renko_brick_parity_exact_match(self) -> None:
        result = self.run_signal_replay(run_id="parity")
        prod = self.root / "bricks.jsonl"
        write_jsonl(
            prod,
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
            parity_run_id="brick-parity",
            replay=result,
            output_directory=self.root / "parity",
            production_renko_path=prod,
        )
        self.assertEqual(report.renko_bricks.exact_match_rate, 100.0)

    def test_brick_direction_run_length_mismatch(self) -> None:
        result = self.run_signal_replay(run_id="parity")
        prod = self.root / "bricks.jsonl"
        rows = [
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
        ]
        rows[0]["direction"] = -rows[0]["direction"]
        write_jsonl(prod, rows)
        report = compare_logic_parity(
            parity_run_id="brick-mismatch",
            replay=result,
            output_directory=self.root / "parity",
            production_renko_path=prod,
        )
        self.assertIn("economic_field_mismatch", report.renko_bricks.mismatch_categories)

    def test_strategy_signal_parity_ignores_random_uuids(self) -> None:
        result = self.run_signal_replay(run_id="parity")
        prod = self.root / "signals.jsonl"
        write_jsonl(
            prod,
            [
                {
                    "signal_id": "random-production-id",
                    "timestamp": signal.timestamp.isoformat(),
                    "symbol": signal.symbol,
                    "side": signal.side,
                    "reason": signal.reason,
                    "strategy": signal.strategy,
                    "trader_id": signal.trader_id,
                    "signal_type": signal.signal_type,
                }
                for signal in result.ledgers.strategy_signals
            ],
        )
        report = compare_logic_parity(
            parity_run_id="signal-parity",
            replay=result,
            output_directory=self.root / "parity",
            production_signals_path=prod,
        )
        self.assertEqual(report.strategy_signals.exact_match_rate, 100.0)

    def test_signal_reason_side_mismatch_detected(self) -> None:
        result = self.run_signal_replay(run_id="parity")
        prod = self.root / "signals.jsonl"
        signal = result.ledgers.strategy_signals[0]
        write_jsonl(
            prod,
            [
                {
                    "timestamp": signal.timestamp.isoformat(),
                    "symbol": signal.symbol,
                    "side": "short" if signal.side == "long" else "long",
                    "reason": signal.reason,
                    "strategy": signal.strategy,
                    "trader_id": signal.trader_id,
                    "signal_type": signal.signal_type,
                }
            ],
        )
        report = compare_logic_parity(
            parity_run_id="signal-mismatch",
            replay=result,
            output_directory=self.root / "parity",
            production_signals_path=prod,
        )
        self.assertIn("field_mismatch", report.strategy_signals.mismatch_categories)

    def test_position_direction_transition_comparison(self) -> None:
        result = self.run_signal_replay(run_id="parity")
        prod = self.root / "positions.jsonl"
        write_jsonl(
            prod,
            [
                {
                    "timestamp": transition.timestamp.isoformat(),
                    "symbol": transition.symbol,
                    "before": {"side": transition.before.side},
                    "after": {"side": transition.after.side},
                }
                for transition in result.ledgers.position_transitions
            ],
        )
        report = compare_logic_parity(
            parity_run_id="position-parity",
            replay=result,
            output_directory=self.root / "parity",
            production_position_transitions_path=prod,
        )
        self.assertEqual(report.position_direction_transitions.exact_match_rate, 100.0)

    def test_first_mismatch_reporting(self) -> None:
        result = self.run_signal_replay(run_id="parity")
        prod = self.root / "bricks.jsonl"
        brick = result.ledgers.renko_bricks[0]
        write_jsonl(
            prod,
            [
                {
                    "timestamp": brick.timestamp.isoformat(),
                    "symbol": brick.symbol,
                    "open": brick.open,
                    "close": brick.close + 1,
                    "direction": brick.direction,
                    "run_len": brick.run_len,
                    "brick_size": brick.brick_size,
                }
            ],
        )
        report = compare_logic_parity(
            parity_run_id="first-mismatch",
            replay=result,
            output_directory=self.root / "parity",
            production_renko_path=prod,
        )
        self.assertEqual(report.renko_bricks.first_mismatch_sequence, 1)
        self.assertEqual(report.renko_bricks.first_mismatch_timestamp, brick.timestamp.isoformat())

    def test_deterministic_parity_report(self) -> None:
        result = self.run_signal_replay(run_id="parity")
        first = compare_logic_parity(parity_run_id="parity-a", replay=result, output_directory=self.root / "parity")
        second = compare_logic_parity(parity_run_id="parity-a", replay=result, output_directory=self.root / "parity")
        self.assertEqual(first, second)

    def test_raw_data_is_referenced_not_copied_by_parity_output(self) -> None:
        result = self.run_signal_replay(run_id="parity")
        compare_logic_parity(parity_run_id="raw-reference", replay=result, output_directory=self.root / "parity")
        files = [path.name for path in (self.root / "parity" / "raw-reference").iterdir()]
        self.assertNotIn("source.jsonl", files)
        self.assertIn("manifest.json", files)

    def test_classification_cannot_exceed_available_evidence(self) -> None:
        result = self.run_signal_replay(run_id="parity")
        report = compare_logic_parity(parity_run_id="limited", replay=result, output_directory=self.root / "parity")
        self.assertEqual(report.classification, "FULL_SESSION_RESEARCH")
        self.assertEqual(report.observations.evidence_status, "production_evidence_missing")

    def test_dollar_brick_mode(self) -> None:
        result = self.run_signal_replay()
        brick = result.manifest["brick_configuration"]
        self.assertEqual(brick["brick_mode"], "BRICK_DOLLARS")
        self.assertEqual(brick["effective_brick_size_dollars"], 1)
        self.assertEqual(result.ledgers.renko_bricks[0].brick_size, 1.0)

    def test_percentage_brick_mode(self) -> None:
        source = self.source(self.signal_rows())
        result = run_replay(
            replay_config(
                source,
                self.root / "out",
                brick_mode="BRICK_PERCENT",
                brick_size_ltp_percent="2",
                strategy_config=strategy_dict(brick_size=999),
            )
        )
        brick = result.manifest["brick_configuration"]
        self.assertEqual(brick["brick_mode"], "BRICK_PERCENT")
        self.assertEqual(brick["reference_price"], 100)
        self.assertEqual(brick["reference_timestamp"], "2026-07-20T13:31:00+00:00")
        self.assertEqual(brick["effective_brick_size_dollars"], 2)

    def test_production_config_brick_mode(self) -> None:
        source = self.source(self.signal_rows())
        result = run_replay(
            replay_config(
                source,
                self.root / "out",
                brick_mode="PRODUCTION_CONFIG",
                strategy_config=strategy_dict(brick_size=3),
            )
        )
        brick = result.manifest["brick_configuration"]
        self.assertEqual(brick["brick_mode"], "PRODUCTION_CONFIG")
        self.assertEqual(brick["effective_brick_size_dollars"], 3)

    def test_contradictory_brick_arguments_rejected(self) -> None:
        source = self.source(self.signal_rows())
        with self.assertRaises(ValueError):
            replay_config(source, self.root / "out", brick_mode="BRICK_DOLLARS", brick_size_dollars="1", brick_size_ltp_percent="2")

    def test_effective_brick_size_passed_to_production_renko_strategy_engine(self) -> None:
        result = self.run_signal_replay()
        self.assertEqual(result.ledgers.renko_bricks[0].brick_size, result.manifest["brick_configuration"]["effective_brick_size_dollars"])

    def test_reference_ltp_and_timestamp_persisted(self) -> None:
        source = self.source(self.signal_rows())
        result = run_replay(replay_config(source, self.root / "out", brick_mode="BRICK_PERCENT", brick_size_ltp_percent="2"))
        brick = result.manifest["brick_configuration"]
        self.assertEqual(brick["reference_price"], 100)
        self.assertEqual(brick["reference_timestamp"], "2026-07-20T13:31:00+00:00")
        self.assertEqual(brick["reference_source"], "first_canonical_observation")

    def test_metric_field_units_are_explicit(self) -> None:
        result = self.run_signal_replay()
        self.assertIn("initial_capital_dollars", result.metrics)
        self.assertIn("buy_hold_return_pct", result.metrics)
        self.assertIn("avg_trade_pnl_dollars", result.metrics)
        self.assertIn("average_trade_pnl_dollars", result.metrics)
        self.assertNotIn("initial_capital", result.metrics)
        self.assertNotIn("buy_and_hold_return", result.metrics)

    def test_exact_canonical_observation_reuse(self) -> None:
        source = self.source(self.signal_rows())
        config = replay_config(source, self.root / "out", run_id="obs-reuse")
        result = run_replay(config)
        source_result = read_historical_source(config)
        sampler = CanonicalOneSecondSipSampler(run_id="obs-reuse")
        expected = []
        for trade in source_result.trades:
            expected.extend(sampler.process_trade(trade).observations)
        self.assertEqual(result.ledgers.canonical_observations, expected)

    def test_existing_production_renko_strategy_engine_is_instantiated(self) -> None:
        result = self.run_signal_replay()
        self.assertGreaterEqual(len(result.ledgers.renko_bricks), 1)
        self.assertIs(RenkoStrategyEngine.__module__, "strategy_engine")

    def test_no_alternative_renko_strategy_or_pnl_implementation_exists_in_replay_package(self) -> None:
        replay_text = "\n".join(path.read_text(encoding="utf-8") for path in Path("replay").glob("*.py"))
        self.assertNotIn("update_renko_build_state(", replay_text)
        self.assertNotIn("class Renko", replay_text)
        self.assertNotIn("class Strategy", replay_text)
        self.assertNotIn("realized = (", replay_text)
        self.assertIn("from strategy_engine import RenkoStrategyConfig, RenkoStrategyEngine", replay_text)
        self.assertIn("from silver.accounting import AccountingLedger", replay_text)

    def test_no_same_observation_fill_and_next_observation_fill(self) -> None:
        result = self.run_signal_replay()
        signal_ts = result.ledgers.strategy_signals[0].timestamp.to_pydatetime()
        first_fill = result.ledgers.simulated_fills[0]
        self.assertGreater(first_fill.timestamp, signal_ts)
        self.assertEqual(str(first_fill.price), "99")

    def test_reversal_close_and_open_fills_remain_separate_and_ordered(self) -> None:
        result = self.run_signal_replay()
        reversal = [fill for fill in result.ledgers.simulated_fills if fill.order_id == result.ledgers.simulated_orders[1].order_id]
        self.assertEqual([fill.leg for fill in reversal], ["close", "open"])
        self.assertEqual([fill.side for fill in reversal], ["sell", "sell_short"])

    def test_close_position_long_tp_uses_actual_accounting_quantity(self) -> None:
        execution = NextObservationExecution(run_id="close-long-tp", trader_id="replay_trader")
        sig, intent = signal_and_intent(side="long", broker_side="sell", reason="take_profit_2_bricks")
        position = PositionState(symbol="TSLA", side="long", qty=Decimal("7"), avg_entry_price=Decimal("100"))
        order = execution.queue(signal=sig, intent=intent, observation=observation("2026-07-20T13:31:00Z", "100"), position=position)
        bundles = execution.fill_pending(observation=observation("2026-07-20T13:31:01Z", "102", 2), position=position)
        self.assertEqual(order.intent.qty, 7)
        self.assertEqual(order.intent.target_qty, 0)
        self.assertEqual(len(bundles), 1)
        fill = bundles[0].fills[0]
        self.assertEqual(fill.side, "sell")
        self.assertEqual(fill.leg, "close")
        self.assertEqual(fill.qty, Decimal("7"))
        self.assertEqual(fill.reason, "take_profit_2_bricks")

    def test_close_position_long_sl_uses_actual_accounting_quantity_not_configured_target(self) -> None:
        execution = NextObservationExecution(run_id="close-long-sl", trader_id="replay_trader")
        sig, intent = signal_and_intent(side="long", broker_side="sell", reason="stop_loss_2_bricks")
        intent = OrderIntent(
            **{
                **intent.__dict__,
                "qty": 48,
                "target_qty": 48,
            }
        )
        position = PositionState(symbol="TSLA", side="long", qty=Decimal("11"), avg_entry_price=Decimal("100"))

        order = execution.queue(signal=sig, intent=intent, observation=observation("2026-07-20T13:31:00Z", "100"), position=position)

        self.assertIsNotNone(order)
        self.assertEqual(order.intent.qty, 11)
        self.assertEqual(order.intent.target_qty, 0)

    def test_close_position_short_sl_uses_actual_accounting_quantity(self) -> None:
        execution = NextObservationExecution(run_id="close-short-sl", trader_id="replay_trader")
        sig, intent = signal_and_intent(side="short", broker_side="buy", reason="stop_loss_3_bricks")
        position = PositionState(symbol="TSLA", side="short", qty=Decimal("5"), avg_entry_price=Decimal("100"))
        execution.queue(signal=sig, intent=intent, observation=observation("2026-07-20T13:31:00Z", "100"), position=position)
        bundles = execution.fill_pending(observation=observation("2026-07-20T13:31:01Z", "103", 2), position=position)
        fill = bundles[0].fills[0]
        self.assertEqual(fill.side, "buy")
        self.assertEqual(fill.leg, "close")
        self.assertEqual(fill.qty, Decimal("5"))
        self.assertEqual(fill.reason, "stop_loss_3_bricks")

    def test_close_position_short_tp_uses_actual_accounting_quantity(self) -> None:
        execution = NextObservationExecution(run_id="close-short-tp", trader_id="replay_trader")
        sig, intent = signal_and_intent(side="short", broker_side="buy", reason="take_profit_1_bricks")
        position = PositionState(symbol="TSLA", side="short", qty=Decimal("9"), avg_entry_price=Decimal("100"))
        execution.queue(signal=sig, intent=intent, observation=observation("2026-07-20T13:31:00Z", "100"), position=position)

        bundles = execution.fill_pending(observation=observation("2026-07-20T13:31:01Z", "99", 2), position=position)

        fill = bundles[0].fills[0]
        self.assertEqual(fill.side, "buy")
        self.assertEqual(fill.leg, "close")
        self.assertEqual(fill.qty, Decimal("9"))
        self.assertEqual(fill.reason, "take_profit_1_bricks")

    def test_stale_flat_close_does_not_queue_zero_quantity_order(self) -> None:
        execution = NextObservationExecution(run_id="flat-close", trader_id="replay_trader")
        sig, intent = signal_and_intent(side="long", broker_side="sell", reason="take_profit_1_bricks")

        order = execution.queue(
            signal=sig,
            intent=intent,
            observation=observation("2026-07-20T13:31:00Z", "100"),
            position=PositionState.flat("TSLA"),
        )

        self.assertIsNone(order)
        self.assertEqual(execution.pending_orders, tuple())

    def test_close_position_exit_does_not_fill_on_same_observation(self) -> None:
        execution = NextObservationExecution(run_id="same-obs-close", trader_id="replay_trader")
        obs = observation("2026-07-20T13:31:00Z", "100")
        sig, intent = signal_and_intent(side="long", broker_side="sell", reason="take_profit_1_bricks")
        position = PositionState(symbol="TSLA", side="long", qty=Decimal("3"), avg_entry_price=Decimal("100"))
        execution.queue(signal=sig, intent=intent, observation=obs, position=position)
        self.assertEqual(execution.fill_pending(observation=obs, position=position), tuple())

    def test_fills_feed_canonical_accounting(self) -> None:
        result = self.run_signal_replay()
        self.assertEqual(len(result.ledgers.position_transitions), len(result.ledgers.simulated_fills))
        self.assertEqual(result.metrics["closed_trades"], len(result.ledgers.closed_trades))

    def test_final_open_position_is_marked_to_market(self) -> None:
        result = self.run_signal_replay()
        position = result.ledgers.accounting_snapshots[-1].position
        final_price = result.ledgers.canonical_observations[-1].close_price
        if position.side == "long":
            expected = (final_price - position.avg_entry_price) * position.qty
        elif position.side == "short":
            expected = (position.avg_entry_price - final_price) * position.qty
        else:
            expected = Decimal("0")
        self.assertEqual(Decimal(str(result.metrics["unrealized_pnl_dollars"])), expected)
        if expected < 0:
            self.assertEqual(Decimal(str(result.metrics["max_drawdown_dollars"])), abs(expected))

    def test_pending_end_of_window_orders_remain_unfilled(self) -> None:
        result = self.run_signal_replay()
        self.assertEqual(len(result.pending_orders), 1)
        self.assertEqual(result.manifest["pending_order_count"], 1)

    def test_unknown_starting_basis_remains_unknown(self) -> None:
        source = self.source([row("2026-07-20T13:30:00.100Z", 100, "t0", 0), row("2026-07-20T13:30:01.100Z", 100, "t1", 1)])
        starting = PositionState(symbol="TSLA", side="long", qty="1", avg_entry_price=None, basis_known=False)
        result = run_replay(replay_config(source, self.root / "out", strategy_config=strategy_dict(brick_size=1000), initial_accounting_position=starting))
        self.assertFalse(result.ledgers.accounting_snapshots[-1].position.basis_known)

    def test_explicit_known_starting_state(self) -> None:
        source = self.source([row("2026-07-20T13:30:00.100Z", 100, "t0", 0), row("2026-07-20T13:30:01.100Z", 100, "t1", 1)])
        starting = PositionState(symbol="TSLA", side="long", qty="1", avg_entry_price="95", basis_known=True)
        result = run_replay(replay_config(source, self.root / "out", strategy_config=strategy_dict(brick_size=1000), initial_accounting_position=starting))
        self.assertEqual(result.ledgers.accounting_snapshots[-1].position.avg_entry_price, 95)

    def test_buy_and_hold_uses_canonical_first_and_final_observations(self) -> None:
        result = self.run_signal_replay()
        first = result.ledgers.canonical_observations[0].close_price
        final = result.ledgers.canonical_observations[-1].close_price
        expected = (final / first - 1) * 100
        self.assertEqual(result.metrics["benchmark_entry_price"], float(first))
        self.assertEqual(result.metrics["benchmark_exit_price"], float(final))
        self.assertEqual(result.metrics["benchmark_quantity"], float(10000 / first))
        self.assertEqual(result.metrics["buy_hold_return_pct"], float(expected))

    def test_buy_and_hold_dollar_calculation(self) -> None:
        result = self.run_signal_replay()
        first = result.ledgers.canonical_observations[0].close_price
        final = result.ledgers.canonical_observations[-1].close_price
        expected = (final - first) * (10000 / first)
        self.assertAlmostEqual(result.metrics["buy_hold_pnl_dollars"], float(expected))

    def test_strategy_outperformance_metrics(self) -> None:
        result = self.run_signal_replay()
        self.assertAlmostEqual(
            result.metrics["strategy_outperformance_dollars"],
            result.metrics["net_pnl_dollars"] - result.metrics["buy_hold_pnl_dollars"],
        )
        self.assertAlmostEqual(
            result.metrics["strategy_outperformance_percentage_points"],
            result.metrics["strategy_return_pct"] - result.metrics["buy_hold_return_pct"],
        )

    def test_report_formatting_for_dollars_and_percentages(self) -> None:
        result = self.run_signal_replay()
        report = Path(result.output_directory, "report.txt").read_text(encoding="utf-8")
        self.assertIn("Effective brick size: $", report)
        self.assertIn("Buy-and-hold PnL: $", report)
        self.assertIn("Buy-and-hold return: ", report)
        self.assertIn("%", report)

    def test_source_event_count_reconciliation(self) -> None:
        result = self.run_signal_replay()
        counts = result.manifest["counts"]
        self.assertEqual(counts["raw_rows_read"], 5)
        self.assertEqual(counts["source_rows_scanned"], 5)
        self.assertEqual(counts["malformed_rows"], 0)
        self.assertEqual(counts["raw_trades_in_window"], 5)
        self.assertEqual(counts["eligibility_decisions"], counts["eligible_trades"] + counts["rejected_trades"])
        self.assertEqual(counts["canonical_observations"], 4)

    def test_production_config_loads_intended_trader(self) -> None:
        loaded = load_production_renko_config(
            "configs/traders.live.example.json",
            "camatron_tsla_1s_renko_2p31165_e1_reversal_1sh_20260710_live",
        )
        self.assertEqual(loaded.strategy_config.symbol, "TSLA")
        self.assertEqual(loaded.strategy_config.strategy_type, "renko_reversal")
        self.assertEqual(loaded.strategy_config.brick_size, 1.62183)
        self.assertEqual(loaded.metadata["configured_renko_source"], "1s_close")
        self.assertEqual(loaded.brick_audit.sizing_mode, "BRICK_DOLLARS")
        self.assertEqual(loaded.brick_audit.authoritative_field, "strategy.brick_size")

    def test_canonical_builder_matches_fleet_runner_for_every_current_renko_trader(self) -> None:
        from fleet_runner import build_strategy_config as fleet_build_strategy_config

        data = json.loads(Path("configs/traders.live.example.json").read_text(encoding="utf-8"))
        for trader in data["traders"]:
            strategy = trader.get("strategy", {})
            if strategy.get("type") not in {"renko_tp_sl", "renko_psar", "renko_reversal"}:
                continue
            with self.subTest(trader=trader["id"]):
                old = fleet_build_strategy_config(trader)
                new = canonical_build_strategy_config(trader)
                self.assertEqual(old, new)

    def test_replay_production_config_wrapper_calls_canonical_builder(self) -> None:
        text = Path("replay/production_config.py").read_text(encoding="utf-8")
        self.assertIn("from shared.config.trader_config import", text)
        self.assertNotIn("RenkoStrategyConfig(", text)
        self.assertNotIn("strategy[\"brick_size\"]", text)

    def test_builder_imports_no_live_execution_or_credentials(self) -> None:
        text = Path("shared/config/trader_config.py").read_text(encoding="utf-8")
        self.assertNotIn("AlpacaExecutionEngine", text)
        self.assertNotIn("load_alpaca_credentials", text)
        self.assertNotIn("websockets", text)

    def test_fixed_dollar_brick_config(self) -> None:
        trader = {
            "id": "name_says_999pct_but_structured_fields_win",
            "symbol": "TSLA",
            "strategy_name": "2p31165_is_label_only",
            "strategy": {"type": "renko_reversal", "brick_size": 2.5, "entry_bricks": 1},
            "sizing": {"qty": 1},
            "session": {"allow_overnight": True},
            "profile": {"renko_brick_pct": 9.0, "ltp_reference": 500},
            "data": {"renko_source": "1s_close"},
        }
        config = canonical_build_strategy_config(trader)
        audit = audit_renko_brick_config(trader, strategy_config=config)
        self.assertEqual(config.brick_size, 2.5)
        self.assertEqual(audit.sizing_mode, "BRICK_DOLLARS")
        self.assertEqual(audit.authoritative_field, "strategy.brick_size")
        self.assertFalse(audit.values_agree)

    def test_percentage_derived_brick_config(self) -> None:
        trader = {
            "id": "pct_config",
            "symbol": "TSLA",
            "strategy": {"type": "renko_reversal", "brick_sizing_mode": "BRICK_PERCENT", "brick_pct": 2, "reference_price": 100, "entry_bricks": 1},
            "sizing": {"qty": 1},
            "session": {"allow_overnight": True},
            "data": {"renko_source": "1s_close"},
        }
        config = canonical_build_strategy_config(trader)
        audit = audit_renko_brick_config(trader, strategy_config=config)
        self.assertEqual(config.brick_size, 2)
        self.assertEqual(audit.sizing_mode, "BRICK_PERCENT")
        self.assertEqual(audit.authoritative_field, "brick_pct + reference_price")

    def test_percentage_mode_rejects_disagreement_with_explicit_brick_size(self) -> None:
        trader = {
            "id": "bad",
            "symbol": "TSLA",
            "strategy": {"type": "renko_reversal", "brick_sizing_mode": "BRICK_PERCENT", "brick_pct": 2, "reference_price": 100, "brick_size": 2.5, "entry_bricks": 1},
            "sizing": {"qty": 1},
            "session": {"allow_overnight": True},
        }
        config = canonical_build_strategy_config(trader)
        with self.assertRaises(TraderConfigError):
            audit_renko_brick_config(trader, strategy_config=config)

    def test_replay_effective_configuration_equals_production_configuration(self) -> None:
        source = self.source(self.signal_rows())
        loaded = load_production_renko_config(
            "configs/traders.live.example.json",
            "camatron_tsla_1s_renko_2p31165_e1_reversal_1sh_20260710_live",
        )
        result = run_replay(
            replay_config(
                source,
                self.root / "out",
                brick_mode="PRODUCTION_CONFIG",
                strategy_config=dict(loaded.strategy_config.__dict__),
                source_config_path=loaded.config_path,
                production_config={**loaded.metadata, "production_config_hash": loaded.config_hash},
            )
        )
        self.assertEqual(result.manifest["brick_configuration"]["effective_brick_size_dollars"], 1.62183)
        self.assertEqual(result.manifest["strategy_configuration"]["source_config_path"], "configs/traders.live.example.json")
        self.assertEqual(result.manifest["strategy_configuration"]["production_config_hash"], loaded.config_hash)

    def test_duplicate_trade_id_with_identical_fields(self) -> None:
        source = self.source([row("2026-07-20T13:30:00.100Z", 100, "dup", 0), row("2026-07-20T13:30:00.100Z", 100, "dup", 1)])
        config = replay_config(source, self.root / "out")
        result = read_historical_source(config)
        self.assertEqual(result.integrity.duplicate_source_id_records, 0)
        self.assertEqual(result.integrity.duplicate_trade_ids, 1)
        self.assertEqual(result.integrity.duplicate_ids_with_identical_values, 1)
        self.assertEqual(result.integrity.duplicate_ids_with_conflicting_values, 0)
        self.assertNotEqual(result.trades[0].source_record_id, result.trades[1].source_record_id)

    def test_duplicate_trade_id_with_conflicting_fields(self) -> None:
        source = self.source([row("2026-07-20T13:30:00.100Z", 100, "dup", 0), row("2026-07-20T13:30:00.100Z", 101, "dup", 1)])
        config = replay_config(source, self.root / "out")
        result = read_historical_source(config)
        self.assertEqual(result.integrity.duplicate_source_id_records, 0)
        self.assertEqual(result.integrity.duplicate_trade_ids, 1)
        self.assertEqual(result.integrity.duplicate_ids_with_conflicting_values, 1)
        self.assertIn("conflicting_duplicate_source_ids", [diag.diagnostic_type for diag in result.diagnostics])

    def test_legitimate_equal_price_equal_time_distinct_ids_remain_separate(self) -> None:
        source = self.source([row("2026-07-20T13:30:00.100Z", 100, "a", 0), row("2026-07-20T13:30:00.100Z", 100, "b", 1)])
        result = read_historical_source(replay_config(source, self.root / "out"))
        self.assertEqual(result.raw_trades_in_window, 2)
        self.assertEqual(result.integrity.duplicate_source_id_records, 0)
        self.assertEqual(result.integrity.duplicate_trade_ids, 0)

    def test_distinct_broker_ids_with_identical_economic_fields_remain_distinct_source_records(self) -> None:
        source = self.source([row("2026-07-20T13:30:00.100Z", 100, "a", 0), row("2026-07-20T13:30:00.100Z", 100, "b", 1)])
        result = read_historical_source(replay_config(source, self.root / "out"))
        self.assertEqual(len({trade.source_record_id for trade in result.trades}), 2)
        self.assertEqual(result.integrity.duplicate_trade_ids, 0)

    def test_missing_trade_id_fallback_identity(self) -> None:
        source = self.source([{**row("2026-07-20T13:30:00.100Z", 100, "", 0), "trade_id": ""}])
        result = read_historical_source(replay_config(source, self.root / "out"))
        self.assertEqual(result.integrity.rows_using_fallback_identity, 1)
        self.assertTrue(result.trades[0].source_record_id.startswith("source_record:"))

    def test_source_identities_are_deterministic_across_repeated_reads(self) -> None:
        source = self.source([row("2026-07-20T13:30:00.100Z", 100, "a", 0), row("2026-07-20T13:30:01.100Z", 101, "b", 1)])
        config = replay_config(source, self.root / "out")
        first = read_historical_source(config)
        second = read_historical_source(config)
        self.assertEqual([trade.source_record_id for trade in first.trades], [trade.source_record_id for trade in second.trades])
        self.assertEqual([trade.economic_event_fingerprint for trade in first.trades], [trade.economic_event_fingerprint for trade in second.trades])

    def test_canonical_observation_lineage_uses_source_record_id(self) -> None:
        source = self.source([row("2026-07-20T13:31:00.100Z", 100, "same", 0), row("2026-07-20T13:31:01.100Z", 101, "same", 1)])
        result = run_replay(replay_config(source, self.root / "out", strategy_config=strategy_dict(brick_size=1000)))
        source_result = read_historical_source(replay_config(source, self.root / "out", strategy_config=strategy_dict(brick_size=1000)))
        self.assertEqual(result.ledgers.canonical_observations[0].source_record_id, source_result.trades[0].source_record_id)
        self.assertEqual(result.ledgers.canonical_observations[0].source_broker_trade_id, "same")

    def test_legitimate_records_not_discarded_because_broker_trade_id_repeats(self) -> None:
        source = self.source(
            [
                row("2026-07-20T13:31:00.100Z", 100, "same", 0),
                row("2026-07-20T13:31:01.100Z", 101, "same", 1),
                row("2026-07-20T13:31:02.100Z", 102, "trigger", 2),
            ]
        )
        result = run_replay(replay_config(source, self.root / "out", strategy_config=strategy_dict(brick_size=1000)))
        self.assertEqual(result.manifest["counts"]["eligible_trades"], 3)
        self.assertEqual(result.manifest["counts"]["canonical_observations"], 2)

    def test_source_integrity_count_reconciliation(self) -> None:
        source = self.source([row("2026-07-20T13:30:00.100Z", 100, "a", 0), row("2026-07-20T13:30:01.100Z", 101, "b", 1)])
        result = run_replay(replay_config(source, self.root / "out"))
        integrity = result.manifest["source_integrity"]
        self.assertEqual(integrity["total_raw_rows"], 2)
        self.assertEqual(integrity["raw_trades_in_window"], 2)
        self.assertEqual(integrity["unique_source_identities"], 2)

    def test_session_completeness_diagnostics(self) -> None:
        source = self.source([row("2026-07-20T13:50:00.100Z", 100, "late", 0)])
        result = run_replay(replay_config(source, self.root / "out", end="2026-07-20T14:00:00Z"))
        self.assertIn("incomplete_session_coverage", [diag.diagnostic_type for diag in result.diagnostics])

    def test_source_scanning_reports_rows_scanned(self) -> None:
        source = self.source([row("2026-07-20T13:30:00.100Z", 100, "a", 0), row("2026-07-20T13:31:00.100Z", 101, "b", 1)])
        result = run_replay(replay_config(source, self.root / "out"))
        self.assertEqual(result.manifest["counts"]["source_rows_scanned"], 2)

    def test_source_scanning_stops_safely_after_end_when_ordering_verified(self) -> None:
        source = self.source(
            [
                row("2026-07-20T13:30:00.100Z", 100, "a", 0),
                row("2026-07-20T13:30:01.100Z", 101, "b", 1),
                row("2026-07-20T13:30:02.100Z", 102, "c", 2),
                row("2026-07-20T13:30:03.100Z", 103, "d", 3),
            ]
        )
        config = replay_config(source, self.root / "out", end="2026-07-20T13:30:01.500Z")
        result = read_historical_source(config)
        self.assertEqual(result.raw_rows_read, 3)
        self.assertTrue(result.integrity.stopped_after_requested_end)

    def test_full_session_trades_output(self) -> None:
        result = self.run_signal_replay(output_level="TRADES")
        out = Path(result.output_directory)
        self.assertTrue((out / "fills.jsonl").exists())
        self.assertTrue((out / "position_transitions.jsonl").exists())
        self.assertTrue((out / "closed_trades.jsonl").exists())
        self.assertTrue((out / "equity_curve.csv").exists())

    def test_force_flat_status_disclosure(self) -> None:
        source = self.source(self.signal_rows())
        result = run_replay(replay_config(source, self.root / "out", strategy_config=strategy_dict(force_flat="15:58:00", allow_overnight=False)))
        self.assertEqual(result.manifest["strategy_configuration"]["force_flat_parity"], "NOT_MODELED")
        self.assertIn("force_flat_not_modeled", [diag.diagnostic_type for diag in result.diagnostics])

    def test_result_quality_classification(self) -> None:
        source = self.source(self.signal_rows())
        smoke = run_replay(replay_config(source, self.root / "out"))
        research = run_replay(replay_config(source, self.root / "out2", start="2026-07-20T13:30:00Z", end="2026-07-20T20:00:00Z"))
        self.assertEqual(smoke.manifest["result_quality_classification"], "SMOKE_VALIDATION")
        self.assertEqual(research.manifest["result_quality_classification"], "FULL_SESSION_RESEARCH")

    def test_pending_end_of_session_orders_remain_pending(self) -> None:
        result = self.run_signal_replay()
        self.assertEqual(result.manifest["counts"]["pending_orders_at_end"], 1)

    def test_deterministic_repeated_full_session_replay(self) -> None:
        source = self.source(self.signal_rows())
        first = run_replay(replay_config(source, self.root / "out", run_id="full", start="2026-07-20T13:30:00Z", end="2026-07-20T20:00:00Z"))
        second = run_replay(replay_config(source, self.root / "out", run_id="full", start="2026-07-20T13:30:00Z", end="2026-07-20T20:00:00Z"))
        one = dict(first.manifest)
        two = dict(second.manifest)
        one.pop("replay_timestamp")
        two.pop("replay_timestamp")
        self.assertEqual(one, two)
        self.assertEqual(first.metrics, second.metrics)

    def test_derived_1s_source_kind_is_disclosed(self) -> None:
        path = self.root / "TSLA_sip_2026-07-10_1s_close.csv"
        write_csv(
            path,
            [
                {"timestamp": "2026-07-20T13:30:00Z", "close": "100"},
                {"timestamp": "2026-07-20T13:30:01Z", "close": "101"},
            ],
        )
        result = run_replay(replay_config(path, self.root / "out"))
        self.assertEqual(result.manifest["source_integrity"]["source_kind"], "sip_derived_1s_close")
        self.assertIn("source_not_raw_sip", [diag.diagnostic_type for diag in result.diagnostics])

    def test_zero_signal_warning(self) -> None:
        source = self.source([row("2026-07-20T13:31:00.100Z", 100, "t0", 0), row("2026-07-20T13:31:01.100Z", 100, "t1", 1)])
        result = run_replay(replay_config(source, self.root / "out", strategy_config=strategy_dict(brick_size=1000)))
        self.assertIn("zero_signals", [diag.diagnostic_type for diag in result.diagnostics])

    def test_short_window_warning(self) -> None:
        result = self.run_signal_replay()
        self.assertIn("short_window", [diag.diagnostic_type for diag in result.diagnostics])

    def test_event_lineage_from_raw_trade_to_closed_trade(self) -> None:
        result = self.run_signal_replay(output_level="AUDIT")
        event_types = [event.event_type for event in result.ledgers.events]
        self.assertIn("RawSipTradeReceived", event_types)
        self.assertIn("CanonicalObservationCreated", event_types)
        self.assertIn("ClosedTradeCreated", event_types)

    def test_output_ledgers_use_deterministic_ordering(self) -> None:
        result = self.run_signal_replay()
        obs_sequences = [obs.emitted_sequence for obs in result.ledgers.canonical_observations]
        order_sequences = [order.sequence for order in result.ledgers.simulated_orders]
        self.assertEqual(obs_sequences, sorted(obs_sequences))
        self.assertEqual(order_sequences, sorted(order_sequences))

    def test_repeat_run_output_equivalence_excluding_processing_timestamps(self) -> None:
        first = self.run_signal_replay(run_id="equiv")
        second = self.run_signal_replay(run_id="equiv")
        first_manifest = dict(first.manifest)
        second_manifest = dict(second.manifest)
        first_manifest.pop("replay_timestamp")
        second_manifest.pop("replay_timestamp")
        self.assertEqual(first_manifest, second_manifest)

    def test_no_alpaca_live_execution_imports_or_calls(self) -> None:
        replay_text = "\n".join(path.read_text(encoding="utf-8") for path in Path("replay").glob("*.py"))
        self.assertNotIn("AlpacaExecutionEngine", replay_text)
        self.assertNotIn("load_alpaca_credentials", replay_text)
        self.assertNotIn("import fleet_runner", replay_text)

    def test_malformed_source_row_diagnostic(self) -> None:
        source = self.source([{"symbol": "TSLA", "price": 100}, row("2026-07-20T13:30:00.100Z", 100, "t0", 0)])
        result = run_replay(replay_config(source, self.root / "out", strategy_config=strategy_dict(brick_size=1000)))
        self.assertIn("malformed_source_row", [diag.diagnostic_type for diag in result.diagnostics])

    def test_no_default_sampler_final_flush(self) -> None:
        source = self.source([row("2026-07-20T13:30:00.100Z", 100, "t0", 0)])
        result = run_replay(replay_config(source, self.root / "out"))
        self.assertEqual(len(result.ledgers.canonical_observations), 0)
        self.assertIn("pending_final_occupied_second", [diag.diagnostic_type for diag in result.diagnostics])

    def test_summary_output_does_not_write_full_audit_ledgers(self) -> None:
        result = self.run_signal_replay(output_level="SUMMARY")
        out = Path(result.output_directory)
        self.assertTrue((out / "manifest.json").exists())
        self.assertTrue((out / "metrics.json").exists())
        self.assertFalse((out / "observations_1s.jsonl").exists())
        self.assertFalse((out / "fills.jsonl").exists())

    def test_trades_output_writes_only_trade_level_ledgers(self) -> None:
        result = self.run_signal_replay(output_level="TRADES")
        out = Path(result.output_directory)
        self.assertTrue((out / "fills.jsonl").exists())
        self.assertTrue((out / "closed_trades.jsonl").exists())
        self.assertFalse((out / "observations_1s.jsonl").exists())

    def test_audit_output_writes_full_lineage(self) -> None:
        result = self.run_signal_replay(output_level="AUDIT")
        out = Path(result.output_directory)
        self.assertTrue((out / "raw_trade_references.jsonl").exists())
        self.assertTrue((out / "observations_1s.jsonl").exists())
        self.assertTrue((out / "accounting_snapshots.jsonl").exists())

    def test_raw_source_data_is_referenced_and_not_copied(self) -> None:
        source = self.source(self.signal_rows())
        result = run_replay(replay_config(source, self.root / "out"))
        out = Path(result.output_directory)
        self.assertEqual(result.manifest["source_dataset_reference"], str(source))
        self.assertFalse((out / source.name).exists())

    def test_compact_manifest_does_not_embed_full_config_or_ledgers(self) -> None:
        result = self.run_signal_replay()
        self.assertNotIn("strategy_config", result.manifest)
        self.assertNotIn("events", result.manifest)
        self.assertNotIn("raw_source_trades", result.manifest)
        self.assertIn("configuration_hash", result.manifest)


if __name__ == "__main__":
    unittest.main()
