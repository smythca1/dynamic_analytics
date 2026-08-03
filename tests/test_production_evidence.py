from __future__ import annotations

import json
import argparse
import os
import tempfile
import unittest
from dataclasses import asdict
from pathlib import Path

import pandas as pd

from fleet_runner import (
    _PENDING_1S_CLOSE_TICKS,
    TraderRuntime,
    build_evidence_logger,
    build_shadow_sampler,
    production_sampler_checkpoint,
    record_evidence_checkpoint,
    record_shadow_for_eligible_tick,
    renko_source_ticks,
    safe_evidence_call,
)
import silver.evidence.logger as evidence_logger_module
from silver.evidence.logger import EvidenceLogger, EvidenceLoggerSettings
from silver.evidence.models import EVIDENCE_SCHEMA_VERSION, EvidenceCheckpoint
from silver.evidence.shadow_sampler import EvidenceShadowSampler, ShadowSamplerSettings, source_record_id_for_tick
from trading_events import MarketTick, OrderIntent, RenkoBrick, Signal


class ProductionEvidenceLoggerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def logger(self, name: str = "evidence.jsonl", **overrides) -> EvidenceLogger:
        settings = {
            "enabled": True,
            "output_path": str(self.root / name),
            "session_id": "session-a",
            "process_id": "process-a",
            "max_buffer_records": 10,
            "flush_every_records": 1,
            "disable_after_failures": 3,
        }
        settings.update(overrides)
        return EvidenceLogger(EvidenceLoggerSettings(**settings))

    def read_rows(self, path: str = "evidence.jsonl") -> list[dict]:
        return [json.loads(line) for line in (self.root / path).read_text().splitlines() if line.strip()]

    def args(self, **overrides):
        values = {
            "evidence_enabled": False,
            "shadow_sampler_enabled": False,
            "evidence_session_id": None,
            "evidence_output": None,
            "evidence_buffer_records": 10,
            "evidence_flush_every": 1,
            "archive_root": str(self.root),
        }
        values.update(overrides)
        return argparse.Namespace(**values)

    def runtime(self, *, evidence: EvidenceLogger | None = None, shadow_sampler=None, renko_source: str = "1s_close") -> TraderRuntime:
        class Engine:
            def __init__(self):
                self.on_tick_calls = 0

            def snapshot(self):
                return {"renko": {"anchor_mode": "grid", "last_brick_close": 100, "run_dir": 1, "run_len": 1}, "trade": {"position": None}}

            def trade_state_snapshot(self):
                return {"trade": {"position": None}}

            def on_tick(self, tick):
                self.on_tick_calls += 1
                return [], []

        return TraderRuntime(
            trader_id="t1",
            symbol="AAPL",
            mode="live",
            credential_profile=None,
            feed="sip",
            enabled=True,
            execution_enabled=False,
            max_position_notional=1000,
            max_daily_loss=0,
            engine=Engine(),
            execution=object(),
            log=object(),
            renko_source=renko_source,
            evidence=evidence,
            shadow_sampler=shadow_sampler,
        )

    def tick(self, timestamp: str, price: float = 100.0, trade_id: str = "trade-1") -> MarketTick:
        return MarketTick(
            timestamp=pd.Timestamp(timestamp),
            symbol="AAPL",
            price=price,
            size=1,
            exchange="Q",
            trade_id=trade_id,
            tape="C",
            conditions=("@",),
        )

    def test_evidence_disabled_means_no_files_and_no_counters(self) -> None:
        logger = EvidenceLogger.disabled()
        logger.record_observation(
            trader_id="t1",
            symbol="AAPL",
            observation_bucket_timestamp="2026-07-10T14:20:00Z",
            close_price=100,
        )
        logger.flush()

        self.assertEqual(list(self.root.iterdir()), [])
        self.assertEqual(logger.counters.records_written, 0)

    def test_observation_logger_records_already_produced_source_tick(self) -> None:
        source_tick = MarketTick(pd.Timestamp("2026-07-10T14:20:00Z"), "AAPL", 101.25, 1, trade_id="src-1")
        trigger_tick = MarketTick(pd.Timestamp("2026-07-10T14:20:01.100Z"), "AAPL", 101.30, 1, trade_id="trg-1")
        logger = self.logger()

        logger.record_observation(
            trader_id="t1",
            symbol="AAPL",
            observation_bucket_timestamp=source_tick.timestamp,
            close_price=source_tick.price,
            source_trade_identity=source_tick.trade_id,
            source_trade_timestamp="UNKNOWN",
            triggering_trade_identity=trigger_tick.trade_id,
            triggering_trade_timestamp=trigger_tick.timestamp,
        )

        row = self.read_rows()[0]
        self.assertEqual(row["schema_version"], EVIDENCE_SCHEMA_VERSION)
        self.assertEqual(row["evidence_type"], "observation")
        self.assertEqual(row["logical_sequence"], 1)
        self.assertEqual(row["payload"]["observation_bucket_timestamp"], "2026-07-10T14:20:00+00:00")
        self.assertEqual(row["payload"]["source_trade_identity"], "src-1")
        self.assertEqual(row["payload"]["triggering_trade_timestamp"], "2026-07-10T14:20:01.100000+00:00")

    def test_renko_logger_records_returned_brick_without_modifying_it(self) -> None:
        brick = RenkoBrick(pd.Timestamp("2026-07-10T14:20:00Z"), "AAPL", 100, 101, 1, 1, 1)
        before = asdict(brick)
        logger = self.logger()

        logger.record_renko_brick(
            trader_id="t1",
            symbol="AAPL",
            brick=brick,
            source_observation_bucket_timestamp=pd.Timestamp("2026-07-10T14:20:00Z"),
        )

        self.assertEqual(asdict(brick), before)
        row = self.read_rows()[0]
        self.assertEqual(row["evidence_type"], "renko_brick")
        self.assertEqual(row["payload"]["brick_logical_timestamp"], "2026-07-10T14:20:00+00:00")
        self.assertEqual(row["payload"]["source_observation_bucket_timestamp"], "2026-07-10T14:20:00+00:00")

    def test_signal_logger_records_existing_signal_and_intent_without_modifying_them(self) -> None:
        signal = Signal("random-live-id", "entry", pd.Timestamp("2026-07-10T14:20:00Z"), "AAPL", "long", "buy", 101, "renko_reversal", "strategy", "t1")
        intent = OrderIntent("client-id", signal.timestamp, "AAPL", "buy", "entry", "market", "strategy", "renko_reversal", qty=1)
        before_signal = asdict(signal)
        before_intent = asdict(intent)
        logger = self.logger()

        sequence = logger.record_signal(
            trader_id="t1",
            symbol="AAPL",
            signal=signal,
            intent=intent,
            source_brick_timestamp=signal.timestamp,
        )

        self.assertEqual(sequence, 1)
        self.assertEqual(asdict(signal), before_signal)
        self.assertEqual(asdict(intent), before_intent)
        row = self.read_rows()[0]
        self.assertEqual(row["payload"]["logical_signal_timestamp"], "2026-07-10T14:20:00+00:00")
        self.assertEqual(row["payload"]["source_brick_timestamp"], "2026-07-10T14:20:00+00:00")
        self.assertEqual(row["payload"]["client_order_id"], "client-id")

    def test_direction_transition_logger_records_existing_before_after_values(self) -> None:
        logger = self.logger()

        logger.record_position_direction(
            trader_id="t1",
            symbol="AAPL",
            logical_transition_timestamp=pd.Timestamp("2026-07-10T14:20:00Z"),
            before_direction="flat",
            after_direction="long",
            triggering_signal_sequence=4,
        )

        row = self.read_rows()[0]
        self.assertEqual(row["evidence_type"], "position_direction")
        self.assertEqual(row["payload"]["before_direction"], "flat")
        self.assertEqual(row["payload"]["after_direction"], "long")
        self.assertEqual(row["payload"]["triggering_signal_sequence"], 4)

    def test_checkpoint_serialization_is_read_only_and_explicit_about_unknowns(self) -> None:
        checkpoint = EvidenceCheckpoint(
            checkpoint_timestamp="2026-07-10T14:20:00Z",
            trader_id="t1",
            symbol="AAPL",
            renko_anchor_mode="grid",
            last_renko_close=101,
            run_direction=1,
            run_length=2,
            completeness=["broker_confirmed_quantity_unknown"],
        )
        before = asdict(checkpoint)
        logger = self.logger()

        logger.record_checkpoint(checkpoint)

        self.assertEqual(asdict(checkpoint), before)
        row = self.read_rows()[0]
        self.assertEqual(row["evidence_type"], "state_checkpoint")
        self.assertEqual(row["payload"]["checkpoint_timestamp"], "2026-07-10T14:20:00Z")
        self.assertIn("broker_confirmed_quantity_unknown", row["payload"]["completeness"])

    def test_write_failure_fails_open_and_emits_visible_diagnostic(self) -> None:
        diagnostics: list[str] = []
        logger = EvidenceLogger(
            EvidenceLoggerSettings(
                enabled=True,
                output_path=str(self.root),
                session_id="session-a",
                process_id="process-a",
                max_buffer_records=10,
                flush_every_records=1,
            ),
            diagnostic_sink=diagnostics.append,
        )

        logger.record_observation(trader_id="t1", symbol="AAPL", observation_bucket_timestamp="ts", close_price=1)

        self.assertEqual(logger.counters.records_written, 0)
        self.assertEqual(logger.counters.write_failures, 1)
        self.assertEqual(logger.counters.evidence_failures, 1)
        self.assertEqual(logger.counters.diagnostics_emitted, 1)
        self.assertIn("trading continues", diagnostics[0])

    def test_record_model_construction_exception_fails_open(self) -> None:
        diagnostics: list[str] = []
        logger = self.logger()
        logger._diagnostic_sink = diagnostics.append
        original = evidence_logger_module.EvidenceRecord

        class BrokenRecord:
            def __init__(self, *args, **kwargs):
                raise RuntimeError("record boom")

        try:
            evidence_logger_module.EvidenceRecord = BrokenRecord
            logger.record_observation(trader_id="t1", symbol="AAPL", observation_bucket_timestamp="ts", close_price=1)
        finally:
            evidence_logger_module.EvidenceRecord = original

        self.assertEqual(logger.counters.evidence_failures, 1)
        self.assertEqual(logger.counters.records_written, 0)
        self.assertIn("record failed", diagnostics[0])

    def test_serialization_exception_fails_open(self) -> None:
        diagnostics: list[str] = []
        logger = self.logger()
        logger._diagnostic_sink = diagnostics.append
        original = evidence_logger_module.json.dumps

        def broken_dumps(*args, **kwargs):
            raise RuntimeError("json boom")

        try:
            evidence_logger_module.json.dumps = broken_dumps
            logger.record_observation(trader_id="t1", symbol="AAPL", observation_bucket_timestamp="ts", close_price=1)
        finally:
            evidence_logger_module.json.dumps = original

        self.assertEqual(logger.counters.evidence_failures, 1)
        self.assertEqual(logger.counters.records_written, 0)

    def test_buffer_append_exception_fails_open(self) -> None:
        diagnostics: list[str] = []
        logger = self.logger(flush_every_records=100)
        logger._diagnostic_sink = diagnostics.append

        class BrokenBuffer(list):
            def append(self, item):
                raise RuntimeError("append boom")

        logger._buffer = BrokenBuffer()
        logger.record_observation(trader_id="t1", symbol="AAPL", observation_bucket_timestamp="ts", close_price=1)

        self.assertEqual(logger.counters.evidence_failures, 1)
        self.assertEqual(logger.counters.records_written, 0)

    def test_failure_diagnostics_are_rate_limited_and_disable_after_threshold(self) -> None:
        diagnostics: list[str] = []
        logger = EvidenceLogger(
            EvidenceLoggerSettings(
                enabled=True,
                output_path=str(self.root),
                session_id="session-a",
                process_id="process-a",
                diagnostic_rate_limit=1,
                disable_after_failures=2,
            ),
            diagnostic_sink=diagnostics.append,
        )

        logger.record_observation(trader_id="t1", symbol="AAPL", observation_bucket_timestamp="ts", close_price=1)
        logger.record_observation(trader_id="t1", symbol="AAPL", observation_bucket_timestamp="ts", close_price=1)

        self.assertEqual(logger.counters.evidence_failures, 2)
        self.assertFalse(logger.enabled)
        self.assertEqual(len(diagnostics), 1)

    def test_safe_visible_diagnostic_never_raises_when_sink_fails(self) -> None:
        logger = self.logger()

        def broken_sink(message: str) -> None:
            raise RuntimeError("diagnostic boom")

        logger._diagnostic_sink = broken_sink

        self.assertFalse(logger.safe_visible_diagnostic("shadow failed"))
        self.assertEqual(logger.counters.diagnostic_failures, 1)

    def test_safe_visible_diagnostic_avoids_recursive_diagnostic_loop(self) -> None:
        calls = 0
        logger = self.logger()

        def broken_sink(message: str) -> None:
            nonlocal calls
            calls += 1
            raise RuntimeError("diagnostic boom")

        logger._diagnostic_sink = broken_sink

        logger.safe_visible_diagnostic("first")
        logger.safe_visible_diagnostic("second")

        self.assertEqual(calls, 1)
        self.assertEqual(logger.counters.diagnostic_failures, 1)

    def test_from_environment_requires_explicit_session_and_output(self) -> None:
        original = {key: os.environ.get(key) for key in ("FLEET_EVIDENCE_ENABLED", "FLEET_EVIDENCE_SESSION_ID", "FLEET_EVIDENCE_OUTPUT")}
        try:
            for key in original:
                os.environ.pop(key, None)
            self.assertFalse(EvidenceLogger.from_environment().enabled)

            os.environ["FLEET_EVIDENCE_ENABLED"] = "1"
            missing_session = EvidenceLogger.from_environment()
            self.assertFalse(missing_session.enabled)
            self.assertIsNone(missing_session.settings.output_path)

            os.environ["FLEET_EVIDENCE_SESSION_ID"] = "session-a"
            missing_output = EvidenceLogger.from_environment()
            self.assertFalse(missing_output.enabled)

            os.environ["FLEET_EVIDENCE_OUTPUT"] = str(self.root / "env.jsonl")
            enabled = EvidenceLogger.from_environment()
            self.assertTrue(enabled.enabled)
            self.assertEqual(enabled.settings.session_id, "session-a")
            self.assertEqual(enabled.settings.output_path, str(self.root / "env.jsonl"))
        finally:
            for key, value in original.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_evidence_sequence_is_deterministic_within_session_and_trader(self) -> None:
        logger = self.logger(flush_every_records=10)

        logger.record_observation(trader_id="t1", symbol="AAPL", observation_bucket_timestamp="t0", close_price=1)
        logger.record_observation(trader_id="t1", symbol="AAPL", observation_bucket_timestamp="t1", close_price=2)
        logger.record_observation(trader_id="t2", symbol="MSFT", observation_bucket_timestamp="t0", close_price=3)
        logger.flush()

        rows = self.read_rows()
        self.assertEqual([row["logical_sequence"] for row in rows], [1, 2, 1])
        self.assertEqual([row["session_id"] for row in rows], ["session-a", "session-a", "session-a"])

    def test_buffer_size_is_bounded(self) -> None:
        logger = self.logger(max_buffer_records=2, flush_every_records=100)

        for idx in range(5):
            logger.record_observation(trader_id="t1", symbol="AAPL", observation_bucket_timestamp=f"t{idx}", close_price=idx)

        self.assertLessEqual(len(logger._buffer), 2)
        logger.flush()
        self.assertEqual(logger.counters.records_written, 5)

    def test_evidence_modules_do_not_import_alpaca_execution(self) -> None:
        for path in (Path("silver/evidence/models.py"), Path("silver/evidence/logger.py"), Path("silver/evidence/shadow_sampler.py")):
            text = path.read_text()
            self.assertNotIn("alpaca", text.lower())
            self.assertNotIn("execution_engine", text)

    def test_shadow_sampler_disabled_by_default(self) -> None:
        evidence = build_evidence_logger(self.args())
        shadow = build_shadow_sampler(self.args(), evidence)

        self.assertFalse(evidence.enabled)
        self.assertIsNone(shadow)

    def test_evidence_enabled_without_shadow_retains_evidence_behavior(self) -> None:
        output = self.root / "evidence.jsonl"
        evidence = build_evidence_logger(
            self.args(evidence_enabled=True, evidence_session_id="session-a", evidence_output=str(output))
        )
        shadow = build_shadow_sampler(
            self.args(evidence_enabled=True, evidence_session_id="session-a", evidence_output=str(output)),
            evidence,
        )

        evidence.record_production_observation(
            trader_id="t1",
            symbol="AAPL",
            sequence=1,
            observation_bucket_timestamp="2026-07-10T14:20:00Z",
            close_price=100,
            production_source_mode="1s_close",
        )

        self.assertTrue(evidence.enabled)
        self.assertIsNone(shadow)
        self.assertEqual(json.loads(output.read_text())["evidence_type"], "production_observation")

    def test_shadow_enabled_without_evidence_warns_and_allocates_no_shadow_state(self) -> None:
        diagnostics: list[str] = []
        evidence = build_evidence_logger(self.args())
        original = __import__("fleet_runner").visible_evidence_warning
        try:
            __import__("fleet_runner").visible_evidence_warning = diagnostics.append
            shadow = build_shadow_sampler(self.args(shadow_sampler_enabled=True), evidence)
        finally:
            __import__("fleet_runner").visible_evidence_warning = original

        self.assertIsNone(shadow)
        self.assertEqual(len(diagnostics), 1)

    def test_missing_session_or_output_disables_evidence_and_shadow_safely(self) -> None:
        self.assertFalse(build_evidence_logger(self.args(evidence_enabled=True, evidence_output=str(self.root / "e.jsonl"))).enabled)
        evidence = build_evidence_logger(self.args(evidence_enabled=True, evidence_session_id="session-a"))
        self.assertFalse(evidence.enabled)
        self.assertIsNone(build_shadow_sampler(self.args(evidence_enabled=True, shadow_sampler_enabled=True), evidence))

    def test_shadow_consumes_only_already_eligible_trade_and_never_calls_engine(self) -> None:
        evidence = self.logger()
        shadow = EvidenceShadowSampler(ShadowSamplerSettings(enabled=True, session_id="session-a", process_id="process-a"))
        runtime = self.runtime(evidence=evidence, shadow_sampler=shadow)

        record_shadow_for_eligible_tick(runtime, self.tick("2026-07-10T14:20:00.100Z"))

        self.assertEqual(runtime.engine.on_tick_calls, 0)
        self.assertEqual(shadow.counters.trades_processed, 1)

    def test_production_sampler_remains_authoritative_and_shadow_cannot_replace_source_ticks(self) -> None:
        _PENDING_1S_CLOSE_TICKS.clear()
        evidence = self.logger()
        shadow = EvidenceShadowSampler(ShadowSamplerSettings(enabled=True, session_id="session-a", process_id="process-a"))
        runtime = self.runtime(evidence=evidence, shadow_sampler=shadow)
        first = self.tick("2026-07-10T14:20:00.100Z", 100, "t0")
        second = self.tick("2026-07-10T14:20:01.100Z", 101, "t1")

        record_shadow_for_eligible_tick(runtime, first)
        self.assertEqual(renko_source_ticks(runtime, first), [])
        record_shadow_for_eligible_tick(runtime, second)
        production = renko_source_ticks(runtime, second)

        self.assertEqual(len(production), 1)
        self.assertEqual(production[0].price, 100)

    def test_shadow_state_is_isolated_by_session_trader_and_symbol(self) -> None:
        first = EvidenceShadowSampler(ShadowSamplerSettings(enabled=True, session_id="s1", process_id="p"))
        second = EvidenceShadowSampler(ShadowSamplerSettings(enabled=True, session_id="s2", process_id="p"))
        tick = self.tick("2026-07-10T14:20:00.100Z", trade_id="same-broker-id")

        first.process_tick(trader_id="t1", tick=tick)
        second.process_tick(trader_id="t1", tick=tick)

        self.assertNotEqual(
            first.checkpoint(trader_id="t1", symbol="AAPL")["pending_source_record_id"],
            second.checkpoint(trader_id="t1", symbol="AAPL")["pending_source_record_id"],
        )

    def test_source_record_ids_are_deterministic_and_not_broker_id_unique(self) -> None:
        first = source_record_id_for_tick(
            session_id="s", trader_id="t", symbol="AAPL", source_sequence=1, sip_timestamp=pd.Timestamp("2026-07-10T14:20:00Z"), broker_trade_id="b"
        )
        repeat = source_record_id_for_tick(
            session_id="s", trader_id="t", symbol="AAPL", source_sequence=1, sip_timestamp=pd.Timestamp("2026-07-10T14:20:00Z"), broker_trade_id="b"
        )
        second = source_record_id_for_tick(
            session_id="s", trader_id="t", symbol="AAPL", source_sequence=2, sip_timestamp=pd.Timestamp("2026-07-10T14:20:00Z"), broker_trade_id="b"
        )

        self.assertEqual(first, repeat)
        self.assertNotEqual(first, second)

    def test_shadow_exception_fails_open_and_is_rate_limited(self) -> None:
        diagnostics: list[str] = []
        evidence = self.logger()
        evidence._diagnostic_sink = diagnostics.append

        class BrokenShadow:
            enabled = True

            class Counters:
                failures = 0

            counters = Counters()

            def process_tick(self, **kwargs):
                raise RuntimeError("boom")

        runtime = self.runtime(evidence=evidence, shadow_sampler=BrokenShadow())

        record_shadow_for_eligible_tick(runtime, self.tick("2026-07-10T14:20:00Z"))
        record_shadow_for_eligible_tick(runtime, self.tick("2026-07-10T14:20:01Z"))

        self.assertIsNone(runtime.shadow_sampler)
        self.assertEqual(len(diagnostics), 1)
        self.assertIn("shadow sampler disabled", diagnostics[0])

    def test_shadow_exception_with_broken_diagnostic_sink_still_allows_engine_on_tick(self) -> None:
        evidence = self.logger()

        def broken_sink(message: str) -> None:
            raise RuntimeError("diagnostic boom")

        evidence._diagnostic_sink = broken_sink

        class BrokenShadow:
            enabled = True

            class Counters:
                failures = 0

            counters = Counters()

            def process_tick(self, **kwargs):
                raise RuntimeError("shadow boom")

        runtime = self.runtime(evidence=evidence, shadow_sampler=BrokenShadow())
        tick = self.tick("2026-07-10T14:20:00Z")

        record_shadow_for_eligible_tick(runtime, tick)
        runtime.engine.on_tick(tick)

        self.assertIsNone(runtime.shadow_sampler)
        self.assertEqual(runtime.engine.on_tick_calls, 1)
        self.assertEqual(evidence.counters.diagnostic_failures, 1)

    def test_safe_evidence_call_failure_does_not_prevent_engine_on_tick(self) -> None:
        diagnostics: list[str] = []
        evidence = self.logger(disable_after_failures=5)
        evidence._diagnostic_sink = diagnostics.append
        runtime = self.runtime(evidence=evidence)
        tick = self.tick("2026-07-10T14:20:00Z")

        safe_evidence_call(runtime, "production_observation", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
        runtime.engine.on_tick(tick)

        self.assertEqual(runtime.engine.on_tick_calls, 1)
        self.assertEqual(evidence.counters.evidence_failures, 1)

    def test_safe_evidence_call_with_broken_diagnostic_sink_still_allows_engine_on_tick(self) -> None:
        evidence = self.logger(disable_after_failures=5)

        def broken_sink(message: str) -> None:
            raise RuntimeError("diagnostic boom")

        evidence._diagnostic_sink = broken_sink
        runtime = self.runtime(evidence=evidence)
        tick = self.tick("2026-07-10T14:20:00Z")

        safe_evidence_call(runtime, "production_observation", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
        runtime.engine.on_tick(tick)

        self.assertEqual(runtime.engine.on_tick_calls, 1)
        self.assertEqual(evidence.counters.evidence_failures, 1)
        self.assertEqual(evidence.counters.diagnostic_failures, 1)

    def test_renko_evidence_failure_does_not_suppress_actions(self) -> None:
        diagnostics: list[str] = []
        evidence = self.logger(disable_after_failures=5)
        evidence._diagnostic_sink = diagnostics.append
        runtime = self.runtime(evidence=evidence)
        action_reached = False

        safe_evidence_call(runtime, "renko_brick", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
        action_reached = True

        self.assertTrue(action_reached)
        self.assertEqual(evidence.counters.evidence_failures, 1)

    def test_signal_evidence_failure_does_not_suppress_execution_handling(self) -> None:
        diagnostics: list[str] = []
        evidence = self.logger(disable_after_failures=5)
        evidence._diagnostic_sink = diagnostics.append
        runtime = self.runtime(evidence=evidence)
        execution_reached = False

        safe_evidence_call(runtime, "strategy_signal", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
        execution_reached = True

        self.assertTrue(execution_reached)
        self.assertEqual(evidence.counters.evidence_failures, 1)

    def test_strategy_actions_still_reach_normal_handling_after_evidence_diagnostic_failure(self) -> None:
        evidence = self.logger(disable_after_failures=5)

        def broken_sink(message: str) -> None:
            raise RuntimeError("diagnostic boom")

        evidence._diagnostic_sink = broken_sink
        runtime = self.runtime(evidence=evidence)
        action_reached = False

        safe_evidence_call(runtime, "strategy_signal", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
        action_reached = True

        self.assertTrue(action_reached)
        self.assertEqual(evidence.counters.diagnostic_failures, 1)

    def test_production_exception_outside_evidence_helper_is_not_swallowed(self) -> None:
        evidence = self.logger()
        runtime = self.runtime(evidence=evidence)

        def production_failure():
            raise RuntimeError("production boom")

        with self.assertRaises(RuntimeError):
            production_failure()

    def test_checkpoint_contains_shadow_and_production_sampler_state(self) -> None:
        _PENDING_1S_CLOSE_TICKS.clear()
        evidence = self.logger()
        shadow = EvidenceShadowSampler(ShadowSamplerSettings(enabled=True, session_id="session-a", process_id="process-a"))
        runtime = self.runtime(evidence=evidence, shadow_sampler=shadow)
        tick = self.tick("2026-07-10T14:20:00.100Z", 100, "t0")
        record_shadow_for_eligible_tick(runtime, tick)
        renko_source_ticks(runtime, tick)

        record_evidence_checkpoint(runtime, reason="test", checkpoint_timestamp="2026-07-10T14:20:00Z")

        checkpoint = [row for row in self.read_rows() if row["evidence_type"] == "state_checkpoint"][0]
        self.assertIn("production_sampler_state", checkpoint["payload"])
        self.assertIn("shadow_sampler_state", checkpoint["payload"])
        self.assertIn("source_record_id", checkpoint["payload"]["production_sampler_state"]["unknown_fields"])
        self.assertEqual(checkpoint["payload"]["shadow_sampler_state"]["seen_source_id_capacity"], 100000)
        self.assertEqual(checkpoint["payload"]["shadow_sampler_state"]["seen_source_id_count"], 1)

    def test_production_pending_state_unknown_when_inaccessible(self) -> None:
        runtime = self.runtime(renko_source="ticks")

        state = production_sampler_checkpoint(runtime)

        self.assertEqual(state["pending_state"], "UNKNOWN")


if __name__ == "__main__":
    unittest.main()
