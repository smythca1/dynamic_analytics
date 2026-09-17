from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

import pandas as pd

from scripts import post_fix_shared_sip_validation_20260825 as runner
from scripts.post_fix_shared_sip_validation_20260825 import (
    CAMATRON_SERVICE,
    MEGATRON_SERVICE,
    TransientSourceStalenessTracker,
    latest_deployment_report_path,
    run_activation_handoff,
)
from silver.market.ipc_consumer import SharedCanonicalStreamConsumer


def event(seq: int = 1, **extra):
    payload = {
        "event_id": f"evt-{seq}",
        "sequence": seq,
        "source_timestamp": f"2026-08-27T13:30:{seq:02d}+00:00",
        "observation_timestamp": f"2026-08-27T13:30:{seq:02d}+00:00",
        "delivery_phase": "live",
    }
    payload.update(extra)
    return payload


class TransientSourceStalenessPolicyTests(unittest.TestCase):
    def make_tracker(self):
        return TransientSourceStalenessTracker(["AAPL"])

    def update(
        self,
        tracker,
        *,
        t: float,
        source_age: float,
        raw_count: int = 1,
        eligible_count: int = 1,
        publication_age: float = 0.1,
        sip_healthy: bool = True,
        consumer_blocks: dict[str, str | None] | None = None,
        latest_event: dict | None = None,
        order_intent_accepted: bool = False,
    ):
        if consumer_blocks is None:
            consumer_blocks = {
                "camatron": "shared_market_data_stale_5.001s" if source_age > 5.0 else None,
                "megatron": "shared_market_data_stale_5.001s" if source_age > 5.0 else None,
            }
        return tracker.update(
            symbol="AAPL",
            now_monotonic=t,
            now_iso=f"2026-08-27T13:30:{int(t) % 60:02d}+00:00",
            source_freshness_age_seconds=source_age,
            raw_trade_count=raw_count,
            eligible_raw_trade_count=eligible_count,
            canonical_publication_liveness_age_seconds=publication_age,
            sip_healthy=sip_healthy,
            consumer_block_reasons=consumer_blocks,
            latest_event=latest_event,
            order_intent_accepted=order_intent_accepted,
        )

    def test_4p999s_source_freshness_remains_healthy(self):
        tracker = self.make_tracker()
        self.assertEqual(self.update(tracker, t=0, source_age=4.999), [])
        self.assertEqual(tracker.summary(now_monotonic=0)["by_symbol"]["AAPL"]["state"], "HEALTHY")

    def test_exactly_5s_source_freshness_remains_healthy(self):
        tracker = self.make_tracker()
        self.assertEqual(self.update(tracker, t=0, source_age=5.0), [])
        self.assertEqual(tracker.summary(now_monotonic=0)["by_symbol"]["AAPL"]["state"], "HEALTHY")

    def test_brief_breach_blocks_both_consumers_and_recovers(self):
        tracker = self.make_tracker()
        self.update(tracker, t=0, source_age=4.0)
        self.assertEqual(self.update(tracker, t=1, source_age=6.0, raw_count=2), [])
        self.assertEqual(self.update(tracker, t=2, source_age=4.0, raw_count=3, eligible_count=2, latest_event=event(2)), [])
        summary = tracker.summary(now_monotonic=35)["by_symbol"]["AAPL"]
        self.assertEqual(summary["state"], "HEALTHY_RECOVERED")
        self.assertEqual(summary["episodes"][0]["final_classification"], "HEALTHY_RECOVERED")
        self.assertEqual(summary["episodes"][0]["recovery_observation_id"], "evt-2")

    def test_exactly_15s_maximum_source_age_passes(self):
        tracker = self.make_tracker()
        self.update(tracker, t=0, source_age=4.0)
        self.assertEqual(self.update(tracker, t=1, source_age=15.0, raw_count=2), [])

    def test_15p001s_source_age_fails(self):
        tracker = self.make_tracker()
        self.update(tracker, t=0, source_age=4.0)
        failures = self.update(tracker, t=1, source_age=15.001, raw_count=2)
        self.assertTrue(any("source_freshness_age_above_15s" in failure for failure in failures))

    def test_exactly_30s_cumulative_exceedance_and_5pct_duty_cycle_pass(self):
        tracker = self.make_tracker()
        self.update(tracker, t=0, source_age=6.0, raw_count=1)
        self.assertEqual(self.update(tracker, t=30.0, source_age=6.0, raw_count=2), [])
        summary = tracker.summary(now_monotonic=30.0)["by_symbol"]["AAPL"]
        self.assertEqual(summary["episodes"][0]["rolling_cumulative_exceedance_seconds"], 30.0)
        self.assertEqual(summary["episodes"][0]["rolling_stale_duty_cycle"], 0.05)

    def test_30p001s_cumulative_exceedance_and_above_5pct_duty_cycle_fail(self):
        tracker = self.make_tracker()
        self.update(tracker, t=0, source_age=6.0, raw_count=1)
        failures = self.update(tracker, t=30.001, source_age=6.0, raw_count=2)
        self.assertTrue(any("rolling_cumulative_source_stale_above_30s" in failure for failure in failures))
        self.assertTrue(any("rolling_source_stale_duty_cycle_above_5pct" in failure for failure in failures))

    def test_multiple_brief_episodes_below_cumulative_ceiling_pass(self):
        tracker = self.make_tracker()
        self.update(tracker, t=0, source_age=6.0, raw_count=1)
        self.update(tracker, t=10, source_age=4.0, raw_count=2, eligible_count=2, latest_event=event(2))
        self.update(tracker, t=100, source_age=6.0, raw_count=3, eligible_count=2)
        self.update(tracker, t=119, source_age=4.0, raw_count=4, eligible_count=3, latest_event=event(3))
        self.assertEqual(tracker.activation_failures(now_monotonic=160), [])

    def test_multiple_brief_episodes_exceeding_cumulative_ceiling_fail(self):
        tracker = self.make_tracker()
        self.update(tracker, t=0, source_age=6.0, raw_count=1)
        self.update(tracker, t=15, source_age=4.0, raw_count=2, eligible_count=2, latest_event=event(2))
        self.update(tracker, t=100, source_age=6.0, raw_count=3, eligible_count=2)
        self.update(tracker, t=116, source_age=4.0, raw_count=4, eligible_count=3, latest_event=event(3))
        self.update(tracker, t=200, source_age=6.0, raw_count=5, eligible_count=3)
        failures = self.update(tracker, t=200.001, source_age=6.0, raw_count=6, eligible_count=3)
        self.assertTrue(any("rolling_cumulative_source_stale_above_30s" in failure for failure in failures))

    def test_one_consumer_failing_to_block_fails(self):
        tracker = self.make_tracker()
        failures = self.update(
            tracker,
            t=0,
            source_age=6.0,
            consumer_blocks={"camatron": "shared_market_data_stale_6.000s", "megatron": None},
        )
        self.assertTrue(any("consumer_missing_source_stale_signal_block" in failure for failure in failures))

    def test_recovery_from_current_eligible_canonical_observation_passes(self):
        tracker = self.make_tracker()
        self.update(tracker, t=0, source_age=6.0)
        self.assertEqual(self.update(tracker, t=1, source_age=4.0, eligible_count=2, latest_event=event(2)), [])

    def test_recovery_without_current_eligible_data_fails(self):
        tracker = self.make_tracker()
        self.update(tracker, t=0, source_age=6.0)
        failures = self.update(tracker, t=1, source_age=4.0, eligible_count=2, latest_event=None)
        self.assertTrue(any("stale_recovery_without_current_eligible_canonical_observation" in failure for failure in failures))

    def test_recovery_from_replayed_data_fails(self):
        tracker = self.make_tracker()
        self.update(tracker, t=0, source_age=6.0)
        failures = self.update(tracker, t=1, source_age=4.0, eligible_count=2, latest_event=event(2, delivery_phase="replay_catchup"))
        self.assertTrue(any("stale_recovery_from_replayed_or_historical_event" in failure for failure in failures))

    def test_recovery_from_late_or_out_of_order_event_fails(self):
        tracker = self.make_tracker()
        self.update(tracker, t=0, source_age=6.0)
        failures = self.update(tracker, t=1, source_age=4.0, eligible_count=2, latest_event=event(2, late=True))
        self.assertTrue(any("stale_recovery_from_late_or_out_of_order_event" in failure for failure in failures))

    def test_recovery_from_ineligible_event_fails(self):
        tracker = self.make_tracker()
        self.update(tracker, t=0, source_age=6.0)
        failures = self.update(tracker, t=1, source_age=4.0, eligible_count=2, latest_event=event(2, eligible=False))
        self.assertTrue(any("stale_recovery_from_ineligible_event" in failure for failure in failures))

    def test_stale_state_during_reconnect_catchup_fails_without_current_live_recovery(self):
        tracker = self.make_tracker()
        self.update(tracker, t=0, source_age=6.0, consumer_blocks={"camatron": "shared_market_data_not_caught_up", "megatron": "shared_market_data_stale_6.000s"})
        failures = self.update(tracker, t=1, source_age=4.0, eligible_count=2, latest_event=event(2, delivery_phase="replay_catchup"))
        self.assertTrue(any("stale_recovery_from_replayed_or_historical_event" in failure for failure in failures))

    def test_local_ingress_failure_fails(self):
        tracker = self.make_tracker()
        self.update(tracker, t=0, source_age=4.0, raw_count=1)
        failures = self.update(tracker, t=7, source_age=6.0, raw_count=1)
        self.assertTrue(any("raw_local_ingress_stale" in failure for failure in failures))

    def test_local_publication_failure_with_current_eligible_input_fails(self):
        tracker = self.make_tracker()
        self.update(tracker, t=0, source_age=4.0, eligible_count=1, publication_age=0.1)
        failures = self.update(tracker, t=1, source_age=4.0, raw_count=2, eligible_count=2, publication_age=6.0)
        self.assertTrue(any("local_canonical_publication_stale_despite_current_eligible_input" in failure for failure in failures))

    def test_sip_disconnect_fails(self):
        tracker = self.make_tracker()
        failures = self.update(tracker, t=0, source_age=6.0, sip_healthy=False)
        self.assertTrue(any("sip_authentication_or_subscription_lost" in failure for failure in failures))

    def test_attempted_order_intent_during_staleness_fails(self):
        tracker = self.make_tracker()
        failures = self.update(tracker, t=0, source_age=6.0, order_intent_accepted=True)
        self.assertTrue(any("order_intent_accepted_while_source_stale" in failure for failure in failures))

    def test_final_30_second_continuous_health_requirement(self):
        tracker = self.make_tracker()
        self.update(tracker, t=0, source_age=4.0)
        failures = tracker.activation_failures(now_monotonic=29.999)
        self.assertTrue(any("final continuously source-fresh period below 30s" in failure for failure in failures))
        self.assertEqual(tracker.activation_failures(now_monotonic=30.0), [])

    def test_stale_state_at_activation_boundary_fails(self):
        tracker = self.make_tracker()
        self.update(tracker, t=0, source_age=6.0)
        failures = tracker.activation_failures(now_monotonic=60.0)
        self.assertTrue(any("stale or fatal at activation boundary" in failure for failure in failures))

    def test_runtime_five_second_signal_block_threshold_is_unchanged(self):
        consumer = SharedCanonicalStreamConsumer(
            socket_path="/tmp/no.sock",
            identity="test",
            expected_contract_fingerprints={},
            state_path="/tmp/no-state.json",
        )
        self.assertEqual(consumer.freshness_threshold_seconds, 5.0)


class ValidationToLiveHandoffTests(unittest.TestCase):
    def make_validation(self, *, status: str = "passed", completed_at: str = "2026-08-27T13:40:00+00:00", failures=None):
        return {"status": status, "completed_at": completed_at, "failures": [] if failures is None else failures}

    def run_handoff(self, validation, *, preflight_exc: Exception | None = None, now: str = "2026-08-27T09:40:05-04:00"):
        started = []

        def start(unit: str) -> None:
            started.append(unit)

        def preflight(_evidence_dir: Path):
            if preflight_exc is not None:
                raise preflight_exc
            return {"preflight": "passed"}

        with tempfile.TemporaryDirectory() as tmp:
            result = run_activation_handoff(
                validation=validation,
                evidence_dir=Path(tmp),
                start_fn=start,
                preflight_fn=preflight,
                now_fn=lambda: pd.Timestamp(now),
                service_snapshot_fn=lambda: {"mock": {"ActiveState": "active"}},
            )
            self.assertTrue((Path(tmp) / "activation_handoff_report.json").exists())
        return result, started

    def test_explicit_synthetic_pass_reaches_mocked_activation_target_once(self):
        result, started = self.run_handoff(self.make_validation())

        self.assertEqual(result["status"], "activated")
        self.assertEqual(started, [CAMATRON_SERVICE, MEGATRON_SERVICE])

    def test_failed_validation_never_reaches_activation_target(self):
        result, started = self.run_handoff(self.make_validation(status="failed", failures=["source stale"]))

        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["reason"], "validation_not_unequivocally_passed")
        self.assertEqual(started, [])

    def test_timeout_after_09_40_minute_never_reaches_activation_target(self):
        result, started = self.run_handoff(self.make_validation(completed_at="2026-08-27T13:41:00+00:00"))

        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["reason"], "validation_completed_after_09_40_et_deadline")
        self.assertEqual(started, [])

    def test_missing_result_never_reaches_activation_target(self):
        result, started = self.run_handoff(None)

        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["reason"], "missing_validation_result")
        self.assertEqual(started, [])

    def test_ambiguous_result_never_reaches_activation_target(self):
        result, started = self.run_handoff({"status": "passed"})

        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["reason"], "missing_validation_completion_timestamp")
        self.assertEqual(started, [])

    def test_failed_broker_or_config_preflight_never_reaches_activation_target(self):
        result, started = self.run_handoff(self.make_validation(), preflight_exc=RuntimeError("broker not flat"))

        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["reason"], "pre_activation_preflight_failed")
        self.assertIn("broker not flat", result["error"])
        self.assertEqual(started, [])

    def test_configuration_mismatch_preflight_never_reaches_activation_target(self):
        result, started = self.run_handoff(self.make_validation(), preflight_exc=RuntimeError("enabled trader mismatch"))

        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["reason"], "pre_activation_preflight_failed")
        self.assertIn("enabled trader mismatch", result["error"])
        self.assertEqual(started, [])

    def test_fewer_than_60_minutes_remaining_never_reaches_activation_target(self):
        result, started = self.run_handoff(
            self.make_validation(),
            now="2026-08-27T15:00:00-04:00",
        )

        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["reason"], "fewer_than_60_minutes_remain_before_regular_close")
        self.assertEqual(started, [])

    def test_latest_deployment_report_uses_timestamped_deploy_directory_name(self):
        original = runner.EVIDENCE_ROOT
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = root / "transient_policy_deploy_20260827T100000Z" / "deployment_schedule_report.json"
            new = root / "auto_handoff_deploy_20260827T110000Z" / "deployment_schedule_report.json"
            old.parent.mkdir()
            new.parent.mkdir()
            old.write_text("{}\n", encoding="utf-8")
            new.write_text("{}\n", encoding="utf-8")
            runner.EVIDENCE_ROOT = root
            try:
                self.assertEqual(latest_deployment_report_path(), new)
            finally:
                runner.EVIDENCE_ROOT = original


if __name__ == "__main__":
    unittest.main()
