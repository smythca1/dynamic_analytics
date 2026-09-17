from __future__ import annotations

from types import SimpleNamespace
import unittest

from scripts.post_fix_shared_sip_validation_20260825 import collect_consumer_latency, stats


def consumer(identity: str, events: list[dict]):
    return SimpleNamespace(identity=identity, events=events)


def event(phase: str, local_latency: float, upstream_age: float = 0.0, sequence: int = 1):
    return {
        "delivery_phase": phase,
        "symbol": "AAPL",
        "sequence": sequence,
        "event_id": f"evt-{sequence}",
        "local_ingress_to_consumer_seconds": local_latency,
        "upstream_exchange_to_ingress_seconds": upstream_age,
        "observation_timestamp": "2026-08-26T15:30:00+00:00",
        "receive_timestamp": "2026-08-26T15:30:01+00:00",
        "received_at": "2026-08-26T15:30:01.010000+00:00",
    }


class ValidationLatencyMetricTests(unittest.TestCase):
    def test_replay_catchup_outlier_is_reported_but_not_live_gate_input(self):
        collected = collect_consumer_latency(
            {
                "camatron": consumer(
                    "camatron",
                    [
                        event("live", 0.010, sequence=1),
                        event("replay_catchup", 14.5, sequence=2),
                    ],
                )
            }
        )

        self.assertEqual(stats(collected["live_latency"])["max"], 0.010)
        self.assertEqual(stats(collected["replay_latency"])["max"], 14.5)
        self.assertEqual(collected["outliers"][0]["delivery_phase"], "replay_catchup")

    def test_upstream_age_outlier_does_not_contaminate_local_latency(self):
        collected = collect_consumer_latency(
            {"camatron": consumer("camatron", [event("live", 0.025, upstream_age=12.0)])}
        )

        self.assertEqual(stats(collected["live_latency"])["max"], 0.025)
        self.assertEqual(collected["outliers"][0]["upstream_exchange_to_ingress_seconds"], 12.0)

    def test_true_live_local_delay_remains_gate_visible(self):
        collected = collect_consumer_latency(
            {
                "camatron": consumer(
                    "camatron",
                    [
                        event("live", 0.020, sequence=1),
                        event("live", 6.0, sequence=2),
                    ],
                )
            }
        )

        self.assertEqual(stats(collected["live_latency"])["max"], 6.0)
        self.assertEqual(collected["outliers"][0]["delivery_phase"], "live")
        self.assertEqual(collected["outliers"][0]["local_ingress_to_consumer_seconds"], 6.0)


if __name__ == "__main__":
    unittest.main()
