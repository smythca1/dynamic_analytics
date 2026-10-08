from __future__ import annotations

import inspect
import time
import unittest
from decimal import Decimal

import pandas as pd

from silver.market import CanonicalOneSecondSipSampler, RawSipTrade, evaluate_trade_eligibility
from silver.market.shadow_parity import compare_with_production_sampler
from trading_events import MarketTick


D = Decimal


def raw(
    timestamp: str,
    *,
    symbol: str = "TSLA",
    price: str = "100",
    trade_id: str | None = None,
    conditions: tuple[str, ...] = ("@",),
    source_sequence: int = 0,
    row_order: int = 0,
    source_record_id: str | None = None,
    trader_id: str | None = "sampler_tsla",
    source_stream_id: str | None = None,
) -> RawSipTrade:
    return RawSipTrade(
        sip_timestamp=timestamp,
        receive_timestamp=timestamp,
        symbol=symbol,
        price=price,
        quantity="1",
        exchange="unit",
        tape="C",
        trade_id=trade_id if trade_id is not None else f"{symbol}-{timestamp}-{price}",
        conditions=conditions,
        source="unit",
        source_sequence=source_sequence,
        row_order=row_order,
        source_record_id=source_record_id,
        trader_id=trader_id,
        source_stream_id=source_stream_id,
    )


def run_sampler(trades: list[RawSipTrade]):
    sampler = CanonicalOneSecondSipSampler(run_id="test-run")
    observations = []
    diagnostics = []
    results = []
    for trade in trades:
        result = sampler.process_trade(trade)
        results.append(result)
        observations.extend(result.observations)
        diagnostics.extend(result.diagnostics)
    return sampler, observations, diagnostics, results


def run_bounded_sampler(trades: list[RawSipTrade], *, capacity: int):
    sampler = CanonicalOneSecondSipSampler(run_id="test-run", max_seen_source_ids=capacity)
    observations = []
    diagnostics = []
    results = []
    for trade in trades:
        result = sampler.process_trade(trade)
        results.append(result)
        observations.extend(result.observations)
        diagnostics.extend(result.diagnostics)
    return sampler, observations, diagnostics, results


class CanonicalMarketSamplerTests(unittest.TestCase):
    def test_alpaca_condition_normalization_preserves_canonical_eligibility_contract(self) -> None:
        cases = (
            ((" ",), ("@",), True),
            ((" ", "F"), ("@", "F"), True),
            ((" ", "I"), ("@", "I"), False),
            ((" ", "F", "I"), ("@", "F", "I"), False),
            ((" ", "4", "I"), ("@", "4", "I"), False),
            ((" ", "O"), ("@", "O"), False),
            (("@",), ("@",), True),
            (("@", "F"), ("@", "F"), True),
            (("X", "4"), ("X", "4"), False),
        )
        for original, expected, eligible in cases:
            with self.subTest(original=original):
                tick = MarketTick(
                    timestamp=pd.Timestamp("2026-07-20T13:30:00Z"),
                    symbol="INTC",
                    price=100.0,
                    size=1.0,
                    conditions=original,
                )
                self.assertEqual(tick.conditions, expected)
                self.assertEqual(evaluate_trade_eligibility(raw("2026-07-20T13:30:00Z", conditions=tick.conditions)).is_eligible, eligible)

    def test_eligible_price_forming_trade(self) -> None:
        decision = evaluate_trade_eligibility(raw("2026-07-20T13:30:00Z", conditions=("@", "F")))
        self.assertTrue(decision.is_eligible)

    def test_rejected_non_price_forming_trade(self) -> None:
        sampler, observations, diagnostics, _ = run_sampler(
            [raw("2026-07-20T13:30:00Z", conditions=("@", "I"))]
        )
        self.assertEqual(observations, [])
        self.assertEqual(sampler.pending_diagnostics(), ())
        self.assertEqual([diag.diagnostic_type for diag in diagnostics], ["rejected_non_price_forming_trade"])

    def test_multiple_eligible_trades_in_one_occupied_second_final_trade_retained(self) -> None:
        _, observations, _, _ = run_sampler(
            [
                raw("2026-07-20T13:30:00.100Z", price="100", trade_id="t1", source_sequence=1),
                raw("2026-07-20T13:30:00.900Z", price="101", trade_id="t2", source_sequence=2),
                raw("2026-07-20T13:30:01.001Z", price="102", trade_id="t3", source_sequence=3),
            ]
        )
        self.assertEqual(len(observations), 1)
        self.assertEqual(observations[0].close_price, D("101"))
        self.assertEqual(observations[0].source_alpaca_trade_id, "t2")

    def test_delayed_emission_after_later_second_trade(self) -> None:
        sampler = CanonicalOneSecondSipSampler(run_id="test-run")
        self.assertEqual(sampler.process_trade(raw("2026-07-20T13:30:00.100Z", trade_id="t1")).observations, ())
        emitted = sampler.process_trade(raw("2026-07-20T13:30:01.001Z", trade_id="t2")).observations
        self.assertEqual(len(emitted), 1)
        self.assertEqual(emitted[0].second_bucket.isoformat(), "2026-07-20T13:30:00+00:00")

    def test_gaps_across_empty_seconds_and_no_synthetic_empty_observations(self) -> None:
        _, observations, _, _ = run_sampler(
            [
                raw("2026-07-20T13:30:00.100Z", price="100", trade_id="t1"),
                raw("2026-07-20T13:30:05.001Z", price="105", trade_id="t2"),
            ]
        )
        self.assertEqual(len(observations), 1)
        self.assertEqual(observations[0].second_bucket.isoformat(), "2026-07-20T13:30:00+00:00")
        self.assertEqual(observations[0].close_price, D("100"))

    def test_no_default_final_flush(self) -> None:
        sampler, observations, _, _ = run_sampler([raw("2026-07-20T13:30:00.100Z", trade_id="t1")])
        self.assertEqual(observations, [])
        self.assertEqual([diag.diagnostic_type for diag in sampler.pending_diagnostics()], ["pending_final_occupied_second"])

    def test_explicit_replay_finalized_flush(self) -> None:
        sampler, _, _, _ = run_sampler([raw("2026-07-20T13:30:00.100Z", trade_id="t1")])
        result = sampler.flush_pending(scope_key=("sampler_tsla", "TSLA"))
        self.assertEqual(len(result.observations), 1)
        self.assertTrue(result.observations[0].replay_finalized)
        self.assertEqual([diag.diagnostic_type for diag in result.diagnostics], ["explicit_replay_finalization_flush"])

    def test_out_of_order_older_trade_diagnostic(self) -> None:
        _, observations, diagnostics, _ = run_sampler(
            [
                raw("2026-07-20T13:30:02.100Z", price="102", trade_id="t2", source_sequence=2),
                raw("2026-07-20T13:30:01.900Z", price="101", trade_id="t1", source_sequence=1),
                raw("2026-07-20T13:30:03.001Z", price="103", trade_id="t3", source_sequence=3),
            ]
        )
        self.assertIn("older_prior_bucket_trade", [diag.diagnostic_type for diag in diagnostics])
        self.assertEqual(len(observations), 1)
        self.assertEqual(observations[0].close_price, D("102"))

    def test_duplicate_source_trade_diagnostic(self) -> None:
        duplicate_record_id = "source_record:duplicate"
        _, observations, diagnostics, _ = run_sampler(
            [
                raw(
                    "2026-07-20T13:30:00.100Z",
                    price="100",
                    trade_id="dup",
                    source_sequence=1,
                    source_record_id=duplicate_record_id,
                ),
                raw(
                    "2026-07-20T13:30:00.900Z",
                    price="101",
                    trade_id="dup",
                    source_sequence=2,
                    source_record_id=duplicate_record_id,
                ),
                raw("2026-07-20T13:30:01.001Z", price="102", trade_id="t3", source_sequence=3),
            ]
        )
        self.assertIn("duplicate_source_trade", [diag.diagnostic_type for diag in diagnostics])
        self.assertEqual(observations[0].close_price, D("100"))

    def test_bounded_cache_detects_duplicate_while_retained(self) -> None:
        duplicate_record_id = "source_record:duplicate"
        _, _, diagnostics, _ = run_bounded_sampler(
            [
                raw("2026-07-20T13:30:00.100Z", trade_id="dup", source_sequence=1, source_record_id=duplicate_record_id),
                raw("2026-07-20T13:30:00.200Z", trade_id="dup", source_sequence=2, source_record_id=duplicate_record_id),
            ],
            capacity=2,
        )

        self.assertIn("duplicate_source_trade", [diag.diagnostic_type for diag in diagnostics])

    def test_bounded_cache_never_exceeds_capacity_and_evicts_oldest_deterministically(self) -> None:
        sampler, _, diagnostics, _ = run_bounded_sampler(
            [
                raw("2026-07-20T13:30:00.100Z", trade_id="t1", source_sequence=1, source_record_id="source_record:1"),
                raw("2026-07-20T13:30:01.100Z", trade_id="t2", source_sequence=2, source_record_id="source_record:2"),
                raw("2026-07-20T13:30:02.100Z", trade_id="t3", source_sequence=3, source_record_id="source_record:3"),
            ],
            capacity=2,
        )
        state = sampler.state(("sampler_tsla", "TSLA"))

        self.assertEqual(state.seen_source_id_capacity, 2)
        self.assertEqual(state.seen_source_id_count, 2)
        self.assertEqual(state.seen_source_id_evictions, 1)
        self.assertEqual([diag.diagnostic_type for diag in diagnostics if diag.diagnostic_type == "source_id_cache_evicted"], ["source_id_cache_evicted"])
        self.assertEqual([diag.details["evicted_source_id"] for diag in diagnostics if diag.diagnostic_type == "source_id_cache_evicted"], ["source_record:1"])

    def test_evicted_source_identity_is_processed_as_nonduplicate(self) -> None:
        _, _, diagnostics, results = run_bounded_sampler(
            [
                raw("2026-07-20T13:30:00.100Z", trade_id="t1", source_sequence=1, source_record_id="source_record:1"),
                raw("2026-07-20T13:30:01.100Z", trade_id="t2", source_sequence=2, source_record_id="source_record:2"),
                raw("2026-07-20T13:30:02.100Z", trade_id="t3", source_sequence=3, source_record_id="source_record:3"),
                raw("2026-07-20T13:30:03.100Z", trade_id="t1-again", source_sequence=4, source_record_id="source_record:1"),
            ],
            capacity=2,
        )

        self.assertNotIn("duplicate_source_trade", [diag.diagnostic_type for diag in diagnostics])
        self.assertIsNotNone(results[-1].eligibility)

    def test_bounded_cache_eviction_sequence_is_deterministic(self) -> None:
        trades = [
            raw("2026-07-20T13:30:00.100Z", trade_id="t1", source_sequence=1, source_record_id="source_record:1"),
            raw("2026-07-20T13:30:01.100Z", trade_id="t2", source_sequence=2, source_record_id="source_record:2"),
            raw("2026-07-20T13:30:02.100Z", trade_id="t3", source_sequence=3, source_record_id="source_record:3"),
            raw("2026-07-20T13:30:03.100Z", trade_id="t4", source_sequence=4, source_record_id="source_record:4"),
        ]
        first = [diag.details for diag in run_bounded_sampler(trades, capacity=2)[2] if diag.diagnostic_type == "source_id_cache_evicted"]
        second = [diag.details for diag in run_bounded_sampler(trades, capacity=2)[2] if diag.diagnostic_type == "source_id_cache_evicted"]

        self.assertEqual(first, second)
        self.assertEqual([item["evicted_source_id"] for item in first], ["source_record:1", "source_record:2"])

    def test_long_synthetic_stream_does_not_grow_seen_state_beyond_capacity(self) -> None:
        trades = [
            raw(f"2026-07-20T13:30:{idx:02d}.100Z", trade_id=f"t{idx}", source_sequence=idx, source_record_id=f"source_record:{idx}")
            for idx in range(10)
        ]
        sampler, _, _, _ = run_bounded_sampler(trades, capacity=3)
        state = sampler.state(("sampler_tsla", "TSLA"))

        self.assertEqual(state.seen_source_id_capacity, 3)
        self.assertEqual(state.seen_source_id_count, 3)
        self.assertEqual(state.seen_source_id_evictions, 7)

    def test_sampler_state_exposes_only_scalar_seen_cache_metadata(self) -> None:
        sampler, _, _, _ = run_bounded_sampler(
            [
                raw("2026-07-20T13:30:00.100Z", trade_id="t1", source_sequence=1, source_record_id="source_record:1"),
                raw("2026-07-20T13:30:01.100Z", trade_id="t2", source_sequence=2, source_record_id="source_record:2"),
            ],
            capacity=2,
        )
        state = sampler.state(("sampler_tsla", "TSLA"))

        self.assertFalse(hasattr(state, "seen_source_trade_ids"))
        self.assertEqual(state.seen_source_id_capacity, 2)
        self.assertEqual(state.seen_source_id_count, 2)
        self.assertEqual(state.seen_source_id_evictions, 0)

    def test_normal_seen_cache_update_path_does_not_copy_full_cache(self) -> None:
        source = inspect.getsource(CanonicalOneSecondSipSampler._remember_source_identity)

        self.assertNotIn("frozenset", source)
        self.assertNotIn("list(seen", source)
        self.assertNotIn("tuple(seen", source)
        self.assertNotIn(".keys())", source)

    def test_seen_cache_update_timing_sanity_across_large_capacities(self) -> None:
        timings: list[float] = []
        for capacity in (100, 10_000, 100_000):
            sampler = CanonicalOneSecondSipSampler(run_id=f"capacity-{capacity}", max_seen_source_ids=capacity)
            start = time.perf_counter()
            for idx in range(125):
                sampler.process_trade(
                    raw(
                        f"2026-07-20T13:{30 + (idx // 60):02d}:{idx % 60:02d}.100Z",
                        trade_id=f"t{capacity}-{idx}",
                        source_sequence=idx,
                        source_record_id=f"source_record:{capacity}:{idx}",
                    )
                )
            timings.append(time.perf_counter() - start)

        # This is a coarse sanity check, not a microsecond benchmark. The 100k
        # capacity path should not show obvious linear work over the 100 path.
        self.assertLess(timings[-1], timings[0] * 20)

    def test_equal_timestamps_with_source_sequence_ordering(self) -> None:
        _, observations, diagnostics, _ = run_sampler(
            [
                raw("2026-07-20T13:30:00.100Z", price="100", trade_id="t1", source_sequence=1),
                raw("2026-07-20T13:30:00.100Z", price="101", trade_id="t2", source_sequence=2),
                raw("2026-07-20T13:30:01.001Z", price="102", trade_id="t3", source_sequence=3),
            ]
        )
        self.assertIn("equal_timestamp_stable_ordering", [diag.diagnostic_type for diag in diagnostics])
        self.assertEqual(observations[0].close_price, D("101"))

    def test_isolated_symbols(self) -> None:
        _, observations, _, _ = run_sampler(
            [
                raw("2026-07-20T13:30:00.100Z", symbol="AAPL", price="200", trade_id="a1", trader_id="sampler"),
                raw("2026-07-20T13:30:00.100Z", symbol="NVDA", price="300", trade_id="n1", trader_id="sampler"),
                raw("2026-07-20T13:30:01.100Z", symbol="AAPL", price="201", trade_id="a2", trader_id="sampler"),
                raw("2026-07-20T13:30:01.100Z", symbol="NVDA", price="301", trade_id="n2", trader_id="sampler"),
            ]
        )
        self.assertEqual([(obs.symbol, obs.close_price) for obs in observations], [("AAPL", D("200")), ("NVDA", D("300"))])

    def test_isolated_trader_source_streams(self) -> None:
        _, observations, _, _ = run_sampler(
            [
                raw("2026-07-20T13:30:00.100Z", price="100", trade_id="a1", trader_id="trader-a"),
                raw("2026-07-20T13:30:00.100Z", price="200", trade_id="b1", trader_id="trader-b"),
                raw("2026-07-20T13:30:01.100Z", price="101", trade_id="a2", trader_id="trader-a"),
                raw("2026-07-20T13:30:01.100Z", price="201", trade_id="b2", trader_id="trader-b"),
            ]
        )
        self.assertEqual([obs.close_price for obs in observations], [D("100"), D("200")])

    def test_deterministic_repeated_runs(self) -> None:
        trades = [
            raw("2026-07-20T13:30:00.100Z", price="100", trade_id="t1", source_sequence=1),
            raw("2026-07-20T13:30:00.900Z", price="101", trade_id="t2", source_sequence=2),
            raw("2026-07-20T13:30:01.001Z", price="102", trade_id="t3", source_sequence=3),
        ]
        first = run_sampler(trades)[1]
        second = run_sampler(trades)[1]
        self.assertEqual(first, second)

    def test_old_versus_new_sampler_shadow_parity(self) -> None:
        fixtures = [
            [
                raw("2026-07-20T13:30:00.100Z", price="100", trade_id="t1", source_sequence=1),
                raw("2026-07-20T13:30:00.900Z", price="101", trade_id="t2", source_sequence=2),
                raw("2026-07-20T13:30:01.001Z", price="102", trade_id="t3", source_sequence=3),
            ],
            [
                raw("2026-07-20T13:30:00.100Z", price="100", trade_id="g1", source_sequence=1),
                raw("2026-07-20T13:30:05.001Z", price="105", trade_id="g2", source_sequence=2),
            ],
            [
                raw("2026-07-20T13:30:02.100Z", price="102", trade_id="o2", source_sequence=2),
                raw("2026-07-20T13:30:01.900Z", price="101", trade_id="o1", source_sequence=1),
                raw("2026-07-20T13:30:03.001Z", price="103", trade_id="o3", source_sequence=3),
            ],
        ]
        for index, fixture in enumerate(fixtures):
            result = compare_with_production_sampler(fixture, run_id=f"shadow-{index}", trader_id="sampler_tsla", symbol="TSLA")
            self.assertTrue(result.matches, result.differences)

    def test_canonical_observation_source_and_triggering_trade_lineage(self) -> None:
        _, observations, _, results = run_sampler(
            [
                raw("2026-07-20T13:30:00.100Z", price="100", trade_id="source", source_sequence=1),
                raw("2026-07-20T13:30:01.001Z", price="102", trade_id="trigger", source_sequence=2),
            ]
        )
        observation = observations[0]
        observation_event = [event for event in results[1].events if event.event_type == "CanonicalObservationCreated"][0]
        self.assertEqual(observation.source_alpaca_trade_id, "source")
        self.assertEqual(observation.triggering_sip_timestamp.isoformat(), "2026-07-20T13:30:01.001000+00:00")
        self.assertIsNotNone(observation.source_eligible_trade_id)
        self.assertEqual(observation.triggering_later_eligible_trade_id, results[1].events[2].event_id)
        self.assertEqual(observation.source_eligible_trade_event_id, observation.source_eligible_trade_id)
        self.assertEqual(observation.triggering_eligible_trade_event_id, results[1].events[2].event_id)
        self.assertEqual(observation.parent_event_id, results[1].events[2].event_id)
        self.assertIsNotNone(observation.root_event_id)
        self.assertEqual(observation_event.parent_event_id, observation.parent_event_id)
        self.assertEqual(observation_event.root_event_id, observation.root_event_id)
        self.assertEqual(observation_event.payload["source_eligible_trade_event_id"], observation.source_eligible_trade_event_id)
        self.assertEqual(observation_event.payload["triggering_eligible_trade_event_id"], observation.triggering_eligible_trade_event_id)

    def test_older_same_second_timestamp_cannot_replace_newer_pending_trade(self) -> None:
        _, observations, diagnostics, _ = run_sampler(
            [
                raw("2026-07-20T13:30:00.200Z", price="101", trade_id="newer", source_sequence=2),
                raw("2026-07-20T13:30:00.100Z", price="100", trade_id="older", source_sequence=3),
                raw("2026-07-20T13:30:01.000Z", price="102", trade_id="trigger", source_sequence=4),
            ]
        )
        self.assertIn("older_same_bucket_trade", [diag.diagnostic_type for diag in diagnostics])
        self.assertEqual(observations[0].close_price, D("101"))
        self.assertEqual(observations[0].source_alpaca_trade_id, "newer")

    def test_same_timestamp_lower_source_sequence_cannot_replace_newer_pending_trade(self) -> None:
        _, observations, diagnostics, _ = run_sampler(
            [
                raw("2026-07-20T13:30:00.100Z", price="101", trade_id="newer", source_sequence=2),
                raw("2026-07-20T13:30:00.100Z", price="100", trade_id="older", source_sequence=1),
                raw("2026-07-20T13:30:01.000Z", price="102", trade_id="trigger", source_sequence=3),
            ]
        )
        self.assertIn("older_same_bucket_trade", [diag.diagnostic_type for diag in diagnostics])
        self.assertEqual(observations[0].close_price, D("101"))

    def test_same_timestamp_higher_source_sequence_replaces_pending_trade(self) -> None:
        _, observations, diagnostics, _ = run_sampler(
            [
                raw("2026-07-20T13:30:00.100Z", price="100", trade_id="older", source_sequence=1),
                raw("2026-07-20T13:30:00.100Z", price="101", trade_id="newer", source_sequence=2),
                raw("2026-07-20T13:30:01.000Z", price="102", trade_id="trigger", source_sequence=3),
            ]
        )
        self.assertIn("equal_timestamp_stable_ordering", [diag.diagnostic_type for diag in diagnostics])
        self.assertEqual(observations[0].close_price, D("101"))

    def test_later_timestamp_in_same_second_replaces_pending_trade(self) -> None:
        _, observations, _, _ = run_sampler(
            [
                raw("2026-07-20T13:30:00.100Z", price="100", trade_id="older", source_sequence=2),
                raw("2026-07-20T13:30:00.200Z", price="101", trade_id="newer", source_sequence=1),
                raw("2026-07-20T13:30:01.000Z", price="102", trade_id="trigger", source_sequence=3),
            ]
        )
        self.assertEqual(observations[0].close_price, D("101"))

    def test_equal_timestamp_equal_sequence_duplicate_identity_does_not_replace_state(self) -> None:
        _, observations, diagnostics, _ = run_sampler(
            [
                raw("2026-07-20T13:30:00.100Z", price="100", trade_id="same", source_sequence=1, row_order=0),
                raw("2026-07-20T13:30:00.100Z", price="101", trade_id="same", source_sequence=1, row_order=0),
                raw("2026-07-20T13:30:01.000Z", price="102", trade_id="trigger", source_sequence=2),
            ]
        )
        self.assertIn("duplicate_source_trade", [diag.diagnostic_type for diag in diagnostics])
        self.assertEqual(observations[0].close_price, D("100"))

    def test_equal_timestamp_equal_sequence_ambiguous_distinct_identity_preserves_pending_state(self) -> None:
        _, observations, diagnostics, _ = run_sampler(
            [
                raw("2026-07-20T13:30:00.100Z", price="100", trade_id="first", source_sequence=1, row_order=0),
                raw("2026-07-20T13:30:00.100Z", price="101", trade_id="second", source_sequence=1, row_order=0),
                raw("2026-07-20T13:30:01.000Z", price="102", trade_id="trigger", source_sequence=2),
            ]
        )
        self.assertIn("ambiguous_equal_ordering_trade", [diag.diagnostic_type for diag in diagnostics])
        self.assertEqual(observations[0].close_price, D("100"))

    def test_older_same_bucket_diagnostic_is_distinct_from_older_prior_bucket(self) -> None:
        _, _, same_bucket_diagnostics, _ = run_sampler(
            [
                raw("2026-07-20T13:30:00.200Z", price="101", trade_id="newer", source_sequence=2),
                raw("2026-07-20T13:30:00.100Z", price="100", trade_id="older", source_sequence=3),
            ]
        )
        _, _, prior_bucket_diagnostics, _ = run_sampler(
            [
                raw("2026-07-20T13:30:02.100Z", price="102", trade_id="pending", source_sequence=2),
                raw("2026-07-20T13:30:01.100Z", price="101", trade_id="older", source_sequence=1),
            ]
        )
        self.assertIn("older_same_bucket_trade", [diag.diagnostic_type for diag in same_bucket_diagnostics])
        self.assertIn("older_prior_bucket_trade", [diag.diagnostic_type for diag in prior_bucket_diagnostics])

    def test_emitted_close_uses_canonically_newest_trade(self) -> None:
        _, observations, _, _ = run_sampler(
            [
                raw("2026-07-20T13:30:00.100Z", price="100", trade_id="first", source_sequence=1, row_order=0),
                raw("2026-07-20T13:30:00.100Z", price="101", trade_id="second", source_sequence=1, row_order=1),
                raw("2026-07-20T13:30:00.200Z", price="102", trade_id="third", source_sequence=0, row_order=0),
                raw("2026-07-20T13:30:01.000Z", price="103", trade_id="trigger", source_sequence=3),
            ]
        )
        self.assertEqual(observations[0].close_price, D("102"))
        self.assertEqual(observations[0].source_alpaca_trade_id, "third")

    def test_observation_created_lineage_matches_observation_lineage(self) -> None:
        _, observations, _, results = run_sampler(
            [
                raw("2026-07-20T13:30:00.100Z", price="100", trade_id="source", source_sequence=1),
                raw("2026-07-20T13:30:01.000Z", price="101", trade_id="trigger", source_sequence=2),
            ]
        )
        observation = observations[0]
        observation_event = [event for event in results[1].events if event.event_type == "CanonicalObservationCreated"][0]
        self.assertEqual(observation.root_event_id, results[0].events[0].event_id)
        self.assertEqual(observation.parent_event_id, results[1].events[2].event_id)
        self.assertEqual(observation_event.root_event_id, observation.root_event_id)
        self.assertEqual(observation_event.parent_event_id, observation.parent_event_id)

    def test_ordering_and_lineage_deterministic_repeated_processing(self) -> None:
        trades = [
            raw("2026-07-20T13:30:00.100Z", price="100", trade_id="first", source_sequence=1),
            raw("2026-07-20T13:30:00.100Z", price="101", trade_id="second", source_sequence=2),
            raw("2026-07-20T13:30:00.100Z", price="99", trade_id="ambiguous", source_sequence=2),
            raw("2026-07-20T13:30:01.000Z", price="102", trade_id="trigger", source_sequence=3),
        ]
        first = run_sampler(trades)
        second = run_sampler(trades)
        self.assertEqual(first[1], second[1])
        self.assertEqual(first[2], second[2])
        self.assertEqual([result.events for result in first[3]], [result.events for result in second[3]])


if __name__ == "__main__":
    unittest.main()
