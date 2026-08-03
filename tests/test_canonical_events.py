from __future__ import annotations

import unittest

from silver.events import CanonicalEvent


class CanonicalEventTests(unittest.TestCase):
    def test_deterministic_event_ids(self) -> None:
        first = CanonicalEvent.build(
            event_type="RawSipTradeReceived",
            event_time="2026-07-20T13:30:00Z",
            run_id="replay-1",
            source="unit",
            symbol="TSLA",
            sequence=1,
            stable_source_identity="unit:TSLA:t1",
        )
        second = CanonicalEvent.build(
            event_type="RawSipTradeReceived",
            event_time="2026-07-20T13:30:00Z",
            run_id="replay-1",
            source="unit",
            symbol="TSLA",
            sequence=1,
            stable_source_identity="unit:TSLA:t1",
        )

        self.assertEqual(first.event_id, second.event_id)

    def test_parent_root_lineage(self) -> None:
        root = CanonicalEvent.build(
            event_type="RawSipTradeReceived",
            event_time="2026-07-20T13:30:00Z",
            run_id="replay-1",
            source="unit",
            symbol="TSLA",
            sequence=1,
            stable_source_identity="unit:TSLA:t1",
        )
        child = CanonicalEvent.build(
            event_type="TradeEligibilityEvaluated",
            event_time="2026-07-20T13:30:00Z",
            run_id="replay-1",
            source="unit",
            symbol="TSLA",
            sequence=2,
            stable_source_identity="unit:TSLA:t1:eligibility",
            parent_event_id=root.event_id,
            root_event_id=root.root_event_id,
        )

        self.assertEqual(child.parent_event_id, root.event_id)
        self.assertEqual(child.root_event_id, root.event_id)

    def test_stable_event_sequence_changes_identity(self) -> None:
        first = CanonicalEvent.build(
            event_type="RawSipTradeReceived",
            event_time="2026-07-20T13:30:00Z",
            run_id="replay-1",
            source="unit",
            symbol="TSLA",
            sequence=1,
            stable_source_identity="unit:TSLA:t1",
        )
        second = CanonicalEvent.build(
            event_type="RawSipTradeReceived",
            event_time="2026-07-20T13:30:00Z",
            run_id="replay-1",
            source="unit",
            symbol="TSLA",
            sequence=2,
            stable_source_identity="unit:TSLA:t1",
        )

        self.assertNotEqual(first.event_id, second.event_id)
        self.assertEqual((first.sequence, second.sequence), (1, 2))


if __name__ == "__main__":
    unittest.main()
