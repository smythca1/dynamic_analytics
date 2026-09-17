from __future__ import annotations

from datetime import datetime, timezone
from unittest import TestCase, mock

import pandas as pd

from silver.market.live_event_stream import live_market_event_from_alpaca_message
from trading_events import utc_timestamp, utc_timestamp_with_info


def pandas_utc(value):
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        raise ValueError(f"Could not parse timestamp: {value}")
    return pd.Timestamp(timestamp)


class TimestampParsingTests(TestCase):
    def assertTimestampEqual(self, left, right):
        self.assertEqual(left.value, right.value)
        self.assertEqual(str(left.tz), str(right.tz))

    def test_fast_iso_corpus_matches_existing_pandas_contract(self):
        values = [
            "2026-08-26T14:30:01Z",
            "2026-08-26T14:30:01.1Z",
            "2026-08-26T14:30:01.123456Z",
            "2026-08-26T14:30:01.123456789Z",
            "2026-08-26T14:30:01.999999999+00:00",
            "2026-08-26T10:30:01.123456789-04:00",
            "2026-11-01T01:30:00.000000001-04:00",
            "2026-11-01T01:30:00.000000001-05:00",
            "2026-08-26 14:30:01.123456789",
        ]
        for value in values:
            with self.subTest(value=value):
                parsed, info = utc_timestamp_with_info(value)
                self.assertFalse(info.used_pandas_fallback)
                self.assertEqual(info.path, "iso_string_fast")
                self.assertTimestampEqual(parsed, pandas_utc(value))

    def test_datetime_and_timestamp_inputs_match_existing_contract(self):
        values = [
            datetime(2026, 8, 26, 14, 30, 1, 123456),
            datetime(2026, 8, 26, 10, 30, 1, 123456, tzinfo=timezone.utc),
            pd.Timestamp("2026-08-26T14:30:01.123456789Z"),
            pd.Timestamp("2026-08-26T14:30:01.123456789"),
        ]
        for value in values:
            with self.subTest(value=repr(value)):
                self.assertTimestampEqual(utc_timestamp(value), pandas_utc(value))

    def test_numeric_inputs_preserve_existing_pandas_nanosecond_units(self):
        for value in [0, 1, 1_700_000_000, 1.0]:
            with self.subTest(value=value):
                parsed, info = utc_timestamp_with_info(value)
                self.assertTrue(info.used_pandas_fallback)
                self.assertTimestampEqual(parsed, pandas_utc(value))

    def test_invalid_missing_and_non_finite_values_raise(self):
        for value in [None, "", "not-a-timestamp", float("nan"), float("inf")]:
            with self.subTest(value=repr(value)):
                with self.assertRaises(ValueError):
                    utc_timestamp(value)

    def test_live_sip_string_fast_path_does_not_call_pandas_to_datetime(self):
        value = "2026-08-26T14:30:01.123456789Z"
        with mock.patch("trading_events.pd.to_datetime", side_effect=AssertionError("slow pandas path called")):
            parsed, info = utc_timestamp_with_info(value)
        self.assertEqual(parsed.isoformat(), "2026-08-26T14:30:01.123456789+00:00")
        self.assertEqual(info.path, "iso_string_fast")

    def test_live_event_stream_preserves_event_identity_and_bucket_boundary_inputs(self):
        message = {
            "T": "t",
            "S": "AAPL",
            "t": "2026-08-26T14:30:01.999999999Z",
            "p": 101.25,
            "s": 100,
            "x": "V",
            "i": "trade-1",
            "z": "C",
            "c": ["@"],
        }
        event = live_market_event_from_alpaca_message(message, source_sequence=7)
        self.assertEqual(event.tick.timestamp.isoformat(), "2026-08-26T14:30:01.999999999+00:00")
        self.assertEqual(event.tick.timestamp.floor("s").isoformat(), "2026-08-26T14:30:01+00:00")
        self.assertEqual(event.source_sequence, 7)
        self.assertTrue(event.eligible)


if __name__ == "__main__":
    import unittest

    unittest.main()
