from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from scripts.tick_retention import (
    EASTERN,
    is_nyse_session,
    purge_tick_data,
    retained_nyse_sessions,
)


class TickRetentionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "ticks"
        self.root.mkdir()
        self.logs: list[dict[str, object]] = []

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _sessions_ending(self, end: str, count: int) -> list[str]:
        cursor = datetime.fromisoformat(f"{end}T18:15:00").replace(tzinfo=EASTERN).date()
        sessions: list[str] = []
        while len(sessions) < count:
            if is_nyse_session(cursor):
                sessions.append(cursor.isoformat())
            cursor -= timedelta(days=1)
        sessions.reverse()
        return sessions

    def _write_tick(self, session: str, symbol: str = "AAPL", text: str = "{}\n") -> Path:
        path = self.root / session / f"{symbol}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return path

    def _purge(self, *, as_of: str, dry_run: bool = True, retention: int = 15) -> dict[str, object]:
        return purge_tick_data(
            retention_trading_days=retention,
            dry_run=dry_run,
            tick_root=self.root,
            as_of=as_of,
            lock_path=Path(self.tmp.name) / "retention.lock",
            log_sink=self.logs.append,
        )

    def test_normal_weekday_retention_selects_sessions_older_than_cutoff(self) -> None:
        sessions = self._sessions_ending("2026-08-19", 20)
        for session in sessions:
            self._write_tick(session, "AAPL", "aa")

        report = self._purge(as_of="2026-08-19T18:15:00-04:00")

        self.assertEqual(report["cutoff_session"], sessions[-15])
        self.assertEqual(report["retained_sessions"], sessions[-15:])
        self.assertEqual([item["session_date"] for item in report["files_selected"]], sessions[:5])
        self.assertEqual(report["bytes_selected"], 10)

    def test_weekend_execution_uses_most_recent_completed_session(self) -> None:
        sessions = self._sessions_ending("2026-08-21", 16)
        for session in sessions:
            self._write_tick(session)

        report = self._purge(as_of="2026-08-23T12:00:00-04:00")

        self.assertEqual(report["retained_sessions"][-1], "2026-08-21")
        self.assertEqual(report["current_session"], None)
        self.assertEqual([item["session_date"] for item in report["files_selected"]], [sessions[0]])

    def test_nyse_holiday_execution_uses_prior_completed_session(self) -> None:
        sessions = self._sessions_ending("2026-09-04", 16)
        for session in sessions:
            self._write_tick(session)

        report = self._purge(as_of="2026-09-07T12:00:00-04:00")

        self.assertEqual(report["retained_sessions"][-1], "2026-09-04")
        self.assertNotIn("2026-09-07", report["retained_sessions"])
        self.assertEqual([item["session_date"] for item in report["files_selected"]], [sessions[0]])

    def test_exactly_15_sessions_deletes_nothing(self) -> None:
        sessions = self._sessions_ending("2026-08-19", 15)
        for session in sessions:
            self._write_tick(session)

        report = self._purge(as_of="2026-08-19T18:15:00-04:00")

        self.assertEqual(report["files_selected"], [])
        self.assertEqual(report["selection_suppressed_reason"], None)

    def test_fewer_than_15_sessions_deletes_nothing(self) -> None:
        sessions = self._sessions_ending("2026-08-19", 14)
        for session in sessions:
            self._write_tick(session)

        report = self._purge(as_of="2026-08-19T18:15:00-04:00")

        self.assertEqual(report["files_selected"], [])
        self.assertEqual(report["selection_suppressed_reason"], "fewer_than_retention_sessions")

    def test_multiple_assets_and_missing_asset_sessions(self) -> None:
        sessions = self._sessions_ending("2026-08-19", 18)
        for session in sessions:
            self._write_tick(session, "AAPL")
        for session in sessions[3:]:
            self._write_tick(session, "NVDA")

        report = self._purge(as_of="2026-08-19T18:15:00-04:00")
        selected = {(item["session_date"], Path(item["path"]).name) for item in report["files_selected"]}

        self.assertEqual(len(selected), 3)
        self.assertEqual(selected, {(session, "AAPL.jsonl") for session in sessions[:3]})

    def test_current_open_session_is_protected(self) -> None:
        sessions = self._sessions_ending("2026-08-19", 16)
        for session in sessions:
            self._write_tick(session)

        report = self._purge(as_of="2026-08-19T10:00:00-04:00")

        self.assertEqual(report["current_session"], "2026-08-19")
        self.assertIn("2026-08-19", report["retained_sessions"])
        self.assertNotIn("2026-08-19", [item["session_date"] for item in report["files_selected"]])

    def test_malformed_unknown_and_non_session_paths_are_skipped(self) -> None:
        sessions = self._sessions_ending("2026-08-19", 16)
        for session in sessions:
            self._write_tick(session)
        (self.root / "README.txt").write_text("not ticks", encoding="utf-8")
        (self.root / "bad-date").mkdir()
        (self.root / "bad-date" / "AAPL.jsonl").write_text("bad", encoding="utf-8")
        self._write_tick("2026-08-15", "AAPL")
        (self.root / sessions[0] / "AAPL.txt").write_text("bad suffix", encoding="utf-8")

        report = self._purge(as_of="2026-08-19T18:15:00-04:00")
        reasons = {item["reason"] for item in report["skipped"]}

        self.assertIn("unrecognized_tick_path", reasons)
        self.assertIn("not_nyse_session", reasons)

    def test_symlinks_are_skipped_and_not_followed(self) -> None:
        sessions = self._sessions_ending("2026-08-19", 16)
        outside = Path(self.tmp.name) / "outside.jsonl"
        outside.write_text("keep", encoding="utf-8")
        for session in sessions:
            self._write_tick(session)
        symlink_path = self.root / sessions[0] / "MSFT.jsonl"
        symlink_path.symlink_to(outside)

        report = self._purge(as_of="2026-08-19T18:15:00-04:00", dry_run=False)

        self.assertTrue(outside.exists())
        skipped = {(Path(item["path"]).name, item["reason"]) for item in report["skipped"]}
        self.assertIn(("MSFT.jsonl", "symlink"), skipped)

    def test_dry_run_does_not_delete_selected_files(self) -> None:
        sessions = self._sessions_ending("2026-08-19", 16)
        old_file = self._write_tick(sessions[0])
        for session in sessions[1:]:
            self._write_tick(session)

        report = self._purge(as_of="2026-08-19T18:15:00-04:00", dry_run=True)

        self.assertTrue(old_file.exists())
        self.assertEqual(len(report["files_selected"]), 1)
        self.assertEqual(report["files_deleted"], [])
        self.assertEqual(self.logs, [])

    def test_real_purge_is_idempotent(self) -> None:
        sessions = self._sessions_ending("2026-08-19", 16)
        old_file = self._write_tick(sessions[0])
        for session in sessions[1:]:
            self._write_tick(session)

        first = self._purge(as_of="2026-08-19T18:15:00-04:00", dry_run=False)
        second = self._purge(as_of="2026-08-19T18:15:00-04:00", dry_run=False)

        self.assertFalse(old_file.exists())
        self.assertEqual(len(first["files_deleted"]), 1)
        self.assertEqual(second["files_selected"], [])

    def test_deletion_is_confined_to_tick_root(self) -> None:
        sessions = self._sessions_ending("2026-08-19", 16)
        outside = Path(self.tmp.name) / "outside.jsonl"
        outside.write_text("keep", encoding="utf-8")
        for session in sessions:
            self._write_tick(session)
        escape = self.root / sessions[0] / "ESCAPE.jsonl"
        escape.symlink_to(outside)

        self._purge(as_of="2026-08-19T18:15:00-04:00", dry_run=False)

        self.assertTrue(outside.exists())
        self.assertEqual(outside.read_text(encoding="utf-8"), "keep")

    def test_daylight_saving_start_boundary(self) -> None:
        retained, current = retained_nyse_sessions(15, datetime(2026, 3, 9, 18, 15, tzinfo=ZoneInfo("America/New_York")))

        self.assertEqual(retained[-1].isoformat(), "2026-03-09")
        self.assertEqual(current.isoformat(), "2026-03-09")

    def test_daylight_saving_end_boundary(self) -> None:
        retained, current = retained_nyse_sessions(15, datetime(2026, 11, 2, 18, 15, tzinfo=ZoneInfo("America/New_York")))

        self.assertEqual(retained[-1].isoformat(), "2026-11-02")
        self.assertEqual(current.isoformat(), "2026-11-02")


if __name__ == "__main__":
    unittest.main()
