from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd


JOURNAL_RECORD_VERSION = "canonical_journal.v1"


@dataclass(frozen=True)
class JournalCorruption:
    line_number: int
    byte_offset: int
    reason: str
    symbol: str | None = None
    sequence: int | None = None
    quarantine_path: str | None = None


@dataclass(frozen=True)
class JournalRecovery:
    events: tuple[dict[str, Any], ...]
    corrupted_records: int
    corruption: JournalCorruption | None = None
    latest_sequences: dict[str, int] = field(default_factory=dict)

    @property
    def healthy(self) -> bool:
        return self.corruption is None


class CanonicalJournal:
    def __init__(self, path: str | Path, *, max_replay_records: int = 10000) -> None:
        self.path = Path(path)
        self.max_replay_records = int(max_replay_records)
        if self.max_replay_records <= 0:
            raise ValueError("max_replay_records must be positive.")
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, event: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        raw = self.encode_record(event) + b"\n"
        fd = os.open(self.path, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o640)
        try:
            os.write(fd, raw)
            os.fsync(fd)
        finally:
            os.close(fd)

    @staticmethod
    def payload_checksum(event: dict[str, Any]) -> str:
        raw = json.dumps(event, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        return sha256(raw).hexdigest()

    @classmethod
    def encode_record(cls, event: dict[str, Any]) -> bytes:
        record = {
            "type": "journal_record",
            "version": JOURNAL_RECORD_VERSION,
            "payload": event,
            "payload_sha256": cls.payload_checksum(event),
        }
        return json.dumps(record, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")

    def _quarantine(self, *, line_number: int, byte_offset: int, reason: str, raw_line: str) -> str:
        path = self.path.with_suffix(self.path.suffix + ".corrupt.jsonl")
        payload = {
            "detected_at": pd.Timestamp.now(tz="UTC").isoformat(),
            "journal_path": str(self.path),
            "line_number": line_number,
            "byte_offset": byte_offset,
            "reason": reason,
            "raw_line": raw_line.rstrip("\n"),
        }
        line = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8") + b"\n"
        fd = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o640)
        try:
            os.write(fd, line)
            os.fsync(fd)
        finally:
            os.close(fd)
        return str(path)

    def _corruption(
        self,
        *,
        line_number: int,
        byte_offset: int,
        reason: str,
        raw_line: str,
        symbol: str | None = None,
        sequence: int | None = None,
    ) -> JournalRecovery:
        quarantine_path = self._quarantine(
            line_number=line_number,
            byte_offset=byte_offset,
            reason=reason,
            raw_line=raw_line,
        )
        return JournalRecovery(
            events=tuple(self._events),
            corrupted_records=1,
            corruption=JournalCorruption(
                line_number=line_number,
                byte_offset=byte_offset,
                reason=reason,
                symbol=symbol,
                sequence=sequence,
                quarantine_path=quarantine_path,
            ),
            latest_sequences=dict(self._latest_sequences),
        )

    def recover(self) -> JournalRecovery:
        if not self.path.exists():
            return JournalRecovery(events=tuple(), corrupted_records=0)
        self._events: list[dict[str, Any]] = []
        self._latest_sequences: dict[str, int] = {}
        with self.path.open("r", encoding="utf-8") as handle:
            physical_line_number = 0
            while True:
                byte_offset = handle.tell()
                line = handle.readline()
                if not line:
                    break
                physical_line_number += 1
                if not line.strip():
                    continue
                line_number = physical_line_number
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    return self._corruption(
                        line_number=line_number,
                        byte_offset=byte_offset,
                        reason="invalid_json_or_truncated_record",
                        raw_line=line,
                    )
                if isinstance(record, dict) and record.get("type") == "journal_record":
                    if record.get("version") != JOURNAL_RECORD_VERSION:
                        return self._corruption(
                            line_number=line_number,
                            byte_offset=byte_offset,
                            reason="unsupported_journal_record_version",
                            raw_line=line,
                        )
                    payload = record.get("payload")
                    expected_checksum = record.get("payload_sha256")
                    if not isinstance(payload, dict):
                        return self._corruption(
                            line_number=line_number,
                            byte_offset=byte_offset,
                            reason="missing_payload",
                            raw_line=line,
                        )
                    actual_checksum = self.payload_checksum(payload)
                    if expected_checksum != actual_checksum:
                        return self._corruption(
                            line_number=line_number,
                            byte_offset=byte_offset,
                            reason="checksum_mismatch",
                            raw_line=line,
                            symbol=str(payload.get("symbol") or "").upper() or None,
                            sequence=int(payload.get("sequence") or 0) or None,
                        )
                elif isinstance(record, dict) and record.get("type") == "observation":
                    payload = record
                else:
                    return self._corruption(
                        line_number=line_number,
                        byte_offset=byte_offset,
                        reason="not_observation_record",
                        raw_line=line,
                    )

                symbol = str(payload.get("symbol") or "").upper()
                sequence = int(payload.get("sequence") or 0)
                if not symbol or sequence <= 0:
                    return self._corruption(
                        line_number=line_number,
                        byte_offset=byte_offset,
                        reason="missing_symbol_or_sequence",
                        raw_line=line,
                        symbol=symbol or None,
                        sequence=sequence or None,
                    )
                expected_sequence = self._latest_sequences.get(symbol, 0) + 1
                if sequence != expected_sequence:
                    return self._corruption(
                        line_number=line_number,
                        byte_offset=byte_offset,
                        reason="non_monotonic_sequence",
                        raw_line=line,
                        symbol=symbol,
                        sequence=sequence,
                    )
                self._latest_sequences[symbol] = sequence
                self._events.append(payload)
        return JournalRecovery(
            events=tuple(self._events[-self.max_replay_records :]),
            corrupted_records=0,
            latest_sequences=dict(self._latest_sequences),
        )

    def replay_after(self, *, symbol: str, last_sequence: int, through_sequence: int | None = None) -> tuple[dict[str, Any], ...]:
        recovered = self.recover()
        symbol_text = symbol.upper()
        upper = int(through_sequence) if through_sequence is not None else None
        return tuple(
            event
            for event in recovered.events
            if str(event.get("symbol") or "").upper() == symbol_text
            and int(event.get("sequence") or 0) > int(last_sequence)
            and (upper is None or int(event.get("sequence") or 0) <= upper)
        )

    def oldest_sequence_by_symbol(self) -> dict[str, int]:
        recovered = self.recover()
        oldest: dict[str, int] = {}
        for event in recovered.events:
            symbol = str(event.get("symbol") or "").upper()
            sequence = int(event.get("sequence") or 0)
            if symbol and sequence > 0 and symbol not in oldest:
                oldest[symbol] = sequence
        return oldest

    def latest_sequence_by_symbol(self) -> dict[str, int]:
        recovered = self.recover()
        latest: dict[str, int] = {}
        for event in recovered.events:
            symbol = str(event.get("symbol") or "").upper()
            sequence = int(event.get("sequence") or 0)
            if symbol:
                latest[symbol] = max(sequence, latest.get(symbol, 0))
        return latest


def session_archive_path(root: str | Path, *, kind: str, timestamp: object, symbol: str) -> Path:
    day = pd.Timestamp(timestamp).tz_convert("America/New_York").date().isoformat()
    return Path(root) / kind / day / f"{symbol.upper()}.jsonl"


def append_jsonl_atomic(path: str | Path, payload: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8") + b"\n"
    fd = os.open(target, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o640)
    try:
        os.write(fd, raw)
        os.fsync(fd)
    finally:
        os.close(fd)


def purge_old_session_dirs(root: str | Path, *, keep_sessions: int) -> list[str]:
    base = Path(root)
    if keep_sessions <= 0 or not base.exists():
        return []
    purged: list[str] = []
    for kind in ("raw", "canonical"):
        kind_root = base / kind
        if not kind_root.exists():
            continue
        sessions = sorted(path for path in kind_root.iterdir() if path.is_dir())
        for path in sessions[:-keep_sessions]:
            for child in path.glob("*"):
                child.unlink()
            path.rmdir()
            purged.append(str(path))
    return purged
