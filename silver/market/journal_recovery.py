"""Operator-only, evidence-preserving repair of a proven rotation boundary.

Ingestion must be stopped; the shared service singleton lock is held throughout.
Market observations are never changed. Ambiguous inputs fail before replacement.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from .canonical_journal import CanonicalJournal, JOURNAL_RECORD_VERSION
from .contract import market_stream_contract
from .ipc_protocol import canonical_event_id


def file_hash(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def scan_segment(path: Path, initial: dict[str, int] | None = None) -> dict:
    """Verify every record; only an archive's first per-symbol value is an anchor."""
    latest = dict(initial or {})
    first = {}
    timestamps = {}
    fingerprints = {}
    count = 0
    with path.open("rb") as handle:
        for number, line in enumerate(handle, 1):
            try:
                if not line.endswith(b"\n"):
                    raise ValueError("incomplete write")
                record = json.loads(line)
                if record.get("type") != "journal_record" or record.get("version") != JOURNAL_RECORD_VERSION:
                    raise ValueError("unsupported journal record")
                event = record["payload"]
                if record["payload_sha256"] != CanonicalJournal.payload_checksum(event):
                    raise ValueError("checksum mismatch")
                symbol, sequence = event["symbol"], event["sequence"]
                if not isinstance(symbol, str) or not symbol or symbol != symbol.upper():
                    raise ValueError("invalid symbol")
                if type(sequence) is not int or sequence <= 0 or event.get("type") != "observation":
                    raise ValueError("invalid observation sequence")
                expected = latest.get(symbol, sequence - 1 if initial is None else 0) + 1
                if sequence != expected:
                    raise ValueError(f"sequence {sequence} != {expected}")
                if symbol not in fingerprints:
                    fingerprints[symbol] = market_stream_contract(symbol=symbol, session_timezone="America/New_York").fingerprint
                fingerprint = fingerprints[symbol]
                if event["contract_fingerprint"] != fingerprint:
                    raise ValueError("canonical fingerprint mismatch")
                identity = canonical_event_id(
                    contract_fingerprint=fingerprint, symbol=symbol, sequence=sequence,
                    observation_timestamp=event["observation_timestamp"],
                    close_price=event["close_price"], source_record_id=event["source_record_id"],
                )
                if event["event_id"] != identity:
                    raise ValueError("event identity mismatch")
                timestamp = datetime.fromisoformat(event["observation_timestamp"].replace("Z", "+00:00"))
                if timestamp.tzinfo is None or (symbol in timestamps and timestamp <= timestamps[symbol]):
                    raise ValueError("non-increasing observation timestamp")
                timestamps[symbol] = timestamp
                first.setdefault(symbol, sequence)
                latest[symbol] = sequence
                count += 1
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(f"{path}:{number}: {exc}") from exc
    if not count:
        raise ValueError(f"empty segment: {path}")
    return {"records": count, "first_sequences": first, "latest_sequences": latest,
            "last_timestamps": {s: t.isoformat() for s, t in timestamps.items()}}


def plan_recovery(journal: Path) -> dict:
    checkpoint = journal.with_suffix(journal.suffix + ".checkpoint.json")
    original = json.loads(checkpoint.read_text())
    if original.get("version") != 1 or not isinstance(original.get("latest_sequences"), dict):
        raise ValueError("unsupported rotation checkpoint")
    archive = Path(original["archived_journal"])
    if not archive.is_absolute():
        archive = journal.parent / archive
    if archive.resolve().parent != journal.resolve().parent or archive.resolve() == journal.resolve():
        raise ValueError("archive must be a distinct adjacent journal segment")
    archived = scan_segment(archive)
    durable = archived["latest_sequences"]
    recorded = original["latest_sequences"]
    if set(recorded) != set(durable) or any(type(v) is not int or v < 0 for v in recorded.values()):
        raise ValueError("checkpoint symbols/values do not match archive")
    advanced = [s for s in durable if recorded[s] != durable[s]]
    if len(advanced) != 1 or recorded[advanced[0]] != durable[advanced[0]] + 1:
        raise ValueError("not exactly one proven in-flight rotation boundary")
    active = scan_segment(journal, durable)
    symbol = advanced[0]
    if active["first_sequences"].get(symbol) != recorded[symbol]:
        raise ValueError("in-flight event not preserved at active boundary")
    # Both segments must continue forward in event time as well as sequence.
    with journal.open("rb") as handle:
        seen = set()
        for line in handle:
            event = json.loads(line)["payload"]
            s = event["symbol"]
            if s not in seen and s in archived["last_timestamps"]:
                before = datetime.fromisoformat(archived["last_timestamps"][s])
                after = datetime.fromisoformat(event["observation_timestamp"].replace("Z", "+00:00"))
                if after <= before:
                    raise ValueError("observation timestamp overlap at rotation boundary")
            seen.add(s)
            if seen >= set(active["first_sequences"]):
                break
    sources = {"journal": journal, "archive": archive, "checkpoint": checkpoint}
    quarantine = journal.with_suffix(journal.suffix + ".corrupt.jsonl")
    if quarantine.exists():
        sources["quarantine"] = quarantine
    return {"reason": "rotation_checkpoint_includes_unappended_event", "advanced_symbol": symbol,
            "original_checkpoint": original, "recovered_checkpoint": dict(original, latest_sequences=durable),
            "archive_verification": archived, "active_verification": active,
            "sources": {k: {"path": str(p), "sha256": file_hash(p)} for k, p in sources.items()}}


def durable_json(path: Path, value: dict) -> None:
    with path.open("x", encoding="utf-8") as handle:
        handle.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def apply_recovery(journal: Path, evidence_root: Path) -> dict:
    plan = plan_recovery(journal)
    evidence = evidence_root / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:12])
    evidence.mkdir(parents=True, exist_ok=False)
    # Capture original bytes before modifying metadata, including the quarantine.
    for name, source in plan["sources"].items():
        original = Path(source["path"])
        destination = evidence / (name + ".original")
        shutil.copyfile(original, destination)
        with destination.open("rb") as handle:
            os.fsync(handle.fileno())
        if file_hash(destination) != source["sha256"] or file_hash(original) != source["sha256"]:
            raise ValueError("input changed during evidence capture")
        destination.chmod(0o440)
    audit = dict(plan, status="PREPARED", evidence_path=str(evidence),
                 prepared_at_utc=datetime.now(timezone.utc).isoformat())
    durable_json(evidence / "prepared.json", audit)
    directory = os.open(evidence, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    directory = os.open(evidence_root, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    checkpoint = Path(plan["sources"]["checkpoint"]["path"])
    replacement = checkpoint.with_name(checkpoint.name + ".recovery-" + uuid4().hex)
    durable_json(replacement, plan["recovered_checkpoint"])
    os.replace(replacement, checkpoint)
    directory = os.open(checkpoint.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    recovered = CanonicalJournal(journal).recover()
    if not recovered.healthy or recovered.latest_sequences != plan["active_verification"]["latest_sequences"]:
        raise RuntimeError("post-repair canonical recovery failed; keep ingestion stopped")
    audit.update(status="PASS", checkpoint_sha256=file_hash(checkpoint),
                 completed_at_utc=datetime.now(timezone.utc).isoformat())
    durable_json(evidence / "result.json", audit)
    directory = os.open(evidence, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return audit


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal", required=True, type=Path)
    parser.add_argument("--lock-path", required=True, type=Path)
    parser.add_argument("--evidence-root", type=Path)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.apply and args.evidence_root is None:
        parser.error("--apply requires --evidence-root")
    from .shared_service import SingletonLock
    lock = SingletonLock(args.lock_path)
    lock.acquire()
    try:
        result = apply_recovery(args.journal, args.evidence_root) if args.apply else plan_recovery(args.journal)
        print(json.dumps(result, sort_keys=True))
    finally:
        lock.release()


if __name__ == "__main__":
    main()
