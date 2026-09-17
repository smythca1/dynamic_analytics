from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import subprocess
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd

from silver.market import SharedMarketDataConfig
from silver.market.canonical_journal import CanonicalJournal
from silver.market.ipc_protocol import read_message, write_message


def utc_now() -> str:
    return pd.Timestamp.now(tz="UTC").isoformat()


def payload_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


class ActiveValidation:
    def __init__(self, *, config_path: Path, evidence_dir: Path, duration_seconds: int) -> None:
        self.config = SharedMarketDataConfig.load(config_path)
        self.evidence_dir = evidence_dir
        self.duration_seconds = int(duration_seconds)
        self.report_path = evidence_dir / "active_stream_validation_report.json"
        self.samples_path = evidence_dir / "active_stream_validation_samples.jsonl"
        self.symbols = set(self.config.symbols)
        self.report: dict[str, Any] = {
            "status": "running",
            "started_at": utc_now(),
            "duration_seconds": self.duration_seconds,
            "config": {
                "symbols": sorted(self.symbols),
                "socket_path": self.config.socket_path,
                "journal_path": self.config.journal_path,
                "health_path": self.config.health_path,
                "feed": self.config.feed,
                "canonical_source": self.config.canonical_source,
            },
            "phases": [],
        }

    def save(self, status: str | None = None, error: Exception | str | None = None) -> None:
        if status is not None:
            self.report["status"] = status
        if error is not None:
            self.report["error"] = str(error)
        self.report["updated_at"] = utc_now()
        self.report_path.write_text(json.dumps(self.report, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")

    def health(self) -> dict[str, Any]:
        path = Path(self.config.health_path)
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    def journal_head(self) -> dict[str, Any]:
        journal = CanonicalJournal(self.config.journal_path, max_replay_records=self.config.replay_limit_records)
        recovery = journal.recover()
        return {
            "healthy": recovery.healthy,
            "corrupted_records": recovery.corrupted_records,
            "latest": recovery.latest_sequences,
            "oldest": journal.oldest_sequence_by_symbol() if recovery.healthy else {},
            "size_bytes": Path(self.config.journal_path).stat().st_size if Path(self.config.journal_path).exists() else 0,
        }

    def service_snapshot(self) -> dict[str, Any]:
        fields = ["ActiveState", "SubState", "MainPID", "NRestarts", "Result"]
        units = ["trading-market-data-live.service", "trading-fleet-live.service", "trading-fleet-megatron-live.service"]
        result: dict[str, Any] = {}
        for unit in units:
            proc = subprocess.run(
                ["systemctl", "show", unit, *[f"-p{field}" for field in fields]],
                text=True,
                capture_output=True,
                check=False,
            )
            result[unit] = dict(line.split("=", 1) for line in proc.stdout.splitlines() if "=" in line)
        return result

    async def sample_loop(self, consumers: dict[str, "RawConsumer"], stop_event: asyncio.Event) -> None:
        sample_count = 0
        while not stop_event.is_set():
            sample = {
                "timestamp": utc_now(),
                "health": self.health(),
                "journal": self.journal_head() if sample_count % 30 == 0 else None,
                "services": self.service_snapshot(),
                "consumers": {identity: consumer.summary() for identity, consumer in consumers.items()},
            }
            with self.samples_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(sample, sort_keys=True, default=str) + "\n")
            sample_count += 1
            await asyncio.sleep(1)

    async def wait_for_fresh_health(self, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            health = self.health()
            ages = health.get("observation_age_seconds") or {}
            if (
                health.get("sip_connected")
                and health.get("subscription_acknowledged")
                and all(float(ages.get(symbol, 999999)) <= self.config.freshness_threshold_seconds for symbol in self.symbols)
            ):
                return True
            await asyncio.sleep(0.5)
        return False

    async def wait_until_caught_up(self, consumer: "RawConsumer", timeout: float, *, min_new_events: int = 1) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if consumer.catchup_complete and len(consumer.events) >= min_new_events and not consumer.gaps:
                return True
            await asyncio.sleep(0.5)
        return False

    async def run(self) -> None:
        self.save()
        initial_head = self.journal_head()["latest"]
        cam = RawConsumer("camatron", self, initial_head, self.evidence_dir / "active_camatron_state.json")
        meg = RawConsumer("megatron", self, initial_head, self.evidence_dir / "active_megatron_state.json")
        consumers = {"camatron": cam, "megatron": meg}
        stop = asyncio.Event()
        sampler = asyncio.create_task(self.sample_loop(consumers, stop))
        try:
            if not await self.wait_for_fresh_health(timeout=120):
                raise RuntimeError(f"fresh health not established: {self.health().get('observation_age_seconds')}")
            await cam.connect()
            await meg.connect()
            if not await self.wait_until_caught_up(cam, timeout=120):
                raise RuntimeError("camatron did not reach catchup/head")
            if not await self.wait_until_caught_up(meg, timeout=120):
                raise RuntimeError("megatron did not reach catchup/head")
            self.report["phases"].append(
                {
                    "phase": "initial_connect",
                    "initial_head": initial_head,
                    "journal": self.journal_head(),
                    "health": self.health(),
                    "camatron": cam.summary(),
                    "megatron": meg.summary(),
                }
            )

            start = time.monotonic()
            reconnected_meg = False
            reconnected_cam = False
            while time.monotonic() - start < self.duration_seconds:
                elapsed = time.monotonic() - start
                if not reconnected_meg and elapsed >= min(150, self.duration_seconds / 3):
                    before = meg.summary()
                    await meg.disconnect()
                    await asyncio.sleep(15)
                    meg = RawConsumer("megatron", self, meg.last_sequences, self.evidence_dir / "active_megatron_state.json")
                    await meg.connect()
                    consumers["megatron"] = meg
                    caught = await self.wait_until_caught_up(meg, timeout=120)
                    self.report["phases"].append({"phase": "megatron_reconnect", "caught_up": caught, "before": before, "after": meg.summary()})
                    if not caught:
                        raise RuntimeError("megatron reconnect did not catch up")
                    reconnected_meg = True
                if not reconnected_cam and elapsed >= min(300, self.duration_seconds * 2 / 3):
                    before = cam.summary()
                    await cam.disconnect()
                    await asyncio.sleep(15)
                    cam = RawConsumer("camatron", self, cam.last_sequences, self.evidence_dir / "active_camatron_state.json")
                    await cam.connect()
                    consumers["camatron"] = cam
                    caught = await self.wait_until_caught_up(cam, timeout=120)
                    self.report["phases"].append({"phase": "camatron_reconnect", "caught_up": caught, "before": before, "after": cam.summary()})
                    if not caught:
                        raise RuntimeError("camatron reconnect did not catch up")
                    reconnected_cam = True
                if not await self.wait_for_fresh_health(timeout=10):
                    raise RuntimeError(f"health became stale: {self.health().get('observation_age_seconds')}")
                await asyncio.sleep(1)

            consumers = {"camatron": cam, "megatron": meg}
            self.report["overlap"] = compare_overlap(consumers)
            self.report["final_health"] = self.health()
            self.report["final_journal"] = self.journal_head()
            self.report["final_services"] = self.service_snapshot()
            failures = []
            for consumer in consumers.values():
                if consumer.gaps:
                    failures.append(f"{consumer.identity} gaps {consumer.gaps[:3]}")
                if consumer.duplicates:
                    failures.append(f"{consumer.identity} duplicates {consumer.duplicates[:3]}")
                if not self.symbols <= set(consumer.last_sequences):
                    failures.append(f"{consumer.identity} missing symbols")
            if self.report["overlap"]["mismatches"]:
                failures.append("consumer overlap hash mismatch")
            if failures:
                raise RuntimeError("; ".join(failures))
            self.save("passed")
        except Exception as exc:
            self.save("failed", exc)
            raise
        finally:
            stop.set()
            await asyncio.gather(sampler, return_exceptions=True)
            await asyncio.gather(*(consumer.disconnect() for consumer in consumers.values()), return_exceptions=True)


class RawConsumer:
    def __init__(self, identity: str, validation: ActiveValidation, last_sequences: dict[str, int], state_path: Path) -> None:
        self.identity = identity
        self.validation = validation
        self.last_sequences = {str(symbol).upper(): int(sequence) for symbol, sequence in dict(last_sequences).items()}
        self.state_path = state_path
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.task: asyncio.Task[None] | None = None
        self.connected_at: str | None = None
        self.disconnected_at: str | None = None
        self.catchup_complete = False
        self.events: list[dict[str, Any]] = []
        self.gaps: list[dict[str, Any]] = []
        self.duplicates: list[dict[str, Any]] = []
        self._seen: set[tuple[str, int, str]] = set()

    def persist(self) -> None:
        self.state_path.write_text(
            json.dumps({"last_sequences": self.last_sequences, "updated_at": utc_now()}, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    async def connect(self) -> None:
        self.reader, self.writer = await asyncio.open_unix_connection(self.validation.config.socket_path)
        self.connected_at = utc_now()
        self.catchup_complete = False
        await write_message(
            self.writer,
            {
                "type": "hello",
                "identity": self.identity,
                "expected_contract_fingerprints": self.validation.config.contract_fingerprints,
                "last_sequences": self.last_sequences,
            },
        )
        self.task = asyncio.create_task(self._read_loop())

    async def _read_loop(self) -> None:
        assert self.reader is not None
        try:
            while True:
                message = await read_message(self.reader)
                message_type = message.get("type")
                if message_type == "catchup_complete":
                    self.catchup_complete = True
                    continue
                if message_type == "gap":
                    self.gaps.append(message)
                    continue
                if message_type != "observation":
                    continue
                symbol = str(message.get("symbol") or "").upper()
                sequence = int(message.get("sequence") or 0)
                event_id = str(message.get("event_id") or "")
                key = (symbol, sequence, event_id)
                if key in self._seen:
                    self.duplicates.append({"symbol": symbol, "sequence": sequence, "event_id": event_id})
                self._seen.add(key)
                previous = int(self.last_sequences.get(symbol, 0))
                if previous and sequence > previous + 1:
                    self.gaps.append({"type": "local_sequence_gap", "symbol": symbol, "previous": previous, "received": sequence})
                self.last_sequences[symbol] = max(previous, sequence)
                event = {
                    "symbol": symbol,
                    "sequence": sequence,
                    "event_id": event_id,
                    "hash": payload_hash(message),
                    "observation_timestamp": message.get("observation_timestamp"),
                    "received_at": utc_now(),
                }
                self.events.append(event)
                self.persist()
                assert self.writer is not None
                await write_message(self.writer, {"type": "ack", "symbol": symbol, "sequence": sequence, "event_id": event_id})
        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
            return
        finally:
            self.disconnected_at = utc_now()

    async def disconnect(self) -> None:
        if self.writer is not None:
            self.writer.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.writer.wait_closed(), timeout=5)
        if self.task is not None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(asyncio.gather(self.task, return_exceptions=True), timeout=5)

    def summary(self) -> dict[str, Any]:
        by_symbol: dict[str, int] = defaultdict(int)
        for event in self.events:
            by_symbol[event["symbol"]] += 1
        return {
            "identity": self.identity,
            "connected_at": self.connected_at,
            "disconnected_at": self.disconnected_at,
            "catchup_complete": self.catchup_complete,
            "event_count": len(self.events),
            "by_symbol": dict(by_symbol),
            "last_sequences": dict(self.last_sequences),
            "gaps": self.gaps,
            "duplicates": self.duplicates,
            "first_event": self.events[0] if self.events else None,
            "last_event": self.events[-1] if self.events else None,
        }


def compare_overlap(consumers: dict[str, RawConsumer]) -> dict[str, Any]:
    maps = {
        identity: {(event["symbol"], event["sequence"]): event for event in consumer.events}
        for identity, consumer in consumers.items()
    }
    identities = sorted(maps)
    if len(identities) != 2:
        return {"count": 0, "mismatches": []}
    left, right = identities
    overlap = set(maps[left]) & set(maps[right])
    mismatches = []
    for key in sorted(overlap):
        if maps[left][key]["event_id"] != maps[right][key]["event_id"] or maps[left][key]["hash"] != maps[right][key]["hash"]:
            mismatches.append({"key": list(key), left: maps[left][key], right: maps[right][key]})
    return {"count": len(overlap), "mismatches": mismatches[:50]}


def main() -> None:
    parser = argparse.ArgumentParser(description="Market-data-only active-session shared stream validation.")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--evidence-dir", required=True, type=Path)
    parser.add_argument("--duration-seconds", type=int, default=600)
    args = parser.parse_args()
    args.evidence_dir.mkdir(parents=True, exist_ok=True)
    asyncio.run(ActiveValidation(config_path=args.config, evidence_dir=args.evidence_dir, duration_seconds=args.duration_seconds).run())


if __name__ == "__main__":
    main()
