import asyncio
import contextlib
import json
import os
import socket
import struct
import threading
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pandas as pd

from silver.market import (
    CanonicalMarketObservationAdapter,
    LiveMarketEvent,
    SharedCanonicalMarketDataServer,
    SharedCanonicalStreamConsumer,
    SharedMarketDataConfig,
    canonical_event_id,
    live_market_event_from_archive_payload,
    market_stream_contract,
    validate_symbol_union,
)
import silver.market.shared_service as shared_service_module
from silver.market.canonical_journal import CanonicalJournal
from silver.market.ipc_consumer import CanonicalStreamEvent
from silver.market.shared_service import (
    AlpacaSipUpstream,
    AsyncArchiveWriter,
    ConsumerState,
    DiagnosticSpikeWriter,
    EventLoopWatchdog,
    EventLoopWatchdogConfig,
    PipelineMetrics,
    SingletonLock,
    load_market_data_credentials,
    run_service,
)


TEST_TMP_ROOT = Path("/tmp/smd")


def temporary_directory():
    TEST_TMP_ROOT.mkdir(exist_ok=True)
    return tempfile.TemporaryDirectory(dir=TEST_TMP_ROOT)


def config_payload(temp_dir: str, *, symbols=("AAPL", "NVDA", "TSLA"), queue_limit=10, disk_min_free_bytes=0):
    return {
        "feed": "sip",
        "canonical_source": "1s_close",
        "symbols": list(symbols),
        "socket_path": f"{temp_dir}/m.sock",
        "journal_path": f"{temp_dir}/canonical_journal.jsonl",
        "archive_root": f"{temp_dir}/archive",
        "health_path": f"{temp_dir}/health.json",
        "replay_limit_records": 3,
        "consumer_queue_limit": queue_limit,
        "freshness_threshold_seconds": 5.0,
        "disk_min_free_bytes": disk_min_free_bytes,
        "disk_min_free_inodes": 0,
        "retention_sessions": 15,
        "credential_profile": "market_data",
        "contract_fingerprints": {
            symbol: market_stream_contract(symbol=symbol, session_timezone="America/New_York").fingerprint
            for symbol in symbols
        },
    }


def trade(symbol: str, timestamp: str, price: float, sequence: int):
    return live_market_event_from_archive_payload(
        {
            "timestamp": timestamp,
            "symbol": symbol,
            "price": price,
            "size": 100,
            "exchange": "V",
            "trade_id": f"{symbol.lower()}-{sequence}",
            "tape": "C",
            "conditions": ["@"],
        },
        source_sequence=sequence,
    )


def journal_event(symbol="AAPL", sequence=1, event_id=None):
    return {
        "version": "canonical_market_ipc.v1",
        "type": "observation",
        "symbol": symbol,
        "sequence": sequence,
        "event_id": event_id or f"{symbol.lower()}-{sequence}",
        "contract_fingerprint": "fp",
        "source_timestamp": "2026-08-21T14:00:00Z",
        "receive_timestamp": "2026-08-21T14:00:00Z",
        "observation_timestamp": f"2026-08-21T14:00:{sequence:02d}Z",
        "close_price": str(100 + sequence),
        "source_record_id": f"raw-{sequence}",
    }


def write_journal(path: Path, *records):
    with path.open("wb") as handle:
        for record in records:
            if isinstance(record, bytes):
                handle.write(record)
                if not record.endswith(b"\n"):
                    handle.write(b"\n")
            else:
                handle.write(CanonicalJournal.encode_record(record) + b"\n")


def decode_buffered_messages(payload: bytes | bytearray):
    messages = []
    offset = 0
    raw = bytes(payload)
    while offset < len(raw):
        size = struct.unpack("!I", raw[offset : offset + 4])[0]
        offset += 4
        messages.append(json.loads(raw[offset : offset + size].decode("utf-8")))
        offset += size
    return messages


class DummyWriter:
    def __init__(self):
        self.closed = False
        self.buffer = bytearray()

    def write(self, payload):
        self.buffer.extend(payload)

    async def drain(self):
        return None

    def close(self):
        self.closed = True


class SharedMarketDataServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = temporary_directory()
        self.config = SharedMarketDataConfig.from_mapping(config_payload(self.temp.name, symbols=("AAPL", "TSLA")))
        self.server = SharedCanonicalMarketDataServer(self.config)
        self.socket_server = await self.server.start_socket_server()
        self.server_context = self.socket_server
        await self.server_context.__aenter__()

    async def asyncTearDown(self):
        if self.server_context is not None:
            self.server_context.close()
            with contextlib.suppress(asyncio.TimeoutError, asyncio.CancelledError):
                await asyncio.wait_for(self.server_context.wait_closed(), timeout=2)
        self.temp.cleanup()

    async def _consumer(self, identity):
        return SharedCanonicalStreamConsumer(
            socket_path=self.config.socket_path,
            identity=identity,
            expected_contract_fingerprints=self.config.contract_fingerprints,
            state_path=Path(self.temp.name) / f"{identity}.json",
        )

    async def test_alpaca_blank_regular_sale_is_normalized_before_shared_filter(self):
        adapter = CanonicalMarketObservationAdapter(run_id="condition-normalization")
        first = live_market_event_from_archive_payload(
            {
                "timestamp": "2026-08-21T14:00:00.100Z",
                "symbol": "AAPL",
                "price": 100.0,
                "size": 100,
                "trade_id": "blank-sale",
                "conditions": [" "],
            },
            source_sequence=1,
        )
        self.assertEqual(first.tick.conditions, ("@",))
        self.assertTrue(first.eligible)
        await self.server.ingest_raw_trade(first, adapter)

        rejected = live_market_event_from_archive_payload(
            {
                "timestamp": "2026-08-21T14:00:00.200Z",
                "symbol": "AAPL",
                "price": 100.1,
                "size": 100,
                "trade_id": "odd-lot-sale",
                "conditions": [" ", "I"],
            },
            source_sequence=2,
        )
        self.assertEqual(rejected.tick.conditions, ("@", "I"))
        self.assertFalse(rejected.eligible)

        second = live_market_event_from_archive_payload(
            {
                "timestamp": "2026-08-21T14:00:01.100Z",
                "symbol": "AAPL",
                "price": 100.2,
                "size": 100,
                "trade_id": "regular-sale",
                "conditions": [" ", "F"],
            },
            source_sequence=3,
        )
        emitted = await self.server.ingest_raw_trade(second, adapter)
        self.assertEqual(len(emitted), 1)
        self.assertEqual(emitted[0]["source_trade_id"], "blank-sale")

    async def test_two_consumers_receive_identical_canonical_observation_and_ack(self):
        camatron = await self._consumer("camatron")
        megatron = await self._consumer("megatron")
        cam_iter = camatron.connect().__aiter__()
        meg_iter = megatron.connect().__aiter__()
        try:
            adapter = CanonicalMarketObservationAdapter(run_id="test-aapl")

            await self.server.ingest_raw_trade(trade("AAPL", "2026-08-21T14:00:00.100Z", 100.0, 1), adapter)
            emitted = await self.server.ingest_raw_trade(trade("AAPL", "2026-08-21T14:00:01.100Z", 101.0, 2), adapter)

            self.assertEqual(len(emitted), 1)
            first = await asyncio.wait_for(cam_iter.__anext__(), timeout=2)
            second = await asyncio.wait_for(meg_iter.__anext__(), timeout=2)
            self.assertEqual(first.payload, second.payload)
            self.assertEqual(first.sequence, 1)
            self.assertEqual(first.event_id, emitted[0]["event_id"])

            await camatron.ack(first)
            await megatron.ack(second)
            for _ in range(20):
                if (
                    self.server.consumers["camatron"].acknowledged.get("AAPL") == 1
                    and self.server.consumers["megatron"].acknowledged.get("AAPL") == 1
                ):
                    break
                await asyncio.sleep(0.01)
            self.assertEqual(self.server.consumers["camatron"].acknowledged["AAPL"], 1)
            self.assertEqual(self.server.consumers["megatron"].acknowledged["AAPL"], 1)
        finally:
            await cam_iter.aclose()
            await meg_iter.aclose()

    async def test_duplicate_consumer_identity_fails_closed(self):
        first = await self._consumer("camatron:tsla")
        first_iter = first.connect().__aiter__()
        first_task = asyncio.create_task(first_iter.__anext__())
        try:
            for _ in range(20):
                if "camatron:tsla" in self.server.consumers:
                    break
                await asyncio.sleep(0.01)
            self.assertIn("camatron:tsla", self.server.consumers)

            duplicate = await self._consumer("camatron:tsla")
            duplicate_iter = duplicate.connect().__aiter__()
            try:
                with self.assertRaisesRegex(RuntimeError, "identity conflict"):
                    await asyncio.wait_for(duplicate_iter.__anext__(), timeout=2)
            finally:
                await duplicate_iter.aclose()
        finally:
            first_task.cancel()
            await asyncio.gather(first_task, return_exceptions=True)
            await first_iter.aclose()

    async def test_reconnect_replays_missed_observation_in_order(self):
        adapter = CanonicalMarketObservationAdapter(run_id="test-replay")
        await self.server.ingest_raw_trade(trade("AAPL", "2026-08-21T14:00:00.100Z", 100.0, 1), adapter)
        await self.server.ingest_raw_trade(trade("AAPL", "2026-08-21T14:00:01.100Z", 101.0, 2), adapter)

        consumer = SharedCanonicalStreamConsumer(
            socket_path=self.config.socket_path,
            identity="camatron",
            expected_contract_fingerprints=self.config.contract_fingerprints,
            state_path=Path(self.temp.name) / "replay.json",
        )
        stream = consumer.connect().__aiter__()
        try:
            event = await asyncio.wait_for(stream.__anext__(), timeout=2)
            self.assertEqual(event.symbol, "AAPL")
            self.assertEqual(event.sequence, 1)
        finally:
            await stream.aclose()

    async def test_replay_window_exhaustion_emits_gap(self):
        await self.server_context.__aexit__(None, None, None)
        self.server_context = None
        small_payload = config_payload(self.temp.name, symbols=("AAPL",), queue_limit=10)
        small_payload["replay_limit_records"] = 3
        small = SharedMarketDataConfig.from_mapping(small_payload)
        self.server = SharedCanonicalMarketDataServer(small)
        self.socket_server = await self.server.start_socket_server()
        self.server_context = self.socket_server
        await self.server_context.__aenter__()
        adapter = CanonicalMarketObservationAdapter(run_id="gap")
        for index in range(6):
            await self.server.ingest_raw_trade(trade("AAPL", f"2026-08-21T14:00:0{index}.100Z", 100 + index, index + 1), adapter)
        reader, writer = await asyncio.open_unix_connection(small.socket_path)
        from silver.market.ipc_protocol import read_message, write_message

        await write_message(
            writer,
            {
                "type": "hello",
                "identity": "camatron",
                "expected_contract_fingerprints": small.contract_fingerprints,
                "last_sequences": {"AAPL": 1},
            },
        )
        self.assertEqual((await asyncio.wait_for(read_message(reader), timeout=2))["type"], "session_header")
        gap = await asyncio.wait_for(read_message(reader), timeout=2)
        self.assertEqual(gap["type"], "gap")
        self.assertEqual(gap["reason"], "replay_window_exhausted")
        writer.close()
        await writer.wait_closed()

    async def test_consumer_sequence_ahead_of_journal_emits_gap(self):
        adapter = CanonicalMarketObservationAdapter(run_id="ahead")
        await self.server.ingest_raw_trade(trade("AAPL", "2026-08-21T14:00:00.100Z", 100.0, 1), adapter)
        await self.server.ingest_raw_trade(trade("AAPL", "2026-08-21T14:00:01.100Z", 101.0, 2), adapter)
        reader, writer = await asyncio.open_unix_connection(self.config.socket_path)
        from silver.market.ipc_protocol import read_message, write_message

        await write_message(
            writer,
            {
                "type": "hello",
                "identity": "camatron",
                "expected_contract_fingerprints": self.config.contract_fingerprints,
                "last_sequences": {"AAPL": 99},
            },
        )
        self.assertEqual((await asyncio.wait_for(read_message(reader), timeout=2))["type"], "session_header")
        gap = await asyncio.wait_for(read_message(reader), timeout=2)
        self.assertEqual(gap["type"], "gap")
        self.assertEqual(gap["reason"], "consumer_sequence_ahead_of_journal")
        writer.close()
        await writer.wait_closed()

    async def test_corrupted_journal_returns_explicit_gap_and_blocks_ingestion(self):
        await self.server_context.__aexit__(None, None, None)
        self.server_context = None
        path = Path(self.config.journal_path)
        write_journal(path, journal_event(sequence=1), b"not-json")
        self.server = SharedCanonicalMarketDataServer(self.config)
        self.socket_server = await self.server.start_socket_server()
        self.server_context = self.socket_server
        await self.server_context.__aenter__()

        reader, writer = await asyncio.open_unix_connection(self.config.socket_path)
        from silver.market.ipc_protocol import read_message, write_message

        await write_message(
            writer,
            {
                "type": "hello",
                "identity": "camatron",
                "expected_contract_fingerprints": self.config.contract_fingerprints,
                "last_sequences": {"AAPL": 1},
            },
        )
        self.assertEqual((await asyncio.wait_for(read_message(reader), timeout=2))["type"], "session_header")
        message = await asyncio.wait_for(read_message(reader), timeout=2)
        if message.get("type") == "observation":
            message = await asyncio.wait_for(read_message(reader), timeout=2)
        gap = message
        self.assertEqual(gap["type"], "gap")
        self.assertEqual(gap["reason"], "journal_corruption")
        self.assertTrue(self.server.health["critical_events"])
        with self.assertRaises(RuntimeError):
            adapter = CanonicalMarketObservationAdapter(run_id="blocked-corrupt-journal")
            await self.server.ingest_raw_trade(trade("AAPL", "2026-08-21T14:00:02.100Z", 102.0, 3), adapter)
            await self.server.ingest_raw_trade(trade("AAPL", "2026-08-21T14:00:03.100Z", 103.0, 4), adapter)
        writer.close()
        await writer.wait_closed()

    async def test_contract_mismatch_fails_closed(self):
        reader, writer = await asyncio.open_unix_connection(self.config.socket_path)
        from silver.market.ipc_protocol import read_message, write_message

        await write_message(
            writer,
            {
                "type": "hello",
                "identity": "camatron",
                "expected_contract_fingerprints": {"AAPL": "wrong"},
                "last_sequences": {},
            },
        )
        message = await read_message(reader)
        self.assertEqual(message["type"], "contract_mismatch")
        writer.close()
        await writer.wait_closed()

    async def test_disk_threshold_fails_closed_through_monitor(self):
        strict = SharedMarketDataConfig.from_mapping(
            config_payload(self.temp.name, symbols=("AAPL",), disk_min_free_bytes=10**30)
        )
        server = SharedCanonicalMarketDataServer(strict)
        with self.assertRaises(RuntimeError):
            await asyncio.wait_for(server.monitor_disk_safety(interval_seconds=60), timeout=2)

    async def test_journal_size_limit_rotates_without_dropping_observation(self):
        strict_payload = config_payload(self.temp.name, symbols=("AAPL",))
        strict_payload["journal_max_bytes"] = 700
        strict = SharedMarketDataConfig.from_mapping(strict_payload)
        server = SharedCanonicalMarketDataServer(strict)
        adapter = CanonicalMarketObservationAdapter(run_id="journal-size")
        await server.ingest_raw_trade(trade("AAPL", "2026-08-21T14:00:00.100Z", 100.0, 1), adapter)
        await server.ingest_raw_trade(trade("AAPL", "2026-08-21T14:00:01.100Z", 101.0, 2), adapter)
        await server.ingest_raw_trade(trade("AAPL", "2026-08-21T14:00:02.100Z", 102.0, 3), adapter)
        await server.ingest_raw_trade(trade("AAPL", "2026-08-21T14:00:03.100Z", 103.0, 4), adapter)
        active = Path(strict.journal_path)
        archives = sorted(active.parent.glob("canonical_journal_*.jsonl"))
        self.assertTrue(archives)
        self.assertEqual([event["sequence"] for event in server.events_by_symbol["AAPL"]], [1, 2, 3])
        self.assertEqual(json.loads(active.read_text(encoding="utf-8").splitlines()[0])["payload"]["sequence"], 3)

    def test_journal_rotation_preserves_archive_and_creates_append_target(self):
        with temporary_directory() as temp_dir:
            path = Path(temp_dir) / "canonical_journal.jsonl"
            journal = CanonicalJournal(path)
            journal.append(journal_event(sequence=1))
            original = path.read_bytes()
            archive = journal.rotate(timestamp="2026-08-21T14:00:01Z", latest_sequences={"AAPL": 1})
            self.assertIsNotNone(archive)
            assert archive is not None
            self.assertEqual(archive.read_bytes(), original)
            self.assertTrue(path.exists())
            self.assertEqual(path.read_bytes(), b"")
            journal.append(journal_event(sequence=2))
            self.assertEqual(journal.latest_sequence_by_symbol()["AAPL"], 2)

    async def test_queue_overflow_marks_consumer_stale_and_records_drop(self):
        writer = DummyWriter()
        queue: asyncio.Queue = asyncio.Queue(maxsize=1)
        await queue.put({"type": "observation"})
        self.server.consumers["camatron"] = ConsumerState(
            identity="camatron",
            writer=writer,
            queue=queue,
            connected_at=pd.Timestamp.now(tz="UTC").isoformat(),
        )
        await self.server.publish_event(
            {
                "type": "observation",
                "version": "canonical_market_ipc.v1",
                "symbol": "AAPL",
                "sequence": 1,
                "event_id": "event-1",
            }
        )
        self.assertTrue(self.server.consumers["camatron"].stale)
        self.assertEqual(self.server.health["dropped_message_count"], 1)

    async def test_slow_consumer_does_not_block_healthy_consumer_publication(self):
        slow_writer = DummyWriter()
        fast_writer = DummyWriter()
        slow_queue: asyncio.Queue = asyncio.Queue(maxsize=1)
        await slow_queue.put({"type": "observation", "symbol": "AAPL", "sequence": 0})
        fast_queue: asyncio.Queue = asyncio.Queue(maxsize=2)
        self.server.consumers["camatron"] = ConsumerState(
            identity="camatron",
            writer=slow_writer,
            queue=slow_queue,
            connected_at=pd.Timestamp.now(tz="UTC").isoformat(),
        )
        self.server.consumers["megatron"] = ConsumerState(
            identity="megatron",
            writer=fast_writer,
            queue=fast_queue,
            connected_at=pd.Timestamp.now(tz="UTC").isoformat(),
        )

        event = {
            "type": "observation",
            "version": "canonical_market_ipc.v1",
            "symbol": "AAPL",
            "sequence": 1,
            "event_id": "event-1",
        }
        await self.server.publish_event(event)

        self.assertTrue(self.server.consumers["camatron"].stale)
        self.assertEqual(await asyncio.wait_for(fast_queue.get(), timeout=1), event)
        self.assertEqual(self.server.health["dropped_message_count"], 1)

    async def test_async_archive_writer_preserves_order_and_records_latency(self):
        metrics = PipelineMetrics()
        writer = AsyncArchiveWriter(max_queue_size=10, metrics=metrics)
        path = Path(self.temp.name) / "archive.jsonl"
        writer.start()
        try:
            writer.enqueue(path, {"sequence": 1}, kind="raw")
            writer.enqueue(path, {"sequence": 2}, kind="raw")
            await writer.join(timeout=2)
        finally:
            await writer.stop()

        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        self.assertEqual([row["sequence"] for row in rows], [1, 2])
        snapshot = metrics.snapshot()
        self.assertEqual(snapshot["counts"]["raw_archive_writes"], 2)
        self.assertGreaterEqual(snapshot["high_water"]["raw_archive_queue_depth"], 1)
        self.assertEqual(snapshot["counts"]["archive_writer_thread_started"], 1)

    async def test_archive_writer_runs_outside_event_loop_thread(self):
        metrics = PipelineMetrics()
        writer = AsyncArchiveWriter(max_queue_size=10, metrics=metrics)
        path = Path(self.temp.name) / "archive-thread.jsonl"
        event_loop_thread_id = threading.get_ident()
        writer.start()
        try:
            writer.enqueue(path, {"sequence": 1}, kind="raw")
            await writer.join(timeout=2)
        finally:
            await writer.stop()

        self.assertIsNotNone(writer.writer_thread_id)
        self.assertNotEqual(writer.writer_thread_id, event_loop_thread_id)
        self.assertEqual(json.loads(path.read_text(encoding="utf-8").strip())["sequence"], 1)

    async def test_late_trade_event_time_does_not_make_canonical_heartbeat_stale(self):
        adapter = CanonicalMarketObservationAdapter(run_id="late-heartbeat")
        older = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(seconds=30)).floor("s")
        await self.server.ingest_raw_trade(
            trade("AAPL", older.isoformat(), 100.0, 1),
            adapter,
        )
        await self.server.ingest_raw_trade(
            trade("AAPL", (older + pd.Timedelta(seconds=1)).isoformat(), 101.0, 2),
            adapter,
        )

        event_age = self.server.health["observation_age_seconds"]["AAPL"]
        heartbeat_age = self.server.health["canonical_heartbeat_age_seconds"]["AAPL"]
        self.assertGreater(event_age, 5.0)
        self.assertLess(heartbeat_age, 5.0)

    async def test_sparse_eligible_trade_updates_canonical_liveness_without_publication(self):
        adapter = CanonicalMarketObservationAdapter(run_id="sparse-heartbeat")
        current = pd.Timestamp.now(tz="UTC").floor("s")

        emitted = await self.server.ingest_raw_trade(
            trade("AAPL", current.isoformat(), 100.0, 1),
            adapter,
        )

        self.assertEqual(emitted, tuple())
        self.assertLess(self.server.health["canonical_heartbeat_age_seconds"]["AAPL"], 5.0)
        self.assertLess(self.server.health["canonical_source_freshness_age_seconds"]["AAPL"], 5.0)
        self.assertIn("AAPL", self.server.health["last_canonical_sampler_admission_by_symbol"])
        self.assertNotIn("AAPL", self.server.health["last_canonical_publication_by_symbol"])

    async def test_ineligible_trade_does_not_satisfy_canonical_freshness(self):
        adapter = CanonicalMarketObservationAdapter(run_id="ineligible-heartbeat")
        current = pd.Timestamp.now(tz="UTC").floor("s")
        base = trade("AAPL", current.isoformat(), 100.0, 1)

        emitted = await self.server.ingest_raw_trade(
            LiveMarketEvent(
                tick=base.tick,
                source_record_id="ineligible",
                broker_trade_id="ineligible",
                source_sequence=1,
                eligible=False,
            ),
            adapter,
        )

        self.assertEqual(emitted, tuple())
        self.assertNotIn("AAPL", self.server.health.get("canonical_heartbeat_age_seconds", {}))
        self.assertNotIn("AAPL", self.server.health.get("canonical_source_freshness_age_seconds", {}))
        self.assertNotIn("AAPL", self.server.health["last_canonical_sampler_admission_by_symbol"])

    async def test_late_eligible_trade_does_not_regress_source_freshness_high_water(self):
        adapter = CanonicalMarketObservationAdapter(run_id="late-source-heartbeat")
        current = pd.Timestamp.now(tz="UTC").floor("s")
        older = current - pd.Timedelta(seconds=30)

        await self.server.ingest_raw_trade(trade("AAPL", current.isoformat(), 100.0, 1), adapter)
        first_source = self.server.health["last_canonical_source_timestamp_by_symbol"]["AAPL"]
        await self.server.ingest_raw_trade(trade("AAPL", older.isoformat(), 101.0, 2), adapter)

        self.assertEqual(self.server.health["last_canonical_source_timestamp_by_symbol"]["AAPL"], first_source)
        self.assertLess(self.server.health["canonical_heartbeat_age_seconds"]["AAPL"], 5.0)
        self.assertLess(self.server.health["canonical_source_freshness_age_seconds"]["AAPL"], 5.0)

    async def test_health_snapshot_writes_are_throttled(self):
        self.server.health["last_canonical_heartbeat_by_symbol"]["AAPL"] = pd.Timestamp.now(tz="UTC").isoformat()
        self.server._refresh_health(force=True)
        path = Path(self.config.health_path)
        first_mtime = path.stat().st_mtime_ns

        self.server._refresh_health()
        self.server._refresh_health()

        self.assertEqual(path.stat().st_mtime_ns, first_mtime)
        self.assertGreaterEqual(self.server.metrics.snapshot()["counts"]["health_snapshot_write_skipped"], 1)

    async def test_sampler_activity_reads_pending_state_without_full_snapshot(self):
        adapter = CanonicalMarketObservationAdapter(run_id="lightweight-pending")
        self.server.health["pending_canonical_bucket_by_symbol"]["AAPL"] = "obsolete"
        with patch.object(adapter, "snapshot", side_effect=AssertionError("full snapshot on hot path")):
            self.server._refresh_sampler_activity("aapl", adapter)
            self.assertNotIn("AAPL", self.server.health["pending_canonical_bucket_by_symbol"])
            await self.server.ingest_raw_trade(trade("AAPL", "2026-08-21T14:00:00.100Z", 100, 1), adapter)
            self.assertEqual(self.server.health["pending_canonical_bucket_by_symbol"]["AAPL"], "2026-08-21T14:00:00+00:00")
            emitted = await self.server.ingest_raw_trade(trade("AAPL", "2026-08-21T14:00:01.100Z", 101, 2), adapter)
        self.assertEqual([row["sequence"] for row in emitted], [1])
        self.assertEqual(self.server.health["pending_canonical_bucket_by_symbol"]["AAPL"], "2026-08-21T14:00:01+00:00")

    async def test_throttled_health_skips_reports_but_keeps_freshness_and_safety(self):
        self.server._refresh_health(force=True)
        self.server._health_write_interval_seconds = 3600
        now = pd.Timestamp.now(tz="UTC")
        self.server.health["last_canonical_observation_by_symbol"]["AAPL"] = now.isoformat()
        self.server._canonical_source_high_water_by_symbol["AAPL"] = now
        self.server._canonical_heartbeat_monotonic_by_symbol["AAPL"] = time.monotonic()
        self.server.health["journal_healthy"] = False
        self.server.health["gap_count"] = 1
        self.server.health["dropped_message_count"] = 2
        stat = Path.stat

        def report_stat(path, *args, **kwargs):
            self.assertNotEqual(path, Path(self.config.journal_path), "journal stat on throttled report")
            return stat(path, *args, **kwargs)

        class ReportConsumers(dict):
            def items(self):
                raise AssertionError("consumer aggregation on throttled report")

        self.server.consumers = ReportConsumers()
        with patch.object(Path, "stat", report_stat), patch.object(
            self.server.metrics, "snapshot", side_effect=AssertionError("metrics report on hot path")
        ):
            self.server._refresh_health()
        self.assertLess(self.server.health["canonical_heartbeat_age_seconds"]["AAPL"], 5)
        self.assertLess(self.server.health["canonical_source_freshness_age_seconds"]["AAPL"], 5)
        self.assertIn("AAPL", self.server.health["observation_age_seconds"])
        self.assertFalse(self.server.health["journal_healthy"])
        self.assertEqual(self.server.health["gap_count"], 1)
        self.assertEqual(self.server.health["dropped_message_count"], 2)

    async def test_health_report_aggregates_when_forced_or_due(self):
        self.server._refresh_health(force=True)
        queue = asyncio.Queue()
        self.server.consumers["report"] = ConsumerState(
            identity="report", writer=DummyWriter(), queue=queue,
            connected_at=pd.Timestamp.now(tz="UTC").isoformat(),
        )
        for force in (True, False):
            with self.subTest(force=force):
                queue.put_nowait({"test": True})
                if not force:
                    self.server._last_health_write_monotonic -= self.server._health_write_interval_seconds + 1
                with patch.object(self.server.metrics, "snapshot", wraps=self.server.metrics.snapshot) as snapshot:
                    self.server._refresh_health(force=force)
                snapshot.assert_called_once()
                report = json.loads(Path(self.config.health_path).read_text())
                self.assertEqual(report["consumers"]["report"]["queue_size"], queue.qsize())

    async def test_journal_fsync_finishes_before_observation_enqueue(self):
        fsync = os.fsync
        durable = []

        def record_fsync(fd):
            fsync(fd)
            durable.append(True)

        testcase = self

        class DurableQueue(asyncio.Queue):
            def put_nowait(self, row):
                testcase.assertTrue(durable, "observation enqueued before journal fsync")
                super().put_nowait(row)

        queue = DurableQueue()
        self.server.consumers["durable"] = ConsumerState(
            identity="durable", writer=DummyWriter(), queue=queue,
            connected_at=pd.Timestamp.now(tz="UTC").isoformat(),
        )
        event = journal_event()
        with patch.object(os, "fsync", side_effect=record_fsync):
            await self.server.publish_event(event)
        self.assertEqual(len(durable), 1)
        self.assertEqual(queue.get_nowait(), event)
        self.assertEqual(self.server.journal.recover().events[-1], event)

    async def test_journal_failure_does_not_enqueue_observation(self):
        queue = asyncio.Queue()
        self.server.consumers["durable"] = ConsumerState(
            identity="durable", writer=DummyWriter(), queue=queue,
            connected_at=pd.Timestamp.now(tz="UTC").isoformat(),
        )
        with patch.object(self.server.journal, "append", side_effect=OSError("durability failure")):
            with self.assertRaisesRegex(OSError, "durability failure"):
                await self.server.publish_event(journal_event())
        self.assertTrue(queue.empty())
        self.assertEqual(self.server.events_by_symbol["AAPL"], [])

    async def test_canonical_journal_only_policy_disables_archive_files_not_journal(self):
        await self.server_context.__aexit__(None, None, None)
        self.server_context = None
        payload = config_payload(self.temp.name, symbols=("AAPL",))
        payload["raw_archive_enabled"] = False
        payload["nonessential_canonical_archive_enabled"] = False
        payload["canonical_journal_enabled"] = True
        cfg = SharedMarketDataConfig.from_mapping(payload)
        server = SharedCanonicalMarketDataServer(cfg)
        self.assertFalse(server.health["raw_archive_enabled"])
        self.assertFalse(server.health["nonessential_canonical_archive_enabled"])
        self.assertTrue(server.health["canonical_journal_enabled"])

        adapter = CanonicalMarketObservationAdapter(run_id="journal-only")
        await server.ingest_raw_trade(trade("AAPL", "2026-08-21T14:00:00.100Z", 100.0, 1), adapter)
        await server.ingest_raw_trade(trade("AAPL", "2026-08-21T14:00:01.100Z", 101.0, 2), adapter)

        self.assertTrue(Path(cfg.journal_path).exists())
        self.assertFalse((Path(cfg.archive_root) / "raw").exists())
        self.assertFalse((Path(cfg.archive_root) / "canonical").exists())
        snapshot = server.metrics.snapshot()
        self.assertEqual(snapshot["counts"]["raw_archive_disabled_skips"], 2)
        self.assertEqual(snapshot["counts"]["canonical_archive_disabled_skips"], 1)
        self.assertEqual(snapshot["counts"]["canonical_journal_writes"], 1)

    async def test_ingest_hot_path_does_not_run_disk_or_retention_filesystem_scans(self):
        await self.server_context.__aexit__(None, None, None)
        self.server_context = None
        payload = config_payload(self.temp.name, symbols=("AAPL",))
        payload["raw_archive_enabled"] = False
        payload["nonessential_canonical_archive_enabled"] = False
        cfg = SharedMarketDataConfig.from_mapping(payload)
        server = SharedCanonicalMarketDataServer(cfg)

        def fail_disk_snapshot():
            raise AssertionError("disk scan must not run from ingest_raw_trade")

        def fail_retention_purge(*_args, **_kwargs):
            raise AssertionError("retention purge must not run from ingest_raw_trade")

        original_purge = shared_service_module.purge_old_session_dirs
        server._disk_usage_snapshot = fail_disk_snapshot  # type: ignore[method-assign]
        shared_service_module.purge_old_session_dirs = fail_retention_purge
        try:
            adapter = CanonicalMarketObservationAdapter(run_id="hot-path")
            await server.ingest_raw_trade(trade("AAPL", "2026-08-21T14:00:00.100Z", 100.0, 1), adapter)
            await server.ingest_raw_trade(trade("AAPL", "2026-08-21T14:00:01.100Z", 101.0, 2), adapter)
        finally:
            shared_service_module.purge_old_session_dirs = original_purge

        self.assertEqual(server.metrics.snapshot()["counts"]["canonical_journal_writes"], 1)

    async def test_disk_monitor_fails_closed_off_ingest_path(self):
        await self.server_context.__aexit__(None, None, None)
        self.server_context = None
        payload = config_payload(self.temp.name, symbols=("AAPL",))
        payload["disk_min_free_bytes"] = 10**30
        cfg = SharedMarketDataConfig.from_mapping(payload)
        server = SharedCanonicalMarketDataServer(cfg)
        with self.assertRaisesRegex(RuntimeError, "disk safety threshold breached"):
            await asyncio.wait_for(server.monitor_disk_safety(interval_seconds=60), timeout=2)
        self.assertIn("disk_safety_check_wall_seconds", server.metrics.snapshot()["durations_seconds"])

    async def test_event_loop_spike_capture_returns_before_slow_diagnostic_write(self):
        writer = DiagnosticSpikeWriter(max_queue_size=10, metrics=self.server.metrics)
        original_write = writer._write_request
        write_started = threading.Event()

        def slow_write(request):
            write_started.set()
            time.sleep(0.2)
            original_write(request)

        writer._write_request = slow_write
        writer.start()
        self.server.diagnostic_spike_writer = writer
        try:
            started = time.perf_counter()
            self.server._capture_event_loop_spike(lag_seconds=0.3)
            elapsed = time.perf_counter() - started
            self.assertLess(elapsed, 0.05)
            self.assertTrue(write_started.wait(timeout=1))
            await writer.join(timeout=2)
        finally:
            await writer.stop()
            self.server.diagnostic_spike_writer = None

        snapshot = self.server.metrics.snapshot()
        self.assertEqual(snapshot["counts"]["event_loop_spike_capture_enqueued"], 1)
        self.assertEqual(snapshot["counts"]["event_loop_spike_captures"], 1)
        paths = list((Path(self.config.archive_root) / "diagnostics" / "event_loop_spikes").glob("spike_*.json"))
        self.assertEqual(len(paths), 1)

    async def test_event_loop_spike_capture_queue_overflow_is_nonblocking(self):
        writer = DiagnosticSpikeWriter(max_queue_size=1, metrics=self.server.metrics)
        self.server.diagnostic_spike_writer = writer
        request = object.__new__(object)
        writer.queue.put_nowait(request)  # type: ignore[arg-type]
        try:
            started = time.perf_counter()
            self.server._capture_event_loop_spike(lag_seconds=0.3)
            elapsed = time.perf_counter() - started
        finally:
            self.server.diagnostic_spike_writer = None

        self.assertLess(elapsed, 0.05)
        snapshot = self.server.metrics.snapshot()
        self.assertEqual(snapshot["counts"]["event_loop_spike_capture_queue_overflow"], 1)

    async def test_event_loop_watchdog_samples_loop_thread_during_block(self):
        root = Path(self.config.archive_root) / "diagnostics" / "event_loop_watchdog_test"
        watchdog = EventLoopWatchdog(
            root=root,
            metrics=self.server.metrics,
            config=EventLoopWatchdogConfig(
                sample_interval_seconds=0.005,
                stall_threshold_seconds=0.02,
                max_samples_per_stall=3,
                max_samples_total=3,
            ),
        )
        watchdog.set_loop_thread(threading.get_ident())
        watchdog.expect_heartbeat(time.perf_counter() + 0.01)
        watchdog.start()
        try:
            time.sleep(0.08)
            for _ in range(20):
                if list(root.glob("watchdog_*.json")):
                    break
                await asyncio.sleep(0.01)
        finally:
            await watchdog.stop()

        paths = list(root.glob("watchdog_*.json"))
        self.assertTrue(paths)
        payload = json.loads(paths[0].read_text(encoding="utf-8"))
        self.assertEqual(payload["loop_thread_id"], threading.get_ident())
        self.assertIn("loop_stack", payload)
        self.assertGreaterEqual(payload["lag_seconds"], 0.02)
        snapshot = self.server.metrics.snapshot()
        self.assertGreaterEqual(snapshot["counts"]["event_loop_watchdog_samples"], 1)

    async def test_archive_queue_overflow_fails_closed_without_silent_drop(self):
        metrics = PipelineMetrics()
        writer = AsyncArchiveWriter(max_queue_size=1, metrics=metrics)
        path = Path(self.temp.name) / "archive-overflow.jsonl"
        writer.enqueue(path, {"sequence": 1}, kind="raw")
        with self.assertRaisesRegex(RuntimeError, "archive queue exhausted"):
            writer.enqueue(path, {"sequence": 2}, kind="raw")
        self.assertEqual(metrics.snapshot()["counts"]["raw_archive_queue_overflow"], 1)

    async def test_replay_to_consumer_uses_snapshot_head_upper_bound(self):
        path = Path(self.config.journal_path)
        write_journal(
            path,
            journal_event(sequence=1),
            journal_event(sequence=2),
            journal_event(sequence=3),
            journal_event(sequence=4),
            journal_event(sequence=5),
        )
        server = SharedCanonicalMarketDataServer(self.config)
        writer = DummyWriter()
        consumer = ConsumerState(
            identity="camatron",
            writer=writer,
            queue=asyncio.Queue(maxsize=10),
            connected_at=pd.Timestamp.now(tz="UTC").isoformat(),
            replaying=True,
        )

        await server._replay_to_consumer(consumer, {"AAPL": 2}, replay_head={"AAPL": 4})

        messages = decode_buffered_messages(writer.buffer)
        self.assertEqual([message["sequence"] for message in messages if message["type"] == "observation"], [3, 4])
        self.assertEqual(messages[-1]["type"], "catchup_complete")

    async def test_live_queue_is_delivered_only_after_replay_catchup_boundary(self):
        path = Path(self.config.journal_path)
        write_journal(path, journal_event(sequence=1), journal_event(sequence=2), journal_event(sequence=3), journal_event(sequence=4))
        server = SharedCanonicalMarketDataServer(self.config)
        writer = DummyWriter()
        consumer = ConsumerState(
            identity="camatron",
            writer=writer,
            queue=asyncio.Queue(maxsize=10),
            connected_at=pd.Timestamp.now(tz="UTC").isoformat(),
            replaying=True,
        )
        server.consumers["camatron"] = consumer
        await server.publish_event(journal_event(sequence=5))

        await server._replay_to_consumer(consumer, {"AAPL": 2}, replay_head={"AAPL": 4})
        consumer.replaying = False
        pump = asyncio.create_task(server._consumer_pump(consumer))
        await consumer.queue.put(None)
        await asyncio.wait_for(pump, timeout=2)

        messages = decode_buffered_messages(writer.buffer)
        self.assertEqual(
            [(message["type"], message.get("sequence")) for message in messages],
            [("observation", 3), ("observation", 4), ("catchup_complete", None), ("observation", 5)],
        )

    async def test_consumer_symbol_subscription_filters_replay_and_live_publication(self):
        await self.server_context.__aexit__(None, None, None)
        self.server_context = None
        payload = config_payload(self.temp.name, symbols=("AAPL", "NVDA", "TSLA"), queue_limit=5)
        cfg = SharedMarketDataConfig.from_mapping(payload)
        path = Path(cfg.journal_path)
        write_journal(
            path,
            journal_event("AAPL", sequence=1),
            journal_event("NVDA", sequence=1),
            journal_event("TSLA", sequence=1),
            journal_event("AAPL", sequence=2),
            journal_event("NVDA", sequence=2),
            journal_event("TSLA", sequence=2),
        )
        self.config = cfg
        self.server = SharedCanonicalMarketDataServer(cfg)
        self.socket_server = await self.server.start_socket_server()
        self.server_context = self.socket_server
        await self.server_context.__aenter__()

        from silver.market.ipc_protocol import read_message, write_message

        reader, writer = await asyncio.open_unix_connection(cfg.socket_path)
        try:
            await write_message(
                writer,
                {
                    "type": "hello",
                    "identity": "megatron",
                    "symbols": ["AAPL", "TSLA"],
                    "expected_contract_fingerprints": cfg.contract_fingerprints,
                    "last_sequences": {"AAPL": 1, "NVDA": 99, "TSLA": 1},
                },
            )

            messages = []
            while True:
                message = await asyncio.wait_for(read_message(reader), timeout=2)
                messages.append(message)
                if message["type"] == "catchup_complete":
                    break

            self.assertEqual(messages[0]["type"], "session_header")
            self.assertEqual(messages[0]["symbols"], ["AAPL", "TSLA"])
            self.assertEqual(messages[0]["available_symbols"], ["AAPL", "NVDA", "TSLA"])
            self.assertNotIn("NVDA", messages[0]["contract_fingerprints"])
            self.assertFalse(any(message["type"] == "gap" for message in messages))
            self.assertEqual(
                [(message["symbol"], message["sequence"]) for message in messages if message["type"] == "observation"],
                [("AAPL", 2), ("TSLA", 2)],
            )
            self.assertEqual(self.server.consumers["megatron"].subscribed_symbols, ("AAPL", "TSLA"))
            self.assertNotIn("NVDA", self.server.consumers["megatron"].acknowledged)

            await self.server.publish_event(journal_event("NVDA", sequence=3))
            await self.server.publish_event(journal_event("AAPL", sequence=3))
            live = await asyncio.wait_for(read_message(reader), timeout=2)
            self.assertEqual((live["symbol"], live["sequence"]), ("AAPL", 3))
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(read_message(reader), timeout=0.05)
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_unknown_consumer_symbol_subscription_fails_closed(self):
        from silver.market.ipc_protocol import read_message, write_message

        reader, writer = await asyncio.open_unix_connection(self.config.socket_path)
        try:
            await write_message(
                writer,
                {
                    "type": "hello",
                    "identity": "megatron",
                    "symbols": ["AAPL", "QQQ"],
                    "expected_contract_fingerprints": self.config.contract_fingerprints,
                    "last_sequences": {},
                },
            )
            message = await asyncio.wait_for(read_message(reader), timeout=2)
            self.assertEqual(message["type"], "contract_mismatch")
            self.assertEqual(message["reason"], "invalid_symbol_subscription")
            self.assertIn("QQQ", message["error"])
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_replay_uses_startup_index_without_recover_rescan(self):
        path = Path(self.config.journal_path)
        write_journal(path, journal_event(sequence=1), journal_event(sequence=2), journal_event(sequence=3))
        server = SharedCanonicalMarketDataServer(self.config)

        def fail_recover():
            raise AssertionError("replay must use startup recovery/index, not recover()")

        server.journal.recover = fail_recover  # type: ignore[method-assign]
        writer = DummyWriter()
        consumer = ConsumerState(
            identity="camatron",
            writer=writer,
            queue=asyncio.Queue(maxsize=10),
            connected_at=pd.Timestamp.now(tz="UTC").isoformat(),
            replaying=True,
        )

        await server._replay_to_consumer(consumer, {"AAPL": 1}, replay_head=dict(server.sequences))

        messages = decode_buffered_messages(writer.buffer)
        self.assertEqual([message["sequence"] for message in messages if message["type"] == "observation"], [2, 3])
        self.assertEqual(messages[-1]["type"], "catchup_complete")
        snapshot = server.metrics.snapshot()
        self.assertEqual(snapshot["counts"]["consumer_replay_sessions"], 1)

    async def test_consumer_acknowledgements_advance_during_large_replay(self):
        await self.server_context.__aexit__(None, None, None)
        self.server_context = None
        payload = config_payload(self.temp.name, symbols=("AAPL",), queue_limit=10)
        payload["replay_limit_records"] = 200
        cfg = SharedMarketDataConfig.from_mapping(payload)
        path = Path(cfg.journal_path)
        write_journal(path, *(journal_event(sequence=i) for i in range(1, 121)))
        self.server = SharedCanonicalMarketDataServer(cfg)
        original_replay = self.server._replay_to_consumer
        ack_seen_during_replay = asyncio.Event()

        async def slow_replay(consumer, last_sequences, *, replay_head=None):
            for symbol in consumer.subscribed_symbols:
                replay = self.server._replay_after(
                    symbol=symbol,
                    last_sequence=int(last_sequences.get(symbol, 0)),
                    through_sequence=dict(replay_head or self.server.sequences).get(symbol, 0),
                )
                for event in replay:
                    consumer.replay_count += 1
                    from silver.market.ipc_protocol import write_message

                    await write_message(consumer.writer, event)
                    if int(event["sequence"]) >= 20:
                        for _ in range(100):
                            if consumer.acknowledged.get("AAPL", 0) >= 20:
                                ack_seen_during_replay.set()
                                break
                            await asyncio.sleep(0.001)
                    await asyncio.sleep(0.001)
            from silver.market.ipc_protocol import write_message

            await write_message(consumer.writer, {"type": "catchup_complete"})

        self.server._replay_to_consumer = slow_replay  # type: ignore[method-assign]
        self.socket_server = await self.server.start_socket_server()
        self.server_context = self.socket_server
        await self.server_context.__aenter__()

        from silver.market.ipc_protocol import read_message, write_message

        reader, writer = await asyncio.open_unix_connection(cfg.socket_path)
        try:
            await write_message(
                writer,
                {
                    "type": "hello",
                    "identity": "camatron",
                    "symbols": ["AAPL"],
                    "expected_contract_fingerprints": cfg.contract_fingerprints,
                    "last_sequences": {"AAPL": 0},
                },
            )
            self.assertEqual((await asyncio.wait_for(read_message(reader), timeout=2))["type"], "session_header")
            observations = 0
            while True:
                message = await asyncio.wait_for(read_message(reader), timeout=5)
                if message["type"] == "catchup_complete":
                    break
                self.assertEqual(message["type"], "observation")
                observations += 1
                await write_message(
                    writer,
                    {
                        "type": "ack",
                        "symbol": message["symbol"],
                        "sequence": message["sequence"],
                        "event_id": message["event_id"],
                    },
                )

            self.assertEqual(observations, 120)
            self.assertTrue(ack_seen_during_replay.is_set())
            self.assertEqual(self.server.consumers["camatron"].acknowledged["AAPL"], 120)
            self.assertEqual(self.server.consumers["camatron"].dropped_message_count, 0)
            self.assertEqual(self.server.consumers["camatron"].gap_count, 0)
        finally:
            self.server._replay_to_consumer = original_replay  # type: ignore[method-assign]
            writer.close()
            await writer.wait_closed()

    async def test_simultaneous_replays_do_not_repeat_full_recovery_scan(self):
        path = Path(self.config.journal_path)
        write_journal(path, *(journal_event(sequence=i) for i in range(1, 6)))
        server = SharedCanonicalMarketDataServer(self.config)
        recover_calls = 0

        def count_recover():
            nonlocal recover_calls
            recover_calls += 1
            raise AssertionError("recover() must not run during replay")

        server.journal.recover = count_recover  # type: ignore[method-assign]

        async def run_replay(identity: str) -> list[dict[str, Any]]:
            writer = DummyWriter()
            consumer = ConsumerState(
                identity=identity,
                writer=writer,
                queue=asyncio.Queue(maxsize=10),
                connected_at=pd.Timestamp.now(tz="UTC").isoformat(),
                replaying=True,
            )
            await server._replay_to_consumer(consumer, {"AAPL": 2}, replay_head=dict(server.sequences))
            return decode_buffered_messages(writer.buffer)

        left, right = await asyncio.gather(run_replay("camatron"), run_replay("megatron"))
        self.assertEqual(recover_calls, 0)
        self.assertEqual(
            [message["event_id"] for message in left if message["type"] == "observation"],
            [message["event_id"] for message in right if message["type"] == "observation"],
        )
        self.assertEqual([message["sequence"] for message in left if message["type"] == "observation"], [3, 4, 5])

    async def test_replay_window_exhaustion_uses_startup_oldest_sequence(self):
        payload = config_payload(self.temp.name, symbols=("AAPL",))
        payload["replay_limit_records"] = 3
        cfg = SharedMarketDataConfig.from_mapping(payload)
        path = Path(cfg.journal_path)
        write_journal(path, *(journal_event(sequence=i) for i in range(1, 7)))
        server = SharedCanonicalMarketDataServer(cfg)
        writer = DummyWriter()
        consumer = ConsumerState(
            identity="camatron",
            writer=writer,
            queue=asyncio.Queue(maxsize=10),
            connected_at=pd.Timestamp.now(tz="UTC").isoformat(),
            replaying=True,
        )

        await server._replay_to_consumer(consumer, {"AAPL": 1}, replay_head=dict(server.sequences))

        messages = decode_buffered_messages(writer.buffer)
        gap = next(message for message in messages if message["type"] == "gap")
        self.assertEqual(gap["reason"], "replay_window_exhausted")
        self.assertEqual(gap["oldest_available_sequence"], 4)

    async def test_upstream_waits_for_connect_auth_and_subscription_ack(self):
        class FakeWebsocket:
            def __init__(self):
                self.messages = [
                    json.dumps([{"T": "success", "msg": "connected"}]),
                    json.dumps([{"T": "success", "msg": "authenticated"}]),
                    json.dumps([{"T": "subscription", "trades": ["AAPL", "TSLA"]}]),
                ]
                self.sent = []

            async def recv(self):
                return self.messages.pop(0)

            async def send(self, payload):
                self.sent.append(json.loads(payload))

        websocket = FakeWebsocket()
        upstream = AlpacaSipUpstream(self.config)
        await upstream._wait_for_control_ack(websocket, phase="connect", expected_message="connected")
        await websocket.send(json.dumps({"action": "auth", "key": "redacted", "secret": "redacted"}))
        await upstream._wait_for_control_ack(websocket, phase="auth", expected_message="authenticated")
        await websocket.send(json.dumps({"action": "subscribe", "trades": list(self.config.symbols)}))
        await upstream._wait_for_control_ack(websocket, phase="subscription")

        self.assertEqual([message["action"] for message in websocket.sent], ["auth", "subscribe"])
        self.assertEqual(websocket.sent[1]["trades"], list(self.config.symbols))

    async def test_stale_socket_with_no_listener_is_recovered(self):
        await self.server_context.__aexit__(None, None, None)
        self.server_context = None
        self.server.cleanup_socket_path()
        path = Path(self.config.socket_path)
        stale = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            stale.bind(str(path))
        finally:
            stale.close()

        self.socket_server = await self.server.start_socket_server()
        self.server_context = self.socket_server
        await self.server_context.__aenter__()
        self.assertTrue(path.exists())
        self.assertTrue(any(event["type"] == "stale_socket_recovered" for event in self.server.health["audit_events"]))

    async def test_active_socket_listener_is_never_unlinked(self):
        await self.server_context.__aexit__(None, None, None)
        self.server_context = None
        self.server.cleanup_socket_path()
        path = Path(self.config.socket_path)
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            listener.bind(str(path))
            listener.listen(1)
            inode = path.stat().st_ino
            with self.assertRaisesRegex(RuntimeError, "active listener"):
                await self.server.start_socket_server()
            self.assertEqual(path.stat().st_ino, inode)
            self.assertTrue(any(event["type"] == "active_socket_owner_detected" for event in self.server.health["audit_events"]))
        finally:
            listener.close()
            path.unlink(missing_ok=True)

    async def test_socket_cleanup_removes_only_owned_socket(self):
        path = Path(self.config.socket_path)
        self.socket_server.close()
        await self.socket_server.wait_closed()
        self.server.cleanup_socket_path()
        self.assertFalse(path.exists())

    async def test_socket_cleanup_does_not_remove_successor_socket(self):
        path = Path(self.config.socket_path)
        old_path = path.with_name("m.sock.old")
        path.rename(old_path)
        successor = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            successor.bind(str(path))
            self.server.cleanup_socket_path()
            self.assertTrue(path.exists())
            self.assertTrue(any(event["type"] == "socket_cleanup_skipped" for event in self.server.health["audit_events"]))
        finally:
            successor.close()
            path.unlink(missing_ok=True)

    async def test_run_service_cancellation_cleans_owned_lock_and_socket(self):
        await self.server_context.__aexit__(None, None, None)
        self.server_context = None
        self.server.cleanup_socket_path()
        payload = config_payload(self.temp.name, symbols=("AAPL",))
        payload["lock_path"] = f"{self.temp.name}/m.lock"
        cfg = SharedMarketDataConfig.from_mapping(payload)

        class IdleUpstream:
            def __init__(self, config, status_callback=None):
                self.status_callback = status_callback

            async def stream(self):
                if self.status_callback is not None:
                    self.status_callback({"sip_connected": True, "authenticated": True, "subscription_acknowledged": True})
                while True:
                    await asyncio.sleep(1)
                    if False:
                        yield None

        task = asyncio.create_task(run_service(cfg, upstream_factory=IdleUpstream))
        try:
            for _ in range(50):
                if Path(cfg.lock_path).exists() and Path(cfg.socket_path).exists():
                    break
                await asyncio.sleep(0.02)
            self.assertTrue(Path(cfg.lock_path).exists())
            self.assertTrue(Path(cfg.socket_path).exists())
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self.assertFalse(Path(cfg.lock_path).exists())
        self.assertFalse(Path(cfg.socket_path).exists())


class SharedMarketDataUnitTests(unittest.TestCase):
    def test_singleton_lock_recovers_stale_legacy_dead_pid(self):
        with temporary_directory() as temp_dir:
            path = Path(temp_dir) / "live.lock"
            path.write_text("999999999\n", encoding="utf-8")
            lock = SingletonLock(path)
            lock.acquire()
            try:
                self.assertTrue(any(event["type"] == "stale_lock_recovered" for event in lock.events))
                self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["pid"], os.getpid())
            finally:
                lock.release()
            self.assertFalse(path.exists())

    def test_singleton_lock_recovers_empty_pathname_with_no_owner(self):
        with temporary_directory() as temp_dir:
            path = Path(temp_dir) / "live.lock"
            path.touch()
            lock = SingletonLock(path)
            lock.acquire()
            try:
                self.assertTrue(any(event["type"] == "stale_lock_recovered" for event in lock.events))
            finally:
                lock.release()
            self.assertFalse(path.exists())

    def test_singleton_lock_active_holder_fails_closed(self):
        with temporary_directory() as temp_dir:
            path = Path(temp_dir) / "live.lock"
            first = SingletonLock(path)
            first.acquire()
            try:
                second = SingletonLock(path)
                with self.assertRaisesRegex(RuntimeError, "held by another process"):
                    second.acquire()
                self.assertTrue(any(event["type"] == "active_lock_owner_detected" for event in second.events))
            finally:
                first.release()

    def test_singleton_lock_ambiguous_owner_fails_closed(self):
        with temporary_directory() as temp_dir:
            path = Path(temp_dir) / "live.lock"
            path.write_text(f"{os.getpid()}\n", encoding="utf-8")
            lock = SingletonLock(path)
            with self.assertRaisesRegex(RuntimeError, "ambiguous"):
                lock.acquire()
            self.assertTrue(any(event["type"] == "ambiguous_lock_owner" for event in lock.events))

    def test_singleton_lock_crash_restart_recovers_dead_owner_metadata(self):
        with temporary_directory() as temp_dir:
            path = Path(temp_dir) / "live.lock"
            path.write_text(
                json.dumps({"pid": 999999999, "process_start_token": "dead-process", "acquired_at_utc": "2026-08-24T13:20:00Z"}),
                encoding="utf-8",
            )
            lock = SingletonLock(path)
            lock.acquire()
            try:
                self.assertTrue(any(event["type"] == "stale_lock_recovered" for event in lock.events))
            finally:
                lock.release()
            self.assertFalse(path.exists())

    def test_singleton_lock_release_does_not_remove_successor_path(self):
        with temporary_directory() as temp_dir:
            path = Path(temp_dir) / "live.lock"
            old_path = Path(temp_dir) / "live.lock.old"
            lock = SingletonLock(path)
            lock.acquire()
            path.rename(old_path)
            path.write_text("successor\n", encoding="utf-8")
            lock.release()
            self.assertTrue(path.exists())
            self.assertEqual(path.read_text(encoding="utf-8"), "successor\n")

    def test_event_ids_are_deterministic(self):
        kwargs = {
            "contract_fingerprint": "fp",
            "symbol": "AAPL",
            "sequence": 1,
            "observation_timestamp": "2026-08-21T14:00:00Z",
            "close_price": "100.00",
            "source_record_id": "raw-1",
        }
        self.assertEqual(canonical_event_id(**kwargs), canonical_event_id(**kwargs))
        changed = dict(kwargs)
        changed["sequence"] = 2
        self.assertNotEqual(canonical_event_id(**kwargs), canonical_event_id(**changed))

    def test_journal_corruption_at_first_record_fails_closed(self):
        with temporary_directory() as temp_dir:
            path = Path(temp_dir) / "journal.jsonl"
            path.write_text("not-json\n", encoding="utf-8")
            recovered = CanonicalJournal(path).recover()
            self.assertFalse(recovered.healthy)
            self.assertEqual(len(recovered.events), 0)
            self.assertEqual(recovered.corrupted_records, 1)
            self.assertEqual(recovered.corruption.reason, "invalid_json_or_truncated_record")

    def test_journal_corruption_at_middle_record_fails_closed_without_skipping(self):
        with temporary_directory() as temp_dir:
            path = Path(temp_dir) / "journal.jsonl"
            write_journal(path, journal_event(sequence=1), b"not-json", journal_event(sequence=2))
            recovered = CanonicalJournal(path).recover()
            self.assertFalse(recovered.healthy)
            self.assertEqual([event["sequence"] for event in recovered.events], [1])
            self.assertEqual(recovered.corruption.reason, "invalid_json_or_truncated_record")

    def test_journal_corruption_at_final_truncated_record_fails_closed(self):
        with temporary_directory() as temp_dir:
            path = Path(temp_dir) / "journal.jsonl"
            path.write_bytes(CanonicalJournal.encode_record(journal_event(sequence=1)) + b"\n{\"type\":\"journal_record\"")
            recovered = CanonicalJournal(path).recover()
            self.assertFalse(recovered.healthy)
            self.assertEqual([event["sequence"] for event in recovered.events], [1])
            self.assertEqual(recovered.corruption.reason, "invalid_json_or_truncated_record")

    def test_journal_checksum_mismatch_fails_closed(self):
        with temporary_directory() as temp_dir:
            path = Path(temp_dir) / "journal.jsonl"
            bad = {
                "type": "journal_record",
                "version": "canonical_journal.v1",
                "payload": journal_event(sequence=1),
                "payload_sha256": "wrong",
            }
            path.write_text(json.dumps(bad, sort_keys=True) + "\n", encoding="utf-8")
            recovered = CanonicalJournal(path).recover()
            self.assertFalse(recovered.healthy)
            self.assertEqual(recovered.corruption.reason, "checksum_mismatch")

    def test_journal_invalid_json_fails_closed(self):
        with temporary_directory() as temp_dir:
            path = Path(temp_dir) / "journal.jsonl"
            path.write_text("{bad json}\n", encoding="utf-8")
            recovered = CanonicalJournal(path).recover()
            self.assertFalse(recovered.healthy)
            self.assertEqual(recovered.corruption.reason, "invalid_json_or_truncated_record")

    def test_journal_non_monotonic_sequence_fails_closed(self):
        with temporary_directory() as temp_dir:
            path = Path(temp_dir) / "journal.jsonl"
            write_journal(path, journal_event(sequence=1), journal_event(sequence=3))
            recovered = CanonicalJournal(path).recover()
            self.assertFalse(recovered.healthy)
            self.assertEqual([event["sequence"] for event in recovered.events], [1])
            self.assertEqual(recovered.corruption.reason, "non_monotonic_sequence")

    def test_archive_paths_rotate_by_new_york_session(self):
        from silver.market.canonical_journal import session_archive_path

        with temporary_directory() as temp_dir:
            path = session_archive_path(temp_dir, kind="canonical", timestamp="2026-08-22T02:00:00Z", symbol="AAPL")
            self.assertIn("/2026-08-21/", str(path))
            self.assertTrue(str(path).startswith(str(Path(temp_dir))))

    def test_market_data_credentials_load_from_protected_secret_file(self):
        with temporary_directory() as temp_dir:
            path = Path(temp_dir) / "secret.env"
            path.write_text(
                "ALPACA_MARKET_DATA_API_KEY_ID=key-id\n"
                "ALPACA_MARKET_DATA_API_SECRET_KEY=secret-key\n",
                encoding="utf-8",
            )
            creds = load_market_data_credentials("market_data", secret_path=path)
            self.assertEqual(creds["key_id"], "key-id")
            self.assertEqual(creds["secret_key"], "secret-key")

    def test_symbol_union_validation_rejects_missing_and_unneeded_qqq(self):
        with temporary_directory() as temp_dir:
            cfg = SharedMarketDataConfig.from_mapping(config_payload(temp_dir, symbols=("AAPL", "QQQ")))
            fleet = {"traders": [{"id": "aapl", "enabled": True, "symbol": "AAPL", "data": {"market_data_source": "shared_canonical_stream", "feed": "sip", "renko_source": "1s_close"}}]}
            with self.assertRaises(ValueError):
                validate_symbol_union(cfg, [fleet], require_shared_source=True)
            missing = SharedMarketDataConfig.from_mapping(config_payload(temp_dir, symbols=("AAPL",)))
            fleet["traders"].append({"id": "tsla", "enabled": True, "symbol": "TSLA", "data": {"market_data_source": "shared_canonical_stream", "feed": "sip", "renko_source": "1s_close"}})
            with self.assertRaises(ValueError):
                validate_symbol_union(missing, [fleet], require_shared_source=True)

    def test_shared_service_does_not_import_trading_client(self):
        text = Path("silver/market/shared_service.py").read_text(encoding="utf-8")
        self.assertNotIn("AlpacaTrader", text)
        self.assertNotIn("alpaca_trader", text)

    def test_service_templates_use_shared_stream_without_fleet_sip_profile(self):
        market = Path("trading-market-data-live.service").read_text(encoding="utf-8")
        camatron = Path("trading-fleet-live.service").read_text(encoding="utf-8")
        megatron = Path("trading-fleet-megatron-live.service").read_text(encoding="utf-8")
        self.assertIn("silver.market.shared_service", market)
        self.assertIn("trading-market-data-live.service", camatron)
        self.assertIn("--market-data-source shared_canonical_stream", camatron)
        self.assertIn("--market-data-source shared_canonical_stream", megatron)
        self.assertNotIn("--market-data-profile", camatron)
        self.assertNotIn("--market-data-profile", megatron)


class SharedStreamGateSynchronizationTests(unittest.IsolatedAsyncioTestCase):
    async def _process_two_observations(self, *, consumer_block=None, enabled=True, unsafe=False, limit=10000, pending_close=False):
        import fleet_runner
        from strategy_engine import RenkoStrategyConfig, RenkoStrategyEngine
        from trading_events import Signal, OrderIntent

        with temporary_directory() as directory:
            consumer = SharedCanonicalStreamConsumer(
                socket_path="/unused-offline.sock", identity="offline", expected_contract_fingerprints={},
                state_path=Path(directory) / "unused.json", symbols=("AAPL",),
            )
            consumer.catchup_complete = True
            consumer.startup_ready_symbols.add("AAPL")
            consumer.symbol_health["AAPL"] = {"journal_healthy": True, "gap_count": 0}
            if consumer_block == "gap":
                consumer.gapped = True
            elif consumer_block == "catchup":
                consumer.catchup_complete = False
            elif consumer_block == "not_ready":
                consumer.startup_ready_symbols.clear()
            elif consumer_block == "journal":
                consumer.symbol_health["AAPL"]["journal_healthy"] = False
            elif consumer_block == "delivery":
                consumer.symbol_health["AAPL"]["symbol_delivery_failure"] = "subscription_not_acknowledged"
            elif consumer_block == "emission":
                consumer.symbol_health["AAPL"].update({
                    "canonical_flush_expected": True,
                    "canonical_flush_expected_at": (pd.Timestamp.now(tz="UTC") - pd.Timedelta(seconds=30)).isoformat(),
                })

            async def observations():
                for sequence in (1, 2):
                    yield CanonicalStreamEvent(journal_event(sequence=sequence))
                await asyncio.Future()

            consumer.connect = observations
            consumer.ack = AsyncMock()
            engine = RenkoStrategyEngine(RenkoStrategyConfig(
                symbol="AAPL", brick_size=1, entry_bricks=1, qty=1,
                strategy_type="renko_tp_sl" if pending_close else "renko_reversal",
                take_profit_bricks=3 if pending_close else None,
                stop_loss_bricks=3 if pending_close else None,
            ))
            engine.reconciliation_unsafe = unsafe
            engine.pending_exit = {"broker_order_id": "existing-close"} if pending_close else None
            runtime = SimpleNamespace(
                trader_id="offline", symbol="AAPL", mode="live", credential_profile=None,
                account_namespace="test", feed="sip", renko_source="1s_close", enabled=enabled,
                execution_enabled=False, contract_fingerprints={}, evidence=None, log=Mock(),
                market_data_signal_block_reason="shared_market_data_not_ready",
                max_position_notional=limit, max_daily_loss=0, execution=Mock(),
                engine=engine,
            )
            timestamp = pd.Timestamp("2026-08-21T14:00:00Z")
            signal = Signal("offline", "exit" if pending_close else "entry", timestamp, "AAPL", "long", "buy", 100, "reversal", "offline")
            intent = OrderIntent("offline", timestamp, "AAPL", "buy", signal.signal_type, "market", "offline", "reversal", qty=1, close_position=pending_close)
            seen = []

            def process(**kwargs):
                reason = fleet_runner.should_block_for_risk(runtime, signal, intent)
                if reason is not None:
                    self.assertFalse(fleet_runner.handle_action(runtime, signal, intent))
                    runtime.execution.execute.assert_not_called()
                source = kwargs["source_event"]
                seen.append((runtime.market_data_signal_block_reason, reason, source.source_record_id, source.source_sequence))
                if len(seen) == 2:
                    raise asyncio.CancelledError()

            args = SimpleNamespace(shared_market_socket="/unused-offline.sock", execute_real=False, reconnect_delay=0)
            with patch.object(fleet_runner, "archive_root", return_value=Path(directory)), patch.object(
                fleet_runner, "archive_engine_state"
            ), patch.object(fleet_runner, "SharedCanonicalStreamConsumer", return_value=consumer), patch.object(
                fleet_runner, "process_market_tick_for_runtime", side_effect=process
            ), patch.object(fleet_runner, "flush_evidence"), patch.object(
                fleet_runner, "reconcile_broker_state_if_due_or_halt", side_effect=AssertionError("unexpected broker access")
            ):
                with self.assertRaises(asyncio.CancelledError):
                    await fleet_runner.run_market_stream("shared_canonical_stream", None, "live", "sip", [runtime], args)
            self.assertEqual([item[2:] for item in seen], [("aapl-1", 1), ("aapl-2", 2)])
            consumer.ack.assert_awaited_once()
            self.assertEqual(consumer.ack.await_args.args[0].sequence, 1)
            return runtime, seen

    async def test_ready_consumer_clears_cached_gate_without_restoring_it(self):
        runtime, seen = await self._process_two_observations()
        self.assertEqual([item[:2] for item in seen], [(None, None), (None, None)])
        self.assertIsNone(runtime.market_data_signal_block_reason)

    async def test_consumer_safety_conditions_remain_fail_closed(self):
        cases = {
            "gap": "shared_market_data_gap", "catchup": "shared_market_data_not_caught_up",
            "not_ready": "shared_market_data_not_ready", "journal": "shared_market_data_gap",
            "delivery": "shared_market_data_symbol_delivery_failed_subscription_not_acknowledged",
            "emission": "shared_market_data_canonical_emission_failed",
        }
        for state, reason in cases.items():
            with self.subTest(state=state):
                runtime, seen = await self._process_two_observations(consumer_block=state)
                self.assertEqual([item[:2] for item in seen], [(reason, reason), (reason, reason)])
                self.assertEqual(runtime.market_data_signal_block_reason, reason)

    async def test_consumer_readiness_does_not_clear_independent_execution_gates(self):
        cases = [
            ({"enabled": False}, "runtime_disabled"),
            ({"unsafe": True}, "reconciliation_unsafe"),
            ({"limit": 10}, "entry_notional_exceeds_limit_10"),
            ({"pending_close": True}, "tp_sl_close_already_submitted"),
        ]
        for kwargs, reason in cases:
            with self.subTest(reason=reason):
                _runtime, seen = await self._process_two_observations(**kwargs)
                self.assertEqual([item[:2] for item in seen], [(None, reason), (None, reason)])


if __name__ == "__main__":
    unittest.main()
