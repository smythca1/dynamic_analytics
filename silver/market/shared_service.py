from __future__ import annotations

import argparse
import asyncio
import contextlib
import errno
import fcntl
import json
import os
import signal
import ssl
import socket
import stat
import sys
import threading
import time
import traceback
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from pathlib import Path
from queue import Empty, Full, Queue
from typing import Any, Awaitable, Callable

import certifi
import pandas as pd
import websockets
from dotenv import load_dotenv

from trading_events import MarketTick, dataclass_to_jsonable, utc_timestamp_with_info

from .canonical_journal import CanonicalJournal, append_jsonl_atomic, purge_old_session_dirs, session_archive_path
from .contract import CANONICAL_RENKO_SOURCE, market_stream_contract
from .ipc_protocol import (
    PROTOCOL_VERSION,
    canonical_event_id,
    read_message,
    validate_consumer_identity,
    write_message,
)
from .live_event_stream import LiveMarketEvent, _stable_source_record_id
from .observation_stream import CanonicalMarketObservationAdapter
from .trade_filter import evaluate_market_tick_eligibility


DATA_STREAM_URL_TEMPLATE = "wss://stream.data.alpaca.markets/v2/{feed}"
PIPELINE_METRIC_WINDOW = 5000


@dataclass(frozen=True)
class SharedMarketDataConfig:
    feed: str
    canonical_source: str
    symbols: tuple[str, ...]
    socket_path: str
    journal_path: str
    archive_root: str
    health_path: str
    replay_limit_records: int
    consumer_queue_limit: int
    freshness_threshold_seconds: float
    disk_min_free_bytes: int
    disk_min_free_inodes: int
    journal_max_bytes: int
    retention_sessions: int
    credential_profile: str
    credential_secret_path: str | None = None
    raw_archive_enabled: bool = True
    nonessential_canonical_archive_enabled: bool = True
    canonical_journal_enabled: bool = True
    socket_mode: int = 0o660
    lock_path: str | None = None
    contract_fingerprints: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, payload: dict[str, Any]) -> "SharedMarketDataConfig":
        symbols = tuple(sorted({str(symbol).upper() for symbol in payload.get("symbols", [])}))
        if not symbols:
            raise ValueError("market-data config requires at least one symbol.")
        canonical_source = str(payload.get("canonical_source") or "").strip()
        if canonical_source != CANONICAL_RENKO_SOURCE:
            raise ValueError(f"canonical_source must be {CANONICAL_RENKO_SOURCE!r}.")
        feed = str(payload.get("feed") or "").strip().lower()
        if feed != "sip":
            raise ValueError("shared production market-data feed must be sip.")
        fingerprints = {
            str(key).upper(): str(value)
            for key, value in dict(payload.get("contract_fingerprints") or {}).items()
        }
        missing = [symbol for symbol in symbols if not fingerprints.get(symbol)]
        if missing:
            raise ValueError(f"missing contract fingerprints for: {missing}")
        mismatched = {
            symbol: {
                "configured": fingerprints[symbol],
                "generated": market_stream_contract(
                    symbol=symbol,
                    session_timezone="America/New_York",
                ).fingerprint,
            }
            for symbol in symbols
            if fingerprints[symbol]
            != market_stream_contract(symbol=symbol, session_timezone="America/New_York").fingerprint
        }
        if mismatched:
            raise ValueError(f"contract fingerprints do not match canonical contracts: {mismatched}")
        return cls(
            feed=feed,
            canonical_source=canonical_source,
            symbols=symbols,
            socket_path=str(payload["socket_path"]),
            journal_path=str(payload["journal_path"]),
            archive_root=str(payload["archive_root"]),
            health_path=str(payload["health_path"]),
            replay_limit_records=int(payload.get("replay_limit_records", 10000)),
            consumer_queue_limit=int(payload.get("consumer_queue_limit", 1000)),
            freshness_threshold_seconds=float(payload.get("freshness_threshold_seconds", 5.0)),
            disk_min_free_bytes=int(payload.get("disk_min_free_bytes", 1_000_000_000)),
            disk_min_free_inodes=int(payload.get("disk_min_free_inodes", 10000)),
            journal_max_bytes=int(payload.get("journal_max_bytes", 1_000_000_000)),
            retention_sessions=int(payload.get("retention_sessions", 15)),
            credential_profile=str(payload["credential_profile"]),
            credential_secret_path=str(payload.get("credential_secret_path") or "") or None,
            raw_archive_enabled=bool(payload.get("raw_archive_enabled", True)),
            nonessential_canonical_archive_enabled=bool(payload.get("nonessential_canonical_archive_enabled", True)),
            canonical_journal_enabled=bool(payload.get("canonical_journal_enabled", True)),
            socket_mode=int(str(payload.get("socket_mode", "660")), 8)
            if isinstance(payload.get("socket_mode"), str)
            else int(payload.get("socket_mode", 0o660)),
            lock_path=str(payload.get("lock_path") or f"{payload['socket_path']}.lock"),
            contract_fingerprints=fingerprints,
        )

    @classmethod
    def load(cls, path: str | Path) -> "SharedMarketDataConfig":
        with Path(path).open("r", encoding="utf-8") as handle:
            return cls.from_mapping(json.load(handle))


def validate_symbol_union(config: SharedMarketDataConfig, fleet_configs: list[dict[str, Any]], *, require_shared_source: bool) -> None:
    enabled_symbols: set[str] = set()
    for fleet in fleet_configs:
        for trader in fleet.get("traders", []):
            if not isinstance(trader, dict) or not trader.get("enabled", True):
                continue
            symbol = str(trader.get("symbol") or "").upper()
            if symbol:
                enabled_symbols.add(symbol)
            data = trader.get("data") if isinstance(trader.get("data"), dict) else {}
            source = str(data.get("market_data_source") or "").strip().lower()
            if require_shared_source and source not in {"shared_canonical_stream", "shared_canonical"}:
                raise ValueError(f"{trader.get('id')}: enabled trader must use shared_canonical_stream.")
            if require_shared_source and str(data.get("feed") or "").lower() not in {"", "sip"}:
                raise ValueError(f"{trader.get('id')}: shared canonical stream requires sip contract.")
            if require_shared_source and str(data.get("renko_source") or "") != CANONICAL_RENKO_SOURCE:
                raise ValueError(f"{trader.get('id')}: shared canonical stream requires 1s_close.")
    missing = enabled_symbols - set(config.symbols)
    if missing:
        raise ValueError(f"shared market-data config missing enabled symbols: {sorted(missing)}")
    if "QQQ" in set(config.symbols) and "QQQ" not in enabled_symbols:
        raise ValueError("QQQ must not be subscribed unless an enabled trader requires it.")


def validate_shared_market_data_config(
    config: SharedMarketDataConfig,
    fleet_configs: list[dict[str, Any]],
) -> None:
    """Validate the shared stream contract against every enabled production trader."""
    validate_symbol_union(config, fleet_configs, require_shared_source=True)

    # Keep the expected fingerprint calculation on the same canonical path used by
    # fleet_runner when it constructs each trader runtime.
    from shared.config.trader_config import build_strategy_config

    mismatches: list[str] = []
    for fleet in fleet_configs:
        for trader in fleet.get("traders", []):
            if not isinstance(trader, dict) or not trader.get("enabled", True):
                continue
            strategy_config = build_strategy_config(trader)
            expected = market_stream_contract(
                symbol=strategy_config.symbol,
                session_timezone=strategy_config.timezone,
            ).fingerprint
            configured = config.contract_fingerprints.get(strategy_config.symbol)
            if configured != expected:
                mismatches.append(
                    f"{trader.get('id')}: shared fingerprint for {strategy_config.symbol} "
                    f"is {configured!r}; expected {expected!r}"
                )
    if mismatches:
        raise ValueError("enabled trader market-stream fingerprints do not match shared config: " + "; ".join(mismatches))


class SingletonLock:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._fd: int | None = None
        self._inode: int | None = None
        self.events: list[dict[str, Any]] = []

    def _audit(self, event_type: str, **payload: Any) -> None:
        event = {
            "type": event_type,
            "timestamp": pd.Timestamp.now(tz="UTC").isoformat(),
            "lock_path": str(self.path),
            **payload,
        }
        self.events.append(event)
        print(json.dumps({"shared_market_data_singleton": event}, sort_keys=True, default=str), file=sys.stderr)

    @staticmethod
    def _pid_exists(pid: int) -> bool:
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    @staticmethod
    def _process_start_token(pid: int) -> str | None:
        proc_stat = Path(f"/proc/{pid}/stat")
        if not proc_stat.exists():
            return None
        try:
            return proc_stat.read_text(encoding="utf-8").split()[21]
        except (OSError, IndexError):
            return None

    @classmethod
    def _current_owner_metadata(cls) -> dict[str, Any]:
        pid = os.getpid()
        return {
            "pid": pid,
            "process_start_token": cls._process_start_token(pid),
            "acquired_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
        }

    @staticmethod
    def _parse_owner_metadata(raw: bytes) -> dict[str, Any]:
        text = raw.decode("utf-8", errors="replace").strip()
        if not text:
            return {"pid": None, "format": "empty"}
        if text.isdigit():
            return {"pid": int(text), "process_start_token": None, "format": "legacy_pid"}
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise RuntimeError("ambiguous shared market-data lock owner metadata") from exc
        if not isinstance(payload, dict):
            raise RuntimeError("ambiguous shared market-data lock owner metadata")
        pid = payload.get("pid")
        if pid is not None:
            try:
                payload["pid"] = int(pid)
            except (TypeError, ValueError) as exc:
                raise RuntimeError("ambiguous shared market-data lock owner metadata") from exc
        return payload

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        existed = self.path.exists()
        if existed:
            mode = self.path.stat().st_mode
            if not stat.S_ISREG(mode):
                self._audit("ambiguous_lock_owner", reason="lock_path_is_not_regular_file")
                raise RuntimeError(f"ambiguous shared market-data singleton lock path: {self.path}")
        self._fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o640)
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in (errno.EACCES, errno.EAGAIN):
                os.lseek(self._fd, 0, os.SEEK_SET)
                owner = self._parse_owner_metadata(os.read(self._fd, 4096))
                self._audit("active_lock_owner_detected", owner=owner)
                os.close(self._fd)
                self._fd = None
                raise RuntimeError(f"shared market-data singleton lock is held by another process: {self.path}") from exc
            os.close(self._fd)
            self._fd = None
            raise

        self._inode = os.fstat(self._fd).st_ino
        if existed:
            os.lseek(self._fd, 0, os.SEEK_SET)
            owner = self._parse_owner_metadata(os.read(self._fd, 4096))
            pid = owner.get("pid")
            start_token = owner.get("process_start_token")
            if isinstance(pid, int) and self._pid_exists(pid):
                current_token = self._process_start_token(pid)
                if start_token is None or current_token is None or str(start_token) == str(current_token):
                    self._audit("ambiguous_lock_owner", owner=owner, current_process_start_token=current_token)
                    fcntl.flock(self._fd, fcntl.LOCK_UN)
                    os.close(self._fd)
                    self._fd = None
                    self._inode = None
                    raise RuntimeError(f"ambiguous shared market-data singleton lock owner: {self.path}")
            self._audit("stale_lock_recovered", previous_owner=owner)

        payload = json.dumps(self._current_owner_metadata(), sort_keys=True).encode("utf-8") + b"\n"
        os.ftruncate(self._fd, 0)
        os.lseek(self._fd, 0, os.SEEK_SET)
        os.write(self._fd, payload)
        os.fsync(self._fd)

    def release(self) -> None:
        if self._fd is not None:
            try:
                current = self.path.stat()
            except FileNotFoundError:
                current = None
            if current is not None and self._inode is not None and current.st_ino == self._inode:
                try:
                    self.path.unlink()
                    self._audit("lock_cleanup_success")
                except OSError as exc:
                    self._audit("lock_cleanup_failure", error=str(exc))
            elif current is not None:
                self._audit("lock_cleanup_skipped", reason="path_inode_changed")
            os.close(self._fd)
            self._fd = None
            self._inode = None


@dataclass
class ConsumerState:
    identity: str
    writer: asyncio.StreamWriter
    queue: asyncio.Queue[dict[str, Any] | None]
    connected_at: str
    subscribed_symbols: tuple[str, ...] = field(default_factory=tuple)
    acknowledged: dict[str, int] = field(default_factory=dict)
    replay_count: int = 0
    gap_count: int = 0
    dropped_message_count: int = 0
    stale: bool = False
    replaying: bool = False
    pending_control_messages: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class ArchiveWriteRequest:
    path: Path
    payload: dict[str, Any]
    kind: str
    enqueued_monotonic: float


@dataclass(frozen=True)
class DiagnosticSpikeRequest:
    root: Path
    timestamp_token: str
    timestamp: str
    lag_seconds: float
    archive_writer_thread_id: int | None
    event_loop_thread_id: int
    tasks: list[dict[str, Any]]
    thread_frames: dict[int, Any]
    queue_state: dict[str, Any]
    health_policy: dict[str, bool]
    archive_root: str
    enqueued_monotonic: float


@dataclass(frozen=True)
class EventLoopWatchdogConfig:
    sample_interval_seconds: float = 0.05
    stall_threshold_seconds: float = 0.25
    max_samples_per_stall: int = 12
    max_samples_total: int = 240


class EventLoopWatchdog:
    def __init__(
        self,
        *,
        root: str | Path,
        metrics: PipelineMetrics,
        config: EventLoopWatchdogConfig | None = None,
    ) -> None:
        self.root = Path(root)
        self.metrics = metrics
        self.config = config or EventLoopWatchdogConfig()
        self.thread: threading.Thread | None = None
        self.closed = threading.Event()
        self.loop_thread_id: int | None = None
        self.expected_heartbeat_deadline = float("inf")
        self.sample_count = 0
        self.current_stall_samples = 0
        self.was_stalled = False
        self.failure: BaseException | None = None
        self._lock = threading.Lock()

    def start(self) -> None:
        if self.thread is None:
            self.thread = threading.Thread(target=self._run, name="shared-event-loop-watchdog", daemon=True)
            self.thread.start()

    async def stop(self, timeout_seconds: float = 2.0) -> None:
        if self.thread is None:
            return
        self.closed.set()
        await asyncio.to_thread(self.thread.join, timeout_seconds)
        if self.thread.is_alive():
            self.metrics.increment("event_loop_watchdog_shutdown_timeouts")
        self.thread = None

    def set_loop_thread(self, thread_id: int) -> None:
        with self._lock:
            self.loop_thread_id = int(thread_id)
            self.expected_heartbeat_deadline = float("inf")

    def expect_heartbeat(self, deadline: float) -> None:
        with self._lock:
            self.expected_heartbeat_deadline = float(deadline)

    def heartbeat(self, next_deadline: float | None = None) -> None:
        with self._lock:
            self.expected_heartbeat_deadline = float("inf") if next_deadline is None else float(next_deadline)
            if self.was_stalled:
                self.current_stall_samples = 0
                self.was_stalled = False

    def _run(self) -> None:
        self.metrics.increment("event_loop_watchdog_thread_started")
        try:
            while not self.closed.wait(self.config.sample_interval_seconds):
                with self._lock:
                    loop_thread_id = self.loop_thread_id
                    lag_seconds = time.perf_counter() - self.expected_heartbeat_deadline
                    sample_count = self.sample_count
                    current_stall_samples = self.current_stall_samples
                if loop_thread_id is None or lag_seconds < self.config.stall_threshold_seconds:
                    continue
                if sample_count >= self.config.max_samples_total:
                    self.metrics.increment("event_loop_watchdog_sample_limit_reached")
                    continue
                if current_stall_samples >= self.config.max_samples_per_stall:
                    self.metrics.increment("event_loop_watchdog_stall_sample_limit_reached")
                    continue
                self._capture_sample(loop_thread_id=loop_thread_id, lag_seconds=lag_seconds)
        except BaseException as exc:
            self.failure = exc
            self.metrics.increment("event_loop_watchdog_failures")
        finally:
            self.metrics.increment("event_loop_watchdog_thread_stopped")

    def _capture_sample(self, *, loop_thread_id: int, lag_seconds: float) -> None:
        try:
            timestamp = pd.Timestamp.now(tz="UTC")
            frames = sys._current_frames()
            loop_frame = frames.get(loop_thread_id)
            payload = {
                "timestamp": timestamp.isoformat(),
                "lag_seconds": lag_seconds,
                "loop_thread_id": loop_thread_id,
                "sampler_thread_id": threading.get_ident(),
                "loop_stack": traceback.format_stack(loop_frame, limit=30) if loop_frame is not None else None,
                "threads": {
                    str(thread_id): traceback.format_stack(frame, limit=20)
                    for thread_id, frame in frames.items()
                },
                "metrics": self.metrics.snapshot(),
                "capture_note": "Captured by watchdog thread while the asyncio event-loop heartbeat was stale.",
            }
            raw = json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n"
            self.root.mkdir(parents=True, exist_ok=True)
            path = self.root / f"watchdog_{timestamp.strftime('%Y%m%dT%H%M%S.%fZ')}_{int(lag_seconds * 1000)}ms.json"
            path.write_text(raw, encoding="utf-8")
            with self._lock:
                self.sample_count += 1
                self.current_stall_samples += 1
                self.was_stalled = True
            self.metrics.increment("event_loop_watchdog_samples")
            self.metrics.observe("event_loop_watchdog_observed_lag_seconds", lag_seconds)
        except Exception:
            self.metrics.increment("event_loop_watchdog_capture_failures")


def append_jsonl_batch_atomic(path: str | Path, payloads: list[dict[str, Any]]) -> None:
    if not payloads:
        return
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    raw = b"".join(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8") + b"\n"
        for payload in payloads
    )
    fd = os.open(target, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o640)
    try:
        os.write(fd, raw)
        os.fsync(fd)
    finally:
        os.close(fd)


class PipelineMetrics:
    def __init__(self, *, window_size: int = PIPELINE_METRIC_WINDOW) -> None:
        self.window_size = int(window_size)
        self._durations: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=self.window_size))
        self._counts: dict[str, int] = defaultdict(int)
        self._gauges: dict[str, float] = {}
        self._high_water: dict[str, float] = defaultdict(float)
        self._lock = threading.Lock()

    def observe(self, name: str, value: float) -> None:
        value = float(value)
        with self._lock:
            self._durations[name].append(value)
            self._counts[name] += 1

    def increment(self, name: str, value: int = 1) -> None:
        with self._lock:
            self._counts[name] += int(value)

    def gauge(self, name: str, value: float) -> None:
        value = float(value)
        with self._lock:
            self._gauges[name] = value
            self._high_water[name] = max(self._high_water[name], value)

    @staticmethod
    def _percentile(values: list[float], pct: float) -> float | None:
        if not values:
            return None
        ordered = sorted(values)
        if len(ordered) == 1:
            return ordered[0]
        rank = (len(ordered) - 1) * (pct / 100.0)
        low = int(rank)
        high = min(low + 1, len(ordered) - 1)
        fraction = rank - low
        return ordered[low] * (1 - fraction) + ordered[high] * fraction

    def snapshot(self) -> dict[str, Any]:
        durations = {}
        with self._lock:
            duration_items = [(name, list(values)) for name, values in self._durations.items()]
            counts = dict(self._counts)
            gauges = dict(self._gauges)
            high_water = dict(self._high_water)
        for name, data in duration_items:
            durations[name] = {
                "count": counts.get(name, 0),
                "window_count": len(data),
                "p50": self._percentile(data, 50),
                "p95": self._percentile(data, 95),
                "p99": self._percentile(data, 99),
                "max": max(data) if data else None,
            }
        return {
            "durations_seconds": durations,
            "counts": counts,
            "gauges": gauges,
            "high_water": high_water,
        }


class AsyncArchiveWriter:
    def __init__(
        self,
        *,
        max_queue_size: int,
        metrics: PipelineMetrics,
        batch_size: int = 1000,
        flush_interval_seconds: float = 0.25,
        fsync_interval_seconds: float = 2.0,
        stop_drain_timeout_seconds: float = 10.0,
    ) -> None:
        self.queue: Queue[ArchiveWriteRequest | None] = Queue(maxsize=max_queue_size)
        self.metrics = metrics
        self.batch_size = max(1, int(batch_size))
        self.flush_interval_seconds = max(0.01, float(flush_interval_seconds))
        self.fsync_interval_seconds = max(self.flush_interval_seconds, float(fsync_interval_seconds))
        self.stop_drain_timeout_seconds = max(0.1, float(stop_drain_timeout_seconds))
        self.thread: threading.Thread | None = None
        self.closed = False
        self.failure: BaseException | None = None
        self.writer_thread_id: int | None = None

    def start(self) -> None:
        if self.thread is None:
            self.thread = threading.Thread(target=self._run, name="shared-raw-archive-writer", daemon=True)
            self.thread.start()

    async def stop(self) -> None:
        if self.thread is None:
            return
        self.closed = True
        started = time.perf_counter()
        while True:
            try:
                self.queue.put(None, timeout=0.1)
                break
            except Full:
                if time.perf_counter() - started > self.stop_drain_timeout_seconds:
                    self.failure = TimeoutError("archive writer shutdown drain timed out")
                    self.metrics.increment("archive_writer_shutdown_timeouts")
                    break
        await asyncio.to_thread(self.thread.join, self.stop_drain_timeout_seconds)
        if self.thread.is_alive():
            self.failure = TimeoutError("archive writer thread did not stop before timeout")
            self.metrics.increment("archive_writer_shutdown_timeouts")
        self.thread = None

    def enqueue(self, path: str | Path, payload: dict[str, Any], *, kind: str) -> None:
        if self.closed:
            raise RuntimeError("archive writer is closed")
        if self.failure is not None:
            raise RuntimeError("archive writer failed; shutting down ingestion") from self.failure
        request = ArchiveWriteRequest(Path(path), payload, kind, time.perf_counter())
        try:
            self.queue.put_nowait(request)
        except Full as exc:
            self.metrics.increment(f"{kind}_archive_queue_overflow")
            raise RuntimeError(f"{kind} archive queue exhausted; shutting down ingestion") from exc
        depth = self.queue.qsize()
        self.metrics.gauge(f"{kind}_archive_queue_depth", depth)
        self.metrics.gauge("archive_queue_depth", depth)

    async def join(self, timeout: float | None = None) -> None:
        await asyncio.wait_for(asyncio.to_thread(self.queue.join), timeout=timeout)

    def _run(self) -> None:
        self.writer_thread_id = threading.get_ident()
        self.metrics.increment("archive_writer_thread_started")
        handles: dict[Path, Any] = {}
        pending: dict[Path, list[tuple[ArchiveWriteRequest, bytes]]] = defaultdict(list)
        pending_counts: dict[Path, int] = defaultdict(int)
        last_flush = time.perf_counter()
        last_fsync = time.perf_counter()
        stop_requested = False
        try:
            while not stop_requested:
                batch: list[ArchiveWriteRequest] = []
                timeout = self.flush_interval_seconds if pending else 0.1
                got_item = False
                try:
                    item = self.queue.get(timeout=timeout)
                    got_item = True
                except Empty:
                    item = None
                if item is None:
                    if got_item:
                        self.queue.task_done()
                        stop_requested = True
                    elif self.closed:
                        stop_requested = True
                    else:
                        self._flush_pending(handles, pending, pending_counts, fsync=False)
                        last_flush = time.perf_counter()
                        continue
                else:
                    batch.append(item)
                    for _ in range(self.batch_size - 1):
                        try:
                            extra = self.queue.get_nowait()
                        except Empty:
                            break
                        if extra is None:
                            self.queue.task_done()
                            stop_requested = True
                            break
                        batch.append(extra)
                pickup_time = time.perf_counter()
                for request in batch:
                    serialize_started = time.perf_counter()
                    raw = json.dumps(request.payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8") + b"\n"
                    self.metrics.observe(f"{request.kind}_archive_serialize_seconds", time.perf_counter() - serialize_started)
                    self.metrics.observe(f"{request.kind}_archive_queue_wait_seconds", pickup_time - request.enqueued_monotonic)
                    pending[request.path].append((request, raw))
                    pending_counts[request.path] += 1
                now = time.perf_counter()
                should_flush = stop_requested or any(count >= self.batch_size for count in pending_counts.values()) or now - last_flush >= self.flush_interval_seconds
                should_fsync = stop_requested or now - last_fsync >= self.fsync_interval_seconds
                if should_flush:
                    self._flush_pending(handles, pending, pending_counts, fsync=should_fsync)
                    last_flush = time.perf_counter()
                    if should_fsync:
                        last_fsync = last_flush
                for _ in batch:
                    self.queue.task_done()
                depth = self.queue.qsize()
                self.metrics.gauge("archive_queue_depth", depth)
        except BaseException as exc:
            self.failure = exc
            self.metrics.increment("archive_write_failures")
        finally:
            try:
                self._flush_pending(handles, pending, pending_counts, fsync=True)
            finally:
                for handle in handles.values():
                    with contextlib.suppress(Exception):
                        handle.close()
                self.metrics.increment("archive_writer_thread_stopped")

    def _flush_pending(
        self,
        handles: dict[Path, Any],
        pending: dict[Path, list[tuple[ArchiveWriteRequest, bytes]]],
        pending_counts: dict[Path, int],
        *,
        fsync: bool,
    ) -> None:
        for path, items in list(pending.items()):
            if not items:
                continue
            kind = items[0][0].kind
            if path not in handles:
                path.parent.mkdir(parents=True, exist_ok=True)
                handles[path] = path.open("ab", buffering=1024 * 1024)
            buffered_started = time.perf_counter()
            handles[path].writelines(raw for _request, raw in items)
            buffered_done = time.perf_counter()
            handles[path].flush()
            flush_done = time.perf_counter()
            if fsync:
                os.fsync(handles[path].fileno())
            fsync_done = time.perf_counter()
            self.metrics.observe(f"{kind}_archive_buffered_write_seconds", buffered_done - buffered_started)
            self.metrics.observe(f"{kind}_archive_flush_seconds", flush_done - buffered_done)
            if fsync:
                self.metrics.observe(f"{kind}_archive_fsync_seconds", fsync_done - flush_done)
            self.metrics.observe(f"{kind}_archive_batch_size", len(items))
            self.metrics.increment(f"{kind}_archive_writes", len(items))
            self.metrics.increment(f"{kind}_archive_batch_writes")
            pending[path] = []
            pending_counts[path] = 0


class DiagnosticSpikeWriter:
    def __init__(self, *, max_queue_size: int, metrics: PipelineMetrics, stop_drain_timeout_seconds: float = 5.0) -> None:
        self.queue: Queue[DiagnosticSpikeRequest | None] = Queue(maxsize=max_queue_size)
        self.metrics = metrics
        self.stop_drain_timeout_seconds = max(0.1, float(stop_drain_timeout_seconds))
        self.thread: threading.Thread | None = None
        self.closed = False
        self.failure: BaseException | None = None
        self.writer_thread_id: int | None = None

    def start(self) -> None:
        if self.thread is None:
            self.thread = threading.Thread(target=self._run, name="shared-diagnostic-spike-writer", daemon=True)
            self.thread.start()

    async def stop(self) -> None:
        if self.thread is None:
            return
        self.closed = True
        with contextlib.suppress(Full):
            self.queue.put(None, timeout=0.1)
        await asyncio.to_thread(self.thread.join, self.stop_drain_timeout_seconds)
        if self.thread.is_alive():
            self.metrics.increment("event_loop_spike_writer_shutdown_timeouts")
        self.thread = None

    def enqueue(self, request: DiagnosticSpikeRequest) -> None:
        if self.closed:
            self.metrics.increment("event_loop_spike_capture_skipped_after_close")
            return
        try:
            self.queue.put_nowait(request)
        except Full:
            self.metrics.increment("event_loop_spike_capture_queue_overflow")
            self.metrics.gauge("event_loop_spike_capture_queue_depth", self.queue.qsize())
            return
        self.metrics.gauge("event_loop_spike_capture_queue_depth", self.queue.qsize())

    async def join(self, timeout: float | None = None) -> None:
        await asyncio.wait_for(asyncio.to_thread(self.queue.join), timeout=timeout)

    def _run(self) -> None:
        self.writer_thread_id = threading.get_ident()
        self.metrics.increment("event_loop_spike_writer_thread_started")
        try:
            while True:
                item = self.queue.get()
                try:
                    if item is None:
                        return
                    self._write_request(item)
                finally:
                    self.queue.task_done()
                    self.metrics.gauge("event_loop_spike_capture_queue_depth", self.queue.qsize())
        except BaseException as exc:
            self.failure = exc
            self.metrics.increment("event_loop_spike_capture_failures")
        finally:
            self.metrics.increment("event_loop_spike_writer_thread_stopped")

    def _write_request(self, request: DiagnosticSpikeRequest) -> None:
        started = time.perf_counter()
        try:
            usage = os.statvfs(request.archive_root)
        except FileNotFoundError:
            usage = os.statvfs(str(Path(request.archive_root).parent))
        payload = {
            "timestamp": request.timestamp,
            "lag_seconds": request.lag_seconds,
            "archive_writer_thread_id": request.archive_writer_thread_id,
            "diagnostic_writer_thread_id": self.writer_thread_id,
            "thread_id": request.event_loop_thread_id,
            "tasks": [
                {
                    **{key: value for key, value in task.items() if key != "frames"},
                    "stack": [
                        "".join(traceback.format_stack(frame, limit=1)).strip()
                        for frame in task.get("frames", [])
                    ],
                }
                for task in request.tasks
            ],
            "threads": {
                str(thread_id): traceback.format_stack(frame, limit=20)
                for thread_id, frame in request.thread_frames.items()
            },
            "queue_state": request.queue_state,
            "health_policy": request.health_policy,
            "disk": {
                "free_bytes": usage.f_bavail * usage.f_frsize,
                "free_inodes": usage.f_favail,
            },
            "loadavg": os.getloadavg() if hasattr(os, "getloadavg") else None,
            "metrics": self.metrics.snapshot(),
        }
        serialize_started = time.perf_counter()
        raw = json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n"
        self.metrics.observe("event_loop_spike_capture_serialize_seconds", time.perf_counter() - serialize_started)
        write_started = time.perf_counter()
        request.root.mkdir(parents=True, exist_ok=True)
        path = request.root / f"spike_{request.timestamp_token}_{int(request.lag_seconds * 1000)}ms.json"
        path.write_text(raw, encoding="utf-8")
        self.metrics.observe("event_loop_spike_capture_write_seconds", time.perf_counter() - write_started)
        self.metrics.observe("event_loop_spike_capture_worker_seconds", time.perf_counter() - started)
        self.metrics.increment("event_loop_spike_captures")


class SharedCanonicalMarketDataServer:
    def __init__(self, config: SharedMarketDataConfig) -> None:
        self.config = config
        self.journal = CanonicalJournal(config.journal_path, max_replay_records=config.replay_limit_records)
        recovered = self.journal.recover()
        self.recovery = recovered
        self.journal_corruption = recovered.corruption
        self.events_by_symbol: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in config.symbols}
        self.sequences: dict[str, int] = {
            symbol: int(recovered.latest_sequences.get(symbol, 0)) for symbol in config.symbols
        }
        for event in recovered.events:
            symbol = str(event.get("symbol") or "").upper()
            if symbol in self.events_by_symbol:
                self.events_by_symbol[symbol].append(event)
                self.sequences[symbol] = max(self.sequences[symbol], int(event.get("sequence") or 0))
        self.corrupted_journal_records = recovered.corrupted_records
        self.consumers: dict[str, ConsumerState] = {}
        self.metrics = PipelineMetrics()
        self.archive_writer: AsyncArchiveWriter | None = None
        self.diagnostic_spike_writer: DiagnosticSpikeWriter | None = None
        self.event_loop_watchdog: EventLoopWatchdog | None = None
        self._canonical_heartbeat_monotonic_by_symbol: dict[str, float] = {}
        self._canonical_publication_monotonic_by_symbol: dict[str, float] = {}
        self._canonical_source_high_water_by_symbol: dict[str, pd.Timestamp] = {}
        self._last_symbol_health_publish_monotonic: dict[str, float] = {}
        self.health: dict[str, Any] = {
            "sip_connected": False,
            "subscription_acknowledged": False,
            "raw_archive_enabled": config.raw_archive_enabled,
            "nonessential_canonical_archive_enabled": config.nonessential_canonical_archive_enabled,
            "canonical_journal_enabled": config.canonical_journal_enabled,
            "upstream_reconnect_count": 0,
            "gap_count": 0,
            "dropped_message_count": 0,
            "corrupted_journal_records": self.corrupted_journal_records,
            "journal_healthy": recovered.healthy,
            "critical_events": [],
            "audit_events": [],
            "last_raw_trade_by_symbol": {},
            "last_eligible_trade_by_symbol": {},
            "raw_trade_count_by_symbol": {},
            "eligible_raw_trade_count_by_symbol": {},
            "excluded_raw_trade_count_by_symbol": {},
            "last_canonical_observation_by_symbol": {},
            "last_canonical_heartbeat_by_symbol": {},
            "last_canonical_publication_by_symbol": {},
            "last_canonical_sampler_admission_by_symbol": {},
            "last_canonical_source_timestamp_by_symbol": {},
            "pending_canonical_bucket_by_symbol": {},
            "canonical_flush_expected_by_symbol": {},
            "canonical_flush_expected_at_by_symbol": {},
            "symbol_delivery_failure_by_symbol": {},
            "consumers": {},
        }
        self._last_health_write_monotonic = 0.0
        self._health_write_interval_seconds = 2.0
        if self.journal_corruption is not None:
            self._critical_event(
                "journal_corruption",
                {
                    "line_number": self.journal_corruption.line_number,
                    "byte_offset": self.journal_corruption.byte_offset,
                    "reason": self.journal_corruption.reason,
                    "symbol": self.journal_corruption.symbol,
                    "sequence": self.journal_corruption.sequence,
                    "quarantine_path": self.journal_corruption.quarantine_path,
                },
            )
        self._socket_inode: int | None = None

    def start_archive_writer(self) -> None:
        if self.archive_writer is None:
            self.archive_writer = AsyncArchiveWriter(
                max_queue_size=max(1000, self.config.consumer_queue_limit * 20),
                metrics=self.metrics,
            )
            self.archive_writer.start()
        if self.diagnostic_spike_writer is None:
            self.diagnostic_spike_writer = DiagnosticSpikeWriter(
                max_queue_size=max(10, self.config.consumer_queue_limit),
                metrics=self.metrics,
            )
            self.diagnostic_spike_writer.start()
        if self.event_loop_watchdog is None:
            self.event_loop_watchdog = EventLoopWatchdog(
                root=Path(self.config.archive_root) / "diagnostics" / "event_loop_watchdog",
                metrics=self.metrics,
            )
            self.event_loop_watchdog.start()

    async def stop_archive_writer(self) -> None:
        if self.event_loop_watchdog is not None:
            await self.event_loop_watchdog.stop()
            self.event_loop_watchdog = None
        if self.diagnostic_spike_writer is not None:
            await self.diagnostic_spike_writer.stop()
            self.diagnostic_spike_writer = None
        if self.archive_writer is not None:
            await self.archive_writer.stop()
            self.archive_writer = None

    async def _archive_jsonl(self, path: str | Path, payload: dict[str, Any], *, kind: str) -> None:
        started = time.perf_counter()
        if kind == "raw" and not self.config.raw_archive_enabled:
            self.metrics.increment("raw_archive_disabled_skips")
            return
        if kind == "canonical" and not self.config.nonessential_canonical_archive_enabled:
            self.metrics.increment("canonical_archive_disabled_skips")
            return
        if self.archive_writer is None:
            append_jsonl_atomic(path, payload)
            self.metrics.observe(f"{kind}_archive_durable_write_seconds", time.perf_counter() - started)
            self.metrics.increment(f"{kind}_archive_writes")
            return
        if self.archive_writer.failure is not None:
            raise RuntimeError("archive writer failed; shutting down ingestion") from self.archive_writer.failure
        self.archive_writer.enqueue(path, payload, kind=kind)
        self.metrics.observe(f"{kind}_archive_enqueue_seconds", time.perf_counter() - started)

    async def monitor_event_loop_lag(self, *, interval_seconds: float = 1.0) -> None:
        if self.event_loop_watchdog is not None:
            self.event_loop_watchdog.set_loop_thread(threading.get_ident())
        expected = time.perf_counter() + interval_seconds
        consecutive_over_500ms = 0
        while True:
            if self.event_loop_watchdog is not None:
                self.event_loop_watchdog.expect_heartbeat(expected)
            await asyncio.sleep(interval_seconds)
            actual = time.perf_counter()
            lag = max(0.0, actual - expected)
            self.metrics.observe("event_loop_lag_seconds", lag)
            if lag > 0.25:
                self.metrics.increment("event_loop_spikes_over_250ms")
                if lag > 0.5:
                    consecutive_over_500ms += 1
                    self.metrics.gauge("event_loop_consecutive_spikes_over_500ms", consecutive_over_500ms)
                else:
                    consecutive_over_500ms = 0
                    self.metrics.gauge("event_loop_consecutive_spikes_over_500ms", 0)
                self._capture_event_loop_spike(lag_seconds=lag)
            else:
                consecutive_over_500ms = 0
                self.metrics.gauge("event_loop_consecutive_spikes_over_500ms", 0)
            expected = actual + interval_seconds
            if self.event_loop_watchdog is not None:
                self.event_loop_watchdog.heartbeat(expected)

    def _capture_event_loop_spike(self, *, lag_seconds: float) -> None:
        try:
            if self.diagnostic_spike_writer is None:
                self.metrics.increment("event_loop_spike_capture_skipped_no_writer")
                return
            root = Path(self.config.archive_root) / "diagnostics" / "event_loop_spikes"
            timestamp = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%S.%fZ")
            tasks = []
            for task in asyncio.all_tasks():
                frames = task.get_stack(limit=10)
                tasks.append(
                    {
                        "name": task.get_name(),
                        "done": task.done(),
                        "cancelled": task.cancelled(),
                        "coro": repr(task.get_coro()),
                        "frames": frames,
                    }
                )
            archive_root = str(Path(self.config.archive_root).parent if not Path(self.config.archive_root).exists() else self.config.archive_root)
            request = DiagnosticSpikeRequest(
                root=root,
                timestamp_token=timestamp,
                timestamp=pd.Timestamp.now(tz="UTC").isoformat(),
                lag_seconds=lag_seconds,
                archive_writer_thread_id=self.archive_writer.writer_thread_id if self.archive_writer else None,
                event_loop_thread_id=threading.get_ident(),
                tasks=tasks,
                thread_frames=sys._current_frames(),
                queue_state={
                    "archive_queue_depth": self.archive_writer.queue.qsize() if self.archive_writer else None,
                    "consumers": {identity: state.queue.qsize() for identity, state in self.consumers.items()},
                },
                health_policy={
                    "raw_archive_enabled": self.config.raw_archive_enabled,
                    "nonessential_canonical_archive_enabled": self.config.nonessential_canonical_archive_enabled,
                    "canonical_journal_enabled": self.config.canonical_journal_enabled,
                },
                archive_root=archive_root,
                enqueued_monotonic=time.perf_counter(),
            )
            self.diagnostic_spike_writer.enqueue(request)
            self.metrics.increment("event_loop_spike_capture_enqueued")
        except Exception as exc:
            self.metrics.increment("event_loop_spike_capture_failures")
            self.health["last_event_loop_spike_capture_error"] = str(exc)

    def _audit_event(self, event_type: str, payload: dict[str, Any] | None = None) -> None:
        event = {
            "type": event_type,
            "timestamp": pd.Timestamp.now(tz="UTC").isoformat(),
            **(payload or {}),
        }
        self.health.setdefault("audit_events", []).append(event)
        self.health["audit_events"] = self.health["audit_events"][-100:]

    def _critical_event(self, event_type: str, payload: dict[str, Any]) -> None:
        event = {
            "type": event_type,
            "severity": "critical",
            "timestamp": pd.Timestamp.now(tz="UTC").isoformat(),
            **payload,
        }
        self.health.setdefault("critical_events", []).append(event)
        self.health["critical_events"] = self.health["critical_events"][-100:]

    def _socket_has_listener(self, path: Path) -> bool:
        probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            probe.settimeout(0.2)
            probe.connect(str(path))
            return True
        except FileNotFoundError:
            return False
        except ConnectionRefusedError:
            return False
        except PermissionError as exc:
            self._audit_event("ambiguous_socket_ownership", {"socket_path": str(path), "error": str(exc)})
            raise RuntimeError(f"ambiguous shared market-data socket ownership: {path}") from exc
        except OSError as exc:
            self._audit_event("ambiguous_socket_ownership", {"socket_path": str(path), "error": str(exc)})
            raise RuntimeError(f"ambiguous shared market-data socket ownership: {path}") from exc
        finally:
            probe.close()

    def _prepare_socket_path(self, path: Path) -> None:
        if not path.exists():
            return
        mode = path.stat().st_mode
        if not stat.S_ISSOCK(mode):
            self._audit_event("ambiguous_socket_ownership", {"socket_path": str(path), "reason": "path_is_not_socket"})
            raise RuntimeError(f"ambiguous shared market-data socket path: {path}")
        if self._socket_has_listener(path):
            self._audit_event("active_socket_owner_detected", {"socket_path": str(path)})
            raise RuntimeError(f"shared market-data socket is already owned by an active listener: {path}")
        path.unlink()
        self._audit_event("stale_socket_recovered", {"socket_path": str(path)})

    async def start_socket_server(self) -> asyncio.AbstractServer:
        path = Path(self.config.socket_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._prepare_socket_path(path)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.bind(str(path))
            sock.listen(socket.SOMAXCONN)
            sock.setblocking(False)
            server = await asyncio.start_unix_server(self._handle_consumer, sock=sock)
        except Exception:
            sock.close()
            raise
        os.chmod(path, self.config.socket_mode)
        self._socket_inode = path.stat().st_ino
        return server

    def cleanup_socket_path(self) -> None:
        path = Path(self.config.socket_path)
        if self._socket_inode is None:
            return
        try:
            current = path.stat()
        except FileNotFoundError:
            self._socket_inode = None
            return
        if current.st_ino != self._socket_inode:
            self._audit_event("socket_cleanup_skipped", {"socket_path": str(path), "reason": "path_inode_changed"})
            self._socket_inode = None
            return
        try:
            path.unlink()
            self._audit_event("socket_cleanup_success", {"socket_path": str(path)})
        except OSError as exc:
            self._audit_event("socket_cleanup_failure", {"socket_path": str(path), "error": str(exc)})
        finally:
            self._socket_inode = None

    async def _handle_consumer(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        consumer: ConsumerState | None = None
        pump: asyncio.Task[None] | None = None
        control_reader: asyncio.Task[None] | None = None
        try:
            hello = await read_message(reader)
            if hello.get("type") != "hello":
                raise ValueError("first consumer message must be hello.")
            identity = validate_consumer_identity(str(hello.get("identity") or ""))
            try:
                subscribed_symbols = self._consumer_subscribed_symbols(hello)
            except ValueError as exc:
                await write_message(writer, {"type": "contract_mismatch", "reason": "invalid_symbol_subscription", "error": str(exc)})
                return
            expected = {str(k).upper(): str(v) for k, v in dict(hello.get("expected_contract_fingerprints") or {}).items()}
            for symbol in subscribed_symbols:
                expected_fp = expected.get(symbol)
                actual_fp = self.config.contract_fingerprints[symbol]
                if expected_fp and expected_fp != actual_fp:
                    await write_message(writer, {"type": "contract_mismatch", "symbol": symbol, "expected": expected_fp, "actual": actual_fp})
                    return
            last_sequences = {str(k).upper(): int(v) for k, v in dict(hello.get("last_sequences") or {}).items()}
            subscribed_last_sequences = {
                symbol: last_sequences[symbol]
                for symbol in subscribed_symbols
                if symbol in last_sequences
            }
            if identity in self.consumers:
                await write_message(
                    writer,
                    {
                        "type": "consumer_identity_conflict",
                        "identity": identity,
                        "reason": "consumer identity is already connected",
                    },
                )
                return
            replay_head = dict(self.sequences)
            consumer = ConsumerState(
                identity=identity,
                writer=writer,
                queue=asyncio.Queue(maxsize=self.config.consumer_queue_limit),
                connected_at=pd.Timestamp.now(tz="UTC").isoformat(),
                subscribed_symbols=subscribed_symbols,
                acknowledged=dict(subscribed_last_sequences),
                replaying=True,
            )
            self.consumers[identity] = consumer
            await write_message(
                writer,
                {
                    "type": "session_header",
                    "symbols": list(subscribed_symbols),
                    "available_symbols": list(self.config.symbols),
                    "contract_fingerprints": {symbol: self.config.contract_fingerprints[symbol] for symbol in subscribed_symbols},
                    "canonical_source": self.config.canonical_source,
                },
            )
            control_reader = asyncio.create_task(self._consumer_control_reader(reader, writer, consumer))
            await self._replay_to_consumer(consumer, last_sequences, replay_head=replay_head)
            consumer.replaying = False
            for message in consumer.pending_control_messages:
                await write_message(writer, message)
            consumer.pending_control_messages.clear()
            if consumer.stale:
                return
            pump = asyncio.create_task(self._consumer_pump(consumer))
            await control_reader
        except (asyncio.IncompleteReadError, ConnectionError, BrokenPipeError):
            pass
        finally:
            if control_reader is not None:
                if not control_reader.done():
                    control_reader.cancel()
                with contextlib.suppress(asyncio.CancelledError, asyncio.IncompleteReadError, ConnectionError, BrokenPipeError):
                    await control_reader
            if consumer is not None and self.consumers.get(consumer.identity) is consumer:
                self.consumers.pop(consumer.identity, None)
            if pump is not None:
                pump.cancel()
            try:
                writer.close()
                await asyncio.wait_for(writer.wait_closed(), timeout=5)
            except Exception:
                pass
            self._refresh_health()

    async def _consumer_control_reader(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        consumer: ConsumerState,
    ) -> None:
        while True:
            message = await read_message(reader)
            if message.get("type") == "ack":
                symbol = str(message.get("symbol") or "").upper()
                if consumer.subscribed_symbols and symbol not in consumer.subscribed_symbols:
                    continue
                consumer.acknowledged[symbol] = max(
                    int(message.get("sequence") or 0),
                    consumer.acknowledged.get(symbol, 0),
                )
            elif message.get("type") == "heartbeat":
                await write_message(writer, {"type": "heartbeat_ack", "server_time": pd.Timestamp.now(tz="UTC").isoformat()})
            self._refresh_health()

    def _consumer_subscribed_symbols(self, hello: dict[str, Any]) -> tuple[str, ...]:
        raw_symbols = hello.get("symbols")
        if raw_symbols is None:
            return tuple(self.config.symbols)
        if not isinstance(raw_symbols, (list, tuple)):
            raise ValueError("consumer symbols must be a list.")
        normalized = tuple(dict.fromkeys(str(symbol).strip().upper() for symbol in raw_symbols if str(symbol).strip()))
        if not normalized:
            raise ValueError("consumer symbols must not be empty.")
        unknown = [symbol for symbol in normalized if symbol not in self.config.symbols]
        if unknown:
            raise ValueError(f"consumer requested symbols outside shared stream: {unknown}")
        return normalized

    async def _consumer_pump(self, consumer: ConsumerState) -> None:
        while True:
            event = await consumer.queue.get()
            if event is None:
                break
            started = time.perf_counter()
            await write_message(consumer.writer, event)
            self.metrics.observe("ipc_socket_send_seconds", time.perf_counter() - started)

    async def _replay_to_consumer(
        self,
        consumer: ConsumerState,
        last_sequences: dict[str, int],
        *,
        replay_head: dict[str, int] | None = None,
    ) -> None:
        replay_started = time.perf_counter()
        oldest = self._oldest_sequence_by_symbol()
        latest = dict(replay_head if replay_head is not None else self.sequences)
        self.metrics.increment("consumer_replay_sessions")
        self.metrics.gauge("consumer_replay_queue_depth", consumer.queue.qsize())
        for symbol in consumer.subscribed_symbols or tuple(self.config.symbols):
            symbol_replay_started = time.perf_counter()
            last_sequence = int(last_sequences.get(symbol, 0))
            latest_sequence = latest.get(symbol, 0)
            if last_sequence > latest_sequence:
                gap = {
                    "type": "gap",
                    "symbol": symbol,
                    "requested_after_sequence": last_sequence,
                    "latest_available_sequence": latest_sequence,
                    "reason": "consumer_sequence_ahead_of_journal",
                }
                consumer.gap_count += 1
                self.health["gap_count"] += 1
                await write_message(consumer.writer, gap)
                continue
            oldest_sequence = oldest.get(symbol)
            if oldest_sequence is not None and last_sequence and last_sequence < oldest_sequence - 1:
                gap = {
                    "type": "gap",
                    "symbol": symbol,
                    "requested_after_sequence": last_sequence,
                    "oldest_available_sequence": oldest_sequence,
                    "reason": "replay_window_exhausted",
                }
                consumer.gap_count += 1
                self.health["gap_count"] += 1
                await write_message(consumer.writer, gap)
                continue
            replay = self._replay_after(
                symbol=symbol,
                last_sequence=last_sequence,
                through_sequence=latest_sequence,
            )
            self.metrics.observe("consumer_replay_records_per_symbol", len(replay))
            for event in replay:
                consumer.replay_count += 1
                await write_message(consumer.writer, event)
            self.metrics.observe("consumer_replay_symbol_seconds", time.perf_counter() - symbol_replay_started)
            if self.journal_corruption is not None:
                gap = {
                    "type": "gap",
                    "symbol": symbol,
                    "requested_after_sequence": last_sequence,
                    "latest_verified_sequence": latest_sequence,
                    "reason": "journal_corruption",
                    "journal_corruption": {
                        "line_number": self.journal_corruption.line_number,
                        "byte_offset": self.journal_corruption.byte_offset,
                        "reason": self.journal_corruption.reason,
                        "symbol": self.journal_corruption.symbol,
                        "sequence": self.journal_corruption.sequence,
                        "quarantine_path": self.journal_corruption.quarantine_path,
                    },
                }
                consumer.gap_count += 1
                self.health["gap_count"] += 1
                await write_message(consumer.writer, gap)
        await write_message(consumer.writer, {"type": "catchup_complete"})
        self.metrics.observe("consumer_replay_session_seconds", time.perf_counter() - replay_started)

    def _oldest_sequence_by_symbol(self) -> dict[str, int]:
        oldest: dict[str, int] = {}
        for symbol, events in self.events_by_symbol.items():
            for event in events:
                sequence = int(event.get("sequence") or 0)
                if sequence > 0:
                    oldest[symbol] = sequence
                    break
        return oldest

    def _replay_after(self, *, symbol: str, last_sequence: int, through_sequence: int | None = None) -> tuple[dict[str, Any], ...]:
        symbol_text = symbol.upper()
        upper = int(through_sequence) if through_sequence is not None else None
        return tuple(
            event
            for event in self.events_by_symbol.get(symbol_text, [])
            if int(event.get("sequence") or 0) > int(last_sequence)
            and (upper is None or int(event.get("sequence") or 0) <= upper)
        )

    async def publish_event(self, event: dict[str, Any]) -> None:
        if self.journal_corruption is not None:
            raise RuntimeError("canonical journal corruption requires operator recovery before ingestion can continue")
        if not self.config.canonical_journal_enabled:
            raise RuntimeError("canonical journal must remain enabled for shared production market data")
        journal_path = Path(self.config.journal_path)
        if journal_path.exists() and journal_path.stat().st_size >= self.config.journal_max_bytes:
            archive_path = self.journal.rotate(latest_sequences=dict(self.sequences))
            self._audit_event(
                "journal_rotated",
                {
                    "journal_path": str(journal_path),
                    "archive_path": str(archive_path) if archive_path else None,
                    "journal_size_bytes": journal_path.stat().st_size if journal_path.exists() else 0,
                    "latest_sequences": dict(self.sequences),
                },
            )
            self.metrics.increment("canonical_journal_rotations")
        symbol = str(event["symbol"]).upper()
        journal_started = time.perf_counter()
        self.journal.append(event)
        self.metrics.observe("canonical_journal_durable_write_seconds", time.perf_counter() - journal_started)
        self.metrics.increment("canonical_journal_writes")
        self.events_by_symbol.setdefault(symbol, []).append(event)
        self.events_by_symbol[symbol] = self.events_by_symbol[symbol][-self.config.replay_limit_records :]
        for consumer in tuple(self.consumers.values()):
            if consumer.subscribed_symbols and symbol not in consumer.subscribed_symbols:
                continue
            try:
                enqueue_started = time.perf_counter()
                consumer.queue.put_nowait(event)
                self.metrics.observe("ipc_publication_enqueue_seconds", time.perf_counter() - enqueue_started)
                self.metrics.gauge(f"consumer_{consumer.identity}_queue_depth", consumer.queue.qsize())
            except asyncio.QueueFull:
                consumer.stale = True
                consumer.dropped_message_count += 1
                self.health["dropped_message_count"] += 1
                gap = {"type": "gap", "symbol": symbol, "sequence": event["sequence"], "reason": "consumer_queue_overflow"}
                if consumer.replaying:
                    consumer.pending_control_messages.append(gap)
                else:
                    await write_message(consumer.writer, gap)
                    consumer.writer.close()
        self._refresh_health()

    async def publish_symbol_health(self, symbol: str, *, force: bool = False) -> None:
        symbol = symbol.upper()
        now_monotonic = time.monotonic()
        if not force and now_monotonic - self._last_symbol_health_publish_monotonic.get(symbol, 0.0) < 1.0:
            return
        self._last_symbol_health_publish_monotonic[symbol] = now_monotonic
        payload = {
            "type": "symbol_health",
            "symbol": symbol,
            "journal_healthy": bool(self.health.get("journal_healthy", False)),
            "gap_count": int(self.health.get("gap_count", 0)),
            "last_raw_trade": self.health.get("last_raw_trade_by_symbol", {}).get(symbol),
            "last_eligible_trade": self.health.get("last_eligible_trade_by_symbol", {}).get(symbol),
            "pending_canonical_bucket": self.health.get("pending_canonical_bucket_by_symbol", {}).get(symbol),
            "canonical_flush_expected": bool(
                self.health.get("canonical_flush_expected_by_symbol", {}).get(symbol, False)
            ),
            "canonical_flush_expected_at": self.health.get(
                "canonical_flush_expected_at_by_symbol", {}
            ).get(symbol),
            "symbol_delivery_failure": self.health.get("symbol_delivery_failure_by_symbol", {}).get(symbol),
            "last_canonical_observation": self.health.get("last_canonical_observation_by_symbol", {}).get(symbol),
        }
        for consumer in tuple(self.consumers.values()):
            if consumer.subscribed_symbols and symbol not in consumer.subscribed_symbols:
                continue
            try:
                consumer.queue.put_nowait(payload)
            except asyncio.QueueFull:
                consumer.stale = True
                consumer.dropped_message_count += 1
                self.health["dropped_message_count"] += 1
                gap = {"type": "gap", "symbol": symbol, "reason": "consumer_queue_overflow"}
                if consumer.replaying:
                    consumer.pending_control_messages.append(gap)
                else:
                    await write_message(consumer.writer, gap)
                    consumer.writer.close()

    def _mark_canonical_sampler_admission(self, symbol: str, source_timestamp: pd.Timestamp) -> None:
        symbol = symbol.upper()
        now_wall = pd.Timestamp.now(tz="UTC")
        now_monotonic = time.monotonic()
        source_utc = pd.Timestamp(source_timestamp).tz_convert("UTC")
        current = self._canonical_source_high_water_by_symbol.get(symbol)
        if current is None or source_utc > current:
            self._canonical_source_high_water_by_symbol[symbol] = source_utc
            self.health["last_canonical_source_timestamp_by_symbol"][symbol] = source_utc.isoformat()
        self._canonical_heartbeat_monotonic_by_symbol[symbol] = now_monotonic
        self.health["last_canonical_heartbeat_by_symbol"][symbol] = now_wall.isoformat()
        self.health["last_canonical_sampler_admission_by_symbol"][symbol] = now_wall.isoformat()

    def _mark_canonical_publication(self, symbol: str) -> None:
        symbol = symbol.upper()
        now_wall = pd.Timestamp.now(tz="UTC")
        now_monotonic = time.monotonic()
        self._canonical_heartbeat_monotonic_by_symbol[symbol] = now_monotonic
        self._canonical_publication_monotonic_by_symbol[symbol] = now_monotonic
        self.health["last_canonical_heartbeat_by_symbol"][symbol] = now_wall.isoformat()
        self.health["last_canonical_publication_by_symbol"][symbol] = now_wall.isoformat()

    async def ingest_raw_trade(self, event: LiveMarketEvent, adapter: CanonicalMarketObservationAdapter) -> tuple[dict[str, Any], ...]:
        ingest_started = time.perf_counter()
        symbol = event.tick.symbol.upper()
        receive_timestamp_utc = pd.Timestamp.now(tz="UTC")
        receive_timestamp = receive_timestamp_utc.isoformat()
        timing = dict(event.local_timing or {})
        callback_entry = timing.get("websocket_callback_entry")
        parse_complete = timing.get("websocket_parse_complete")
        timestamp_parse_complete = timing.get("timestamp_parse_complete")
        filter_complete = timing.get("trade_filter_complete")
        if callback_entry is not None:
            self.metrics.observe("stage_exchange_event_to_websocket_callback_seconds", max(0.0, (receive_timestamp_utc - pd.Timestamp(event.tick.timestamp)).total_seconds()))
        if callback_entry is not None and parse_complete is not None:
            self.metrics.observe("stage_websocket_callback_to_parse_seconds", parse_complete - callback_entry)
        if parse_complete is not None and timestamp_parse_complete is not None:
            self.metrics.observe("stage_parse_to_timestamp_seconds", timestamp_parse_complete - parse_complete)
        if timestamp_parse_complete is not None and filter_complete is not None:
            self.metrics.observe("stage_timestamp_to_trade_filter_seconds", filter_complete - timestamp_parse_complete)
        if parse_complete is not None and filter_complete is not None:
            self.metrics.observe("stage_parse_to_trade_filter_seconds", filter_complete - parse_complete)
        if filter_complete is not None:
            self.metrics.observe("stage_filter_to_sampler_acceptance_seconds", ingest_started - filter_complete)
        self.health["last_raw_trade_by_symbol"][symbol] = event.tick.timestamp.isoformat()
        raw_counts = self.health.setdefault("raw_trade_count_by_symbol", {})
        raw_counts[symbol] = int(raw_counts.get(symbol, 0)) + 1
        raw_archive_payload = {
            "type": "raw_trade",
            "receive_timestamp": receive_timestamp,
            "ingest_timestamp": receive_timestamp,
            "source_record_id": event.source_record_id,
            "eligible": event.eligible,
            **dataclass_to_jsonable(event.tick),
        }
        await self._archive_jsonl(
            session_archive_path(self.config.archive_root, kind="raw", timestamp=event.tick.timestamp, symbol=symbol),
            raw_archive_payload,
            kind="raw",
        )
        if not event.eligible:
            excluded_counts = self.health.setdefault("excluded_raw_trade_count_by_symbol", {})
            excluded_counts[symbol] = int(excluded_counts.get(symbol, 0)) + 1
            self.metrics.increment("raw_trades_rejected")
            self.metrics.observe("ingest_raw_trade_seconds", time.perf_counter() - ingest_started)
            self._refresh_health()
            await self.publish_symbol_health(symbol)
            return tuple()
        eligible_counts = self.health.setdefault("eligible_raw_trade_count_by_symbol", {})
        eligible_counts[symbol] = int(eligible_counts.get(symbol, 0)) + 1
        self.health["last_eligible_trade_by_symbol"][symbol] = event.tick.timestamp.isoformat()
        sampler_started = time.perf_counter()
        result = adapter.process_tick(event.tick, trader_id="shared_market_data_service", source_event=event)
        sampler_done = time.perf_counter()
        self.metrics.observe("canonical_sampler_process_seconds", sampler_done - sampler_started)
        self.metrics.observe("stage_sampler_acceptance_to_canonical_finalize_seconds", sampler_done - sampler_started)
        self.metrics.increment("raw_trades_accepted")
        if result.eligibility is not None and result.eligibility.is_eligible:
            self._mark_canonical_sampler_admission(symbol, event.tick.timestamp)
            self._refresh_health()
        if result.observations:
            self.health["canonical_flush_expected_by_symbol"][symbol] = True
            self.health.setdefault("canonical_flush_expected_at_by_symbol", {})[symbol] = pd.Timestamp.now(tz="UTC").isoformat()
        self._refresh_sampler_activity(symbol, adapter)
        if result.observations:
            await self.publish_symbol_health(symbol, force=True)
        emitted: list[dict[str, Any]] = []
        for observation in result.observations:
            finalize_started = time.perf_counter()
            sequence = self.sequences[symbol] + 1
            self.sequences[symbol] = sequence
            close_price = str(observation.close_price)
            observation_timestamp = pd.Timestamp(observation.second_bucket).isoformat()
            contract_fingerprint = self.config.contract_fingerprints[symbol]
            payload = {
                "version": PROTOCOL_VERSION,
                "type": "observation",
                "symbol": symbol,
                "sequence": sequence,
                "contract_fingerprint": contract_fingerprint,
                "source_timestamp": pd.Timestamp(observation.source_sip_timestamp).isoformat(),
                "receive_timestamp": receive_timestamp,
                "ingest_timestamp": receive_timestamp,
                "observation_timestamp": observation_timestamp,
                "close_price": close_price,
                "source_record_id": observation.source_record_id,
                "source_trade_id": observation.source_alpaca_trade_id,
                "source_exchange": observation.source_exchange,
                "source_tape": observation.source_tape,
            }
            payload["event_id"] = canonical_event_id(
                contract_fingerprint=contract_fingerprint,
                symbol=symbol,
                sequence=sequence,
                observation_timestamp=observation_timestamp,
                close_price=close_price,
                source_record_id=observation.source_record_id,
            )
            self.metrics.observe("canonical_close_finalize_seconds", time.perf_counter() - finalize_started)
            await self._archive_jsonl(
                session_archive_path(self.config.archive_root, kind="canonical", timestamp=observation.second_bucket, symbol=symbol),
                payload,
                kind="canonical",
            )
            self.health["last_canonical_observation_by_symbol"][symbol] = observation_timestamp
            self._mark_canonical_publication(symbol)
            await self.publish_event(payload)
            emitted.append(payload)
            self.metrics.increment("canonical_closes_finalized")
        self.health["canonical_flush_expected_by_symbol"][symbol] = False
        self.health.setdefault("canonical_flush_expected_at_by_symbol", {}).pop(symbol, None)
        self._refresh_sampler_activity(symbol, adapter)
        await self.publish_symbol_health(symbol, force=True)
        self.metrics.observe("ingest_raw_trade_seconds", time.perf_counter() - ingest_started)
        return tuple(emitted)

    def _refresh_sampler_activity(self, symbol: str, adapter: CanonicalMarketObservationAdapter) -> None:
        symbol = symbol.upper()
        state = adapter.sampler.state(("shared_market_data_service", symbol))
        pending_bucket = state.pending_bucket.isoformat() if state is not None and state.pending_bucket is not None else None
        if pending_bucket is None:
            self.health["pending_canonical_bucket_by_symbol"].pop(symbol, None)
        else:
            self.health["pending_canonical_bucket_by_symbol"][symbol] = pending_bucket

    def _disk_usage_snapshot(self) -> dict[str, int]:
        usage = os.statvfs(str(Path(self.config.archive_root).parent if not Path(self.config.archive_root).exists() else self.config.archive_root))
        return {
            "free_bytes": usage.f_bavail * usage.f_frsize,
            "free_inodes": usage.f_favail,
        }

    def _record_disk_snapshot(self, snapshot: dict[str, int]) -> None:
        free_bytes = int(snapshot["free_bytes"])
        free_inodes = int(snapshot["free_inodes"])
        self.health["disk_free_bytes"] = free_bytes
        self.health["disk_free_inodes"] = free_inodes
        if free_bytes < self.config.disk_min_free_bytes or free_inodes < self.config.disk_min_free_inodes:
            raise RuntimeError("disk safety threshold breached; shutting down ingestion")

    def _check_disk(self) -> None:
        self._record_disk_snapshot(self._disk_usage_snapshot())

    async def monitor_disk_safety(self, *, interval_seconds: float = 1.0) -> None:
        while True:
            wall_started = time.perf_counter()
            cpu_started = time.process_time()
            snapshot = await asyncio.to_thread(self._disk_usage_snapshot)
            self.metrics.observe("disk_safety_check_wall_seconds", time.perf_counter() - wall_started)
            self.metrics.observe("disk_safety_check_cpu_seconds", time.process_time() - cpu_started)
            self._record_disk_snapshot(snapshot)
            await asyncio.sleep(interval_seconds)

    async def monitor_retention_purge(self, *, interval_seconds: float = 60.0) -> None:
        while True:
            wall_started = time.perf_counter()
            cpu_started = time.process_time()
            await asyncio.to_thread(
                purge_old_session_dirs,
                self.config.archive_root,
                keep_sessions=self.config.retention_sessions,
            )
            self.metrics.observe("retention_purge_wall_seconds", time.perf_counter() - wall_started)
            self.metrics.observe("retention_purge_cpu_seconds", time.process_time() - cpu_started)
            await asyncio.sleep(interval_seconds)

    def _refresh_health(self, *, force: bool = False) -> None:
        now = pd.Timestamp.now(tz="UTC")
        now_monotonic = time.monotonic()
        ages = {}
        for symbol, timestamp in dict(self.health.get("last_canonical_observation_by_symbol") or {}).items():
            ages[symbol] = (now - pd.Timestamp(timestamp)).total_seconds()
        self.health["observation_age_seconds"] = ages
        heartbeat_ages = {}
        for symbol, timestamp in self._canonical_heartbeat_monotonic_by_symbol.items():
            heartbeat_ages[symbol] = max(0.0, now_monotonic - timestamp)
        self.health["canonical_heartbeat_age_seconds"] = heartbeat_ages
        publication_ages = {}
        for symbol, timestamp in self._canonical_publication_monotonic_by_symbol.items():
            publication_ages[symbol] = max(0.0, now_monotonic - timestamp)
        self.health["canonical_publication_liveness_age_seconds"] = publication_ages
        source_ages = {}
        for symbol, timestamp in self._canonical_source_high_water_by_symbol.items():
            source_ages[symbol] = max(0.0, (now - timestamp).total_seconds())
        self.health["canonical_source_freshness_age_seconds"] = source_ages
        # Keep freshness/safety state current on every event; materialize the
        # report-only consumer and filesystem aggregates only when writing it.
        now_monotonic = time.perf_counter()
        should_write = force or now_monotonic - self._last_health_write_monotonic >= self._health_write_interval_seconds
        if not should_write and Path(self.config.health_path).exists():
            self.metrics.increment("health_snapshot_write_skipped")
            return
        self.health["consumers"] = {
            identity: {
                "connected": True,
                "subscribed_symbols": list(state.subscribed_symbols),
                "acknowledged": state.acknowledged,
                "queue_size": state.queue.qsize(),
                "replay_count": state.replay_count,
                "gap_count": state.gap_count,
                "dropped_message_count": state.dropped_message_count,
                "stale": state.stale,
            }
            for identity, state in self.consumers.items()
        }
        self.health["journal_size_bytes"] = Path(self.config.journal_path).stat().st_size if Path(self.config.journal_path).exists() else 0
        started = time.perf_counter()
        self.health["pipeline_metrics"] = self.metrics.snapshot()
        Path(self.config.health_path).parent.mkdir(parents=True, exist_ok=True)
        temp = Path(self.config.health_path).with_suffix(".tmp")
        temp.write_text(json.dumps(self.health, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
        temp.replace(self.config.health_path)
        self._last_health_write_monotonic = time.perf_counter()
        self.metrics.observe("health_snapshot_write_seconds", self._last_health_write_monotonic - started)


class AlpacaSipUpstream:
    def __init__(self, config: SharedMarketDataConfig, status_callback: Callable[[dict[str, Any]], None] | None = None) -> None:
        self.config = config
        self.status_callback = status_callback

    def _status(self, payload: dict[str, Any]) -> None:
        if self.status_callback is not None:
            self.status_callback(payload)

    async def _recv_messages(self, websocket: Any) -> list[dict[str, Any]]:
        raw = await websocket.recv()
        messages = json.loads(raw)
        return [message for message in (messages if isinstance(messages, list) else [messages]) if isinstance(message, dict)]

    async def _wait_for_control_ack(self, websocket: Any, *, phase: str, expected_message: str | None = None) -> list[dict[str, Any]]:
        while True:
            messages = await self._recv_messages(websocket)
            for message in messages:
                if message.get("T") == "error":
                    raise RuntimeError(f"Alpaca SIP stream error during {phase}: {message}")
                if message.get("T") == "success" and (expected_message is None or message.get("msg") == expected_message):
                    return messages
                if phase == "subscription" and message.get("T") == "subscription":
                    return messages

    async def stream(self) -> Any:
        credentials = load_market_data_credentials(
            self.config.credential_profile,
            secret_path=self.config.credential_secret_path,
        )
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        sequence = 0
        async with websockets.connect(DATA_STREAM_URL_TEMPLATE.format(feed=self.config.feed), ssl=ssl_context) as websocket:
            await self._wait_for_control_ack(websocket, phase="connect", expected_message="connected")
            self._status({"sip_connected": True})
            await websocket.send(json.dumps({"action": "auth", "key": credentials["key_id"], "secret": credentials["secret_key"]}))
            await self._wait_for_control_ack(websocket, phase="auth", expected_message="authenticated")
            self._status({"authenticated": True})
            await websocket.send(json.dumps({"action": "subscribe", "trades": list(self.config.symbols)}))
            subscription = await self._wait_for_control_ack(websocket, phase="subscription")
            acknowledged_symbols = {
                str(symbol).upper()
                for message in subscription
                if message.get("T") == "subscription"
                for symbol in list(message.get("trades") or [])
            }
            missing_symbols = sorted(set(self.config.symbols) - acknowledged_symbols)
            self._status(
                {
                    "subscription_acknowledged": True,
                    "subscription": subscription,
                    "symbol_delivery_failure_by_symbol": {
                        symbol: "subscription_not_acknowledged" for symbol in missing_symbols
                    },
                }
            )
            async for raw in websocket:
                frame_started = time.perf_counter()
                messages = json.loads(raw)
                parsed_at = time.perf_counter()
                self._status(
                    {
                        "_pipeline_metric": {
                            "observe": {"websocket_message_parse_seconds": parsed_at - frame_started},
                            "increment": {"websocket_frames": 1},
                            "gauge": {"websocket_frame_bytes": len(raw) if hasattr(raw, "__len__") else 0},
                        }
                    }
                )
                for message in messages if isinstance(messages, list) else [messages]:
                    if not isinstance(message, dict):
                        continue
                    if message.get("T") == "t":
                        sequence += 1
                        filter_started = time.perf_counter()
                        timestamp_cpu_started = time.process_time()
                        timestamp_started = time.perf_counter()
                        timestamp, timestamp_info = utc_timestamp_with_info(message["t"])
                        timestamp_done = time.perf_counter()
                        timestamp_cpu_seconds = time.process_time() - timestamp_cpu_started
                        tick = MarketTick(
                            timestamp=timestamp,
                            symbol=str(message["S"]).upper(),
                            price=float(message["p"]),
                            size=float(message["s"]),
                            exchange=str(message.get("x", "")),
                            trade_id=str(message.get("i", "")),
                            tape=str(message.get("z", "")),
                            conditions=tuple(str(condition) for condition in message.get("c", []) if condition is not None),
                        )
                        event = LiveMarketEvent(
                            tick=tick,
                            source_record_id=_stable_source_record_id(tick, None),
                            broker_trade_id=tick.trade_id or None,
                            source_sequence=sequence,
                            eligible=evaluate_market_tick_eligibility(tick),
                        )
                        filter_done = time.perf_counter()
                        event = LiveMarketEvent(
                            tick=event.tick,
                            source_record_id=event.source_record_id,
                            broker_trade_id=event.broker_trade_id,
                            source_sequence=event.source_sequence,
                            eligible=event.eligible,
                            archive_path=event.archive_path,
                            local_timing={
                                "websocket_callback_entry": frame_started,
                                "websocket_parse_complete": parsed_at,
                                "timestamp_parse_complete": timestamp_done,
                                "trade_filter_complete": filter_done,
                            },
                        )
                        timestamp_metric_increment = {
                            "sip_trade_messages": 1,
                            f"timestamp_input_type_{timestamp_info.input_type}": 1,
                            f"timestamp_path_{timestamp_info.path}": 1,
                        }
                        if timestamp_info.used_pandas_fallback:
                            timestamp_metric_increment["timestamp_pandas_fallback_calls"] = 1
                        self._status(
                            {
                                "_pipeline_metric": {
                                    "observe": {
                                        "timestamp_conversion_wall_seconds": timestamp_done - timestamp_started,
                                        "timestamp_conversion_cpu_seconds": timestamp_cpu_seconds,
                                        "trade_filter_and_event_build_seconds": filter_done - filter_started,
                                    },
                                    "increment": timestamp_metric_increment,
                                }
                            }
                        )
                        yield event
                    elif message.get("T") == "subscription":
                        continue
                    elif message.get("T") == "error":
                        raise RuntimeError(f"Alpaca SIP stream error: {message}")


def _parse_secret_file(path: str | Path) -> dict[str, str]:
    payload: dict[str, str] = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        payload[key.strip()] = value.strip().strip("'\"")
    return payload


def load_market_data_credentials(profile: str, *, secret_path: str | Path | None = None) -> dict[str, str]:
    if secret_path:
        secrets = _parse_secret_file(secret_path)
        key_id = (
            secrets.get("ALPACA_MARKET_DATA_API_KEY_ID")
            or secrets.get("APCA_API_KEY_ID")
            or secrets.get("ALPACA_API_KEY")
        )
        secret_key = (
            secrets.get("ALPACA_MARKET_DATA_API_SECRET_KEY")
            or secrets.get("APCA_API_SECRET_KEY")
            or secrets.get("ALPACA_SECRET_KEY")
        )
        if not key_id or not secret_key:
            raise RuntimeError(f"Missing market-data credentials in protected secret file for profile {profile!r}.")
        return {"key_id": key_id, "secret_key": secret_key}

    normalized = "".join(ch if ch.isalnum() else "_" for ch in str(profile).upper()).strip("_")
    key_names = (
        f"ALPACA_PROFILE_{normalized}_API_KEY_ID",
        f"ALPACA_PROFILE_{normalized}_API_KEY",
        f"APCA_PROFILE_{normalized}_API_KEY_ID",
    )
    secret_names = (
        f"ALPACA_PROFILE_{normalized}_API_SECRET_KEY",
        f"ALPACA_PROFILE_{normalized}_SECRET_KEY",
        f"APCA_PROFILE_{normalized}_API_SECRET_KEY",
    )
    key_id = next((os.getenv(name) for name in key_names if os.getenv(name)), None)
    secret_key = next((os.getenv(name) for name in secret_names if os.getenv(name)), None)
    if not key_id or not secret_key:
        raise RuntimeError(f"Missing market-data credentials for profile {profile!r}.")
    return {"key_id": key_id, "secret_key": secret_key}


async def run_service(config: SharedMarketDataConfig, *, upstream_factory: Callable[[SharedMarketDataConfig], Any] = AlpacaSipUpstream) -> None:
    lock = SingletonLock(config.lock_path or f"{config.socket_path}.lock")
    lock.acquire()
    server = SharedCanonicalMarketDataServer(config)
    server.start_archive_writer()
    for event in lock.events:
        server._audit_event(str(event.get("type") or "singleton_event"), event)
    socket_server = await server.start_socket_server()
    adapters = {symbol: CanonicalMarketObservationAdapter(run_id=f"shared_market_data:{symbol}") for symbol in config.symbols}
    try:
        async with socket_server:
            def record_status(payload: dict[str, Any]) -> None:
                metric_payload = payload.pop("_pipeline_metric", None)
                if isinstance(metric_payload, dict):
                    for name, value in dict(metric_payload.get("observe") or {}).items():
                        server.metrics.observe(str(name), float(value))
                    for name, value in dict(metric_payload.get("increment") or {}).items():
                        server.metrics.increment(str(name), int(value))
                    for name, value in dict(metric_payload.get("gauge") or {}).items():
                        server.metrics.gauge(str(name), float(value))
                server.health.update(payload)
                if payload:
                    server._refresh_health(force=True)

            try:
                upstream = upstream_factory(config, status_callback=record_status)
            except TypeError:
                upstream = upstream_factory(config)
            async def ingest() -> None:
                processed_since_yield = 0
                while True:
                    try:
                        server.health["upstream_reconnect_count"] += 1
                        async for event in upstream.stream():
                            server.health["sip_connected"] = True
                            server.health["subscription_acknowledged"] = True
                            if event.tick.symbol in adapters:
                                await server.ingest_raw_trade(event, adapters[event.tick.symbol])
                            processed_since_yield += 1
                            if processed_since_yield >= 50:
                                processed_since_yield = 0
                                server.metrics.increment("ingest_cooperative_yields")
                                await asyncio.sleep(0)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        server.health["sip_connected"] = False
                        server.health["last_upstream_error"] = str(exc)
                        server._refresh_health(force=True)
                        await asyncio.sleep(5)

            loop = asyncio.get_running_loop()
            stop_event = asyncio.Event()
            signal_handlers: list[signal.Signals] = []
            for sig in (signal.SIGINT, signal.SIGTERM):
                with contextlib.suppress(NotImplementedError):
                    loop.add_signal_handler(sig, stop_event.set)
                    signal_handlers.append(sig)
            socket_task = asyncio.create_task(socket_server.serve_forever())
            ingest_task = asyncio.create_task(ingest())
            lag_task = asyncio.create_task(server.monitor_event_loop_lag())
            disk_task = asyncio.create_task(server.monitor_disk_safety())
            retention_task = asyncio.create_task(server.monitor_retention_purge())
            stop_task = asyncio.create_task(stop_event.wait())
            try:
                done, pending = await asyncio.wait(
                    {socket_task, ingest_task, lag_task, disk_task, retention_task, stop_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if stop_task in done:
                    for task in (socket_task, ingest_task, lag_task, disk_task, retention_task):
                        task.cancel()
                    with contextlib.suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(
                            asyncio.gather(socket_task, ingest_task, lag_task, disk_task, retention_task, return_exceptions=True),
                            timeout=5,
                        )
                    return
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                for task in done:
                    task.result()
            finally:
                for sig in signal_handlers:
                    with contextlib.suppress(NotImplementedError, RuntimeError):
                        loop.remove_signal_handler(sig)
    finally:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(server.stop_archive_writer(), timeout=10)
        socket_server.close()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(socket_server.wait_closed(), timeout=5)
        server.cleanup_socket_path()
        server._refresh_health(force=True)
        lock.release()


def main() -> None:
    parser = argparse.ArgumentParser(description="Shared canonical SIP market-data service.")
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    load_dotenv()
    asyncio.run(run_service(SharedMarketDataConfig.load(args.config)))


if __name__ == "__main__":
    main()
