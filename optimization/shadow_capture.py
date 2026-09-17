from __future__ import annotations

import asyncio
import json
import ssl
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any, AsyncIterable, Callable

import certifi
import websockets

from replay.historical_source import file_sha256
from silver.market import CanonicalMarketObservationAdapter, LiveMarketEvent, evaluate_market_tick_eligibility, market_stream_contract
from silver.market.one_second_sampler import SAMPLER_VERSION
from silver.market.trade_filter import TRADE_FILTER_VERSION
from trading_events import MarketTick

from .historical_sip import _first_env, _load_env_file, disk_capacity_preflight, reject_production_tick_root
from .market_calendar import nyse_session_window


ALPACA_DATA_STREAM_URL_TEMPLATE = "wss://stream.data.alpaca.markets/v2/{feed}"
SHADOW_CAPTURE_SCHEMA_VERSION = "research.alpaca_sip_shadow_capture.v1"
SHADOW_CAPTURE_PRODUCER_VERSION = "optimization.shadow_capture.v1"
RESEARCH_STREAM_ID_PREFIX = "shadow_capture"


@dataclass(frozen=True)
class ShadowCaptureRequest:
    symbols: tuple[str, ...]
    session_date: date
    output_root: str = "artifacts/research/shadow-capture"
    run_id: str | None = None
    feed: str = "sip"
    env_file: str | None = ".env"
    credential_profile: str | None = None
    stream_url: str | None = None
    min_free_gb: float = 20.0
    max_disk_used_pct: float = 90.0
    duration_seconds: float | None = None
    stop_after_session_end_seconds: float = 60.0
    heartbeat_seconds: float = 30.0
    connect_timeout_seconds: float = 30.0
    ping_interval_seconds: float = 20.0
    ping_timeout_seconds: float = 20.0
    started_at_override: datetime | None = None


@dataclass(frozen=True)
class ShadowCaptureResult:
    run_id: str
    output_directory: str
    manifest_path: str
    status: str
    completeness: str
    started_at: str
    completed_at: str | None
    raw_counts: dict[str, int]
    canonical_counts: dict[str, int]
    diagnostics_summary: dict[str, int]


class JsonlWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.count = 0
        self.digest = sha256()
        path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = path.open("a", encoding="utf-8")

    def write(self, payload: dict[str, Any]) -> None:
        line = json.dumps(_jsonable(payload), sort_keys=True, separators=(",", ":")) + "\n"
        self._handle.write(line)
        self._handle.flush()
        self.digest.update(line.encode("utf-8"))
        self.count += 1

    def close(self) -> None:
        self._handle.close()


def run_shadow_capture(
    request: ShadowCaptureRequest,
    *,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> ShadowCaptureResult:
    return asyncio.run(capture_shadow_session(request, now=now))


async def capture_shadow_session(
    request: ShadowCaptureRequest,
    *,
    message_source: AsyncIterable[dict[str, Any]] | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> ShadowCaptureResult:
    request = _normalize_request(request)
    _validate_research_only_request(request)
    output_dir = capture_output_directory(request)
    output_dir.mkdir(parents=True, exist_ok=True)
    disk_preflight = disk_capacity_preflight(output_dir, min_free_gb=request.min_free_gb, max_used_pct=request.max_disk_used_pct)
    start_time = _utc(request.started_at_override or now())
    writers = CaptureWriters.open(output_dir, request.symbols)
    state = CaptureState(request=request, output_dir=output_dir, started_at=start_time, writers=writers, now=now)
    write_manifest(state, status="RUNNING", disk_preflight=disk_preflight)
    try:
        source = message_source if message_source is not None else alpaca_sip_trade_stream(request)
        async for message in source:
            current_time = now()
            if should_stop_capture(request, current_time=current_time, started_at=start_time):
                break
            state.process_message(message, received_at=current_time)
    except Exception as exc:
        state.completed_at = now()
        state.error = str(exc)
        write_manifest(state, status="FAILED", disk_preflight=disk_preflight)
        raise
    finally:
        writers.close()
    state.completed_at = now()
    write_manifest(state, status="COMPLETE", disk_preflight=disk_preflight)
    return shadow_capture_result(state, status="COMPLETE")


async def alpaca_sip_trade_stream(request: ShadowCaptureRequest) -> AsyncIterable[dict[str, Any]]:
    _load_env_file(request.env_file)
    credentials = alpaca_stream_credentials(profile=request.credential_profile)
    stream_url = request.stream_url or ALPACA_DATA_STREAM_URL_TEMPLATE.format(feed=request.feed)
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    async with websockets.connect(
        stream_url,
        ssl=ssl_context,
        ping_interval=request.ping_interval_seconds,
        ping_timeout=request.ping_timeout_seconds,
        max_queue=None,
        open_timeout=request.connect_timeout_seconds,
    ) as websocket:
        await websocket.send(json.dumps({"action": "auth", "key": credentials["key"], "secret": credentials["secret"]}))
        await websocket.send(json.dumps({"action": "subscribe", "trades": list(request.symbols)}))
        connected_at = datetime.now(timezone.utc)
        while True:
            if should_stop_capture(request, current_time=datetime.now(timezone.utc), started_at=connected_at):
                break
            try:
                raw_message = await asyncio.wait_for(websocket.recv(), timeout=max(1.0, float(request.heartbeat_seconds)))
            except asyncio.TimeoutError:
                continue
            messages = decode_ws_payload(raw_message)
            if isinstance(messages, dict):
                messages = [messages]
            for message in messages:
                if not isinstance(message, dict):
                    continue
                message_type = str(message.get("T") or "")
                if message_type in {"success", "subscription"}:
                    continue
                if message_type == "error":
                    raise RuntimeError(f"Alpaca market stream error: {_redacted_stream_error(message)}")
                if message_type == "t":
                    yield message


def load_shadow_capture_manifest(capture_dir: str | Path) -> dict[str, Any]:
    manifest_path = Path(capture_dir) / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Missing shadow capture manifest: {manifest_path}")
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def capture_output_directory(request: ShadowCaptureRequest) -> Path:
    run_id = request.run_id or default_run_id(request.session_date)
    return Path(request.output_root) / request.session_date.isoformat() / run_id


def default_run_id(session_date: date, *, now: Callable[[], datetime] = lambda: datetime.now(timezone.utc)) -> str:
    stamp = now().strftime("%Y%m%dT%H%M%SZ")
    return f"shadow_{session_date.isoformat()}_{stamp}"


def should_stop_capture(request: ShadowCaptureRequest, *, current_time: datetime, started_at: datetime) -> bool:
    current_time = _utc(current_time)
    if request.duration_seconds is not None and (current_time - started_at).total_seconds() >= request.duration_seconds:
        return True
    window = nyse_session_window(request.session_date)
    stop_at = window.end_utc.timestamp() + max(0.0, float(request.stop_after_session_end_seconds))
    return current_time.timestamp() >= stop_at


def alpaca_stream_credentials(*, profile: str | None = None) -> dict[str, str]:
    profile_key = f"ALPACA_PROFILE_{profile.strip().upper()}_" if profile else ""
    if profile_key:
        key = _first_env(profile_key + "API_KEY_ID", profile_key + "API_KEY", f"APCA_PROFILE_{profile.strip().upper()}_API_KEY_ID")
        secret = _first_env(profile_key + "API_SECRET_KEY", profile_key + "SECRET_KEY", f"APCA_PROFILE_{profile.strip().upper()}_API_SECRET_KEY")
    else:
        key = _first_env("APCA_API_KEY_ID", "ALPACA_API_KEY_ID", "ALPACA_API_KEY", "APCA_PAPER_API_KEY_ID", "ALPACA_PAPER_API_KEY_ID")
        secret = _first_env("APCA_API_SECRET_KEY", "ALPACA_API_SECRET_KEY", "ALPACA_SECRET_KEY", "ALPACA_PAPER_API_SECRET_KEY", "ALPACA_PAPER_SECRET_KEY")
    missing = []
    if not key:
        missing.append("Alpaca API key id")
    if not secret:
        missing.append("Alpaca API secret")
    if missing:
        raise RuntimeError("Shadow SIP capture unavailable: missing " + " and ".join(missing) + ".")
    return {"key": key, "secret": secret}


@dataclass
class CaptureWriters:
    raw: dict[str, JsonlWriter]
    canonical: dict[str, JsonlWriter]
    diagnostics: dict[str, JsonlWriter]

    @classmethod
    def open(cls, output_dir: Path, symbols: tuple[str, ...]) -> "CaptureWriters":
        return cls(
            raw={symbol: JsonlWriter(output_dir / "raw" / symbol / "sip_trades.jsonl") for symbol in symbols},
            canonical={symbol: JsonlWriter(output_dir / "canonical" / symbol / "observations_1s_close.jsonl") for symbol in symbols},
            diagnostics={symbol: JsonlWriter(output_dir / "diagnostics" / symbol / "sampler_diagnostics.jsonl") for symbol in symbols},
        )

    def close(self) -> None:
        for writer in [*self.raw.values(), *self.canonical.values(), *self.diagnostics.values()]:
            writer.close()


@dataclass
class CaptureState:
    request: ShadowCaptureRequest
    output_dir: Path
    started_at: datetime
    writers: CaptureWriters
    now: Callable[[], datetime]
    completed_at: datetime | None = None
    error: str | None = None
    source_sequence: int = 0
    seen_source_records: set[str] | None = None
    adapters: dict[str, CanonicalMarketObservationAdapter] | None = None
    raw_counts: dict[str, int] | None = None
    canonical_counts: dict[str, int] | None = None
    diagnostics_summary: dict[str, int] | None = None
    first_event_time: dict[str, str | None] | None = None
    last_event_time: dict[str, str | None] | None = None
    first_received_at: dict[str, str | None] | None = None
    last_received_at: dict[str, str | None] | None = None

    def __post_init__(self) -> None:
        symbols = self.request.symbols
        self.seen_source_records = set()
        self.adapters = {symbol: CanonicalMarketObservationAdapter(run_id=self.request.run_id or default_run_id(self.request.session_date)) for symbol in symbols}
        self.raw_counts = {symbol: 0 for symbol in symbols}
        self.canonical_counts = {symbol: 0 for symbol in symbols}
        self.diagnostics_summary = {}
        self.first_event_time = {symbol: None for symbol in symbols}
        self.last_event_time = {symbol: None for symbol in symbols}
        self.first_received_at = {symbol: None for symbol in symbols}
        self.last_received_at = {symbol: None for symbol in symbols}

    def process_message(self, message: dict[str, Any], *, received_at: datetime) -> None:
        symbol = str(message.get("S") or "").upper()
        if symbol not in self.request.symbols:
            return
        self.source_sequence += 1
        event = shadow_market_event_from_alpaca_message(message, source_sequence=self.source_sequence, archive_path=str(self.writers.raw[symbol].path))
        if event.source_record_id in self.seen_source_records:
            self._count_diagnostic("duplicate_source_record")
            return
        self.seen_source_records.add(event.source_record_id)
        raw_row = raw_capture_payload(message, event=event, received_at=received_at)
        self.writers.raw[symbol].write(raw_row)
        self.raw_counts[symbol] += 1
        event_iso = event.tick.timestamp.isoformat()
        received_iso = _utc(received_at).isoformat()
        if self.first_event_time[symbol] is None:
            self.first_event_time[symbol] = event_iso
        if self.first_received_at[symbol] is None:
            self.first_received_at[symbol] = received_iso
        self.last_event_time[symbol] = event_iso
        self.last_received_at[symbol] = received_iso
        result = self.adapters[symbol].process_tick(
            event.tick,
            trader_id=f"{RESEARCH_STREAM_ID_PREFIX}:{symbol}",
            source_event=event,
        )
        for diagnostic in result.diagnostics:
            self._count_diagnostic(diagnostic.diagnostic_type)
            self.writers.diagnostics[symbol].write({"received_at": received_iso, "diagnostic": diagnostic})
        for observation in result.observations:
            self.writers.canonical[symbol].write({"received_at": received_iso, "observation": observation})
            self.canonical_counts[symbol] += 1

    def _count_diagnostic(self, diagnostic_type: str) -> None:
        self.diagnostics_summary[diagnostic_type] = self.diagnostics_summary.get(diagnostic_type, 0) + 1


def shadow_market_event_from_alpaca_message(message: dict[str, Any], *, source_sequence: int, archive_path: str | None = None) -> LiveMarketEvent:
    tick = MarketTick.from_alpaca_trade(message)
    return LiveMarketEvent(
        tick=tick,
        source_record_id=shadow_source_record_id(tick, source_sequence=source_sequence, raw_message=message),
        broker_trade_id=tick.trade_id or None,
        source_sequence=source_sequence,
        eligible=evaluate_market_tick_eligibility(tick),
        archive_path=archive_path,
    )


def shadow_source_record_id(tick: MarketTick, *, source_sequence: int, raw_message: dict[str, Any]) -> str:
    payload = {
        "timestamp": tick.timestamp.isoformat(),
        "symbol": tick.symbol.upper(),
        "price": tick.price,
        "size": tick.size,
        "exchange": tick.exchange,
        "trade_id": tick.trade_id,
        "tape": tick.tape,
        "conditions": list(tick.conditions),
        "source_sequence": int(source_sequence),
        "raw_sha256": sha256(json.dumps(raw_message, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest(),
    }
    return "shadow_source_record:" + sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def raw_capture_payload(message: dict[str, Any], *, event: LiveMarketEvent, received_at: datetime) -> dict[str, Any]:
    tick = event.tick
    return {
        "schema_version": SHADOW_CAPTURE_SCHEMA_VERSION,
        "provider": "alpaca",
        "feed": "sip",
        "message_type": message.get("T"),
        "received_at": _utc(received_at).isoformat(),
        "timestamp": tick.timestamp.isoformat(),
        "symbol": tick.symbol,
        "price": tick.price,
        "size": tick.size,
        "exchange": tick.exchange,
        "trade_id": tick.trade_id,
        "tape": tick.tape,
        "conditions": list(tick.conditions),
        "source_sequence": event.source_sequence,
        "source_record_id": event.source_record_id,
        "broker_trade_id": event.broker_trade_id,
        "eligible": event.eligible,
        "raw": message,
    }


def write_manifest(state: CaptureState, *, status: str, disk_preflight: dict[str, Any]) -> None:
    manifest_path = state.output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(_jsonable(capture_manifest(state, status=status, disk_preflight=disk_preflight)), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def capture_manifest(state: CaptureState, *, status: str, disk_preflight: dict[str, Any]) -> dict[str, Any]:
    request = state.request
    window = nyse_session_window(request.session_date)
    completeness = capture_completeness(session_date=request.session_date, started_at=state.started_at, completed_at=state.completed_at)
    contracts = {symbol: asdict(market_stream_contract(symbol=symbol, session_timezone="America/New_York")) for symbol in request.symbols}
    paths = {
        symbol: {
            "raw": str(state.writers.raw[symbol].path),
            "canonical_1s_close": str(state.writers.canonical[symbol].path),
            "diagnostics": str(state.writers.diagnostics[symbol].path),
        }
        for symbol in request.symbols
    }
    checksums = {
        symbol: {
            "raw": _path_sha256_if_exists(state.writers.raw[symbol].path),
            "canonical_1s_close": _path_sha256_if_exists(state.writers.canonical[symbol].path),
            "diagnostics": _path_sha256_if_exists(state.writers.diagnostics[symbol].path),
        }
        for symbol in request.symbols
    }
    return {
        "schema_version": SHADOW_CAPTURE_SCHEMA_VERSION,
        "producer_version": SHADOW_CAPTURE_PRODUCER_VERSION,
        "run_id": request.run_id,
        "status": status,
        "completeness": completeness,
        "requested_session": request.session_date.isoformat(),
        "session_window": {"start_utc": window.start_utc.isoformat(), "end_utc": window.end_utc.isoformat()},
        "started_at": state.started_at.isoformat(),
        "completed_at": state.completed_at.isoformat() if state.completed_at else None,
        "symbols": list(request.symbols),
        "provider": "alpaca",
        "feed": request.feed,
        "subscription": {"trades": list(request.symbols)},
        "market_data_endpoint": "alpaca_data_stream",
        "execution_endpoints_enabled": False,
        "broker_order_endpoints_enabled": False,
        "production_state_mutation": False,
        "production_trader_restart": False,
        "credential_profile": request.credential_profile,
        "credentials_logged": False,
        "canonical_filter_version": TRADE_FILTER_VERSION,
        "canonical_sampler_version": SAMPLER_VERSION,
        "canonical_adapter": "CanonicalMarketObservationAdapter",
        "market_stream_contracts": contracts,
        "market_stream_fingerprints": {symbol: contracts[symbol]["fingerprint"] for symbol in request.symbols},
        "raw_counts": dict(state.raw_counts or {}),
        "canonical_counts": dict(state.canonical_counts or {}),
        "diagnostics_summary": dict(state.diagnostics_summary or {}),
        "first_event_time": dict(state.first_event_time or {}),
        "last_event_time": dict(state.last_event_time or {}),
        "first_received_at": dict(state.first_received_at or {}),
        "last_received_at": dict(state.last_received_at or {}),
        "paths": paths,
        "checksums": checksums,
        "disk_preflight": disk_preflight,
        "output_directory": str(state.output_dir),
        "error": state.error,
        "notes": [
            "Research-only capture; no strategy engines are instantiated.",
            "Only Alpaca market-data stream auth and trade subscription are used.",
            "Canonical observations are emitted by the Silver delayed 1s_close adapter.",
            "Completeness is COMPLETE only when capture starts no later than regular-session open and completes after regular-session close.",
        ],
    }


def capture_completeness(*, session_date: date, started_at: datetime, completed_at: datetime | None) -> str:
    window = nyse_session_window(session_date)
    started = _utc(started_at)
    completed = _utc(completed_at) if completed_at is not None else None
    if started > window.start_utc:
        return "PARTIAL_STARTED_AFTER_OPEN"
    if completed is None or completed < window.end_utc:
        return "PARTIAL_INCOMPLETE"
    return "COMPLETE"


def shadow_capture_result(state: CaptureState, *, status: str) -> ShadowCaptureResult:
    return ShadowCaptureResult(
        run_id=str(state.request.run_id),
        output_directory=str(state.output_dir),
        manifest_path=str(state.output_dir / "manifest.json"),
        status=status,
        completeness=capture_completeness(session_date=state.request.session_date, started_at=state.started_at, completed_at=state.completed_at),
        started_at=state.started_at.isoformat(),
        completed_at=state.completed_at.isoformat() if state.completed_at else None,
        raw_counts=dict(state.raw_counts or {}),
        canonical_counts=dict(state.canonical_counts or {}),
        diagnostics_summary=dict(state.diagnostics_summary or {}),
    )


def decode_ws_payload(raw_message: str | bytes) -> Any:
    if isinstance(raw_message, bytes):
        raw_message = raw_message.decode("utf-8")
    return json.loads(raw_message)


def _normalize_request(request: ShadowCaptureRequest) -> ShadowCaptureRequest:
    symbols = tuple(symbol.strip().upper() for symbol in request.symbols if symbol.strip())
    if not symbols:
        raise ValueError("Shadow capture requires at least one symbol.")
    run_id = request.run_id or default_run_id(request.session_date)
    return ShadowCaptureRequest(
        symbols=symbols,
        session_date=request.session_date,
        output_root=request.output_root,
        run_id=run_id,
        feed=request.feed.lower(),
        env_file=request.env_file,
        credential_profile=request.credential_profile,
        stream_url=request.stream_url,
        min_free_gb=request.min_free_gb,
        max_disk_used_pct=request.max_disk_used_pct,
        duration_seconds=request.duration_seconds,
        stop_after_session_end_seconds=request.stop_after_session_end_seconds,
        heartbeat_seconds=request.heartbeat_seconds,
        connect_timeout_seconds=request.connect_timeout_seconds,
        ping_interval_seconds=request.ping_interval_seconds,
        ping_timeout_seconds=request.ping_timeout_seconds,
        started_at_override=request.started_at_override,
    )


def _validate_research_only_request(request: ShadowCaptureRequest) -> None:
    if request.feed != "sip":
        raise ValueError("Research shadow capture requires feed='sip'.")
    reject_production_tick_root(request.output_root)
    raw_path = Path(request.output_root)
    normalized = str(raw_path)
    resolved = str(raw_path.resolve(strict=False))
    blocked_roots = (
        "artifacts/live",
        str(Path("artifacts/live").resolve(strict=False)),
        "/root/trading-server/artifacts/live",
    )
    for root in blocked_roots:
        if normalized == root or normalized.startswith(root.rstrip("/") + "/") or resolved == root or resolved.startswith(root.rstrip("/") + "/"):
            raise ValueError(f"Refusing to write research shadow capture into production artifact root: {request.output_root}")
    if request.stream_url and "api.alpaca.markets" in request.stream_url:
        raise ValueError("Shadow capture stream_url must be a market-data stream endpoint, not an Alpaca trading endpoint.")


def _redacted_stream_error(message: dict[str, Any]) -> dict[str, Any]:
    redacted = dict(message)
    for key in list(redacted):
        if "key" in key.lower() or "secret" in key.lower() or "token" in key.lower():
            redacted[key] = "[REDACTED]"
    return redacted


def _path_sha256_if_exists(path: Path) -> str | None:
    if not path.exists():
        return None
    return file_sha256(path)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return _jsonable(asdict(value))
    if isinstance(value, dict):
        return {str(key): _jsonable(val) for key, val in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value
