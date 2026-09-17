from __future__ import annotations

import gzip
import json
import os
import shutil
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable

import requests

from replay.historical_source import file_sha256

from .market_calendar import nyse_session_window


ALPACA_DATA_BASE_URL = "https://data.alpaca.markets"
BACKFILL_SCHEMA_VERSION = "research.alpaca_sip_backfill.v1"


@dataclass(frozen=True)
class HistoricalSipBackfillRequest:
    symbols: tuple[str, ...]
    sessions: tuple[date, ...]
    output_root: str = "artifacts/research/raw-data"
    feed: str = "sip"
    limit: int = 10_000
    request_sleep_seconds: float = 0.0
    max_pages_per_session: int | None = None
    min_free_gb: float = 20.0
    max_disk_used_pct: float = 90.0
    env_file: str | None = ".env"
    credential_profile: str | None = None
    reuse_existing: bool = True


@dataclass(frozen=True)
class BackfilledFile:
    symbol: str
    session_date: date
    raw_path: str
    manifest_path: str
    reused: bool
    raw_record_count: int
    first_timestamp: str | None
    last_timestamp: str | None
    sha256: str
    page_count: int
    byte_size: int


@dataclass(frozen=True)
class HistoricalSipBackfillResult:
    files: tuple[BackfilledFile, ...]
    output_root: str
    started_at: str
    completed_at: str
    disk_preflight: dict[str, Any]


def research_raw_path(output_root: str | Path, symbol: str, session: date) -> Path:
    return Path(output_root) / "raw" / "alpaca" / "sip" / symbol.upper() / session.isoformat() / "sip_trades.jsonl.gz"


def backfill_historical_sip(
    request: HistoricalSipBackfillRequest,
    *,
    session_factory: Callable[[], requests.Session] = requests.Session,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> HistoricalSipBackfillResult:
    if request.feed.lower() != "sip":
        raise ValueError("Research backfill requires feed='sip'.")
    output_root = Path(request.output_root)
    reject_production_tick_root(output_root)
    disk_preflight = disk_capacity_preflight(
        output_root,
        min_free_gb=request.min_free_gb,
        max_used_pct=request.max_disk_used_pct,
    )
    _load_env_file(request.env_file)
    headers = alpaca_data_headers(profile=request.credential_profile)
    started = now().isoformat()
    client = session_factory()
    files: list[BackfilledFile] = []
    for symbol in request.symbols:
        for session_date in request.sessions:
            files.append(
                fetch_symbol_session(
                    client=client,
                    headers=headers,
                    request=request,
                    symbol=symbol.upper(),
                    session_date=session_date,
                    retrieved_at=now,
                )
            )
    return HistoricalSipBackfillResult(
        files=tuple(files),
        output_root=str(output_root),
        started_at=started,
        completed_at=now().isoformat(),
        disk_preflight=disk_preflight,
    )


def fetch_symbol_session(
    *,
    client: requests.Session,
    headers: dict[str, str],
    request: HistoricalSipBackfillRequest,
    symbol: str,
    session_date: date,
    retrieved_at: Callable[[], datetime],
) -> BackfilledFile:
    raw_path = research_raw_path(request.output_root, symbol, session_date)
    manifest_path = raw_path.with_suffix(".manifest.json")
    if raw_path.exists():
        if not request.reuse_existing:
            raise FileExistsError(f"Refusing to overwrite immutable raw research data: {raw_path}")
        if not manifest_path.exists():
            raise FileExistsError(f"Existing raw research data has no manifest: {raw_path}")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        checksum = file_sha256(raw_path)
        if manifest.get("sha256") != checksum:
            raise FileExistsError(f"Existing raw research data checksum does not match manifest: {raw_path}")
        return BackfilledFile(
            symbol=symbol,
            session_date=session_date,
            raw_path=str(raw_path),
            manifest_path=str(manifest_path),
            reused=True,
            raw_record_count=int(manifest.get("raw_record_count") or 0),
            first_timestamp=manifest.get("first_timestamp"),
            last_timestamp=manifest.get("last_timestamp"),
            sha256=checksum,
            page_count=int(manifest.get("pagination", {}).get("page_count") or 0),
            byte_size=raw_path.stat().st_size,
        )

    window = nyse_session_window(session_date)
    params: dict[str, Any] = {
        "start": _iso_z(window.start_utc),
        "end": _iso_z(window.end_utc),
        "feed": request.feed,
        "limit": int(request.limit),
        "sort": "asc",
    }
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = raw_path.with_name(raw_path.name + ".tmp")
    if tmp_path.exists():
        tmp_path.unlink()
    digest = sha256()
    count = 0
    first_timestamp: str | None = None
    last_timestamp: str | None = None
    page_count = 0
    page_summaries: list[dict[str, Any]] = []
    started_at = retrieved_at().isoformat()
    try:
        with gzip.open(tmp_path, "wt", encoding="utf-8", compresslevel=6) as handle:
            while True:
                payload = _get_json_with_retries(
                    client,
                    f"{ALPACA_DATA_BASE_URL}/v2/stocks/{symbol}/trades",
                    headers=headers,
                    params=params,
                )
                trades = payload.get("trades") or []
                page_count += 1
                page_first = None
                page_last = None
                for trade in trades:
                    row = normalize_alpaca_trade(trade, symbol=symbol, row_order=count)
                    line = json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n"
                    handle.write(line)
                    digest.update(line.encode("utf-8"))
                    timestamp = str(row.get("t") or row.get("timestamp"))
                    first_timestamp = timestamp if first_timestamp is None else first_timestamp
                    last_timestamp = timestamp
                    page_first = timestamp if page_first is None else page_first
                    page_last = timestamp
                    count += 1
                page_summaries.append(
                    {
                        "page": page_count,
                        "records": len(trades),
                        "first_timestamp": page_first,
                        "last_timestamp": page_last,
                        "next_page_token_present": bool(payload.get("next_page_token")),
                    }
                )
                token = payload.get("next_page_token")
                if not token:
                    break
                if request.max_pages_per_session is not None and page_count >= request.max_pages_per_session:
                    raise RuntimeError(f"Stopped at max_pages_per_session={request.max_pages_per_session} for {symbol} {session_date}.")
                params["page_token"] = token
                if request.request_sleep_seconds > 0:
                    time.sleep(request.request_sleep_seconds)
        tmp_path.replace(raw_path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        raise
    compressed_checksum = file_sha256(raw_path)
    completed_at = retrieved_at().isoformat()
    manifest = {
        "schema_version": BACKFILL_SCHEMA_VERSION,
        "provider": "alpaca",
        "feed": request.feed,
        "symbol": symbol,
        "requested_session": session_date.isoformat(),
        "request_parameters": {key: value for key, value in params.items() if key != "page_token"},
        "session_window": {"start_utc": window.start_utc.isoformat(), "end_utc": window.end_utc.isoformat()},
        "retrieval": {"started_at": started_at, "completed_at": completed_at},
        "pagination": {"page_count": page_count, "pages": page_summaries},
        "raw_record_count": count,
        "first_timestamp": first_timestamp,
        "last_timestamp": last_timestamp,
        "sha256": compressed_checksum,
        "uncompressed_payload_sha256": digest.hexdigest(),
        "path": str(raw_path),
        "immutable": True,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return BackfilledFile(
        symbol=symbol,
        session_date=session_date,
        raw_path=str(raw_path),
        manifest_path=str(manifest_path),
        reused=False,
        raw_record_count=count,
        first_timestamp=first_timestamp,
        last_timestamp=last_timestamp,
        sha256=compressed_checksum,
        page_count=page_count,
        byte_size=raw_path.stat().st_size,
    )


def normalize_alpaca_trade(trade: dict[str, Any], *, symbol: str, row_order: int) -> dict[str, Any]:
    row = dict(trade)
    row.setdefault("S", symbol.upper())
    row.setdefault("symbol", symbol.upper())
    row.setdefault("provider", "alpaca")
    row.setdefault("feed", "sip")
    row["row_order"] = row_order
    row["source_sequence"] = row_order
    return row


def alpaca_data_headers(*, profile: str | None = None) -> dict[str, str]:
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
        raise RuntimeError("Historical SIP access unavailable: missing " + " and ".join(missing) + ".")
    return {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}


def disk_capacity_preflight(path: str | Path, *, min_free_gb: float, max_used_pct: float) -> dict[str, Any]:
    target = Path(path)
    probe = target
    while not probe.exists() and probe.parent != probe:
        probe = probe.parent
    usage = shutil.disk_usage(probe)
    used_pct = (usage.used / usage.total * 100.0) if usage.total else 100.0
    free_gb = usage.free / (1024**3)
    result = {
        "path": str(target),
        "probe_path": str(probe),
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
        "free_gb": round(free_gb, 3),
        "used_pct": round(used_pct, 3),
        "min_free_gb": min_free_gb,
        "max_used_pct": max_used_pct,
        "pass": free_gb >= min_free_gb and used_pct <= max_used_pct,
    }
    if not result["pass"]:
        raise RuntimeError(f"Disk preflight failed: {json.dumps(result, sort_keys=True)}")
    return result


def reject_production_tick_root(path: str | Path) -> None:
    normalized = str(Path(path))
    blocked = (
        "/root/trading-server/artifacts/live/ticks",
        "artifacts/live/ticks",
    )
    for root in blocked:
        if normalized == root or normalized.startswith(root.rstrip("/") + "/"):
            raise ValueError(f"Refusing to write research backfill into production tick root: {path}")


def _load_env_file(path: str | None) -> None:
    if not path:
        return
    env_path = Path(path)
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def _first_env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def _get_json_with_retries(
    client: requests.Session,
    url: str,
    *,
    headers: dict[str, str],
    params: dict[str, Any],
    attempts: int = 5,
) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = client.get(url, headers=headers, params=params, timeout=60)
            if response.status_code in {408, 429, 500, 502, 503, 504} and attempt < attempts:
                time.sleep(min(2.0 * attempt, 10.0))
                continue
            if response.status_code in {401, 403}:
                raise RuntimeError(f"Historical SIP access unavailable or insufficient entitlement: HTTP {response.status_code}.")
            if response.status_code >= 400:
                raise RuntimeError(f"Historical SIP request failed: HTTP {response.status_code}: {response.text[:240]}")
            payload = response.json()
            if not isinstance(payload, dict):
                raise RuntimeError("Historical SIP response was not a JSON object.")
            return payload
        except requests.RequestException as exc:
            last_error = exc
            if attempt >= attempts:
                break
            time.sleep(min(2.0 * attempt, 10.0))
    raise RuntimeError(f"Historical SIP request failed after {attempts} attempts: {last_error}") from last_error


def _iso_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
