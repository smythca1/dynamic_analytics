from __future__ import annotations

import gzip
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from replay.historical_source import file_sha256
from silver.market import market_stream_contract
from silver.market.trade_filter import evaluate_market_tick_eligibility
from trading_events import MarketTick

from .market_calendar import is_nyse_early_close, nyse_session_window


COVERAGE_SCHEMA_VERSION = "research.streaming_coverage.v1"


@dataclass(frozen=True)
class CoverageReport:
    symbol: str
    session_date: date
    source_path: str
    raw_trade_count: int
    canonical_observation_count: int
    first_timestamp: str | None
    last_timestamp: str | None
    expected_start_utc: str
    expected_end_utc: str
    coverage_percentage: float
    missing_intervals: tuple[dict[str, Any], ...]
    duplicate_timestamps: int
    duplicate_trade_ids: int
    out_of_order_records: int
    invalid_prices: int
    malformed_rows: int
    off_session_records: int
    maximum_gap_seconds: float | None
    early_close: bool
    halt_handling: str
    source_checksum: str
    canonical_checksum: str
    contract_fingerprint: str
    status: str
    failure_reasons: tuple[str, ...]


def validate_coverage(
    *,
    symbol: str,
    session_date: date,
    source_path: str | Path,
    max_gap_seconds: int = 900,
    boundary_tolerance_seconds: int = 120,
) -> CoverageReport:
    path = Path(source_path)
    if not path.exists():
        window = nyse_session_window(session_date)
        contract = market_stream_contract(symbol=symbol, session_timezone="America/New_York")
        return CoverageReport(
            symbol.upper(),
            session_date,
            str(path),
            0,
            0,
            None,
            None,
            window.start_utc.isoformat(),
            window.end_utc.isoformat(),
            0.0,
            tuple(),
            0,
            0,
            0,
            0,
            0,
            0,
            None,
            is_nyse_early_close(session_date),
            "not_detected",
            "",
            "",
            contract.fingerprint,
            "FAIL",
            ("missing_source_file",),
        )
    window = nyse_session_window(session_date)
    contract = market_stream_contract(symbol=symbol, session_timezone="America/New_York")
    source_checksum = file_sha256(path)
    canonical_digest = sha256()
    raw_count = 0
    canonical_count = 0
    malformed = 0
    invalid_prices = 0
    off_session = 0
    duplicate_timestamps = 0
    duplicate_trade_ids = 0
    out_of_order = 0
    pending_observation: dict[str, Any] | None = None
    seen_timestamps: set[str] = set()
    seen_trade_ids: set[str] = set()
    first_ts: datetime | None = None
    last_ts: datetime | None = None
    prev_ts: datetime | None = None
    max_gap: float | None = None
    missing: list[dict[str, Any]] = []
    for row_order, row in _iter_json_rows(path):
        try:
            tick = _row_to_tick(row, symbol=symbol)
        except Exception:
            malformed += 1
            continue
        timestamp = tick.timestamp.to_pydatetime(warn=False)
        if tick.price <= 0:
            invalid_prices += 1
            continue
        raw_count += 1
        timestamp_key = timestamp.isoformat()
        if timestamp_key in seen_timestamps:
            duplicate_timestamps += 1
        seen_timestamps.add(timestamp_key)
        if tick.trade_id:
            trade_key = f"{tick.symbol}:{tick.trade_id}"
            if trade_key in seen_trade_ids:
                duplicate_trade_ids += 1
            seen_trade_ids.add(trade_key)
        if first_ts is None:
            first_ts = timestamp
        last_ts = timestamp
        if timestamp < window.start_utc or timestamp > window.end_utc:
            off_session += 1
        if prev_ts is not None:
            gap = (timestamp - prev_ts).total_seconds()
            if gap < 0:
                out_of_order += 1
            elif window.start_utc <= prev_ts <= window.end_utc and gap > max_gap_seconds:
                missing.append({"start": prev_ts.isoformat(), "end": timestamp.isoformat(), "gap_seconds": gap})
                max_gap = gap if max_gap is None else max(max_gap, gap)
            elif gap >= 0:
                max_gap = gap if max_gap is None else max(max_gap, gap)
        prev_ts = timestamp
        if window.start_utc <= timestamp <= window.end_utc and evaluate_market_tick_eligibility(tick):
            emitted, pending_observation = _process_canonical_close(
                pending_observation,
                tick=tick,
                row_order=row_order,
            )
            if emitted is not None:
                canonical_count += 1
                canonical_digest.update((_canonical_semantic_line(emitted)).encode("utf-8"))
    reasons: list[str] = []
    if raw_count == 0:
        reasons.append("no_raw_trades")
    if canonical_count == 0:
        reasons.append("no_canonical_observations")
    if malformed:
        reasons.append("malformed_rows")
    if invalid_prices:
        reasons.append("invalid_prices")
    if out_of_order:
        reasons.append("out_of_order_records")
    if first_ts is None or first_ts > window.start_utc + pd.Timedelta(seconds=boundary_tolerance_seconds).to_pytimedelta():
        reasons.append("late_first_trade")
    if last_ts is None or last_ts < window.end_utc - pd.Timedelta(seconds=boundary_tolerance_seconds).to_pytimedelta():
        reasons.append("early_last_trade")
    if missing:
        reasons.append("large_intraday_gaps")
    coverage = _coverage_percentage(first_ts, last_ts, expected_start=window.start_utc, expected_end=window.end_utc)
    return CoverageReport(
        symbol.upper(),
        session_date,
        str(path),
        raw_count,
        canonical_count,
        first_ts.isoformat() if first_ts else None,
        last_ts.isoformat() if last_ts else None,
        window.start_utc.isoformat(),
        window.end_utc.isoformat(),
        coverage,
        tuple(missing[:100]),
        duplicate_timestamps,
        duplicate_trade_ids,
        out_of_order,
        invalid_prices,
        malformed,
        off_session,
        max_gap,
        is_nyse_early_close(session_date),
        "not_detected",
        source_checksum,
        canonical_digest.hexdigest(),
        contract.fingerprint,
        "PASS" if not reasons else "FAIL",
        tuple(reasons),
    )


def write_coverage_reports(path: str | Path, reports: Iterable[CoverageReport]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": COVERAGE_SCHEMA_VERSION,
        "reports": [asdict(report) for report in reports],
    }
    target.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def _iter_json_rows(path: Path):
    opener = gzip.open if path.suffixes[-2:] == [".jsonl", ".gz"] else open
    with opener(path, "rt", encoding="utf-8") as handle:
        for index, line in enumerate(handle):
            if line.strip():
                yield index, json.loads(line)


def _row_to_tick(row: dict[str, Any], *, symbol: str) -> MarketTick:
    timestamp = row.get("t") or row.get("timestamp") or row.get("sip_timestamp")
    price = row.get("p") or row.get("price")
    size = row.get("s") or row.get("size") or row.get("quantity") or 1
    conditions = row.get("c") or row.get("conditions") or ()
    if isinstance(conditions, str):
        conditions = [item.strip() for item in conditions.split(",") if item.strip()]
    return MarketTick(
        timestamp=pd.Timestamp(timestamp),
        symbol=str(row.get("S") or row.get("symbol") or symbol).upper(),
        price=float(price),
        size=float(size),
        exchange=str(row.get("x") or row.get("exchange") or ""),
        trade_id=str(row.get("i") or row.get("trade_id") or ""),
        tape=str(row.get("z") or row.get("tape") or ""),
        conditions=tuple(str(item) for item in conditions),
    )


def _process_canonical_close(
    pending: dict[str, Any] | None,
    *,
    tick: MarketTick,
    row_order: int,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    timestamp = tick.timestamp.to_pydatetime(warn=False)
    second = timestamp.replace(microsecond=0)
    current = {
        "symbol": tick.symbol,
        "second_bucket": second.isoformat(),
        "close_price": str(tick.price),
        "source_sip_timestamp": timestamp.isoformat(),
        "source_alpaca_trade_id": tick.trade_id,
        "source_exchange": tick.exchange,
        "source_tape": tick.tape,
        "row_order": row_order,
    }
    if pending is None:
        return None, current
    if current["second_bucket"] == pending["second_bucket"]:
        if (current["source_sip_timestamp"], current["row_order"]) >= (pending["source_sip_timestamp"], pending["row_order"]):
            return None, current
        return None, pending
    if current["second_bucket"] > pending["second_bucket"]:
        return pending, current
    return None, pending


def _canonical_semantic_line(observation: dict[str, Any]) -> str:
    payload = {
        "symbol": observation["symbol"],
        "second_bucket": observation["second_bucket"],
        "close_price": observation["close_price"],
        "source_sip_timestamp": observation["source_sip_timestamp"],
        "source_alpaca_trade_id": observation["source_alpaca_trade_id"],
        "source_exchange": observation["source_exchange"],
        "source_tape": observation["source_tape"],
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"


def _coverage_percentage(first_ts: datetime | None, last_ts: datetime | None, *, expected_start: datetime, expected_end: datetime) -> float:
    if first_ts is None or last_ts is None:
        return 0.0
    expected = max((expected_end - expected_start).total_seconds(), 1.0)
    covered_start = max(first_ts, expected_start)
    covered_end = min(last_ts, expected_end)
    covered = max((covered_end - covered_start).total_seconds(), 0.0)
    return round(covered / expected * 100.0, 6)
