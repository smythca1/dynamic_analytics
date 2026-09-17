from __future__ import annotations

import gzip
import json
from dataclasses import asdict, dataclass
from datetime import date
from decimal import Decimal
from hashlib import sha256
from itertools import zip_longest
from pathlib import Path
from typing import Any

import pandas as pd

from silver.market.trade_filter import evaluate_market_tick_eligibility

from .coverage import _process_canonical_close, _row_to_tick
from .market_calendar import nyse_session_window


OVERLAP_SCHEMA_VERSION = "research.live_historical_overlap.v1"


@dataclass(frozen=True)
class OverlapReport:
    symbol: str
    session_date: date
    live_path: str
    historical_path: str
    live_raw_count: int
    historical_raw_count: int
    raw_exact_match_percentage: float
    canonical_exact_match_percentage: float
    maximum_price_difference: str
    first_divergence: dict[str, Any] | None
    divergence_causes: dict[str, int]
    live_canonical_count: int
    historical_canonical_count: int
    live_canonical_checksum: str
    historical_canonical_checksum: str
    changes_renko_bricks: bool | str
    changes_signals_or_order_intents: bool | str
    status: str
    failure_reasons: tuple[str, ...]


def compare_overlap(
    *,
    symbol: str,
    session_date: date,
    live_path: str | Path,
    historical_path: str | Path,
) -> OverlapReport:
    live = Path(live_path)
    hist = Path(historical_path)
    window = nyse_session_window(session_date)
    causes: dict[str, int] = {}
    first_divergence: dict[str, Any] | None = None
    raw_matches = 0
    raw_total = 0
    max_price_diff = Decimal("0")
    live_raw_count = 0
    hist_raw_count = 0
    for index, pair in enumerate(zip_longest(_iter_rows(live, window=window), _iter_rows(hist, window=window))):
        live_item, hist_item = pair
        if live_item is not None:
            live_raw_count += 1
        if hist_item is not None:
            hist_raw_count += 1
        raw_total += 1
        if live_item is None or hist_item is None:
            cause = "missing_row_live" if live_item is None else "missing_row_historical"
            causes[cause] = causes.get(cause, 0) + 1
            if first_divergence is None:
                first_divergence = {"row_index": index, "cause": cause}
            continue
        live_sig = _raw_signature(live_item[1], symbol=symbol)
        hist_sig = _raw_signature(hist_item[1], symbol=symbol)
        if live_sig == hist_sig:
            raw_matches += 1
            continue
        cause = _raw_divergence_cause(live_sig, hist_sig)
        causes[cause] = causes.get(cause, 0) + 1
        price_diff = abs(Decimal(live_sig["price"]) - Decimal(hist_sig["price"]))
        max_price_diff = max(max_price_diff, price_diff)
        if first_divergence is None:
            first_divergence = {
                "row_index": index,
                "cause": cause,
                "live": live_sig,
                "historical": hist_sig,
            }
    live_canonical = _canonical_series(live, symbol=symbol, label="live", window=window)
    hist_canonical = _canonical_series(hist, symbol=symbol, label="historical", window=window)
    canonical_matches = 0
    canonical_total = max(len(live_canonical), len(hist_canonical))
    canonical_first_divergence = None
    for index, pair in enumerate(zip_longest(live_canonical, hist_canonical)):
        if pair[0] == pair[1]:
            canonical_matches += 1
            continue
        if canonical_first_divergence is None:
            canonical_first_divergence = {"canonical_index": index, "live": pair[0], "historical": pair[1]}
    if first_divergence is None and canonical_first_divergence is not None:
        first_divergence = {"cause": "canonical_observation_divergence", **canonical_first_divergence}
    failure_reasons: list[str] = []
    if raw_total and raw_matches != raw_total:
        failure_reasons.append("raw_economic_fields_diverged")
    if canonical_total and canonical_matches != canonical_total:
        failure_reasons.append("canonical_observations_diverged")
    if len(live_canonical) != len(hist_canonical):
        failure_reasons.append("canonical_count_mismatch")
    return OverlapReport(
        symbol.upper(),
        session_date,
        str(live),
        str(hist),
        live_raw_count,
        hist_raw_count,
        _pct(raw_matches, raw_total),
        _pct(canonical_matches, canonical_total),
        str(max_price_diff),
        first_divergence,
        causes,
        len(live_canonical),
        len(hist_canonical),
        _series_checksum(live_canonical),
        _series_checksum(hist_canonical),
        bool(failure_reasons) if canonical_total else "UNKNOWN_NO_CANONICAL_INPUT",
        bool(failure_reasons) if canonical_total else "UNKNOWN_NO_CANONICAL_INPUT",
        "PASS" if not failure_reasons else "FAIL",
        tuple(failure_reasons),
    )


def write_overlap_reports(path: str | Path, reports: list[OverlapReport]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps({"schema_version": OVERLAP_SCHEMA_VERSION, "reports": [asdict(report) for report in reports]}, indent=2, sort_keys=True, default=str)
        + "\n",
        encoding="utf-8",
    )


def _iter_rows(path: Path, *, window=None):
    opener = gzip.open if path.suffixes[-2:] == [".jsonl", ".gz"] else open
    with opener(path, "rt", encoding="utf-8") as handle:
        for index, line in enumerate(handle):
            if line.strip():
                row = json.loads(line)
                if window is not None and not _row_in_window(row, window=window):
                    continue
                yield index, row


def _raw_signature(row: dict[str, Any], *, symbol: str) -> dict[str, str]:
    conditions = row.get("c") or row.get("conditions") or ()
    if isinstance(conditions, str):
        conditions = [item.strip() for item in conditions.split(",") if item.strip()]
    timestamp = row.get("t") or row.get("timestamp") or row.get("sip_timestamp")
    return {
        "timestamp": _normalized_timestamp(timestamp),
        "symbol": str(row.get("S") or row.get("symbol") or symbol).upper(),
        "price": _normalized_decimal(row.get("p") or row.get("price")),
        "size": _normalized_decimal(row.get("s") or row.get("size") or row.get("quantity") or 1),
        "exchange": str(row.get("x") or row.get("exchange") or ""),
        "tape": str(row.get("z") or row.get("tape") or ""),
        "conditions": ",".join(str(item) for item in conditions),
        "trade_id": str(row.get("i") or row.get("trade_id") or ""),
    }


def _raw_divergence_cause(live: dict[str, str], hist: dict[str, str]) -> str:
    for key in ("timestamp", "price", "size", "exchange", "tape", "conditions", "trade_id"):
        if live.get(key) != hist.get(key):
            return f"{key}_mismatch"
    return "unknown_mismatch"


def _canonical_series(path: Path, *, symbol: str, label: str, window) -> list[dict[str, str]]:
    series: list[dict[str, str]] = []
    pending_observation: dict[str, Any] | None = None
    for row_order, row in _iter_rows(path, window=window):
        try:
            tick = _row_to_tick(row, symbol=symbol)
        except Exception:
            continue
        if tick.price <= 0 or not evaluate_market_tick_eligibility(tick):
            continue
        emitted, pending_observation = _process_canonical_close(
            pending_observation,
            tick=tick,
            row_order=row_order,
        )
        if emitted is not None:
            series.append(_canonical_overlap_row(emitted))
    return series


def _canonical_overlap_row(observation: dict[str, Any]) -> dict[str, str]:
    return {
        "second_bucket": observation["second_bucket"],
        "close_price": observation["close_price"],
        "source_sip_timestamp": observation["source_sip_timestamp"],
        "source_alpaca_trade_id": observation["source_alpaca_trade_id"],
        "source_exchange": observation["source_exchange"],
        "source_tape": observation["source_tape"],
    }


def _series_checksum(series: list[dict[str, str]]) -> str:
    digest = sha256()
    for row in series:
        digest.update((json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8"))
    return digest.hexdigest()


def _pct(matches: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round(matches / total * 100.0, 6)


def _normalized_timestamp(value: Any) -> str:
    if value is None:
        return ""
    return pd.Timestamp(value).isoformat()


def _normalized_decimal(value: Any) -> str:
    if value is None:
        return ""
    number = Decimal(str(value)).normalize()
    if number == number.to_integral():
        return str(number.quantize(Decimal("1")))
    return format(number, "f")


def _row_in_window(row: dict[str, Any], *, window) -> bool:
    timestamp = row.get("t") or row.get("timestamp") or row.get("sip_timestamp")
    if timestamp is None:
        return False
    parsed = pd.Timestamp(timestamp).to_pydatetime(warn=False)
    return window.start_utc <= parsed <= window.end_utc
