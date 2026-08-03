from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

from silver.events.models import utc_datetime
from silver.market import RawSipTrade

from .models import ReplayConfig, ReplayDiagnostic, SourceIntegritySummary


@dataclass(frozen=True)
class HistoricalSourceResult:
    trades: tuple[RawSipTrade, ...]
    diagnostics: tuple[ReplayDiagnostic, ...]
    source_hash: str
    source_reference: str
    raw_rows_read: int
    malformed_rows: int
    raw_trades_in_window: int
    integrity: SourceIntegritySummary


def file_sha256(path: str | Path) -> str:
    digest = sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_historical_source(config: ReplayConfig) -> HistoricalSourceResult:
    path = Path(config.source_data_path)
    source_hash = file_sha256(path)
    diagnostics: list[ReplayDiagnostic] = []
    rows = _read_rows(path)
    trades: list[RawSipTrade] = []
    raw_rows_read = 0
    malformed_rows = 0
    integrity = _IntegrityBuilder(source_kind=_source_kind(path))
    stopped_after_requested_end = False
    for row_order, row in rows:
        raw_rows_read += 1
        integrity.observe_raw_row(row)
        try:
            trade = _row_to_trade(row, row_order=row_order, config=config, source_ref=str(path), source_hash=source_hash)
        except Exception as exc:
            malformed_rows += 1
            integrity.observe_malformed()
            diagnostics.append(
                ReplayDiagnostic(
                    diagnostic_type="malformed_source_row",
                    message=str(exc),
                    severity="error",
                    sequence=row_order,
                    details={"source": str(path), "row_order": row_order},
                )
            )
            continue
        integrity.observe_trade(trade)
        if config.start <= trade.sip_timestamp <= config.end and trade.symbol == config.symbol:
            trades.append(trade)
            integrity.observe_window_trade(trade)
        elif trade.symbol == config.symbol and trade.sip_timestamp > config.end and integrity.out_of_order_records == 0:
            stopped_after_requested_end = True
            break
    trades.sort(key=lambda trade: trade.ordering_key)
    summary = integrity.summary(
        raw_rows_read=raw_rows_read,
        malformed_rows=malformed_rows,
        raw_trades_in_window=len(trades),
        stopped_after_requested_end=stopped_after_requested_end,
    )
    diagnostics.extend(_integrity_diagnostics(summary))
    return HistoricalSourceResult(
        tuple(trades),
        tuple(diagnostics),
        source_hash,
        str(path),
        raw_rows_read,
        malformed_rows,
        len(trades),
        summary,
    )


def _read_rows(path: Path) -> Iterable[tuple[int, dict]]:
    suffix = path.suffix.lower()
    if suffix == ".jsonl":
        with path.open("r", encoding="utf-8") as handle:
            for index, line in enumerate(handle):
                if line.strip():
                    yield index, json.loads(line)
        return
    if suffix == ".csv":
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for index, row in enumerate(reader):
                yield index, dict(row)
        return
    raise ValueError(f"Unsupported historical source format: {path.suffix}")


def _timestamp(value, *, config: ReplayConfig):
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None and config.source_timezone is None:
            raise ValueError("Naive source timestamp requires explicit source timezone.")
    ts = utc_datetime(value)
    if value is not None and getattr(ts, "tzinfo", None) is None:
        ts = ts.replace(tzinfo=ZoneInfo(config.source_timezone))
    return ts


def _row_to_trade(row: dict, *, row_order: int, config: ReplayConfig, source_ref: str, source_hash: str) -> RawSipTrade:
    timestamp = row.get("timestamp") or row.get("t") or row.get("sip_timestamp")
    if timestamp is None:
        raise ValueError("Missing SIP timestamp.")
    conditions = row.get("conditions") or row.get("c") or ()
    if isinstance(conditions, str):
        stripped = conditions.strip()
        if stripped.startswith("["):
            conditions = json.loads(stripped)
        elif stripped:
            conditions = [item.strip() for item in stripped.split(",") if item.strip()]
        else:
            conditions = []
    broker_trade_id = str(row.get("trade_id") or row.get("i") or "")
    source_row_order = int(row.get("row_order") or row_order)
    return RawSipTrade(
        sip_timestamp=_timestamp(timestamp, config=config),
        receive_timestamp=row.get("logged_at") or row.get("receive_timestamp") or row.get("received_at") or timestamp,
        symbol=str(row.get("symbol") or row.get("S") or config.symbol).upper(),
        price=row.get("price") or row.get("p") or row.get("close"),
        quantity=row.get("size") or row.get("s") or row.get("quantity") or row.get("volume") or 1,
        exchange=str(row.get("exchange") or row.get("x") or ""),
        tape=str(row.get("tape") or row.get("z") or ""),
        trade_id=broker_trade_id,
        broker_trade_id=broker_trade_id,
        source_record_id=_source_record_id(
            source_dataset_hash=source_hash,
            source_dataset_reference=source_ref,
            source_row_order=source_row_order,
            symbol=str(row.get("symbol") or row.get("S") or config.symbol).upper(),
            sip_timestamp=_timestamp(timestamp, config=config).isoformat(),
            broker_trade_id=broker_trade_id,
        ),
        source_dataset_reference=source_ref,
        source_dataset_hash=source_hash,
        source_row_order=source_row_order,
        economic_event_fingerprint=_economic_row_fingerprint(
            symbol=str(row.get("symbol") or row.get("S") or config.symbol).upper(),
            sip_timestamp=_timestamp(timestamp, config=config).isoformat(),
            price=str(row.get("price") or row.get("p") or row.get("close")),
            quantity=str(row.get("size") or row.get("s") or row.get("quantity") or row.get("volume") or 1),
            exchange=str(row.get("exchange") or row.get("x") or ""),
            tape=str(row.get("tape") or row.get("z") or ""),
            conditions=tuple(str(item) for item in conditions),
            broker_trade_id=broker_trade_id,
        ),
        conditions=tuple(str(item) for item in conditions),
        source="historical_file",
        source_sequence=int(row.get("source_sequence") or row.get("sequence") or row_order),
        row_order=source_row_order,
        raw_payload_ref=f"{source_ref}:{row_order}",
        trader_id=config.trader_id,
    )


class _IntegrityBuilder:
    def __init__(self, *, source_kind: str) -> None:
        self.source_kind = source_kind
        self.row_hashes: set[str] = set()
        self.exact_duplicate_rows = 0
        self.source_identities: set[str] = set()
        self.trade_id_values: dict[str, tuple[str, str]] = {}
        self.duplicate_source_id_records = 0
        self.duplicate_trade_ids = 0
        self.duplicate_ids_with_identical_values = 0
        self.duplicate_ids_with_conflicting_values = 0
        self.rows_using_fallback_identity = 0
        self.ambiguous_equal_order_records = 0
        self.out_of_order_records = 0
        self.previous_ordering_key = None
        self.seen_ordering_keys: set[tuple] = set()
        self.timestamp_start = None
        self.timestamp_end = None
        self.window_timestamp_start = None
        self.window_timestamp_end = None
        self.previous_window_timestamp = None
        self.maximum_timestamp_gap_seconds = None

    def observe_raw_row(self, row: dict) -> None:
        fingerprint = json.dumps(row, sort_keys=True, default=str)
        if fingerprint in self.row_hashes:
            self.exact_duplicate_rows += 1
        self.row_hashes.add(fingerprint)

    def observe_malformed(self) -> None:
        return

    def observe_trade(self, trade: RawSipTrade) -> None:
        if self.timestamp_start is None or trade.sip_timestamp < self.timestamp_start:
            self.timestamp_start = trade.sip_timestamp
        if self.timestamp_end is None or trade.sip_timestamp > self.timestamp_end:
            self.timestamp_end = trade.sip_timestamp
        if self.previous_ordering_key is not None and trade.ordering_key < self.previous_ordering_key:
            self.out_of_order_records += 1
        if trade.ordering_key in self.seen_ordering_keys:
            self.ambiguous_equal_order_records += 1
        self.previous_ordering_key = trade.ordering_key
        self.seen_ordering_keys.add(trade.ordering_key)
        if not trade.broker_trade_id:
            self.rows_using_fallback_identity += 1
        if trade.source_identity in self.source_identities:
            self.duplicate_source_id_records += 1
        self.source_identities.add(trade.source_identity)
        if trade.broker_trade_id:
            key = f"{trade.source}:{trade.symbol}:{trade.broker_trade_id}"
            economic = trade.economic_event_fingerprint or _economic_fingerprint(trade)
            if key in self.trade_id_values:
                self.duplicate_trade_ids += 1
                if self.trade_id_values[key][0] == economic:
                    self.duplicate_ids_with_identical_values += 1
                else:
                    self.duplicate_ids_with_conflicting_values += 1
            self.trade_id_values.setdefault(key, (economic, trade.raw_payload_ref or ""))

    def observe_window_trade(self, trade: RawSipTrade) -> None:
        if self.window_timestamp_start is None or trade.sip_timestamp < self.window_timestamp_start:
            self.window_timestamp_start = trade.sip_timestamp
        if self.window_timestamp_end is None or trade.sip_timestamp > self.window_timestamp_end:
            self.window_timestamp_end = trade.sip_timestamp
        if self.previous_window_timestamp is not None:
            gap = (trade.sip_timestamp - self.previous_window_timestamp).total_seconds()
            if self.maximum_timestamp_gap_seconds is None or gap > self.maximum_timestamp_gap_seconds:
                self.maximum_timestamp_gap_seconds = gap
        self.previous_window_timestamp = trade.sip_timestamp

    def summary(
        self,
        *,
        raw_rows_read: int,
        malformed_rows: int,
        raw_trades_in_window: int,
        stopped_after_requested_end: bool,
    ) -> SourceIntegritySummary:
        duplicate_rate = (
            Decimal(str(self.duplicate_source_id_records)) / Decimal(str(raw_rows_read)) * Decimal("100")
            if raw_rows_read
            else Decimal("0")
        )
        return SourceIntegritySummary(
            total_raw_rows=raw_rows_read,
            source_rows_scanned=raw_rows_read,
            raw_trades_in_window=raw_trades_in_window,
            unique_source_identities=len(self.source_identities),
            exact_duplicate_rows=self.exact_duplicate_rows,
            duplicate_source_id_records=self.duplicate_source_id_records,
            duplicate_trade_ids=self.duplicate_trade_ids,
            duplicate_ids_with_identical_values=self.duplicate_ids_with_identical_values,
            duplicate_ids_with_conflicting_values=self.duplicate_ids_with_conflicting_values,
            rows_using_fallback_identity=self.rows_using_fallback_identity,
            ambiguous_equal_order_records=self.ambiguous_equal_order_records,
            out_of_order_records=self.out_of_order_records,
            malformed_rows=malformed_rows,
            timestamp_range_start=self.timestamp_start,
            timestamp_range_end=self.timestamp_end,
            window_timestamp_start=self.window_timestamp_start,
            window_timestamp_end=self.window_timestamp_end,
            maximum_timestamp_gap_seconds=Decimal(str(self.maximum_timestamp_gap_seconds))
            if self.maximum_timestamp_gap_seconds is not None
            else None,
            duplicate_rate_pct=duplicate_rate,
            stopped_after_requested_end=stopped_after_requested_end,
            source_kind=self.source_kind,
        )


def _source_kind(path: Path) -> str:
    name = path.name.lower()
    if name.endswith("_1s_close.csv") or "1s_close" in name:
        return "sip_derived_1s_close"
    return "raw_sip_trades"


def _economic_fingerprint(trade: RawSipTrade) -> str:
    return "|".join(
        [
            trade.sip_timestamp.isoformat(),
            trade.symbol,
            str(trade.price),
            str(trade.quantity),
            trade.exchange,
            trade.tape,
            ",".join(trade.conditions),
        ]
    )


def _source_record_id(
    *,
    source_dataset_hash: str,
    source_dataset_reference: str,
    source_row_order: int,
    symbol: str,
    sip_timestamp: str,
    broker_trade_id: str,
) -> str:
    stable = "|".join([source_dataset_hash, source_dataset_reference, str(source_row_order), symbol, sip_timestamp, broker_trade_id])
    return "source_record:" + sha256(stable.encode("utf-8")).hexdigest()


def _economic_row_fingerprint(
    *,
    symbol: str,
    sip_timestamp: str,
    price: str,
    quantity: str,
    exchange: str,
    tape: str,
    conditions: tuple[str, ...],
    broker_trade_id: str,
) -> str:
    stable = "|".join([symbol, sip_timestamp, price, quantity, exchange, tape, ",".join(conditions), broker_trade_id])
    return "economic:" + sha256(stable.encode("utf-8")).hexdigest()


def _integrity_diagnostics(summary: SourceIntegritySummary) -> list[ReplayDiagnostic]:
    diagnostics: list[ReplayDiagnostic] = []
    if summary.duplicate_source_id_records:
        diagnostics.append(
            ReplayDiagnostic(
                "source_duplicate_id_summary",
                "Historical source contains repeated source identities.",
                details={
                    "duplicate_source_id_records": summary.duplicate_source_id_records,
                    "duplicate_rate_pct": str(summary.duplicate_rate_pct),
                },
            )
        )
    if summary.duplicate_ids_with_conflicting_values:
        diagnostics.append(
            ReplayDiagnostic(
                "conflicting_duplicate_source_ids",
                "Historical source contains repeated trade IDs with conflicting economic fields.",
                "error",
                details={"count": summary.duplicate_ids_with_conflicting_values},
            )
        )
    if summary.out_of_order_records:
        diagnostics.append(
            ReplayDiagnostic(
                "source_out_of_order_records",
                "Historical source file order is not monotonic by canonical ordering key.",
                details={"count": summary.out_of_order_records},
            )
        )
    if summary.source_kind != "raw_sip_trades":
        diagnostics.append(
            ReplayDiagnostic(
                "source_not_raw_sip",
                "Source is not raw SIP trades; source-level production parity is not verifiable.",
                details={"source_kind": summary.source_kind},
            )
        )
    return diagnostics
