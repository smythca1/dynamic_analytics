from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .historical_source import file_sha256
from .models import ReplayConfig


@dataclass(frozen=True)
class BronzeDatasetMetadata:
    symbol: str
    date: str
    feed: str
    requested_start: str
    requested_end: str
    actual_earliest_timestamp: str | None
    actual_latest_timestamp: str | None
    row_count: int
    source_hash: str
    retrieval_timestamp: str
    page_count: int | None
    completion_status: str
    warnings: tuple[str, ...]


def build_bronze_metadata(*, config: ReplayConfig, source_path: str | Path, date: str) -> BronzeDatasetMetadata:
    path = Path(source_path)
    source_hash = file_sha256(path)
    row_count = 0
    first_ts: str | None = None
    last_ts: str | None = None
    warnings: list[str] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row_count += 1
            try:
                row: dict[str, Any] = json.loads(line)
            except json.JSONDecodeError:
                warnings.append(f"malformed_json_line:{row_count}")
                continue
            if str(row.get("feed") or "").lower() != "sip":
                warnings.append(f"non_sip_feed_line:{row_count}")
            ts = row.get("timestamp") or row.get("t") or row.get("sip_timestamp")
            if ts:
                first_ts = first_ts or str(ts)
                last_ts = str(ts)
    return BronzeDatasetMetadata(
        symbol=config.symbol,
        date=date,
        feed="sip",
        requested_start=config.start.isoformat(),
        requested_end=config.end.isoformat(),
        actual_earliest_timestamp=first_ts,
        actual_latest_timestamp=last_ts,
        row_count=row_count,
        source_hash=source_hash,
        retrieval_timestamp=datetime.now(timezone.utc).isoformat(),
        page_count=None,
        completion_status="complete" if row_count and not any(item.startswith("malformed") for item in warnings) else "warning",
        warnings=tuple(warnings),
    )


def write_bronze_metadata(metadata: BronzeDatasetMetadata, path: str | Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(asdict(metadata), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return target
