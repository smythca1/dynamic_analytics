from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, AsyncIterator

import pandas as pd

from trading_events import MarketTick, utc_timestamp_with_info
from .trade_filter import evaluate_market_tick_eligibility


@dataclass(frozen=True)
class LiveMarketEvent:
    tick: MarketTick
    source_record_id: str
    broker_trade_id: str | None
    source_sequence: int | None
    eligible: bool
    archive_path: str | None = None
    local_timing: dict[str, float] | None = None


def live_market_event_from_archive_payload(
    payload: dict[str, Any],
    *,
    archive_path: str | None = None,
    source_sequence: int | None = None,
) -> LiveMarketEvent:
    tick = MarketTick(
        timestamp=pd.Timestamp(payload["timestamp"]),
        symbol=str(payload["symbol"]).upper(),
        price=float(payload["price"]),
        size=float(payload.get("size", 0.0) or 0.0),
        exchange=str(payload.get("exchange", "")),
        trade_id=str(payload.get("trade_id", "")),
        tape=str(payload.get("tape", "")),
        conditions=tuple(str(condition) for condition in payload.get("conditions", []) if condition is not None),
    )
    producer_sequence = _source_sequence(payload, None)
    resolved_sequence = producer_sequence if producer_sequence is not None else source_sequence
    return LiveMarketEvent(
        tick=tick,
        source_record_id=str(payload.get("source_record_id") or _stable_source_record_id(tick, producer_sequence)),
        broker_trade_id=tick.trade_id or None,
        source_sequence=resolved_sequence,
        eligible=bool(payload.get("eligible", evaluate_market_tick_eligibility(tick))),
        archive_path=archive_path,
    )


def live_market_event_from_alpaca_message(message: dict[str, Any], *, source_sequence: int) -> LiveMarketEvent:
    timestamp, _info = utc_timestamp_with_info(message["t"])
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
    return LiveMarketEvent(
        tick=tick,
        source_record_id=_stable_source_record_id(tick, None),
        broker_trade_id=tick.trade_id or None,
        source_sequence=source_sequence,
        eligible=evaluate_market_tick_eligibility(tick),
    )


def archive_tail_path(root: str | Path, symbol: str, timestamp: pd.Timestamp | None = None) -> Path:
    day = pd.Timestamp(timestamp or pd.Timestamp.now(tz="UTC")).tz_convert("UTC").date().isoformat()
    return Path(root) / "ticks" / day / f"{symbol.upper()}.jsonl"


class LocalArchiveMarketDataTail:
    def __init__(
        self,
        *,
        root: str | Path,
        symbols: list[str],
        poll_seconds: float = 0.1,
        start_at_end: bool = True,
    ) -> None:
        self.root = Path(root)
        self.symbols = sorted({symbol.upper() for symbol in symbols})
        self.poll_seconds = max(0.01, float(poll_seconds))
        self.start_at_end = bool(start_at_end)
        self._offsets: dict[Path, int] = {}
        self._sequences: dict[str, int] = {}
        self._seen: set[str] = set()

    async def stream(self) -> AsyncIterator[LiveMarketEvent]:
        while True:
            emitted = False
            for symbol in self.symbols:
                path = archive_tail_path(self.root, symbol)
                for event in self.poll_path(path):
                    emitted = True
                    yield event
            if not emitted:
                await asyncio.sleep(self.poll_seconds)

    def poll_path(self, path: Path) -> tuple[LiveMarketEvent, ...]:
        if not path.exists():
            if path not in self._offsets:
                self._offsets[path] = 0
            return tuple()
        if path not in self._offsets:
            self._offsets[path] = path.stat().st_size if self.start_at_end else 0
        events: list[LiveMarketEvent] = []
        with path.open("r", encoding="utf-8") as handle:
            handle.seek(self._offsets[path])
            for line in handle:
                if not line.strip():
                    continue
                payload = json.loads(line)
                symbol = str(payload.get("symbol") or path.stem).upper()
                sequence = self._sequences.get(symbol, 0) + 1
                self._sequences[symbol] = sequence
                event = live_market_event_from_archive_payload(
                    payload,
                    archive_path=str(path),
                    source_sequence=sequence,
                )
                if event.source_record_id in self._seen:
                    continue
                self._seen.add(event.source_record_id)
                events.append(event)
            self._offsets[path] = handle.tell()
        return tuple(events)


def _source_sequence(payload: dict[str, Any], fallback: int | None) -> int | None:
    raw = payload.get("source_sequence")
    if raw is None:
        return fallback
    try:
        return int(raw)
    except (TypeError, ValueError):
        return fallback


def _stable_source_record_id(tick: MarketTick, source_sequence: int | None) -> str:
    payload = {
        "timestamp": tick.timestamp.isoformat(),
        "symbol": tick.symbol.upper(),
        "price": tick.price,
        "size": tick.size,
        "exchange": tick.exchange,
        "trade_id": tick.trade_id,
        "tape": tick.tape,
        "conditions": list(tick.conditions),
        "source_sequence": source_sequence,
    }
    return sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
