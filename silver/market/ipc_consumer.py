from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Callable

import pandas as pd

from trading_events import MarketTick

from .ipc_protocol import read_message, write_message


@dataclass(frozen=True)
class CanonicalStreamEvent:
    payload: dict[str, Any]

    @property
    def symbol(self) -> str:
        return str(self.payload["symbol"]).upper()

    @property
    def sequence(self) -> int:
        return int(self.payload["sequence"])

    @property
    def event_id(self) -> str:
        return str(self.payload["event_id"])

    def to_market_tick(self) -> MarketTick:
        return MarketTick(
            timestamp=pd.Timestamp(self.payload["observation_timestamp"]),
            symbol=self.symbol,
            price=float(self.payload["close_price"]),
            size=1.0,
            exchange=str(self.payload.get("source_exchange") or ""),
            trade_id=self.event_id,
            tape=str(self.payload.get("source_tape") or ""),
            conditions=(),
        )


class SharedCanonicalStreamConsumer:
    def __init__(
        self,
        *,
        socket_path: str,
        identity: str,
        expected_contract_fingerprints: dict[str, str],
        state_path: str | Path,
        symbols: tuple[str, ...] | list[str] | set[str] | None = None,
        freshness_threshold_seconds: float = 5.0,
    ) -> None:
        self.socket_path = socket_path
        self.identity = identity
        self.expected_contract_fingerprints = {str(k).upper(): str(v) for k, v in expected_contract_fingerprints.items()}
        self.symbols = self._normalize_symbols(symbols)
        self.state_path = Path(state_path)
        self.freshness_threshold_seconds = float(freshness_threshold_seconds)
        self.state = self.load_state()
        self.gapped = False
        self.gapped_symbols: set[str] = set()
        self.catchup_complete = False
        self.last_observation_at: pd.Timestamp | None = None
        self.last_observation_at_by_symbol: dict[str, pd.Timestamp] = {}
        self._writer: asyncio.StreamWriter | None = None

    @staticmethod
    def _normalize_symbols(symbols: tuple[str, ...] | list[str] | set[str] | None) -> tuple[str, ...] | None:
        if symbols is None:
            return None
        normalized = tuple(dict.fromkeys(str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()))
        if not normalized:
            raise ValueError("shared canonical stream symbols must not be empty.")
        return normalized

    def load_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {"last_sequences": {}, "last_event_ids": {}}
        payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return {"last_sequences": {}, "last_event_ids": {}}
        payload.setdefault("last_sequences", {})
        payload.setdefault("last_event_ids", {})
        return payload

    def _persist_state(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        temp.write_text(json.dumps(self.state, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
        temp.replace(self.state_path)

    async def connect(self) -> AsyncIterator[CanonicalStreamEvent]:
        reader, writer = await asyncio.open_unix_connection(self.socket_path)
        self._writer = writer
        try:
            hello = {
                "type": "hello",
                "identity": self.identity,
                "expected_contract_fingerprints": self.expected_contract_fingerprints,
                "last_sequences": self.state.get("last_sequences", {}),
                "last_event_ids": self.state.get("last_event_ids", {}),
            }
            if self.symbols is not None:
                hello["symbols"] = list(self.symbols)
            await write_message(writer, hello)
            seen: set[tuple[str, int, str]] = set()
            while True:
                message = await read_message(reader)
                message_type = message.get("type")
                if message_type == "session_header":
                    continue
                if message_type == "catchup_complete":
                    self.catchup_complete = True
                    continue
                if message_type == "gap":
                    symbol = str(message.get("symbol") or "").upper().strip()
                    if symbol:
                        self.gapped_symbols.add(symbol)
                    else:
                        self.gapped = True
                    continue
                if message_type == "contract_mismatch":
                    self.gapped = True
                    raise RuntimeError(f"shared canonical stream contract mismatch: {message}")
                if message_type != "observation":
                    continue
                event = CanonicalStreamEvent(message)
                key = (event.symbol, event.sequence, event.event_id)
                if key in seen:
                    continue
                current_sequence = int(self.state.get("last_sequences", {}).get(event.symbol, 0))
                current_event_id = self.state.get("last_event_ids", {}).get(event.symbol)
                if event.sequence < current_sequence or (event.sequence == current_sequence and event.event_id == current_event_id):
                    await self.ack(event)
                    continue
                if event.sequence > current_sequence + 1 and current_sequence > 0:
                    self.gapped_symbols.add(event.symbol)
                    continue
                seen.add(key)
                observation_at = pd.Timestamp(event.payload["observation_timestamp"])
                self.last_observation_at = observation_at
                self.last_observation_at_by_symbol[event.symbol] = observation_at
                yield event
        finally:
            if self._writer is writer:
                self._writer = None
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def ack(self, event: CanonicalStreamEvent) -> None:
        self.state.setdefault("last_sequences", {})[event.symbol] = event.sequence
        self.state.setdefault("last_event_ids", {})[event.symbol] = event.event_id
        self.state["updated_at"] = pd.Timestamp.now(tz="UTC").isoformat()
        self._persist_state()
        if self._writer is not None:
            await write_message(self._writer, {"type": "ack", "symbol": event.symbol, "sequence": event.sequence, "event_id": event.event_id})

    def signal_block_reason(self, symbol: str | None = None) -> str | None:
        normalized_symbol = str(symbol or "").upper().strip() or None
        if self.gapped or (normalized_symbol and normalized_symbol in self.gapped_symbols):
            return "shared_market_data_gap"
        if not self.catchup_complete:
            return "shared_market_data_not_caught_up"
        last_observation_at = self.last_observation_at_by_symbol.get(normalized_symbol) if normalized_symbol else self.last_observation_at
        if last_observation_at is None:
            return "shared_market_data_no_observation"
        age = (pd.Timestamp.now(tz="UTC") - last_observation_at).total_seconds()
        if age > self.freshness_threshold_seconds:
            return f"shared_market_data_stale_{age:.3f}s"
        return None
