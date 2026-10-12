from __future__ import annotations

import asyncio
import json
import re
import struct
from hashlib import sha256
from typing import Any


PROTOCOL_VERSION = "canonical_market_ipc.v1"
MAX_FRAME_BYTES = 4 * 1024 * 1024
CONSUMER_IDENTITY_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_.:-]{0,127}$")


class ProtocolError(ValueError):
    pass


def canonical_event_id(
    *,
    contract_fingerprint: str,
    symbol: str,
    sequence: int,
    observation_timestamp: str,
    close_price: str,
    source_record_id: str | None,
) -> str:
    payload = {
        "contract_fingerprint": contract_fingerprint,
        "symbol": symbol.upper(),
        "sequence": int(sequence),
        "observation_timestamp": observation_timestamp,
        "close_price": str(close_price),
        "source_record_id": source_record_id,
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return sha256(raw.encode("utf-8")).hexdigest()


def encode_message(message: dict[str, Any]) -> bytes:
    if message.get("version") != PROTOCOL_VERSION:
        message = {"version": PROTOCOL_VERSION, **message}
    payload = json.dumps(message, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    if len(payload) > MAX_FRAME_BYTES:
        raise ProtocolError(f"IPC frame exceeds {MAX_FRAME_BYTES} bytes.")
    return struct.pack("!I", len(payload)) + payload


async def write_message(writer: asyncio.StreamWriter, message: dict[str, Any]) -> None:
    writer.write(encode_message(message))
    await writer.drain()


async def read_message(reader: asyncio.StreamReader) -> dict[str, Any]:
    header = await reader.readexactly(4)
    (size,) = struct.unpack("!I", header)
    if size <= 0 or size > MAX_FRAME_BYTES:
        raise ProtocolError(f"Invalid IPC frame size: {size}.")
    payload = await reader.readexactly(size)
    message = json.loads(payload.decode("utf-8"))
    validate_message(message)
    return message


def validate_message(message: dict[str, Any]) -> None:
    if not isinstance(message, dict):
        raise ProtocolError("IPC message must be a JSON object.")
    if message.get("version") != PROTOCOL_VERSION:
        raise ProtocolError(f"Unsupported IPC protocol version: {message.get('version')!r}.")
    if not message.get("type"):
        raise ProtocolError("IPC message missing type.")


def validate_consumer_identity(identity: str) -> str:
    normalized = str(identity or "").strip().lower()
    if not CONSUMER_IDENTITY_PATTERN.fullmatch(normalized):
        raise ProtocolError(f"Invalid consumer identity: {identity!r}.")
    return normalized
