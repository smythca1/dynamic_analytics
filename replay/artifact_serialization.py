from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any


def normalize_decimal_string(value: Decimal) -> str:
    if not value.is_finite():
        return str(value)
    normalized = value.normalize()
    if normalized == 0:
        return "0"
    text = format(normalized, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text == "-0" else text


def artifact_jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return artifact_jsonable(asdict(value))
    if isinstance(value, dict):
        return {str(key): artifact_jsonable(val) for key, val in value.items()}
    if isinstance(value, (list, tuple)):
        return [artifact_jsonable(item) for item in value]
    if isinstance(value, Decimal):
        return normalize_decimal_string(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def artifact_json_dumps(payload: Any, *, indent: int | None = 2) -> str:
    return json.dumps(
        artifact_jsonable(payload),
        indent=indent,
        sort_keys=True,
        separators=(",", ":") if indent is None else None,
    )


def artifact_payload_sha256(payload: Any) -> str:
    canonical = artifact_json_dumps(payload, indent=None)
    return sha256(canonical.encode("utf-8")).hexdigest()


def write_artifact_json(path: Path, payload: Any) -> str:
    text = artifact_json_dumps(payload, indent=2) + "\n"
    path.write_text(text, encoding="utf-8")
    return sha256(path.read_bytes()).hexdigest()

