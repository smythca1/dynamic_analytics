from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


EVIDENCE_SCHEMA_VERSION = "production_parity_evidence.v1"
UNKNOWN = "UNKNOWN"


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def jsonable(value: Any) -> Any:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "as_integer_ratio"):
        return float(value)
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [jsonable(item) for item in value]
    if hasattr(value, "__dataclass_fields__"):
        return {key: jsonable(getattr(value, key)) for key in value.__dataclass_fields__}
    return str(value)


@dataclass(frozen=True)
class EvidenceRecord:
    schema_version: str
    evidence_type: str
    recorded_at: str
    trader_id: str
    symbol: str
    session_id: str
    process_id: str
    logical_sequence: int
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class EvidenceCheckpoint:
    checkpoint_timestamp: str
    trader_id: str
    symbol: str
    configuration_hash: str | None = None
    renko_anchor_mode: str | None = None
    renko_anchor_price: Any = None
    last_renko_close: Any = None
    run_direction: Any = None
    run_length: Any = None
    strategy_position_state: dict[str, Any] | None = None
    broker_confirmed_quantity: Any = UNKNOWN
    accounting_quantity: Any = UNKNOWN
    accounting_basis_status: str = UNKNOWN
    pending_order_identifiers: list[str] = field(default_factory=list)
    force_flat_state: dict[str, Any] | None = None
    production_sampler_state: dict[str, Any] | None = None
    shadow_sampler_state: dict[str, Any] | None = None
    component_versions: dict[str, str] = field(default_factory=dict)
    completeness: list[str] = field(default_factory=list)
    unknown_fields: list[str] = field(default_factory=list)
    canonical_sequence: Any = UNKNOWN
    canonical_event_id: str = UNKNOWN
    evidence_chain_hash: str = UNKNOWN


def stable_path(value: str | Path | None) -> str | None:
    return None if value is None else str(Path(value))
