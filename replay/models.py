from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

from silver.accounting import PositionState
from silver.events.models import utc_datetime
from .execution import ExecutionFriction, RawExecutionAssumptions


OutputLevel = Literal["SUMMARY", "TRADES", "AUDIT"]
ExecutionModel = Literal["NEXT_CANONICAL_OBSERVATION_V1", "RAW_SIP_NATIVE_V1"]
BrickMode = Literal["BRICK_DOLLARS", "BRICK_PERCENT", "PRODUCTION_CONFIG"]


def to_decimal(value: Decimal | int | float | str) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


@dataclass(frozen=True)
class ReplayConfig:
    run_id: str
    trader_id: str
    symbol: str
    start: datetime | str
    end: datetime | str
    timezone: str
    session_mode: str
    strategy_config: dict[str, Any]
    initial_capital: Decimal | int | float | str
    warmup_start: datetime | str | None = None
    comparison_start: datetime | str | None = None
    comparison_end: datetime | str | None = None
    brick_mode: BrickMode = "BRICK_DOLLARS"
    brick_size_dollars: Decimal | int | float | str | None = None
    brick_size_ltp_percent: Decimal | int | float | str | None = None
    derived_brick_size_ltp_percent: Decimal | int | float | str | None = None
    brick_reference_price: Decimal | int | float | str | None = None
    brick_reference_timestamp: datetime | str | None = None
    brick_reference_source: str | None = None
    brick_rounding_policy: str = "Decimal exact; no quantization in V1"
    effective_brick_size_dollars: Decimal | int | float | str | None = None
    initial_renko_state: dict[str, Any] | None = None
    initial_strategy_state: dict[str, Any] | None = None
    initial_accounting_position: PositionState | None = None
    initial_accounting_basis: Decimal | int | float | str | None = None
    start_state_path: str | None = None
    start_state_completeness: str = "not_supplied"
    execution_model: ExecutionModel = "NEXT_CANONICAL_OBSERVATION_V1"
    execution_version: str = "replay.execution.next_observation.v1"
    sampler_final_flush_policy: str = "NO_FLUSH"
    source_data_path: str = ""
    source_semantic_mode: str = "raw_sip_trades"
    source_config_path: str | None = None
    production_config: dict[str, Any] = field(default_factory=dict)
    production_evidence: dict[str, Any] = field(default_factory=dict)
    force_flat_replay_policy: str = "NOT_MODELED"
    output_directory: str = "artifacts/replays"
    output_level: OutputLevel = "SUMMARY"
    replay_version: str = "canonical_replay_v1"
    sampler_version: str = "silver.one_second_sip_sampler.v1"
    renko_version: str = "production.RenkoStrategyEngine"
    accounting_version: str = "silver.accounting.v1"
    metrics_version: str = "replay.metrics.v1"
    source_timezone: str | None = None
    execution_friction: ExecutionFriction = field(default_factory=ExecutionFriction)
    conservative_protections: bool = False
    raw_execution: RawExecutionAssumptions = field(default_factory=RawExecutionAssumptions)

    def __post_init__(self) -> None:
        object.__setattr__(self, "start", utc_datetime(self.start))
        object.__setattr__(self, "end", utc_datetime(self.end))
        if self.warmup_start is not None:
            object.__setattr__(self, "warmup_start", utc_datetime(self.warmup_start))
        if self.comparison_start is not None:
            object.__setattr__(self, "comparison_start", utc_datetime(self.comparison_start))
        if self.comparison_end is not None:
            object.__setattr__(self, "comparison_end", utc_datetime(self.comparison_end))
        object.__setattr__(self, "symbol", self.symbol.upper())
        object.__setattr__(self, "output_level", str(self.output_level).upper())
        object.__setattr__(self, "brick_mode", str(self.brick_mode).upper())
        if self.brick_size_dollars is not None:
            object.__setattr__(self, "brick_size_dollars", to_decimal(self.brick_size_dollars))
        object.__setattr__(
            self,
            "brick_size_ltp_percent",
            to_decimal(self.brick_size_ltp_percent) if self.brick_size_ltp_percent is not None else None,
        )
        object.__setattr__(
            self,
            "derived_brick_size_ltp_percent",
            to_decimal(self.derived_brick_size_ltp_percent) if self.derived_brick_size_ltp_percent is not None else None,
        )
        if self.brick_reference_price is not None:
            object.__setattr__(self, "brick_reference_price", to_decimal(self.brick_reference_price))
        if self.brick_reference_timestamp is not None:
            object.__setattr__(self, "brick_reference_timestamp", utc_datetime(self.brick_reference_timestamp))
        if self.effective_brick_size_dollars is not None:
            object.__setattr__(self, "effective_brick_size_dollars", to_decimal(self.effective_brick_size_dollars))
        object.__setattr__(self, "initial_capital", to_decimal(self.initial_capital))
        if self.end < self.start:
            raise ValueError("Replay end must be >= start.")
        if self.comparison_start is not None and self.comparison_start < self.start:
            raise ValueError("comparison_start must be >= replay start.")
        if self.comparison_end is not None and self.comparison_end > self.end:
            raise ValueError("comparison_end must be <= replay end.")
        if self.comparison_start is not None and self.comparison_end is not None and self.comparison_end < self.comparison_start:
            raise ValueError("comparison_end must be >= comparison_start.")
        if self.output_level not in {"SUMMARY", "TRADES", "AUDIT"}:
            raise ValueError("output_level must be SUMMARY, TRADES, or AUDIT.")
        if self.execution_model not in {"NEXT_CANONICAL_OBSERVATION_V1", "RAW_SIP_NATIVE_V1"}:
            raise ValueError("Unsupported replay execution model")
        if self.execution_model == "RAW_SIP_NATIVE_V1":
            object.__setattr__(self, "execution_version", "replay.execution.raw_sip_native.v1")
            object.__setattr__(self, "force_flat_replay_policy", "RESEARCH_TIMER_NEXT_RAW_PRICE")
            if self.source_semantic_mode != "raw_sip_trades":
                raise ValueError("Raw execution requires raw SIP evidence")
            if self.initial_accounting_position is not None and self.initial_accounting_position.side is not None:
                raise ValueError("Raw execution cannot reconstruct initial live protection from position alone")
            if self.initial_strategy_state is not None:
                raise ValueError("Raw execution requires fresh strategy state; restored broker lifecycle is unavailable")
        if self.brick_mode not in {"BRICK_DOLLARS", "BRICK_PERCENT", "PRODUCTION_CONFIG"}:
            raise ValueError("brick_mode must be BRICK_DOLLARS, BRICK_PERCENT, or PRODUCTION_CONFIG.")
        if self.brick_mode == "BRICK_DOLLARS":
            if self.brick_size_dollars is None or self.brick_size_ltp_percent is not None:
                raise ValueError("BRICK_DOLLARS requires brick_size_dollars and rejects brick_pct.")
        if self.brick_mode == "BRICK_PERCENT":
            if self.brick_size_ltp_percent is None or self.brick_size_dollars is not None:
                raise ValueError("BRICK_PERCENT requires brick_pct and rejects brick_size_dollars.")
        if self.brick_mode == "PRODUCTION_CONFIG":
            if self.brick_size_dollars is not None or self.brick_size_ltp_percent is not None:
                raise ValueError("PRODUCTION_CONFIG rejects explicit brick_size_dollars and brick_pct.")


@dataclass(frozen=True)
class ReplayDiagnostic:
    diagnostic_type: str
    message: str
    severity: Literal["warning", "error"] = "warning"
    timestamp: datetime | None = None
    sequence: int | None = None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SourceIntegritySummary:
    total_raw_rows: int = 0
    source_rows_scanned: int = 0
    raw_trades_in_window: int = 0
    unique_source_identities: int = 0
    exact_duplicate_rows: int = 0
    duplicate_source_id_records: int = 0
    duplicate_trade_ids: int = 0
    duplicate_ids_with_identical_values: int = 0
    duplicate_ids_with_conflicting_values: int = 0
    rows_using_fallback_identity: int = 0
    ambiguous_equal_order_records: int = 0
    out_of_order_records: int = 0
    malformed_rows: int = 0
    timestamp_range_start: datetime | None = None
    timestamp_range_end: datetime | None = None
    window_timestamp_start: datetime | None = None
    window_timestamp_end: datetime | None = None
    maximum_timestamp_gap_seconds: Decimal | None = None
    duplicate_rate_pct: Decimal = Decimal("0")
    stopped_after_requested_end: bool = False
    source_kind: str = "raw_sip_trades"


@dataclass(frozen=True)
class ReplayResult:
    context: Any
    metrics: dict[str, Any]
    manifest: dict[str, Any]
    output_directory: str
    ledgers: Any
    pending_orders: tuple[Any, ...]
    diagnostics: tuple[ReplayDiagnostic, ...]


@dataclass(frozen=True)
class ReplayStartState:
    snapshot_timestamp: datetime | None
    trader_id: str
    symbol: str
    renko_state: dict[str, Any] | None = None
    strategy_state: dict[str, Any] | None = None
    broker_position_quantity: Decimal | None = None
    accounting_position: PositionState | None = None
    accounting_basis_status: str = "UNKNOWN"
    pending_orders: tuple[dict[str, Any], ...] = ()
    force_flat_state: dict[str, Any] | None = None
    source_references: tuple[str, ...] = ()
    state_schema_version: str = "replay_start_state.v1"
    completeness: str = "incomplete"

    def __post_init__(self) -> None:
        if self.snapshot_timestamp is not None:
            object.__setattr__(self, "snapshot_timestamp", utc_datetime(self.snapshot_timestamp))
        object.__setattr__(self, "symbol", self.symbol.upper())
