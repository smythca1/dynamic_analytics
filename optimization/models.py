from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal


OPTIMIZER_VERSION = "canonical_renko_optimizer.v3"
RECOMMENDATION_RULES_VERSION = "robust_renko_recommendation.v3"
ROBUSTNESS_RULES_VERSION = "renko_neighbor_robustness.v2"
SENSITIVITY_RULES_VERSION = "renko_neighbor_sensitivity.v1"
PREPARATION_VERSION = "canonical_market_preparation.v1"

CandidateType = Literal[
    "BRICK_PERCENT_GRID",
    "CURRENT_PRODUCTION",
    "CURRENT_PRODUCTION_CAPITAL_ALLOCATED",
    "CURRENT_PRODUCTION_REVERSAL_CAPITAL_ALLOCATED",
]
CandidateStatus = Literal["COMPLETED", "FAILED", "SKIPPED"]
WarmupPolicy = Literal["RESEARCH_FLAT_AT_SCORE_START"]
PositionSizingMode = Literal["PRODUCTION_CONFIG_QUANTITY_V1", "CAPITAL_ALLOCATED_WHOLE_SHARES_V1"]
ParameterGridMode = Literal["BRICK_ONLY_V1", "RENKO_TP_SL_ENTRY_TP_SL_GRID_V1"]
EntryConfirmation = Literal["E1", "E2", "E3"]
StrategyFamily = Literal["DIRECT_REVERSAL_BASELINE", "FLATTENING_TP_SL_STRATEGY"]
StrategyTypeName = Literal["renko_reversal", "renko_tp_sl"]
HostRole = Literal["local-research", "research-server", "live-trading"]


@dataclass(frozen=True)
class SessionWindow:
    session_date: date
    start_utc: datetime
    end_utc: datetime

    def __post_init__(self) -> None:
        if self.start_utc.tzinfo is None or self.end_utc.tzinfo is None:
            raise ValueError("SessionWindow timestamps must be timezone-aware.")
        object.__setattr__(self, "start_utc", self.start_utc.astimezone(timezone.utc))
        object.__setattr__(self, "end_utc", self.end_utc.astimezone(timezone.utc))
        if self.end_utc < self.start_utc:
            raise ValueError("Session end must be after session start.")


@dataclass(frozen=True)
class TraderSelection:
    trader_id: str
    symbol: str
    strategy_name: str
    strategy_type: str
    strategy_config: dict[str, Any]
    production_metadata: dict[str, Any]
    production_config_hash: str


@dataclass(frozen=True)
class SourceFileInfo:
    symbol: str
    session_date: date
    path: str
    exists: bool
    source_type: str
    sha256: str | None = None
    row_count: int | None = None
    earliest_timestamp: str | None = None
    latest_timestamp: str | None = None
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class PreparedSource:
    preparation_id: str
    symbol: str
    session_dates: tuple[date, ...]
    source_files: tuple[SourceFileInfo, ...]
    source_combined_hash: str
    observation_count: int
    output_directory: str
    observations_path: str
    diagnostics_summary: dict[str, int]
    data_quality_status: str
    reused: bool = False
    market_stream_contract: dict[str, Any] = field(default_factory=dict)
    approved_canonical_path: bool = True
    promotable: bool = True


@dataclass(frozen=True)
class StrategyOptimizationParameters:
    strategy_family: StrategyFamily
    strategy_type: StrategyTypeName
    entry_confirmation: EntryConfirmation
    take_profit_bricks: int | None = None
    stop_loss_bricks: int | None = None

    def __post_init__(self) -> None:
        if self.strategy_family not in {"DIRECT_REVERSAL_BASELINE", "FLATTENING_TP_SL_STRATEGY"}:
            raise ValueError(f"Unsupported strategy family: {self.strategy_family}")
        if self.strategy_type not in {"renko_reversal", "renko_tp_sl"}:
            raise ValueError(f"Unsupported strategy type: {self.strategy_type}")
        if self.entry_confirmation not in {"E1", "E2", "E3"}:
            raise ValueError(f"Unsupported entry confirmation: {self.entry_confirmation}")
        if self.strategy_type == "renko_reversal":
            if self.take_profit_bricks is not None or self.stop_loss_bricks is not None:
                raise ValueError("renko_reversal optimization parameters must not carry TP/SL fields.")
            if self.strategy_family != "DIRECT_REVERSAL_BASELINE":
                raise ValueError("renko_reversal candidates must use DIRECT_REVERSAL_BASELINE.")
        if self.strategy_type == "renko_tp_sl":
            if self.strategy_family != "FLATTENING_TP_SL_STRATEGY":
                raise ValueError("renko_tp_sl candidates must use FLATTENING_TP_SL_STRATEGY.")
            for name, value in (("take_profit_bricks", self.take_profit_bricks), ("stop_loss_bricks", self.stop_loss_bricks)):
                if value not in {1, 2, 3}:
                    raise ValueError(f"{name} must be one of 1, 2, 3.")

    @property
    def entry_bricks(self) -> int:
        return {"E1": 1, "E2": 2, "E3": 3}[self.entry_confirmation]

    @property
    def take_profit_confirmation(self) -> str:
        return f"TP_E{self.take_profit_bricks}" if self.take_profit_bricks is not None else "NOT_APPLICABLE"

    @property
    def stop_loss_confirmation(self) -> str:
        return f"SL_E{self.stop_loss_bricks}" if self.stop_loss_bricks is not None else "NOT_APPLICABLE"

    @property
    def direct_reversal_enabled(self) -> bool:
        return self.strategy_type == "renko_reversal"

    @property
    def exits_flat_before_reentry(self) -> bool:
        return self.strategy_type == "renko_tp_sl"

    def identity_parts(self) -> tuple[object, ...]:
        if self.strategy_type == "renko_reversal":
            return (self.strategy_family, self.strategy_type, self.entry_bricks)
        return (
            self.strategy_family,
            self.strategy_type,
            self.entry_bricks,
            self.take_profit_bricks,
            self.stop_loss_bricks,
        )


@dataclass(frozen=True)
class OptimizationConfig:
    optimization_id: str
    production_config_path: str
    session_dates: tuple[date, ...]
    source_root: str = "artifacts/bronze/market"
    selected_trader_ids: tuple[str, ...] = ("all-enabled-renko",)
    warmup_policy: WarmupPolicy = "RESEARCH_FLAT_AT_SCORE_START"
    initial_capital: Decimal | int | float | str = Decimal("10000")
    execution_model: str = "NEXT_CANONICAL_OBSERVATION_V1"
    brick_start_pct: Decimal | int | float | str = Decimal("0.05")
    brick_end_pct: Decimal | int | float | str = Decimal("3.00")
    brick_step_pct_points: Decimal | int | float | str = Decimal("0.01")
    position_sizing_mode: PositionSizingMode = "PRODUCTION_CONFIG_QUANTITY_V1"
    parameter_grid_mode: ParameterGridMode = "BRICK_ONLY_V1"
    minimum_closed_trades: int = 10
    minimum_average_trade_pnl_per_share: Decimal | int | float | str = Decimal("0.10")
    max_drawdown_pct_threshold: Decimal | int | float | str = Decimal("25")
    local_profitable_pct_threshold: Decimal | int | float | str = Decimal("60")
    recommendation_rules_version: str = RECOMMENDATION_RULES_VERSION
    output_directory: str = "artifacts/optimizations"
    prepared_output_root: str = "artifacts/prepared_market"
    process_count: int | str = 1
    candidate_batch_size: int = 1
    max_memory_gb: Decimal | int | float | str | None = None
    nice_level: int = 0
    host_role: HostRole = "local-research"
    checkpoint_every_candidates: int = 25
    progress_every_seconds: int = 30
    resume: bool = False
    retry_failed: bool = False
    output_level: str = "SUMMARY"
    preflight_only: bool = False
    candidate_pcts: tuple[Decimal | int | float | str, ...] | None = None
    include_current_production: bool = True
    selected_entry_confirmations: tuple[str, ...] | None = None
    selected_take_profit_bricks: tuple[int, ...] | None = None
    selected_stop_loss_bricks: tuple[int, ...] | None = None
    include_disabled_research_traders: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "initial_capital", Decimal(str(self.initial_capital)))
        object.__setattr__(self, "brick_start_pct", Decimal(str(self.brick_start_pct)))
        object.__setattr__(self, "brick_end_pct", Decimal(str(self.brick_end_pct)))
        object.__setattr__(self, "brick_step_pct_points", Decimal(str(self.brick_step_pct_points)))
        object.__setattr__(self, "minimum_average_trade_pnl_per_share", Decimal(str(self.minimum_average_trade_pnl_per_share)))
        object.__setattr__(self, "max_drawdown_pct_threshold", Decimal(str(self.max_drawdown_pct_threshold)))
        object.__setattr__(self, "local_profitable_pct_threshold", Decimal(str(self.local_profitable_pct_threshold)))
        if self.max_memory_gb is not None:
            object.__setattr__(self, "max_memory_gb", Decimal(str(self.max_memory_gb)))
        if self.candidate_pcts is not None:
            object.__setattr__(self, "candidate_pcts", tuple(Decimal(str(item)) for item in self.candidate_pcts))
        if self.selected_entry_confirmations is not None:
            entries = tuple(str(item).strip().upper() for item in self.selected_entry_confirmations)
            if not entries or any(item not in {"E1", "E2", "E3"} for item in entries):
                raise ValueError("selected_entry_confirmations must contain E1, E2, and/or E3.")
            object.__setattr__(self, "selected_entry_confirmations", entries)
        if self.selected_take_profit_bricks is not None:
            values = tuple(int(item) for item in self.selected_take_profit_bricks)
            if not values or any(item not in {1, 2, 3} for item in values):
                raise ValueError("selected_take_profit_bricks must contain values from 1, 2, 3.")
            object.__setattr__(self, "selected_take_profit_bricks", values)
        if self.selected_stop_loss_bricks is not None:
            values = tuple(int(item) for item in self.selected_stop_loss_bricks)
            if not values or any(item not in {1, 2, 3} for item in values):
                raise ValueError("selected_stop_loss_bricks must contain values from 1, 2, 3.")
            object.__setattr__(self, "selected_stop_loss_bricks", values)
        object.__setattr__(self, "output_level", self.output_level.upper())
        if self.process_count != "auto":
            object.__setattr__(self, "process_count", int(self.process_count))
            if self.process_count < 1:
                raise ValueError("process_count must be >= 1.")
        if self.candidate_batch_size < 1:
            raise ValueError("candidate_batch_size must be >= 1.")
        if self.checkpoint_every_candidates < 1:
            raise ValueError("checkpoint_every_candidates must be >= 1.")
        if self.progress_every_seconds < 1:
            raise ValueError("progress_every_seconds must be >= 1.")
        if self.host_role not in {"local-research", "research-server", "live-trading"}:
            raise ValueError(f"Unsupported host role: {self.host_role}")
        if self.host_role == "live-trading" and (self.process_count == "auto" or int(self.process_count) > 1):
            raise ValueError("live-trading host role permits only low-impact one-process optimization checks.")
        if not self.session_dates:
            raise ValueError("At least one session date is required.")
        if self.brick_step_pct_points <= 0:
            raise ValueError("brick step must be > 0.")
        if self.position_sizing_mode not in {"PRODUCTION_CONFIG_QUANTITY_V1", "CAPITAL_ALLOCATED_WHOLE_SHARES_V1"}:
            raise ValueError(f"Unsupported position sizing mode: {self.position_sizing_mode}")
        if self.parameter_grid_mode not in {"BRICK_ONLY_V1", "RENKO_TP_SL_ENTRY_TP_SL_GRID_V1"}:
            raise ValueError(f"Unsupported parameter grid mode: {self.parameter_grid_mode}")

    @property
    def output_path(self) -> Path:
        root = Path(self.output_directory)
        return root if root.name == self.optimization_id else root / self.optimization_id


@dataclass(frozen=True)
class CandidateSpec:
    candidate_id: str
    candidate_type: CandidateType
    trader: TraderSelection
    brick_pct: Decimal | None
    brick_size_dollars: Decimal
    reference_price: Decimal
    reference_price_timestamp: datetime
    production_config_quantity: Decimal
    research_quantity: Decimal
    allocated_capital: Decimal
    unused_cash: Decimal
    position_sizing_mode: PositionSizingMode
    strategy_parameters: StrategyOptimizationParameters
    reference_price_policy: str = "SESSION_START_REFERENCE_V1"
    rounding_policy: str = "Decimal exact; no quantization in optimizer V1"


@dataclass(frozen=True)
class CandidateResult:
    spec: CandidateSpec
    status: CandidateStatus
    row: dict[str, Any]
    selected_output_directory: str | None = None
    error_type: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class OptimizationResult:
    config: OptimizationConfig
    traders: tuple[TraderSelection, ...]
    prepared_sources: dict[str, PreparedSource]
    candidate_rows: tuple[dict[str, Any], ...]
    recommendations: dict[str, Any]
    cross_asset_summary: dict[str, Any]
    output_directory: str
    diagnostics_summary: dict[str, int] = field(default_factory=dict)
    preflight: dict[str, Any] | None = None


def regular_session_window(session: date) -> SessionWindow:
    return SessionWindow(
        session_date=session,
        start_utc=datetime.combine(session, time(13, 30), tzinfo=timezone.utc),
        end_utc=datetime.combine(session, time(20, 0), tzinfo=timezone.utc),
    )
