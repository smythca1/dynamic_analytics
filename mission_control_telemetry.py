from __future__ import annotations

import json
import math
import os
import re
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, time as local_time, timezone
from pathlib import Path
from typing import Any, Callable, Iterable
from zoneinfo import ZoneInfo


ALLOWED_DATA_LABELS = frozenset(
    {
        "environment",
        "fleet",
        "strategy",
        "symbol",
        "side",
        "service",
        "event_type",
    }
)
PROMETHEUS_CONTROL_LABELS = frozenset({"le"})
LABEL_ORDER = (
    "environment",
    "fleet",
    "strategy",
    "symbol",
    "side",
    "service",
    "event_type",
    "le",
)

EVENT_TYPE_BUCKETS = {
    "market_tick": "MARKET_TICK",
    "renko_brick": "BRICK_CREATED",
    "signal": "SIGNAL_GENERATED",
    "execution": "ORDER_SUBMITTED",
    "execution_error": "EXECUTION_ERROR",
    "risk_block": "STRATEGY_BLOCKED",
    "market_data_signal_block": "MARKET_DATA_BLOCKED",
    "state_reconciled": "RECONCILIATION_SUCCESS",
    "state_reconcile_error": "RECONCILIATION_FAILURE",
    "broker_native_protection_submitted": "PROTECTIVE_ORDER_SUBMITTED",
    "broker_native_protection_verified": "PROTECTIVE_ORDER_VERIFIED",
    "broker_native_protection_recovery": "PROTECTIVE_ORDER_RECOVERY",
    "reversal_catastrophic_stop_submitted": "PROTECTIVE_ORDER_SUBMITTED",
    "reversal_catastrophic_stop_recovery": "PROTECTIVE_ORDER_RECOVERY",
    "critical_protection_invariant_violation": "PROTECTION_INVARIANT_FAILURE",
    "critical_reversal_catastrophic_stop_recovery_error": "PROTECTION_INVARIANT_FAILURE",
    "critical_runtime_halt": "RUNTIME_HALTED",
    "broker_force_flat": "FORCE_FLAT",
    "duplicate_shared_canonical_event_skipped": "DUPLICATE_MARKET_EVENT",
}

EXECUTION_FILLED_STATUSES = frozenset({"filled", "partially_filled"})
EXECUTION_ERROR_STATUSES = frozenset({"rejected", "canceled", "expired", "failed", "error"})
POSITION_SIDES = ("flat", "long", "short")
HISTOGRAM_BUCKETS_SECONDS = (0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0)
NY_TIMEZONE = "America/New_York"


BrokerReaderFactory = Callable[[str | None, str | None], Any]


@dataclass(frozen=True)
class TelemetryConfig:
    environment: str
    fleet_config_paths: tuple[Path, ...]
    archive_root: Path = Path("artifacts/live")
    market_data_config_path: Path | None = None
    market_data_health_path: Path | None = None
    canonical_journal_path: Path | None = None
    event_tail_max_bytes: int = 1_048_576
    event_tail_max_lines: int = 5_000
    json_max_bytes: int = 5_242_880
    regular_session_stale_seconds: float = 15.0
    trader_heartbeat_stale_seconds: float = 60.0

    @classmethod
    def from_environment(cls) -> "TelemetryConfig":
        environment = (
            os.getenv("MISSION_CONTROL_ENVIRONMENT")
            or os.getenv("TRADING_ENVIRONMENT")
            or os.getenv("ENVIRONMENT")
            or "production"
        )
        archive_root = _path_from_env("MISSION_CONTROL_ARCHIVE_ROOT") or Path("artifacts/live")
        fleet_config_paths = _fleet_config_paths_from_environment()
        market_config_path = _path_from_env("MISSION_CONTROL_MARKET_DATA_CONFIG_PATH")
        if market_config_path is None and Path("configs/market-data.live.example.json").exists():
            market_config_path = Path("configs/market-data.live.example.json")
        return cls(
            environment=_bounded_label_value(environment),
            fleet_config_paths=fleet_config_paths,
            archive_root=archive_root,
            market_data_config_path=market_config_path,
            market_data_health_path=_path_from_env("MISSION_CONTROL_MARKET_DATA_HEALTH_PATH"),
            canonical_journal_path=_path_from_env("MISSION_CONTROL_CANONICAL_JOURNAL_PATH"),
            event_tail_max_bytes=_env_int("MISSION_CONTROL_EVENT_TAIL_MAX_BYTES", 1_048_576),
            event_tail_max_lines=_env_int("MISSION_CONTROL_EVENT_TAIL_MAX_LINES", 5_000),
            json_max_bytes=_env_int("MISSION_CONTROL_JSON_MAX_BYTES", 5_242_880),
            regular_session_stale_seconds=_env_float("MISSION_CONTROL_REGULAR_SESSION_STALE_SECONDS", 15.0),
            trader_heartbeat_stale_seconds=_env_float("MISSION_CONTROL_TRADER_HEARTBEAT_STALE_SECONDS", 60.0),
        )


@dataclass(frozen=True)
class JsonReadResult:
    payload: Any | None
    mtime_seconds: float | None = None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None


@dataclass(frozen=True)
class JsonlTailResult:
    events: tuple[dict[str, Any], ...]
    malformed_lines: int = 0
    missing: bool = False
    truncated: bool = False
    bytes_read: int = 0


@dataclass(frozen=True)
class FleetSpec:
    fleet: str
    trader_id: str
    symbol: str
    strategy: str
    strategy_type: str
    enabled: bool
    execution_enabled: bool
    mode: str
    credential_profile: str | None
    account_namespace: str
    event_log_path: Path
    state_path: Path
    session: dict[str, Any]


@dataclass
class BrokerFleetSnapshot:
    connected: bool = False
    accounts: list[dict[str, Any]] = field(default_factory=list)
    positions: list[dict[str, Any]] = field(default_factory=list)
    orders: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class EventContext:
    fleet: str
    strategy: str
    symbol: str
    side: str
    trader_id: str


class PrometheusRenderer:
    def __init__(self) -> None:
        self._definitions: dict[str, tuple[str, str]] = {}
        self._lines: list[str] = []

    def sample(
        self,
        name: str,
        value: object,
        labels: dict[str, object] | None = None,
        *,
        help_text: str,
        metric_type: str = "gauge",
    ) -> None:
        self._define(name, help_text, metric_type)
        self._lines.append(f"{name}{self._format_labels(labels or {})} {_format_metric_value(value)}")

    def histogram(
        self,
        name: str,
        observations: dict[tuple[tuple[str, str], ...], list[float]],
        *,
        help_text: str,
        buckets: Iterable[float] = HISTOGRAM_BUCKETS_SECONDS,
    ) -> None:
        self._define(name, help_text, "histogram")
        for label_tuple in sorted(observations):
            values = sorted(observations[label_tuple])
            base_labels = dict(label_tuple)
            cumulative = 0
            for bucket in buckets:
                cumulative = sum(1 for value in values if value <= bucket)
                self._lines.append(
                    f"{name}_bucket{self._format_labels({**base_labels, 'le': _bucket_label(bucket)})} {cumulative}"
                )
            self._lines.append(
                f"{name}_bucket{self._format_labels({**base_labels, 'le': '+Inf'})} {len(values)}"
            )
            self._lines.append(f"{name}_count{self._format_labels(base_labels)} {len(values)}")
            self._lines.append(f"{name}_sum{self._format_labels(base_labels)} {_format_metric_value(sum(values))}")

    def render(self) -> str:
        return "\n".join(self._lines) + "\n"

    def _define(self, name: str, help_text: str, metric_type: str) -> None:
        previous = self._definitions.get(name)
        if previous is not None:
            return
        self._definitions[name] = (help_text, metric_type)
        self._lines.append(f"# HELP {name} {help_text}")
        self._lines.append(f"# TYPE {name} {metric_type}")

    def _format_labels(self, labels: dict[str, object]) -> str:
        unknown = set(labels).difference(ALLOWED_DATA_LABELS | PROMETHEUS_CONTROL_LABELS)
        if unknown:
            raise ValueError(f"unsupported Prometheus labels: {sorted(unknown)}")
        if not labels:
            return ""
        ordered = sorted(labels.items(), key=lambda item: LABEL_ORDER.index(item[0]) if item[0] in LABEL_ORDER else 999)
        rendered = ",".join(f'{key}="{_escape_label_value(value)}"' for key, value in ordered)
        return "{" + rendered + "}"


class HistogramTracker:
    def __init__(self) -> None:
        self._observations: dict[str, dict[tuple[tuple[str, str], ...], list[float]]] = defaultdict(lambda: defaultdict(list))

    def observe(self, name: str, labels: dict[str, object], value: float) -> None:
        if not math.isfinite(value) or value < 0:
            return
        label_tuple = tuple(sorted((key, _bounded_label_value(val)) for key, val in labels.items()))
        self._observations[name][label_tuple].append(value)

    def render(self, renderer: PrometheusRenderer) -> None:
        renderer.histogram(
            "renko_execution_ack_latency_seconds",
            self._observations.get("renko_execution_ack_latency_seconds", {}),
            help_text="Order acknowledgement latency observed from bounded execution event tails.",
        )
        renderer.histogram(
            "renko_execution_fill_latency_seconds",
            self._observations.get("renko_execution_fill_latency_seconds", {}),
            help_text="Order fill latency observed only when execution events contain fill timestamps.",
        )


class BoundedJsonlTail:
    def __init__(self, *, max_bytes: int, max_lines: int) -> None:
        self.max_bytes = max(1, int(max_bytes))
        self.max_lines = max(1, int(max_lines))
        self._offsets: dict[Path, int] = {}

    def read_new(self, path: Path) -> JsonlTailResult:
        target = Path(path)
        try:
            stat_result = target.stat()
        except FileNotFoundError:
            self._offsets[target] = 0
            return JsonlTailResult(events=tuple(), missing=True)
        except OSError:
            return JsonlTailResult(events=tuple(), missing=True)

        size = stat_result.st_size
        previous_offset = self._offsets.get(target)
        truncated = False
        if previous_offset is None or previous_offset > size:
            start = max(0, size - self.max_bytes)
            truncated = start > 0
        else:
            start = previous_offset
            if size - start > self.max_bytes:
                start = max(0, size - self.max_bytes)
                truncated = True

        events: list[dict[str, Any]] = []
        malformed = 0
        bytes_read = 0
        try:
            with target.open("rb") as handle:
                handle.seek(start)
                if start > 0:
                    dropped = handle.readline()
                    bytes_read += len(dropped)
                while len(events) < self.max_lines:
                    raw = handle.readline()
                    if not raw:
                        break
                    bytes_read += len(raw)
                    line = raw.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line.decode("utf-8", errors="replace"))
                    except json.JSONDecodeError:
                        malformed += 1
                        continue
                    if isinstance(payload, dict):
                        events.append(payload)
                    else:
                        malformed += 1
                self._offsets[target] = handle.tell()
        except OSError:
            return JsonlTailResult(events=tuple(), missing=True, truncated=truncated, bytes_read=bytes_read)

        return JsonlTailResult(
            events=tuple(events),
            malformed_lines=malformed,
            missing=False,
            truncated=truncated,
            bytes_read=bytes_read,
        )


class MissionControlTelemetryCollector:
    def __init__(self, config: TelemetryConfig) -> None:
        self.config = config
        self._event_tail = BoundedJsonlTail(
            max_bytes=config.event_tail_max_bytes,
            max_lines=config.event_tail_max_lines,
        )
        self._journal_tail = BoundedJsonlTail(
            max_bytes=config.event_tail_max_bytes,
            max_lines=config.event_tail_max_lines,
        )
        self._activity_counters: Counter[tuple[str, str, str, str, str, str]] = Counter()
        self._signal_counters: Counter[tuple[str, str, str, str]] = Counter()
        self._orders_submitted_counters: Counter[tuple[str, str, str, str]] = Counter()
        self._orders_filled_counters: Counter[tuple[str, str, str, str]] = Counter()
        self._strategy_block_counters: Counter[tuple[str, str, str, str]] = Counter()
        self._execution_error_counters: Counter[tuple[str, str, str, str]] = Counter()
        self._reconciliation_failure_counters: Counter[tuple[str, str, str]] = Counter()
        self._jsonl_malformed_counters: Counter[str] = Counter()
        self._histograms = HistogramTracker()
        self._latest_event_by_trader: dict[str, float] = {}
        self._latest_brick_by_trader: dict[str, float] = {}
        self._latest_reconciliation_success_by_trader: dict[str, float] = {}
        self._latest_canonical_by_symbol: dict[str, dict[str, Any]] = {}

    def collect_prometheus(
        self,
        *,
        broker_reader_for: BrokerReaderFactory | None = None,
        now: datetime | None = None,
    ) -> str:
        started = time.perf_counter()
        now_utc = _coerce_utc_datetime(now)
        renderer = PrometheusRenderer()
        source_errors: Counter[tuple[tuple[str, str], ...]] = Counter()
        try:
            self._collect(renderer, source_errors, broker_reader_for=broker_reader_for, now=now_utc)
            renderer.sample(
                "renko_telemetry_collector_up",
                1,
                {"environment": self.config.environment, "service": "collector"},
                help_text="Mission Control telemetry collector completed without an unhandled exception.",
            )
        except Exception:
            source_errors[_label_tuple({"environment": self.config.environment, "service": "collector"})] = 1
            renderer.sample(
                "renko_telemetry_collector_up",
                0,
                {"environment": self.config.environment, "service": "collector"},
                help_text="Mission Control telemetry collector completed without an unhandled exception.",
            )
        finally:
            for labels_tuple, value in sorted(source_errors.items()):
                renderer.sample(
                    "renko_telemetry_source_error",
                    value,
                    dict(labels_tuple),
                    help_text="Read-only telemetry source error state by bounded source.",
                )
            renderer.sample(
                "renko_telemetry_scrape_duration_seconds",
                time.perf_counter() - started,
                {"environment": self.config.environment, "service": "collector"},
                help_text="Wall-clock duration of the Mission Control metrics scrape.",
            )
        return renderer.render()

    def _collect(
        self,
        renderer: PrometheusRenderer,
        source_errors: Counter[tuple[tuple[str, str], ...]],
        *,
        broker_reader_for: BrokerReaderFactory | None,
        now: datetime,
    ) -> None:
        market_open = is_regular_market_session_open(now)
        renderer.sample(
            "renko_market_session_open",
            int(market_open),
            {"environment": self.config.environment, "service": "market_calendar"},
            help_text="NYSE regular-session approximation used for freshness/readiness gating.",
        )

        market_paths = self._market_data_paths(renderer, source_errors)
        health = self._load_market_health(renderer, source_errors, market_paths.health_path)
        self._update_canonical_journal(renderer, source_errors, market_paths.journal_path)

        specs = self._load_fleet_specs(renderer, source_errors)
        specs_by_trader = {spec.trader_id: spec for spec in specs}
        event_paths = sorted({spec.event_log_path for spec in specs})
        self._update_event_counters(renderer, source_errors, event_paths, specs_by_trader)

        broker_snapshots = self._collect_broker_snapshots(
            renderer,
            source_errors,
            specs,
            broker_reader_for=broker_reader_for,
            now=now,
        )
        self._emit_market_data_metrics(renderer, health, specs, market_open=market_open, now=now)
        readiness_inputs = self._emit_trader_metrics(
            renderer,
            source_errors,
            specs,
            broker_snapshots,
            market_open=market_open,
            now=now,
        )
        self._emit_broker_and_portfolio_metrics(renderer, broker_snapshots, specs)
        self._emit_activity_metrics(renderer)
        self._emit_readiness(renderer, specs, broker_snapshots, readiness_inputs, market_open=market_open, health=health, now=now)
        self._histograms.render(renderer)

    def _market_data_paths(
        self,
        renderer: PrometheusRenderer,
        source_errors: Counter[tuple[tuple[str, str], ...]],
    ) -> "MarketDataPaths":
        health_path = self.config.market_data_health_path
        journal_path = self.config.canonical_journal_path
        config_path = self.config.market_data_config_path
        if (health_path is None or journal_path is None) and config_path is not None:
            result = _read_json_document(config_path, max_bytes=self.config.json_max_bytes)
            labels = {"environment": self.config.environment, "service": "market_data_config"}
            source_errors[_label_tuple(labels)] = 0 if result.ok else 1
            if result.ok:
                renderer.sample(
                    "renko_telemetry_source_last_success_timestamp_seconds",
                    result.mtime_seconds,
                    labels,
                    help_text="Unix timestamp for the last successful bounded telemetry source read.",
                )
                payload = result.payload if isinstance(result.payload, dict) else {}
                if health_path is None and payload.get("health_path"):
                    health_path = Path(str(payload["health_path"]))
                if journal_path is None and payload.get("journal_path"):
                    journal_path = Path(str(payload["journal_path"]))
            else:
                renderer.sample(
                    "renko_telemetry_source_last_success_timestamp_seconds",
                    math.nan,
                    labels,
                    help_text="Unix timestamp for the last successful bounded telemetry source read.",
                )
        return MarketDataPaths(health_path=health_path, journal_path=journal_path)

    def _load_market_health(
        self,
        renderer: PrometheusRenderer,
        source_errors: Counter[tuple[tuple[str, str], ...]],
        path: Path | None,
    ) -> dict[str, Any] | None:
        labels = {"environment": self.config.environment, "service": "market_data_health"}
        if path is None:
            source_errors[_label_tuple(labels)] = 1
            renderer.sample(
                "renko_telemetry_source_last_success_timestamp_seconds",
                math.nan,
                labels,
                help_text="Unix timestamp for the last successful bounded telemetry source read.",
            )
            return None
        result = _read_json_document(path, max_bytes=self.config.json_max_bytes)
        source_errors[_label_tuple(labels)] = 0 if result.ok else 1
        renderer.sample(
            "renko_telemetry_source_last_success_timestamp_seconds",
            result.mtime_seconds if result.ok else math.nan,
            labels,
            help_text="Unix timestamp for the last successful bounded telemetry source read.",
        )
        return result.payload if result.ok and isinstance(result.payload, dict) else None

    def _update_canonical_journal(
        self,
        renderer: PrometheusRenderer,
        source_errors: Counter[tuple[tuple[str, str], ...]],
        path: Path | None,
    ) -> None:
        labels = {"environment": self.config.environment, "service": "canonical_journal"}
        if path is None:
            source_errors[_label_tuple(labels)] = 1
            return
        result = self._journal_tail.read_new(path)
        source_errors[_label_tuple(labels)] = 1 if result.missing or result.malformed_lines else 0
        self._jsonl_malformed_counters["canonical_journal"] += result.malformed_lines
        renderer.sample(
            "renko_telemetry_jsonl_tail_truncated",
            int(result.truncated),
            labels,
            help_text="Whether a bounded JSONL source had unread bytes skipped to enforce scrape bounds.",
        )
        renderer.sample(
            "renko_telemetry_jsonl_bytes_read",
            result.bytes_read,
            labels,
            help_text="Bytes read from a bounded JSONL source during this scrape.",
        )
        invalid_records = 0
        for row in result.events:
            event = _unwrap_canonical_journal_event(row)
            if not event:
                invalid_records += 1
                continue
            symbol = str(event.get("symbol") or "").upper()
            if not symbol:
                invalid_records += 1
                continue
            current_sequence = int(_safe_float(self._latest_canonical_by_symbol.get(symbol, {}).get("sequence"), 0))
            sequence = int(_safe_float(event.get("sequence"), current_sequence))
            if sequence >= current_sequence:
                self._latest_canonical_by_symbol[symbol] = event
        if invalid_records:
            self._jsonl_malformed_counters["canonical_journal"] += invalid_records
            source_errors[_label_tuple(labels)] = 1
        renderer.sample(
            "renko_telemetry_malformed_jsonl_lines_total",
            self._jsonl_malformed_counters["canonical_journal"],
            labels,
            help_text="Malformed JSONL lines observed by the telemetry collector.",
            metric_type="counter",
        )

    def _load_fleet_specs(
        self,
        renderer: PrometheusRenderer,
        source_errors: Counter[tuple[tuple[str, str], ...]],
    ) -> list[FleetSpec]:
        specs: list[FleetSpec] = []
        labels = {"environment": self.config.environment, "service": "fleet_config"}
        any_error = False
        last_success: float | None = None
        for path in self.config.fleet_config_paths:
            result = _read_json_document(path, max_bytes=self.config.json_max_bytes)
            any_error = any_error or not result.ok
            if not result.ok or not isinstance(result.payload, dict):
                continue
            if result.mtime_seconds is not None:
                last_success = max(last_success or 0.0, result.mtime_seconds)
            specs.extend(self._fleet_specs_from_config(result.payload))
        source_errors[_label_tuple(labels)] = 1 if any_error else 0
        renderer.sample(
            "renko_telemetry_source_last_success_timestamp_seconds",
            last_success if last_success is not None else math.nan,
            labels,
            help_text="Unix timestamp for the last successful bounded telemetry source read.",
        )
        return specs

    def _fleet_specs_from_config(self, config: dict[str, Any]) -> list[FleetSpec]:
        fleet = _bounded_label_value(config.get("fleet_id") or config.get("experiment_id") or "unknown")
        runtime_roots = config.get("runtime_roots") if isinstance(config.get("runtime_roots"), dict) else {}
        state_root = Path(str(runtime_roots.get("state"))) if runtime_roots.get("state") else self.config.archive_root / "state"
        specs: list[FleetSpec] = []
        for trader in config.get("traders", []):
            if not isinstance(trader, dict):
                continue
            trader_id = str(trader.get("id") or "unknown")
            symbol = str(trader.get("symbol") or "").upper()
            strategy = _bounded_label_value(trader.get("strategy_name") or trader_id)
            strategy_config = trader.get("strategy") if isinstance(trader.get("strategy"), dict) else {}
            credential_profile = trader.get("credential_profile")
            account_namespace = (
                trader.get("account_namespace")
                or trader.get("account_alias")
                or credential_profile
                or config.get("account_namespace")
                or "default"
            )
            specs.append(
                FleetSpec(
                    fleet=fleet,
                    trader_id=trader_id,
                    symbol=symbol,
                    strategy=strategy,
                    strategy_type=str(strategy_config.get("type") or "unknown"),
                    enabled=bool(trader.get("enabled", True)),
                    execution_enabled=bool(trader.get("execution_enabled", True)),
                    mode=str(trader.get("mode") or os.getenv("ALPACA_TRADING_MODE") or "paper").lower(),
                    credential_profile=str(credential_profile) if credential_profile not in {None, ""} else None,
                    account_namespace=_bounded_label_value(account_namespace),
                    event_log_path=Path(str(trader.get("event_log") or os.getenv("LIVE_EVENT_LOG") or "artifacts/live/live_events.jsonl")),
                    state_path=state_root / f"{_slug(trader_id)}.json",
                    session=trader.get("session") if isinstance(trader.get("session"), dict) else {},
                )
            )
        return specs

    def _update_event_counters(
        self,
        renderer: PrometheusRenderer,
        source_errors: Counter[tuple[tuple[str, str], ...]],
        paths: list[Path],
        specs_by_trader: dict[str, FleetSpec],
    ) -> None:
        labels = {"environment": self.config.environment, "service": "event_log"}
        any_error = False
        any_truncated = False
        total_bytes_read = 0
        for path in paths:
            result = self._event_tail.read_new(path)
            any_error = any_error or result.missing or result.malformed_lines > 0
            any_truncated = any_truncated or result.truncated
            total_bytes_read += result.bytes_read
            self._jsonl_malformed_counters["event_log"] += result.malformed_lines
            for event in result.events:
                self._record_event(event, specs_by_trader)
        source_errors[_label_tuple(labels)] = 1 if any_error else 0
        renderer.sample(
            "renko_telemetry_jsonl_tail_truncated",
            int(any_truncated),
            labels,
            help_text="Whether a bounded JSONL source had unread bytes skipped to enforce scrape bounds.",
        )
        renderer.sample(
            "renko_telemetry_jsonl_bytes_read",
            total_bytes_read,
            labels,
            help_text="Bytes read from a bounded JSONL source during this scrape.",
        )
        renderer.sample(
            "renko_telemetry_malformed_jsonl_lines_total",
            self._jsonl_malformed_counters["event_log"],
            labels,
            help_text="Malformed JSONL lines observed by the telemetry collector.",
            metric_type="counter",
        )

    def _record_event(self, event: dict[str, Any], specs_by_trader: dict[str, FleetSpec]) -> None:
        event_type = str(event.get("event_type") or "unknown")
        bucket = EVENT_TYPE_BUCKETS.get(event_type, "OTHER")
        context = self._event_context(event, specs_by_trader)
        self._activity_counters[
            (
                self.config.environment,
                context.fleet,
                context.strategy,
                context.symbol,
                context.side,
                bucket,
            )
        ] += 1
        timestamp = _parse_timestamp_seconds(event.get("logged_at"))
        if timestamp is not None and context.trader_id:
            self._latest_event_by_trader[context.trader_id] = max(
                timestamp,
                self._latest_event_by_trader.get(context.trader_id, 0.0),
            )
        if event_type == "renko_brick" and timestamp is not None and context.trader_id:
            self._latest_brick_by_trader[context.trader_id] = max(
                timestamp,
                self._latest_brick_by_trader.get(context.trader_id, 0.0),
            )
        if event_type == "state_reconciled" and timestamp is not None and context.trader_id:
            self._latest_reconciliation_success_by_trader[context.trader_id] = max(
                timestamp,
                self._latest_reconciliation_success_by_trader.get(context.trader_id, 0.0),
            )
        if event_type == "state_reconcile_error":
            self._reconciliation_failure_counters[(self.config.environment, context.fleet, context.symbol)] += 1
        if event_type == "signal":
            self._signal_counters[(context.fleet, context.strategy, context.symbol, context.side)] += 1
        elif event_type == "execution":
            self._orders_submitted_counters[(context.fleet, context.strategy, context.symbol, context.side)] += 1
            self._record_execution_event(event, context)
        elif event_type in {"risk_block", "market_data_signal_block"}:
            self._strategy_block_counters[(context.fleet, context.strategy, context.symbol, context.side)] += 1
        elif event_type == "execution_error":
            self._execution_error_counters[(context.fleet, context.strategy, context.symbol, context.side)] += 1

    def _record_execution_event(self, event: dict[str, Any], context: EventContext) -> None:
        payload = _event_payload(event)
        status = str(payload.get("status") or "").lower()
        if status in EXECUTION_FILLED_STATUSES:
            self._orders_filled_counters[(context.fleet, context.strategy, context.symbol, context.side)] += 1
        if status in EXECUTION_ERROR_STATUSES:
            self._execution_error_counters[(context.fleet, context.strategy, context.symbol, context.side)] += 1
        labels = {
            "environment": self.config.environment,
            "fleet": context.fleet,
            "strategy": context.strategy,
            "symbol": context.symbol,
            "side": context.side,
        }
        event_timestamp = _parse_timestamp_seconds(payload.get("timestamp")) or _parse_timestamp_seconds(event.get("logged_at"))
        intent = payload.get("intent") if isinstance(payload.get("intent"), dict) else {}
        intent_timestamp = _parse_timestamp_seconds(intent.get("timestamp"))
        if event_timestamp is not None and intent_timestamp is not None:
            self._histograms.observe("renko_execution_ack_latency_seconds", labels, event_timestamp - intent_timestamp)
        response = payload.get("response") if isinstance(payload.get("response"), dict) else {}
        fill_timestamp = _parse_timestamp_seconds(response.get("filled_at"))
        if fill_timestamp is not None and intent_timestamp is not None:
            self._histograms.observe("renko_execution_fill_latency_seconds", labels, fill_timestamp - intent_timestamp)

    def _event_context(self, event: dict[str, Any], specs_by_trader: dict[str, FleetSpec]) -> EventContext:
        payload = _event_payload(event)
        intent = payload.get("intent") if isinstance(payload.get("intent"), dict) else {}
        trader_id = str(payload.get("trader_id") or intent.get("trader_id") or "")
        spec = specs_by_trader.get(trader_id)
        symbol = str(intent.get("symbol") or payload.get("symbol") or (spec.symbol if spec else "") or "unknown").upper()
        if spec is None and symbol != "unknown":
            candidates = [candidate for candidate in specs_by_trader.values() if candidate.symbol == symbol]
            if len(candidates) == 1:
                spec = candidates[0]
                trader_id = spec.trader_id
        side = _normalize_side(intent.get("side") or payload.get("side"))
        return EventContext(
            fleet=spec.fleet if spec else "unknown",
            strategy=spec.strategy if spec else _bounded_label_value(intent.get("strategy") or payload.get("strategy") or "unknown"),
            symbol=symbol or "unknown",
            side=side,
            trader_id=trader_id,
        )

    def _collect_broker_snapshots(
        self,
        renderer: PrometheusRenderer,
        source_errors: Counter[tuple[tuple[str, str], ...]],
        specs: list[FleetSpec],
        *,
        broker_reader_for: BrokerReaderFactory | None,
        now: datetime,
    ) -> dict[str, BrokerFleetSnapshot]:
        grouped: dict[str, set[tuple[str, str | None]]] = defaultdict(set)
        for spec in specs:
            if spec.enabled:
                grouped[spec.fleet].add((spec.mode, spec.credential_profile))
        snapshots: dict[str, BrokerFleetSnapshot] = {fleet: BrokerFleetSnapshot() for fleet in grouped}
        for fleet, account_keys in sorted(grouped.items()):
            snapshot = snapshots[fleet]
            labels = {"environment": self.config.environment, "fleet": fleet, "service": "broker"}
            if broker_reader_for is None:
                source_errors[_label_tuple(labels)] = 1
                continue
            connected = True
            for mode, profile in sorted(account_keys, key=lambda item: (item[0], item[1] or "")):
                try:
                    reader = broker_reader_for(mode, profile)
                    snapshot.accounts.append(reader.get_account())
                    snapshot.positions.extend(reader.get_positions())
                    snapshot.orders.extend(reader.get_orders(status="open", limit=100))
                except Exception:
                    connected = False
            snapshot.connected = connected
            source_errors[_label_tuple(labels)] = 0 if connected else 1
            renderer.sample(
                "renko_telemetry_source_last_success_timestamp_seconds",
                now.timestamp() if connected else math.nan,
                labels,
                help_text="Unix timestamp for the last successful bounded telemetry source read.",
            )
        return snapshots

    def _emit_market_data_metrics(
        self,
        renderer: PrometheusRenderer,
        health: dict[str, Any] | None,
        specs: list[FleetSpec],
        *,
        market_open: bool,
        now: datetime,
    ) -> None:
        labels = {"environment": self.config.environment, "service": "shared_market_data"}
        health_payload = health or {}
        renderer.sample(
            "renko_sip_connected",
            int(bool(health_payload.get("sip_connected"))),
            labels,
            help_text="Shared SIP upstream connection state from shared market-data health JSON.",
        )
        renderer.sample(
            "renko_sip_reconnects_total",
            int(_safe_float(health_payload.get("upstream_reconnect_count"), 0)),
            labels,
            help_text="Shared SIP upstream reconnect count from shared market-data health JSON.",
            metric_type="counter",
        )
        gap_count = int(_safe_float(health_payload.get("gap_count"), 0))
        consumer_gaps = _consumer_gap_count(health_payload)
        renderer.sample(
            "renko_market_data_gap_detected",
            int(gap_count > 0 or consumer_gaps > 0),
            labels,
            help_text="Whether shared market-data health reports any canonical or consumer gap.",
        )

        symbols = sorted({spec.symbol for spec in specs if spec.symbol} | set(_dict_keys(health_payload.get("raw_trade_count_by_symbol"))) | set(self._latest_canonical_by_symbol))
        raw_counts = health_payload.get("raw_trade_count_by_symbol") if isinstance(health_payload.get("raw_trade_count_by_symbol"), dict) else {}
        last_raw = health_payload.get("last_raw_trade_by_symbol") if isinstance(health_payload.get("last_raw_trade_by_symbol"), dict) else {}
        age_by_symbol = health_payload.get("observation_age_seconds") if isinstance(health_payload.get("observation_age_seconds"), dict) else {}
        last_observation = (
            health_payload.get("last_canonical_observation_by_symbol")
            if isinstance(health_payload.get("last_canonical_observation_by_symbol"), dict)
            else {}
        )
        for symbol in symbols:
            symbol_labels = {"environment": self.config.environment, "symbol": symbol, "service": "shared_market_data"}
            renderer.sample(
                "renko_sip_ticks_total",
                int(_safe_float(raw_counts.get(symbol), 0)),
                symbol_labels,
                help_text="Raw SIP trade count by symbol from shared market-data health JSON.",
                metric_type="counter",
            )
            renderer.sample(
                "renko_sip_last_event_timestamp_seconds",
                _parse_timestamp_seconds(last_raw.get(symbol)) or math.nan,
                symbol_labels,
                help_text="Last raw SIP trade timestamp by symbol.",
            )
            canonical = self._latest_canonical_by_symbol.get(symbol, {})
            latest_observation_seconds = _parse_timestamp_seconds(
                canonical.get("observation_timestamp") or last_observation.get(symbol)
            )
            observation_age = _safe_float(age_by_symbol.get(symbol), math.nan)
            if not math.isfinite(observation_age) and latest_observation_seconds is not None:
                observation_age = max(0.0, now.timestamp() - latest_observation_seconds)
            stale = int(market_open and (not math.isfinite(observation_age) or observation_age > self.config.regular_session_stale_seconds))
            renderer.sample(
                "renko_canonical_observations_total",
                int(_safe_float(canonical.get("sequence"), 0)) if canonical else math.nan,
                symbol_labels,
                help_text="Latest canonical observation sequence by symbol from bounded canonical journal tail.",
                metric_type="counter",
            )
            renderer.sample(
                "renko_canonical_last_observation_timestamp_seconds",
                latest_observation_seconds if latest_observation_seconds is not None else math.nan,
                symbol_labels,
                help_text="Last canonical observation timestamp by symbol.",
            )
            renderer.sample(
                "renko_canonical_observation_age_seconds",
                observation_age,
                symbol_labels,
                help_text="Age of the last canonical observation by symbol.",
            )
            renderer.sample(
                "renko_market_data_stale",
                stale,
                symbol_labels,
                help_text="Market-data freshness failure, gated off outside the regular market session.",
            )
            renderer.sample(
                "renko_last_price",
                _safe_float(canonical.get("close_price"), math.nan),
                symbol_labels,
                help_text="Last canonical close price from bounded canonical journal tail.",
            )

    def _emit_trader_metrics(
        self,
        renderer: PrometheusRenderer,
        source_errors: Counter[tuple[tuple[str, str], ...]],
        specs: list[FleetSpec],
        broker_snapshots: dict[str, BrokerFleetSnapshot],
        *,
        market_open: bool,
        now: datetime,
    ) -> dict[str, dict[str, bool]]:
        readiness: dict[str, dict[str, bool]] = {}
        for spec in specs:
            labels = _strategy_labels(self.config.environment, spec)
            state_result = _read_json_document(spec.state_path, max_bytes=self.config.json_max_bytes)
            state_error_labels = {**labels, "service": "state_snapshot"}
            source_errors[_label_tuple(state_error_labels)] = 0 if state_result.ok else 1
            renderer.sample(
                "renko_telemetry_source_last_success_timestamp_seconds",
                state_result.mtime_seconds if state_result.ok else math.nan,
                state_error_labels,
                help_text="Unix timestamp for the last successful bounded telemetry source read.",
            )
            state = state_result.payload if state_result.ok and isinstance(state_result.payload, dict) else {}
            state_valid = _strategy_state_valid(spec, state)
            engine = state.get("engine") if isinstance(state.get("engine"), dict) else {}
            trade = engine.get("trade") if isinstance(engine.get("trade"), dict) else {}
            internal_position = trade.get("position") if isinstance(trade.get("position"), dict) else None
            internal_side = _position_side(internal_position)
            internal_qty = _safe_float(internal_position.get("qty") if internal_position else 0.0, 0.0)
            heartbeat = _parse_timestamp_seconds(state.get("saved_at"))
            latest_event = self._latest_event_by_trader.get(spec.trader_id)
            if latest_event is not None:
                heartbeat = max(heartbeat or 0.0, latest_event)
            heartbeat_age = max(0.0, now.timestamp() - heartbeat) if heartbeat else math.inf
            heartbeat_ok = bool(heartbeat) and (not market_open or heartbeat_age <= self.config.trader_heartbeat_stale_seconds)
            reconciliation_unsafe = bool(trade.get("reconciliation_unsafe"))
            broker_snapshot = broker_snapshots.get(spec.fleet, BrokerFleetSnapshot())
            broker_position = _broker_position_for_symbol(broker_snapshot.positions, spec.symbol)
            broker_orders = _broker_orders_for_symbol(broker_snapshot.orders, spec.symbol)
            position_match = _positions_match(internal_position, broker_position) if broker_snapshot.connected else False
            pending_orders = len(broker_orders) if broker_snapshot.connected else math.nan
            protective_present = int(any(_looks_like_protective_order(order) for order in broker_orders)) if broker_snapshot.connected else 0
            strategy_armed = int(
                spec.enabled
                and spec.execution_enabled
                and state_valid
                and not reconciliation_unsafe
                and is_trader_entry_session_active(spec, now)
            )

            renderer.sample("renko_trader_up", int(spec.enabled and state_valid and heartbeat_ok), labels, help_text="Trader heartbeat health from state snapshots and bounded event tails.")
            renderer.sample("renko_trader_last_heartbeat_timestamp_seconds", heartbeat if heartbeat else math.nan, labels, help_text="Last trader heartbeat timestamp from state snapshot or bounded event tail.")
            renderer.sample("renko_strategy_armed", strategy_armed, labels, help_text="Read-only view of whether strategy config and session gates are armed.")
            renderer.sample("renko_strategy_state_valid", int(state_valid), labels, help_text="Strategy snapshot matches configured trader, symbol, strategy type, and brick size where applicable.")
            renderer.sample("renko_execution_enabled", int(spec.execution_enabled), labels, help_text="Configured execution_enabled flag.")
            for side in POSITION_SIDES:
                renderer.sample("renko_position_state", int(internal_side == side), {**labels, "side": side}, help_text="One-hot internal strategy position side from persisted state.")
            renderer.sample("renko_position_quantity", abs(internal_qty), labels, help_text="Internal strategy position quantity from persisted state.")
            renderer.sample("renko_position_entry_price", _safe_float(internal_position.get("entry_price") if internal_position else math.nan, math.nan), labels, help_text="Internal strategy position entry price from persisted state.")
            renderer.sample("renko_position_unrealized_pnl", _safe_float(broker_position.get("unrealized_pl") if broker_position else 0.0, math.nan) if broker_snapshot.connected else math.nan, labels, help_text="Broker-reported unrealized PnL for the trader symbol.")
            renderer.sample("renko_reconciliation_status", int(position_match and not reconciliation_unsafe), labels, help_text="Read-only internal-vs-broker position match status.")
            renderer.sample("renko_pending_orders", pending_orders, labels, help_text="Broker open order count by trader symbol.")
            renderer.sample("renko_protective_orders_present", protective_present, labels, help_text="Presence of broker open protective orders by symbol; this does not call protection reconciliation.")
            renderer.sample("renko_internal_vs_broker_position_match", int(position_match), labels, help_text="Read-only internal persisted position versus broker position match.")
            renderer.sample("renko_orphan_broker_position_detected", int(broker_snapshot.connected and broker_position is not None and internal_position is None), labels, help_text="Broker position exists while persisted internal strategy position is flat or missing.")
            renderer.sample("renko_orphan_internal_position_detected", int(broker_snapshot.connected and broker_position is None and internal_position is not None), labels, help_text="Persisted internal strategy position exists while broker position is flat.")
            renderer.sample("renko_reconciliation_last_success_timestamp_seconds", self._latest_reconciliation_success_by_trader.get(spec.trader_id, math.nan), labels, help_text="Latest reconciliation success timestamp seen in bounded event tails.")

            renko = engine.get("renko") if isinstance(engine.get("renko"), dict) else {}
            if spec.strategy_type.startswith("renko"):
                renderer.sample("renko_last_brick_price", _safe_float(renko.get("last_brick_close"), math.nan), labels, help_text="Last Renko brick close from persisted strategy state.")
                renderer.sample("renko_last_brick_timestamp_seconds", self._latest_brick_by_trader.get(spec.trader_id, math.nan), labels, help_text="Last Renko brick timestamp observed in bounded event tails.")
                renderer.sample("renko_bricks_total", int(_safe_float(renko.get("processed_bricks"), 0)), labels, help_text="Processed Renko brick count from persisted strategy state.", metric_type="counter")
                renderer.sample("renko_brick_direction", int(_safe_float(renko.get("run_dir"), 0)), labels, help_text="Current Renko run direction from persisted strategy state: -1 short, 0 none, 1 long.")

            readiness[spec.trader_id] = {
                "state_valid": bool(state_valid),
                "heartbeat_ok": heartbeat_ok,
                "position_match": bool(position_match),
                "reconciliation_safe": not reconciliation_unsafe,
            }
        return readiness

    def _emit_broker_and_portfolio_metrics(
        self,
        renderer: PrometheusRenderer,
        broker_snapshots: dict[str, BrokerFleetSnapshot],
        specs: list[FleetSpec],
    ) -> None:
        fleets = sorted({spec.fleet for spec in specs} | set(broker_snapshots))
        for fleet in fleets:
            labels = {"environment": self.config.environment, "fleet": fleet, "service": "broker"}
            snapshot = broker_snapshots.get(fleet, BrokerFleetSnapshot())
            account = _aggregate_accounts(snapshot.accounts)
            renderer.sample("renko_broker_connected", int(snapshot.connected), labels, help_text="Whether all read-only broker GET calls for the fleet succeeded.")
            renderer.sample("renko_broker_account_equity", account.get("equity", math.nan), labels, help_text="Broker account equity, aggregated by fleet.")
            renderer.sample("renko_broker_buying_power", account.get("buying_power", math.nan), labels, help_text="Broker account buying power, aggregated by fleet.")
            renderer.sample("renko_broker_cash", account.get("cash", math.nan), labels, help_text="Broker account cash, aggregated by fleet.")
            renderer.sample("renko_broker_daytrading_buying_power", account.get("daytrading_buying_power", math.nan), labels, help_text="Broker day-trading buying power, aggregated by fleet.")
            renderer.sample("renko_broker_margin_used", account.get("margin_used", math.nan), labels, help_text="Derived broker margin used as max(gross exposure minus equity, 0).")
            renderer.sample("renko_broker_open_orders", len(snapshot.orders) if snapshot.connected else math.nan, labels, help_text="Broker open order count by fleet.")
            renderer.sample("renko_portfolio_total_equity", account.get("equity", math.nan), labels, help_text="Portfolio total equity from broker account.")
            renderer.sample("renko_portfolio_gross_exposure", account.get("gross_exposure", math.nan), labels, help_text="Portfolio gross exposure from broker account long and short market values.")
            renderer.sample("renko_portfolio_net_exposure", account.get("net_exposure", math.nan), labels, help_text="Portfolio net exposure from broker account long minus short market values.")
            renderer.sample("renko_portfolio_cash", account.get("cash", math.nan), labels, help_text="Portfolio cash from broker account.")
            renderer.sample("renko_portfolio_unrealized_pnl", _sum_unrealized_pnl(snapshot.positions) if snapshot.connected else math.nan, labels, help_text="Portfolio unrealized PnL from broker positions.")
            renderer.sample("renko_portfolio_realized_pnl_today", math.nan, labels, help_text="Intentionally NaN until live canonical realized PnL exists; no fabricated realized PnL.")
            accounting_labels = {"environment": self.config.environment, "fleet": fleet, "service": "canonical_accounting"}
            renderer.sample("renko_telemetry_source_error", 1, accounting_labels, help_text="Read-only telemetry source error state by bounded source.")
            for symbol in sorted({spec.symbol for spec in specs if spec.fleet == fleet and spec.symbol} | {str(position.get("symbol") or "").upper() for position in snapshot.positions}):
                symbol_labels = {"environment": self.config.environment, "fleet": fleet, "symbol": symbol, "service": "broker"}
                position = _broker_position_for_symbol(snapshot.positions, symbol)
                renderer.sample("renko_broker_position_qty", _safe_float(position.get("qty") if position else 0.0, 0.0) if snapshot.connected else math.nan, symbol_labels, help_text="Broker position quantity by symbol.")
                renderer.sample("renko_broker_position_market_value", _safe_float(position.get("market_value") if position else 0.0, 0.0) if snapshot.connected else math.nan, symbol_labels, help_text="Broker position market value by symbol.")

    def _emit_activity_metrics(self, renderer: PrometheusRenderer) -> None:
        for environment, fleet, strategy, symbol, side, event_type in sorted(self._activity_counters):
            renderer.sample(
                "renko_activity_events_total",
                self._activity_counters[(environment, fleet, strategy, symbol, side, event_type)],
                {
                    "environment": environment,
                    "fleet": fleet,
                    "strategy": strategy,
                    "symbol": symbol,
                    "side": side,
                    "event_type": event_type,
                },
                help_text="Bounded live activity events observed by Mission Control.",
                metric_type="counter",
            )
        self._emit_counter_family(renderer, "renko_signals_total", self._signal_counters, "Strategy signal count observed in bounded event tails.")
        self._emit_counter_family(renderer, "renko_orders_submitted_total", self._orders_submitted_counters, "Order submission count observed in bounded execution event tails.")
        self._emit_counter_family(renderer, "renko_orders_filled_total", self._orders_filled_counters, "Filled order count observed only when bounded execution events contain filled status.")
        self._emit_counter_family(renderer, "renko_strategy_blocks_total", self._strategy_block_counters, "Strategy/risk block count observed in bounded event tails.")
        self._emit_counter_family(renderer, "renko_execution_errors_total", self._execution_error_counters, "Execution error count observed in bounded event tails.")
        for environment, fleet, symbol in sorted(self._reconciliation_failure_counters):
            renderer.sample(
                "renko_reconciliation_failures_total",
                self._reconciliation_failure_counters[(environment, fleet, symbol)],
                {"environment": environment, "fleet": fleet, "symbol": symbol, "service": "reconciliation"},
                help_text="Reconciliation failure events observed in bounded event tails.",
                metric_type="counter",
            )

    def _emit_counter_family(
        self,
        renderer: PrometheusRenderer,
        name: str,
        counters: Counter[tuple[str, str, str, str]],
        help_text: str,
    ) -> None:
        for fleet, strategy, symbol, side in sorted(counters):
            renderer.sample(
                name,
                counters[(fleet, strategy, symbol, side)],
                {
                    "environment": self.config.environment,
                    "fleet": fleet,
                    "strategy": strategy,
                    "symbol": symbol,
                    "side": side,
                },
                help_text=help_text,
                metric_type="counter",
            )

    def _emit_readiness(
        self,
        renderer: PrometheusRenderer,
        specs: list[FleetSpec],
        broker_snapshots: dict[str, BrokerFleetSnapshot],
        readiness_inputs: dict[str, dict[str, bool]],
        *,
        market_open: bool,
        health: dict[str, Any] | None,
        now: datetime,
    ) -> None:
        health_payload = health or {}
        for fleet in sorted({spec.fleet for spec in specs}):
            enabled_specs = [spec for spec in specs if spec.fleet == fleet and spec.enabled]
            broker_ok = broker_snapshots.get(fleet, BrokerFleetSnapshot()).connected
            trader_ok = all(
                readiness_inputs.get(spec.trader_id, {}).get("state_valid", False)
                and readiness_inputs.get(spec.trader_id, {}).get("heartbeat_ok", False)
                and readiness_inputs.get(spec.trader_id, {}).get("position_match", False)
                and readiness_inputs.get(spec.trader_id, {}).get("reconciliation_safe", False)
                for spec in enabled_specs
            )
            market_ok = True
            if market_open:
                market_ok = bool(health_payload.get("sip_connected"))
                for symbol in {spec.symbol for spec in enabled_specs if spec.symbol}:
                    latest = self._latest_canonical_by_symbol.get(symbol, {})
                    timestamp = _parse_timestamp_seconds(latest.get("observation_timestamp"))
                    if timestamp is None:
                        timestamp = _parse_timestamp_seconds(
                            (health_payload.get("last_canonical_observation_by_symbol") or {}).get(symbol)
                            if isinstance(health_payload.get("last_canonical_observation_by_symbol"), dict)
                            else None
                        )
                    if timestamp is None or now.timestamp() - timestamp > self.config.regular_session_stale_seconds:
                        market_ok = False
            renderer.sample(
                "renko_readiness",
                int(bool(enabled_specs) and broker_ok and trader_ok and market_ok),
                {"environment": self.config.environment, "fleet": fleet, "service": "readiness"},
                help_text="Fleet readiness gate from read-only telemetry sources; market freshness is gated by regular-session state.",
            )


@dataclass(frozen=True)
class MarketDataPaths:
    health_path: Path | None
    journal_path: Path | None


def render_emergency_metrics(environment: str = "production") -> str:
    renderer = PrometheusRenderer()
    renderer.sample(
        "renko_telemetry_collector_up",
        0,
        {"environment": _bounded_label_value(environment), "service": "collector"},
        help_text="Mission Control telemetry collector completed without an unhandled exception.",
    )
    renderer.sample(
        "renko_telemetry_source_error",
        1,
        {"environment": _bounded_label_value(environment), "service": "collector"},
        help_text="Read-only telemetry source error state by bounded source.",
    )
    return renderer.render()


def is_regular_market_session_open(now: datetime | None = None) -> bool:
    current = _coerce_utc_datetime(now).astimezone(ZoneInfo(NY_TIMEZONE))
    if current.weekday() >= 5:
        return False
    return local_time(9, 30) <= current.time() < local_time(16, 0)


def is_trader_entry_session_active(spec: FleetSpec, now: datetime | None = None) -> bool:
    session = spec.session
    timezone_name = str(session.get("timezone") or NY_TIMEZONE)
    try:
        local_now = _coerce_utc_datetime(now).astimezone(ZoneInfo(timezone_name))
    except Exception:
        local_now = _coerce_utc_datetime(now).astimezone(ZoneInfo(NY_TIMEZONE))
    if local_now.weekday() >= 5:
        return False
    active_start = _parse_date(session.get("active_start_date"))
    active_end = _parse_date(session.get("active_end_date"))
    if active_start and local_now.date() < active_start:
        return False
    if active_end and local_now.date() > active_end:
        return False
    entry_start = _parse_local_time(session.get("entry_start")) or local_time(9, 31)
    entry_end = _parse_local_time(session.get("entry_end")) or local_time(15, 59, 59)
    return entry_start <= local_now.time() <= entry_end


def _fleet_config_paths_from_environment() -> tuple[Path, ...]:
    explicit = os.getenv("MISSION_CONTROL_CONFIG_PATHS")
    if explicit:
        return _dedupe_paths(Path(os.path.expanduser(part)) for part in explicit.split(os.pathsep) if part.strip())
    paths: list[Path] = []
    trader_config_path = os.getenv("TRADER_CONFIG_PATH")
    if trader_config_path:
        paths.append(Path(os.path.expanduser(trader_config_path)))
    else:
        paths.append(Path("configs/traders.live.example.json"))
    megatron = Path("configs/accounts/megatron.live.json")
    if megatron.exists():
        paths.append(megatron)
    return _dedupe_paths(paths)


def _dedupe_paths(paths: Iterable[Path]) -> tuple[Path, ...]:
    seen: set[str] = set()
    result: list[Path] = []
    for path in paths:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        result.append(path)
    return tuple(result)


def _path_from_env(name: str) -> Path | None:
    value = os.getenv(name)
    return Path(os.path.expanduser(value)) if value not in {None, ""} else None


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def _read_json_document(path: Path, *, max_bytes: int) -> JsonReadResult:
    try:
        stat_result = path.stat()
    except FileNotFoundError:
        return JsonReadResult(payload=None, error="missing")
    except OSError:
        return JsonReadResult(payload=None, error="stat_failed")
    if stat_result.st_size > max_bytes:
        return JsonReadResult(payload=None, mtime_seconds=stat_result.st_mtime, error="too_large")
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            return JsonReadResult(payload=json.load(handle), mtime_seconds=stat_result.st_mtime)
    except json.JSONDecodeError:
        return JsonReadResult(payload=None, mtime_seconds=stat_result.st_mtime, error="malformed")
    except OSError:
        return JsonReadResult(payload=None, mtime_seconds=stat_result.st_mtime, error="read_failed")


def _unwrap_canonical_journal_event(row: dict[str, Any]) -> dict[str, Any] | None:
    if row.get("type") == "journal_record" and isinstance(row.get("payload"), dict):
        payload = row["payload"]
        return payload if payload.get("type") == "observation" else None
    return row if row.get("type") == "observation" else None


def _event_payload(event: dict[str, Any]) -> dict[str, Any]:
    payload = event.get("payload")
    return payload if isinstance(payload, dict) else {}


def _strategy_labels(environment: str, spec: FleetSpec) -> dict[str, object]:
    return {
        "environment": environment,
        "fleet": spec.fleet,
        "strategy": spec.strategy,
        "symbol": spec.symbol or "unknown",
    }


def _strategy_state_valid(spec: FleetSpec, state: dict[str, Any]) -> bool:
    engine = state.get("engine") if isinstance(state.get("engine"), dict) else None
    if engine is None:
        return False
    if str(state.get("trader_id") or engine.get("trader_id") or "") != spec.trader_id:
        return False
    if str(state.get("symbol") or engine.get("symbol") or "").upper() != spec.symbol:
        return False
    if str(engine.get("strategy_type") or "") != spec.strategy_type:
        return False
    if spec.strategy_type.startswith("renko"):
        renko = engine.get("renko") if isinstance(engine.get("renko"), dict) else {}
        configured = _safe_float(state.get("configured_brick_size"), math.nan)
        actual = _safe_float(renko.get("brick_size"), math.nan)
        if not (math.isfinite(configured) and math.isfinite(actual) and abs(configured - actual) < 1e-9):
            return False
    return True


def _positions_match(internal_position: dict[str, Any] | None, broker_position: dict[str, Any] | None) -> bool:
    if internal_position is None and broker_position is None:
        return True
    if internal_position is None or broker_position is None:
        return False
    internal_side = _position_side(internal_position)
    broker_side = _normalize_side(broker_position.get("side"))
    if internal_side != broker_side:
        return False
    internal_qty = abs(_safe_float(internal_position.get("qty"), 0.0))
    broker_qty = abs(_safe_float(broker_position.get("qty"), 0.0))
    return abs(internal_qty - broker_qty) < 1e-9


def _position_side(position: dict[str, Any] | None) -> str:
    if not position:
        return "flat"
    side = _normalize_side(position.get("side"))
    return side if side in {"long", "short"} else "flat"


def _broker_position_for_symbol(positions: list[dict[str, Any]], symbol: str) -> dict[str, Any] | None:
    symbol_upper = symbol.upper()
    for position in positions:
        if str(position.get("symbol") or "").upper() == symbol_upper:
            return position
    return None


def _broker_orders_for_symbol(orders: list[dict[str, Any]], symbol: str) -> list[dict[str, Any]]:
    symbol_upper = symbol.upper()
    return [order for order in orders if str(order.get("symbol") or "").upper() == symbol_upper]


def _looks_like_protective_order(order: dict[str, Any]) -> bool:
    order_class = str(order.get("order_class") or "").lower()
    order_type = str(order.get("type") or "").lower()
    if order_class in {"bracket", "oco"}:
        return True
    return order_type in {"stop", "stop_limit", "trailing_stop"}


def _aggregate_accounts(accounts: list[dict[str, Any]]) -> dict[str, float]:
    if not accounts:
        return {}
    equity = sum(_safe_float(account.get("equity"), 0.0) for account in accounts)
    buying_power = sum(_safe_float(account.get("buying_power"), 0.0) for account in accounts)
    cash = sum(_safe_float(account.get("cash"), 0.0) for account in accounts)
    daytrading_buying_power = sum(_safe_float(account.get("daytrading_buying_power"), 0.0) for account in accounts)
    long_market_value = sum(abs(_safe_float(account.get("long_market_value"), 0.0)) for account in accounts)
    short_market_value = sum(abs(_safe_float(account.get("short_market_value"), 0.0)) for account in accounts)
    gross_exposure = long_market_value + short_market_value
    net_exposure = long_market_value - short_market_value
    return {
        "equity": equity,
        "buying_power": buying_power,
        "cash": cash,
        "daytrading_buying_power": daytrading_buying_power,
        "gross_exposure": gross_exposure,
        "net_exposure": net_exposure,
        "margin_used": max(0.0, gross_exposure - equity),
    }


def _sum_unrealized_pnl(positions: list[dict[str, Any]]) -> float:
    return sum(_safe_float(position.get("unrealized_pl"), 0.0) for position in positions)


def _consumer_gap_count(health: dict[str, Any]) -> int:
    consumers = health.get("consumers")
    if not isinstance(consumers, dict):
        return 0
    total = 0
    for consumer in consumers.values():
        if isinstance(consumer, dict):
            total += int(_safe_float(consumer.get("gap_count"), 0))
    return total


def _dict_keys(value: Any) -> tuple[str, ...]:
    if not isinstance(value, dict):
        return tuple()
    return tuple(str(key).upper() for key in value)


def _label_tuple(labels: dict[str, object]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted((key, _bounded_label_value(value)) for key, value in labels.items()))


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        result = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _parse_timestamp_seconds(value: object) -> float | None:
    if value in {None, ""}:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        numeric = float(value)
        return numeric if math.isfinite(numeric) else None
    text = str(value).strip()
    match = re.match(
        r"^(?P<prefix>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})(?P<fraction>\.\d+)?(?P<tz>Z|z|[+-]\d{2}:?\d{2})?$",
        text,
    )
    if match:
        fraction = match.group("fraction") or ""
        if len(fraction) > 7:
            fraction = fraction[:7]
        tz_text = match.group("tz") or "+00:00"
        if tz_text.upper() == "Z":
            tz_text = "+00:00"
        elif re.match(r"^[+-]\d{4}$", tz_text):
            tz_text = f"{tz_text[:3]}:{tz_text[3:]}"
        text = f"{match.group('prefix')}{fraction}{tz_text}"
    try:
        parsed = datetime.fromisoformat(text.replace(" ", "T"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).timestamp()


def _coerce_utc_datetime(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(tz=timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_date(value: object) -> Any:
    if value in {None, ""}:
        return None
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except ValueError:
        return None


def _parse_local_time(value: object) -> local_time | None:
    if value in {None, ""}:
        return None
    parts = str(value).split(":")
    try:
        if len(parts) == 2:
            return local_time(int(parts[0]), int(parts[1]))
        if len(parts) == 3:
            return local_time(int(parts[0]), int(parts[1]), int(float(parts[2])))
    except ValueError:
        return None
    return None


def _normalize_side(value: object) -> str:
    text = str(value or "").strip().lower()
    if text in {"long", "buy"}:
        return "long"
    if text in {"short", "sell"}:
        return "short"
    if text == "flat":
        return "flat"
    return "unknown"


def _slug(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_") or "unknown"


def _bounded_label_value(value: object) -> str:
    text = str(value if value not in {None, ""} else "unknown")
    return text[:200]


def _format_metric_value(value: object) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if value is None:
        return "NaN"
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return "NaN"
    if math.isnan(number):
        return "NaN"
    if math.isinf(number):
        return "+Inf" if number > 0 else "-Inf"
    if number.is_integer():
        return str(int(number))
    return f"{number:.12g}"


def _bucket_label(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.12g}"


def _escape_label_value(value: object) -> str:
    text = _bounded_label_value(value)
    return text.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')
