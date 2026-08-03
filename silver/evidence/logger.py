from __future__ import annotations

import json
import os
import sys
import threading
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

from .models import EVIDENCE_SCHEMA_VERSION, UNKNOWN, EvidenceCheckpoint, EvidenceRecord, jsonable, utc_now_iso


@dataclass
class EvidenceLoggerCounters:
    records_written: int = 0
    bytes_written: int = 0
    write_failures: int = 0
    records_dropped: int = 0
    diagnostics_emitted: int = 0
    evidence_failures: int = 0
    diagnostic_failures: int = 0


@dataclass(frozen=True)
class EvidenceLoggerSettings:
    enabled: bool = False
    output_path: str | None = None
    session_id: str = "default"
    process_id: str = "default"
    max_buffer_records: int = 100
    flush_every_records: int = 1
    fail_policy: str = "fail_open"
    diagnostic_rate_limit: int = 1
    disable_after_failures: int = 3

    def __post_init__(self) -> None:
        if self.max_buffer_records < 1:
            raise ValueError("max_buffer_records must be >= 1.")
        if self.flush_every_records < 1:
            raise ValueError("flush_every_records must be >= 1.")
        if self.fail_policy != "fail_open":
            raise ValueError("Only fail_open evidence logging is supported.")
        if self.disable_after_failures < 1:
            raise ValueError("disable_after_failures must be >= 1.")


class EvidenceLogger:
    def __init__(
        self,
        settings: EvidenceLoggerSettings | None = None,
        *,
        diagnostic_sink: Callable[[str], None] | None = None,
    ) -> None:
        self.settings = settings or EvidenceLoggerSettings()
        self.counters = EvidenceLoggerCounters()
        self._diagnostic_sink = diagnostic_sink or (lambda message: print(message, file=sys.stderr))
        self._lock = threading.Lock()
        self._buffer: list[str] = []
        self._sequences: dict[str, int] = {}
        self._failure_diagnostics_remaining = max(0, self.settings.diagnostic_rate_limit)
        self._path = Path(self.settings.output_path) if self.settings.output_path else None
        self._disabled_after_failure = False

    @property
    def enabled(self) -> bool:
        return self.settings.enabled and not self._disabled_after_failure

    @classmethod
    def disabled(cls) -> "EvidenceLogger":
        return cls(EvidenceLoggerSettings(enabled=False))

    @classmethod
    def from_environment(
        cls,
        *,
        default_root: str | Path = "artifacts/live/evidence",
        diagnostic_sink: Callable[[str], None] | None = None,
    ) -> "EvidenceLogger":
        enabled = str(os.environ.get("FLEET_EVIDENCE_ENABLED", "")).lower() in {"1", "true", "yes", "on"}
        if not enabled:
            return cls.disabled()
        session_id = os.environ.get("FLEET_EVIDENCE_SESSION_ID")
        process_id = os.environ.get("FLEET_EVIDENCE_PROCESS_ID") or str(os.getpid())
        output_path = os.environ.get("FLEET_EVIDENCE_OUTPUT")
        if not session_id:
            logger = cls.disabled()
            logger._diagnostic_sink = diagnostic_sink or logger._diagnostic_sink
            logger.record_failure("evidence disabled: missing explicit FLEET_EVIDENCE_SESSION_ID")
            return logger
        if not output_path:
            logger = cls.disabled()
            logger._diagnostic_sink = diagnostic_sink or logger._diagnostic_sink
            logger.record_failure("evidence disabled: missing explicit FLEET_EVIDENCE_OUTPUT")
            return logger
        max_buffer_records = int(os.environ.get("FLEET_EVIDENCE_BUFFER_RECORDS", "100"))
        flush_every_records = int(os.environ.get("FLEET_EVIDENCE_FLUSH_EVERY", "1"))
        return cls(
            EvidenceLoggerSettings(
                enabled=enabled,
                output_path=output_path,
                session_id=session_id,
                process_id=process_id,
                max_buffer_records=max_buffer_records,
                flush_every_records=flush_every_records,
            ),
            diagnostic_sink=diagnostic_sink,
        )

    def record_observation(
        self,
        *,
        trader_id: str,
        symbol: str,
        observation_bucket_timestamp: Any,
        close_price: Any,
        source_trade_identity: str | None = None,
        source_trade_timestamp: Any = UNKNOWN,
        triggering_trade_identity: str | None = None,
        triggering_trade_timestamp: Any = UNKNOWN,
    ) -> None:
        self._record_from_factory(
            "observation",
            trader_id,
            symbol,
            lambda: {
                "observation_bucket_timestamp": jsonable(observation_bucket_timestamp),
                "close_price": jsonable(close_price),
                "source_trade_identity": source_trade_identity,
                "source_trade_timestamp": jsonable(source_trade_timestamp),
                "triggering_trade_identity": triggering_trade_identity,
                "triggering_trade_timestamp": jsonable(triggering_trade_timestamp),
            },
        )

    def record_production_observation(
        self,
        *,
        trader_id: str,
        symbol: str,
        sequence: int,
        observation_bucket_timestamp: Any,
        close_price: Any,
        production_source_mode: str,
    ) -> None:
        self._record_from_factory(
            "production_observation",
            trader_id,
            symbol,
            lambda: {
                "sequence": sequence,
                "observation_bucket_timestamp": jsonable(observation_bucket_timestamp),
                "close_price": jsonable(close_price),
                "production_source_mode": production_source_mode,
            },
        )

    def record_shadow_observation(
        self,
        *,
        trader_id: str,
        symbol: str,
        sequence: int,
        observation_bucket_timestamp: Any,
        close_price: Any,
        source_record_id: str | None,
        broker_trade_id: str | None,
        source_trade_timestamp: Any,
        triggering_source_record_id: str | None,
        triggering_trade_timestamp: Any,
    ) -> None:
        self._record_from_factory(
            "shadow_observation",
            trader_id,
            symbol,
            lambda: {
                "sequence": sequence,
                "observation_bucket_timestamp": jsonable(observation_bucket_timestamp),
                "close_price": jsonable(close_price),
                "source_record_id": source_record_id,
                "broker_trade_id": broker_trade_id,
                "source_trade_timestamp": jsonable(source_trade_timestamp),
                "triggering_source_record_id": triggering_source_record_id,
                "triggering_trade_timestamp": jsonable(triggering_trade_timestamp),
            },
        )

    def record_shadow_sampler_diagnostic(
        self,
        *,
        trader_id: str,
        symbol: str,
        diagnostic_type: str,
        message: str,
        source_record_id: str | None = None,
    ) -> None:
        self._record_from_factory(
            "shadow_sampler_diagnostic",
            trader_id,
            symbol,
            lambda: {
                "diagnostic_type": diagnostic_type,
                "message": message,
                "source_record_id": source_record_id,
            },
        )

    def record_shadow_sampler_checkpoint(self, *, trader_id: str, symbol: str, checkpoint: dict[str, Any]) -> None:
        self._record_from_factory("shadow_sampler_checkpoint", trader_id, symbol, lambda: checkpoint)

    def record_production_sampler_checkpoint(self, *, trader_id: str, symbol: str, checkpoint: dict[str, Any]) -> None:
        self._record_from_factory("production_sampler_checkpoint", trader_id, symbol, lambda: checkpoint)

    def record_runtime_summary(self, *, trader_id: str, symbol: str, summary: dict[str, Any]) -> None:
        self._record_from_factory("evidence_runtime_summary", trader_id, symbol, lambda: summary)

    def record_renko_brick(
        self,
        *,
        trader_id: str,
        symbol: str,
        brick: Any,
        source_observation_bucket_timestamp: Any,
    ) -> None:
        self._record_from_factory(
            "renko_brick",
            trader_id,
            symbol,
            lambda: {
                "brick_logical_timestamp": jsonable(getattr(brick, "timestamp", None)),
                "source_observation_bucket_timestamp": jsonable(source_observation_bucket_timestamp),
                "open": jsonable(getattr(brick, "open", None)),
                "close": jsonable(getattr(brick, "close", None)),
                "direction": jsonable(getattr(brick, "direction", None)),
                "run_length": jsonable(getattr(brick, "run_len", None)),
                "effective_brick_size": jsonable(getattr(brick, "brick_size", None)),
            },
        )

    def record_signal(
        self,
        *,
        trader_id: str,
        symbol: str,
        signal: Any,
        intent: Any,
        source_brick_timestamp: Any = UNKNOWN,
    ) -> int | None:
        return self._record_from_factory(
            "strategy_signal",
            trader_id,
            symbol,
            lambda: {
                "logical_signal_timestamp": jsonable(getattr(signal, "timestamp", None)),
                "source_brick_timestamp": jsonable(source_brick_timestamp),
                "side": jsonable(getattr(signal, "side", None)),
                "broker_side": jsonable(getattr(signal, "broker_side", None)),
                "reason": jsonable(getattr(signal, "reason", None)),
                "signal_type": jsonable(getattr(signal, "signal_type", None)),
                "strategy": jsonable(getattr(signal, "strategy", None)),
                "target_direction": jsonable(getattr(signal, "side", None)),
                "quantity": jsonable(getattr(intent, "qty", None) or getattr(intent, "target_qty", None)),
                "signal_id": jsonable(getattr(signal, "signal_id", None)),
                "client_order_id": jsonable(getattr(intent, "client_order_id", None)),
            },
        )

    def record_position_direction(
        self,
        *,
        trader_id: str,
        symbol: str,
        logical_transition_timestamp: Any,
        before_direction: str | None,
        after_direction: str | None,
        triggering_signal_sequence: int | None = None,
    ) -> None:
        self._record_from_factory(
            "position_direction",
            trader_id,
            symbol,
            lambda: {
                "logical_transition_timestamp": jsonable(logical_transition_timestamp),
                "before_direction": before_direction,
                "after_direction": after_direction,
                "triggering_signal_sequence": triggering_signal_sequence,
            },
        )

    def record_checkpoint(self, checkpoint: EvidenceCheckpoint) -> None:
        self._record_from_factory(
            "state_checkpoint",
            checkpoint.trader_id,
            checkpoint.symbol,
            lambda: asdict(checkpoint),
        )

    def flush(self) -> None:
        if not self.enabled:
            return
        with self._lock:
            try:
                self._flush_locked()
            except Exception as exc:
                self._handle_failure_locked(f"evidence flush failed; trading continues: {exc}", dropped=len(self._buffer))
                self._buffer = []

    def record_failure(self, message: str) -> None:
        with self._lock:
            self._handle_failure_locked(message, dropped=0)

    def _record_from_factory(self, evidence_type: str, trader_id: str, symbol: str, payload_factory: Callable[[], dict[str, Any]]) -> int | None:
        if not self.enabled:
            return None
        try:
            payload = payload_factory()
        except Exception as exc:
            self.record_failure(f"evidence payload construction failed for {evidence_type}; trading continues: {exc}")
            return None
        return self._record(evidence_type, trader_id, symbol, payload)

    def _record(self, evidence_type: str, trader_id: str, symbol: str, payload: dict[str, Any]) -> int | None:
        if not self.enabled:
            return None
        with self._lock:
            try:
                sequence = self._next_sequence_locked(trader_id)
                record = EvidenceRecord(
                    schema_version=EVIDENCE_SCHEMA_VERSION,
                    evidence_type=evidence_type,
                    recorded_at=utc_now_iso(),
                    trader_id=trader_id,
                    symbol=symbol.upper(),
                    session_id=self.settings.session_id,
                    process_id=self.settings.process_id,
                    logical_sequence=sequence,
                    payload=payload,
                )
                line = json.dumps(jsonable(asdict(record)), sort_keys=True, separators=(",", ":")) + "\n"
                if len(self._buffer) >= self.settings.max_buffer_records:
                    self._flush_locked()
                if len(self._buffer) >= self.settings.max_buffer_records:
                    self._handle_failure_locked("evidence buffer full; dropping record", dropped=1)
                    return sequence
                self._buffer.append(line)
                if len(self._buffer) >= self.settings.flush_every_records:
                    self._flush_locked()
                return sequence
            except Exception as exc:
                self._handle_failure_locked(f"evidence record failed for {evidence_type}; trading continues: {exc}", dropped=1)
                return None

    def _next_sequence_locked(self, trader_id: str) -> int:
        current = self._sequences.get(trader_id, 0) + 1
        self._sequences[trader_id] = current
        return current

    def _flush_locked(self) -> None:
        if not self._buffer:
            return
        if self._path is None:
            self._buffer.clear()
            self.counters.records_dropped += 1
            self._diagnose_locked("evidence output path is not configured")
            return
        lines = self._buffer
        self._buffer = []
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with self._path.open("a", encoding="utf-8") as handle:
                handle.writelines(lines)
            self.counters.records_written += len(lines)
            self.counters.bytes_written += sum(len(line.encode("utf-8")) for line in lines)
        except Exception as exc:
            self._handle_failure_locked(f"evidence write failed; trading continues: {exc}", dropped=len(lines))

    def _handle_failure_locked(self, message: str, *, dropped: int) -> None:
        self.counters.evidence_failures += 1
        self.counters.write_failures += 1
        self.counters.records_dropped += dropped
        self._diagnose_locked(message)
        if self.counters.evidence_failures >= self.settings.disable_after_failures:
            self._disabled_after_failure = True
            self._buffer = []
            self._diagnose_locked("evidence logging disabled after repeated failures")

    def _diagnose_locked(self, message: str) -> None:
        if self._failure_diagnostics_remaining <= 0:
            return
        self._failure_diagnostics_remaining -= 1
        try:
            self.counters.diagnostics_emitted += 1
            self._diagnostic_sink(f"[evidence] {message}")
        except Exception:
            self.counters.diagnostic_failures += 1

    def visible_diagnostic(self, message: str) -> None:
        self.safe_visible_diagnostic(message)

    def safe_visible_diagnostic(self, message: str) -> bool:
        with self._lock:
            before = self.counters.diagnostic_failures
            try:
                self._diagnose_locked(str(message))
            except Exception:
                self.counters.diagnostic_failures += 1
            return self.counters.diagnostic_failures == before
