from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import ssl
import sys
import time
from dataclasses import dataclass, field
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable
from datetime import date, timedelta

import certifi
import pandas as pd
import websockets
from dotenv import load_dotenv

from alpaca_trader import load_alpaca_credentials
from broker_protection import ProtectionVerification, verify_broker_native_protection
from execution_engine import AlpacaExecutionEngine, JsonlEventLog, ShadowExecutionEngine
from reversal_protection import ReversalStopVerification, verify_reversal_catastrophic_stop
from reversal_protection import build_reversal_catastrophic_stop_plan, catastrophic_stop_order_payload
from silver.execution import authoritative_position_from_alpaca
from shared.config.trader_config import (
    as_float,
    build_strategy_config,
    canonical_observation_source_for_non_renko,
    load_production_renko_config,
    normalize_renko_source,
    production_consumer_identity,
    validate_renko_trader_source,
    validate_production_fleet_config,
)
from renko_builder import RenkoBuildState, initial_renko_close
from silver.evidence import EvidenceLogger
from silver.evidence.models import EvidenceCheckpoint, UNKNOWN
from silver.evidence.shadow_sampler import EvidenceShadowSampler, ShadowSamplerSettings
from silver.market import (
    CANONICAL_RENKO_SOURCE,
    CanonicalMarketObservationAdapter,
    SharedCanonicalStreamConsumer,
    LiveMarketEvent,
    LocalArchiveMarketDataTail,
    contract_snapshot,
    live_market_event_from_alpaca_message,
)
from strategy_engine import (
    TP_SL_EXIT_LOCK_STATES,
    TP_SL_LIFECYCLE_ENTRY_PLANNED,
    TP_SL_LIFECYCLE_EXIT_PENDING,
    MeanReversionStrategyConfig,
    MeanReversionStrategyEngine,
    RenkoStrategyConfig,
    RenkoStrategyEngine,
)
from trading_events import MarketTick, OrderIntent, Signal, dataclass_to_jsonable, is_price_forming_trade


DATA_STREAM_URL_TEMPLATE = "wss://stream.data.alpaca.markets/v2/{feed}"
BROKER_MANAGED_RECONCILE_INTERVAL_SECONDS = 5.0
BROKER_FORCE_FLAT_POLL_SECONDS = 0.5
RENKO_BRICK_CHANGE_MIGRATION_POLICY = "RESET_RENKO_ON_BRICK_CHANGE_PRESERVE_POSITION_V1"
_LAST_BROKER_MANAGED_RECONCILE: dict[str, float] = {}
_BROKER_FORCE_FLAT_DATES: set[tuple[str, str, str, str]] = set()


class BrokerEngineStateMismatch(RuntimeError):
    pass


@dataclass(frozen=True)
class TraderRuntime:
    trader_id: str
    symbol: str
    mode: str
    credential_profile: str | None
    feed: str
    enabled: bool
    execution_enabled: bool
    max_position_notional: float
    max_daily_loss: float
    engine: RenkoStrategyEngine | MeanReversionStrategyEngine
    execution: AlpacaExecutionEngine | ShadowExecutionEngine
    log: JsonlEventLog
    account_namespace: str | None = None
    expected_account_id: str | None = None
    market_data_source: str = "alpaca"
    market_data_profile: str | None = None
    renko_source: str = CANONICAL_RENKO_SOURCE
    market_adapter: CanonicalMarketObservationAdapter | None = None
    contract_fingerprints: dict[str, Any] | None = None
    evidence: EvidenceLogger | None = None
    shadow_sampler: EvidenceShadowSampler | None = None
    config_hash: str | None = None
    migration_evidence_path: str | None = None
    market_data_signal_block_reason: str | None = None
    processed_canonical_event_ids: dict[str, str] = field(default_factory=dict)


_PRODUCTION_OBSERVATION_SEQUENCES: dict[tuple[str, str], int] = {}
TERMINAL_EXECUTION_FAILURE_STATUSES = {
    "canceled",
    "cancelled",
    "expired",
    "failed",
    "rejected",
    "suspended",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a config-driven fleet of live strategy traders.")
    parser.add_argument("--config", required=True, help="Fleet JSON config path.")
    parser.add_argument("--execute-real", action="store_true", help="Submit Alpaca orders. Defaults to shadow mode.")
    parser.add_argument("--force-mode", choices=["paper", "live"], help="Override trader mode for the whole fleet.")
    parser.add_argument("--force-feed", choices=["iex", "sip", "delayed_sip", "boats", "overnight"], help="Override trader feed.")
    parser.add_argument("--market-data-source", choices=["alpaca", "local_archive_tail", "shared_canonical_stream"], help="Override market-data source for the whole fleet.")
    parser.add_argument("--market-data-profile", help="Alpaca credential profile used only for Alpaca market-data authentication.")
    parser.add_argument("--data-credential-profile", help="Deprecated alias for --market-data-profile.")
    parser.add_argument("--shared-market-data-config", help="Shared canonical market-data service JSON config.")
    parser.add_argument("--shared-market-socket", help="Override shared canonical market-data Unix socket path.")
    parser.add_argument("--shared-market-freshness-seconds", type=float, default=5.0)
    parser.add_argument("--local-market-poll-seconds", type=float, default=0.1, help="Poll interval for local archive-tail market data.")
    parser.add_argument("--local-market-start-at-beginning", action="store_true", help="Replay existing local archive lines before tailing.")
    parser.add_argument("--log-output", help="Override all trader event logs.")
    parser.add_argument("--archive-root", default="artifacts/live", help="Root directory for tick, Renko, and state archives.")
    parser.add_argument(
        "--market-archive-root",
        help="Read-only archive root for local_archive_tail market data. Defaults to --archive-root.",
    )
    parser.add_argument("--no-persist-ticks", action="store_true", help="Disable raw tick archive writes.")
    parser.add_argument("--reconnect-delay", type=float, default=5.0)
    parser.add_argument("--log-ticks", action="store_true")
    parser.add_argument("--evidence-enabled", action="store_true", help="Enable append-only parity evidence logging.")
    parser.add_argument("--evidence-output", help="Evidence JSONL output path. Defaults to artifacts/live/evidence/<session>.jsonl.")
    parser.add_argument("--evidence-session-id", help="Stable evidence session identifier for deterministic process-scope lineage.")
    parser.add_argument("--evidence-buffer-records", type=int, default=100, help="Maximum bounded evidence buffer size.")
    parser.add_argument("--evidence-flush-every", type=int, default=1, help="Flush evidence after this many buffered records.")
    parser.add_argument("--shadow-sampler-enabled", action="store_true", help="Enable canonical sampler shadow evidence. Requires evidence mode.")
    return parser


def load_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        config = json.load(handle)
    if not isinstance(config.get("traders"), list):
        raise ValueError("Fleet config must contain a top-level traders array.")
    return config


def tick_second_bucket(timestamp: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(timestamp).tz_convert("UTC").floor("s")


def archive_root(args: argparse.Namespace) -> Path:
    return Path(str(args.archive_root))


def market_archive_root(args: argparse.Namespace) -> Path:
    return Path(str(getattr(args, "market_archive_root", None) or args.archive_root))


def slug(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_") or "unknown"


def normalize_optional_namespace(value: object) -> str | None:
    raw = str(value or "").strip().lower()
    return raw or None


def shared_consumer_identity(runtime: TraderRuntime) -> str:
    """Return the stable IPC identity for one trader runtime."""
    return production_consumer_identity(
        {
            "id": runtime.trader_id,
            "account_namespace": runtime.account_namespace,
            "credential_profile": runtime.credential_profile,
        }
    )


def normalize_market_data_source(value: object) -> str:
    raw = str(value or "alpaca").strip().lower().replace("-", "_")
    if raw in {"alpaca", "alpaca_sip", "alpaca_ws"}:
        return "alpaca"
    if raw in {"local_archive", "local_archive_tail", "archive_tail", "shared_camatron_sip"}:
        return "local_archive_tail"
    if raw in {"shared_canonical", "shared_canonical_stream", "canonical_ipc"}:
        return "shared_canonical_stream"
    raise ValueError(f"Unsupported market_data_source: {value}")


def cli_market_data_profile(args: argparse.Namespace) -> str | None:
    return normalize_optional_namespace(
        getattr(args, "market_data_profile", None) or getattr(args, "data_credential_profile", None)
    )


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    temp_path.replace(path)


def archive_date(timestamp: pd.Timestamp) -> str:
    return pd.Timestamp(timestamp).tz_convert("UTC").date().isoformat()


def tick_archive_path(root: Path, tick: MarketTick) -> Path:
    return root / "ticks" / archive_date(tick.timestamp) / f"{tick.symbol.upper()}.jsonl"


def renko_archive_path(root: Path, brick_symbol: str, trader_id: str, timestamp: pd.Timestamp) -> Path:
    return root / "renko" / archive_date(timestamp) / f"{slug(trader_id)}_{brick_symbol.upper()}.jsonl"


def state_snapshot_path(root: Path, trader_id: str) -> Path:
    return root / "state" / f"{slug(trader_id)}.json"


def archive_tick(root: Path, tick: MarketTick, mode: str, feed: str, source_event: LiveMarketEvent | None = None) -> None:
    payload = dataclass_to_jsonable(tick)
    payload["mode"] = mode
    payload["feed"] = feed
    if source_event is not None:
        payload["source_record_id"] = source_event.source_record_id
        payload["broker_trade_id"] = source_event.broker_trade_id
        payload["source_sequence"] = source_event.source_sequence
        payload["eligible"] = source_event.eligible
    payload["logged_at"] = pd.Timestamp.now(tz="UTC").isoformat()
    append_jsonl(tick_archive_path(root, tick), payload)


def archive_renko_brick(root: Path, runtime: TraderRuntime, brick_payload: dict[str, Any]) -> None:
    payload = dict(brick_payload)
    payload["mode"] = runtime.mode
    payload["feed"] = runtime.feed
    payload["credential_profile"] = runtime.credential_profile
    payload["logged_at"] = pd.Timestamp.now(tz="UTC").isoformat()
    append_jsonl(
        renko_archive_path(root, runtime.symbol, runtime.trader_id, pd.Timestamp(payload["timestamp"])),
        payload,
    )


def archive_engine_state(root: Path, runtime: TraderRuntime, reason: str) -> None:
    strategy_config = getattr(runtime.engine, "config", None)
    payload = {
        "saved_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "reason": reason,
        "trader_id": runtime.trader_id,
        "symbol": runtime.symbol,
        "mode": runtime.mode,
        "credential_profile": runtime.credential_profile,
        "account_namespace": runtime.account_namespace,
        "market_data_source": runtime.market_data_source,
        "market_data_profile": runtime.market_data_profile,
        "feed": runtime.feed,
        "execution_enabled": runtime.execution_enabled,
        "configuration_hash": runtime.config_hash,
        "contract_fingerprints": runtime.contract_fingerprints,
        "sampler_state": runtime.market_adapter.snapshot(trader_id=runtime.trader_id, symbol=runtime.symbol)
        if runtime.market_adapter is not None
        else None,
        "canonical_accounting_state": {
            "status": "NOT_PERSISTED_IN_LIVE_RUNNER_V1",
            "reason": "broker/accounting reconciliation is outside the strategy engine snapshot",
        },
        "pending_orders_state": {
            "status": "NOT_PERSISTED_IN_LIVE_RUNNER_V1",
            "reason": "broker orders are reconciled from Alpaca execution state",
        },
        "canonical_market_checkpoints": {
            "processed_event_ids": runtime.processed_canonical_event_ids,
        },
        "configured_brick_size": getattr(strategy_config, "brick_size", None),
        "configured_quantity": getattr(strategy_config, "qty", None),
        "engine": runtime.engine.snapshot(),
    }
    write_json_atomic(state_snapshot_path(root, runtime.trader_id), payload)
    safe_evidence_call(runtime, "state_checkpoint", lambda: record_evidence_checkpoint(runtime, reason=reason, checkpoint_timestamp=payload["saved_at"]))


def evidence_enabled(args: argparse.Namespace) -> bool:
    if bool(getattr(args, "evidence_enabled", False)):
        return True
    return str(os.environ.get("FLEET_EVIDENCE_ENABLED", "")).lower() in {"1", "true", "yes", "on"}


def shadow_sampler_requested(args: argparse.Namespace) -> bool:
    if bool(getattr(args, "shadow_sampler_enabled", False)):
        return True
    return str(os.environ.get("FLEET_SHADOW_SAMPLER_ENABLED", "")).lower() in {"1", "true", "yes", "on"}


def visible_evidence_warning(message: str) -> None:
    print(f"[evidence] {message}", file=sys.stderr)


def build_evidence_logger(args: argparse.Namespace) -> EvidenceLogger:
    if not evidence_enabled(args):
        return EvidenceLogger.disabled()
    from silver.evidence.logger import EvidenceLoggerSettings

    session_id = getattr(args, "evidence_session_id", None) or os.environ.get("FLEET_EVIDENCE_SESSION_ID")
    output_path = getattr(args, "evidence_output", None) or os.environ.get("FLEET_EVIDENCE_OUTPUT")
    if not session_id:
        visible_evidence_warning("evidence disabled: missing explicit FLEET_EVIDENCE_SESSION_ID or --evidence-session-id")
        return EvidenceLogger.disabled()
    if not output_path:
        visible_evidence_warning("evidence disabled: missing explicit FLEET_EVIDENCE_OUTPUT or --evidence-output")
        return EvidenceLogger.disabled()
    return EvidenceLogger(
        EvidenceLoggerSettings(
            enabled=True,
            output_path=output_path,
            session_id=session_id,
            process_id=os.environ.get("FLEET_EVIDENCE_PROCESS_ID") or str(os.getpid()),
            max_buffer_records=int(getattr(args, "evidence_buffer_records", 100)),
            flush_every_records=int(getattr(args, "evidence_flush_every", 1)),
        )
    )


def build_shadow_sampler(args: argparse.Namespace, evidence_logger: EvidenceLogger) -> EvidenceShadowSampler | None:
    if not shadow_sampler_requested(args):
        return None
    if not evidence_logger.enabled:
        visible_evidence_warning("shadow sampler disabled: evidence logging is not enabled")
        return None
    if not evidence_logger.settings.session_id:
        visible_evidence_warning("shadow sampler disabled: missing evidence session ID")
        return None
    if not evidence_logger.settings.output_path:
        visible_evidence_warning("shadow sampler disabled: missing evidence output path")
        return None
    return EvidenceShadowSampler(
        ShadowSamplerSettings(
            enabled=True,
            session_id=evidence_logger.settings.session_id,
            process_id=evidence_logger.settings.process_id,
        )
    )


def production_sampler_checkpoint(runtime: TraderRuntime) -> dict[str, Any]:
    if runtime.market_adapter is None:
        return {
            "production_source_mode": runtime.renko_source,
            "pending_state": UNKNOWN,
            "unknown_fields": ["canonical_market_adapter"],
        }
    snapshot = runtime.market_adapter.snapshot(trader_id=runtime.trader_id, symbol=runtime.symbol)
    state = snapshot.get("state") if isinstance(snapshot, dict) else None
    pending_trade = state.get("pending_trade") if isinstance(state, dict) else None
    raw_trade = pending_trade.get("raw_trade") if isinstance(pending_trade, dict) else None
    return {
        **snapshot,
        "production_source_mode": runtime.renko_source,
        "pending_second_bucket": state.get("pending_bucket") if isinstance(state, dict) else None,
        "pending_price": raw_trade.get("price") if isinstance(raw_trade, dict) else None,
        "pending_sip_timestamp": raw_trade.get("sip_timestamp") if isinstance(raw_trade, dict) else None,
        "pending_trade_id": raw_trade.get("trade_id") if isinstance(raw_trade, dict) else None,
        "pending_source_record_id": raw_trade.get("source_record_id") if isinstance(raw_trade, dict) else None,
        "unknown_fields": [],
    }


def safe_evidence_call(runtime: TraderRuntime, operation_name: str, callback: Callable[[], Any]) -> Any:
    if runtime.evidence is None or not runtime.evidence.enabled:
        return None
    try:
        return callback()
    except Exception as exc:
        try:
            runtime.evidence.record_failure(f"evidence {operation_name} failed; trading continues: {exc}")
        except Exception as diagnostic_exc:
            runtime.evidence.safe_visible_diagnostic(f"evidence {operation_name} failed and diagnostic failed; trading continues: {diagnostic_exc}")
        return None


def record_evidence_checkpoint(runtime: TraderRuntime, *, reason: str, checkpoint_timestamp: Any) -> None:
    if runtime.evidence is None or not runtime.evidence.enabled:
        return
    snapshot = runtime.engine.snapshot()
    renko = snapshot.get("renko") if isinstance(snapshot, dict) else {}
    trade = snapshot.get("trade") if isinstance(snapshot, dict) else {}
    position = trade.get("position") if isinstance(trade, dict) else None
    completeness: list[str] = []
    if not isinstance(renko, dict):
        renko = {}
        completeness.append("renko_state_missing")
    if position is None:
        completeness.append("strategy_position_state_missing")
    completeness.extend(["broker_confirmed_quantity_unknown", "accounting_state_unknown"])
    production_sampler_state = production_sampler_checkpoint(runtime)
    shadow_sampler_state = runtime.shadow_sampler.checkpoint(trader_id=runtime.trader_id, symbol=runtime.symbol) if runtime.shadow_sampler else None
    if shadow_sampler_state is None:
        completeness.append("shadow_sampler_state_unavailable")
    checkpoint = EvidenceCheckpoint(
        checkpoint_timestamp=str(checkpoint_timestamp),
        trader_id=runtime.trader_id,
        symbol=runtime.symbol,
        configuration_hash=runtime.config_hash,
        renko_anchor_mode=renko.get("anchor_mode"),
        renko_anchor_price=renko.get("anchor_price"),
        last_renko_close=renko.get("last_brick_close"),
        run_direction=renko.get("run_dir"),
        run_length=renko.get("run_len"),
        strategy_position_state=position if isinstance(position, dict) else None,
        broker_confirmed_quantity=UNKNOWN,
        accounting_quantity=UNKNOWN,
        accounting_basis_status=UNKNOWN,
        pending_order_identifiers=[],
        force_flat_state={"reason": reason, "force_flat_dates": trade.get("force_flat_dates", []) if isinstance(trade, dict) else []},
        production_sampler_state=production_sampler_state,
        shadow_sampler_state=shadow_sampler_state,
        component_versions={
            "renko": "production.RenkoStrategyEngine",
            "evidence": "production_parity_evidence.v1",
            "market_stream_contract": (runtime.contract_fingerprints or {}).get("market_stream", {}).get("version"),
            "renko_strategy_contract": (runtime.contract_fingerprints or {}).get("renko_strategy", {}).get("version"),
        },
        completeness=completeness,
        unknown_fields=production_sampler_state.get("unknown_fields", []) + ["broker_confirmed_quantity", "accounting_quantity", "accounting_basis_status"],
    )
    safe_evidence_call(runtime, "checkpoint_record", lambda: runtime.evidence.record_checkpoint(checkpoint))
    safe_evidence_call(
        runtime,
        "production_sampler_checkpoint",
        lambda: runtime.evidence.record_production_sampler_checkpoint(trader_id=runtime.trader_id, symbol=runtime.symbol, checkpoint=production_sampler_state),
    )
    if shadow_sampler_state is not None:
        safe_evidence_call(
            runtime,
            "shadow_sampler_checkpoint",
            lambda: runtime.evidence.record_shadow_sampler_checkpoint(trader_id=runtime.trader_id, symbol=runtime.symbol, checkpoint=shadow_sampler_state),
        )


def evidence_after_direction(signal: Signal) -> str | None:
    if signal.signal_type == "exit":
        return "flat"
    return signal.side


def evidence_before_direction(trade_snapshot: dict[str, Any]) -> str:
    trade = trade_snapshot.get("trade") if isinstance(trade_snapshot, dict) else {}
    position = trade.get("position") if isinstance(trade, dict) else None
    if isinstance(position, dict) and position.get("side"):
        return str(position["side"])
    return "flat"


def load_state_snapshot_payload(root: Path, trader_id: str) -> tuple[Path, dict[str, Any] | None, dict[str, Any] | None, str | None]:
    path = state_snapshot_path(root, trader_id)
    if not path.exists():
        return path, None, None, None
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    snapshot = payload.get("engine") if isinstance(payload, dict) else None
    if not isinstance(snapshot, dict):
        return path, payload if isinstance(payload, dict) else None, None, "state file missing engine snapshot"
    return path, payload, snapshot, None


def validate_renko_state_contract(runtime: TraderRuntime, state_payload: dict[str, Any], state_path: Path) -> None:
    if not runtime.enabled:
        return
    expected = runtime.contract_fingerprints
    if not expected:
        raise RuntimeError(f"{runtime.trader_id}: missing runtime Renko contract fingerprints.")
    stored = state_payload.get("contract_fingerprints")
    if not isinstance(stored, dict):
        raise RuntimeError(
            f"{runtime.trader_id}: existing state {state_path} has no canonical Renko contract fingerprints; "
            "it cannot be reused under the canonical 1-second contract."
        )
    for layer in ("market_stream", "renko_strategy"):
        expected_layer = expected.get(layer) if isinstance(expected, dict) else None
        stored_layer = stored.get(layer) if isinstance(stored, dict) else None
        expected_fingerprint = expected_layer.get("fingerprint") if isinstance(expected_layer, dict) else None
        stored_fingerprint = stored_layer.get("fingerprint") if isinstance(stored_layer, dict) else None
        if not expected_fingerprint or stored_fingerprint != expected_fingerprint:
            raise RuntimeError(
                f"{runtime.trader_id}: state {state_path} {layer} contract fingerprint mismatch; "
                "reconstruct canonical state before startup."
            )
    if not isinstance(state_payload.get("sampler_state"), dict):
        raise RuntimeError(
            f"{runtime.trader_id}: state {state_path} has no canonical sampler checkpoint; "
            "reconstruct canonical state before startup."
        )


def restore_canonical_market_checkpoints(runtime: TraderRuntime, state_payload: dict[str, Any] | None) -> None:
    if not isinstance(state_payload, dict):
        return
    checkpoints = state_payload.get("canonical_market_checkpoints")
    if not isinstance(checkpoints, dict):
        return
    processed = checkpoints.get("processed_event_ids")
    if isinstance(processed, dict):
        runtime.processed_canonical_event_ids.update(
            {str(symbol).upper(): str(event_id) for symbol, event_id in processed.items() if event_id}
        )


def shared_canonical_event_id_from_tick(runtime: TraderRuntime, tick: MarketTick) -> str | None:
    if runtime.market_data_source != "shared_canonical_stream":
        return None
    event_id = str(tick.trade_id or "").strip()
    return event_id or None


def mark_processed_shared_canonical_event(runtime: TraderRuntime, tick: MarketTick) -> None:
    event_id = shared_canonical_event_id_from_tick(runtime, tick)
    if event_id:
        runtime.processed_canonical_event_ids[runtime.symbol] = event_id


def restore_engine_state(root: Path, trader_id: str, engine: RenkoStrategyEngine) -> tuple[bool, str | None]:
    path, _payload, snapshot, error = load_state_snapshot_payload(root, trader_id)
    if snapshot is None and error is None:
        return False, None
    if error:
        return False, error
    engine.restore_snapshot(snapshot)
    return True, str(path)


def snapshot_brick_size(snapshot: dict[str, Any] | None) -> float | None:
    renko = snapshot.get("renko") if isinstance(snapshot, dict) else None
    if not isinstance(renko, dict) or renko.get("brick_size") is None:
        return None
    try:
        return float(renko["brick_size"])
    except (TypeError, ValueError):
        return None


def is_renko_brick_change(snapshot: dict[str, Any] | None, engine: RenkoStrategyEngine) -> bool:
    old_brick = snapshot_brick_size(snapshot)
    return old_brick is not None and abs(old_brick - float(engine.config.brick_size)) > 1e-9


def quarantine_state_snapshot(root: Path, trader_id: str, payload: dict[str, Any], reason: str) -> str:
    digest = sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:16]
    path = root / "state" / "rejected" / f"{slug(trader_id)}_{digest}.json"
    write_json_atomic(
        path,
        {
            "rejected_at": pd.Timestamp.now(tz="UTC").isoformat(),
            "trader_id": trader_id,
            "policy": RENKO_BRICK_CHANGE_MIGRATION_POLICY,
            "reason": reason,
            "snapshot_hash": digest,
            "original_state": payload,
        },
    )
    return str(path)


def broker_position_reference_price(runtime: TraderRuntime, broker_position: dict[str, Any] | None) -> tuple[float, str, str | None]:
    if broker_position is not None:
        current_price = as_float(broker_position.get("current_price"), default=0.0)
        if current_price > 0:
            return current_price, "broker_position.current_price", None
    if not isinstance(runtime.execution, AlpacaExecutionEngine):
        raise RuntimeError(f"{runtime.trader_id}: safe bootstrap price requires live Alpaca execution.")
    latest_price, latest_timestamp = runtime.execution.trader.get_latest_trade_price(runtime.symbol, feed=runtime.feed)
    return float(latest_price), "alpaca_latest_trade", latest_timestamp


def broker_position_to_trade_state(
    runtime: TraderRuntime,
    broker_position: dict[str, Any] | None,
    *,
    timestamp: pd.Timestamp,
    reference_price: float,
    previous_trade_state: dict[str, Any] | None,
) -> dict[str, Any]:
    force_flat_dates = []
    if isinstance(previous_trade_state, dict) and isinstance(previous_trade_state.get("force_flat_dates"), list):
        force_flat_dates = previous_trade_state["force_flat_dates"]
    if broker_position is None:
        return {
            "position": None,
            "psar_sar": None,
            "psar_ep": None,
            "psar_af": None,
            "force_flat_dates": force_flat_dates,
        }
    side = str(broker_position.get("side", "")).lower().strip()
    if side not in {"long", "short"}:
        raise RuntimeError(f"{runtime.trader_id}: broker position side is ambiguous: {broker_position!r}")
    qty = abs(as_float(broker_position.get("qty"), default=0.0))
    if qty <= 0:
        raise RuntimeError(f"{runtime.trader_id}: broker position quantity is ambiguous: {broker_position!r}")
    entry_price = as_float(broker_position.get("avg_entry_price"), default=0.0) or reference_price
    return {
        "position": {
            "symbol": runtime.symbol,
            "side": side,
            "entry_time": timestamp.isoformat(),
            "entry_price": float(entry_price),
            "qty": float(qty),
        },
        "psar_sar": None,
        "psar_ep": None,
        "psar_af": None,
        "force_flat_dates": force_flat_dates,
    }


def migration_evidence_path(root: Path, trader_id: str, timestamp: pd.Timestamp) -> Path:
    stamp = timestamp.strftime("%Y%m%dT%H%M%SZ")
    return root / "migrations" / f"{slug(trader_id)}_{stamp}.json"


def update_migration_evidence_once(runtime: TraderRuntime, field: str, value: Any) -> None:
    if runtime.migration_evidence_path is None or value is None:
        return
    path = Path(runtime.migration_evidence_path)
    if not path.exists():
        return
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get(field) is not None:
        return
    payload[field] = value.isoformat() if hasattr(value, "isoformat") else value
    write_json_atomic(path, payload)


def migrate_renko_brick_change_state(
    root: Path,
    runtime: TraderRuntime,
    *,
    state_payload: dict[str, Any],
    state_snapshot: dict[str, Any],
    state_path: Path,
    restore_error: str,
) -> str:
    if not isinstance(runtime.execution, AlpacaExecutionEngine):
        raise RuntimeError(f"{runtime.trader_id}: brick-size migration requires broker reconciliation.")
    timestamp = pd.Timestamp.now(tz="UTC")
    renko = state_snapshot.get("renko") if isinstance(state_snapshot, dict) else {}
    trade = state_snapshot.get("trade") if isinstance(state_snapshot, dict) else {}
    if not isinstance(renko, dict):
        raise RuntimeError(f"{runtime.trader_id}: incompatible state is missing Renko details.")
    old_brick_size = float(renko.get("brick_size"))
    new_brick_size = float(runtime.engine.config.brick_size)
    rejected_path = quarantine_state_snapshot(root, runtime.trader_id, state_payload, restore_error)
    broker_before = runtime.execution.trader._get_position(runtime.symbol)
    reference_price, reference_source, reference_timestamp = broker_position_reference_price(runtime, broker_before)

    runtime.engine.renko_state = RenkoBuildState(
        brick_size=new_brick_size,
        anchor_mode=runtime.engine.config.renko_anchor_mode,
        anchor_price=runtime.engine.config.renko_anchor_price,
    )
    runtime.engine.renko_state.last_brick_close = initial_renko_close(reference_price, runtime.engine.renko_state)
    runtime.engine.renko_state.bricks.clear()
    runtime.engine.renko_state.run_dir = 0
    runtime.engine.renko_state.run_len = 0
    runtime.engine.processed_bricks = 0

    previous_trade_state = trade if isinstance(trade, dict) else None
    runtime.engine.restore_trade_state(
        broker_position_to_trade_state(
            runtime,
            broker_before,
            timestamp=timestamp,
            reference_price=reference_price,
            previous_trade_state=previous_trade_state,
        )
    )
    broker_after = runtime.execution.trader._get_position(runtime.symbol)
    path = migration_evidence_path(root, runtime.trader_id, timestamp)
    strategy_before = previous_trade_state.get("position") if isinstance(previous_trade_state, dict) else None
    strategy_after = runtime.engine.trade_state_snapshot().get("position")
    evidence = {
        "policy": RENKO_BRICK_CHANGE_MIGRATION_POLICY,
        "symbol": runtime.symbol,
        "trader_id": runtime.trader_id,
        "previous_config_hash": state_payload.get("configuration_hash") or state_payload.get("config_hash"),
        "new_config_hash": runtime.config_hash,
        "previous_brick_size": old_brick_size,
        "new_brick_size": new_brick_size,
        "previous_configured_quantity": state_payload.get("configured_quantity") or (strategy_before or {}).get("qty"),
        "new_configured_quantity": runtime.engine.config.qty,
        "broker_position_before_migration": broker_before,
        "strategy_position_before_migration": strategy_before,
        "rejected_renko_state_identifier": rejected_path,
        "rejected_renko_state_source": str(state_path),
        "rejected_renko_state_reason": restore_error,
        "bootstrap_reference_price": reference_price,
        "bootstrap_source": reference_source,
        "bootstrap_source_timestamp": reference_timestamp,
        "bootstrap_timestamp": timestamp.isoformat(),
        "first_post_bootstrap_market_observation_timestamp": None,
        "first_post_bootstrap_brick_timestamp": None,
        "first_eligible_signal_timestamp": None,
        "broker_position_after_migration": broker_after,
        "strategy_position_after_migration": strategy_after,
        "startup_orders": 0,
        "startup_fills": 0,
    }
    write_json_atomic(path, evidence)
    object.__setattr__(runtime, "migration_evidence_path", str(path))
    runtime.log.write("renko_state_migrated", evidence)
    archive_engine_state(root, runtime, reason="renko_brick_change_migration_bootstrap")
    return str(path)


def warm_start_runtime_from_archived_ticks(root: Path, runtime: TraderRuntime, days: int) -> int:
    if days <= 0:
        return 0

    ticks_root = root / "ticks"
    if not ticks_root.exists():
        return 0

    cutoff = date.today() - timedelta(days=days)
    tick_files: list[Path] = []
    for path in sorted(ticks_root.glob(f"*/{runtime.symbol.upper()}.jsonl")):
        try:
            path_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if path_date >= cutoff:
            tick_files.append(path)

    ticks: list[MarketTick] = []
    seen_ticks: set[tuple[str, str, float, float, str]] = set()
    raw_tick_count = 0
    for path in tick_files:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                payload = json.loads(line)
                raw_tick_count += 1
                tick = MarketTick(
                    timestamp=pd.Timestamp(payload["timestamp"]),
                    symbol=str(payload["symbol"]).upper(),
                    price=float(payload["price"]),
                    size=float(payload.get("size", 0.0) or 0.0),
                    exchange=str(payload.get("exchange", "")),
                    trade_id=str(payload.get("trade_id", "")),
                    tape=str(payload.get("tape", "")),
                    conditions=tuple(str(item) for item in payload.get("conditions", []) if item is not None),
                )
                dedupe_key = (
                    tick.timestamp.isoformat(),
                    tick.trade_id,
                    tick.price,
                    tick.size,
                    tick.exchange,
                )
                if dedupe_key in seen_ticks:
                    continue
                seen_ticks.add(dedupe_key)
                ticks.append(tick)

    ticks.sort(key=lambda tick: tick.timestamp)
    for tick in ticks:
        for source_tick, _observation in source_observations_for_runtime(runtime, tick, None):
            runtime.engine.on_tick(source_tick)

    if ticks:
        runtime.log.write(
            "state_warm_started",
            {
                "trader_id": runtime.trader_id,
                "symbol": runtime.symbol,
                "source": "archived_ticks",
                "days": days,
                "tick_count": len(ticks),
                "raw_tick_count": raw_tick_count,
                "skipped_duplicate_ticks": raw_tick_count - len(ticks),
                "first_tick": ticks[0].timestamp.isoformat(),
                "last_tick": ticks[-1].timestamp.isoformat(),
            },
        )
        archive_engine_state(root, runtime, reason="warm_started_archived_ticks")

    return len(ticks)


def is_renko_tp_sl_runtime(runtime: TraderRuntime) -> bool:
    return isinstance(runtime.engine, RenkoStrategyEngine) and runtime.engine.config.strategy_type == "renko_tp_sl"


def is_reversal_catastrophic_stop_runtime(runtime: TraderRuntime) -> bool:
    return (
        isinstance(runtime.engine, RenkoStrategyEngine)
        and runtime.engine.config.strategy_type == "renko_reversal"
        and str(getattr(runtime.engine.config, "exit_management", "")).lower().strip() == "catastrophic_stop"
    )


def halt_runtime(root: Path, runtime: TraderRuntime, reason: str, payload: dict[str, Any] | None = None) -> None:
    object.__setattr__(runtime, "enabled", False)
    if hasattr(runtime.engine, "mark_reconciliation_unsafe"):
        transition = runtime.engine.mark_reconciliation_unsafe(reason, evidence=payload or {})
        log_reconciliation_transition(runtime, transition)
    runtime.log.write(
        "critical_runtime_halt",
        {
            "trader_id": runtime.trader_id,
            "symbol": runtime.symbol,
            "reason": reason,
            "payload": payload or {},
            "timestamp": pd.Timestamp.now(tz="UTC").isoformat(),
        },
    )
    archive_engine_state(root, runtime, reason=f"halt_{reason}")


def log_reconciliation_transition(runtime: TraderRuntime, transition: dict[str, Any] | None) -> None:
    if not transition:
        return
    runtime.log.write(
        "reconciliation_safety_transition",
        {
            "trader_id": runtime.trader_id,
            "account_namespace": runtime.account_namespace,
            **transition,
        },
    )


def mark_reconciliation_safe(
    root: Path,
    runtime: TraderRuntime,
    *,
    reason: str,
    evidence: dict[str, Any],
) -> None:
    if not hasattr(runtime.engine, "mark_reconciliation_safe"):
        return
    transition = runtime.engine.mark_reconciliation_safe(reason, evidence=evidence)
    log_reconciliation_transition(runtime, transition)
    if transition:
        archive_engine_state(root, runtime, reason=f"reconciliation_safe_{reason}")


def _decimal_equal(left: Decimal, right: Decimal) -> bool:
    return abs(left - right) <= Decimal("0.000001")


def tp_sl_reconciliation_error(runtime: TraderRuntime, broker_position: dict[str, Any], engine_position: Any) -> dict[str, Any] | None:
    authoritative = authoritative_position_from_alpaca(broker_position, symbol=runtime.symbol)
    if authoritative.is_flat:
        return {
            "reason": "broker_position_quantity_invalid",
            "broker_position": broker_position,
            "authoritative_position": {
                "source": authoritative.source,
                "side": authoritative.side,
                "qty": str(authoritative.qty),
            },
        }
    configured_qty = getattr(runtime.engine.config, "qty", None)
    if configured_qty is not None:
        configured = Decimal(str(configured_qty))
        if not _decimal_equal(authoritative.qty, configured):
            return {
                "reason": "broker_position_quantity_violates_tp_sl_invariant",
                "broker_position": broker_position,
                "broker_side": authoritative.side,
                "broker_qty": str(authoritative.qty),
                "configured_target_qty": str(configured),
            }

    lifecycle_state = str(getattr(runtime.engine, "tp_sl_lifecycle_state", ""))
    if lifecycle_state in TP_SL_EXIT_LOCK_STATES:
        return None
    if isinstance(engine_position, dict):
        engine_side = str(engine_position.get("side", "")).lower().strip()
        engine_qty = Decimal(str(engine_position.get("qty") or 0))
        if engine_side in {"long", "short"} and authoritative.side != engine_side:
            return {
                "reason": "broker_engine_side_mismatch",
                "broker_position": broker_position,
                "broker_side": authoritative.side,
                "engine_position": engine_position,
            }
        if engine_qty > 0 and not _decimal_equal(authoritative.qty, abs(engine_qty)):
            return {
                "reason": "broker_engine_quantity_mismatch",
                "broker_position": broker_position,
                "broker_qty": str(authoritative.qty),
                "engine_position": engine_position,
            }
    return None


def broker_nested_open_orders(runtime: TraderRuntime) -> list[dict[str, Any]]:
    if not isinstance(runtime.execution, AlpacaExecutionEngine):
        return []
    trader = runtime.execution.trader
    if hasattr(trader, "get_open_orders_nested"):
        return list(trader.get_open_orders_nested())
    if hasattr(trader, "get_orders"):
        try:
            return list(trader.get_orders(status="open", limit=500, nested=True))
        except TypeError:
            return list(trader.get_orders(status="open", limit=500))
    return []


def _order_ids(order: dict[str, Any]) -> set[str]:
    ids: set[str] = set()
    if order.get("id"):
        ids.add(str(order["id"]))
    for leg in order.get("legs") or []:
        if isinstance(leg, dict) and leg.get("id"):
            ids.add(str(leg["id"]))
    return ids


def _summarize_order(order: dict[str, Any]) -> dict[str, Any]:
    summary = {
        key: order.get(key)
        for key in (
            "id",
            "client_order_id",
            "symbol",
            "side",
            "type",
            "order_type",
            "order_class",
            "qty",
            "filled_qty",
            "status",
            "parent_order_id",
            "limit_price",
            "stop_price",
        )
        if key in order
    }
    legs = []
    for leg in order.get("legs") or []:
        if not isinstance(leg, dict):
            continue
        legs.append(
            {
                key: leg.get(key)
                for key in (
                    "id",
                    "client_order_id",
                    "symbol",
                    "side",
                    "type",
                    "order_type",
                    "order_class",
                    "qty",
                    "filled_qty",
                    "status",
                    "parent_order_id",
                    "limit_price",
                    "stop_price",
                )
                if key in leg
            }
        )
    if legs:
        summary["legs"] = legs
    return summary


def symbol_open_orders(runtime: TraderRuntime) -> list[dict[str, Any]]:
    return [
        order
        for order in broker_nested_open_orders(runtime)
        if isinstance(order, dict) and str(order.get("symbol") or "").upper().strip() == runtime.symbol
    ]


def unexpected_symbol_open_orders(runtime: TraderRuntime, *, allowed_order_ids: set[str] | None = None) -> list[dict[str, Any]]:
    allowed = allowed_order_ids or set()
    unexpected: list[dict[str, Any]] = []
    for order in symbol_open_orders(runtime):
        ids = _order_ids(order)
        if ids and ids <= allowed:
            continue
        unexpected.append(_summarize_order(order))
    return unexpected


def protection_allowed_order_ids(verification: Any) -> set[str]:
    ids = {
        getattr(verification, "parent_order_id", None),
        getattr(verification, "take_profit_order_id", None),
        getattr(verification, "stop_loss_order_id", None),
        getattr(verification, "stop_order_id", None),
    }
    return {str(item) for item in ids if item}


def reconciliation_evidence(
    runtime: TraderRuntime,
    *,
    broker_position: dict[str, Any] | None,
    engine_position: Any,
    open_orders: list[dict[str, Any]],
    verification: Any | None = None,
) -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "symbol": runtime.symbol,
        "broker_position": broker_position,
        "engine_position": engine_position,
        "open_orders": [_summarize_order(order) for order in open_orders],
    }
    if verification is not None:
        evidence["verification"] = {
            "protected": bool(getattr(verification, "protected", False)),
            "state": getattr(verification, "state", None),
            "parent_order_id": getattr(verification, "parent_order_id", None),
            "take_profit_order_id": getattr(verification, "take_profit_order_id", None),
            "stop_loss_order_id": getattr(verification, "stop_loss_order_id", None),
            "protected_quantity": getattr(verification, "protected_quantity", None),
            "issues": [issue.__dict__ for issue in getattr(verification, "issues", ())],
        }
    return evidence


def broker_nested_protection_orders(runtime: TraderRuntime) -> list[dict[str, Any]]:
    orders = broker_nested_open_orders(runtime)
    protection_state = getattr(runtime.engine, "protection_state", None)
    parent_order_id = (
        str(protection_state.get("parent_order_id")).strip()
        if isinstance(protection_state, dict) and protection_state.get("parent_order_id")
        else ""
    )
    trader = runtime.execution.trader if isinstance(runtime.execution, AlpacaExecutionEngine) else None
    if parent_order_id and trader is not None and hasattr(trader, "get_order_nested"):
        parent = trader.get_order_nested(parent_order_id)
        if isinstance(parent, dict):
            existing_ids = {str(order.get("id")) for order in orders if isinstance(order, dict) and order.get("id")}
            if str(parent.get("id")) not in existing_ids:
                orders.append(parent)
    return orders


def runtime_expected_market_contract_fingerprint(runtime: TraderRuntime) -> str | None:
    contract_fingerprints = runtime.contract_fingerprints or {}
    market_stream = contract_fingerprints.get("market_stream") if isinstance(contract_fingerprints, dict) else None
    if isinstance(market_stream, dict) and market_stream.get("fingerprint"):
        return str(market_stream["fingerprint"])
    return getattr(runtime.engine.config, "market_contract_fingerprint", None)


def current_exit_plan(runtime: TraderRuntime) -> dict[str, Any] | None:
    protection_state = getattr(runtime.engine, "protection_state", None)
    if isinstance(protection_state, dict) and isinstance(protection_state.get("exit_plan"), dict):
        return protection_state["exit_plan"]
    trade = runtime.engine.trade_state_snapshot()
    if isinstance(trade.get("protection_state"), dict) and isinstance(trade["protection_state"].get("exit_plan"), dict):
        return trade["protection_state"]["exit_plan"]
    return None


def current_reversal_stop_plan(runtime: TraderRuntime) -> dict[str, Any] | None:
    protection_state = getattr(runtime.engine, "protection_state", None)
    if isinstance(protection_state, dict) and isinstance(protection_state.get("stop_plan"), dict):
        return protection_state["stop_plan"]
    trade = runtime.engine.trade_state_snapshot()
    if isinstance(trade.get("protection_state"), dict) and isinstance(trade["protection_state"].get("stop_plan"), dict):
        return trade["protection_state"]["stop_plan"]
    return None


def verify_runtime_broker_protection(runtime: TraderRuntime, broker_position: dict[str, Any]) -> ProtectionVerification:
    protection_state = getattr(runtime.engine, "protection_state", None)
    return verify_broker_native_protection(
        symbol=runtime.symbol,
        broker_position=broker_position,
        nested_orders=broker_nested_protection_orders(runtime),
        exit_plan=current_exit_plan(runtime),
        protection_state=protection_state if isinstance(protection_state, dict) else None,
        expected_config_fingerprint=getattr(runtime.engine.config, "candidate_config_fingerprint", None),
        expected_market_contract_fingerprint=runtime_expected_market_contract_fingerprint(runtime),
    )


def verify_runtime_reversal_stop(runtime: TraderRuntime, broker_position: dict[str, Any]) -> ReversalStopVerification:
    return verify_reversal_catastrophic_stop(
        symbol=runtime.symbol,
        broker_position=broker_position,
        open_orders=broker_nested_open_orders(runtime),
        stop_plan=current_reversal_stop_plan(runtime),
        expected_config_fingerprint=getattr(runtime.engine.config, "candidate_config_fingerprint", None),
        expected_market_contract_fingerprint=runtime_expected_market_contract_fingerprint(runtime),
    )


def _recovery_attempt(runtime: TraderRuntime) -> int:
    protection_state = getattr(runtime.engine, "protection_state", None)
    if not isinstance(protection_state, dict):
        return 0
    if protection_state.get("recovery_attempt") is not None:
        return int(protection_state.get("recovery_attempt") or 0)
    legacy = protection_state.get("recovery_state") or protection_state.get("reversal_recovery_state")
    return int((legacy or {}).get("recovery_attempt") or 0) if isinstance(legacy, dict) else 0


def _max_recovery_attempts(runtime: TraderRuntime) -> int:
    return int(getattr(getattr(runtime.engine, "config", None), "protection_max_recovery_attempts", 1))


def _position_qty(position: dict[str, Any] | None) -> Decimal:
    if not isinstance(position, dict):
        return Decimal("0")
    return abs(Decimal(str(position.get("qty") or position.get("qty_available") or 0)))


def _format_qty(value: Decimal) -> str:
    return format(value, "f").rstrip("0").rstrip(".")


def _format_price(value: Decimal) -> str:
    return f"{value:.2f}"


def _recovery_client_order_id(plan: dict[str, Any], attempt: int, suffix: str) -> str:
    seed = "|".join(
        str(item)
        for item in (
            plan.get("lifecycle_id"),
            plan.get("client_order_id"),
            plan.get("candidate_config_fingerprint"),
            plan.get("market_contract_fingerprint"),
            attempt,
            suffix,
        )
    )
    return f"{str(plan.get('client_order_id') or 'recovery')[:31]}-{suffix}{attempt}-{sha256(seed.encode('utf-8')).hexdigest()[:10]}"[:48]


def _has_active_unmanaged_closing_order(runtime: TraderRuntime, broker_position: dict[str, Any]) -> bool:
    closing_side = "sell" if str(broker_position.get("side") or "").lower().strip() == "long" else "buy"
    for order in broker_nested_open_orders(runtime):
        if str(order.get("symbol") or "").upper() != runtime.symbol:
            continue
        if str(order.get("side") or "").lower().strip() != closing_side:
            continue
        if str(order.get("order_class") or "").lower().strip() in {"bracket", "oco"}:
            continue
        if order.get("stop_price") is None:
            return True
    return False


def _recovery_oco_payload(symbol: str, broker_position: dict[str, Any], exit_plan: dict[str, Any], attempt: int) -> dict[str, Any]:
    position_side = str(broker_position.get("side") or "").lower().strip()
    if position_side not in {"long", "short"}:
        raise ValueError("Broker position side must be long or short.")
    qty = _position_qty(broker_position)
    if qty <= 0:
        raise ValueError("Recovery OCO quantity must be positive.")
    base = Decimal(str(broker_position.get("avg_entry_price") or exit_plan.get("entry_reference_price") or 0))
    current = Decimal(str(broker_position.get("current_price") or 0))
    tp = Decimal(str(exit_plan["take_profit_limit_price"]))
    sl = Decimal(str(exit_plan["stop_loss_trigger_price"]))
    if position_side == "long" and (tp <= base or sl >= base or (current > 0 and (tp <= current or sl >= current))):
        raise ValueError("Invalid long recovery TP/SL relationship.")
    if position_side == "short" and (tp >= base or sl <= base or (current > 0 and (tp >= current or sl <= current))):
        raise ValueError("Invalid short recovery TP/SL relationship.")
    exit_side = "sell" if position_side == "long" else "buy"
    return {
        "symbol": symbol.upper(),
        "side": exit_side,
        "type": "limit",
        "time_in_force": "gtc",
        "qty": _format_qty(qty),
        "order_class": "oco",
        "client_order_id": _recovery_client_order_id(exit_plan, attempt, "r"),
        "extended_hours": False,
        "take_profit": {"limit_price": _format_price(tp)},
        "stop_loss": {"stop_price": _format_price(sl)},
    }


def _recover_runtime_broker_protection(root: Path, runtime: TraderRuntime, broker_position: dict[str, Any] | None = None) -> str | None:
    if not isinstance(runtime.execution, AlpacaExecutionEngine):
        return None
    if not (
        is_renko_tp_sl_runtime(runtime)
        and str(getattr(runtime.engine.config, "exit_management", "")).lower().strip() == "bracket"
    ):
        return None
    trader = runtime.execution.trader
    required_methods = ("get_position", "get_open_orders_nested", "submit_recovery_oco")
    if not all(hasattr(trader, method) for method in required_methods):
        return None
    exit_plan = current_exit_plan(runtime)
    if not isinstance(exit_plan, dict):
        return None
    protection_state = getattr(runtime.engine, "protection_state", None)
    if not isinstance(protection_state, dict) or not protection_state.get("parent_order_id"):
        return None
    position = broker_position if isinstance(broker_position, dict) else trader.get_position(runtime.symbol)
    verification = verify_runtime_broker_protection(runtime, position)
    if verification.protected:
        runtime.engine.apply_tp_sl_protection_evidence(
            exit_plan=exit_plan,
            verification=verification,
            broker_position=position,
        )
        return str(getattr(runtime.engine, "tp_sl_lifecycle_state", ""))
    if not isinstance(position, dict) or _position_qty(position) <= 0:
        return None
    attempt = _recovery_attempt(runtime) + 1
    if attempt > _max_recovery_attempts(runtime) or _has_active_unmanaged_closing_order(runtime, position):
        return None
    try:
        order = _recovery_oco_payload(runtime.symbol, position, exit_plan, attempt)
        response = trader.submit_recovery_oco(order)
        recovered = verify_broker_native_protection(
            symbol=runtime.symbol,
            broker_position=trader.get_position(runtime.symbol),
            nested_orders=broker_nested_open_orders(runtime),
            exit_plan=exit_plan,
            protection_state={"parent_order_id": str(response.get("id") or "")},
            expected_config_fingerprint=getattr(runtime.engine.config, "candidate_config_fingerprint", None),
            expected_market_contract_fingerprint=runtime_expected_market_contract_fingerprint(runtime),
        )
    except Exception as exc:
        if hasattr(runtime.engine, "mark_position_unprotected"):
            runtime.engine.mark_position_unprotected("broker_native_recovery_exception", {"error": str(exc)})
        runtime.log.write(
            "critical_protection_recovery_error",
            {"trader_id": runtime.trader_id, "symbol": runtime.symbol, "error": str(exc)},
        )
        archive_engine_state(root, runtime, reason="broker_native_recovery_exception")
        raise BrokerEngineStateMismatch(f"{runtime.trader_id} {runtime.symbol} broker protection recovery failed: {exc}") from exc

    runtime.engine.apply_tp_sl_protection_evidence(
        exit_plan=exit_plan,
        verification=recovered,
        broker_position=trader.get_position(runtime.symbol),
        extra={"recovery_attempt": attempt, "recovery_order": order, "recovery_response": response},
    )

    runtime.log.write(
        "broker_native_protection_recovery",
        {
            "trader_id": runtime.trader_id,
            "symbol": runtime.symbol,
            "state": getattr(runtime.engine, "tp_sl_lifecycle_state", None),
            "recovery_attempt": attempt,
            "recovery_client_order_id": order["client_order_id"],
            "verification_protected": recovered.protected,
            "verification_issues": [issue.__dict__ for issue in recovered.issues],
        },
    )
    archive_engine_state(root, runtime, reason="broker_native_protection_recovery")

    if recovered.protected:
        return str(getattr(runtime.engine, "tp_sl_lifecycle_state", ""))

    raise BrokerEngineStateMismatch(
        f"{runtime.trader_id} {runtime.symbol} broker protection recovery failed verification"
    )


def _active_single_stop_order_id(runtime: TraderRuntime) -> str | None:
    ids = [
        str(order.get("id"))
        for order in broker_nested_open_orders(runtime)
        if isinstance(order, dict)
        and str(order.get("symbol") or "").upper() == runtime.symbol
        and order.get("stop_price") is not None
        and str(order.get("order_class") or "").lower().strip() not in {"bracket", "oco"}
    ]
    return ids[0] if len(ids) == 1 else None


def _reversal_stop_plan_for_position(runtime: TraderRuntime, broker_position: dict[str, Any] | None) -> dict[str, Any] | None:
    existing = current_reversal_stop_plan(runtime)
    if isinstance(existing, dict):
        return existing
    if not isinstance(broker_position, dict) or _position_qty(broker_position) <= 0:
        return None
    side = str(broker_position.get("side") or "").lower().strip()
    entry_side = "buy" if side == "long" else "sell"
    return build_reversal_catastrophic_stop_plan(
        trader_id=runtime.trader_id,
        strategy_id=getattr(runtime.engine.config, "strategy_name", runtime.trader_id),
        symbol=runtime.symbol,
        entry_side=entry_side,
        quantity=_position_qty(broker_position),
        entry_reference_price=broker_position.get("avg_entry_price") or broker_position.get("current_price"),
        brick_size=getattr(runtime.engine.config, "brick_size"),
        catastrophic_stop_bricks=int(getattr(runtime.engine.config, "catastrophic_stop_bricks") or 0),
        client_order_id=_recovery_client_order_id({"client_order_id": runtime.trader_id}, 1, "cat"),
        candidate_config_fingerprint=getattr(runtime.engine.config, "candidate_config_fingerprint", None),
        market_contract_fingerprint=runtime_expected_market_contract_fingerprint(runtime),
    )


def _recover_runtime_reversal_catastrophic_stop(root: Path, runtime: TraderRuntime, broker_position: dict[str, Any] | None = None) -> str | None:
    if not isinstance(runtime.execution, AlpacaExecutionEngine):
        return None
    if not is_reversal_catastrophic_stop_runtime(runtime):
        return None
    trader = runtime.execution.trader
    required_methods = ("get_position", "get_open_orders_nested", "submit_catastrophic_stop")
    if not all(hasattr(trader, method) for method in required_methods):
        return None
    position = broker_position if isinstance(broker_position, dict) else trader.get_position(runtime.symbol)
    stop_plan = _reversal_stop_plan_for_position(runtime, position)
    verification = verify_reversal_catastrophic_stop(
        symbol=runtime.symbol,
        broker_position=position,
        open_orders=broker_nested_open_orders(runtime),
        stop_plan=stop_plan,
        expected_config_fingerprint=getattr(runtime.engine.config, "candidate_config_fingerprint", None),
        expected_market_contract_fingerprint=runtime_expected_market_contract_fingerprint(runtime),
    )
    if verification.protected and isinstance(stop_plan, dict):
        runtime.engine.apply_reversal_stop_evidence(
            stop_plan=stop_plan,
            verification=verification,
            broker_position=position,
        )
        return str(getattr(runtime.engine, "tp_sl_lifecycle_state", ""))
    if not isinstance(position, dict) or _position_qty(position) <= 0 or not isinstance(stop_plan, dict):
        return None
    if _active_single_stop_order_id(runtime) is not None:
        return None
    attempt = _recovery_attempt(runtime) + 1
    if attempt > _max_recovery_attempts(runtime):
        return None
    try:
        order = catastrophic_stop_order_payload(stop_plan)
        response = trader.submit_catastrophic_stop(order)
        recovered = verify_reversal_catastrophic_stop(
            symbol=runtime.symbol,
            broker_position=trader.get_position(runtime.symbol),
            open_orders=broker_nested_open_orders(runtime),
            stop_plan=stop_plan,
            expected_config_fingerprint=getattr(runtime.engine.config, "candidate_config_fingerprint", None),
            expected_market_contract_fingerprint=runtime_expected_market_contract_fingerprint(runtime),
        )
    except Exception as exc:
        if hasattr(runtime.engine, "mark_position_unprotected"):
            runtime.engine.mark_position_unprotected("reversal_catastrophic_stop_recovery_exception", {"error": str(exc)})
        runtime.log.write(
            "critical_reversal_catastrophic_stop_recovery_error",
            {"trader_id": runtime.trader_id, "symbol": runtime.symbol, "error": str(exc)},
        )
        archive_engine_state(root, runtime, reason="reversal_catastrophic_stop_recovery_exception")
        raise BrokerEngineStateMismatch(f"{runtime.trader_id} {runtime.symbol} catastrophic stop recovery failed: {exc}") from exc

    runtime.engine.apply_reversal_stop_evidence(
        stop_plan=stop_plan,
        verification=recovered,
        broker_position=trader.get_position(runtime.symbol),
        extra={"recovery_attempt": attempt, "recovery_order": order, "recovery_response": response},
    )

    runtime.log.write(
        "reversal_catastrophic_stop_recovery",
        {
            "trader_id": runtime.trader_id,
            "symbol": runtime.symbol,
            "state": getattr(runtime.engine, "tp_sl_lifecycle_state", None),
            "recovery_attempt": attempt,
            "recovery_client_order_id": order["client_order_id"],
            "verification_protected": recovered.protected,
            "verification_issues": [issue.__dict__ for issue in recovered.issues],
        },
    )
    archive_engine_state(root, runtime, reason="reversal_catastrophic_stop_recovery")

    if recovered.protected:
        return str(getattr(runtime.engine, "tp_sl_lifecycle_state", ""))
    raise BrokerEngineStateMismatch(
        f"{runtime.trader_id} {runtime.symbol} catastrophic stop recovery failed verification"
    )


def matching_external_close_fill(runtime: TraderRuntime, engine_position: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(runtime.execution, AlpacaExecutionEngine):
        return None
    trader = runtime.execution.trader
    if not hasattr(trader, "get_recent_fills"):
        return None
    expected_side = "sell" if str(engine_position.get("side") or "").lower().strip() == "long" else "buy"
    expected_qty = Decimal(str(abs(float(engine_position.get("qty") or 0))))
    for fill in trader.get_recent_fills(runtime.symbol):
        side = str(fill.get("side") or "").lower().strip()
        qty = Decimal(str(fill.get("qty") or 0))
        if side == expected_side and qty >= expected_qty:
            return fill
    return None


def _tp_sl_protection_state(runtime: TraderRuntime) -> dict[str, Any] | None:
    protection_state = getattr(runtime.engine, "protection_state", None)
    if isinstance(protection_state, dict):
        return protection_state
    trade = runtime.engine.trade_state_snapshot()
    protection_state = trade.get("protection_state") if isinstance(trade.get("protection_state"), dict) else None
    return protection_state if isinstance(protection_state, dict) else None


def _known_tp_sl_child_close_reason(runtime: TraderRuntime, order_id: str) -> str | None:
    protection_state = _tp_sl_protection_state(runtime) or {}
    if order_id and order_id == str(protection_state.get("take_profit_order_id") or ""):
        return f"take_profit_{getattr(runtime.engine.config, 'take_profit_bricks', '')}_bricks"
    if order_id and order_id == str(protection_state.get("stop_loss_order_id") or ""):
        return f"stop_loss_{getattr(runtime.engine.config, 'stop_loss_bricks', '')}_bricks"
    return None


def matching_broker_native_tp_sl_close_fill(runtime: TraderRuntime, engine_position: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(runtime.execution, AlpacaExecutionEngine):
        return None
    protection_state = _tp_sl_protection_state(runtime)
    if not isinstance(protection_state, dict):
        return None
    known_child_ids = {
        str(protection_state.get("take_profit_order_id") or "").strip(),
        str(protection_state.get("stop_loss_order_id") or "").strip(),
    }
    known_child_ids.discard("")
    if not known_child_ids:
        return None
    trader = runtime.execution.trader
    if not hasattr(trader, "get_recent_fills"):
        return None

    symbol = runtime.symbol.upper()
    position_side = str(engine_position.get("side") or "").lower().strip()
    expected_side = "sell" if position_side == "long" else "buy"
    expected_qty = Decimal(str(abs(float(engine_position.get("qty") or 0))))
    if expected_qty <= 0:
        return None

    for fill in trader.get_recent_fills(runtime.symbol):
        order_id = str(fill.get("order_id") or "").strip()
        if order_id not in known_child_ids:
            continue
        if str(fill.get("symbol") or "").upper().strip() != symbol:
            continue
        if str(fill.get("side") or "").lower().strip() != expected_side:
            continue
        qty = Decimal(str(fill.get("qty") or fill.get("filled_qty") or 0))
        if not _decimal_equal(qty, expected_qty):
            continue
        reason = _known_tp_sl_child_close_reason(runtime, order_id)
        if reason is None:
            continue
        result = dict(fill)
        result["broker_native_tp_sl_reason"] = reason
        return result
    return None


def broker_flat_orders_are_known_tp_sl_cleanup(runtime: TraderRuntime, orders: list[dict[str, Any]]) -> bool:
    protection_state = _tp_sl_protection_state(runtime)
    if not isinstance(protection_state, dict):
        return False
    known_ids = {
        str(protection_state.get("parent_order_id") or "").strip(),
        str(protection_state.get("take_profit_order_id") or "").strip(),
        str(protection_state.get("stop_loss_order_id") or "").strip(),
    }
    known_ids.discard("")
    parent_id = str(protection_state.get("parent_order_id") or "").strip()
    if not known_ids:
        return False

    symbol = runtime.symbol.upper()
    relevant_orders = []
    for order in orders:
        if not isinstance(order, dict):
            continue
        order_symbols = {str(order.get("symbol") or "").upper().strip()}
        for leg in order.get("legs") or []:
            if isinstance(leg, dict):
                order_symbols.add(str(leg.get("symbol") or "").upper().strip())
        if symbol in order_symbols:
            relevant_orders.append(order)
    if not relevant_orders:
        return True

    for order in relevant_orders:
        ids = _order_ids(order)
        if not ids or not ids <= known_ids:
            return False
        order_parent = str(order.get("parent_order_id") or order.get("parent_id") or "").strip()
        if order_parent and parent_id and order_parent != parent_id:
            return False
        for leg in order.get("legs") or []:
            if not isinstance(leg, dict):
                continue
            leg_parent = str(leg.get("parent_order_id") or leg.get("parent_id") or "").strip()
            if leg_parent and parent_id and leg_parent != parent_id:
                return False
    return True


def reconcile_broker_native_tp_sl_close_fill(
    root: Path,
    runtime: TraderRuntime,
    engine_trade: dict[str, Any],
    engine_position: dict[str, Any],
    fill: dict[str, Any],
) -> None:
    entry_price = Decimal(str(engine_position.get("entry_price") or 0))
    fill_price = Decimal(str(fill.get("price") or fill.get("filled_avg_price") or 0))
    qty = Decimal(str(fill.get("qty") or fill.get("filled_qty") or 0))
    side_mult = Decimal("1") if engine_position.get("side") == "long" else Decimal("-1")
    realized_pnl = (fill_price - entry_price) * qty * side_mult
    runtime.engine.mark_external_close_filled(
        timestamp=fill.get("transaction_time") or fill.get("filled_at"),
        reason=str(fill["broker_native_tp_sl_reason"]),
        order_id=str(fill.get("order_id") or fill.get("id") or ""),
        source="alpaca_reconciliation_broker_native_tp_sl_fill",
        fill_price=str(fill_price),
        filled_qty=str(qty),
        realized_pnl=str(realized_pnl),
    )
    runtime.log.write(
        "state_reconciled",
        {
            "trader_id": runtime.trader_id,
            "symbol": runtime.symbol,
            "reason": "broker_flat_broker_native_tp_sl_close_reconciled",
            "fill": fill,
            "previous_trade_state": engine_trade,
        },
    )
    mark_reconciliation_safe(
        root,
        runtime,
        reason="broker_native_tp_sl_close_reconciled",
        evidence=reconciliation_evidence(
            runtime,
            broker_position=None,
            engine_position=None,
            open_orders=[],
            verification=None,
        ),
    )
    archive_engine_state(root, runtime, reason="broker_flat_broker_native_tp_sl_close_reconciled")


def matching_reversal_catastrophic_stop_fill(runtime: TraderRuntime, engine_position: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(runtime.execution, AlpacaExecutionEngine):
        return None
    trader = runtime.execution.trader
    protection_state = getattr(runtime.engine, "protection_state", None)
    if not isinstance(protection_state, dict):
        trade = runtime.engine.trade_state_snapshot()
        protection_state = trade.get("protection_state") if isinstance(trade.get("protection_state"), dict) else None
    if not isinstance(protection_state, dict):
        return None

    stop_order_id = str(protection_state.get("stop_order_id") or "").strip()
    stop_plan = protection_state.get("stop_plan") if isinstance(protection_state.get("stop_plan"), dict) else {}
    expected_client_order_id = str(protection_state.get("stop_client_order_id") or stop_plan.get("stop_client_order_id") or "").strip()
    if not stop_order_id:
        return None

    order: dict[str, Any] | None = None
    if hasattr(trader, "get_order"):
        order = trader.get_order(stop_order_id)
    elif hasattr(trader, "get_order_nested"):
        order = trader.get_order_nested(stop_order_id)
    if not isinstance(order, dict):
        return None
    if str(order.get("id") or "").strip() != stop_order_id:
        return None
    if expected_client_order_id and str(order.get("client_order_id") or "").strip() != expected_client_order_id:
        return None
    if str(order.get("symbol") or "").upper().strip() != runtime.symbol:
        return None

    position_side = str(engine_position.get("side") or "").lower().strip()
    expected_side = "sell" if position_side == "long" else "buy"
    if str(order.get("side") or "").lower().strip() != expected_side:
        return None
    order_type = str(order.get("type") or order.get("order_type") or "").lower().strip()
    if order_type not in {"stop", "stop_limit"}:
        return None
    if str(order.get("status") or "").lower().strip() != "filled":
        return None

    expected_qty = Decimal(str(abs(float(engine_position.get("qty") or 0))))
    filled_qty = Decimal(str(order.get("filled_qty") or 0))
    if expected_qty <= 0 or filled_qty < expected_qty:
        return None

    fill_payload: dict[str, Any] = {
        "id": order.get("id"),
        "order_id": order.get("id"),
        "symbol": order.get("symbol"),
        "side": order.get("side"),
        "qty": str(filled_qty),
        "price": order.get("filled_avg_price"),
        "transaction_time": order.get("filled_at"),
        "order": order,
        "stop_plan": stop_plan,
    }
    if hasattr(trader, "get_recent_fills"):
        for fill in trader.get_recent_fills(runtime.symbol):
            if str(fill.get("order_id") or "").strip() != stop_order_id:
                continue
            side = str(fill.get("side") or "").lower().strip()
            qty = Decimal(str(fill.get("qty") or 0))
            if side == expected_side and qty >= expected_qty:
                fill_payload.update(fill)
                break
    if not fill_payload.get("price"):
        return None
    return fill_payload


def last_closed_reversal_stop_is_terminal(runtime: TraderRuntime) -> bool:
    if not isinstance(runtime.execution, AlpacaExecutionEngine):
        return False
    last_closed = getattr(runtime.engine, "last_closed_exit", None)
    if not isinstance(last_closed, dict):
        return False
    if last_closed.get("reason") != "reversal_catastrophic_stop_broker_native":
        return False
    order_id = str(last_closed.get("order_id") or "").strip()
    if not order_id:
        return False
    trader = runtime.execution.trader
    order = trader.get_order(order_id) if hasattr(trader, "get_order") else None
    if not isinstance(order, dict):
        return False
    return str(order.get("status") or "").lower().strip() == "filled"


def log_protection_invariant_result(
    root: Path,
    runtime: TraderRuntime,
    verification: Any,
    broker_position: dict[str, Any] | None = None,
) -> None:
    if verification.protected:
        runtime.log.write(
            "broker_native_protection_verified",
            {
                "trader_id": runtime.trader_id,
                "symbol": runtime.symbol,
                "parent_order_id": getattr(verification, "parent_order_id", None),
                "take_profit_order_id": getattr(verification, "take_profit_order_id", None),
                "stop_loss_order_id": getattr(verification, "stop_loss_order_id", None),
                "stop_order_id": getattr(verification, "stop_order_id", None),
                "protected_quantity": verification.protected_quantity,
            },
        )
        return
    payload = {
        "trader_id": runtime.trader_id,
        "symbol": runtime.symbol,
        "state": verification.state,
        "remaining_position_quantity": getattr(verification, "remaining_position_quantity", None),
        "broker_position": broker_position,
        "issues": [issue.__dict__ for issue in verification.issues],
    }
    runtime.log.write("critical_protection_invariant_violation", payload)
    if hasattr(runtime.engine, "mark_position_unprotected"):
        runtime.engine.mark_position_unprotected("broker_native_protection_verification_failed", payload)
    archive_engine_state(root, runtime, reason="broker_native_protection_verification_failed")


def reconcile_engine_with_broker(root: Path, runtime: TraderRuntime) -> None:
    if not isinstance(runtime.execution, AlpacaExecutionEngine):
        return

    broker_position = runtime.execution.trader._get_position(runtime.symbol)
    engine_trade = runtime.engine.trade_state_snapshot()
    engine_position = engine_trade.get("position")

    broker_native_runtime = (
        is_renko_tp_sl_runtime(runtime)
        and str(getattr(runtime.engine.config, "exit_management", "")).lower().strip() == "bracket"
    )
    reversal_stop_runtime = is_reversal_catastrophic_stop_runtime(runtime)
    if broker_position is None and reversal_stop_runtime and isinstance(engine_position, dict):
        external_fill = matching_reversal_catastrophic_stop_fill(runtime, engine_position)
        if external_fill is not None and hasattr(runtime.engine, "mark_external_close_filled"):
            stop_plan = external_fill.get("stop_plan") if isinstance(external_fill.get("stop_plan"), dict) else {}
            entry_price = Decimal(str(stop_plan.get("entry_reference_price") or engine_position.get("entry_price") or 0))
            fill_price = Decimal(str(external_fill.get("price") or external_fill.get("filled_avg_price") or 0))
            qty = Decimal(str(external_fill.get("qty") or external_fill.get("filled_qty") or 0))
            side_mult = Decimal("1") if engine_position.get("side") == "long" else Decimal("-1")
            realized_pnl = (fill_price - entry_price) * qty * side_mult
            runtime.engine.mark_external_close_filled(
                timestamp=external_fill.get("transaction_time") or external_fill.get("filled_at"),
                reason="reversal_catastrophic_stop_broker_native",
                order_id=str(external_fill.get("order_id") or external_fill.get("id") or ""),
                source="alpaca_reconciliation_reversal_catastrophic_stop_fill",
                fill_price=str(fill_price),
                filled_qty=str(qty),
                realized_pnl=str(realized_pnl),
            )
            runtime.log.write(
                "state_reconciled",
                {
                    "trader_id": runtime.trader_id,
                    "symbol": runtime.symbol,
                    "reason": "broker_flat_reversal_catastrophic_stop_fill_reconciled",
                    "fill": external_fill,
                    "previous_trade_state": engine_trade,
                },
            )
            archive_engine_state(root, runtime, reason="broker_flat_reversal_catastrophic_stop_fill_reconciled")
            return

    if broker_position is None and (broker_native_runtime or reversal_stop_runtime):
        if reversal_stop_runtime and engine_position is None and last_closed_reversal_stop_is_terminal(runtime):
            return
        nested_orders = broker_nested_open_orders(runtime)
        if broker_native_runtime and isinstance(engine_position, dict):
            broker_native_fill = matching_broker_native_tp_sl_close_fill(runtime, engine_position)
            if broker_native_fill is not None and broker_flat_orders_are_known_tp_sl_cleanup(runtime, nested_orders):
                reconcile_broker_native_tp_sl_close_fill(
                    root,
                    runtime,
                    engine_trade,
                    engine_position,
                    broker_native_fill,
                )
                return
        if reversal_stop_runtime:
            orphan_verification = verify_reversal_catastrophic_stop(
                symbol=runtime.symbol,
                broker_position=None,
                open_orders=nested_orders,
                stop_plan=current_reversal_stop_plan(runtime),
                expected_config_fingerprint=getattr(runtime.engine.config, "candidate_config_fingerprint", None),
                expected_market_contract_fingerprint=runtime_expected_market_contract_fingerprint(runtime),
            )
        else:
            orphan_verification = verify_broker_native_protection(
                symbol=runtime.symbol,
                broker_position=None,
                nested_orders=nested_orders,
                exit_plan=current_exit_plan(runtime),
                protection_state=getattr(runtime.engine, "protection_state", None)
                if isinstance(getattr(runtime.engine, "protection_state", None), dict)
                else None,
                expected_config_fingerprint=getattr(runtime.engine.config, "candidate_config_fingerprint", None),
                expected_market_contract_fingerprint=runtime_expected_market_contract_fingerprint(runtime),
            )
        if orphan_verification.issues:
            log_protection_invariant_result(root, runtime, orphan_verification, None)
            raise BrokerEngineStateMismatch(
                f"{runtime.trader_id} {runtime.symbol} has orphan broker-native protection while broker is flat"
            )
        if broker_native_runtime and engine_position is None:
            open_orders = symbol_open_orders(runtime)
            unexpected_orders = unexpected_symbol_open_orders(runtime)
            if unexpected_orders:
                error = {
                    "reason": "broker_flat_engine_flat_unexpected_symbol_open_order",
                    "broker_position": None,
                    "engine_position": None,
                    "unexpected_open_orders": unexpected_orders,
                }
                runtime.log.write(
                    "state_reconcile_error",
                    {
                        "trader_id": runtime.trader_id,
                        "symbol": runtime.symbol,
                        **error,
                    },
                )
                halt_runtime(root, runtime, str(error["reason"]), error)
                raise BrokerEngineStateMismatch(
                    f"{runtime.trader_id} {runtime.symbol} has unexpected open broker orders while flat"
                )
            mark_reconciliation_safe(
                root,
                runtime,
                reason="broker_local_flat_no_owned_orders",
                evidence=reconciliation_evidence(
                    runtime,
                    broker_position=None,
                    engine_position=engine_position,
                    open_orders=open_orders,
                    verification=orphan_verification,
                ),
            )
            return

    if broker_position is not None and (broker_native_runtime or reversal_stop_runtime):
        verification = verify_runtime_reversal_stop(runtime, broker_position) if reversal_stop_runtime else verify_runtime_broker_protection(runtime, broker_position)
        if verification.protected and engine_position is None:
            if reversal_stop_runtime:
                stop_plan = current_reversal_stop_plan(runtime) or {}
                runtime.engine.apply_reversal_stop_evidence(
                    stop_plan=stop_plan,
                    verification=verification,
                    broker_position=broker_position,
                )
            else:
                exit_plan = current_exit_plan(runtime) or {}
                runtime.engine.apply_tp_sl_protection_evidence(
                    exit_plan=exit_plan,
                    verification=verification,
                    broker_position=broker_position,
                )
            runtime.log.write(
                "state_reconciled",
                {
                    "trader_id": runtime.trader_id,
                    "symbol": runtime.symbol,
                    "reason": "broker_position_valid_protection_adopted",
                    "protection": getattr(runtime.engine, "protection_state", None),
                },
            )
            archive_engine_state(root, runtime, reason="broker_position_valid_protection_adopted")
            return
        if verification.protected:
            if reversal_stop_runtime:
                runtime.engine.apply_reversal_stop_evidence(
                    stop_plan=current_reversal_stop_plan(runtime) or {},
                    verification=verification,
                    broker_position=broker_position,
                )
            else:
                runtime.engine.apply_tp_sl_protection_evidence(
                    exit_plan=current_exit_plan(runtime) or {},
                    verification=verification,
                    broker_position=broker_position,
                )
            log_protection_invariant_result(root, runtime, verification, broker_position)
        else:
            recovery_state = (
                _recover_runtime_reversal_catastrophic_stop(root, runtime, broker_position)
                if reversal_stop_runtime
                else _recover_runtime_broker_protection(root, runtime, broker_position)
            )
            if recovery_state in {TP_SL_LIFECYCLE_ENTRY_PLANNED, "OPEN"}:
                return
            log_protection_invariant_result(root, runtime, verification, broker_position)
            raise BrokerEngineStateMismatch(
                f"{runtime.trader_id} {runtime.symbol} broker position lacks valid broker-native protection"
            )

    if broker_position is None and engine_position is not None:
        if is_renko_tp_sl_runtime(runtime) or reversal_stop_runtime:
            lifecycle_state = str(getattr(runtime.engine, "tp_sl_lifecycle_state", ""))
            if lifecycle_state in TP_SL_EXIT_LOCK_STATES or getattr(runtime.engine, "pending_exit", None):
                runtime.engine.mark_close_filled(
                    timestamp=pd.Timestamp.now(tz="UTC"),
                    reason=(getattr(runtime.engine, "pending_exit", {}) or {}).get("reason"),
                    order_id=(getattr(runtime.engine, "pending_exit", {}) or {}).get("broker_order_id"),
                    source="alpaca_reconciliation_broker_flat",
                )
                runtime.log.write(
                    "state_reconciled",
                    {
                        "trader_id": runtime.trader_id,
                        "symbol": runtime.symbol,
                        "reason": "broker_flat_tp_sl_close_confirmed",
                        "previous_trade_state": engine_trade,
                    },
                )
                archive_engine_state(root, runtime, reason="broker_flat_tp_sl_close_confirmed")
                return
            if isinstance(engine_position, dict):
                external_fill = matching_external_close_fill(runtime, engine_position)
                if external_fill is not None and hasattr(runtime.engine, "mark_external_close_filled"):
                    entry_price = Decimal(str(engine_position.get("entry_price") or 0))
                    fill_price = Decimal(str(external_fill.get("price") or external_fill.get("filled_avg_price") or 0))
                    qty = Decimal(str(external_fill.get("qty") or 0))
                    side_mult = Decimal("1") if engine_position.get("side") == "long" else Decimal("-1")
                    realized_pnl = (fill_price - entry_price) * qty * side_mult
                    runtime.engine.mark_external_close_filled(
                        timestamp=external_fill.get("transaction_time") or external_fill.get("filled_at"),
                        reason="manual_external_close",
                        order_id=str(external_fill.get("order_id") or external_fill.get("id") or ""),
                        source="alpaca_manual_close_reconciliation",
                        fill_price=str(fill_price),
                        filled_qty=str(qty),
                        realized_pnl=str(realized_pnl),
                    )
                    runtime.log.write(
                        "state_reconciled",
                        {
                            "trader_id": runtime.trader_id,
                            "symbol": runtime.symbol,
                            "reason": "broker_flat_manual_external_close_reconciled",
                            "fill": external_fill,
                            "previous_trade_state": engine_trade,
                        },
                    )
                    archive_engine_state(root, runtime, reason="broker_flat_manual_external_close_reconciled")
                    return
            runtime.log.write(
                "state_reconcile_error",
                {
                    "trader_id": runtime.trader_id,
                    "symbol": runtime.symbol,
                    "reason": "broker_flat_engine_position_without_pending_close",
                    "previous_trade_state": engine_trade,
                },
            )
            halt_runtime(
                root,
                runtime,
                "broker_flat_engine_position_without_pending_close",
                {"previous_trade_state": engine_trade},
            )
            raise BrokerEngineStateMismatch(
                f"{runtime.trader_id} engine has {runtime.symbol} position but broker is flat"
            )
        runtime.engine.restore_trade_state(
            {
                "position": None,
                "psar_sar": None,
                "psar_ep": None,
                "psar_af": None,
                "force_flat_dates": engine_trade.get("force_flat_dates", []),
            }
        )
        runtime.log.write(
            "state_reconciled",
            {
                "trader_id": runtime.trader_id,
                "symbol": runtime.symbol,
                "reason": "broker_flat_engine_position_cleared",
                "previous_trade_state": engine_trade,
            },
        )
        archive_engine_state(root, runtime, reason="broker_reconciled_flat")
        return

    if broker_position is not None and engine_position is None:
        runtime.log.write(
            "state_reconcile_error",
            {
                "trader_id": runtime.trader_id,
                "symbol": runtime.symbol,
                "reason": "broker_position_exists_engine_flat",
                "broker_position": broker_position,
            },
        )
        if is_renko_tp_sl_runtime(runtime) or reversal_stop_runtime:
            halt_runtime(
                root,
                runtime,
                "broker_position_exists_engine_flat",
                {"broker_position": broker_position},
            )
        else:
            archive_engine_state(root, runtime, reason="broker_position_exists_engine_flat")
        raise BrokerEngineStateMismatch(
            f"{runtime.trader_id} has broker {runtime.symbol} position but engine is flat"
        )

    if broker_position is not None and is_renko_tp_sl_runtime(runtime):
        error = tp_sl_reconciliation_error(runtime, broker_position, engine_position)
        if error is not None:
            runtime.log.write(
                "state_reconcile_error",
                {
                    "trader_id": runtime.trader_id,
                    "symbol": runtime.symbol,
                    **error,
                },
            )
            halt_runtime(root, runtime, str(error["reason"]), error)
            raise BrokerEngineStateMismatch(
                f"{runtime.trader_id} {runtime.symbol} reconciliation unsafe: {error['reason']}"
            )
        if broker_native_runtime:
            open_orders = symbol_open_orders(runtime)
            unexpected_orders = unexpected_symbol_open_orders(
                runtime,
                allowed_order_ids=protection_allowed_order_ids(verification),
            )
            if unexpected_orders:
                error = {
                    "reason": "broker_open_engine_open_unexpected_symbol_open_order",
                    "broker_position": broker_position,
                    "engine_position": engine_position,
                    "unexpected_open_orders": unexpected_orders,
                }
                runtime.log.write(
                    "state_reconcile_error",
                    {
                        "trader_id": runtime.trader_id,
                        "symbol": runtime.symbol,
                        **error,
                    },
                )
                halt_runtime(root, runtime, str(error["reason"]), error)
                raise BrokerEngineStateMismatch(
                    f"{runtime.trader_id} {runtime.symbol} has unexpected open broker orders while open"
                )
            mark_reconciliation_safe(
                root,
                runtime,
                reason="broker_local_open_valid_protection",
                evidence=reconciliation_evidence(
                    runtime,
                    broker_position=broker_position,
                    engine_position=engine_position,
                    open_orders=open_orders,
                    verification=verification,
                ),
            )


def reconcile_broker_state_if_due(root: Path, runtime: TraderRuntime) -> None:
    if not isinstance(runtime.execution, AlpacaExecutionEngine):
        return

    now = time.monotonic()
    last = _LAST_BROKER_MANAGED_RECONCILE.get(runtime.trader_id, 0.0)
    if now - last < BROKER_MANAGED_RECONCILE_INTERVAL_SECONDS:
        return
    _LAST_BROKER_MANAGED_RECONCILE[runtime.trader_id] = now

    try:
        reconcile_engine_with_broker(root, runtime)
    except BrokerEngineStateMismatch:
        raise
    except Exception as exc:
        runtime.log.write(
            "state_reconcile_error",
            {
                "trader_id": runtime.trader_id,
                "symbol": runtime.symbol,
                "reason": "broker_state_reconcile_failed",
                "error": str(exc),
            },
        )


def runtime_force_flat_date(runtime: TraderRuntime, timestamp: pd.Timestamp) -> str | None:
    force_flat = getattr(runtime.engine, "force_flat", None)
    if runtime.engine.config.allow_overnight or force_flat is None:
        return None

    local_timestamp = runtime.engine._local_timestamp(timestamp)
    current_date = local_timestamp.date().isoformat()
    active_start_date = getattr(runtime.engine, "active_start_date", None)
    active_end_date = getattr(runtime.engine, "active_end_date", None)
    if active_start_date and current_date < active_start_date:
        return None
    if active_end_date and current_date > active_end_date:
        return None
    if local_timestamp.time() < force_flat:
        return None
    return current_date


def force_flat_key(runtime: TraderRuntime, current_date: str) -> tuple[str, str, str, str]:
    return (runtime.mode, runtime.credential_profile or "", runtime.symbol, current_date)


def force_flat_client_order_id(runtime: TraderRuntime, timestamp: pd.Timestamp) -> str:
    local_timestamp = runtime.engine._local_timestamp(timestamp)
    suffix = f"ff-{local_timestamp.strftime('%Y%m%d-%H%M%S')}"
    prefix = slug(runtime.trader_id).lower()
    max_prefix_length = max(1, 48 - len(suffix) - 1)
    return f"{prefix[:max_prefix_length]}-{suffix}"[:48]


def mark_runtime_force_flat_state(root: Path, runtime: TraderRuntime, current_date: str, reason: str) -> None:
    engine_trade = runtime.engine.trade_state_snapshot()
    force_flat_dates = set(engine_trade.get("force_flat_dates", []))
    already_flat_for_date = engine_trade.get("position") is None and current_date in force_flat_dates
    force_flat_dates.add(current_date)
    runtime.engine.restore_trade_state(
        {
            "position": None,
            "psar_sar": None,
            "psar_ep": None,
            "psar_af": None,
            "force_flat_dates": sorted(force_flat_dates),
        }
    )
    if not already_flat_for_date:
        archive_engine_state(root, runtime, reason=reason)


def broker_force_flat_if_due(
    root: Path,
    runtime: TraderRuntime,
    timestamp: pd.Timestamp,
    *,
    price: float | None = None,
    source: str,
) -> bool:
    current_date = runtime_force_flat_date(runtime, timestamp)
    if current_date is None:
        return False

    key = force_flat_key(runtime, current_date)
    if key in _BROKER_FORCE_FLAT_DATES:
        engine_trade = runtime.engine.trade_state_snapshot()
        if engine_trade.get("position") is None:
            mark_runtime_force_flat_state(root, runtime, current_date, reason=f"broker_force_flat_already_handled_{source}")
        else:
            runtime.log.write(
                "broker_force_flat_already_handled",
                {
                    "trader_id": runtime.trader_id,
                    "symbol": runtime.symbol,
                    "source": source,
                    "current_date": current_date,
                    "engine_trade": engine_trade,
                },
            )
        return False

    broker_position = None
    if isinstance(runtime.execution, AlpacaExecutionEngine):
        try:
            broker_position = runtime.execution.trader._get_position(runtime.symbol)
        except Exception as exc:
            runtime.log.write(
                "broker_force_flat_position_error",
                {
                    "trader_id": runtime.trader_id,
                    "symbol": runtime.symbol,
                    "source": source,
                    "error": str(exc),
                },
            )

    engine_position = runtime.engine.trade_state_snapshot().get("position")
    broker_position_side = str((broker_position or {}).get("side", "")).lower().strip()
    if broker_position_side == "short":
        strategy_side = "short"
        broker_side = "buy"
    elif broker_position_side == "long":
        strategy_side = "long"
        broker_side = "sell"
    elif isinstance(engine_position, dict) and engine_position.get("side") == "short":
        strategy_side = "short"
        broker_side = "buy"
    else:
        strategy_side = "long"
        broker_side = "sell"

    signal = Signal(
        signal_id=f"{runtime.trader_id}-{runtime.symbol.lower()}-force-flat-{current_date.replace('-', '')}",
        signal_type="exit",
        timestamp=timestamp,
        symbol=runtime.symbol,
        side=strategy_side,  # type: ignore[arg-type]
        broker_side=broker_side,  # type: ignore[arg-type]
        price=float(
            price
            if price is not None
            else as_float((broker_position or {}).get("current_price"), default=0.0)
            or as_float((broker_position or {}).get("avg_entry_price"), default=0.0)
        ),
        reason="session_force_flat",
        strategy=runtime.engine.config.strategy_name,
        trader_id=runtime.trader_id,
    )
    intent = OrderIntent(
        client_order_id=force_flat_client_order_id(runtime, timestamp),
        timestamp=timestamp,
        symbol=runtime.symbol,
        side=broker_side,  # type: ignore[arg-type]
        signal_type="exit",
        order_type="market",
        strategy=runtime.engine.config.strategy_name,
        reason="session_force_flat",
        close_position=True,
        trader_id=runtime.trader_id,
        account_namespace=runtime.account_namespace,
    )

    runtime.log.write(
        "broker_force_flat",
        {
            "trader_id": runtime.trader_id,
            "symbol": runtime.symbol,
            "source": source,
            "current_date": current_date,
            "broker_position": broker_position,
            "engine_trade": runtime.engine.trade_state_snapshot(),
            "intent": dataclass_to_jsonable(intent),
        },
    )
    runtime.log.write("signal", dataclass_to_jsonable(signal))
    _BROKER_FORCE_FLAT_DATES.add(key)
    engine_position_before = runtime.engine.trade_state_snapshot().get("position")
    try:
        event = runtime.execution.execute(intent)
    except Exception as exc:
        _BROKER_FORCE_FLAT_DATES.discard(key)
        runtime.log.write(
            "execution_error",
            {
                "trader_id": runtime.trader_id,
                "symbol": runtime.symbol,
                "error": str(exc),
                "signal": dataclass_to_jsonable(signal),
                "intent": dataclass_to_jsonable(intent),
            },
        )
        return False

    if is_renko_tp_sl_runtime(runtime) and engine_position_before is not None:
        status = str(getattr(event, "status", "") or "").lower()
        if status in {"no open position", "already flat"}:
            runtime.engine.mark_close_filled(
                timestamp=getattr(event, "timestamp", None) or timestamp,
                reason=intent.reason,
                order_id=None,
                source="broker_force_flat_already_flat",
            )
        else:
            runtime.engine.mark_close_submitted(intent, response=getattr(event, "response", None))
        archive_engine_state(root, runtime, reason=f"broker_force_flat_{source}")
    else:
        mark_runtime_force_flat_state(root, runtime, current_date, reason=f"broker_force_flat_{source}")
    return True


def behavior_profile_block_reason(trader: dict[str, Any]) -> str | None:
    profile = trader.get("profile") if isinstance(trader.get("profile"), dict) else {}
    risk = trader.get("risk") if isinstance(trader.get("risk"), dict) else {}
    max_trade_cost = as_float(risk.get("max_trade_cost_proxy_bps"), default=0.0)
    max_p95_vol = as_float(risk.get("max_1m_p95_abs_return_bps"), default=0.0)

    trade_cost = as_float(profile.get("trade_cost_proxy_bps"), default=-1.0)
    if max_trade_cost > 0 and trade_cost >= 0 and trade_cost > max_trade_cost:
        return f"profile_trade_cost_proxy_{trade_cost:.4f}_exceeds_{max_trade_cost:.4f}_bps"

    p95_vol = as_float(profile.get("p95_abs_1m_return_bps"), default=-1.0)
    if max_p95_vol > 0 and p95_vol >= 0 and p95_vol > max_p95_vol:
        return f"profile_p95_abs_1m_return_{p95_vol:.4f}_exceeds_{max_p95_vol:.4f}_bps"

    return None


def build_runtimes(config: dict[str, Any], args: argparse.Namespace) -> list[TraderRuntime]:
    runtimes: list[TraderRuntime] = []
    evidence_logger = build_evidence_logger(args)
    shadow_sampler = build_shadow_sampler(args, evidence_logger)
    for trader in config["traders"]:
        if not isinstance(trader, dict):
            continue
        trader_id = str(trader.get("id") or "")
        if not trader_id:
            raise ValueError("Every trader must have an id.")
        symbol = str(trader.get("symbol") or "").upper()
        if not symbol:
            raise ValueError(f"{trader_id}: missing symbol.")

        mode = args.force_mode or str(trader.get("mode") or "paper").lower()
        credential_profile = trader.get("credential_profile")
        account_namespace = normalize_optional_namespace(
            trader.get("account_namespace") or trader.get("account_alias") or credential_profile
        )
        expected_account_id = str(trader.get("expected_account_id")) if trader.get("expected_account_id") else None
        data = trader.get("data") if isinstance(trader.get("data"), dict) else {}
        feed = args.force_feed or str(data.get("feed") or "iex").lower()
        market_data_source = normalize_market_data_source(getattr(args, "market_data_source", None) or data.get("market_data_source"))
        market_data_profile = cli_market_data_profile(args) or normalize_optional_namespace(data.get("market_data_profile"))
        log_path = args.log_output or trader.get("event_log") or "artifacts/live/fleet_events.jsonl"
        risk = trader.get("risk") if isinstance(trader.get("risk"), dict) else {}
        max_position_notional = as_float(risk.get("max_position_notional"), default=250000.0)
        max_daily_loss = as_float(risk.get("max_daily_loss"), default=0.0)
        execution_enabled = bool(trader.get("execution_enabled", True))
        if (
            bool(trader.get("enabled", True))
            and execution_enabled
            and bool(getattr(args, "execute_real", False))
            and str(mode).lower() == "live"
            and market_data_source != "shared_canonical_stream"
        ):
            raise ValueError(f"{trader_id}: live real trading must use shared_canonical_stream market data.")
        if market_data_source == "shared_canonical_stream" and feed != "sip":
            raise ValueError(f"{trader_id}: shared_canonical_stream requires feed=sip.")
        behavior_block_reason = behavior_profile_block_reason(trader)
        log = JsonlEventLog(log_path)
        strategy_config = build_strategy_config(trader)
        if isinstance(strategy_config, RenkoStrategyConfig):
            renko_source = validate_renko_trader_source(trader)
            market_adapter = CanonicalMarketObservationAdapter(run_id=f"production:{trader_id}")
            contract_fingerprints = contract_snapshot(
                symbol=symbol,
                strategy_config=strategy_config,
                session_timezone=strategy_config.timezone,
            )
        else:
            configured_source = data.get("renko_source")
            if configured_source == CANONICAL_RENKO_SOURCE:
                renko_source = canonical_observation_source_for_non_renko()
                market_adapter = CanonicalMarketObservationAdapter(run_id=f"production:{trader_id}")
            else:
                renko_source = "ticks"
                market_adapter = None
            contract_fingerprints = None
        if isinstance(strategy_config, MeanReversionStrategyConfig):
            engine = MeanReversionStrategyEngine(strategy_config)
        else:
            engine = RenkoStrategyEngine(strategy_config)
        execution = (
            AlpacaExecutionEngine(
                mode=mode,
                credential_profile=credential_profile,
                account_namespace=account_namespace,
                expected_account_id=expected_account_id,
                max_notional=max_position_notional,
                log=log,
            )
            if args.execute_real and execution_enabled
            else ShadowExecutionEngine(log=log)
        )
        config_hash = None
        if isinstance(strategy_config, RenkoStrategyConfig):
            try:
                config_hash = load_production_renko_config(args.config, trader_id).config_hash
            except Exception as exc:
                log.write(
                    "config_hash_error",
                    {
                        "trader_id": trader_id,
                        "symbol": symbol,
                        "error": str(exc),
                    },
                )
        runtime = TraderRuntime(
            trader_id=trader_id,
            symbol=symbol,
            mode=mode,
            credential_profile=str(credential_profile) if credential_profile else None,
            account_namespace=account_namespace,
            expected_account_id=expected_account_id,
            market_data_source=market_data_source,
            market_data_profile=market_data_profile,
            feed=feed,
            renko_source=renko_source,
            market_adapter=market_adapter,
            contract_fingerprints=contract_fingerprints,
            enabled=bool(trader.get("enabled", True)) and behavior_block_reason is None,
            execution_enabled=execution_enabled,
            max_position_notional=max_position_notional,
            max_daily_loss=max_daily_loss,
            engine=engine,
            execution=execution,
            log=log,
            evidence=evidence_logger,
            shadow_sampler=shadow_sampler,
            config_hash=config_hash,
        )
        root = archive_root(args)
        restored = False
        restored_path = None
        state_path, state_payload, state_snapshot, state_error = load_state_snapshot_payload(root, trader_id)
        restore_canonical_market_checkpoints(runtime, state_payload)
        if isinstance(strategy_config, RenkoStrategyConfig) and state_payload is not None:
            validate_renko_state_contract(runtime, state_payload, state_path)
            sampler_state = state_payload.get("sampler_state")
            if runtime.market_adapter is not None and isinstance(sampler_state, dict):
                runtime.market_adapter.restore_snapshot(sampler_state)
        if state_snapshot is None and state_error:
            restored_path = state_error
            log.write(
                "state_restore_error",
                {
                    "trader_id": trader_id,
                    "symbol": symbol,
                    "error": state_error,
                },
            )
        elif state_snapshot is not None:
            try:
                runtime.engine.restore_snapshot(state_snapshot)
                restored = True
                restored_path = str(state_path)
            except Exception as exc:
                restored_path = str(state_path)
                error = str(exc)
                if (
                    isinstance(runtime.engine, RenkoStrategyEngine)
                    and state_payload is not None
                    and is_renko_brick_change(state_snapshot, runtime.engine)
                ):
                    try:
                        migration_path = migrate_renko_brick_change_state(
                            root,
                            runtime,
                            state_payload=state_payload,
                            state_snapshot=state_snapshot,
                            state_path=state_path,
                            restore_error=error,
                        )
                        restored_path = migration_path
                        restored = True
                    except Exception as migration_exc:
                        log.write(
                            "renko_state_migration_error",
                            {
                                "trader_id": trader_id,
                                "symbol": symbol,
                                "policy": RENKO_BRICK_CHANGE_MIGRATION_POLICY,
                                "state_path": str(state_path),
                                "restore_error": error,
                                "migration_error": str(migration_exc),
                            },
                        )
                        raise
                else:
                    log.write(
                        "state_restore_error",
                        {
                            "trader_id": trader_id,
                            "symbol": symbol,
                            "error": error,
                        },
                    )
        runtime.log.write(
            "trader_configured",
            {
                "trader_id": runtime.trader_id,
                "symbol": runtime.symbol,
                "mode": runtime.mode,
                "credential_profile": runtime.credential_profile,
                "account_namespace": runtime.account_namespace,
                "expected_account_id_configured": bool(runtime.expected_account_id),
                "market_data_source": runtime.market_data_source,
                "market_data_profile": runtime.market_data_profile,
                "feed": runtime.feed,
                "renko_source": runtime.renko_source,
                "enabled": runtime.enabled,
                "execution_enabled": runtime.execution_enabled,
                "execution_mode": "real" if args.execute_real and runtime.execution_enabled else "shadow",
                "max_position_notional": runtime.max_position_notional,
                "max_daily_loss": runtime.max_daily_loss,
                "behavior_profile_block_reason": behavior_block_reason,
                "state_restored": restored,
                "state_path": restored_path,
                "config_hash": runtime.config_hash,
                "contract_fingerprints": runtime.contract_fingerprints,
                "migration_evidence_path": runtime.migration_evidence_path,
            },
        )
        if runtime.evidence is not None and runtime.evidence.enabled:
            safe_evidence_call(
                runtime,
                "runtime_summary",
                lambda: runtime.evidence.record_runtime_summary(
                    trader_id=runtime.trader_id,
                    symbol=runtime.symbol,
                    summary={
                        "evidence_enabled": True,
                        "shadow_sampler_enabled": runtime.shadow_sampler is not None and runtime.shadow_sampler.enabled,
                        "production_source_mode": runtime.renko_source,
                        "contract_fingerprints": runtime.contract_fingerprints,
                        "market_data_source": runtime.market_data_source,
                        "market_data_profile": runtime.market_data_profile,
                        "account_namespace": runtime.account_namespace,
                    },
                ),
            )
        if behavior_block_reason:
            runtime.log.write(
                "behavior_profile_block",
                {
                    "trader_id": runtime.trader_id,
                    "symbol": runtime.symbol,
                    "reason": behavior_block_reason,
                },
            )
        warm_start = trader.get("warm_start") if isinstance(trader.get("warm_start"), dict) else {}
        if not restored and bool(warm_start.get("enabled", False)):
            warm_start_runtime_from_archived_ticks(
                archive_root(args),
                runtime,
                days=int(warm_start.get("days", 10)),
            )
        reconcile_engine_with_broker(archive_root(args), runtime)
        runtimes.append(runtime)
    return runtimes


def next_production_observation_sequence(runtime: TraderRuntime) -> int:
    key = (runtime.trader_id, runtime.symbol)
    sequence = _PRODUCTION_OBSERVATION_SEQUENCES.get(key, 0) + 1
    _PRODUCTION_OBSERVATION_SEQUENCES[key] = sequence
    return sequence


def record_shadow_for_eligible_tick(runtime: TraderRuntime, tick: MarketTick) -> None:
    if runtime.evidence is None or not runtime.evidence.enabled or runtime.shadow_sampler is None:
        return
    try:
        result = runtime.shadow_sampler.process_tick(trader_id=runtime.trader_id, tick=tick)
        for observation in result.observations:
            runtime.evidence.record_shadow_observation(
                trader_id=runtime.trader_id,
                symbol=runtime.symbol,
                sequence=int(observation["sequence"]),
                observation_bucket_timestamp=observation["observation_bucket_timestamp"],
                close_price=observation["close_price"],
                source_record_id=observation.get("source_record_id"),
                broker_trade_id=observation.get("broker_trade_id"),
                source_trade_timestamp=observation.get("source_trade_timestamp"),
                triggering_source_record_id=observation.get("triggering_source_record_id"),
                triggering_trade_timestamp=observation.get("triggering_trade_timestamp"),
            )
        for diagnostic in result.diagnostics:
            runtime.evidence.record_shadow_sampler_diagnostic(
                trader_id=runtime.trader_id,
                symbol=runtime.symbol,
                diagnostic_type=str(diagnostic.get("diagnostic_type")),
                message=str(diagnostic.get("message")),
                source_record_id=diagnostic.get("source_record_id"),
            )
    except Exception as exc:
        if runtime.shadow_sampler is not None:
            runtime.shadow_sampler.counters.failures += 1
        object.__setattr__(runtime, "shadow_sampler", None)
        runtime.evidence.safe_visible_diagnostic(f"shadow sampler disabled after failure for {runtime.trader_id}/{runtime.symbol}: {exc}")


def source_observations_for_runtime(
    runtime: TraderRuntime,
    tick: MarketTick,
    source_event: LiveMarketEvent | None,
) -> tuple[tuple[MarketTick, Any | None], ...]:
    if runtime.market_data_source == "shared_canonical_stream":
        return ((tick, {"source": "shared_canonical_stream"}),)
    if runtime.market_adapter is None:
        if isinstance(runtime.engine, RenkoStrategyEngine):
            raise RuntimeError(f"{runtime.trader_id}: Renko runtime has no canonical market observation adapter.")
        return ((tick, None),)
    result = runtime.market_adapter.process_tick(
        tick,
        trader_id=runtime.trader_id,
        source_event=source_event,
        account_id=runtime.account_namespace,
    )
    observation_ticks = runtime.market_adapter.observation_ticks(result)
    return tuple((observation_ticks[index], observation) for index, observation in enumerate(result.observations))


def flush_evidence(runtimes: list[TraderRuntime]) -> None:
    seen: set[int] = set()
    for runtime in runtimes:
        if runtime.evidence is None:
            continue
        marker = id(runtime.evidence)
        if marker in seen:
            continue
        seen.add(marker)
        try:
            runtime.evidence.flush()
        except Exception as exc:
            runtime.evidence.record_failure(f"evidence flush failed; trading continues: {exc}")


def group_runtimes(runtimes: list[TraderRuntime]) -> dict[tuple[str, str, str, str], list[TraderRuntime]]:
    grouped: dict[tuple[str, str, str, str], list[TraderRuntime]] = {}
    for runtime in runtimes:
        if not runtime.enabled:
            continue
        key = (
            runtime.market_data_source,
            runtime.market_data_profile or "",
            runtime.mode,
            runtime.feed,
        )
        grouped.setdefault(key, []).append(runtime)
    return grouped


async def close_stream_after_pending_read(stream: Any, pending_read: asyncio.Task[Any]) -> None:
    if not pending_read.done():
        pending_read.cancel()
    await asyncio.gather(pending_read, return_exceptions=True)
    await stream.aclose()


def decode_ws_payload(raw_message: str | bytes) -> Any:
    if isinstance(raw_message, bytes):
        raw_message = raw_message.decode("utf-8")
    return json.loads(raw_message)


def is_current_tp_sl_entry_plan(runtime: TraderRuntime, signal: Signal, intent: OrderIntent) -> bool:
    if not is_renko_tp_sl_runtime(runtime):
        return False
    if str(getattr(runtime.engine.config, "exit_management", "")).lower().strip() != "bracket":
        return False
    if str(getattr(runtime.engine, "tp_sl_lifecycle_state", "")) != TP_SL_LIFECYCLE_ENTRY_PLANNED:
        return False
    if signal.signal_type != "entry" or intent.close_position:
        return False
    if getattr(runtime.engine, "pending_exit", None) is not None:
        return False
    if getattr(runtime.engine, "protection_state", None) is not None:
        return False
    position = runtime.engine.trade_state_snapshot().get("position")
    if not isinstance(position, dict):
        return False
    if str(position.get("symbol", "")).upper() != signal.symbol.upper():
        return False
    if str(position.get("side", "")).lower() != str(signal.side).lower():
        return False
    if pd.Timestamp(position.get("entry_time")) != pd.Timestamp(signal.timestamp):
        return False
    target_qty = intent.qty or runtime.engine.config.qty
    position_qty = position.get("qty")
    if target_qty is not None and position_qty is not None and abs(float(position_qty) - float(target_qty)) > 1e-9:
        return False
    return True


def should_block_for_risk(runtime: TraderRuntime, signal: Signal, intent: OrderIntent) -> str | None:
    if not runtime.enabled:
        return "runtime_disabled"
    if runtime.market_data_signal_block_reason:
        return runtime.market_data_signal_block_reason
    if bool(getattr(runtime.engine, "reconciliation_unsafe", False)):
        return "reconciliation_unsafe"
    if (
        intent.close_position
        and is_renko_tp_sl_runtime(runtime)
        and isinstance(getattr(runtime.engine, "pending_exit", None), dict)
        and bool(getattr(runtime.engine, "pending_exit", {}).get("broker_order_id"))
    ):
        return "tp_sl_close_already_submitted"
    if signal.signal_type != "entry":
        return None
    if (
        is_renko_tp_sl_runtime(runtime)
        and str(getattr(runtime.engine, "tp_sl_lifecycle_state", "")) in TP_SL_EXIT_LOCK_STATES
        and not is_current_tp_sl_entry_plan(runtime, signal, intent)
    ):
        return "tp_sl_exit_pending"
    estimated_order_notional = intent.notional or ((intent.qty or 0.0) * signal.price)
    reason_text = str(signal.reason or "").lower()
    is_reversal = "reversal" in reason_text or "reverse" in reason_text
    max_order_notional = runtime.max_position_notional * (2 if is_reversal else 1)
    notional_tolerance = max(1.0, max_order_notional * 0.01)
    if (
        estimated_order_notional
        and runtime.max_position_notional
        and estimated_order_notional > max_order_notional + notional_tolerance
    ):
        return f"entry_notional_exceeds_limit_{runtime.max_position_notional:g}"
    if isinstance(runtime.execution, AlpacaExecutionEngine):
        account = runtime.execution.trader.get_account()
        available_buying_power = max(
            as_float(account.get("buying_power"), default=0.0),
            as_float(account.get("daytrading_buying_power"), default=0.0),
        )
        if not is_reversal and estimated_order_notional and available_buying_power < estimated_order_notional:
            return f"buying_power_{available_buying_power:.2f}_below_notional_{estimated_order_notional:.2f}"
        if intent.side == "sell" and not bool(account.get("shorting_enabled")):
            return "account_shorting_disabled"
        if intent.side == "sell":
            asset = runtime.execution.trader.get_asset(runtime.symbol)
            if not bool(asset.get("shortable")):
                runtime.log.write(
                    "short_entry_rejected",
                    {
                        "trader_id": runtime.trader_id,
                        "symbol": runtime.symbol,
                        "reason": "asset_not_shortable",
                        "asset": {
                            "symbol": asset.get("symbol"),
                            "status": asset.get("status"),
                            "tradable": asset.get("tradable"),
                            "marginable": asset.get("marginable"),
                            "shortable": asset.get("shortable"),
                            "easy_to_borrow": asset.get("easy_to_borrow"),
                        },
                        "signal": dataclass_to_jsonable(signal),
                        "intent": dataclass_to_jsonable(intent),
                    },
                )
                return "asset_not_shortable"
        positions = runtime.execution.trader.get_positions()
        for position in positions:
            if str(position.get("symbol", "")).upper() != runtime.symbol:
                continue
            market_value = abs(as_float(position.get("market_value"), default=0.0))
            unrealized_pl = as_float(position.get("unrealized_pl"), default=0.0)
            if market_value > 0 and not is_reversal:
                return "broker_position_already_open"
            if runtime.max_daily_loss and unrealized_pl <= -abs(runtime.max_daily_loss):
                return f"symbol_unrealized_loss_exceeds_limit_{runtime.max_daily_loss:g}"
    return None


def handle_action(runtime: TraderRuntime, signal: Signal, intent: OrderIntent, root: Path | None = None) -> bool:
    block_reason = should_block_for_risk(runtime, signal, intent)
    if block_reason:
        runtime.log.write(
            "risk_block",
            {
                "trader_id": runtime.trader_id,
                "symbol": runtime.symbol,
                "reason": block_reason,
                "signal": dataclass_to_jsonable(signal),
                "intent": dataclass_to_jsonable(intent),
            },
        )
        return False

    runtime.log.write("signal", dataclass_to_jsonable(signal))
    try:
        event = runtime.execution.execute(intent)
    except Exception as exc:
        if root is not None and is_reversal_catastrophic_stop_runtime(runtime):
            try:
                _recover_runtime_reversal_catastrophic_stop(root, runtime)
            except Exception as recovery_exc:
                runtime.log.write(
                    "critical_reversal_catastrophic_stop_recovery_error",
                    {
                        "trader_id": runtime.trader_id,
                        "symbol": runtime.symbol,
                        "error": str(recovery_exc),
                    },
                )
        runtime.log.write(
            "execution_error",
            {
                "trader_id": runtime.trader_id,
                "symbol": runtime.symbol,
                "error": str(exc),
                "signal": dataclass_to_jsonable(signal),
                "intent": dataclass_to_jsonable(intent),
            },
        )
        return False
    status = str(getattr(event, "status", "") or "").lower()
    if status in TERMINAL_EXECUTION_FAILURE_STATUSES:
        if (
            signal.signal_type == "entry"
            and is_renko_tp_sl_runtime(runtime)
            and str(intent.exit_management or "").lower().strip() == "bracket"
            and hasattr(runtime.engine, "mark_position_unprotected")
        ):
            runtime.engine.mark_position_unprotected(f"execution_status_{status}", getattr(event, "response", None))
        if (
            signal.signal_type == "entry"
            and is_reversal_catastrophic_stop_runtime(runtime)
            and str(intent.exit_management or "").lower().strip() == "catastrophic_stop"
            and hasattr(runtime.engine, "mark_position_unprotected")
        ):
            runtime.engine.mark_position_unprotected(f"execution_status_{status}", getattr(event, "response", None))
        runtime.log.write(
            "execution_error",
            {
                "trader_id": runtime.trader_id,
                "symbol": runtime.symbol,
                "error": f"execution_status_{status}",
                "signal": dataclass_to_jsonable(signal),
                "intent": dataclass_to_jsonable(intent),
                "execution_event": dataclass_to_jsonable(event),
            },
        )
        return False
    if (
        signal.signal_type == "entry"
        and is_renko_tp_sl_runtime(runtime)
        and str(intent.exit_management or "").lower().strip() == "bracket"
        and hasattr(runtime.engine, "mark_entry_protection_submitted")
    ):
        runtime.engine.mark_entry_protection_submitted(intent, response=getattr(event, "response", None))
        runtime.log.write(
            "broker_native_protection_submitted",
            {
                "trader_id": runtime.trader_id,
                "symbol": runtime.symbol,
                "lifecycle_state": getattr(runtime.engine, "tp_sl_lifecycle_state", None),
                "protection_state": getattr(runtime.engine, "protection_state", None),
            },
        )
        if root is not None:
            _recover_runtime_broker_protection(root, runtime)
    if (
        signal.signal_type == "entry"
        and is_reversal_catastrophic_stop_runtime(runtime)
        and str(intent.exit_management or "").lower().strip() == "catastrophic_stop"
        and hasattr(runtime.engine, "mark_reversal_catastrophic_stop_submitted")
    ):
        runtime.engine.mark_reversal_catastrophic_stop_submitted(intent, response=getattr(event, "response", None))
        runtime.log.write(
            "reversal_catastrophic_stop_submitted",
            {
                "trader_id": runtime.trader_id,
                "symbol": runtime.symbol,
                "lifecycle_state": getattr(runtime.engine, "tp_sl_lifecycle_state", None),
                "protection_state": getattr(runtime.engine, "protection_state", None),
            },
        )
        if root is not None:
            _recover_runtime_reversal_catastrophic_stop(root, runtime)
    if intent.close_position and hasattr(runtime.engine, "mark_close_submitted"):
        if status in {"no open position", "already flat"}:
            runtime.engine.mark_close_filled(
                timestamp=getattr(event, "timestamp", None),
                reason=intent.reason,
                order_id=None,
                source="alpaca_already_flat",
            )
        else:
            runtime.engine.mark_close_submitted(intent, response=getattr(event, "response", None))
    return True


def should_isolate_runtime_failure(runtime: TraderRuntime) -> bool:
    return True


def should_isolate_broker_engine_mismatch(runtime: TraderRuntime) -> bool:
    return True


def reconcile_broker_state_if_due_or_halt(root: Path, runtime: TraderRuntime) -> None:
    try:
        reconcile_broker_state_if_due(root, runtime)
    except BrokerEngineStateMismatch as exc:
        if should_isolate_broker_engine_mismatch(runtime):
            halt_runtime(root, runtime, "broker_engine_state_mismatch", {"error": str(exc)})
            return
        raise


def process_market_tick_for_runtime(
    *,
    root: Path,
    args: argparse.Namespace,
    runtime: TraderRuntime,
    tick: MarketTick,
    source_event: LiveMarketEvent | None = None,
) -> None:
    try:
        _process_market_tick_for_runtime(root=root, args=args, runtime=runtime, tick=tick, source_event=source_event)
    except BrokerEngineStateMismatch as exc:
        if should_isolate_broker_engine_mismatch(runtime):
            halt_runtime(root, runtime, "broker_engine_state_mismatch", {"error": str(exc)})
            return
        raise
    except Exception as exc:
        runtime.log.write(
            "runtime_processing_error",
            {
                "trader_id": runtime.trader_id,
                "symbol": runtime.symbol,
                "account_namespace": runtime.account_namespace,
                "market_data_source": runtime.market_data_source,
                "error": str(exc),
                "tick": dataclass_to_jsonable(tick),
            },
        )
        if should_isolate_runtime_failure(runtime):
            halt_runtime(
                root,
                runtime,
                "runtime_processing_error",
                {"error": str(exc), "tick": dataclass_to_jsonable(tick)},
            )
            return
        raise


def _process_market_tick_for_runtime(
    *,
    root: Path,
    args: argparse.Namespace,
    runtime: TraderRuntime,
    tick: MarketTick,
    source_event: LiveMarketEvent | None = None,
) -> None:
    if not runtime.enabled:
        return
    record_shadow_for_eligible_tick(runtime, tick)
    if args.log_ticks:
        payload = dataclass_to_jsonable(tick)
        payload["trader_id"] = runtime.trader_id
        if source_event is not None:
            payload["source_record_id"] = source_event.source_record_id
            payload["source_sequence"] = source_event.source_sequence
            payload["eligible"] = source_event.eligible
            payload["archive_path"] = source_event.archive_path
        runtime.log.write("market_tick", payload)

    shared_event_id = shared_canonical_event_id_from_tick(runtime, tick)
    if shared_event_id and runtime.processed_canonical_event_ids.get(runtime.symbol) == shared_event_id:
        broker_force_flat_if_due(
            root,
            runtime,
            tick.timestamp,
            price=tick.price,
            source="duplicate_shared_canonical_event",
        )
        reconcile_broker_state_if_due_or_halt(root, runtime)
        runtime.log.write(
            "duplicate_shared_canonical_event_skipped",
            {
                "trader_id": runtime.trader_id,
                "symbol": runtime.symbol,
                "event_id": shared_event_id,
                "timestamp": tick.timestamp.isoformat(),
            },
        )
        archive_engine_state(root, runtime, reason="duplicate_shared_canonical_event_skipped")
        return

    broker_force_flat_if_due(
        root,
        runtime,
        tick.timestamp,
        price=tick.price,
        source="tick",
    )
    reconcile_broker_state_if_due_or_halt(root, runtime)
    source_observations = source_observations_for_runtime(runtime, tick, source_event)
    if not source_observations:
        archive_engine_state(root, runtime, reason="tick")
        return

    for source_tick, observation in source_observations:
        if runtime.evidence is not None and runtime.evidence.enabled:
            production_sequence = next_production_observation_sequence(runtime)
            safe_evidence_call(
                runtime,
                "production_observation",
                lambda: runtime.evidence.record_production_observation(
                    trader_id=runtime.trader_id,
                    symbol=runtime.symbol,
                    sequence=production_sequence,
                    observation_bucket_timestamp=source_tick.timestamp,
                    close_price=source_tick.price,
                    production_source_mode=runtime.renko_source,
                ),
            )
            source_trade_identity = getattr(observation, "source_record_id", None) or (
                source_event.source_record_id if source_event is not None else source_tick.trade_id or None
            )
            triggering_trade_identity = source_event.source_record_id if source_event is not None else tick.trade_id or None
            safe_evidence_call(
                runtime,
                "observation",
                lambda: runtime.evidence.record_observation(
                    trader_id=runtime.trader_id,
                    symbol=runtime.symbol,
                    observation_bucket_timestamp=source_tick.timestamp,
                    close_price=source_tick.price,
                    source_trade_identity=source_trade_identity,
                    source_trade_timestamp=getattr(observation, "source_sip_timestamp", UNKNOWN),
                    triggering_trade_identity=triggering_trade_identity,
                    triggering_trade_timestamp=getattr(observation, "triggering_sip_timestamp", tick.timestamp),
                ),
            )
        trade_snapshot = runtime.engine.trade_state_snapshot()
        update_migration_evidence_once(
            runtime,
            "first_post_bootstrap_market_observation_timestamp",
            source_tick.timestamp,
        )
        renko_bricks, actions = runtime.engine.on_tick(source_tick)
        for brick in renko_bricks:
            update_migration_evidence_once(
                runtime,
                "first_post_bootstrap_brick_timestamp",
                brick.timestamp,
            )
            if runtime.evidence is not None and runtime.evidence.enabled:
                safe_evidence_call(
                    runtime,
                    "renko_brick",
                    lambda: runtime.evidence.record_renko_brick(
                        trader_id=runtime.trader_id,
                        symbol=runtime.symbol,
                        brick=brick,
                        source_observation_bucket_timestamp=source_tick.timestamp,
                    ),
                )
            brick_payload = dataclass_to_jsonable(brick)
            if source_event is not None:
                brick_payload["source_record_id"] = source_event.source_record_id
                brick_payload["source_sequence"] = source_event.source_sequence
            runtime.log.write("renko_brick", brick_payload)
            archive_renko_brick(root, runtime, brick_payload)
        for signal, intent in actions:
            update_migration_evidence_once(
                runtime,
                "first_eligible_signal_timestamp",
                signal.timestamp,
            )
            if runtime.evidence is not None and runtime.evidence.enabled:
                source_brick_timestamp = renko_bricks[-1].timestamp if renko_bricks else UNKNOWN
                signal_sequence = safe_evidence_call(
                    runtime,
                    "strategy_signal",
                    lambda: runtime.evidence.record_signal(
                        trader_id=runtime.trader_id,
                        symbol=runtime.symbol,
                        signal=signal,
                        intent=intent,
                        source_brick_timestamp=source_brick_timestamp,
                    ),
                )
                safe_evidence_call(
                    runtime,
                    "position_direction",
                    lambda: runtime.evidence.record_position_direction(
                        trader_id=runtime.trader_id,
                        symbol=runtime.symbol,
                        logical_transition_timestamp=signal.timestamp,
                        before_direction=evidence_before_direction(trade_snapshot),
                        after_direction=evidence_after_direction(signal),
                        triggering_signal_sequence=signal_sequence,
                    ),
                )
            if not handle_action(runtime, signal, intent, root=root):
                runtime.engine.restore_trade_state(trade_snapshot)
                reconcile_engine_with_broker(root, runtime)
                mark_processed_shared_canonical_event(runtime, source_tick)
                archive_engine_state(root, runtime, reason="action_rejected")
                break
            mark_processed_shared_canonical_event(runtime, source_tick)
            archive_engine_state(root, runtime, reason="action_executed")
        else:
            mark_processed_shared_canonical_event(runtime, source_tick)
            archive_engine_state(root, runtime, reason=runtime.renko_source)


async def run_market_stream(
    market_data_source: str,
    market_data_profile: str | None,
    mode: str,
    feed: str,
    runtimes: list[TraderRuntime],
    args: argparse.Namespace,
) -> None:
    root = archive_root(args)
    symbols = sorted({runtime.symbol for runtime in runtimes})
    by_symbol: dict[str, list[TraderRuntime]] = {}
    for runtime in runtimes:
        by_symbol.setdefault(runtime.symbol, []).append(runtime)

    if market_data_source == "shared_canonical_stream":
        identities = [shared_consumer_identity(runtime) for runtime in runtimes]
        if len(identities) != len(set(identities)):
            raise RuntimeError(f"duplicate shared canonical consumer identity: {identities}")
        socket_path = getattr(args, "shared_market_socket", None)
        if not socket_path and getattr(args, "shared_market_data_config", None):
            with Path(str(args.shared_market_data_config)).open("r", encoding="utf-8") as handle:
                socket_path = json.load(handle)["socket_path"]
        if not socket_path:
            raise RuntimeError("shared_canonical_stream requires --shared-market-socket or --shared-market-data-config.")
        for runtime in runtimes:
            runtime.log.write(
                "runner_started",
                {
                    "trader_id": runtime.trader_id,
                    "symbol": runtime.symbol,
                    "mode": runtime.mode,
                    "credential_profile": runtime.credential_profile,
                    "account_namespace": runtime.account_namespace,
                    "market_data_source": market_data_source,
                    "market_data_profile": None,
                    "feed": runtime.feed,
                    "renko_source": runtime.renko_source,
                    "execution_mode": "real" if args.execute_real and runtime.execution_enabled else "shadow",
                    "shared_market_socket": socket_path,
                },
            )
            archive_engine_state(root, runtime, reason="runner_started")
        while True:
            consumers: dict[str, SharedCanonicalStreamConsumer] = {}
            streams: dict[str, Any] = {}
            pending_reads: dict[asyncio.Task[Any], str] = {}
            for runtime in runtimes:
                identity = shared_consumer_identity(runtime)
                market = (runtime.contract_fingerprints or {}).get("market_stream") if isinstance(runtime.contract_fingerprints, dict) else None
                fingerprint = market.get("fingerprint") if isinstance(market, dict) else None
                consumer = SharedCanonicalStreamConsumer(
                    socket_path=str(socket_path),
                    identity=identity,
                    expected_contract_fingerprints={runtime.symbol: str(fingerprint)} if fingerprint else {},
                    state_path=root / "market_consumers" / f"{slug(identity)}.json",
                    symbols=(runtime.symbol,),
                    freshness_threshold_seconds=float(getattr(args, "shared_market_freshness_seconds", 5.0)),
                )
                consumers[runtime.trader_id] = consumer
                stream = consumer.connect().__aiter__()
                streams[runtime.trader_id] = stream
                pending_reads[asyncio.create_task(stream.__anext__())] = runtime.trader_id
            try:
                while True:
                    done, _pending = await asyncio.wait(set(pending_reads), timeout=1.0)
                    if not done:
                        for runtime in runtimes:
                            consumer = consumers[runtime.trader_id]
                            block_reason = consumer.signal_block_reason(runtime.symbol)
                            previous_block_reason = runtime.market_data_signal_block_reason
                            object.__setattr__(runtime, "market_data_signal_block_reason", block_reason)
                            if block_reason is not None and block_reason != previous_block_reason:
                                runtime.log.write(
                                    "market_data_signal_block",
                                    {
                                        "trader_id": runtime.trader_id,
                                        "symbol": runtime.symbol,
                                        "reason": block_reason,
                                    },
                                )
                            reconcile_broker_state_if_due_or_halt(root, runtime)
                        continue
                    for completed in done:
                        trader_id = pending_reads.pop(completed)
                        canonical_event = completed.result()
                        consumer = consumers[trader_id]
                        stream = streams[trader_id]
                        pending_reads[asyncio.create_task(stream.__anext__())] = trader_id
                        runtime = next(item for item in runtimes if item.trader_id == trader_id)
                        tick = canonical_event.to_market_tick()
                        block_reason = consumer.signal_block_reason(runtime.symbol)
                        previous_block_reason = runtime.market_data_signal_block_reason
                        if block_reason is not None:
                            object.__setattr__(runtime, "market_data_signal_block_reason", block_reason)
                            runtime.log.write(
                                "market_data_signal_block",
                                {
                                    "trader_id": runtime.trader_id,
                                    "symbol": runtime.symbol,
                                    "reason": block_reason,
                                    "event_id": canonical_event.event_id,
                                    "sequence": canonical_event.sequence,
                                },
                            )
                        process_market_tick_for_runtime(root=root, args=args, runtime=runtime, tick=tick, source_event=None)
                        object.__setattr__(runtime, "market_data_signal_block_reason", previous_block_reason)
                        await consumer.ack(canonical_event)
            except asyncio.CancelledError:
                for task, trader_id in list(pending_reads.items()):
                    await close_stream_after_pending_read(streams[trader_id], task)
                flush_evidence(runtimes)
                raise
            except Exception as exc:
                for task, trader_id in list(pending_reads.items()):
                    await close_stream_after_pending_read(streams[trader_id], task)
                for runtime in runtimes:
                    runtime.log.write(
                        "stream_error",
                        {
                            "trader_id": runtime.trader_id,
                            "stream": "shared_canonical_market_data",
                            "error": str(exc),
                            "timestamp": pd.Timestamp.now(tz="UTC").isoformat(),
                        },
                    )
                    reconcile_broker_state_if_due_or_halt(root, runtime)
                await asyncio.sleep(args.reconnect_delay)

    if market_data_source == "local_archive_tail":
        tail = LocalArchiveMarketDataTail(
            root=market_archive_root(args),
            symbols=symbols,
            poll_seconds=float(getattr(args, "local_market_poll_seconds", 0.1)),
            start_at_end=not bool(getattr(args, "local_market_start_at_beginning", False)),
        )
        for runtime in runtimes:
            runtime.log.write(
                "runner_started",
                {
                    "trader_id": runtime.trader_id,
                    "symbol": runtime.symbol,
                    "mode": runtime.mode,
                    "credential_profile": runtime.credential_profile,
                    "account_namespace": runtime.account_namespace,
                    "market_data_source": market_data_source,
                    "market_data_profile": None,
                    "feed": runtime.feed,
                    "renko_source": runtime.renko_source,
                    "execution_mode": "real" if args.execute_real and runtime.execution_enabled else "shadow",
                },
            )
            archive_engine_state(root, runtime, reason="runner_started")

        async for event in tail.stream():
            if not event.eligible:
                continue
            for runtime in by_symbol.get(event.tick.symbol, []):
                process_market_tick_for_runtime(root=root, args=args, runtime=runtime, tick=event.tick, source_event=event)
        return

    credentials = load_alpaca_credentials(mode, profile=market_data_profile)
    data_url = DATA_STREAM_URL_TEMPLATE.format(feed=feed)
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    source_sequence = 0
    seen_source_records: set[str] = set()

    while True:
        try:
            async with websockets.connect(
                data_url,
                ssl=ssl_context,
                ping_interval=20,
                ping_timeout=20,
                max_queue=None,
            ) as websocket:
                await websocket.send(
                    json.dumps(
                        {
                            "action": "auth",
                            "key": credentials.key_id,
                            "secret": credentials.secret_key,
                        }
                    )
                )
                await websocket.send(json.dumps({"action": "subscribe", "trades": symbols}))
                for runtime in runtimes:
                    runtime.log.write(
                        "runner_started",
                        {
                            "trader_id": runtime.trader_id,
                            "symbol": runtime.symbol,
                            "mode": runtime.mode,
                            "credential_profile": runtime.credential_profile,
                            "account_namespace": runtime.account_namespace,
                            "market_data_source": market_data_source,
                            "market_data_profile": market_data_profile,
                            "feed": runtime.feed,
                            "renko_source": runtime.renko_source,
                            "execution_mode": "real" if args.execute_real and runtime.execution_enabled else "shadow",
                        },
                    )
                    archive_engine_state(root, runtime, reason="runner_started")

                async for raw_message in websocket:
                    messages = decode_ws_payload(raw_message)
                    if isinstance(messages, dict):
                        messages = [messages]

                    for message in messages:
                        message_type = message.get("T")
                        if message_type in {"success", "subscription"}:
                            for runtime in runtimes:
                                runtime.log.write(
                                    "stream_status",
                                    {
                                        "trader_id": runtime.trader_id,
                                        "mode": mode,
                                        "feed": feed,
                                        "payload": message,
                                    },
                                )
                            continue
                        if message_type == "error":
                            raise RuntimeError(f"Alpaca market stream error: {message}")
                        if message_type != "t":
                            continue

                        source_sequence += 1
                        source_event = live_market_event_from_alpaca_message(message, source_sequence=source_sequence)
                        tick = source_event.tick
                        if source_event.source_record_id in seen_source_records:
                            continue
                        seen_source_records.add(source_event.source_record_id)
                        if not args.no_persist_ticks:
                            archive_tick(root, tick, mode=mode, feed=feed, source_event=source_event)
                        if not source_event.eligible:
                            continue
                        for runtime in by_symbol.get(tick.symbol, []):
                            process_market_tick_for_runtime(
                                root=root,
                                args=args,
                                runtime=runtime,
                                tick=tick,
                                source_event=source_event,
                            )
        except asyncio.CancelledError:
            flush_evidence(runtimes)
            raise
        except Exception as exc:
            for runtime in runtimes:
                runtime.log.write(
                    "stream_error",
                    {
                        "trader_id": runtime.trader_id,
                        "stream": "market_data",
                        "mode": mode,
                        "feed": feed,
                        "error": str(exc),
                        "timestamp": pd.Timestamp.now(tz="UTC").isoformat(),
                    },
            )
            await asyncio.sleep(args.reconnect_delay)


async def run_force_flat_scheduler(runtimes: list[TraderRuntime], args: argparse.Namespace) -> None:
    root = archive_root(args)
    while True:
        timestamp = pd.Timestamp.now(tz="UTC")
        for runtime in runtimes:
            if not runtime.enabled:
                continue
            try:
                broker_force_flat_if_due(root, runtime, timestamp, source="scheduler")
            except Exception as exc:
                runtime.log.write(
                    "broker_force_flat_scheduler_error",
                    {
                        "trader_id": runtime.trader_id,
                        "symbol": runtime.symbol,
                        "error": str(exc),
                        "timestamp": timestamp.isoformat(),
                    },
                )
        await asyncio.sleep(BROKER_FORCE_FLAT_POLL_SECONDS)


async def async_main() -> None:
    load_dotenv()
    args = build_parser().parse_args()
    config = load_config(args.config)
    active_live_requested = (args.force_mode or "").lower() == "live" or any(
        isinstance(trader, dict)
        and bool(trader.get("enabled", True))
        and bool(trader.get("execution_enabled", True))
        and str(trader.get("mode") or "paper").lower() == "live"
        for trader in config.get("traders", [])
    )
    if args.execute_real and active_live_requested:
        validate_production_fleet_config(config, config_path=args.config)
    runtimes = build_runtimes(config, args)
    grouped = group_runtimes(runtimes)
    if not grouped:
        raise RuntimeError("No enabled traders in fleet config.")

    tasks = [
        run_market_stream(
            market_data_source=market_data_source,
            market_data_profile=market_data_profile or None,
            mode=mode,
            feed=feed,
            runtimes=items,
            args=args,
        )
        for (market_data_source, market_data_profile, mode, feed), items in grouped.items()
    ]
    tasks.append(run_force_flat_scheduler(runtimes, args))
    await asyncio.gather(*tasks)


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        print("Stopped fleet runner.")


if __name__ == "__main__":
    main()
