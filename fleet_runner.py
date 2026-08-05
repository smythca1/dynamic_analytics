from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import ssl
import sys
import time
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable
from datetime import date, timedelta

import certifi
import pandas as pd
import websockets
from dotenv import load_dotenv

from alpaca_trader import load_alpaca_credentials
from execution_engine import AlpacaExecutionEngine, JsonlEventLog, ShadowExecutionEngine
from shared.config.trader_config import (
    as_float,
    build_strategy_config,
    load_production_renko_config,
    normalize_renko_source,
)
from renko_builder import RenkoBuildState, initial_renko_close
from silver.evidence import EvidenceLogger
from silver.evidence.models import EvidenceCheckpoint, UNKNOWN
from silver.evidence.shadow_sampler import EvidenceShadowSampler, ShadowSamplerSettings
from strategy_engine import MeanReversionStrategyConfig, MeanReversionStrategyEngine, RenkoStrategyConfig, RenkoStrategyEngine
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
    renko_source: str = "ticks"
    evidence: EvidenceLogger | None = None
    shadow_sampler: EvidenceShadowSampler | None = None
    config_hash: str | None = None
    migration_evidence_path: str | None = None


_PENDING_1S_CLOSE_TICKS: dict[str, tuple[pd.Timestamp, MarketTick]] = {}
_PRODUCTION_OBSERVATION_SEQUENCES: dict[tuple[str, str], int] = {}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a config-driven fleet of live strategy traders.")
    parser.add_argument("--config", required=True, help="Fleet JSON config path.")
    parser.add_argument("--execute-real", action="store_true", help="Submit Alpaca orders. Defaults to shadow mode.")
    parser.add_argument("--force-mode", choices=["paper", "live"], help="Override trader mode for the whole fleet.")
    parser.add_argument("--force-feed", choices=["iex", "sip", "delayed_sip", "boats", "overnight"], help="Override trader feed.")
    parser.add_argument("--log-output", help="Override all trader event logs.")
    parser.add_argument("--archive-root", default="artifacts/live", help="Root directory for tick, Renko, and state archives.")
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


def renko_source_ticks(runtime: TraderRuntime, tick: MarketTick) -> list[MarketTick]:
    if runtime.renko_source == "ticks":
        return [tick]

    bucket = tick_second_bucket(tick.timestamp)
    pending = _PENDING_1S_CLOSE_TICKS.get(runtime.trader_id)
    if pending is None:
        _PENDING_1S_CLOSE_TICKS[runtime.trader_id] = (bucket, tick)
        return []

    pending_bucket, pending_tick = pending
    if bucket == pending_bucket:
        _PENDING_1S_CLOSE_TICKS[runtime.trader_id] = (bucket, tick)
        return []
    if bucket < pending_bucket:
        return []

    _PENDING_1S_CLOSE_TICKS[runtime.trader_id] = (bucket, tick)
    return [
        MarketTick(
            timestamp=pending_bucket,
            symbol=pending_tick.symbol,
            price=pending_tick.price,
            size=pending_tick.size,
            exchange=pending_tick.exchange,
            trade_id=pending_tick.trade_id,
            tape=pending_tick.tape,
            conditions=pending_tick.conditions,
        )
    ]


def archive_root(args: argparse.Namespace) -> Path:
    return Path(str(args.archive_root))


def slug(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_") or "unknown"


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


def archive_tick(root: Path, tick: MarketTick, mode: str, feed: str) -> None:
    payload = dataclass_to_jsonable(tick)
    payload["mode"] = mode
    payload["feed"] = feed
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
        "feed": runtime.feed,
        "execution_enabled": runtime.execution_enabled,
        "configuration_hash": runtime.config_hash,
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
    if runtime.renko_source != "1s_close":
        return {
            "production_source_mode": runtime.renko_source,
            "pending_state": UNKNOWN,
            "unknown_fields": ["pending_second_bucket", "pending_trade"],
        }
    pending = _PENDING_1S_CLOSE_TICKS.get(runtime.trader_id)
    if pending is None:
        return {
            "production_source_mode": runtime.renko_source,
            "pending_second_bucket": None,
            "pending_price": None,
            "pending_sip_timestamp": None,
            "pending_trade_id": None,
            "unknown_fields": ["source_record_id"],
        }
    pending_bucket, pending_tick = pending
    return {
        "production_source_mode": runtime.renko_source,
        "pending_second_bucket": pending_bucket.isoformat(),
        "pending_price": pending_tick.price,
        "pending_sip_timestamp": pending_tick.timestamp.isoformat(),
        "pending_trade_id": pending_tick.trade_id or None,
        "unknown_fields": ["source_record_id"],
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
        runtime.engine.on_tick(tick)

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


def reconcile_engine_with_broker(root: Path, runtime: TraderRuntime) -> None:
    if not isinstance(runtime.execution, AlpacaExecutionEngine):
        return

    broker_position = runtime.execution.trader._get_position(runtime.symbol)
    engine_trade = runtime.engine.trade_state_snapshot()
    engine_position = engine_trade.get("position")

    if broker_position is None and engine_position is not None:
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
        archive_engine_state(root, runtime, reason="broker_position_exists_engine_flat")
        raise BrokerEngineStateMismatch(
            f"{runtime.trader_id} has broker {runtime.symbol} position but engine is flat"
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
        mark_runtime_force_flat_state(root, runtime, current_date, reason=f"broker_force_flat_already_handled_{source}")
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
    try:
        runtime.execution.execute(intent)
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
        data = trader.get("data") if isinstance(trader.get("data"), dict) else {}
        feed = args.force_feed or str(data.get("feed") or "iex").lower()
        renko_source = normalize_renko_source(data.get("renko_source"))
        log_path = args.log_output or trader.get("event_log") or "artifacts/live/fleet_events.jsonl"
        risk = trader.get("risk") if isinstance(trader.get("risk"), dict) else {}
        max_position_notional = as_float(risk.get("max_position_notional"), default=250000.0)
        max_daily_loss = as_float(risk.get("max_daily_loss"), default=0.0)
        execution_enabled = bool(trader.get("execution_enabled", True))
        behavior_block_reason = behavior_profile_block_reason(trader)
        log = JsonlEventLog(log_path)
        strategy_config = build_strategy_config(trader)
        if isinstance(strategy_config, MeanReversionStrategyConfig):
            engine = MeanReversionStrategyEngine(strategy_config)
        else:
            engine = RenkoStrategyEngine(strategy_config)
        execution = (
            AlpacaExecutionEngine(
                mode=mode,
                credential_profile=credential_profile,
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
            feed=feed,
            renko_source=renko_source,
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


def group_runtimes(runtimes: list[TraderRuntime]) -> dict[tuple[str, str], list[TraderRuntime]]:
    grouped: dict[tuple[str, str], list[TraderRuntime]] = {}
    for runtime in runtimes:
        if not runtime.enabled:
            continue
        grouped.setdefault((runtime.mode, runtime.feed), []).append(runtime)
    return grouped


def decode_ws_payload(raw_message: str | bytes) -> Any:
    if isinstance(raw_message, bytes):
        raw_message = raw_message.decode("utf-8")
    return json.loads(raw_message)


def should_block_for_risk(runtime: TraderRuntime, signal: Signal, intent: OrderIntent) -> str | None:
    if signal.signal_type != "entry":
        return None
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


def handle_action(runtime: TraderRuntime, signal: Signal, intent: OrderIntent) -> bool:
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
        runtime.execution.execute(intent)
    except Exception as exc:
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
    return True


async def run_market_stream(
    mode: str,
    feed: str,
    runtimes: list[TraderRuntime],
    args: argparse.Namespace,
) -> None:
    credentials = load_alpaca_credentials(mode, profile=None)
    data_url = DATA_STREAM_URL_TEMPLATE.format(feed=feed)
    root = archive_root(args)
    symbols = sorted({runtime.symbol for runtime in runtimes})
    by_symbol: dict[str, list[TraderRuntime]] = {}
    for runtime in runtimes:
        by_symbol.setdefault(runtime.symbol, []).append(runtime)
    ssl_context = ssl.create_default_context(cafile=certifi.where())

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

                        tick = MarketTick.from_alpaca_trade(message)
                        if not args.no_persist_ticks:
                            archive_tick(root, tick, mode=mode, feed=feed)
                        if not is_price_forming_trade(tick):
                            continue
                        for runtime in by_symbol.get(tick.symbol, []):
                            record_shadow_for_eligible_tick(runtime, tick)
                            if args.log_ticks:
                                payload = dataclass_to_jsonable(tick)
                                payload["trader_id"] = runtime.trader_id
                                runtime.log.write("market_tick", payload)

                            broker_force_flat_if_due(
                                root,
                                runtime,
                                tick.timestamp,
                                price=tick.price,
                                source="tick",
                            )
                            reconcile_broker_state_if_due(root, runtime)
                            source_ticks = renko_source_ticks(runtime, tick)
                            if not source_ticks:
                                archive_engine_state(root, runtime, reason="tick")
                                continue

                            for source_tick in source_ticks:
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
                                    safe_evidence_call(
                                        runtime,
                                        "observation",
                                        lambda: runtime.evidence.record_observation(
                                            trader_id=runtime.trader_id,
                                            symbol=runtime.symbol,
                                            observation_bucket_timestamp=source_tick.timestamp,
                                            close_price=source_tick.price,
                                            source_trade_identity=source_tick.trade_id or None,
                                            source_trade_timestamp=source_tick.timestamp if runtime.renko_source == "ticks" else UNKNOWN,
                                            triggering_trade_identity=tick.trade_id or None,
                                            triggering_trade_timestamp=tick.timestamp,
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
                                    if not handle_action(runtime, signal, intent):
                                        runtime.engine.restore_trade_state(trade_snapshot)
                                        reconcile_engine_with_broker(root, runtime)
                                        archive_engine_state(root, runtime, reason="action_rejected")
                                        break
                                    archive_engine_state(root, runtime, reason="action_executed")
                                else:
                                    archive_engine_state(root, runtime, reason=runtime.renko_source)
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
    runtimes = build_runtimes(config, args)
    grouped = group_runtimes(runtimes)
    if not grouped:
        raise RuntimeError("No enabled traders in fleet config.")

    tasks = [run_market_stream(mode=mode, feed=feed, runtimes=items, args=args) for (mode, feed), items in grouped.items()]
    tasks.append(run_force_flat_scheduler(runtimes, args))
    await asyncio.gather(*tasks)


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        print("Stopped fleet runner.")


if __name__ == "__main__":
    main()
