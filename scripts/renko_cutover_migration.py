#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from replay.historical_source import file_sha256, read_historical_source
from replay.models import ReplayConfig
from shared.config.trader_config import load_production_renko_config
from silver.market import CANONICAL_RENKO_SOURCE, CanonicalOneSecondSipSampler, contract_snapshot, observation_to_market_tick
from strategy_engine import RenkoStrategyEngine, TP_SL_LIFECYCLE_FLAT


SCHEMA_VERSION = "renko_cutover_migration_state.v1"
MANIFEST_VERSION = "renko_cutover_migration_manifest.v1"


class MigrationError(RuntimeError):
    pass


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result = run_migration(args)
    except MigrationError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline fail-closed Renko canonical cutover state migration.")
    parser.add_argument("--mode", required=True, choices=("camatron-continuation", "megatron-flat-boundary"))
    parser.add_argument("--account", required=True)
    parser.add_argument("--trader-id", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--cutoff", required=True, help="Inclusive UTC cutoff timestamp.")
    parser.add_argument("--config", required=True, help="Target canonical trader config JSON.")
    parser.add_argument("--state", required=True, help="Current deployed trader state JSON copied read-only.")
    parser.add_argument("--broker-snapshot", required=True, help="Read-only broker snapshot JSON.")
    parser.add_argument("--sip-file", required=True, action="append", help="Explicit immutable SIP input file. Repeatable.")
    parser.add_argument("--session-date", required=True, action="append", help="Required session date covered by SIP input.")
    parser.add_argument("--output-dir", required=True, help="New staging directory. Must be absolute and non-live.")
    parser.add_argument("--old-renko-source", required=True, help="Effective pre-migration production renko_source.")
    parser.add_argument("--validation-only", action="store_true")
    return parser


def run_migration(args: argparse.Namespace) -> dict[str, Any]:
    symbol = args.symbol.upper()
    cutoff = parse_utc(args.cutoff)
    output_dir = validate_output_dir(args.output_dir, validation_only=bool(args.validation_only))

    loaded = load_production_renko_config(args.config, args.trader_id)
    target = loaded.strategy_config
    if target.symbol.upper() != symbol:
        raise MigrationError(f"{args.trader_id}: target config symbol {target.symbol} does not match requested {symbol}.")

    state_path = Path(args.state)
    state = load_json(state_path)
    broker_snapshot_path = Path(args.broker_snapshot)
    broker_snapshot = load_json(broker_snapshot_path)
    account_evidence = broker_account_evidence(broker_snapshot, args.account)
    symbol_evidence = broker_symbol_evidence(account_evidence, symbol)

    reconcile = reconcile_mode(
        mode=args.mode,
        symbol=symbol,
        state=state,
        broker=symbol_evidence,
        old_renko_source=args.old_renko_source,
    )

    trades, source_summary = load_explicit_sip_trades(
        sip_files=[Path(item) for item in args.sip_file],
        session_dates=tuple(args.session_date),
        symbol=symbol,
        trader_id=args.trader_id,
        cutoff=cutoff,
        target=target,
    )
    reconstructed = reconstruct_state(
        trades=trades,
        trader_id=args.trader_id,
        symbol=symbol,
        target=target,
        cutoff=cutoff,
        mode=args.mode,
        prior_state=state,
    )
    continuation_match = None
    if args.mode == "camatron-continuation":
        continuation_match = compare_engine_state(reconstructed["engine"], (state.get("engine") or {}))
        if not continuation_match["matches"]:
            raise MigrationError(
                f"{args.trader_id}: Camatron exact continuation mismatch: "
                f"{continuation_match['reason']}"
            )

    checkpoint_validation = validate_checkpoint_restore(
        trades=trades,
        trader_id=args.trader_id,
        symbol=symbol,
        target=target,
        mode=args.mode,
    )
    if not checkpoint_validation["matches"]:
        raise MigrationError(
            f"{args.trader_id}: checkpoint continuation validation failed: "
            f"{checkpoint_validation['reason']}"
        )

    source_dataset_hash = stable_hash(
        {
            "sip_files": source_summary["files"],
            "cutoff": cutoff.isoformat(),
            "symbol": symbol,
        }
    )
    fingerprints = contract_snapshot(
        symbol=symbol,
        strategy_config=target,
        session_timezone=target.timezone,
        source_dataset_hash=source_dataset_hash,
    )
    staged_state = {
        "schema_version": SCHEMA_VERSION,
        "migration_mode": args.mode,
        "account": args.account,
        "trader_id": args.trader_id,
        "symbol": symbol,
        "cutoff": cutoff.isoformat(),
        "configuration_hash": loaded.config_hash,
        "target_config_path": str(Path(args.config).resolve()),
        "old_state_hash": file_sha256(state_path),
        "broker_snapshot_hash": file_sha256(broker_snapshot_path),
        "source_summary": source_summary,
        "contract_fingerprints": fingerprints,
        "old_contract": {
            "renko_source": args.old_renko_source,
            "state_configuration_hash": state.get("configuration_hash"),
            "state_contract_fingerprints": state.get("contract_fingerprints"),
        },
        "new_contract": {
            "renko_source": CANONICAL_RENKO_SOURCE,
            "contract_fingerprints": fingerprints,
        },
        "sampler_state": reconstructed["sampler_state"],
        "engine": reconstructed["engine"],
        "canonical_accounting_state": canonical_accounting_state(symbol_evidence),
        "pending_orders_state": {
            "status": "VERIFIED_NONE",
            "source": "read_only_broker_snapshot",
            "open_orders": [],
        },
        "migration_marker": migration_marker(args.mode, args.old_renko_source, state, cutoff),
        "validation": {
            "broker_internal_reconciliation": reconcile,
            "checkpoint_restoration": checkpoint_validation,
            "observation_fingerprint": reconstructed["observation_fingerprint"],
            "renko_fingerprint": reconstructed["renko_fingerprint"],
            "signal_fingerprint": reconstructed["signal_fingerprint"],
            "intent_fingerprint": reconstructed["intent_fingerprint"],
            "no_order_submission_api_used": True,
            "camatron_exact_continuation": continuation_match,
        },
    }
    manifest = build_manifest(
        args=args,
        state_path=state_path,
        broker_snapshot_path=broker_snapshot_path,
        loaded_config_hash=loaded.config_hash,
        source_summary=source_summary,
        staged_state=staged_state,
    )

    if args.validation_only:
        return {
            "status": "VALIDATION_ONLY_PASS",
            "trader_id": args.trader_id,
            "symbol": symbol,
            "mode": args.mode,
            "checkpoint_restoration_matches": True,
            "state_fingerprint": manifest["outputs"]["state_sha256"],
        }

    output_dir.mkdir(parents=False, exist_ok=False)
    state_output = output_dir / "state.json"
    manifest_output = output_dir / "manifest.json"
    validation_output = output_dir / "validation.json"
    write_json(state_output, staged_state)
    write_json(manifest_output, manifest)
    write_json(validation_output, staged_state["validation"])
    return {
        "status": "STAGED",
        "trader_id": args.trader_id,
        "symbol": symbol,
        "mode": args.mode,
        "output_dir": str(output_dir),
        "state_sha256": file_sha256(state_output),
        "manifest_sha256": file_sha256(manifest_output),
        "validation_sha256": file_sha256(validation_output),
    }


def validate_output_dir(raw: str, *, validation_only: bool) -> Path:
    candidate = Path(raw).expanduser()
    if not candidate.is_absolute():
        raise MigrationError("output-dir must be an absolute path.")
    resolved = candidate.resolve()
    text = str(resolved)
    if resolved == Path("/") or resolved.parent == resolved:
        raise MigrationError("output-dir is too broad.")
    if "/artifacts/live/" in text or text.endswith("/artifacts/live"):
        raise MigrationError("output-dir must not target live production artifacts.")
    if "/configs/" in text or text.endswith("/configs"):
        raise MigrationError("output-dir must not target configuration directories.")
    if not validation_only and resolved.exists() and any(resolved.iterdir()):
        raise MigrationError("output-dir must be new or empty.")
    return resolved


def parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise MigrationError("cutoff must be timezone-aware.")
    return parsed.astimezone(timezone.utc)


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise MigrationError(f"required file does not exist: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise MigrationError(f"expected JSON object: {path}")
    return payload


def broker_account_evidence(snapshot: dict[str, Any], account: str) -> dict[str, Any]:
    accounts = snapshot.get("accounts")
    if isinstance(accounts, dict):
        evidence = accounts.get(account)
        if not isinstance(evidence, dict):
            raise MigrationError(f"broker snapshot has no account evidence for {account!r}.")
        return evidence
    return snapshot


def broker_symbol_evidence(account_evidence: dict[str, Any], symbol: str) -> dict[str, Any]:
    positions = [row for row in account_evidence.get("positions", []) if str(row.get("symbol", "")).upper() == symbol]
    if len(positions) > 1:
        raise MigrationError(f"{symbol}: broker snapshot has multiple position rows.")
    open_orders = [row for row in account_evidence.get("open_orders", []) if str(row.get("symbol", "")).upper() == symbol]
    recent_orders = [row for row in account_evidence.get("recent_orders", []) if str(row.get("symbol", "")).upper() == symbol]
    return {
        "account": account_evidence.get("account", {}),
        "position": positions[0] if positions else None,
        "open_orders": open_orders,
        "recent_orders": recent_orders,
    }


def reconcile_mode(
    *,
    mode: str,
    symbol: str,
    state: dict[str, Any],
    broker: dict[str, Any],
    old_renko_source: str,
) -> dict[str, Any]:
    open_orders = broker["open_orders"]
    if open_orders:
        raise MigrationError(f"{symbol}: unresolved broker open orders block migration.")
    internal = internal_position(state, symbol)
    broker_position = broker_position_state(broker["position"], symbol)
    pending_exit = ((state.get("engine") or {}).get("trade") or {}).get("pending_exit")
    lifecycle = ((state.get("engine") or {}).get("trade") or {}).get("tp_sl_lifecycle_state")

    if mode == "megatron-flat-boundary":
        if old_renko_source == CANONICAL_RENKO_SOURCE:
            raise MigrationError("megatron-flat-boundary requires a noncanonical old renko_source.")
        if broker_position is not None:
            raise MigrationError(f"{symbol}: Megatron flat-boundary requires broker flat; broker is {broker_position}.")
        if internal is not None:
            raise MigrationError(f"{symbol}: Megatron flat-boundary requires internal flat; internal is {internal}.")
        if pending_exit is not None:
            raise MigrationError(f"{symbol}: pending TP/SL exit blocks flat-boundary migration.")
        if lifecycle not in {None, TP_SL_LIFECYCLE_FLAT}:
            raise MigrationError(f"{symbol}: TP/SL lifecycle must be FLAT for flat-boundary migration; got {lifecycle!r}.")
        return {
            "status": "PASS",
            "mode": mode,
            "broker_position": "FLAT",
            "internal_position": "FLAT",
            "open_orders": 0,
        }

    if mode == "camatron-continuation":
        if old_renko_source != CANONICAL_RENKO_SOURCE:
            raise MigrationError("camatron-continuation requires an already canonical old renko_source.")
        if broker_position is None or internal is None:
            if broker_position != internal:
                raise MigrationError(f"{symbol}: broker/internal position mismatch: broker={broker_position} internal={internal}.")
        elif broker_position["side"] != internal["side"] or broker_position["qty"] != internal["qty"]:
            raise MigrationError(f"{symbol}: broker/internal position mismatch: broker={broker_position} internal={internal}.")
        return {
            "status": "PASS",
            "mode": mode,
            "broker_position": broker_position or "FLAT",
            "internal_position": internal or "FLAT",
            "open_orders": 0,
        }

    raise MigrationError(f"unsupported migration mode {mode}.")


def internal_position(state: dict[str, Any], symbol: str) -> dict[str, Any] | None:
    position = ((state.get("engine") or {}).get("trade") or {}).get("position")
    if not isinstance(position, dict):
        return None
    qty = abs_decimal(position.get("qty"))
    if qty == 0:
        return None
    return {
        "symbol": symbol,
        "side": str(position.get("side")),
        "qty": str(qty.normalize()),
        "entry_price": str(position.get("entry_price")),
        "entry_time": position.get("entry_time"),
    }


def broker_position_state(position: dict[str, Any] | None, symbol: str) -> dict[str, Any] | None:
    if not isinstance(position, dict):
        return None
    qty_signed = Decimal(str(position.get("qty", "0")))
    if qty_signed == 0:
        return None
    side = str(position.get("side") or ("long" if qty_signed > 0 else "short")).lower()
    return {
        "symbol": symbol,
        "side": side,
        "qty": str(abs(qty_signed).normalize()),
        "avg_entry_price": str(position.get("avg_entry_price")),
        "market_value": position.get("market_value"),
        "unrealized_pl": position.get("unrealized_pl"),
    }


def canonical_accounting_state(broker: dict[str, Any]) -> dict[str, Any]:
    position = broker_position_state(broker["position"], str((broker["position"] or {}).get("symbol", "")))
    return {
        "version": "silver.accounting.cutover.v1",
        "basis_source": "read_only_broker_snapshot_avg_entry_price",
        "position": position,
        "raw_broker_position": broker["position"],
    }


def load_explicit_sip_trades(
    *,
    sip_files: list[Path],
    session_dates: tuple[str, ...],
    symbol: str,
    trader_id: str,
    cutoff: datetime,
    target: Any,
) -> tuple[list[Any], dict[str, Any]]:
    if not sip_files:
        raise MigrationError("at least one --sip-file is required.")
    required_dates = {str(item) for item in session_dates}
    seen_dates: set[str] = set()
    all_trades: list[Any] = []
    files = []
    for path in sip_files:
        if not path.exists():
            raise MigrationError(f"SIP input does not exist: {path}")
        source_hash = file_sha256(path)
        date_hint = date_hint_from_path(path)
        if date_hint:
            seen_dates.add(date_hint)
        config = ReplayConfig(
            run_id=f"migration:{trader_id}",
            trader_id=trader_id,
            symbol=symbol,
            start="1900-01-01T00:00:00Z",
            end=cutoff,
            timezone=target.timezone,
            session_mode="regular",
            strategy_config=asdict(target),
            initial_capital=0,
            brick_mode="PRODUCTION_CONFIG",
            source_data_path=str(path),
        )
        result = read_historical_source(config)
        if result.malformed_rows:
            raise MigrationError(f"{path}: malformed SIP rows present ({result.malformed_rows}).")
        all_trades.extend(result.trades)
        files.append(
            {
                "path": str(path.resolve()),
                "sha256": source_hash,
                "date_hint": date_hint,
                "raw_rows_read": result.raw_rows_read,
                "raw_trades_in_window": result.raw_trades_in_window,
                "timestamp_start": result.integrity.timestamp_range_start.isoformat() if result.integrity.timestamp_range_start else None,
                "timestamp_end": result.integrity.timestamp_range_end.isoformat() if result.integrity.timestamp_range_end else None,
            }
        )
    missing = sorted(required_dates - seen_dates)
    if missing:
        raise MigrationError(f"incomplete SIP coverage; missing explicit files for sessions {missing}.")
    all_trades.sort(key=lambda trade: trade.ordering_key)
    if not all_trades:
        raise MigrationError("no SIP trades found at or before cutoff.")
    return all_trades, {
        "symbol": symbol,
        "cutoff": cutoff.isoformat(),
        "required_session_dates": sorted(required_dates),
        "files": files,
        "trade_count_through_cutoff": len(all_trades),
        "first_trade": all_trades[0].sip_timestamp.isoformat(),
        "last_trade": all_trades[-1].sip_timestamp.isoformat(),
        "combined_input_hash": stable_hash({"files": files}),
    }


def date_hint_from_path(path: Path) -> str | None:
    for part in reversed(path.parts):
        if len(part) >= 10 and part[:4].isdigit() and part[4] == "-" and part[7] == "-":
            return part[:10]
    return None


def reconstruct_state(
    *,
    trades: list[Any],
    trader_id: str,
    symbol: str,
    target: Any,
    cutoff: datetime,
    mode: str,
    prior_state: dict[str, Any],
) -> dict[str, Any]:
    sampler = CanonicalOneSecondSipSampler(run_id=f"migration:{trader_id}")
    engine = RenkoStrategyEngine(target)
    observation_keys: list[dict[str, Any]] = []
    brick_keys: list[dict[str, Any]] = []
    signal_keys: list[dict[str, Any]] = []
    intent_keys: list[dict[str, Any]] = []
    for trade in trades:
        if trade.sip_timestamp > cutoff:
            continue
        result = sampler.process_trade(trade)
        for observation in result.observations:
            observation_keys.append(observation_key(observation))
            bricks, actions = engine.on_tick(observation_to_market_tick(observation))
            brick_keys.extend(brick_key(item) for item in bricks)
            for signal, intent in actions:
                signal_keys.append(signal_key(signal))
                intent_keys.append(intent_key(intent))

    engine_snapshot = engine.snapshot()
    if mode == "megatron-flat-boundary":
        engine_snapshot["trade"] = flat_trade_state(prior_state)

    return {
        "sampler_state": sampler.snapshot(scope_key=(trader_id, symbol)),
        "engine": engine_snapshot,
        "observation_fingerprint": stable_hash({"observations": observation_keys}),
        "renko_fingerprint": stable_hash({"bricks": brick_keys, "renko": engine_snapshot["renko"]}),
        "signal_fingerprint": stable_hash({"signals": signal_keys}),
        "intent_fingerprint": stable_hash({"intents": intent_keys}),
    }


def validate_checkpoint_restore(
    *,
    trades: list[Any],
    trader_id: str,
    symbol: str,
    target: Any,
    mode: str,
) -> dict[str, Any]:
    if len(trades) < 3:
        return {
            "matches": True,
            "reason": "insufficient trades for split replay; no continuation risk exercised",
            "split_index": None,
        }
    split_index = max(1, len(trades) // 2)
    uninterrupted_sampler = CanonicalOneSecondSipSampler(run_id=f"migration:{trader_id}:checkpoint")
    uninterrupted_engine = RenkoStrategyEngine(target)
    restored_sampler = CanonicalOneSecondSipSampler(run_id=f"migration:{trader_id}:checkpoint")
    restored_engine = RenkoStrategyEngine(target)

    for trade in trades[:split_index]:
        process_trade(uninterrupted_sampler, uninterrupted_engine, trade)
        process_trade(restored_sampler, restored_engine, trade)
    sampler_snapshot = restored_sampler.snapshot(scope_key=(trader_id, symbol))
    engine_snapshot = restored_engine.snapshot()
    if mode == "megatron-flat-boundary":
        engine_snapshot["trade"] = flat_trade_state({})

    restore_probe = RenkoStrategyEngine(target)
    restore_probe.restore_snapshot(engine_snapshot)
    expected_processed = int((engine_snapshot.get("renko") or {}).get("processed_bricks") or 0)
    restored_processed = int((restore_probe.snapshot().get("renko") or {}).get("processed_bricks") or 0)
    if restored_processed != expected_processed:
        return {
            "matches": False,
            "reason": "target runtime restore_snapshot does not preserve processed_bricks",
            "split_index": split_index,
            "expected_processed_bricks": expected_processed,
            "restored_processed_bricks": restored_processed,
        }

    if mode == "megatron-flat-boundary":
        uninterrupted_engine = RenkoStrategyEngine(target)
        uninterrupted_engine.restore_snapshot(engine_snapshot)

    for trade in trades[split_index:]:
        process_trade(uninterrupted_sampler, uninterrupted_engine, trade)

    resumed_sampler = CanonicalOneSecondSipSampler(run_id=f"migration:{trader_id}:checkpoint")
    resumed_sampler.restore_snapshot(sampler_snapshot)
    resumed_engine = RenkoStrategyEngine(target)
    resumed_engine.restore_snapshot(engine_snapshot)
    for trade in trades[split_index:]:
        process_trade(resumed_sampler, resumed_engine, trade)

    uninterrupted_payload = {
        "sampler": uninterrupted_sampler.snapshot(scope_key=(trader_id, symbol)),
        "engine": uninterrupted_engine.snapshot(),
    }
    resumed_payload = {
        "sampler": resumed_sampler.snapshot(scope_key=(trader_id, symbol)),
        "engine": resumed_engine.snapshot(),
    }
    left = stable_hash(uninterrupted_payload)
    right = stable_hash(resumed_payload)
    return {
        "matches": left == right,
        "reason": "match" if left == right else "restored sampler/engine continuation diverged from uninterrupted processing",
        "split_index": split_index,
        "uninterrupted_fingerprint": left,
        "restored_fingerprint": right,
    }


def compare_engine_state(reconstructed: dict[str, Any], production: dict[str, Any]) -> dict[str, Any]:
    reconstructed_key = engine_comparison_key(reconstructed)
    production_key = engine_comparison_key(production)
    if reconstructed_key == production_key:
        return {
            "matches": True,
            "reason": "match",
            "fingerprint": stable_hash(reconstructed_key),
        }
    return {
        "matches": False,
        "reason": "reconstructed_engine_state_does_not_match_production_state",
        "reconstructed": reconstructed_key,
        "production": production_key,
        "reconstructed_fingerprint": stable_hash(reconstructed_key),
        "production_fingerprint": stable_hash(production_key),
    }


def engine_comparison_key(engine: dict[str, Any]) -> dict[str, Any]:
    renko = engine.get("renko") if isinstance(engine.get("renko"), dict) else {}
    trade = engine.get("trade") if isinstance(engine.get("trade"), dict) else {}
    position = trade.get("position") if isinstance(trade.get("position"), dict) else None
    if position is not None:
        position = {
            "symbol": str(position.get("symbol", "")).upper(),
            "side": str(position.get("side")),
            "entry_time": str(position.get("entry_time")),
            "entry_price": f"{float(position.get('entry_price')):.12g}",
            "qty": f"{float(position.get('qty')):.12g}",
        }
    return {
        "symbol": str(engine.get("symbol", "")).upper(),
        "strategy_type": engine.get("strategy_type"),
        "renko": {
            "brick_size": f"{float(renko.get('brick_size')):.12g}" if renko.get("brick_size") is not None else None,
            "last_brick_close": f"{float(renko.get('last_brick_close')):.12g}" if renko.get("last_brick_close") is not None else None,
            "run_dir": int(renko.get("run_dir") or 0),
            "run_len": int(renko.get("run_len") or 0),
            "processed_bricks": int(renko.get("processed_bricks") or 0),
            "anchor_mode": renko.get("anchor_mode") or "first_price",
            "anchor_price": f"{float(renko.get('anchor_price')):.12g}" if renko.get("anchor_price") is not None else None,
        },
        "trade": {
            "position": position,
            "tp_sl_lifecycle_state": trade.get("tp_sl_lifecycle_state"),
            "pending_exit": trade.get("pending_exit"),
            "force_flat_dates": sorted(trade.get("force_flat_dates") or []),
            "reconciliation_unsafe": bool(trade.get("reconciliation_unsafe", False)),
        },
    }


def process_trade(sampler: CanonicalOneSecondSipSampler, engine: RenkoStrategyEngine, trade: Any) -> None:
    result = sampler.process_trade(trade)
    for observation in result.observations:
        engine.on_tick(observation_to_market_tick(observation))


def flat_trade_state(prior_state: dict[str, Any]) -> dict[str, Any]:
    prior_trade = ((prior_state.get("engine") or {}).get("trade") or {}) if isinstance(prior_state, dict) else {}
    return {
        "position": None,
        "psar_sar": None,
        "psar_ep": None,
        "psar_af": None,
        "force_flat_dates": sorted(prior_trade.get("force_flat_dates") or []),
        "tp_sl_lifecycle_state": TP_SL_LIFECYCLE_FLAT,
        "pending_exit": None,
        "last_closed_exit": None,
        "reconciliation_unsafe": False,
    }


def migration_marker(mode: str, old_renko_source: str, prior_state: dict[str, Any], cutoff: datetime) -> dict[str, Any]:
    if mode == "megatron-flat-boundary":
        return {
            "type": "CONTRACT_MIGRATION_FLAT",
            "cutoff": cutoff.isoformat(),
            "pre_boundary_renko_source": old_renko_source,
            "post_boundary_renko_source": CANONICAL_RENKO_SOURCE,
            "intentionally_reset_fields": [
                "strategy_position",
                "canonical_accounting_position",
                "tp_sl_lifecycle_state",
                "pending_exit",
                "last_closed_exit",
            ],
            "reconstructed_fields": [
                "sampler_state",
                "renko_state",
                "market_stream_contract_fingerprint",
                "renko_strategy_contract_fingerprint",
            ],
            "prior_last_closed_exit": ((prior_state.get("engine") or {}).get("trade") or {}).get("last_closed_exit"),
        }
    return {
        "type": "CANONICAL_CONTINUATION",
        "cutoff": cutoff.isoformat(),
        "pre_boundary_renko_source": old_renko_source,
        "post_boundary_renko_source": CANONICAL_RENKO_SOURCE,
    }


def build_manifest(
    *,
    args: argparse.Namespace,
    state_path: Path,
    broker_snapshot_path: Path,
    loaded_config_hash: str,
    source_summary: dict[str, Any],
    staged_state: dict[str, Any],
) -> dict[str, Any]:
    state_payload = canonical_json(staged_state).encode("utf-8")
    return {
        "manifest_version": MANIFEST_VERSION,
        "account": args.account,
        "trader_id": args.trader_id,
        "symbol": args.symbol.upper(),
        "mode": args.mode,
        "cutoff": parse_utc(args.cutoff).isoformat(),
        "code": git_identity(),
        "inputs": {
            "target_config": {"path": str(Path(args.config).resolve()), "file_sha256": file_sha256(args.config), "loaded_config_hash": loaded_config_hash},
            "state": {"path": str(state_path.resolve()), "sha256": file_sha256(state_path)},
            "broker_snapshot": {"path": str(broker_snapshot_path.resolve()), "sha256": file_sha256(broker_snapshot_path)},
            "sip": source_summary,
        },
        "outputs": {
            "state_sha256": sha256(state_payload).hexdigest(),
            "state_fingerprint": stable_hash(staged_state),
        },
        "safety": {
            "offline_only": True,
            "order_submission_api_used": False,
            "live_state_paths_written": False,
            "validation_only": bool(args.validation_only),
        },
    }


def git_identity() -> dict[str, Any]:
    def run_git(*parts: str) -> str | None:
        try:
            return subprocess.check_output(["git", *parts], text=True, stderr=subprocess.DEVNULL).strip()
        except Exception:
            return None

    return {
        "head_sha": run_git("rev-parse", "HEAD"),
        "head_tree_sha": run_git("rev-parse", "HEAD^{tree}"),
        "status_short_sha256": stable_hash({"status": run_git("status", "--short") or ""}),
    }


def observation_key(observation: Any) -> dict[str, Any]:
    return {
        "timestamp": pd.Timestamp(observation.second_bucket).isoformat(),
        "price": str(observation.close_price),
        "source_record_id": observation.source_record_id,
        "source_trade_id": observation.source_broker_trade_id,
    }


def brick_key(brick: Any) -> dict[str, Any]:
    return {
        "timestamp": pd.Timestamp(brick.timestamp).isoformat(),
        "close": f"{float(brick.close):.12g}",
        "direction": int(brick.direction),
        "run_len": int(brick.run_len),
    }


def signal_key(signal: Any) -> dict[str, Any]:
    return {
        "timestamp": pd.Timestamp(signal.timestamp).isoformat(),
        "side": signal.side,
        "type": signal.signal_type,
        "reason": signal.reason,
        "price": f"{float(signal.price):.12g}",
    }


def intent_key(intent: Any) -> dict[str, Any]:
    return {
        "timestamp": pd.Timestamp(intent.timestamp).isoformat(),
        "side": intent.side,
        "type": intent.signal_type,
        "reason": intent.reason,
        "qty": None if intent.qty is None else f"{float(intent.qty):.12g}",
        "close_position": bool(intent.close_position),
    }


def abs_decimal(value: Any) -> Decimal:
    if value in {None, ""}:
        return Decimal("0")
    return abs(Decimal(str(value)))


def canonical_json(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str) + "\n"


def stable_hash(payload: Any) -> str:
    return sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, sort_keys=True, indent=2, default=str) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
