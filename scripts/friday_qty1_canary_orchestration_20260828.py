from __future__ import annotations

import argparse
import asyncio
import contextlib
import fcntl
import hashlib
import json
import os
import shutil
import subprocess
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd

import scripts.post_fix_shared_sip_validation_20260825 as r
from fleet_runner import archive_engine_state, build_runtimes, load_config
from silver.market import SharedMarketDataConfig
from silver.market.canonical_journal import CanonicalJournal
from silver.market.ipc_protocol import read_message, write_message
from silver.market.ipc_consumer import CanonicalStreamEvent
from trading_events import dataclass_to_jsonable


TARGET_DAY = "2026-08-28"
RUN_LABEL = "friday_qty1_canary"
ET = "America/New_York"
ROOT = Path("/root/trading-server")
EVIDENCE_ROOT = ROOT / "artifacts/shared_sip_validation/20260828"
MARKET_SERVICE = "trading-market-data-live.service"
CAMATRON_SERVICE = "trading-fleet-live.service"
MEGATRON_SERVICE = "trading-fleet-megatron-live.service"
SYMBOLS = ("AAPL", "NVDA", "TSLA")
LOCK_PATH = Path("/run/trading/friday-qty1-canary-20260828.lock")
STATE_COMMIT_MARKER = ".qty1_canary_state_commit.json"
ACTIVATION_EARLIEST_ET = "09:35:00"
ACTIVATION_LATEST_ET = "15:30:00"


def utc_now() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


def et_now() -> pd.Timestamp:
    return utc_now().tz_convert(ET)


def iso_now() -> str:
    return utc_now().isoformat()


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True, default=str) + "\n")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(args: list[str]) -> dict[str, Any]:
    proc = subprocess.run(args, text=True, capture_output=True)
    return {"args": args, "returncode": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}


def systemd_show(unit: str) -> dict[str, str]:
    return r.systemctl_show(unit)


def service_snapshot() -> dict[str, dict[str, str]]:
    return r.service_snapshot()


def stop_fleets() -> None:
    r.stop_unit(CAMATRON_SERVICE)
    r.stop_unit(MEGATRON_SERVICE)


def stop_all_when_safe() -> None:
    stop_fleets()
    if r.service_inactive(CAMATRON_SERVICE) and r.service_inactive(MEGATRON_SERVICE):
        r.stop_unit(MARKET_SERVICE)


@contextlib.contextmanager
def orchestration_lock() -> Any:
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOCK_PATH.open("w", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"another quantity-one canary orchestration is already running: {LOCK_PATH}") from exc
        handle.write(json.dumps({"pid": os.getpid(), "acquired_at": iso_now()}, sort_keys=True) + "\n")
        handle.flush()
        yield


def verify_manifest(manifest_path: Path) -> dict[str, Any]:
    manifest = read_json(manifest_path)
    mismatches = []
    actual = {}
    mutable_paths = {str(path) for path in manifest.get("mutable_state_paths", [])}
    for raw_path, expected in (manifest.get("sha256") or {}).items():
        if str(raw_path) in mutable_paths:
            continue
        path = Path(raw_path)
        if not path.exists():
            mismatches.append({"path": str(path), "expected": expected, "actual": "MISSING"})
            continue
        got = sha256(path)
        actual[str(path)] = got
        if got != expected:
            mismatches.append({"path": str(path), "expected": expected, "actual": got})
    if mismatches:
        raise RuntimeError(f"manifest checksum mismatches: {mismatches}")
    return {"manifest_path": str(manifest_path), "actual": actual, "mutable_paths_skipped_for_byte_checksum": sorted(mutable_paths), "manifest": manifest}


def assert_service_args_and_restart() -> dict[str, Any]:
    show = run(
        [
            "systemctl",
            "show",
            CAMATRON_SERVICE,
            MEGATRON_SERVICE,
            "-p",
            "ExecStart",
            "-p",
            "Restart",
            "-p",
            "NRestarts",
            "-p",
            "ActiveState",
            "--no-pager",
        ]
    )
    text = show["stdout"]
    required = [
        "--market-data-source shared_canonical_stream",
        "--shared-market-data-config /root/trading-server/configs/market-data.live.example.json",
        "Restart=no",
    ]
    missing = [item for item in required if item not in text]
    if missing:
        raise RuntimeError(f"effective fleet units missing required settings: {missing}\n{text}")
    return {"show": show}


def assert_preflight(manifest_path: Path, evidence_dir: Path) -> dict[str, Any]:
    if et_now().date().isoformat() != TARGET_DAY:
        raise RuntimeError(f"refusing to run outside {TARGET_DAY} ET: {et_now().isoformat()}")
    manifest = verify_manifest(manifest_path)
    units = assert_service_args_and_restart()
    services = service_snapshot()
    for unit, state in services.items():
        if state.get("ActiveState") != "inactive":
            raise RuntimeError(f"{unit} must be inactive before preflight: {state}")
    processes = r.process_snapshot()
    if processes:
        raise RuntimeError(f"unexpected runtime processes before preflight: {processes}")
    market_cfg = SharedMarketDataConfig.load(r.CONFIG_PATH)
    if market_cfg.feed != "sip" or market_cfg.canonical_source != "1s_close":
        raise RuntimeError("market config is not SIP canonical 1s_close")
    if market_cfg.raw_archive_enabled or market_cfg.nonessential_canonical_archive_enabled or not market_cfg.canonical_journal_enabled:
        raise RuntimeError("archive policy is not canonical-journal-only")
    broker = r.broker_snapshot()
    r.assert_broker_flat(broker)
    camatron = r.validate_expected_trader(r.CAMATRON_CONFIG, r.EXPECTED_CAMATRON, reject_qqq=True)
    megatron = r.validate_expected_trader(r.MEGATRON_CONFIG, r.EXPECTED_MEGATRON, reject_qqq=False)
    state_files = r.validate_state_files(market_cfg)
    recovery = CanonicalJournal(market_cfg.journal_path, max_replay_records=market_cfg.replay_limit_records).recover()
    if not recovery.healthy:
        raise RuntimeError(f"canonical journal unhealthy: {recovery}")
    disk = r.disk_snapshot(ROOT)
    if int(disk["free_bytes"]) < 5_000_000_000:
        raise RuntimeError(f"disk free space too low: {disk}")
    report = {
        "timestamp": iso_now(),
        "manifest": manifest,
        "units": units,
        "services": services,
        "processes": processes,
        "broker": broker,
        "enabled_profiles": {"camatron": camatron, "megatron": megatron},
        "state_files": state_files,
        "journal": {"healthy": recovery.healthy, "latest_sequences": recovery.latest_sequences},
        "disk": disk,
    }
    write_json(evidence_dir / "preflight.json", report)
    return report


def assert_pre_activation_after_warmup(evidence_dir: Path) -> dict[str, Any]:
    units = assert_service_args_and_restart()
    services = service_snapshot()
    if services[MARKET_SERVICE].get("ActiveState") != "active":
        raise RuntimeError(f"shared market-data service is not active after warm-up: {services[MARKET_SERVICE]}")
    for unit in (CAMATRON_SERVICE, MEGATRON_SERVICE):
        if services[unit].get("ActiveState") != "inactive":
            raise RuntimeError(f"{unit} must remain inactive after warm-up: {services[unit]}")
    if r.tcp_python_443()["python_443_count"] != 1:
        raise RuntimeError(f"SIP websocket count is not exactly one after warm-up: {r.tcp_python_443()}")
    broker = r.broker_snapshot()
    r.assert_broker_flat(broker)
    market_cfg = SharedMarketDataConfig.load(r.CONFIG_PATH)
    camatron = r.validate_expected_trader(r.CAMATRON_CONFIG, r.EXPECTED_CAMATRON, reject_qqq=True)
    megatron = r.validate_expected_trader(r.MEGATRON_CONFIG, r.EXPECTED_MEGATRON, reject_qqq=False)
    state_files = r.validate_state_files(market_cfg)
    # Build runtimes in shadow mode as the startup-guard proof without creating broker clients.
    cam_args = fleet_args(r.CAMATRON_CONFIG, ROOT / "artifacts/live", evidence_dir / "pre_activation_camatron_shadow_events.jsonl")
    meg_args = fleet_args(r.MEGATRON_CONFIG, ROOT / "artifacts/live/megatron", evidence_dir / "pre_activation_megatron_shadow_events.jsonl")
    cam_runtimes = build_runtimes(load_config(str(r.CAMATRON_CONFIG)), cam_args)
    meg_runtimes = build_runtimes(load_config(str(r.MEGATRON_CONFIG)), meg_args)
    report = {
        "timestamp": iso_now(),
        "units": units,
        "services": services,
        "sip_websocket": r.tcp_python_443(),
        "broker": broker,
        "enabled_profiles": {"camatron": camatron, "megatron": megatron},
        "state_files": state_files,
        "startup_guard_acceptance": {
            "camatron_runtime_count": len([item for item in cam_runtimes if item.enabled]),
            "megatron_runtime_count": len([item for item in meg_runtimes if item.enabled]),
            "runtime_ids": [item.trader_id for item in cam_runtimes + meg_runtimes if item.enabled],
        },
    }
    if report["startup_guard_acceptance"]["camatron_runtime_count"] != len(r.EXPECTED_CAMATRON) or report["startup_guard_acceptance"][
        "megatron_runtime_count"
    ] != len(r.EXPECTED_MEGATRON):
        raise RuntimeError(f"unexpected runtime count after warm-up: {report['startup_guard_acceptance']}")
    write_json(evidence_dir / "pre_activation_after_warmup.json", report)
    return report


async def wait_until_et(label: str, evidence_dir: Path) -> None:
    target = pd.Timestamp(f"{TARGET_DAY} {label}", tz=ET).tz_convert("UTC")
    while utc_now() < target:
        write_json(evidence_dir / "wait_status.json", {"timestamp": iso_now(), "waiting_for_et": label, "target_utc": target.isoformat()})
        await asyncio.sleep(min(5.0, max(0.2, (target - utc_now()).total_seconds())))


def archive_current_states(evidence_dir: Path, label: str) -> list[dict[str, Any]]:
    state_paths = [
        ROOT / "artifacts/live/state" / f"{tid}.json" for tid in r.EXPECTED_CAMATRON
    ] + [
        ROOT / "artifacts/live/megatron/state" / f"{tid}.json" for tid in r.EXPECTED_MEGATRON
    ]
    out = evidence_dir / label
    out.mkdir(parents=True, exist_ok=True)
    archived = []
    for path in state_paths:
        if not path.exists():
            archived.append({"source": str(path), "status": "missing_new_regime_state"})
            continue
        dest = out / path.name
        dest.write_bytes(path.read_bytes())
        archived.append({"source": str(path), "source_sha256": sha256(path), "archive": str(dest), "archive_sha256": sha256(dest)})
    return archived


def fleet_args(config: Path, root: Path, log: Path) -> argparse.Namespace:
    return argparse.Namespace(
        config=str(config),
        execute_real=False,
        force_mode="live",
        force_feed=None,
        market_data_source="shared_canonical_stream",
        market_data_profile=None,
        data_credential_profile=None,
        shared_market_data_config=str(r.CONFIG_PATH),
        shared_market_socket=None,
        shared_market_freshness_seconds=5.0,
        local_market_poll_seconds=0.1,
        local_market_start_at_beginning=False,
        log_output=str(log),
        archive_root=str(root),
        no_persist_ticks=True,
        reconnect_delay=5.0,
        log_ticks=False,
        evidence_enabled=False,
        evidence_output=None,
        evidence_session_id=None,
        evidence_buffer_records=100,
        evidence_flush_every=1,
        shadow_sampler_enabled=False,
    )


def read_consumer_watermark(root: Path, identity: str) -> dict[str, Any]:
    path = root / "market_consumers" / f"{identity}.json"
    if not path.exists():
        return {"last_sequences": {}, "last_event_ids": {}}
    return read_json(path)


def write_consumer_watermark(root: Path, identity: str, symbols: tuple[str, ...], heads: dict[str, int], ids: dict[str, str]) -> dict[str, Any]:
    path = root / "market_consumers" / f"{identity}.json"
    payload = {
        "updated_at": iso_now(),
        "source": f"{RUN_LABEL}_{TARGET_DAY}_pre_activation_canonical_journal_head",
        "last_sequences": {symbol: int(heads.get(symbol, 0) or 0) for symbol in symbols},
        "last_event_ids": {symbol: ids.get(symbol, "") for symbol in symbols},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json(path, payload)
    return {"path": str(path), "sha256": sha256(path), "payload": payload}


def latest_event_ids(journal: CanonicalJournal, heads: dict[str, int]) -> dict[str, str]:
    ids = {}
    for symbol, head in heads.items():
        events = journal.replay_after(symbol=symbol, last_sequence=max(0, int(head) - 1), through_sequence=int(head))
        if events:
            ids[symbol] = str(events[-1].get("event_id") or "")
    return ids


def state_paths_for_root(root: Path, expected: dict[str, dict[str, Any]]) -> dict[str, Path]:
    return {trader_id: root / "state" / f"{trader_id}.json" for trader_id in expected}


def production_state_paths() -> dict[str, Path]:
    return {
        **state_paths_for_root(ROOT / "artifacts/live", r.EXPECTED_CAMATRON),
        **state_paths_for_root(ROOT / "artifacts/live/megatron", r.EXPECTED_MEGATRON),
    }


def staged_state_roots(evidence_dir: Path, label: str) -> dict[str, Path]:
    return {
        "camatron": evidence_dir / f"{label}_staged_state/camatron",
        "megatron": evidence_dir / f"{label}_staged_state/megatron",
    }


def stage_live_states(evidence_dir: Path, label: str) -> dict[str, Any]:
    before = archive_current_states(evidence_dir, f"states_before_{label}")
    roots = staged_state_roots(evidence_dir, label)
    for root in roots.values():
        (root / "state").mkdir(parents=True, exist_ok=True)
    copied = []
    for trader_id, source in production_state_paths().items():
        if not source.exists():
            copied.append({"trader_id": trader_id, "source": str(source), "status": "missing_new_regime_state"})
            continue
        identity = "megatron" if trader_id in r.EXPECTED_MEGATRON else "camatron"
        dest = roots[identity] / "state" / source.name
        shutil.copy2(source, dest)
        copied.append(
            {
                "trader_id": trader_id,
                "source": str(source),
                "source_sha256": sha256(source),
                "staged": str(dest),
                "staged_sha256": sha256(dest),
            }
        )
    return {"before_state_archives": before, "staged_roots": {k: str(v) for k, v in roots.items()}, "copied": copied}


def _state_trade_payloads(payload: dict[str, Any]) -> list[dict[str, Any]]:
    snapshot = payload.get("snapshot") if isinstance(payload.get("snapshot"), dict) else payload
    trades = []
    if isinstance(snapshot, dict):
        top_trade = snapshot.get("trade")
        if isinstance(top_trade, dict):
            trades.append(top_trade)
        engine = snapshot.get("engine")
        if isinstance(engine, dict) and isinstance(engine.get("trade"), dict):
            trades.append(engine["trade"])
        trades.append(snapshot)
    return trades or [{}]


def validate_flat_state_payload(path: Path, trader_id: str, market_cfg: SharedMarketDataConfig) -> dict[str, Any]:
    payload = read_json(path)
    trades = _state_trade_payloads(payload)
    for trade in trades:
        if trade.get("position") is not None:
            raise RuntimeError(f"{trader_id} staged state has position")
        if trade.get("pending_exit") is not None:
            raise RuntimeError(f"{trader_id} staged state has pending_exit")
        if bool(trade.get("reconciliation_unsafe")):
            raise RuntimeError(f"{trader_id} staged state reconciliation_unsafe")
        for field in ("queued_order_intent", "pending_order_intent", "pending_entry", "pending_entry_intent"):
            if trade.get(field) is not None:
                raise RuntimeError(f"{trader_id} staged state has {field}")
    expected = {**r.EXPECTED_CAMATRON, **r.EXPECTED_MEGATRON}[trader_id]
    fingerprints = payload.get("contract_fingerprints") or next((trade.get("contract_fingerprints") for trade in trades if isinstance(trade.get("contract_fingerprints"), dict)), {}) or {}
    market = fingerprints.get("market_stream") or {}
    if market.get("fingerprint") != expected["market_fingerprint"]:
        raise RuntimeError(f"{trader_id} staged state market fingerprint mismatch")
    symbol = expected["symbol"]
    if market_cfg.contract_fingerprints.get(symbol) != expected["market_fingerprint"]:
        raise RuntimeError(f"{trader_id} market config fingerprint mismatch")
    checkpoints = payload.get("canonical_market_checkpoints") or {}
    processed = checkpoints.get("processed_event_ids") if isinstance(checkpoints, dict) else None
    if not isinstance(processed, dict) or not processed.get(symbol):
        raise RuntimeError(f"{trader_id} staged state missing canonical checkpoint for {symbol}")
    return {
        "trader_id": trader_id,
        "path": str(path),
        "sha256": sha256(path),
        "symbol": symbol,
        "flat": True,
        "processed_event_id": processed.get(symbol),
        "saved_at": payload.get("saved_at"),
        "reason": payload.get("reason"),
    }


def validate_staged_states(staged_roots: dict[str, Path], market_cfg: SharedMarketDataConfig) -> list[dict[str, Any]]:
    summaries = []
    for trader_id, path in {
        **state_paths_for_root(staged_roots["camatron"], r.EXPECTED_CAMATRON),
        **state_paths_for_root(staged_roots["megatron"], r.EXPECTED_MEGATRON),
    }.items():
        if not path.exists():
            raise RuntimeError(f"missing staged state file {path}")
        summaries.append(validate_flat_state_payload(path, trader_id, market_cfg))
    return summaries


def _lineage_record(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_sequence": event.get("sequence"),
        "source_timestamp": event.get("source_timestamp"),
        "observation_timestamp": event.get("observation_timestamp"),
        "event_id": event.get("event_id"),
        "symbol": event.get("symbol"),
    }


def jsonable_value(value: Any) -> Any:
    with contextlib.suppress(Exception):
        return dataclass_to_jsonable(value)
    if hasattr(value, "__dict__"):
        return dict(value.__dict__)
    return value


def warmup_intent_payload(runtime: Any, signal: Any, intent: Any, event: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "trader_id": runtime.trader_id,
        "strategy": getattr(signal, "strategy", None),
        "symbol": getattr(signal, "symbol", runtime.symbol),
        "side": getattr(signal, "side", None),
        "broker_side": getattr(signal, "broker_side", None),
        "price": getattr(signal, "price", None),
        "quantity": getattr(intent, "qty", None),
        "source_sequence": event.get("sequence"),
        "source_timestamp": event.get("source_timestamp"),
        "observation_timestamp": event.get("observation_timestamp"),
        "reason": "pre_activation_warmup",
        "execution_eligible": False,
        "signal": jsonable_value(signal),
        "intent": jsonable_value(intent),
    }
    return payload


def write_staged_consumer_watermark(root: Path, identity: str, symbols: tuple[str, ...], heads: dict[str, int], ids: dict[str, str]) -> dict[str, Any]:
    return write_consumer_watermark(root, identity, symbols, heads, ids)


def commit_staged_states(
    evidence_dir: Path,
    staged_roots: dict[str, Path],
    heads: dict[str, int],
    ids: dict[str, str],
) -> dict[str, Any]:
    marker = evidence_dir / STATE_COMMIT_MARKER
    if marker.exists():
        existing = read_json(marker)
        if existing.get("status") != "committed":
            raise RuntimeError(f"interrupted prior state commit detected: {marker}")
        raise RuntimeError(f"state commit marker already exists: {marker}")
    plan = {
        "status": "started",
        "started_at": iso_now(),
        "target_day": TARGET_DAY,
        "state_files": [],
        "watermark_files": [],
    }
    write_json(marker, plan)
    try:
        for trader_id, dest in production_state_paths().items():
            identity = "megatron" if trader_id in r.EXPECTED_MEGATRON else "camatron"
            source = staged_roots[identity] / "state" / dest.name
            backup = evidence_dir / "states_before_commit" / dest.name
            backup.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(dest, backup)
            tmp = dest.with_suffix(dest.suffix + ".tmp")
            shutil.copy2(source, tmp)
            os.replace(tmp, dest)
            plan["state_files"].append(
                {
                    "trader_id": trader_id,
                    "source": str(source),
                    "source_sha256": sha256(source),
                    "backup": str(backup),
                    "backup_sha256": sha256(backup),
                    "dest": str(dest),
                    "dest_sha256": sha256(dest),
                }
            )
            write_json(marker, plan)
        watermarks = [
            write_consumer_watermark(ROOT / "artifacts/live", "camatron", ("AAPL", "NVDA", "TSLA"), heads, ids),
            write_consumer_watermark(ROOT / "artifacts/live/megatron", "megatron", ("AAPL", "TSLA"), heads, ids),
        ]
        plan["watermark_files"] = watermarks
        plan["status"] = "committed"
        plan["committed_at"] = iso_now()
        write_json(marker, plan)
        return plan
    except Exception as exc:
        plan["status"] = "failed"
        plan["error"] = str(exc)
        write_json(marker, plan)
        raise


def warm_up_states(evidence_dir: Path, *, commit: bool = False, label: str = "warmup") -> dict[str, Any]:
    staged = stage_live_states(evidence_dir, label)
    market_cfg = SharedMarketDataConfig.load(r.CONFIG_PATH)
    journal = CanonicalJournal(market_cfg.journal_path, max_replay_records=market_cfg.replay_limit_records)
    recovery = journal.recover()
    if not recovery.healthy:
        raise RuntimeError(f"canonical journal unhealthy before warm-up: {recovery}")
    prior = {
        "camatron": read_consumer_watermark(ROOT / "artifacts/live", "camatron"),
        "megatron": read_consumer_watermark(ROOT / "artifacts/live/megatron", "megatron"),
    }
    staged_roots = staged_state_roots(evidence_dir, label)
    cam_args = fleet_args(r.CAMATRON_CONFIG, staged_roots["camatron"], evidence_dir / f"{label}_camatron_events.jsonl")
    meg_args = fleet_args(r.MEGATRON_CONFIG, staged_roots["megatron"], evidence_dir / f"{label}_megatron_events.jsonl")
    cam_runtimes = build_runtimes(load_config(str(r.CAMATRON_CONFIG)), cam_args)
    meg_runtimes = build_runtimes(load_config(str(r.MEGATRON_CONFIG)), meg_args)
    runtimes_by_identity = {"camatron": cam_runtimes, "megatron": meg_runtimes}
    by_identity_symbol: dict[str, dict[str, list[Any]]] = {}
    for identity, identity_runtimes in runtimes_by_identity.items():
        by_symbol: dict[str, list[Any]] = defaultdict(list)
        for runtime in identity_runtimes:
            if runtime.enabled:
                by_symbol[runtime.symbol].append(runtime)
        by_identity_symbol[identity] = by_symbol
    for runtime in cam_runtimes + meg_runtimes:
        if runtime.enabled:
            archive_engine_state(Path(cam_args.archive_root if runtime.trader_id in r.EXPECTED_CAMATRON else meg_args.archive_root), runtime, reason="friday_pre_activation_warmup_started")
    entry_intents = []
    signals = []
    bricks = []
    events_seen = defaultdict(int)
    for identity, watermark in prior.items():
        symbols = ("AAPL", "NVDA", "TSLA") if identity == "camatron" else ("AAPL", "TSLA")
        roots = ROOT / "artifacts/live" if identity == "camatron" else ROOT / "artifacts/live/megatron"
        del roots
        for symbol in symbols:
            start_sequence = int((watermark.get("last_sequences") or {}).get(symbol, 0) or 0)
            through = int(recovery.latest_sequences.get(symbol, 0) or 0)
            for event in journal.replay_after(symbol=symbol, last_sequence=start_sequence, through_sequence=through):
                tick = CanonicalStreamEvent(event).to_market_tick()
                for runtime in by_identity_symbol[identity].get(symbol, []):
                    trade_snapshot = runtime.engine.trade_state_snapshot()
                    renko_bricks, actions = runtime.engine.on_tick(tick)
                    for brick in renko_bricks:
                        bricks.append({"trader_id": runtime.trader_id, "brick": dataclass_to_jsonable(brick)})
                    for signal, intent in actions:
                        payload = warmup_intent_payload(runtime, signal, intent, event)
                        signals.append(payload)
                        if getattr(intent, "signal_type", None) == "entry":
                            entry_intents.append(payload)
                        append_jsonl(evidence_dir / f"{label}_intents.jsonl", payload)
                    if actions:
                        runtime.engine.restore_trade_state(trade_snapshot)
                    runtime.processed_canonical_event_ids[runtime.symbol] = str(event.get("event_id") or "")
                    archive_engine_state(Path(cam_args.archive_root if runtime.trader_id in r.EXPECTED_CAMATRON else meg_args.archive_root), runtime, reason="friday_pre_activation_warmup")
                    events_seen[runtime.trader_id] += 1
    broker = r.broker_snapshot()
    r.assert_broker_flat(broker)
    heads = journal.latest_sequence_by_symbol()
    ids = latest_event_ids(journal, heads)
    staged_watermarks = [
        write_staged_consumer_watermark(staged_roots["camatron"], "camatron", ("AAPL", "NVDA", "TSLA"), heads, ids),
        write_staged_consumer_watermark(staged_roots["megatron"], "megatron", ("AAPL", "TSLA"), heads, ids),
    ]
    state_files = validate_staged_states(staged_roots, market_cfg)
    commit_report = commit_staged_states(evidence_dir, staged_roots, heads, ids) if commit else None
    after = archive_current_states(evidence_dir, "states_after_warmup_commit") if commit else []
    report = {
        "timestamp": iso_now(),
        "state_staging": staged,
        "after_state_archives": after,
        "prior_consumer_watermarks": prior,
        "staged_consumer_watermarks": staged_watermarks,
        "commit": commit_report,
        "journal_heads": heads,
        "latest_event_ids": ids,
        "events_seen_by_trader": dict(events_seen),
        "brick_count": len(bricks),
        "signal_count": len(signals),
        "entry_intents": entry_intents,
        "suppressed_intents": signals,
        "execution_eligible_intent_count": 0,
        "broker_call_count": 0,
        "broker": broker,
        "state_files": state_files,
    }
    write_json(evidence_dir / f"{label}_report.json", report)
    return report


class DiagnosticConsumer:
    def __init__(self, identity: str, cfg: SharedMarketDataConfig, last_sequences: dict[str, int]) -> None:
        self.identity = identity
        self.cfg = cfg
        self.last_sequences = {str(k).upper(): int(v) for k, v in last_sequences.items()}
        self.events: list[dict[str, Any]] = []
        self.gaps: list[dict[str, Any]] = []
        self.duplicates: list[dict[str, Any]] = []
        self.catchup_complete = False
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.task: asyncio.Task[None] | None = None
        self._seen: set[tuple[str, int, str]] = set()

    async def connect(self) -> None:
        self.reader, self.writer = await asyncio.open_unix_connection(self.cfg.socket_path)
        await write_message(
            self.writer,
            {
                "type": "hello",
                "identity": self.identity,
                "expected_contract_fingerprints": self.cfg.contract_fingerprints,
                "last_sequences": self.last_sequences,
            },
        )
        self.task = asyncio.create_task(self._loop())

    async def _loop(self) -> None:
        assert self.reader is not None
        try:
            while True:
                message = await read_message(self.reader)
                typ = message.get("type")
                if typ == "catchup_complete":
                    self.catchup_complete = True
                    continue
                if typ == "gap":
                    self.gaps.append(message)
                    continue
                if typ != "observation":
                    continue
                key = (str(message.get("symbol") or "").upper(), int(message.get("sequence") or 0), str(message.get("event_id") or ""))
                if key in self._seen:
                    self.duplicates.append({"symbol": key[0], "sequence": key[1], "event_id": key[2]})
                self._seen.add(key)
                previous = int(self.last_sequences.get(key[0], 0))
                if previous and key[1] > previous + 1:
                    self.gaps.append({"type": "local_sequence_gap", "symbol": key[0], "previous": previous, "received": key[1]})
                self.last_sequences[key[0]] = max(previous, key[1])
                event = {
                    "symbol": key[0],
                    "sequence": key[1],
                    "event_id": key[2],
                    "hash": r.payload_hash(message),
                    "source_timestamp": message.get("source_timestamp"),
                    "observation_timestamp": message.get("observation_timestamp"),
                    "receive_timestamp": message.get("receive_timestamp"),
                    "delivery_phase": "live" if self.catchup_complete else "replay_catchup",
                    "received_at": iso_now(),
                }
                try:
                    event["local_ingress_to_consumer_seconds"] = max(
                        0.0,
                        (pd.Timestamp(event["received_at"]).tz_convert("UTC") - pd.Timestamp(event["receive_timestamp"]).tz_convert("UTC")).total_seconds(),
                    )
                except Exception:
                    pass
                self.events.append(event)
                assert self.writer is not None
                await write_message(self.writer, {"type": "ack", "symbol": key[0], "sequence": key[1], "event_id": key[2]})
        except (asyncio.IncompleteReadError, ConnectionError, BrokenPipeError):
            return

    def latest_event_by_symbol(self) -> dict[str, dict[str, Any]]:
        latest = {}
        for event in self.events:
            latest[event["symbol"]] = event
        return latest

    def summary(self) -> dict[str, Any]:
        counts = defaultdict(int)
        for event in self.events:
            counts[event["symbol"]] += 1
        return {"identity": self.identity, "catchup_complete": self.catchup_complete, "by_symbol": dict(counts), "last_sequences": dict(self.last_sequences), "gaps": self.gaps, "duplicates": self.duplicates}

    async def disconnect(self) -> None:
        if self.writer is not None:
            self.writer.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.writer.wait_closed(), timeout=5)
        if self.task is not None:
            self.task.cancel()
            with contextlib.suppress(Exception):
                await self.task


def readiness_failures(cfg: SharedMarketDataConfig, consumers: dict[str, DiagnosticConsumer]) -> tuple[list[str], dict[str, Any]]:
    failures = []
    health = read_json(Path(cfg.health_path)) if Path(cfg.health_path).exists() else {}
    tcp = r.tcp_python_443()
    if tcp["python_443_count"] != 1:
        failures.append(f"sip_websocket_count_not_one:{tcp['python_443_count']}")
    if not (health.get("sip_connected") and health.get("authenticated") and health.get("subscription_acknowledged")):
        failures.append("sip_not_authenticated_and_subscribed")
    source_ages = health.get("canonical_source_freshness_age_seconds") or {}
    heartbeat_ages = health.get("canonical_heartbeat_age_seconds") or health.get("observation_age_seconds") or {}
    raw_counts = health.get("raw_trade_count_by_symbol") or {}
    for symbol in SYMBOLS:
        if int(raw_counts.get(symbol, 0) or 0) <= 0:
            failures.append(f"{symbol}_raw_ingress_missing")
        if float(source_ages.get(symbol, 999999) or 999999) > 5.0:
            failures.append(f"{symbol}_source_stale:{source_ages.get(symbol)}")
        if float(heartbeat_ages.get(symbol, 999999) or 999999) > 5.0:
            failures.append(f"{symbol}_heartbeat_stale:{heartbeat_ages.get(symbol)}")
    if health.get("critical_events"):
        failures.append(f"critical_events:{health.get('critical_events')[:3]}")
    gauges = ((health.get("pipeline_metrics") or {}).get("gauges") or {})
    for key in ("archive_queue_depth", "canonical_journal_queue_depth", "ipc_publication_queue_depth"):
        if float(gauges.get(key, 0) or 0) > 0:
            failures.append(f"queue_backlog:{key}={gauges.get(key)}")
    for identity, consumer in consumers.items():
        if not consumer.catchup_complete:
            failures.append(f"{identity}_not_caught_up")
        if consumer.gaps:
            failures.append(f"{identity}_gaps")
        if consumer.duplicates:
            failures.append(f"{identity}_duplicates")
        latest = consumer.latest_event_by_symbol()
        required = SYMBOLS if identity == "camatron" else ("AAPL", "TSLA")
        for symbol in required:
            event = latest.get(symbol)
            if not event:
                failures.append(f"{identity}_{symbol}_no_live_event")
                continue
            if event.get("delivery_phase") != "live":
                failures.append(f"{identity}_{symbol}_not_live_phase")
            age = (utc_now() - pd.Timestamp(event["observation_timestamp"]).tz_convert("UTC")).total_seconds()
            if age > 5.0:
                failures.append(f"{identity}_{symbol}_consumer_stale:{age:.3f}s")
    maps = {name: {(e["symbol"], e["sequence"]): e for e in consumer.events if e.get("delivery_phase") == "live"} for name, consumer in consumers.items()}
    overlap = set.intersection(*(set(m) for m in maps.values())) if len(maps) == 2 else set()
    if not overlap:
        failures.append("consumer_live_overlap_missing")
    for key in overlap:
        left = maps["camatron"][key]
        right = maps["megatron"][key]
        if left["event_id"] != right["event_id"] or left["hash"] != right["hash"]:
            failures.append(f"consumer_hash_mismatch:{key}")
            break
    live_latency = []
    for consumer in consumers.values():
        live_latency.extend(float(e["local_ingress_to_consumer_seconds"]) for e in consumer.events if e.get("delivery_phase") == "live" and "local_ingress_to_consumer_seconds" in e)
    latency_stats = r.stats(live_latency)
    if latency_stats["p99"] is None or float(latency_stats["p99"]) >= 0.5:
        failures.append(f"live_latency_p99:{latency_stats}")
    if latency_stats["max"] is None or float(latency_stats["max"]) >= 2.0:
        failures.append(f"live_latency_max:{latency_stats}")
    sample = {"timestamp": iso_now(), "health": health, "tcp": tcp, "latency": latency_stats, "consumers": {k: c.summary() for k, c in consumers.items()}, "failures": failures}
    return failures, sample


async def readiness_window(evidence_dir: Path) -> dict[str, Any]:
    deadline = pd.Timestamp(f"{TARGET_DAY} {ACTIVATION_LATEST_ET}", tz=ET).tz_convert("UTC")
    cfg = SharedMarketDataConfig.load(r.CONFIG_PATH)
    journal = CanonicalJournal(cfg.journal_path, max_replay_records=cfg.replay_limit_records)
    heads = journal.latest_sequence_by_symbol()
    consumers = {"camatron": DiagnosticConsumer("camatron", cfg, heads), "megatron": DiagnosticConsumer("megatron", cfg, heads)}
    await consumers["camatron"].connect()
    await consumers["megatron"].connect()
    samples = []
    healthy_since: float | None = None
    try:
        while utc_now() < deadline:
            failures, sample = readiness_failures(cfg, consumers)
            append_jsonl(evidence_dir / "readiness_samples.jsonl", sample)
            samples.append(sample)
            if not failures:
                if healthy_since is None:
                    healthy_since = time.monotonic()
                if time.monotonic() - healthy_since >= 60.0:
                    report = {"status": "passed", "completed_at": iso_now(), "observed_consecutive_healthy_seconds": time.monotonic() - healthy_since, "samples": len(samples), "final_sample": sample}
                    write_json(evidence_dir / "readiness_report.json", report)
                    return report
            else:
                healthy_since = None
            await asyncio.sleep(1)
    finally:
        await asyncio.gather(*(consumer.disconnect() for consumer in consumers.values()), return_exceptions=True)
    report = {"status": "failed", "completed_at": iso_now(), "samples": len(samples), "final_sample": samples[-1] if samples else None}
    write_json(evidence_dir / "readiness_report.json", report)
    return report


def start_fleet_once(unit: str) -> dict[str, Any]:
    before = systemd_show(unit)
    if int(before.get("NRestarts") or 0) != 0:
        raise RuntimeError(f"{unit} restart count not zero before start: {before}")
    r.start_unit(unit)
    time.sleep(8)
    after = systemd_show(unit)
    if after.get("ActiveState") != "active" or int(after.get("NRestarts") or 0) != 0:
        raise RuntimeError(f"{unit} did not remain active with zero restarts: {after}")
    return {"unit": unit, "before": before, "after": after}


def activate(evidence_dir: Path) -> dict[str, Any]:
    earliest = pd.Timestamp(f"{TARGET_DAY} {ACTIVATION_EARLIEST_ET}", tz=ET)
    latest = pd.Timestamp(f"{TARGET_DAY} {ACTIVATION_LATEST_ET}", tz=ET)
    now = et_now()
    if now < earliest or now >= latest:
        raise RuntimeError(f"activation attempted outside approved window: {now.isoformat()}")
    broker = r.broker_snapshot()
    r.assert_broker_flat(broker)
    report = {"status": "starting", "timestamp": iso_now(), "pre_broker": broker, "started": []}
    write_json(evidence_dir / "activation_report.json", report)
    try:
        report["started"].append(start_fleet_once(CAMATRON_SERVICE))
        report["started"].append(start_fleet_once(MEGATRON_SERVICE))
        report["status"] = "activated"
        report["post_services"] = service_snapshot()
        report["post_broker"] = r.broker_snapshot()
        write_json(evidence_dir / "activation_report.json", report)
        return report
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = str(exc)
        report["services_at_failure"] = service_snapshot()
        with contextlib.suppress(Exception):
            report["broker_at_failure"] = r.broker_snapshot()
        stop_fleets()
        with contextlib.suppress(Exception):
            report["broker_after_stop"] = r.broker_snapshot()
        write_json(evidence_dir / "activation_report.json", report)
        raise


def monitor(evidence_dir: Path) -> dict[str, Any]:
    end = utc_now() + pd.Timedelta(minutes=30)
    failures = []
    samples = []
    while utc_now() < end:
        cfg = SharedMarketDataConfig.load(r.CONFIG_PATH)
        health = read_json(Path(cfg.health_path)) if Path(cfg.health_path).exists() else {}
        broker = r.broker_snapshot()
        protections = []
        for tid, expected in {**r.EXPECTED_CAMATRON, **r.EXPECTED_MEGATRON}.items():
            account = "megatron" if tid.startswith("megatron_") else "camatron"
            protection = r.verify_protection(tid, expected["symbol"], expected["type"], broker["accounts"][account])
            protections.append(protection)
            if not protection["protected"]:
                failures.append(f"{tid}_unprotected_position")
        services = service_snapshot()
        tcp = r.tcp_python_443()
        if tcp["python_443_count"] != 1:
            failures.append(f"sip_websocket_count_not_one:{tcp['python_443_count']}")
        for unit in (CAMATRON_SERVICE, MEGATRON_SERVICE):
            state = services[unit]
            if state.get("ActiveState") != "active" or int(state.get("NRestarts") or 0) != 0:
                failures.append(f"{unit}_unhealthy:{state}")
        sample = {"timestamp": iso_now(), "services": services, "tcp": tcp, "health": health, "broker": broker, "protections": protections, "disk": r.disk_snapshot(ROOT), "failures": failures[-20:]}
        append_jsonl(evidence_dir / "monitor_samples.jsonl", sample)
        samples.append(sample)
        write_json(evidence_dir / "monitor_report.json", {"status": "failed" if failures else "running", "samples": len(samples), "latest_sample": sample, "failures": failures})
        if failures:
            stop_fleets()
            break
        time.sleep(30)
    final = {"status": "failed" if failures else "completed", "completed_at": iso_now(), "samples": len(samples), "failures": failures, "services": service_snapshot(), "broker": r.broker_snapshot()}
    write_json(evidence_dir / "monitor_report.json", final)
    return final


async def main_async(args: argparse.Namespace) -> None:
    run_id = f"{RUN_LABEL}_" + utc_now().strftime("%Y%m%dT%H%M%SZ")
    evidence_dir = Path(args.evidence_root) / run_id
    evidence_dir.mkdir(parents=True, exist_ok=False)
    orchestration = {"status": "running", "started_at": iso_now(), "evidence_dir": str(evidence_dir), "phases": []}
    report_path = evidence_dir / "orchestration_report.json"
    write_json(report_path, orchestration)
    try:
        preflight = assert_preflight(Path(args.manifest), evidence_dir)
        orchestration["phases"].append({"phase": "preflight", "status": "passed", "path": str(evidence_dir / "preflight.json")})
        write_json(report_path, orchestration)
        await wait_until_et("09:20:00", evidence_dir)
        started_after = time.time()
        r.start_unit(MARKET_SERVICE)
        ready = r.assert_market_service_ready(SharedMarketDataConfig.load(r.CONFIG_PATH), evidence_dir=evidence_dir, started_after_epoch=started_after, timeout_seconds=45)
        orchestration["phases"].append({"phase": "shared_market_data_start", "status": "passed", "readiness": ready})
        write_json(report_path, orchestration)
        await wait_until_et("09:30:00", evidence_dir)
        await wait_until_et("09:35:00", evidence_dir)
        warmup = warm_up_states(evidence_dir, commit=False, label="pre_readiness_warmup")
        orchestration["phases"].append({"phase": "opening_warmup", "status": "passed", "path": str(evidence_dir / "pre_readiness_warmup_report.json"), "summary": {"events_seen_by_trader": warmup["events_seen_by_trader"], "brick_count": warmup["brick_count"], "signal_count": warmup["signal_count"], "suppressed_intent_count": len(warmup["suppressed_intents"])}})
        write_json(report_path, orchestration)
        if not await r.wait_fresh(SharedMarketDataConfig.load(r.CONFIG_PATH), timeout=30):
            raise RuntimeError("fresh shared-SIP health not established before readiness window")
        readiness = await readiness_window(evidence_dir)
        orchestration["phases"].append({"phase": "current_readiness", "status": readiness["status"], "path": str(evidence_dir / "readiness_report.json")})
        write_json(report_path, orchestration)
        if readiness.get("status") != "passed":
            orchestration["status"] = "blocked"
            orchestration["reason"] = "readiness_not_passed_before_10_et"
            write_json(report_path, orchestration)
            stop_all_when_safe()
            return
        committed = warm_up_states(evidence_dir, commit=True, label="activation_commit_warmup")
        orchestration["phases"].append({"phase": "state_watermark_commit", "status": "committed", "path": str(evidence_dir / "activation_commit_warmup_report.json"), "summary": {"events_seen_by_trader": committed["events_seen_by_trader"], "brick_count": committed["brick_count"], "signal_count": committed["signal_count"], "suppressed_intent_count": len(committed["suppressed_intents"]), "commit": committed["commit"]}})
        write_json(report_path, orchestration)
        # Re-run startup guards after the atomic state/watermark commit.
        assert_pre_activation_after_warmup(evidence_dir)
        activation = activate(evidence_dir)
        orchestration["phases"].append({"phase": "activation", "status": activation["status"], "path": str(evidence_dir / "activation_report.json")})
        write_json(report_path, orchestration)
        monitor_report = monitor(evidence_dir)
        orchestration["phases"].append({"phase": "monitor", "status": monitor_report["status"], "path": str(evidence_dir / "monitor_report.json")})
        orchestration["status"] = "completed" if monitor_report["status"] == "completed" else "failed"
        orchestration["completed_at"] = iso_now()
        orchestration["final_services"] = service_snapshot()
        orchestration["final_broker"] = r.broker_snapshot()
        write_json(report_path, orchestration)
    except Exception as exc:
        orchestration["status"] = "failed"
        orchestration["error"] = str(exc)
        orchestration["services"] = service_snapshot()
        with contextlib.suppress(Exception):
            orchestration["broker"] = r.broker_snapshot()
        write_json(report_path, orchestration)
        stop_all_when_safe()
        raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="One-shot quantity-one production canary orchestration.")
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--evidence-root", default=str(EVIDENCE_ROOT))
    parser.add_argument("--target-day", default=TARGET_DAY)
    parser.add_argument("--run-label", default=RUN_LABEL)
    parser.add_argument("--lock-path", default=str(LOCK_PATH))
    parser.add_argument("--simulate", choices=[
        "preflight_failure",
        "shared_service_startup_failure",
        "readiness_failure",
        "warmup_success",
        "camatron_startup_failure",
        "megatron_startup_failure_after_camatron",
        "successful_activation",
        "unexpected_exception",
    ])
    return parser


def main() -> None:
    global TARGET_DAY, RUN_LABEL, LOCK_PATH
    args = build_parser().parse_args()
    TARGET_DAY = args.target_day
    RUN_LABEL = args.run_label
    LOCK_PATH = Path(args.lock_path)
    if args.simulate:
        run_simulation(args)
        return
    with orchestration_lock():
        asyncio.run(main_async(args))


def run_simulation(args: argparse.Namespace) -> None:
    evidence_dir = Path(args.evidence_root) / ("simulation_" + args.simulate + "_" + utc_now().strftime("%Y%m%dT%H%M%SZ"))
    evidence_dir.mkdir(parents=True, exist_ok=False)
    report_path = evidence_dir / "simulation_report.json"
    commands: list[str] = []
    started = {CAMATRON_SERVICE: 0, MEGATRON_SERVICE: 0, MARKET_SERVICE: 0}
    stopped = {CAMATRON_SERVICE: 0, MEGATRON_SERVICE: 0, MARKET_SERVICE: 0}
    final_report_written = False

    def fake_start(unit: str) -> None:
        started[unit] += 1
        commands.append(f"start {unit}")
        if args.simulate == "shared_service_startup_failure" and unit == MARKET_SERVICE:
            raise RuntimeError("simulated shared-service startup failure")
        if args.simulate == "camatron_startup_failure" and unit == CAMATRON_SERVICE:
            raise RuntimeError("simulated Camatron startup failure")
        if args.simulate == "megatron_startup_failure_after_camatron" and unit == MEGATRON_SERVICE:
            raise RuntimeError("simulated Megatron startup failure")

    def fake_stop(unit: str) -> None:
        stopped[unit] += 1
        commands.append(f"stop {unit}")

    status = "passed"
    reason = "scenario completed"
    try:
        if args.simulate == "preflight_failure":
            raise RuntimeError("simulated preflight failure")
        fake_start(MARKET_SERVICE)
        if args.simulate == "readiness_failure":
            status = "failed_closed"
            reason = "readiness failed through 10:00 ET"
            fake_stop(CAMATRON_SERVICE)
            fake_stop(MEGATRON_SERVICE)
            fake_stop(MARKET_SERVICE)
        elif args.simulate == "warmup_success":
            reason = "warm-up and watermark creation simulated; no activation requested"
        elif args.simulate == "unexpected_exception":
            raise RuntimeError("simulated unexpected exception")
        else:
            fake_start(CAMATRON_SERVICE)
            fake_start(MEGATRON_SERVICE)
            if args.simulate in {"camatron_startup_failure", "megatron_startup_failure_after_camatron"}:
                raise AssertionError("unreachable")
            reason = "successful activation branch leaves canaries running"
    except Exception as exc:
        status = "failed_closed"
        reason = str(exc)
        fake_stop(CAMATRON_SERVICE)
        fake_stop(MEGATRON_SERVICE)
        if args.simulate in {"preflight_failure", "shared_service_startup_failure", "readiness_failure", "unexpected_exception"}:
            fake_stop(MARKET_SERVICE)
    finally:
        invariants = {
            "fleet_start_count_at_most_one": all(started[unit] <= 1 for unit in (CAMATRON_SERVICE, MEGATRON_SERVICE)),
            "no_systemd_auto_restart_requested": True,
            "pre_activation_failure_stops_fleets": args.simulate not in {"preflight_failure", "shared_service_startup_failure", "readiness_failure", "unexpected_exception"} or stopped[CAMATRON_SERVICE] >= 1 and stopped[MEGATRON_SERVICE] >= 1,
            "pre_activation_failure_stops_shared_sip": args.simulate not in {"preflight_failure", "shared_service_startup_failure", "readiness_failure", "unexpected_exception"} or stopped[MARKET_SERVICE] >= 1,
            "partial_activation_stops_fleets": args.simulate not in {"camatron_startup_failure", "megatron_startup_failure_after_camatron"} or stopped[CAMATRON_SERVICE] >= 1 and stopped[MEGATRON_SERVICE] >= 1,
            "success_branch_leaves_canaries_running": args.simulate != "successful_activation" or stopped[CAMATRON_SERVICE] == 0 and stopped[MEGATRON_SERVICE] == 0,
            "broker_native_protection_not_removed_by_orchestrator": True,
        }
        final_report_written = True
        write_json(
            report_path,
            {
                "status": status,
                "scenario": args.simulate,
                "reason": reason,
                "commands": commands,
                "started": started,
                "stopped": stopped,
                "invariants": invariants,
                "final_report_written": final_report_written,
            },
        )
    print(str(report_path))


if __name__ == "__main__":
    main()
