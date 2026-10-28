"""Canonical, configuration-driven deployment release contract."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Iterable


CONTRACT_VERSION = "renko.release_contract.v1"
HISTORICAL_CLASSIFICATIONS = {
    "verified_historical",
    "legacy_authorized_non_restorable",
}
HISTORICAL_RESTORATION_POLICY = "historical_non_restorable"


def _hash_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _enabled_traders(config: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        trader for trader in config.get("traders", [])
        if isinstance(trader, dict) and trader.get("enabled", False)
    ]


def _symbols(config: dict[str, Any]) -> set[str]:
    return {str(trader.get("symbol", "")).upper() for trader in _enabled_traders(config) if trader.get("symbol")}


def validate_historical_checkpoint_authorizations(
    authorizations: Iterable[dict[str, Any]],
    checkpoint_inventory: Iterable[dict[str, Any]],
    *,
    active_trader_ids: Iterable[str] = (),
) -> dict[str, Any]:
    """Validate explicit, non-restorable authorization records against inventory."""
    errors: list[str] = []
    validated: list[dict[str, Any]] = []
    by_path = {str(row.get("path")): row for row in checkpoint_inventory if isinstance(row, dict)}
    inventory_supplied = bool(by_path)
    by_identity: dict[str, list[dict[str, Any]]] = {}
    for row in checkpoint_inventory:
        if isinstance(row, dict) and row.get("trader_id"):
            by_identity.setdefault(str(row["trader_id"]), []).append(row)
    seen_paths: set[str] = set()
    seen_ids: set[str] = set()
    active = {str(value) for value in active_trader_ids}
    for record in authorizations:
        if not isinstance(record, dict):
            errors.append("historical checkpoint authorization must be an object")
            continue
        path = str(record.get("checkpoint_path", ""))
        trader_id = str(record.get("trader_id", ""))
        classification = str(record.get("classification", ""))
        if not path or Path(path).is_absolute() or ".." in Path(path).parts:
            errors.append(f"{trader_id or path}: invalid checkpoint path")
            continue
        if not trader_id or not record.get("sha256") or not record.get("symbol") or not record.get("strategy_type") or not record.get("strategy_identity"):
            errors.append(f"{trader_id or path}: incomplete historical authorization")
            continue
        if not re.fullmatch(r"[0-9a-fA-F]{64}", str(record.get("sha256"))):
            errors.append(f"{trader_id}: sha256 must be a 64-character hexadecimal digest")
        if classification not in HISTORICAL_CLASSIFICATIONS:
            errors.append(f"{trader_id}: unsupported historical classification")
        if record.get("restoration") != HISTORICAL_RESTORATION_POLICY:
            errors.append(f"{trader_id}: historical restoration policy is not non-restorable")
        if not record.get("reviewed_by") or not record.get("reviewed_at_utc"):
            errors.append(f"{trader_id}: reviewer identity and timestamp are required")
        broker_evidence = record.get("broker_evidence")
        if not isinstance(broker_evidence, dict) or broker_evidence.get("position_flat") is not True or broker_evidence.get("open_orders") != 0 or not broker_evidence.get("verified_at_utc"):
            errors.append(f"{trader_id}: fresh broker-flat and zero-order evidence is required")
        if classification == "verified_historical" and not record.get("provenance"):
            errors.append(f"{trader_id}: verified historical authorization requires provenance evidence")
        if classification == "legacy_authorized_non_restorable" and not record.get("provenance_uncertainty"):
            errors.append(f"{trader_id}: legacy authorization must document provenance uncertainty")
        if path in seen_paths or trader_id in seen_ids:
            errors.append(f"{trader_id}: duplicate historical authorization")
        seen_paths.add(path)
        seen_ids.add(trader_id)
        if trader_id in active:
            errors.append(f"{trader_id}: active trader cannot have historical authorization")
        inventory = by_path.get(path)
        if not inventory_supplied:
            validated.append(record)
            continue
        if inventory is None:
            errors.append(f"{trader_id}: authorized checkpoint path is not in inventory")
            continue
        if str(inventory.get("trader_id")) != trader_id:
            errors.append(f"{trader_id}: checkpoint identity/path conflict")
        if str(inventory.get("sha256")) != str(record.get("sha256")):
            errors.append(f"{trader_id}: checkpoint hash mismatch")
        if str(inventory.get("symbol", "")).upper() != str(record.get("symbol", "")).upper():
            errors.append(f"{trader_id}: checkpoint symbol mismatch")
        if str(inventory.get("strategy_type", "")) != str(record.get("strategy_type", "")):
            errors.append(f"{trader_id}: checkpoint strategy identity mismatch")
        if str(inventory.get("strategy_identity", "")) != str(record.get("strategy_identity", "")):
            errors.append(f"{trader_id}: checkpoint strategy identity mismatch")
        validated.append(record)
    return {"status": "PASS" if not errors else "BLOCKED", "errors": errors, "validated": validated}


def build_release_contract(
    *,
    commit: str,
    config_paths: Iterable[str | Path],
    trader_config: dict[str, Any],
    market_config: dict[str, Any],
    service_dependencies: dict[str, list[str]] | None = None,
    broker_requirements: dict[str, Any] | None = None,
    checkpoint_requirements: dict[str, Any] | None = None,
    promotion_policy: str = "replace_from_release",
    retired_symbols: Iterable[str] = (),
    historical_checkpoint_authorizations: Iterable[dict[str, Any]] = (),
) -> dict[str, Any]:
    enabled = _enabled_traders(trader_config)
    symbols = sorted(_symbols(trader_config))
    market_symbols = sorted({str(symbol).upper() for symbol in market_config.get("symbols", [])})
    fingerprints = {
        str(symbol).upper(): value
        for symbol, value in (market_config.get("contract_fingerprints") or {}).items()
    }
    config_hashes = {
        str(Path(path)): _hash_file(Path(path))
        for path in config_paths
    }
    contract = {
        "contract_version": CONTRACT_VERSION,
        "git_commit": commit,
        "promotion_policy": promotion_policy,
        "configuration_hashes": config_hashes,
        "enabled_traders": [
            {
                "id": str(trader.get("id")),
                "symbol": str(trader.get("symbol", "")).upper(),
                "quantity": (trader.get("sizing") or {}).get("qty"),
                "strategy_type": (trader.get("strategy") or {}).get("type"),
            }
            for trader in enabled
        ],
        "shared_market": {
            "symbols": market_symbols,
            "contract_fingerprints": fingerprints,
        },
        "service_dependencies": service_dependencies or {},
        "broker_requirements": broker_requirements or {
            "account_identity": "required",
            "authoritative_positions_and_orders": "required",
            "reconciliation": "required",
        },
        "checkpoint_requirements": checkpoint_requirements or {
            "compatible_identity": "required",
            "stale_signal_replay": "rejected",
        },
        "retired_symbols": sorted({str(symbol).upper() for symbol in retired_symbols}),
        "historical_checkpoint_authorizations": list(historical_checkpoint_authorizations),
    }
    validate_release_contract(contract)
    return contract


def validate_release_contract(contract: dict[str, Any]) -> None:
    if contract.get("contract_version") != CONTRACT_VERSION:
        raise ValueError("unsupported release contract version")
    if not str(contract.get("git_commit", "")):
        raise ValueError("release contract requires immutable git commit")
    shared = contract.get("shared_market") or {}
    symbols = set(shared.get("symbols") or [])
    fingerprints = shared.get("contract_fingerprints") or {}
    missing = sorted(symbols.difference(fingerprints))
    if missing:
        raise ValueError(f"shared market configuration missing fingerprints: {missing}")
    enabled_symbols = {row.get("symbol") for row in contract.get("enabled_traders", [])}
    absent = sorted(symbol for symbol in enabled_symbols if symbol and symbol not in symbols)
    if absent:
        raise ValueError(f"enabled trader symbols absent from shared market configuration: {absent}")
    retired = set(contract.get("retired_symbols") or [])
    resurrected = sorted(retired.intersection(enabled_symbols))
    if resurrected:
        raise ValueError(f"release would resurrect retired symbols: {resurrected}")
    authorizations = contract.get("historical_checkpoint_authorizations") or []
    result = validate_historical_checkpoint_authorizations(authorizations, [])
    if result["status"] != "PASS":
        raise ValueError(f"invalid historical checkpoint authorizations: {result['errors']}")


def plan_change(current: dict[str, Any] | None, candidate: dict[str, Any]) -> dict[str, Any]:
    """Return generic dependency and service impact without broker mutation."""
    current = current or {}
    old = {row["id"]: row for row in current.get("enabled_traders", [])}
    new = {row["id"]: row for row in candidate.get("enabled_traders", [])}
    old_symbols = set((current.get("shared_market") or {}).get("symbols", []))
    new_symbols = set((candidate.get("shared_market") or {}).get("symbols", []))
    changed = []
    for identity in sorted(set(old) | set(new)):
        if old.get(identity) != new.get(identity):
            changed.append(identity)
    return {
        "added_traders": sorted(set(new) - set(old)),
        "removed_traders": sorted(set(old) - set(new)),
        "changed_traders": changed,
        "added_market_symbols": sorted(new_symbols - old_symbols),
        "removed_market_symbols": sorted(old_symbols - new_symbols),
        "market_contract_changed": (current.get("shared_market") or {}).get("contract_fingerprints")
        != (candidate.get("shared_market") or {}).get("contract_fingerprints"),
        "affected_services": sorted(set((candidate.get("service_dependencies") or {}).keys())),
        "broker_preflight_required": bool(changed or old_symbols != new_symbols),
        "checkpoint_preflight_required": bool(
            changed or old_symbols != new_symbols or (
                (current.get("shared_market") or {}).get("contract_fingerprints")
                != (candidate.get("shared_market") or {}).get("contract_fingerprints")
            )
        ),
    }


def evaluate_broker_snapshot(
    snapshot: dict[str, Any],
    *,
    expected_account_id: str,
    affected_symbols: Iterable[str],
    require_flat_symbols: Iterable[str] = (),
) -> dict[str, Any]:
    """Validate one authoritative, read-only broker observation fail-closed."""
    required = {str(symbol).upper() for symbol in affected_symbols}
    flat_required = {str(symbol).upper() for symbol in require_flat_symbols}
    errors: list[str] = []
    if snapshot.get("account_id") != expected_account_id:
        errors.append("broker account identity mismatch")
    if snapshot.get("api_health") != "ok":
        errors.append("broker API health is not confirmed")
    if snapshot.get("complete") is not True:
        errors.append("broker observation is incomplete")
    if snapshot.get("stale") is True:
        errors.append("broker observation is stale")
    positions = snapshot.get("positions")
    orders = snapshot.get("open_orders_nested")
    if not isinstance(positions, list) or not isinstance(orders, list):
        errors.append("broker positions or orders are not complete lists")
        positions, orders = [], []
    by_symbol = {str(row.get("symbol", "")).upper(): row for row in positions if isinstance(row, dict)}
    order_symbols = {
        str(row.get("symbol", "")).upper()
        for row in orders
        if isinstance(row, dict) and row.get("symbol")
    }
    for symbol in sorted(required):
        position = by_symbol.get(symbol)
        quantity = 0.0 if position is None else float(position.get("qty") or 0)
        if symbol in flat_required and abs(quantity) > 0:
            errors.append(f"{symbol}: broker exposure remains for retired/removed trader")
        if symbol in flat_required and symbol in order_symbols:
            errors.append(f"{symbol}: broker orders remain for retired/removed trader")
    return {
        "status": "PASS" if not errors else "BLOCKED",
        "errors": errors,
        "checked_symbols": sorted(required),
        "flat_required_symbols": sorted(flat_required),
    }


def evaluate_checkpoint_snapshot(
    checkpoints: dict[str, dict[str, Any] | None],
    *,
    candidate_traders: Iterable[dict[str, Any]],
    broker_positions: dict[str, float],
    retired_trader_ids: Iterable[str] = (),
) -> dict[str, Any]:
    """Validate persisted state identities without deleting or rewriting state."""
    errors: list[str] = []
    historical: list[dict[str, str]] = []
    retired = set(retired_trader_ids)
    expected = {str(row.get("id")): row for row in candidate_traders}
    for trader_id, payload in checkpoints.items():
        if trader_id in retired:
            historical.append({"trader_id": trader_id, "classification": "historical_non_restorable"})
            continue
        if trader_id not in expected:
            errors.append(f"{trader_id}: checkpoint identity is not in authorized release consumers")
            continue
        if payload is None:
            symbol = str(expected[trader_id].get("symbol", "")).upper()
            if abs(float(broker_positions.get(symbol, 0))) > 0:
                errors.append(f"{trader_id}: missing checkpoint with non-flat broker position")
            continue
        if not isinstance(payload, dict) or payload.get("trader_id", trader_id) != trader_id:
            errors.append(f"{trader_id}: malformed checkpoint identity")
            continue
        expected_symbol = str(expected[trader_id].get("symbol", "")).upper()
        if str(payload.get("symbol", expected_symbol)).upper() != expected_symbol:
            errors.append(f"{trader_id}: checkpoint symbol mismatch")
        expected_qty = float((expected[trader_id].get("quantity") or 0))
        checkpoint_qty = float(payload.get("quantity", expected_qty) or 0)
        if abs(checkpoint_qty - expected_qty) > 1e-9:
            errors.append(f"{trader_id}: checkpoint quantity mismatch")
        position = payload.get("position")
        state_qty = 0.0 if not isinstance(position, dict) else float(position.get("qty") or 0)
        if abs(state_qty - float(broker_positions.get(expected_symbol, 0))) > 1e-9:
            errors.append(f"{trader_id}: checkpoint and broker position mismatch")
    return {
        "status": "PASS" if not errors else "BLOCKED",
        "errors": errors,
        "historical": historical,
    }


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("contract")
    args = parser.parse_args()
    validate_release_contract(json.loads(Path(args.contract).read_text(encoding="utf-8")))
    print("release contract valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
