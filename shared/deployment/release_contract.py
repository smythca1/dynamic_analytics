"""Canonical, configuration-driven deployment release contract."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable


CONTRACT_VERSION = "renko.release_contract.v1"


def _hash_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _enabled_traders(config: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        trader for trader in config.get("traders", [])
        if isinstance(trader, dict) and trader.get("enabled", False)
    ]


def _symbols(config: dict[str, Any]) -> set[str]:
    return {str(trader.get("symbol", "")).upper() for trader in _enabled_traders(config) if trader.get("symbol")}


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
