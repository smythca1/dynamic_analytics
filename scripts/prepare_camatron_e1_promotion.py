from __future__ import annotations

import argparse
import copy
import difflib
import json
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from shared.config.trader_config import audit_renko_brick_config, build_strategy_config, load_production_renko_config, production_metadata
from silver.market import contract_snapshot


POLICY = "RESET_RENKO_ON_BRICK_CHANGE_PRESERVE_POSITION_V1"


@dataclass(frozen=True)
class ApprovedTarget:
    symbol: str
    trader_id: str
    brick_size: float
    quantity: int
    risk_cap: int
    expected_initial_side: str = "long"
    expected_initial_quantity: int = 1


TARGETS = (
    ApprovedTarget(
        symbol="AAPL",
        trader_id="camatron_aapl_1s_renko_1p3443_e1_reversal_1sh_20260701_live",
        brick_size=2.161965,
        quantity=8,
        risk_cap=3500,
    ),
    ApprovedTarget(
        symbol="NVDA",
        trader_id="camatron_nvda_1s_renko_1p149851_e1_reversal_1sh_20260701_live",
        brick_size=0.8841015,
        quantity=11,
        risk_cap=3500,
    ),
    ApprovedTarget(
        symbol="TSLA",
        trader_id="camatron_tsla_1s_renko_2p31165_e1_reversal_1sh_20260710_live",
        brick_size=1.62183,
        quantity=7,
        risk_cap=3500,
    ),
)


class PromotionError(RuntimeError):
    pass


def git_head() -> str | None:
    try:
        result = subprocess.run(["git", "rev-parse", "HEAD"], text=True, capture_output=True, check=True)
    except Exception:
        return None
    return result.stdout.strip()


def stable_hash(payload: Any) -> str:
    return sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def pretty_json(payload: Any) -> str:
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def find_trader(data: dict[str, Any], trader_id: str) -> dict[str, Any]:
    for trader in data.get("traders", []):
        if isinstance(trader, dict) and trader.get("id") == trader_id:
            return trader
    raise PromotionError(f"missing approved trader {trader_id}")


def proposed_per_trader_hash(config_path: Path, trader: dict[str, Any]) -> str:
    strategy_config = build_strategy_config(trader)
    brick_audit = audit_renko_brick_config(trader, strategy_config=strategy_config)
    metadata = production_metadata(config_path, str(trader["id"]), trader, strategy_config, brick_audit)
    compact = {
        "trader_id": trader["id"],
        "strategy_config": asdict(strategy_config),
        "metadata": metadata,
        "brick_audit": asdict(brick_audit),
    }
    return sha256(json.dumps(compact, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def apply_approved_targets(config_path: Path, current: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    proposed = copy.deepcopy(current)
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for target in TARGETS:
        old_trader = find_trader(current, target.trader_id)
        trader = find_trader(proposed, target.trader_id)
        seen.add(target.trader_id)
        old_loaded = load_production_renko_config(config_path, target.trader_id)

        strategy = trader.setdefault("strategy", {})
        sizing = trader.setdefault("sizing", {})
        risk = trader.setdefault("risk", {})
        profile = trader.get("profile") if isinstance(trader.get("profile"), dict) else None

        old_risk_cap = risk.get("max_position_notional")
        strategy["brick_size"] = target.brick_size
        sizing["qty"] = target.quantity
        risk["max_position_notional"] = target.risk_cap
        if profile is not None:
            profile["brick_size"] = target.brick_size
            profile["live_qty"] = target.quantity

        new_config = build_strategy_config(trader)
        if new_config.strategy_type != "renko_reversal":
            raise PromotionError(f"{target.trader_id}: strategy_type changed to {new_config.strategy_type}")
        if int(new_config.entry_bricks) != 1:
            raise PromotionError(f"{target.trader_id}: entry_bricks changed to {new_config.entry_bricks}")

        rows.append(
            {
                "symbol": target.symbol,
                "trader_id": target.trader_id,
                "old_brick_size": old_loaded.strategy_config.brick_size,
                "new_brick_size": new_config.brick_size,
                "old_quantity": old_loaded.strategy_config.qty,
                "new_quantity": new_config.qty,
                "old_risk_cap": old_risk_cap,
                "new_risk_cap": target.risk_cap,
                "old_strategy_type": old_loaded.strategy_config.strategy_type,
                "new_strategy_type": new_config.strategy_type,
                "old_entry_bricks": old_loaded.strategy_config.entry_bricks,
                "new_entry_bricks": new_config.entry_bricks,
                "old_per_trader_hash": old_loaded.config_hash,
                "new_per_trader_hash": proposed_per_trader_hash(config_path, trader),
                "target_contract_fingerprints": contract_snapshot(
                    symbol=target.symbol,
                    strategy_config=new_config,
                    session_timezone=new_config.timezone,
                ),
                "intended_first_reversal_quantity": target.expected_initial_quantity + target.quantity,
                "intended_resulting_position": {"side": "short", "quantity": target.quantity},
            }
        )

    changed_ids = {
        str(before.get("id"))
        for before, after in zip(current.get("traders", []), proposed.get("traders", []))
        if before != after
    }
    if changed_ids != seen:
        raise PromotionError(f"unexpected trader records changed: {sorted(changed_ids - seen)}")
    return proposed, rows


def exact_config_diff(current: dict[str, Any], proposed: dict[str, Any]) -> str:
    old = pretty_json(current).splitlines(keepends=True)
    new = pretty_json(proposed).splitlines(keepends=True)
    return "".join(difflib.unified_diff(old, new, fromfile="current_config", tofile="proposed_config"))


def normalize_preflight(preflight: dict[str, Any]) -> dict[str, Any]:
    if "target_assets" in preflight:
        return preflight
    account_key = preflight.get("primary_account_key")
    account = (preflight.get("accounts") or {}).get(account_key, {})
    positions = (preflight.get("positions_by_mode") or {}).get(account_key, [])
    orders = (preflight.get("orders_by_mode") or {}).get(account_key, [])
    by_symbol = {str(position.get("symbol", "")).upper(): position for position in positions}
    assets = {}
    for target in TARGETS:
        broker_position = by_symbol.get(target.symbol)
        assets[target.symbol] = {
            "symbol": target.symbol,
            "trader_id": target.trader_id,
            "broker_position": broker_position,
            "strategy_position": None,
            "open_orders": [order for order in orders if str(order.get("symbol", "")).upper() == target.symbol],
            "reference_price": float((broker_position or {}).get("current_price") or 0),
        }
    return {"account": account, "target_assets": assets, "open_orders": orders}


def exposure_report(preflight: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    normalized = normalize_preflight(preflight)
    assets = normalized.get("target_assets", {})
    by_symbol = {row["symbol"]: row for row in rows}
    per_symbol = {}
    combined_target = 0.0
    combined_reversal = 0.0
    for target in TARGETS:
        asset = assets.get(target.symbol, {})
        price = float(asset.get("reference_price") or (asset.get("broker_position") or {}).get("current_price") or 0)
        target_notional = price * target.quantity
        reversal_notional = price * (target.quantity + target.expected_initial_quantity)
        combined_target += target_notional
        combined_reversal += reversal_notional
        per_symbol[target.symbol] = {
            "price": price,
            "target_quantity": target.quantity,
            "first_reversal_quantity": by_symbol[target.symbol]["intended_first_reversal_quantity"],
            "target_notional": target_notional,
            "first_reversal_notional": reversal_notional,
            "risk_cap": target.risk_cap,
        }
    account = normalized.get("account", {})
    effective_buying_power = float(account.get("effective_buying_power") or account.get("buying_power") or 0)
    regt_buying_power = float(account.get("regt_buying_power") or 0)
    return {
        "per_symbol": per_symbol,
        "combined_target_gross_exposure": combined_target,
        "combined_concurrent_reversal_order_notional": combined_reversal,
        "effective_buying_power": effective_buying_power,
        "regt_buying_power": regt_buying_power,
        "effective_buying_power_headroom": effective_buying_power - combined_reversal,
        "regt_buying_power_headroom": regt_buying_power - combined_reversal,
    }


def validate_preflight(preflight: dict[str, Any], rows: list[dict[str, Any]]) -> list[str]:
    normalized = normalize_preflight(preflight)
    account = normalized.get("account", {})
    errors: list[str] = []
    if account.get("trading_blocked") or account.get("account_blocked") or account.get("transfers_blocked"):
        errors.append("account has an active restriction")
    if account.get("shorting_enabled") is False:
        errors.append("shorting is disabled")
    for target in TARGETS:
        asset = (normalized.get("target_assets") or {}).get(target.symbol, {})
        if asset.get("open_orders"):
            errors.append(f"{target.symbol}: open orders block promotion")
        broker = asset.get("broker_position")
        strategy = asset.get("strategy_position")
        if strategy is not None:
            broker_side = None if broker is None else str(broker.get("side", "")).lower()
            strategy_side = str(strategy.get("side", "")).lower() if isinstance(strategy, dict) else None
            broker_qty = 0.0 if broker is None else abs(float(broker.get("qty") or 0))
            strategy_qty = abs(float(strategy.get("qty") or 0)) if isinstance(strategy, dict) else 0.0
            if broker_side != strategy_side or broker_qty != strategy_qty:
                errors.append(f"{target.symbol}: broker and strategy positions disagree")
        price = float(asset.get("reference_price") or (broker or {}).get("current_price") or 0)
        if price <= 0:
            errors.append(f"{target.symbol}: missing bootstrap reference price")
    exposure = exposure_report(normalized, rows)
    if exposure["effective_buying_power"] < exposure["combined_concurrent_reversal_order_notional"]:
        errors.append("effective buying power below concurrent reversal exposure")
    return errors


def validate_complete_migration(results: dict[str, Any]) -> list[str]:
    errors = []
    for target in TARGETS:
        row = results.get(target.symbol)
        if not isinstance(row, dict) or row.get("status") != "ok":
            errors.append(f"{target.symbol}: migration did not complete")
    return errors


def migration_plan(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "symbol": row["symbol"],
            "trader_id": row["trader_id"],
            "policy": POLICY,
            "state_reset_required": row["old_brick_size"] != row["new_brick_size"],
            "preserve_broker_position": True,
            "bootstrap_without_signal": True,
            "old_brick_size": row["old_brick_size"],
            "new_brick_size": row["new_brick_size"],
            "old_quantity": row["old_quantity"],
            "new_quantity": row["new_quantity"],
        }
        for row in rows
    ]


def write_artifacts(config_path: Path, out: Path, preflight_path: Path | None = None) -> dict[str, Any]:
    current = json.loads(config_path.read_text(encoding="utf-8"))
    proposed, rows = apply_approved_targets(config_path, current)
    diff = exact_config_diff(current, proposed)
    preflight = json.loads(preflight_path.read_text(encoding="utf-8")) if preflight_path else None
    preflight_errors = validate_preflight(preflight, rows) if preflight else []
    exposure = exposure_report(preflight, rows) if preflight else None
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_commit": git_head(),
        "deployment_commit": None,
        "policy": POLICY,
        "config_path": str(config_path),
        "current_config_hash": stable_hash(current),
        "proposed_config_hash": stable_hash(proposed),
        "approved_targets": [asdict(target) for target in TARGETS],
        "per_trader": rows,
        "migration_plan": migration_plan(rows),
        "preflight_errors": preflight_errors,
        "exposure": exposure,
        "rollback_command": "./deploy-trading --rollback <deployment_id> --restart-fleet",
        "deployment_id": None,
        "service_status": None,
        "post_deployment_validation": None,
        "orders_and_fills_during_deployment_window": None,
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "manifest.json").write_text(pretty_json(manifest), encoding="utf-8")
    (out / "proposed_config.json").write_text(pretty_json(proposed), encoding="utf-8")
    (out / "rollback_config.json").write_text(pretty_json(current), encoding="utf-8")
    (out / "config_diff.patch").write_text(diff, encoding="utf-8")
    (out / "migration_plan.json").write_text(pretty_json(migration_plan(rows)), encoding="utf-8")
    if preflight is not None:
        (out / "preflight.json").write_text(pretty_json(preflight), encoding="utf-8")
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare Camatron E1 brick and quantity promotion artifacts.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--preflight-json")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest = write_artifacts(
        Path(args.config),
        Path(args.out),
        preflight_path=Path(args.preflight_json) if args.preflight_json else None,
    )
    if manifest["preflight_errors"]:
        raise SystemExit("preflight failed: " + "; ".join(manifest["preflight_errors"]))
    print(pretty_json({"artifact_dir": args.out, "current_config_hash": manifest["current_config_hash"], "proposed_config_hash": manifest["proposed_config_hash"]}))


if __name__ == "__main__":
    main()
