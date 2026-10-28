import json
import hashlib
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path("/root/trading-server")
SYMBOLS = ("AAPL", "NVDA", "TSLA")
UNITS = [
    "trading-market-data-live.service",
    "trading-fleet-live.service",
    "trading-fleet-megatron-live.service",
    "trading-tick-retention.service",
    "trading-tick-retention.timer",
]
SERVICE_PROPS = [
    "Id",
    "ActiveState",
    "SubState",
    "Result",
    "MainPID",
    "ExecMainStartTimestamp",
    "ActiveEnterTimestamp",
    "Restart",
    "NRestarts",
    "MemoryCurrent",
]
STATE_FILES = {
    "camatron_aapl": ROOT / "artifacts/live/state/camatron_aapl_1s_renko_0p10_e2_tp3_sl3_qty1_20260904_live.json",
    "camatron_nvda": ROOT / "artifacts/live/state/camatron_nvda_1s_renko_0p22_e1_tp3_sl3_qty1_20260904_live.json",
    "camatron_tsla": ROOT / "artifacts/live/state/camatron_tsla_1s_renko_0p10_e2_tp2_sl3_qty1_20260904_live.json",
    "megatron_aapl": ROOT / "artifacts/live/megatron/state/megatron_aapl_1s_renko_0p12_e1_reversal_qty1_20260904_live.json",
    "megatron_nvda": ROOT / "artifacts/live/megatron/state/megatron_nvda_1s_renko_0p20_e1_reversal_qty1_20260904_live.json",
    "megatron_tsla": ROOT / "artifacts/live/megatron/state/megatron_tsla_1s_renko_0p54_e1_reversal_qty1_20260904_live.json",
}


def iso_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def run(args, timeout=20):
    try:
        proc = subprocess.run(args, text=True, capture_output=True, timeout=timeout)
        return {"returncode": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}
    except Exception as exc:
        return {"error": repr(exc)}


def systemctl_show(unit):
    args = ["systemctl", "show", unit, "--no-pager"]
    for prop in SERVICE_PROPS:
        args.extend(["-p", prop])
    result = run(args)
    data = {"_raw": result}
    for line in result.get("stdout", "").splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            data[key] = value
    return data


def load_dotenv(path):
    if not path.exists():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def state_meta(path):
    if not path.exists():
        return {"path": str(path), "exists": False}
    stat = path.stat()
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    try:
        payload = json.loads(path.read_text())
    except Exception as exc:
        payload = {"_error": repr(exc)}
    engine = payload.get("engine") if isinstance(payload, dict) else {}
    trade = engine.get("trade") if isinstance(engine, dict) and isinstance(engine.get("trade"), dict) else {}
    if not trade and isinstance(payload, dict) and isinstance(payload.get("trade"), dict):
        trade = payload["trade"]
    if not trade and isinstance(payload, dict):
        trade = payload
    position = trade.get("position") if isinstance(trade, dict) else None
    lifecycle = trade.get("tp_sl_lifecycle_state") if isinstance(trade, dict) else None
    protection = trade.get("protection_state") if isinstance(trade, dict) else None
    pending_exit = trade.get("pending_exit") if isinstance(trade, dict) else None
    side = position.get("side") if isinstance(position, dict) else None
    qty = position.get("qty") or position.get("quantity") if isinstance(position, dict) else None
    return {
        "path": str(path),
        "exists": True,
        "size": stat.st_size,
        "mtime_utc": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
        "sha256": digest.hexdigest(),
        "position_side": side,
        "position_qty": qty,
        "lifecycle_state": lifecycle,
        "pending_exit": pending_exit,
        "protection_state": protection,
        "saved_at": payload.get("saved_at") if isinstance(payload, dict) else None,
    }


def broker_snapshot():
    output = {"accounts": {}}
    try:
        from alpaca_trader import AlpacaTrader, load_alpaca_credentials

        load_dotenv(ROOT / ".env")
        for name, profile in (("camatron", None), ("megatron", "megatron")):
            try:
                credentials = load_alpaca_credentials("live", profile=profile)
                trader = AlpacaTrader(credentials, max_notional=1000000)
                positions = trader.get_positions()
                open_orders = trader.get_open_orders_nested(500)
                symbol_set = set(SYMBOLS)
                output["accounts"][name] = {
                    "profile": profile or "default_live",
                    "account": trader.get_account(),
                    "target_positions": [p for p in positions if str(p.get("symbol", "")).upper() in symbol_set],
                    "target_open_orders_nested": [o for o in open_orders if str(o.get("symbol", "")).upper() in symbol_set],
                    "target_position_count": sum(1 for p in positions if str(p.get("symbol", "")).upper() in symbol_set),
                    "target_open_order_count": sum(1 for o in open_orders if str(o.get("symbol", "")).upper() in symbol_set),
                }
            except Exception as exc:
                output["accounts"][name] = {"profile": profile or "default_live", "error": repr(exc)}
    except Exception as exc:
        output["error"] = repr(exc)
    return output


def main():
    result = {
        "captured_at": iso_now(),
        "hostname": run(["hostname"]).get("stdout", "").strip(),
        "services": {unit: systemctl_show(unit) for unit in UNITS},
        "states": {name: state_meta(path) for name, path in STATE_FILES.items()},
        "broker": broker_snapshot(),
    }
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
