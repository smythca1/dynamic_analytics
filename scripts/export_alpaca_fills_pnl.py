from __future__ import annotations

import argparse
import csv
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests


DEFAULT_BASE_URL = "https://api.alpaca.markets"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export Alpaca fill activities and compute realized P&L from buys/sells.")
    parser.add_argument("--event-log", default="artifacts/live/fleet_live_events.jsonl")
    parser.add_argument("--after", default="2026-07-01T00:00:00Z")
    parser.add_argument("--until", default="2026-07-22T00:00:00Z")
    parser.add_argument("--output-csv", default="artifacts/live/alpaca_fills_20260701_20260721.csv")
    parser.add_argument("--output-json", default="artifacts/live/alpaca_fills_pnl_20260701_20260721.json")
    parser.add_argument("--page-size", type=int, default=100)
    return parser.parse_args()


def load_dotenv(path: Path = Path(".env")) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def first_env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def alpaca_config() -> tuple[str, dict[str, str]]:
    load_dotenv()
    base_url = first_env("ALPACA_LIVE_BASE_URL", "ALPACA_BASE_URL") or DEFAULT_BASE_URL
    key = first_env("APCA_API_KEY_ID", "ALPACA_API_KEY_ID", "ALPACA_API_KEY")
    secret = first_env("APCA_API_SECRET_KEY", "ALPACA_API_SECRET_KEY", "ALPACA_SECRET_KEY")
    if not key or not secret:
        raise RuntimeError("Missing Alpaca live credentials.")
    return base_url.rstrip("/"), {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}


def parse_ts(raw: object) -> datetime | None:
    if raw in {None, ""}:
        return None
    text = str(raw)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if "." in text:
        prefix, suffix = text.split(".", 1)
        zone = ""
        for marker in ("+", "-"):
            idx = suffix.find(marker)
            if idx > 0:
                zone = suffix[idx:]
                suffix = suffix[:idx]
                break
        text = f"{prefix}.{suffix[:6].ljust(6, '0')}{zone}"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso_z(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def iter_order_map(event_log: Path, after: datetime, until: datetime) -> dict[str, dict[str, Any]]:
    order_map: dict[str, dict[str, Any]] = {}
    with event_log.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("event_type") != "execution":
                continue
            logged_at = parse_ts(event.get("logged_at"))
            if logged_at is None or logged_at < after or logged_at >= until:
                continue
            payload = event.get("payload") or {}
            if payload.get("mode") != "real":
                continue
            intent = payload.get("intent") or {}
            response = payload.get("response") or {}
            base = {
                "logged_at": event.get("logged_at"),
                "trader_id": intent.get("trader_id"),
                "symbol": str(intent.get("symbol") or "").upper(),
                "intent_side": intent.get("side"),
                "reason": intent.get("reason"),
                "signal_type": intent.get("signal_type"),
                "signal_ts": intent.get("timestamp"),
            }
            for key, leg_name in (("response", "single"), ("close_response", "close"), ("open_response", "open")):
                order = response.get(key)
                if not isinstance(order, dict) or not order.get("id"):
                    continue
                order_map[str(order["id"])] = {
                    **base,
                    "leg": leg_name,
                    "client_order_id": order.get("client_order_id"),
                    "order_side": order.get("side"),
                }
    return order_map


def fetch_fills(base_url: str, headers: dict[str, str], after: datetime, until: datetime, page_size: int) -> list[dict[str, Any]]:
    fills: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    cursor = after
    while True:
        response = requests.get(
            f"{base_url}/v2/account/activities/FILL",
            headers=headers,
            params={
                "after": iso_z(cursor),
                "until": iso_z(until),
                "direction": "asc",
                "page_size": page_size,
            },
            timeout=30,
        )
        if response.status_code in {408, 429, 500, 502, 503, 504}:
            time.sleep(2)
            continue
        if response.status_code >= 400:
            raise RuntimeError(f"Alpaca FILL export failed {response.status_code}: {response.text[:500]}")
        page = response.json()
        if not isinstance(page, list):
            raise RuntimeError(f"Unexpected Alpaca FILL response: {page!r}")
        new_rows = []
        latest_ts = cursor
        for row in page:
            row_id = str(row.get("id") or "")
            if row_id and row_id in seen_ids:
                continue
            if row_id:
                seen_ids.add(row_id)
            tx = parse_ts(row.get("transaction_time"))
            if tx and tx > latest_ts:
                latest_ts = tx
            new_rows.append(row)
        fills.extend(new_rows)
        if len(page) < page_size or not new_rows:
            break
        cursor = latest_ts + timedelta(microseconds=1)
    fills.sort(key=lambda row: (str(row.get("transaction_time") or ""), str(row.get("id") or "")))
    return fills


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "transaction_time",
        "trader_id",
        "symbol",
        "side",
        "qty",
        "price",
        "order_id",
        "client_order_id",
        "reason",
        "leg",
        "activity_id",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in columns})


def realized_pnl(rows: list[dict[str, Any]]) -> tuple[float, float, list[dict[str, float]]]:
    lots: list[dict[str, float]] = []
    realized = 0.0
    closed_qty = 0.0
    for row in rows:
        qty = float(row["qty"])
        price = float(row["price"])
        signed_qty = qty if str(row["side"]).lower() == "buy" else -qty
        remaining = signed_qty
        while abs(remaining) > 1e-9 and lots and (lots[0]["qty"] > 0) != (remaining > 0):
            lot = lots[0]
            close_qty = min(abs(remaining), abs(lot["qty"]))
            if lot["qty"] > 0:
                realized += (price - lot["price"]) * close_qty
            else:
                realized += (lot["price"] - price) * close_qty
            lot["qty"] += close_qty if lot["qty"] < 0 else -close_qty
            remaining += close_qty if remaining < 0 else -close_qty
            closed_qty += close_qty
            if abs(lot["qty"]) < 1e-9:
                lots.pop(0)
        if abs(remaining) > 1e-9:
            lots.append({"qty": remaining, "price": price})
    return realized, closed_qty, lots


def summarize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row["trader_id"]), []).append(row)
    summaries = []
    for trader_id, trader_rows in sorted(grouped.items()):
        pnl, closed_qty, lots = realized_pnl(trader_rows)
        buy_qty = sum(float(row["qty"]) for row in trader_rows if row["side"] == "buy")
        sell_qty = sum(float(row["qty"]) for row in trader_rows if row["side"] == "sell")
        buy_notional = sum(float(row["qty"]) * float(row["price"]) for row in trader_rows if row["side"] == "buy")
        sell_notional = sum(float(row["qty"]) * float(row["price"]) for row in trader_rows if row["side"] == "sell")
        summaries.append(
            {
                "trader_id": trader_id,
                "symbol": trader_rows[0]["symbol"],
                "fills": len(trader_rows),
                "buy_qty": buy_qty,
                "sell_qty": sell_qty,
                "buy_notional": buy_notional,
                "sell_notional": sell_notional,
                "closed_qty": closed_qty,
                "realized_pnl": pnl,
                "open_qty": sum(lot["qty"] for lot in lots),
                "open_lots": lots,
                "first_fill": trader_rows[0]["transaction_time"],
                "last_fill": trader_rows[-1]["transaction_time"],
            }
        )
    return summaries


def main() -> int:
    args = parse_args()
    after = parse_ts(args.after)
    until = parse_ts(args.until)
    if after is None or until is None:
        raise RuntimeError("Could not parse --after/--until.")
    base_url, headers = alpaca_config()
    order_map = iter_order_map(Path(args.event_log), after, until)
    fills = fetch_fills(base_url, headers, after, until, args.page_size)
    mapped_rows: list[dict[str, Any]] = []
    unmatched = 0
    for fill in fills:
        order_id = str(fill.get("order_id") or "")
        meta = order_map.get(order_id)
        if not meta:
            unmatched += 1
            continue
        mapped_rows.append(
            {
                "transaction_time": fill.get("transaction_time"),
                "trader_id": meta.get("trader_id"),
                "symbol": str(fill.get("symbol") or meta.get("symbol") or "").upper(),
                "side": str(fill.get("side") or meta.get("order_side") or "").lower(),
                "qty": float(fill.get("qty") or 0.0),
                "price": float(fill.get("price") or 0.0),
                "order_id": order_id,
                "client_order_id": meta.get("client_order_id"),
                "reason": meta.get("reason"),
                "leg": meta.get("leg"),
                "activity_id": fill.get("id"),
            }
        )
    mapped_rows = [row for row in mapped_rows if row["trader_id"] and row["symbol"] and row["qty"] > 0 and row["price"] > 0]
    mapped_rows.sort(key=lambda row: (str(row["transaction_time"]), str(row["activity_id"])))
    write_csv(Path(args.output_csv), mapped_rows)
    result = {
        "after": iso_z(after),
        "until": iso_z(until),
        "raw_fill_activities": len(fills),
        "mapped_fill_activities": len(mapped_rows),
        "unmatched_fill_activities": unmatched,
        "csv": args.output_csv,
        "summary": summarize(mapped_rows),
    }
    output = Path(args.output_json)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
