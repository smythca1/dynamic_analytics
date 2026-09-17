from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests


ALPACA_DATA_BASE_URL = "https://data.alpaca.markets"
ALPACA_LIVE_BASE_URL = "https://api.alpaca.markets"
EASTERN = ZoneInfo("America/New_York")


@dataclass
class Leg:
    trader_id: str
    symbol: str
    client_order_id: str
    side: str
    logged_at: datetime | None
    signal_ts: datetime | None
    reason: str | None
    leg: str


@dataclass
class Fill:
    trader_id: str
    symbol: str
    client_order_id: str
    side: str
    qty: float
    price: float
    filled_at: datetime
    submitted_at: datetime | None
    reason: str | None
    leg: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare July live trader P&L against same-window buy-and-hold.")
    parser.add_argument("--config", default="artifacts/production_current/traders.live.20260721.json")
    parser.add_argument("--event-log", default="artifacts/production_current/fleet_live_events_20260721_latest.jsonl")
    parser.add_argument("--start", default="2026-07-01")
    parser.add_argument("--end", default="2026-07-22", help="Exclusive UTC/order end date, YYYY-MM-DD.")
    parser.add_argument("--end-ts", default="", help="Optional exclusive UTC/order end timestamp, overriding --end.")
    parser.add_argument("--feed", default="sip")
    parser.add_argument("--output", default="artifacts/production_current/july_trader_vs_buy_hold_20260721.json")
    parser.add_argument("--request-sleep", type=float, default=0.05)
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


def alpaca_headers() -> dict[str, str]:
    load_dotenv()
    key = first_env("APCA_API_KEY_ID", "ALPACA_API_KEY_ID", "ALPACA_API_KEY")
    secret = first_env("APCA_API_SECRET_KEY", "ALPACA_API_SECRET_KEY", "ALPACA_SECRET_KEY")
    if not key or not secret:
        raise RuntimeError("Missing live Alpaca credentials.")
    return {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}


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


def request_json(url: str, headers: dict[str, str], params: dict[str, object], attempts: int = 5) -> dict[str, Any] | list[dict[str, Any]]:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=60)
            if response.status_code in {408, 429, 500, 502, 503, 504} and attempt < attempts:
                time.sleep(min(2.0 * attempt, 10.0))
                continue
            if response.status_code >= 400:
                raise RuntimeError(f"Alpaca request failed {response.status_code}: {response.text[:500]}")
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            if attempt >= attempts:
                break
            time.sleep(min(2.0 * attempt, 10.0))
    raise RuntimeError(f"Alpaca request failed after {attempts} attempts: {last_error}") from last_error


def load_current_config(path: Path) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    config = json.loads(path.read_text(encoding="utf-8"))
    current: dict[str, dict[str, Any]] = {}
    by_symbol: dict[str, dict[str, Any]] = {}
    for trader in config.get("traders", []):
        if not isinstance(trader, dict):
            continue
        tid = str(trader.get("id") or "")
        symbol = str(trader.get("symbol") or "").upper()
        if not tid or not symbol:
            continue
        if trader.get("enabled") and trader.get("execution_enabled") and trader.get("mode") == "live":
            current[tid] = trader
            by_symbol[symbol] = trader
    return current, by_symbol


def iter_events(path: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    skipped = 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                skipped += 1
    if skipped:
        print(f"Skipped {skipped} malformed event log lines", flush=True)
    return events


def collect_execution_legs(events: list[dict[str, Any]], start: datetime, end: datetime) -> dict[str, list[Leg]]:
    legs_by_trader: dict[str, list[Leg]] = {}
    for event in events:
        if event.get("event_type") != "execution":
            continue
        logged_at = parse_ts(event.get("logged_at"))
        if logged_at is None or logged_at < start or logged_at >= end:
            continue
        payload = event.get("payload") or {}
        if payload.get("mode") != "real":
            continue
        intent = payload.get("intent") or {}
        trader_id = str(intent.get("trader_id") or "")
        symbol = str(intent.get("symbol") or "").upper()
        if not trader_id or not symbol:
            continue
        response = payload.get("response") or {}
        signal_ts = parse_ts(intent.get("timestamp"))
        reason = intent.get("reason")

        def add_leg(order: dict[str, Any], leg_name: str) -> None:
            client_order_id = str(order.get("client_order_id") or "")
            if not client_order_id:
                return
            side = str(order.get("side") or intent.get("side") or "").lower()
            if side not in {"buy", "sell"}:
                return
            legs_by_trader.setdefault(trader_id, []).append(
                Leg(
                    trader_id=trader_id,
                    symbol=symbol,
                    client_order_id=client_order_id,
                    side=side,
                    logged_at=logged_at,
                    signal_ts=signal_ts,
                    reason=str(reason) if reason is not None else None,
                    leg=leg_name,
                )
            )

        order = response.get("response") or response.get("order")
        if isinstance(order, dict):
            add_leg(order, "single")
        for key, leg_name in (("close_response", "close"), ("open_response", "open")):
            child = response.get(key)
            if isinstance(child, dict):
                add_leg(child, leg_name)
    return legs_by_trader


def collect_event_windows(events: list[dict[str, Any]], start: datetime, end: datetime) -> dict[str, dict[str, datetime]]:
    windows: dict[str, dict[str, datetime]] = {}
    for event in events:
        logged_at = parse_ts(event.get("logged_at"))
        if logged_at is None or logged_at < start or logged_at >= end:
            continue
        payload = event.get("payload") or {}
        intent = payload.get("intent") if isinstance(payload.get("intent"), dict) else {}
        trader_id = str(payload.get("trader_id") or intent.get("trader_id") or "")
        if not trader_id:
            continue
        window = windows.setdefault(trader_id, {"first": logged_at, "last": logged_at})
        if logged_at < window["first"]:
            window["first"] = logged_at
        if logged_at > window["last"]:
            window["last"] = logged_at
    return windows


def fetch_orders(headers: dict[str, str], start: datetime, end: datetime, request_sleep: float) -> dict[str, dict[str, Any]]:
    url = f"{ALPACA_LIVE_BASE_URL}/v2/orders"
    params: dict[str, object] = {
        "status": "all",
        "limit": 500,
        "direction": "asc",
        "nested": "false",
        "after": iso_z(start),
        "until": iso_z(end),
    }
    by_client: dict[str, dict[str, Any]] = {}
    seen_order_ids: set[str] = set()
    pages = 0
    while True:
        payload = request_json(url, headers, params)
        if not isinstance(payload, list):
            raise RuntimeError(f"Unexpected Alpaca order payload: {payload!r}")
        pages += 1
        latest_submitted_at: datetime | None = None
        new_rows = 0
        for order in payload:
            order_id = str(order.get("id") or "")
            if order_id and order_id in seen_order_ids:
                continue
            if order_id:
                seen_order_ids.add(order_id)
            client_order_id = str(order.get("client_order_id") or "")
            if client_order_id:
                by_client[client_order_id] = order
            submitted_at = parse_ts(order.get("submitted_at"))
            if submitted_at and (latest_submitted_at is None or submitted_at > latest_submitted_at):
                latest_submitted_at = submitted_at
            new_rows += 1
        print(f"Fetched order page {pages}: {len(payload)} rows, {len(by_client)} client ids", flush=True)
        if len(payload) < 500 or new_rows == 0 or latest_submitted_at is None:
            break
        params["after"] = iso_z(latest_submitted_at + timedelta(microseconds=1))
        if request_sleep > 0:
            time.sleep(request_sleep)
    return by_client


def fills_for_legs(legs_by_trader: dict[str, list[Leg]], orders: dict[str, dict[str, Any]]) -> tuple[dict[str, list[Fill]], dict[str, dict[str, int]]]:
    fills_by_trader: dict[str, list[Fill]] = {}
    counts: dict[str, dict[str, int]] = {}
    for trader_id, legs in legs_by_trader.items():
        stats = counts.setdefault(trader_id, {"execution_legs": len(legs), "missing_orders": 0, "unfilled_legs": 0})
        for leg in legs:
            order = orders.get(leg.client_order_id)
            if order is None:
                stats["missing_orders"] += 1
                continue
            qty = float(order.get("filled_qty") or 0.0)
            price = float(order.get("filled_avg_price") or 0.0)
            filled_at = parse_ts(order.get("filled_at"))
            if qty <= 0.0 or price <= 0.0 or filled_at is None:
                stats["unfilled_legs"] += 1
                continue
            fills_by_trader.setdefault(trader_id, []).append(
                Fill(
                    trader_id=trader_id,
                    symbol=leg.symbol,
                    client_order_id=leg.client_order_id,
                    side=str(order.get("side") or leg.side).lower(),
                    qty=qty,
                    price=price,
                    filled_at=filled_at,
                    submitted_at=parse_ts(order.get("submitted_at")),
                    reason=leg.reason,
                    leg=leg.leg,
                )
            )
    for fills in fills_by_trader.values():
        fills.sort(key=lambda row: (row.filled_at, row.submitted_at or row.filled_at, row.client_order_id))
    return fills_by_trader, counts


def realized_and_position(fills: list[Fill]) -> tuple[float, float, list[dict[str, float]], int]:
    lots: list[dict[str, float]] = []
    realized = 0.0
    closed_trades = 0
    for fill in fills:
        qty = fill.qty if fill.side == "buy" else -fill.qty
        remaining = qty
        while remaining and lots and (lots[0]["qty"] > 0) != (remaining > 0):
            lot = lots[0]
            close_qty = min(abs(remaining), abs(lot["qty"]))
            if lot["qty"] > 0 and remaining < 0:
                realized += (fill.price - lot["price"]) * close_qty
            elif lot["qty"] < 0 and remaining > 0:
                realized += (lot["price"] - fill.price) * close_qty
            lot["qty"] += close_qty if lot["qty"] < 0 else -close_qty
            remaining += close_qty if remaining < 0 else -close_qty
            closed_trades += 1
            if abs(lot["qty"]) < 1e-9:
                lots.pop(0)
        if abs(remaining) > 1e-9:
            lots.append({"qty": remaining, "price": fill.price})
    open_qty = sum(lot["qty"] for lot in lots)
    return realized, open_qty, lots, closed_trades


def fetch_bars(headers: dict[str, str], symbol: str, start: datetime, end: datetime, feed: str) -> list[dict[str, Any]]:
    url = f"{ALPACA_DATA_BASE_URL}/v2/stocks/{symbol}/bars"
    params: dict[str, object] = {
        "start": iso_z(start),
        "end": iso_z(end),
        "timeframe": "1Min",
        "feed": feed,
        "limit": 10000,
        "sort": "asc",
    }
    bars: list[dict[str, Any]] = []
    while True:
        payload = request_json(url, headers, params)
        if not isinstance(payload, dict):
            raise RuntimeError(f"Unexpected Alpaca bars payload: {payload!r}")
        bars.extend(payload.get("bars") or [])
        token = payload.get("next_page_token")
        if not token:
            break
        params["page_token"] = token
    return bars


def regular_session_bounds(day: date) -> tuple[datetime, datetime]:
    start = datetime.combine(day, dt_time(9, 30), tzinfo=EASTERN)
    end = datetime.combine(day, dt_time(16, 0), tzinfo=EASTERN)
    return start.astimezone(timezone.utc), end.astimezone(timezone.utc)


def price_window(headers: dict[str, str], symbol: str, active_day: date, end: datetime, feed: str) -> dict[str, Any]:
    start, _ = regular_session_bounds(active_day)
    bars = fetch_bars(headers, symbol, start, end, feed)
    if not bars:
        raise RuntimeError(f"No bars returned for {symbol} from {iso_z(start)} to {iso_z(end)}")
    first = bars[0]
    last = bars[-1]
    start_ts = parse_ts(first.get("t"))
    end_ts = parse_ts(last.get("t"))
    return {
        "start_at": iso_z(start_ts) if start_ts else None,
        "end_at": iso_z(end_ts) if end_ts else None,
        "start_price": float(first["c"]),
        "end_price": float(last["c"]),
        "bar_count": len(bars),
    }


def infer_active_day(
    trader_id: str,
    current_config: dict[str, dict[str, Any]],
    fills: list[Fill],
    start_day: date,
) -> date:
    trader = current_config.get(trader_id)
    if trader:
        session = trader.get("session") if isinstance(trader.get("session"), dict) else {}
        raw = session.get("active_start_date")
        if raw:
            return datetime.strptime(str(raw), "%Y-%m-%d").date()
    if fills:
        return fills[0].filled_at.astimezone(EASTERN).date()
    return start_day


def summarize_trader(
    trader_id: str,
    fills: list[Fill],
    counts: dict[str, int],
    current_config: dict[str, dict[str, Any]],
    event_window: dict[str, datetime],
    headers: dict[str, str],
    start_day: date,
    end: datetime,
    feed: str,
) -> dict[str, Any]:
    symbol = fills[0].symbol if fills else ""
    active_day = infer_active_day(trader_id, current_config, fills, start_day)
    comparison_end = end
    if trader_id not in current_config and event_window.get("last"):
        comparison_end = min(end, event_window["last"] + timedelta(minutes=1))
    prices = price_window(headers, symbol, active_day, comparison_end, feed)
    realized, open_qty, lots, closed_trades = realized_and_position(fills)
    mark = prices["end_price"]
    unrealized = sum((mark - lot["price"]) * lot["qty"] if lot["qty"] > 0 else (lot["price"] - mark) * abs(lot["qty"]) for lot in lots)
    total_pnl = realized + unrealized
    buy_hold_pnl = prices["end_price"] - prices["start_price"]
    start_price = prices["start_price"]
    return {
        "trader_id": trader_id,
        "symbol": symbol,
        "status": "current" if trader_id in current_config else "retired_or_replaced",
        "active_day": active_day.isoformat(),
        "comparison_end_exclusive": iso_z(comparison_end),
        "first_fill_at": iso_z(fills[0].filled_at) if fills else None,
        "last_fill_at": iso_z(fills[-1].filled_at) if fills else None,
        "execution_legs": counts.get("execution_legs", 0),
        "filled_legs": len(fills),
        "missing_orders": counts.get("missing_orders", 0),
        "unfilled_legs": counts.get("unfilled_legs", 0),
        "closed_lots": closed_trades,
        "realized_pnl": realized,
        "unrealized_pnl": unrealized,
        "total_pnl": total_pnl,
        "open_qty": open_qty,
        "mark_price": mark,
        "trader_return_pct_on_1share_start_notional": 100.0 * total_pnl / start_price if start_price else None,
        "buy_hold_pnl_1share": buy_hold_pnl,
        "buy_hold_return_pct": 100.0 * buy_hold_pnl / start_price if start_price else None,
        "outperformance_usd": total_pnl - buy_hold_pnl,
        "outperformance_pct_points": (100.0 * total_pnl / start_price) - (100.0 * buy_hold_pnl / start_price) if start_price else None,
        "price_window": prices,
    }


def main() -> int:
    args = parse_args()
    headers = alpaca_headers()
    start_day = datetime.strptime(args.start, "%Y-%m-%d").date()
    start = datetime.combine(start_day, dt_time.min, tzinfo=timezone.utc)
    end = parse_ts(args.end_ts) if args.end_ts else None
    if end is None:
        end = datetime.combine(datetime.strptime(args.end, "%Y-%m-%d").date(), dt_time.min, tzinfo=timezone.utc)
    current_config, _ = load_current_config(Path(args.config))
    events = iter_events(Path(args.event_log))
    legs_by_trader = collect_execution_legs(events, start, end)
    event_windows = collect_event_windows(events, start, end)
    orders = fetch_orders(headers, start, end, args.request_sleep)
    fills_by_trader, counts = fills_for_legs(legs_by_trader, orders)

    rows: list[dict[str, Any]] = []
    for trader_id, fills in sorted(fills_by_trader.items()):
        if not fills:
            continue
        symbol = fills[0].symbol
        if not trader_id.startswith("camatron_"):
            continue
        if symbol not in {"AAPL", "NVDA", "TSLA", "QQQ"}:
            continue
        rows.append(
            summarize_trader(
                trader_id=trader_id,
                fills=fills,
                counts=counts.get(trader_id, {}),
                current_config=current_config,
                event_window=event_windows.get(trader_id, {}),
                headers=headers,
                start_day=start_day,
                end=end,
                feed=args.feed,
            )
        )

    result = {
        "generated_at": iso_z(datetime.now(timezone.utc)),
        "event_log": args.event_log,
        "config": args.config,
        "start": args.start,
        "end_exclusive": iso_z(end),
        "comparison": "Actual filled-order P&L plus mark-to-market for open inventory versus one-share buy-and-hold over each trader's live window.",
        "order_count": len(orders),
        "traders": rows,
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
