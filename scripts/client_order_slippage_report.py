from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


LIVE_BASE_URL = "https://api.alpaca.markets"
PAPER_BASE_URL = "https://paper-api.alpaca.markets"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Report fill slippage by querying Alpaca client order IDs from live events.")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--trader-id", default="")
    parser.add_argument("--event-log", default="artifacts/live/fleet_live_events.jsonl")
    parser.add_argument("--mode", default="live", choices=["live", "paper"])
    parser.add_argument("--credential-profile", default="", help="Optional ALPACA_PROFILE_<NAME> credential profile.")
    parser.add_argument("--limit-legs", type=int, default=0)
    parser.add_argument("--sleep", type=float, default=0.02)
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


def alpaca_config(mode: str, profile: str = "") -> tuple[str, dict[str, str]]:
    load_dotenv()
    normalized_profile = profile.strip().upper()
    if normalized_profile:
        base_url = (
            first_env(f"ALPACA_PROFILE_{normalized_profile}_BASE_URL", f"APCA_PROFILE_{normalized_profile}_BASE_URL")
            or (PAPER_BASE_URL if mode == "paper" else LIVE_BASE_URL)
        )
        key = first_env(
            f"ALPACA_PROFILE_{normalized_profile}_API_KEY_ID",
            f"ALPACA_PROFILE_{normalized_profile}_API_KEY",
        )
        secret = first_env(
            f"ALPACA_PROFILE_{normalized_profile}_API_SECRET_KEY",
            f"ALPACA_PROFILE_{normalized_profile}_SECRET_KEY",
        )
    elif mode == "live":
        base_url = first_env("ALPACA_LIVE_BASE_URL", "ALPACA_BASE_URL") or LIVE_BASE_URL
        key = first_env("APCA_API_KEY_ID", "ALPACA_API_KEY_ID", "ALPACA_API_KEY")
        secret = first_env("APCA_API_SECRET_KEY", "ALPACA_API_SECRET_KEY", "ALPACA_SECRET_KEY")
    else:
        base_url = first_env("ALPACA_PAPER_BASE_URL", "ALPACA_BASE_URL") or PAPER_BASE_URL
        key = first_env("ALPACA_PAPER_API_KEY_ID", "ALPACA_PAPER_API_KEY", "ALPACA_API_KEY_ID", "ALPACA_API_KEY")
        secret = first_env("ALPACA_PAPER_API_SECRET_KEY", "ALPACA_PAPER_SECRET_KEY", "ALPACA_API_SECRET_KEY", "ALPACA_SECRET_KEY")
    if not key or not secret:
        profile_label = f" profile {profile}" if profile else ""
        raise RuntimeError(f"Missing Alpaca {mode}{profile_label} credentials.")
    return base_url.rstrip("/"), {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}


def iter_events(path: Path) -> tuple[list[dict[str, Any]], int]:
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
    return events, skipped


def execution_legs(path: Path, symbol: str, trader_id: str) -> tuple[list[dict[str, Any]], int]:
    events, skipped = iter_events(path)
    signal_prices: dict[tuple[str, str, str], float] = {}
    for event in events:
        if event.get("event_type") != "signal":
            continue
        payload = event.get("payload") or {}
        if str(payload.get("symbol", "")).upper() != symbol:
            continue
        if trader_id and payload.get("trader_id") != trader_id:
            continue
        if payload.get("price") is None:
            continue
        key = (
            str(payload.get("timestamp") or ""),
            str(payload.get("reason") or ""),
            str(payload.get("broker_side") or ""),
        )
        signal_prices[key] = float(payload["price"])

    legs: list[dict[str, Any]] = []
    for event in events:
        if event.get("event_type") != "execution":
            continue
        payload = event.get("payload") or {}
        intent = payload.get("intent") or {}
        if payload.get("mode") != "real":
            continue
        if str(intent.get("symbol", "")).upper() != symbol:
            continue
        if trader_id and intent.get("trader_id") != trader_id:
            continue
        signal_price = intent.get("price")
        if signal_price is None:
            signal_price = signal_prices.get(
                (
                    str(intent.get("timestamp") or ""),
                    str(intent.get("reason") or ""),
                    str(intent.get("side") or ""),
                )
            )
        if signal_price is None:
            continue
        response = payload.get("response") or {}
        signal_price = float(signal_price)

        order = response.get("response") or response.get("order")
        if isinstance(order, dict) and order.get("client_order_id"):
            legs.append(
                {
                    "logged_at": event.get("logged_at"),
                    "signal_ts": intent.get("timestamp"),
                    "trader_id": intent.get("trader_id"),
                    "reason": intent.get("reason"),
                    "leg": "single",
                    "client_order_id": order.get("client_order_id"),
                    "side": order.get("side") or intent.get("side"),
                    "signal_price": signal_price,
                }
            )

        for key, leg_name in (("close_response", "close"), ("open_response", "open")):
            child = response.get(key)
            if isinstance(child, dict) and child.get("client_order_id"):
                legs.append(
                    {
                        "logged_at": event.get("logged_at"),
                        "signal_ts": intent.get("timestamp"),
                        "trader_id": intent.get("trader_id"),
                        "reason": intent.get("reason"),
                        "leg": leg_name,
                        "client_order_id": child.get("client_order_id"),
                        "side": child.get("side"),
                        "signal_price": signal_price,
                    }
                )
    return legs, skipped


def request_json(url: str, headers: dict[str, str], attempts: int = 4) -> dict[str, Any] | None:
    request = urllib.request.Request(url, headers=headers, method="GET")
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            if exc.code in {408, 429, 500, 502, 503, 504} and attempt < attempts:
                time.sleep(min(2.0 * attempt, 8.0))
                continue
            body = exc.read().decode("utf-8", errors="replace")[:300]
            raise RuntimeError(f"Alpaca request failed {exc.code}: {body}") from exc
        except urllib.error.URLError as exc:
            if attempt >= attempts:
                raise RuntimeError(f"Alpaca request failed: {exc}") from exc
            time.sleep(min(2.0 * attempt, 8.0))
    return None


def get_order_by_client_id(base_url: str, headers: dict[str, str], client_order_id: str) -> dict[str, Any] | None:
    query = urllib.parse.urlencode({"client_order_id": client_order_id})
    return request_json(f"{base_url}/v2/orders:by_client_order_id?{query}", headers)


def adverse_slippage(side: str, signal_price: float, fill_price: float) -> float:
    normalized = side.lower().strip()
    if normalized == "buy":
        return fill_price - signal_price
    if normalized == "sell":
        return signal_price - fill_price
    raise ValueError(f"Unsupported side: {side}")


def pct(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, round((percentile / 100.0) * (len(ordered) - 1))))
    return ordered[idx]


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    filled = [row for row in rows if row.get("filled_avg_price") and float(row.get("filled_qty") or 0.0) > 0]
    filled_shares = sum(float(row.get("filled_qty") or 0.0) for row in filled)
    total_adverse = sum(float(row.get("adverse_slippage_usd") or 0.0) for row in filled)
    per_share = [float(row.get("adverse_slippage_per_share") or 0.0) for row in filled]
    positive = [value for value in per_share if value > 0]
    improved_or_even = [value for value in per_share if value <= 0]
    return {
        "filled_legs": len(filled),
        "filled_shares": filled_shares,
        "total_adverse_slippage_usd": total_adverse,
        "avg_adverse_slippage_per_share_share_weighted": total_adverse / filled_shares if filled_shares else None,
        "avg_adverse_slippage_per_share_leg_weighted": statistics.mean(per_share) if per_share else None,
        "median_adverse_slippage_per_share": statistics.median(per_share) if per_share else None,
        "p90_adverse_slippage_per_share": pct(per_share, 90),
        "p95_adverse_slippage_per_share": pct(per_share, 95),
        "best_price_improvement_per_share": min(per_share) if per_share else None,
        "worst_adverse_slippage_per_share": max(per_share) if per_share else None,
        "adverse_leg_count": len(positive),
        "improved_or_even_leg_count": len(improved_or_even),
    }


def main() -> int:
    args = parse_args()
    symbol = args.symbol.upper()
    legs, skipped = execution_legs(Path(args.event_log), symbol, args.trader_id)
    if args.limit_legs:
        legs = legs[-args.limit_legs :]
    base_url, headers = alpaca_config(args.mode, args.credential_profile)

    rows: list[dict[str, Any]] = []
    for idx, leg in enumerate(legs, start=1):
        row = dict(leg)
        order = get_order_by_client_id(base_url, headers, str(leg["client_order_id"]))
        if not order:
            row["status"] = "missing_from_alpaca"
            rows.append(row)
            continue
        row.update(
            {
                "alpaca_order_id": order.get("id"),
                "status": order.get("status"),
                "submitted_at": order.get("submitted_at"),
                "filled_at": order.get("filled_at"),
                "filled_avg_price": order.get("filled_avg_price"),
                "filled_qty": order.get("filled_qty"),
            }
        )
        fill = float(order.get("filled_avg_price") or 0.0)
        qty = float(order.get("filled_qty") or 0.0)
        if fill and qty:
            adverse = adverse_slippage(str(row["side"]), float(row["signal_price"]), fill)
            row.update(
                {
                    "signed_fill_minus_signal": fill - float(row["signal_price"]),
                    "adverse_slippage_per_share": adverse,
                    "adverse_slippage_usd": adverse * qty,
                }
            )
        rows.append(row)
        if args.sleep > 0 and idx < len(legs):
            time.sleep(args.sleep)

    result = {
        "symbol": symbol,
        "trader_id": args.trader_id or None,
        "event_log": args.event_log,
        "mode": args.mode,
        "credential_profile": args.credential_profile or None,
        "execution_legs": len(legs),
        "skipped_malformed_log_lines": skipped,
        "summary": summarize(rows),
        "rows": rows,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
