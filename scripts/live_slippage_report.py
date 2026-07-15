import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from alpaca_trader import AlpacaTrader, load_alpaca_credentials


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Report live fill slippage versus strategy signal prices.")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--trader-id", default="")
    parser.add_argument("--event-log", default="artifacts/live/fleet_live_events.jsonl")
    parser.add_argument("--mode", default="live")
    parser.add_argument("--order-limit", type=int, default=500)
    return parser.parse_args()


def execution_legs(event_log: Path, symbol: str, trader_id: str) -> tuple[list[dict], int]:
    events: list[dict] = []
    skipped = 0
    for line in event_log.open():
        if not line.strip():
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            skipped += 1
            continue

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

    legs: list[dict] = []
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

        signal_price = float(signal_price)
        response = payload.get("response") or {}

        order = response.get("response") or response.get("order")
        if isinstance(order, dict) and order.get("client_order_id"):
            legs.append(
                {
                    "logged_at": event.get("logged_at"),
                    "signal_ts": intent.get("timestamp"),
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
                        "reason": intent.get("reason"),
                        "leg": leg_name,
                        "client_order_id": child.get("client_order_id"),
                        "side": child.get("side"),
                        "signal_price": signal_price,
                    }
                )
    return legs, skipped


def adverse_slippage(side: str, signal_price: float, fill_price: float) -> float:
    normalized_side = side.lower().strip()
    if normalized_side == "buy":
        return fill_price - signal_price
    if normalized_side == "sell":
        return signal_price - fill_price
    raise ValueError(f"Unsupported side: {side}")


def main() -> None:
    args = parse_args()
    symbol = args.symbol.upper()
    legs, skipped_malformed_log_lines = execution_legs(Path(args.event_log), symbol, args.trader_id)

    trader = AlpacaTrader(load_alpaca_credentials(args.mode), max_notional=1_000_000)
    orders = trader.get_orders("all", args.order_limit)
    by_client = {order.get("client_order_id"): order for order in orders if order.get("symbol") == symbol}

    rows = []
    for leg in legs:
        row = dict(leg)
        order = by_client.get(leg["client_order_id"])
        if not order:
            row["status"] = "missing_from_recent_order_query"
            rows.append(row)
            continue

        row.update(
            {
                "status": order.get("status"),
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

    filled = [row for row in rows if row.get("filled_avg_price") and float(row.get("filled_qty") or 0.0) > 0]
    filled_shares = sum(float(row.get("filled_qty") or 0.0) for row in filled)
    total_adverse = sum(float(row.get("adverse_slippage_usd") or 0.0) for row in filled)
    per_share = [float(row.get("adverse_slippage_per_share") or 0.0) for row in filled]

    print(
        json.dumps(
            {
                "symbol": symbol,
                "trader_id": args.trader_id or None,
                "execution_legs": len(legs),
                "skipped_malformed_log_lines": skipped_malformed_log_lines,
                "filled_legs": len(filled),
                "filled_shares": filled_shares,
                "total_adverse_slippage_usd": total_adverse,
                "avg_adverse_slippage_per_share": total_adverse / filled_shares if filled_shares else None,
                "best_price_improvement_per_share": min(per_share) if per_share else None,
                "worst_adverse_slippage_per_share": max(per_share) if per_share else None,
                "rows": rows,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
