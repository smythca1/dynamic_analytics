import argparse
import bisect
import json
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Estimate dollar impact of price movement during order latency.")
    parser.add_argument("--archive-root", default="artifacts/live")
    parser.add_argument("--event-log", default="artifacts/live/fleet_live_events.jsonl")
    parser.add_argument("--trader", action="append", default=[], help="Symbol=trader_id mapping. Can be repeated.")
    parser.add_argument("--clock", choices=["logged_at", "timestamp"], default="logged_at")
    parser.add_argument("--start-field", choices=["signal_logged_at", "event_ts"], default="signal_logged_at")
    return parser.parse_args()


def parse_ts(value: object) -> datetime | None:
    if not value:
        return None
    raw = str(value)
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    return datetime.fromisoformat(raw).astimezone(timezone.utc)


def parse_traders(raw_traders: list[str]) -> dict[str, str]:
    traders = {}
    for raw in raw_traders:
        if "=" not in raw:
            raise ValueError(f"--trader must be Symbol=trader_id, got {raw!r}")
        symbol, trader_id = raw.split("=", 1)
        traders[symbol.upper()] = trader_id
    return traders


def load_events(path: Path) -> tuple[list[dict], int]:
    events = []
    skipped = 0
    for line in path.open():
        if not line.strip():
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            skipped += 1
    return events, skipped


def load_ticks(root: Path, symbols: set[str], rows: list[dict], clock: str) -> dict[str, list[tuple[datetime, float]]]:
    needed_dates = {
        parse_ts(row["signal_logged_at"]).date().isoformat()
        for row in rows
        if parse_ts(row.get("signal_logged_at")) is not None
    } | {
        parse_ts(row["submitted_at"]).date().isoformat()
        for row in rows
        if parse_ts(row.get("submitted_at")) is not None
    }
    out = {symbol: [] for symbol in symbols}
    for symbol in symbols:
        for date in sorted(needed_dates):
            path = root / "ticks" / date / f"{symbol}.jsonl"
            if not path.exists():
                continue
            for line in path.open():
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                ts = parse_ts(payload.get(clock))
                price = payload.get("price")
                if ts is None or price is None:
                    continue
                out[symbol].append((ts, float(price)))
        out[symbol].sort(key=lambda item: item[0])
    return out


def last_tick_at_or_before(ticks: list[tuple[datetime, float]], timestamp: datetime | None) -> tuple[datetime, float] | None:
    if timestamp is None or not ticks:
        return None
    idx = bisect.bisect_right(ticks, (timestamp, float("inf"))) - 1
    if idx < 0:
        return None
    return ticks[idx]


def adverse_price_move(side: str, start_price: float, end_price: float) -> float:
    normalized = side.lower().strip()
    if normalized == "buy":
        return end_price - start_price
    if normalized == "sell":
        return start_price - end_price
    raise ValueError(f"Unsupported side: {side}")


def summarize(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"avg": None, "median": None, "min": None, "max": None, "sum": None}
    return {
        "avg": mean(values),
        "median": median(values),
        "min": min(values),
        "max": max(values),
        "sum": sum(values),
    }


def execution_rows(events: list[dict], traders: dict[str, str]) -> list[dict]:
    signals: dict[tuple[object, object, object, object], dict] = {}
    for event in events:
        if event.get("event_type") != "signal":
            continue
        payload = event.get("payload") or {}
        symbol = str(payload.get("symbol", "")).upper()
        if traders and traders.get(symbol) != payload.get("trader_id"):
            continue
        key = (
            payload.get("trader_id"),
            payload.get("timestamp"),
            payload.get("reason"),
            payload.get("broker_side"),
        )
        signals[key] = {
            "signal_logged_at": event.get("logged_at"),
            "signal_price": payload.get("price"),
            "signal_id": payload.get("signal_id"),
        }

    rows = []
    for event in events:
        if event.get("event_type") != "execution":
            continue
        payload = event.get("payload") or {}
        intent = payload.get("intent") or {}
        symbol = str(intent.get("symbol", "")).upper()
        if traders and traders.get(symbol) != intent.get("trader_id"):
            continue

        key = (
            intent.get("trader_id"),
            intent.get("timestamp"),
            intent.get("reason"),
            intent.get("side"),
        )
        signal = signals.get(key, {})
        response = payload.get("response") or {}
        leg_items = []
        order = response.get("response") or response.get("order")
        if isinstance(order, dict) and order.get("client_order_id"):
            leg_items.append(("single", order))
        for child_key, leg_name in (("close_response", "close"), ("open_response", "open")):
            child = response.get(child_key)
            if isinstance(child, dict) and child.get("client_order_id"):
                leg_items.append((leg_name, child))

        for leg, order in leg_items:
            if not signal.get("signal_logged_at") or not (order.get("submitted_at") or order.get("created_at")):
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "trader_id": intent.get("trader_id"),
                    "reason": intent.get("reason"),
                    "leg": leg,
                    "side": order.get("side") or intent.get("side"),
                    "client_order_id": order.get("client_order_id"),
                    "event_ts": intent.get("timestamp"),
                    "signal_logged_at": signal.get("signal_logged_at"),
                    "submitted_at": order.get("submitted_at") or order.get("created_at"),
                }
            )
    return rows


def main() -> None:
    args = parse_args()
    traders = parse_traders(args.trader)
    events, skipped = load_events(Path(args.event_log))
    rows = execution_rows(events, traders)
    ticks_by_symbol = load_ticks(Path(args.archive_root), set(traders), rows, args.clock)

    for row in rows:
        signal_time = parse_ts(row[args.start_field])
        submitted_time = parse_ts(row["submitted_at"])
        ticks = ticks_by_symbol.get(row["symbol"], [])
        start_tick = last_tick_at_or_before(ticks, signal_time)
        end_tick = last_tick_at_or_before(ticks, submitted_time)
        if start_tick is None or end_tick is None:
            row["latency_price_impact_usd"] = None
            continue
        start_ts, start_price = start_tick
        end_ts, end_price = end_tick
        impact = adverse_price_move(str(row["side"]), start_price, end_price)
        row.update(
            {
                "latency_start_tick_ts": start_ts.isoformat(),
                "latency_start_price": start_price,
                "latency_end_tick_ts": end_ts.isoformat(),
                "latency_end_price": end_price,
                "latency_price_impact_per_share": impact,
                "latency_price_impact_usd": impact,
            }
        )

    by_symbol = {}
    for symbol in sorted(set(traders)):
        symbol_rows = [row for row in rows if row["symbol"] == symbol]
        impacts = [
            float(row["latency_price_impact_usd"])
            for row in symbol_rows
            if row.get("latency_price_impact_usd") is not None
        ]
        by_symbol[symbol] = {
            "legs": len(symbol_rows),
            "priced_legs": len(impacts),
            "latency_price_impact_usd": summarize(impacts),
        }

    all_impacts = [
        float(row["latency_price_impact_usd"]) for row in rows if row.get("latency_price_impact_usd") is not None
    ]
    print(
        json.dumps(
            {
                "clock": args.clock,
                "start_field": args.start_field,
                "combined": {
                    "legs": len(rows),
                    "priced_legs": len(all_impacts),
                    "latency_price_impact_usd": summarize(all_impacts),
                },
                "by_symbol": by_symbol,
                "rows": rows,
                "skipped_malformed_log_lines": skipped,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
