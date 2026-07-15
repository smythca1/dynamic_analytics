import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Report live order timing from signal events to Alpaca submission.")
    parser.add_argument("--event-log", default="artifacts/live/fleet_live_events.jsonl")
    parser.add_argument(
        "--trader",
        action="append",
        default=[],
        help="Symbol=trader_id mapping. Can be repeated.",
    )
    return parser.parse_args()


def parse_ts(value: object) -> datetime | None:
    if not value:
        return None
    raw = str(value)
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    return datetime.fromisoformat(raw).astimezone(timezone.utc)


def ms_between(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    return (end - start).total_seconds() * 1000.0


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


def parse_traders(raw_traders: list[str]) -> dict[str, str]:
    traders = {}
    for raw in raw_traders:
        if "=" not in raw:
            raise ValueError(f"--trader must be Symbol=trader_id, got {raw!r}")
        symbol, trader_id = raw.split("=", 1)
        traders[symbol.upper()] = trader_id
    return traders


def summarize(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"avg": None, "median": None, "min": None, "max": None}
    return {
        "avg": mean(values),
        "median": median(values),
        "min": min(values),
        "max": max(values),
    }


def main() -> None:
    args = parse_args()
    traders = parse_traders(args.trader)
    events, skipped = load_events(Path(args.event_log))

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
            event_ts = parse_ts(intent.get("timestamp"))
            signal_logged_at = parse_ts(signal.get("signal_logged_at"))
            submitted_at = parse_ts(order.get("submitted_at") or order.get("created_at"))
            created_at = parse_ts(order.get("created_at"))
            execution_logged_at = parse_ts(event.get("logged_at"))
            filled_at = parse_ts(order.get("filled_at"))
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
                    "submitted_at": order.get("submitted_at"),
                    "created_at": order.get("created_at"),
                    "filled_at": order.get("filled_at"),
                    "event_to_submitted_ms": ms_between(event_ts, submitted_at),
                    "signal_log_to_submitted_ms": ms_between(signal_logged_at, submitted_at),
                    "submitted_to_execution_log_ms": ms_between(submitted_at, execution_logged_at),
                    "submitted_to_filled_ms": ms_between(submitted_at, filled_at),
                    "created_to_submitted_ms": ms_between(created_at, submitted_at),
                }
            )

    by_symbol = {}
    for symbol in sorted({row["symbol"] for row in rows} | set(traders)):
        symbol_rows = [row for row in rows if row["symbol"] == symbol]
        by_symbol[symbol] = {
            "legs": len(symbol_rows),
            "event_to_submitted_ms": summarize(
                [row["event_to_submitted_ms"] for row in symbol_rows if row["event_to_submitted_ms"] is not None]
            ),
            "signal_log_to_submitted_ms": summarize(
                [
                    row["signal_log_to_submitted_ms"]
                    for row in symbol_rows
                    if row["signal_log_to_submitted_ms"] is not None
                ]
            ),
            "submitted_to_filled_ms": summarize(
                [
                    row["submitted_to_filled_ms"]
                    for row in symbol_rows
                    if row["submitted_to_filled_ms"] is not None
                ]
            ),
        }

    print(
        json.dumps(
            {
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
