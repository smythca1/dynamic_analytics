from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


PRICE_FORMING_CONDITIONS = {frozenset({"@"}), frozenset({"@", "F"})}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert archived tick JSONL into filtered 1-second close CSV.")
    parser.add_argument("--archive-root", default="artifacts/live")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--dates", nargs="+", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def is_price_forming(payload: dict) -> bool:
    conditions = frozenset(str(item) for item in payload.get("conditions", []) if item is not None)
    if not conditions:
        return True
    return conditions in PRICE_FORMING_CONDITIONS


def main() -> int:
    args = parse_args()
    symbol = str(args.symbol).upper()
    root = Path(args.archive_root)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    raw_ticks = 0
    kept_ticks = 0
    seconds = 0
    pending_bucket: str | None = None
    pending_payload: dict | None = None

    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["timestamp", "close"])

        for archive_date in args.dates:
            path = root / "ticks" / archive_date / f"{symbol}.jsonl"
            if not path.exists():
                continue
            with path.open("r", encoding="utf-8") as tick_file:
                for line in tick_file:
                    if not line.strip():
                        continue
                    raw_ticks += 1
                    payload = json.loads(line)
                    if not is_price_forming(payload):
                        continue
                    kept_ticks += 1
                    bucket = str(payload["timestamp"])[:19]
                    if pending_bucket is None:
                        pending_bucket = bucket
                        pending_payload = payload
                        continue
                    if bucket == pending_bucket:
                        pending_payload = payload
                        continue
                    if bucket < pending_bucket:
                        continue
                    if pending_payload is not None:
                        writer.writerow([f"{pending_bucket}Z", f"{float(pending_payload['price']):.6f}"])
                        seconds += 1
                    pending_bucket = bucket
                    pending_payload = payload

        if pending_bucket is not None and pending_payload is not None:
            writer.writerow([f"{pending_bucket}Z", f"{float(pending_payload['price']):.6f}"])
            seconds += 1

    print(
        json.dumps(
            {
                "symbol": symbol,
                "dates": args.dates,
                "output": str(output),
                "raw_ticks": raw_ticks,
                "kept_ticks": kept_ticks,
                "seconds": seconds,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
