from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from renko_builder import RenkoBuildState, update_renko_build_state
from silver.market import CANONICAL_RENKO_SOURCE, CanonicalMarketObservationAdapter
from trading_events import MarketTick


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild Renko JSONL archives from archived tick JSONL.")
    parser.add_argument("--archive-root", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--dates", nargs="+", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--brick-size", type=float, required=True)
    parser.add_argument("--anchor-mode", default="grid")
    parser.add_argument("--anchor-price", type=float, default=0.0)
    parser.add_argument("--renko-source", choices=(CANONICAL_RENKO_SOURCE,), default=CANONICAL_RENKO_SOURCE)
    return parser.parse_args()


def iter_archived_ticks(root: Path, symbol: str, dates: Iterable[str]) -> Iterable[dict[str, Any]]:
    for archive_date in dates:
        path = root / "ticks" / archive_date / f"{symbol.upper()}.jsonl"
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                payload = json.loads(line)
                yield payload


def iter_source_ticks(root: Path, symbol: str, dates: list[str], renko_source: str) -> Iterable[dict[str, Any]]:
    if renko_source != CANONICAL_RENKO_SOURCE:
        raise RuntimeError(f"Unsupported noncanonical renko_source: {renko_source}")
    adapter = CanonicalMarketObservationAdapter(run_id=f"rebuild_renko:{symbol.upper()}")
    for payload in iter_archived_ticks(root, symbol, dates):
        tick = MarketTick(
            timestamp=pd.Timestamp(payload["timestamp"]),
            symbol=str(payload["symbol"]).upper(),
            price=float(payload["price"]),
            size=float(payload.get("size", 0.0) or 0.0),
            exchange=str(payload.get("exchange", "")),
            trade_id=str(payload.get("trade_id", "")),
            tape=str(payload.get("tape", "")),
            conditions=tuple(str(item) for item in payload.get("conditions", []) if item is not None),
        )
        result = adapter.process_tick(tick, trader_id=f"rebuild_renko_{symbol.lower()}")
        for observation_tick in adapter.observation_ticks(result):
            emitted = dict(payload)
            emitted["timestamp"] = observation_tick.timestamp.isoformat()
            emitted["price"] = observation_tick.price
            emitted["trade_id"] = observation_tick.trade_id
            yield emitted


def main() -> int:
    args = parse_args()
    symbol = str(args.symbol).upper()
    root = Path(args.archive_root)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    for archive_date in args.dates:
        stale = output_dir / f"{archive_date}.jsonl"
        if stale.exists():
            stale.unlink()
    state = RenkoBuildState(
        brick_size=args.brick_size,
        anchor_mode=args.anchor_mode,
        anchor_price=args.anchor_price,
    )
    processed = 0
    raw_source_ticks = 0
    generated_files: set[Path] = set()

    for tick in iter_source_ticks(root, symbol, args.dates, args.renko_source):
        raw_source_ticks += 1
        timestamp = pd.Timestamp(tick["timestamp"])
        update_renko_build_state(state, [timestamp], [float(tick["price"])])
        new_rows = state.bricks[processed:]
        processed = len(state.bricks)
        for row in new_rows:
            close = float(row["brick_close"])
            direction = int(row["direction"])
            brick_timestamp = pd.Timestamp(row["timestamp"])
            payload = {
                "timestamp": brick_timestamp.isoformat(),
                "symbol": symbol,
                "open": close - (direction * args.brick_size),
                "close": close,
                "direction": direction,
                "run_len": int(row["run_len"]),
                "brick_size": args.brick_size,
            }
            path = output_dir / f"{brick_timestamp.tz_convert('UTC').date().isoformat()}.jsonl"
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, sort_keys=True) + "\n")
            generated_files.add(path)

    summary = {
        "symbol": symbol,
        "dates": args.dates,
        "renko_source": args.renko_source,
        "price_filter": "conditions exactly @ or @,F",
        "source_tick_count": raw_source_ticks,
        "brick_count": len(state.bricks),
        "outputs": sorted(str(path) for path in generated_files),
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
