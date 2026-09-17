from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fleet_runner import (
    build_strategy_config,
    load_config,
    normalize_renko_source,
)
from silver.market import CanonicalMarketObservationAdapter
from strategy_engine import MeanReversionStrategyConfig, MeanReversionStrategyEngine, RenkoStrategyEngine
from trading_events import MarketTick, Signal


@dataclass(frozen=True)
class SignalRecord:
    timestamp: pd.Timestamp
    signal_type: str
    symbol: str
    side: str
    broker_side: str
    price: float
    reason: str

    @property
    def key(self) -> tuple[str, str, str, str, str, str, float]:
        return (
            self.timestamp.isoformat(),
            self.signal_type,
            self.symbol,
            self.side,
            self.broker_side,
            self.reason,
            round(self.price, 6),
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Replay archived live ticks through the production strategy engine "
            "and compare deterministic signals against live signal events."
        )
    )
    parser.add_argument("--config", default="configs/traders.live.example.json")
    parser.add_argument("--trader-id", required=True)
    parser.add_argument("--archive-root", default="artifacts/live")
    parser.add_argument("--event-log", default="artifacts/live/fleet_live_events.jsonl")
    parser.add_argument("--start-date", help="UTC archive start date, YYYY-MM-DD.")
    parser.add_argument("--end-date", help="UTC archive end date, YYYY-MM-DD.")
    parser.add_argument("--threshold", type=float, default=0.98)
    parser.add_argument("--max-mismatches", type=int, default=10)
    parser.add_argument("--json-output", help="Optional path to write the parity report JSON.")
    return parser.parse_args()


def utc_timestamp(value: Any) -> pd.Timestamp:
    return pd.Timestamp(pd.to_datetime(value, utc=True))


def parse_date(raw: str | None) -> pd.Timestamp | None:
    if not raw:
        return None
    return pd.Timestamp(raw, tz="UTC")


def in_window(timestamp: pd.Timestamp, start: pd.Timestamp | None, end_exclusive: pd.Timestamp | None) -> bool:
    if start is not None and timestamp < start:
        return False
    if end_exclusive is not None and timestamp >= end_exclusive:
        return False
    return True


def load_trader(config_path: Path, trader_id: str) -> dict[str, Any]:
    config = load_config(config_path)
    for trader in config["traders"]:
        if isinstance(trader, dict) and trader.get("id") == trader_id:
            return trader
    raise RuntimeError(f"Trader id not found in config: {trader_id}")


def archive_dates(root: Path, symbol: str, start: pd.Timestamp | None, end_exclusive: pd.Timestamp | None) -> list[str]:
    ticks_root = root / "ticks"
    if not ticks_root.exists():
        raise RuntimeError(f"Missing tick archive root: {ticks_root}")

    dates: list[str] = []
    for path in sorted(ticks_root.glob(f"*/{symbol.upper()}.jsonl")):
        raw_date = path.parent.name
        try:
            day = pd.Timestamp(raw_date, tz="UTC")
        except Exception:
            continue
        # Include one extra archive date after the requested end so 1s-close
        # replay can flush the final pending second exactly like production.
        replay_end = end_exclusive + pd.Timedelta(days=1) if end_exclusive is not None else None
        if in_window(day, start.normalize() if start is not None else None, replay_end):
            dates.append(raw_date)
    return dates


def iter_archived_ticks(
    root: Path,
    symbol: str,
    dates: Iterable[str],
) -> Iterable[MarketTick]:
    for archive_date in dates:
        path = root / "ticks" / archive_date / f"{symbol.upper()}.jsonl"
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                payload = json.loads(line)
                yield MarketTick(
                    timestamp=utc_timestamp(payload["timestamp"]),
                    symbol=str(payload["symbol"]).upper(),
                    price=float(payload["price"]),
                    size=float(payload.get("size", 0.0) or 0.0),
                    exchange=str(payload.get("exchange", "")),
                    trade_id=str(payload.get("trade_id", "")),
                    tape=str(payload.get("tape", "")),
                    conditions=tuple(str(item) for item in payload.get("conditions", []) if item is not None),
                )


def iter_source_ticks(
    root: Path,
    symbol: str,
    dates: Iterable[str],
    renko_source: str,
) -> tuple[Iterable[MarketTick], dict[str, int]]:
    if renko_source != "1s_close":
        raise RuntimeError(f"Unsupported renko_source: {renko_source}")

    stats = {"raw_tick_count": 0, "source_tick_count": 0}

    def one_second_closes() -> Iterable[MarketTick]:
        adapter = CanonicalMarketObservationAdapter(run_id=f"live_signal_parity:{symbol.upper()}")
        for tick in iter_archived_ticks(root, symbol, dates):
            stats["raw_tick_count"] += 1
            result = adapter.process_tick(tick, trader_id=f"live_signal_parity_{symbol.lower()}")
            for source_tick in adapter.observation_ticks(result):
                stats["source_tick_count"] += 1
                yield source_tick

    return one_second_closes(), stats


def signal_from_payload(payload: dict[str, Any]) -> SignalRecord:
    return SignalRecord(
        timestamp=utc_timestamp(payload["timestamp"]),
        signal_type=str(payload["signal_type"]),
        symbol=str(payload["symbol"]).upper(),
        side=str(payload["side"]),
        broker_side=str(payload["broker_side"]),
        price=float(payload["price"]),
        reason=str(payload["reason"]),
    )


def signal_from_dataclass(signal: Signal) -> SignalRecord:
    return SignalRecord(
        timestamp=utc_timestamp(signal.timestamp),
        signal_type=signal.signal_type,
        symbol=signal.symbol.upper(),
        side=signal.side,
        broker_side=signal.broker_side,
        price=float(signal.price),
        reason=signal.reason,
    )


def load_live_signals(
    event_log: Path,
    trader_id: str,
    start: pd.Timestamp | None,
    end_exclusive: pd.Timestamp | None,
) -> tuple[list[SignalRecord], int]:
    signals: list[SignalRecord] = []
    skipped_lines = 0
    if not event_log.exists():
        raise RuntimeError(f"Missing event log: {event_log}")
    with event_log.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                skipped_lines += 1
                continue
            if event.get("event_type") != "signal":
                continue
            payload = event.get("payload")
            if not isinstance(payload, dict):
                continue
            if payload.get("trader_id") != trader_id:
                continue
            signal = signal_from_payload(payload)
            if in_window(signal.timestamp, start, end_exclusive):
                signals.append(signal)
    return signals, skipped_lines


def replay_signals(
    trader: dict[str, Any],
    archive_root: Path,
    start: pd.Timestamp | None,
    end_exclusive: pd.Timestamp | None,
) -> tuple[list[SignalRecord], dict[str, int]]:
    symbol = str(trader["symbol"]).upper()
    data = trader.get("data") if isinstance(trader.get("data"), dict) else {}
    renko_source = normalize_renko_source(data.get("renko_source"))
    strategy_config = build_strategy_config(trader)
    if isinstance(strategy_config, MeanReversionStrategyConfig):
        engine = MeanReversionStrategyEngine(strategy_config)
    else:
        engine = RenkoStrategyEngine(strategy_config)

    dates = archive_dates(archive_root, symbol, start, end_exclusive)
    source_ticks, source_stats = iter_source_ticks(archive_root, symbol, dates, renko_source)
    replayed: list[SignalRecord] = []
    for source_tick in source_ticks:
        _bricks, actions = engine.on_tick(source_tick)
        for signal, _intent in actions:
            record = signal_from_dataclass(signal)
            if in_window(record.timestamp, start, end_exclusive):
                replayed.append(record)
    return replayed, source_stats


def compare_signals(live: list[SignalRecord], replayed: list[SignalRecord], max_mismatches: int) -> dict[str, Any]:
    live_keys = [signal.key for signal in live]
    replay_keys = [signal.key for signal in replayed]
    matcher = SequenceMatcher(a=live_keys, b=replay_keys, autojunk=False)
    matched = sum(block.size for block in matcher.get_matching_blocks())
    denominator = max(len(live), len(replayed), 1)
    accuracy = matched / denominator
    live_recall = matched / len(live) if live else (1.0 if not replayed else 0.0)
    replay_precision = matched / len(replayed) if replayed else (1.0 if not live else 0.0)

    mismatches: list[dict[str, Any]] = []
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            continue
        mismatches.append(
            {
                "tag": tag,
                "live_index": [i1, i2],
                "replay_index": [j1, j2],
                "live": [format_signal(signal) for signal in live[i1:i2]],
                "replayed": [format_signal(signal) for signal in replayed[j1:j2]],
            }
        )
        if len(mismatches) >= max_mismatches:
            break

    return {
        "live_signal_count": len(live),
        "replayed_signal_count": len(replayed),
        "matched_signal_count": matched,
        "accuracy": accuracy,
        "live_recall": live_recall,
        "replay_precision": replay_precision,
        "mismatches": mismatches,
    }


def format_signal(signal: SignalRecord) -> dict[str, Any]:
    return {
        "timestamp": signal.timestamp.isoformat(),
        "signal_type": signal.signal_type,
        "symbol": signal.symbol,
        "side": signal.side,
        "broker_side": signal.broker_side,
        "price": round(signal.price, 6),
        "reason": signal.reason,
    }


def main() -> int:
    args = parse_args()
    start = parse_date(args.start_date)
    end_exclusive = parse_date(args.end_date) + pd.Timedelta(days=1) if args.end_date else None
    trader = load_trader(Path(args.config), args.trader_id)
    live, skipped_event_log_lines = load_live_signals(Path(args.event_log), args.trader_id, start, end_exclusive)
    replayed, source_stats = replay_signals(trader, Path(args.archive_root), start, end_exclusive)
    comparison = compare_signals(live, replayed, args.max_mismatches)
    report = {
        "trader_id": args.trader_id,
        "symbol": trader["symbol"],
        "start_date": args.start_date,
        "end_date": args.end_date,
        **source_stats,
        "skipped_event_log_lines": skipped_event_log_lines,
        "threshold": args.threshold,
        "passed": comparison["accuracy"] >= args.threshold,
        **comparison,
    }

    if args.json_output:
        output_path = Path(args.json_output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
