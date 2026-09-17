from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from fleet_runner import build_strategy_config
from aapl_e1_reversal_brick_sweep import (
    PricePoint,
    fetch_alpaca_trades_as_1s_closes,
    iso_z,
    load_csv_prices,
    write_prices_csv,
)
from strategy_engine import MeanReversionStrategyEngine, RenkoStrategyEngine
from trading_events import MarketBar


@dataclass
class Position:
    side: str
    qty: float
    price: float
    timestamp: pd.Timestamp


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay configured live strategies and compare with buy-and-hold.")
    parser.add_argument("--config", default="configs/traders.live.example.json")
    parser.add_argument("--start", default="2026-07-01")
    parser.add_argument("--end", default="2026-07-21")
    parser.add_argument("--feed", default="sip")
    parser.add_argument("--output", default="artifacts/live/july_strategy_replay_vs_buy_hold_20260721.json")
    parser.add_argument("--source-csv", action="append", default=[], help="SYMBOL=path or SYMBOL=path+path")
    parser.add_argument("--request-sleep", type=float, default=0.02)
    parser.add_argument("--max-pages-per-day", type=int, default=0)
    parser.add_argument("--slippage", type=float, default=0.0, help="Adverse dollars per fill.")
    parser.add_argument("--current-only", action="store_true")
    return parser.parse_args()


def parse_day(raw: str) -> date:
    return datetime.strptime(raw, "%Y-%m-%d").date()


def parse_source_csv(values: list[str]) -> dict[str, list[Path]]:
    out: dict[str, list[Path]] = {}
    for value in values:
        symbol, raw_paths = value.split("=", 1)
        out[symbol.strip().upper()] = [Path(item) for item in raw_paths.split("+") if item.strip()]
    return out


def cache_path(symbol: str, feed: str, start: date, end: date) -> Path:
    return Path("artifacts/market_data") / f"{symbol.upper()}_{feed}_{start}_{end}_1s_close.csv"


def load_or_fetch(symbol: str, start: date, end: date, args: argparse.Namespace, sources: dict[str, list[Path]]) -> tuple[str, list[PricePoint]]:
    if symbol in sources:
        points: list[PricePoint] = []
        for path in sources[symbol]:
            points.extend(load_csv_prices(path))
        points.sort(key=lambda item: item.timestamp)
        deduped: list[PricePoint] = []
        for point in points:
            if deduped and deduped[-1].timestamp == point.timestamp:
                deduped[-1] = point
            else:
                deduped.append(point)
        return "+".join(str(path) for path in sources[symbol]), deduped

    path = cache_path(symbol, args.feed, start, end)
    if path.exists():
        return str(path), load_csv_prices(path)
    points = fetch_alpaca_trades_as_1s_closes(
        symbol=symbol,
        start=start,
        end=end,
        feed=args.feed,
        request_sleep=args.request_sleep,
        max_pages_per_day=args.max_pages_per_day,
    )
    write_prices_csv(path, points)
    return str(path), points


def active_traders(config: dict[str, Any], current_only: bool) -> list[dict[str, Any]]:
    traders = []
    for trader in config.get("traders", []):
        if not isinstance(trader, dict):
            continue
        if trader.get("mode") != "live":
            continue
        if not trader.get("execution_enabled"):
            continue
        if current_only and not trader.get("enabled"):
            continue
        if str(trader.get("symbol", "")).upper() not in {"AAPL", "NVDA", "TSLA", "QQQ"}:
            continue
        session = trader.get("session") if isinstance(trader.get("session"), dict) else {}
        active_start = session.get("active_start_date")
        if active_start and str(active_start) > "2026-07-21":
            continue
        traders.append(trader)
    return traders


def in_active_window(point: PricePoint, trader: dict[str, Any], start: date, end: date) -> bool:
    local_day = pd.Timestamp(point.timestamp).tz_convert("America/New_York").date()
    if local_day < start or local_day > end:
        return False
    session = trader.get("session") if isinstance(trader.get("session"), dict) else {}
    active_start = session.get("active_start_date")
    active_end = session.get("active_end_date")
    if active_start and local_day.isoformat() < str(active_start):
        return False
    if active_end and local_day.isoformat() > str(active_end):
        return False
    return True


def trader_active_start(trader: dict[str, Any], default: date) -> date:
    session = trader.get("session") if isinstance(trader.get("session"), dict) else {}
    active_start = session.get("active_start_date")
    if active_start:
        parsed = parse_day(str(active_start))
        return max(default, parsed)
    return default


def close_price(position: Position, price: float, slippage: float) -> float:
    return price - slippage if position.side == "long" else price + slippage


def open_price(side: str, price: float, slippage: float) -> float:
    return price + slippage if side == "long" else price - slippage


def position_pnl(position: Position, price: float) -> float:
    if position.side == "long":
        return (price - position.price) * position.qty
    return (position.price - price) * position.qty


def replay(trader: dict[str, Any], points: list[PricePoint], start: date, end: date, slippage: float) -> dict[str, Any]:
    config = build_strategy_config(trader)
    engine = MeanReversionStrategyEngine(config) if config.strategy_type == "chop_mean_reversion" else RenkoStrategyEngine(config)
    symbol = str(trader["symbol"]).upper()
    filtered = [point for point in points if in_active_window(point, trader, start, end)]
    if len(filtered) < 2:
        return {"trader_id": trader["id"], "symbol": symbol, "error": "insufficient_points", "points": len(filtered)}

    position: Position | None = None
    realized = 0.0
    wins = losses = flats = 0
    closed_trades = 0
    actions = 0
    first = filtered[0]
    last = filtered[-1]

    for point in filtered:
        bar = MarketBar(timestamp=point.timestamp, symbol=symbol, open=point.close, high=point.close, low=point.close, close=point.close)
        for signal, intent in engine.on_bar(bar):
            actions += 1
            signal_side = "long" if intent.side == "buy" else "short"
            if intent.close_position:
                if position is None:
                    continue
                pnl = position_pnl(position, close_price(position, signal.price, slippage))
                realized += pnl
                closed_trades += 1
                wins += pnl > 0
                losses += pnl < 0
                flats += abs(pnl) < 1e-12
                position = None
                continue

            target_qty = float(intent.target_qty or intent.qty or config.qty or 1.0)
            if position is not None and position.side != signal_side:
                pnl = position_pnl(position, close_price(position, signal.price, slippage))
                realized += pnl
                closed_trades += 1
                wins += pnl > 0
                losses += pnl < 0
                flats += abs(pnl) < 1e-12
            position = Position(
                side=signal_side,
                qty=target_qty,
                price=open_price(signal_side, signal.price, slippage),
                timestamp=signal.timestamp,
            )

    unrealized = position_pnl(position, last.close) if position else 0.0
    total = realized + unrealized
    buy_hold = last.close - first.close
    if config.qty is not None:
        buy_hold *= float(config.qty)
        denominator = first.close * float(config.qty)
    else:
        denominator = float(config.notional or 10000.0)
        buy_hold = denominator * ((last.close / first.close) - 1.0)

    return {
        "trader_id": trader["id"],
        "symbol": symbol,
        "strategy_type": config.strategy_type,
        "start_at": iso_z(first.timestamp),
        "end_at": iso_z(last.timestamp),
        "start_price": first.close,
        "end_price": last.close,
        "points": len(filtered),
        "actions": actions,
        "closed_trades": closed_trades,
        "wins": wins,
        "losses": losses,
        "flats": flats,
        "open_position": None if position is None else {"side": position.side, "qty": position.qty, "entry_price": position.price},
        "realized_pnl": realized,
        "unrealized_pnl": unrealized,
        "total_pnl": total,
        "return_pct": 100.0 * total / denominator if denominator else None,
        "buy_hold_pnl": buy_hold,
        "buy_hold_return_pct": 100.0 * buy_hold / denominator if denominator else None,
        "outperformance_pnl": total - buy_hold,
        "outperformance_pct_points": (100.0 * total / denominator - 100.0 * buy_hold / denominator) if denominator else None,
    }


def main() -> int:
    args = parse_args()
    start = parse_day(args.start)
    end = parse_day(args.end)
    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    sources = parse_source_csv(args.source_csv)
    traders = active_traders(config, args.current_only)
    symbols = sorted({str(trader["symbol"]).upper() for trader in traders})
    symbol_starts = {
        symbol: min(
            trader_active_start(trader, start)
            for trader in traders
            if str(trader["symbol"]).upper() == symbol
        )
        for symbol in symbols
    }
    price_data: dict[str, tuple[str, list[PricePoint]]] = {}
    for symbol in symbols:
        fetch_start = symbol_starts[symbol]
        price_data[symbol] = load_or_fetch(symbol, fetch_start, end, args, sources)
    rows = []
    for trader in traders:
        symbol = str(trader["symbol"]).upper()
        rows.append(replay(trader, price_data[symbol][1], start, end, args.slippage))
    result = {
        "config": args.config,
        "start": args.start,
        "end": args.end,
        "slippage_per_fill": args.slippage,
        "sources": {symbol: source for symbol, (source, _) in price_data.items()},
        "traders": rows,
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
