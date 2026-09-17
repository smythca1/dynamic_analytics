from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fleet_runner import build_strategy_config
from strategy_engine import RenkoStrategyConfig, RenkoStrategyEngine
from trading_events import MarketTick, RenkoBrick, is_price_forming_trade


DATA_BASE_URL = "https://data.alpaca.markets"


@dataclass(frozen=True)
class PricePoint:
    timestamp: pd.Timestamp
    close: float


@dataclass(frozen=True)
class DataQualityStats:
    raw_rows: int = 0
    valid_rows: int = 0
    invalid_rows: int = 0
    non_finite_prices: int = 0
    out_of_order_rows: int = 0
    duplicate_timestamps: int = 0
    unchanged_prices: int = 0
    first_raw_timestamp: str | None = None
    last_raw_timestamp: str | None = None
    first_output_timestamp: str | None = None
    last_output_timestamp: str | None = None
    sorted_or_deduped: bool = False


@dataclass
class SimPosition:
    side: str
    qty: float
    entry_price: float
    entry_time: pd.Timestamp


@dataclass(frozen=True)
class PendingFill:
    order_id: str
    action: str
    side: str
    qty: float
    signal_time: pd.Timestamp
    signal_price: float
    order_submission_time: pd.Timestamp
    reason: str


@dataclass(frozen=True)
class SimFill:
    order_id: str
    action: str
    side: str
    qty: float
    signal_time: str
    signal_price: float
    order_submission_time: str
    fill_time: str
    fill_price: float
    reason: str
    pnl: float | None = None


@dataclass
class SweepRow:
    trader_id: str
    symbol: str
    brick_size: float
    execution_model: str
    slippage_per_fill: float
    renko_bricks: int
    signals: int
    simulated_fills: int
    pending_fills: int
    closed_trades: int
    wins: int
    losses: int
    flats: int
    gross_profit: float
    gross_loss: float
    realized_pnl: float
    unrealized_pnl: float
    total_pnl: float
    max_drawdown: float
    win_rate_pct: float
    profit_factor: float
    avg_trade_pnl: float
    open_side: str | None
    open_qty: float
    open_entry_price: float | None
    first_signal: str | None
    last_signal: str | None
    first_price_ts: str
    last_price_ts: str
    first_price: float
    last_price: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Sweep 1-second-close Renko brick sizes through the production RenkoStrategyEngine "
            "for E1 renko_reversal traders."
        )
    )
    parser.add_argument("--config", default="artifacts/production_current/traders.live.20260721.json")
    parser.add_argument("--symbols", default="", help="Comma-separated symbols. Default: enabled live E1 traders in config.")
    parser.add_argument("--trader-id", action="append", default=[], help="Specific trader id to sweep. May be repeated.")
    parser.add_argument("--start", default="2026-07-01")
    parser.add_argument("--end", default="2026-07-21", help="Inclusive date, YYYY-MM-DD.")
    parser.add_argument("--feed", default="sip", choices=["sip", "iex", "boats", "otc"])
    parser.add_argument("--source-csv", action="append", default=[], help="SYMBOL=path with timestamp,close columns.")
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--brick-sizes", default="", help="Comma-separated absolute brick sizes.")
    parser.add_argument("--min-pct", type=float, default=0.05, help="Min brick as percent of last price.")
    parser.add_argument("--max-pct", type=float, default=1.50, help="Max brick as percent of last price.")
    parser.add_argument("--step-pct", type=float, default=0.01, help="Brick percent step.")
    parser.add_argument(
        "--execution-model",
        choices=["idealized", "slippage", "next_event"],
        default="slippage",
        help="Fill model: idealized=signal threshold, slippage=signal threshold +/- slippage, next_event=next market event after signal.",
    )
    parser.add_argument("--slippage", type=float, default=0.03, help="Adverse dollars per simulated fill in slippage mode.")
    parser.add_argument("--write-renko-bars", choices=["none", "top", "all"], default="top")
    parser.add_argument("--top", type=int, default=25)
    parser.add_argument("--request-sleep", type=float, default=0.02)
    parser.add_argument("--max-pages-per-day", type=int, default=0)
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
        raise RuntimeError("Missing Alpaca credentials.")
    return {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}


def parse_day(raw: str) -> date:
    return datetime.strptime(raw, "%Y-%m-%d").date()


def parse_ts(raw: object) -> pd.Timestamp:
    text = str(raw)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return pd.Timestamp(parsed).tz_convert("UTC")


def iso_z(ts: pd.Timestamp | datetime) -> str:
    timestamp = pd.Timestamp(ts)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC").isoformat().replace("+00:00", "Z")


def session_bounds_utc(day: date) -> tuple[pd.Timestamp, pd.Timestamp]:
    start = pd.Timestamp(datetime(day.year, day.month, day.day, 9, 30), tz="America/New_York")
    end = pd.Timestamp(datetime(day.year, day.month, day.day, 16, 0), tz="America/New_York")
    return start.tz_convert("UTC"), end.tz_convert("UTC")


def cache_path(symbol: str, feed: str, day: date) -> Path:
    return Path("artifacts/market_data/daily") / f"{symbol.upper()}_{feed}_{day}_price_forming_1s_close.csv"


def quality_stats(raw: list[PricePoint], output: list[PricePoint], invalid_rows: int = 0, non_finite_prices: int = 0) -> DataQualityStats:
    out_of_order = 0
    duplicate_timestamps = 0
    unchanged = 0
    seen: set[pd.Timestamp] = set()
    previous_ts: pd.Timestamp | None = None
    previous_price: float | None = None
    for point in raw:
        if previous_ts is not None and point.timestamp < previous_ts:
            out_of_order += 1
        if point.timestamp in seen:
            duplicate_timestamps += 1
        if previous_price is not None and abs(point.close - previous_price) < 1e-12:
            unchanged += 1
        seen.add(point.timestamp)
        previous_ts = point.timestamp
        previous_price = point.close
    return DataQualityStats(
        raw_rows=len(raw) + invalid_rows,
        valid_rows=len(raw),
        invalid_rows=invalid_rows,
        non_finite_prices=non_finite_prices,
        out_of_order_rows=out_of_order,
        duplicate_timestamps=duplicate_timestamps,
        unchanged_prices=unchanged,
        first_raw_timestamp=iso_z(raw[0].timestamp) if raw else None,
        last_raw_timestamp=iso_z(raw[-1].timestamp) if raw else None,
        first_output_timestamp=iso_z(output[0].timestamp) if output else None,
        last_output_timestamp=iso_z(output[-1].timestamp) if output else None,
        sorted_or_deduped=bool(out_of_order or duplicate_timestamps),
    )


def merge_quality_stats(parts: list[DataQualityStats], output: list[PricePoint]) -> DataQualityStats:
    if not parts:
        return quality_stats(output, output)
    raw_rows = sum(part.raw_rows for part in parts)
    valid_rows = sum(part.valid_rows for part in parts)
    invalid_rows = sum(part.invalid_rows for part in parts)
    non_finite_prices = sum(part.non_finite_prices for part in parts)
    out_of_order_rows = sum(part.out_of_order_rows for part in parts)
    duplicate_timestamps = sum(part.duplicate_timestamps for part in parts)
    unchanged_prices = sum(part.unchanged_prices for part in parts)
    return DataQualityStats(
        raw_rows=raw_rows,
        valid_rows=valid_rows,
        invalid_rows=invalid_rows,
        non_finite_prices=non_finite_prices,
        out_of_order_rows=out_of_order_rows,
        duplicate_timestamps=duplicate_timestamps,
        unchanged_prices=unchanged_prices,
        first_raw_timestamp=parts[0].first_raw_timestamp,
        last_raw_timestamp=parts[-1].last_raw_timestamp,
        first_output_timestamp=iso_z(output[0].timestamp) if output else None,
        last_output_timestamp=iso_z(output[-1].timestamp) if output else None,
        sorted_or_deduped=any(part.sorted_or_deduped for part in parts),
    )


def load_csv_prices(path: Path) -> tuple[list[PricePoint], DataQualityStats]:
    rows: list[PricePoint] = []
    invalid_rows = 0
    non_finite_prices = 0
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fields = {name.lower(): name for name in (reader.fieldnames or [])}
        time_key = fields.get("timestamp") or fields.get("t") or fields.get("time")
        close_key = fields.get("close") or fields.get("c") or fields.get("price") or fields.get("p")
        if not time_key or not close_key:
            raise RuntimeError(f"{path} needs timestamp and close/price columns.")
        for row in reader:
            if not row.get(time_key) or not row.get(close_key):
                invalid_rows += 1
                continue
            try:
                close = float(row[close_key])
                if not math.isfinite(close):
                    non_finite_prices += 1
                    invalid_rows += 1
                    continue
                rows.append(PricePoint(timestamp=parse_ts(row[time_key]), close=close))
            except (TypeError, ValueError):
                invalid_rows += 1
    output = dedupe_points(rows)
    return output, quality_stats(rows, output, invalid_rows=invalid_rows, non_finite_prices=non_finite_prices)


def write_prices_csv(path: Path, points: list[PricePoint]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["timestamp", "close"])
        for point in points:
            writer.writerow([iso_z(point.timestamp), f"{point.close:.6f}"])


def request_json(url: str, headers: dict[str, str], params: dict[str, object], attempts: int = 5) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=60)
            if response.status_code in {408, 429, 500, 502, 503, 504} and attempt < attempts:
                time.sleep(min(2.0 * attempt, 10.0))
                continue
            if response.status_code >= 400:
                raise RuntimeError(f"Alpaca request failed {response.status_code}: {response.text[:500]}")
            payload = response.json()
            if not isinstance(payload, dict):
                raise RuntimeError(f"Unexpected Alpaca response: {payload!r}")
            return payload
        except requests.RequestException as exc:
            last_error = exc
            if attempt >= attempts:
                break
            time.sleep(min(2.0 * attempt, 10.0))
    raise RuntimeError(f"Alpaca request failed after {attempts} attempts: {last_error}") from last_error


def fetch_trade_day(symbol: str, day: date, feed: str, headers: dict[str, str], request_sleep: float, max_pages: int) -> tuple[list[PricePoint], DataQualityStats]:
    start, end = session_bounds_utc(day)
    params: dict[str, object] = {
        "start": iso_z(start),
        "end": iso_z(end),
        "feed": feed,
        "limit": 10000,
        "sort": "asc",
    }
    by_second: dict[pd.Timestamp, MarketTick] = {}
    pages = 0
    while True:
        payload = request_json(f"{DATA_BASE_URL}/v2/stocks/{symbol.upper()}/trades", headers, params)
        for trade in payload.get("trades") or []:
            tick = MarketTick.from_alpaca_trade(trade)
            if not is_price_forming_trade(tick):
                continue
            by_second[tick.timestamp.floor("s")] = tick
        pages += 1
        token = payload.get("next_page_token")
        if not token or (max_pages and pages >= max_pages):
            break
        params["page_token"] = token
        if request_sleep > 0:
            time.sleep(request_sleep)
    points = [PricePoint(timestamp=ts, close=tick.price) for ts, tick in sorted(by_second.items())]
    return points, quality_stats(points, points)


def load_or_fetch(symbol: str, start: date, end: date, args: argparse.Namespace, source_csvs: dict[str, Path]) -> tuple[str, list[PricePoint], DataQualityStats]:
    if symbol in source_csvs:
        points, stats = load_csv_prices(source_csvs[symbol])
        return str(source_csvs[symbol]), points, stats

    headers = alpaca_headers()
    points: list[PricePoint] = []
    stats_parts: list[DataQualityStats] = []
    current = start
    while current <= end:
        path = cache_path(symbol, args.feed, current)
        if path.exists():
            day_points, day_stats = load_csv_prices(path)
            print(f"{symbol} {current}: loaded {len(day_points)} cached price-forming 1s closes", flush=True)
        else:
            day_points, day_stats = fetch_trade_day(symbol, current, args.feed, headers, args.request_sleep, args.max_pages_per_day)
            write_prices_csv(path, day_points)
            print(f"{symbol} {current}: fetched {len(day_points)} price-forming 1s closes", flush=True)
        points.extend(day_points)
        stats_parts.append(day_stats)
        current += timedelta(days=1)
    output = dedupe_points(points)
    return f"daily:{args.feed}:{start}:{end}:price_forming_1s_close", output, merge_quality_stats(stats_parts, output)


def dedupe_points(points: list[PricePoint]) -> list[PricePoint]:
    out: list[PricePoint] = []
    for point in sorted(points, key=lambda row: row.timestamp):
        if out and out[-1].timestamp == point.timestamp:
            out[-1] = point
        else:
            out.append(point)
    return out


def parse_source_csvs(raw_values: list[str]) -> dict[str, Path]:
    out: dict[str, Path] = {}
    for raw in raw_values:
        symbol, path = raw.split("=", 1)
        out[symbol.strip().upper()] = Path(path.strip())
    return out


def selected_traders(config: dict[str, Any], args: argparse.Namespace) -> list[dict[str, Any]]:
    symbols = {item.strip().upper() for item in args.symbols.split(",") if item.strip()}
    ids = set(args.trader_id)
    out: list[dict[str, Any]] = []
    for trader in config.get("traders", []):
        if not isinstance(trader, dict):
            continue
        strategy = trader.get("strategy") if isinstance(trader.get("strategy"), dict) else {}
        if strategy.get("type") != "renko_reversal":
            continue
        if not trader.get("enabled") or not trader.get("execution_enabled") or trader.get("mode") != "live":
            continue
        if ids and trader.get("id") not in ids:
            continue
        if symbols and str(trader.get("symbol", "")).upper() not in symbols:
            continue
        out.append(trader)
    return out


def brick_grid(points: list[PricePoint], args: argparse.Namespace) -> list[float]:
    if args.brick_sizes.strip():
        return sorted({round(float(item), 6) for item in args.brick_sizes.split(",") if item.strip()})
    last = points[-1].close
    values: list[float] = []
    pct = args.min_pct
    while pct <= args.max_pct + 1e-12:
        values.append(round(last * pct / 100.0, 6))
        pct += args.step_pct
    return [value for value in sorted(set(values)) if value > 0]


def with_brick_size(trader: dict[str, Any], brick_size: float) -> dict[str, Any]:
    copied = json.loads(json.dumps(trader))
    copied["strategy"]["brick_size"] = brick_size
    copied["strategy"]["type"] = "renko_reversal"
    copied["strategy"]["entry_bricks"] = int(copied["strategy"].get("entry_bricks") or 1)
    copied.setdefault("data", {})["renko_source"] = "1s_close"
    return copied


def adverse_open_price(side: str, price: float, slippage: float) -> float:
    return price + slippage if side == "long" else price - slippage


def adverse_close_price(side: str, price: float, slippage: float) -> float:
    return price - slippage if side == "long" else price + slippage


def model_price(action: str, side: str, price: float, execution_model: str, slippage: float) -> float:
    if execution_model in {"idealized", "next_event"}:
        return price
    if action == "open":
        return adverse_open_price(side, price, slippage)
    return adverse_close_price(side, price, slippage)


def position_pnl(position: SimPosition, close_price: float) -> float:
    if position.side == "long":
        return (close_price - position.entry_price) * position.qty
    return (position.entry_price - close_price) * position.qty


def simulate(
    trader: dict[str, Any],
    points: list[PricePoint],
    brick_size: float,
    execution_model: str,
    slippage: float,
) -> tuple[SweepRow, list[RenkoBrick], list[SimFill]]:
    config = build_strategy_config(with_brick_size(trader, brick_size))
    if not isinstance(config, RenkoStrategyConfig) or config.strategy_type != "renko_reversal":
        raise RuntimeError(f"{trader.get('id')}: not an E1 renko_reversal trader.")

    engine = RenkoStrategyEngine(config)
    position: SimPosition | None = None
    pending: list[PendingFill] = []
    seen_fills: set[str] = set()
    fill_log: list[SimFill] = []
    realized = 0.0
    gross_profit = 0.0
    gross_loss = 0.0
    wins = losses = flats = closed_trades = simulated_fills = signals = 0
    first_signal: pd.Timestamp | None = None
    last_signal: pd.Timestamp | None = None
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    renko_bricks: list[RenkoBrick] = []

    def apply_fill(order: PendingFill, point: PricePoint) -> None:
        nonlocal position, realized, gross_profit, gross_loss
        nonlocal wins, losses, flats, closed_trades, simulated_fills, equity, peak, max_drawdown
        if order.order_id in seen_fills:
            return
        seen_fills.add(order.order_id)
        fill_price = model_price(order.action, order.side, point.close, execution_model, slippage)
        pnl: float | None = None
        if order.action == "close":
            if position is not None:
                pnl = position_pnl(position, fill_price)
                realized += pnl
                gross_profit += max(pnl, 0.0)
                gross_loss += min(pnl, 0.0)
                wins += pnl > 0
                losses += pnl < 0
                flats += abs(pnl) < 1e-12
                closed_trades += 1
                position = None
        elif order.action == "open":
            position = SimPosition(side=order.side, qty=order.qty, entry_price=fill_price, entry_time=point.timestamp)
        else:
            raise RuntimeError(f"Unsupported fill action: {order.action}")
        simulated_fills += 1
        fill_log.append(
            SimFill(
                order_id=order.order_id,
                action=order.action,
                side=order.side,
                qty=order.qty,
                signal_time=iso_z(order.signal_time),
                signal_price=order.signal_price,
                order_submission_time=iso_z(order.order_submission_time),
                fill_time=iso_z(point.timestamp),
                fill_price=fill_price,
                reason=order.reason,
                pnl=pnl,
            )
        )
        equity = realized
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity - peak)

    def submit_fill(order: PendingFill, point: PricePoint) -> None:
        if execution_model == "next_event":
            pending.append(order)
        else:
            apply_fill(order, PricePoint(timestamp=order.order_submission_time, close=order.signal_price))

    for index, point in enumerate(points):
        if pending:
            due = pending
            pending = []
            for order in due:
                if point.timestamp <= order.order_submission_time:
                    raise RuntimeError("next_event fill attempted without a later market event")
                apply_fill(order, point)

        # This is the same state transition used by RenkoStrategyEngine.on_tick after the symbol check.
        bricks, actions = engine._process_price(timestamp=point.timestamp, price=point.close)
        renko_bricks.extend(bricks)
        for action_index, (signal, intent) in enumerate(actions):
            signals += 1
            first_signal = first_signal or signal.timestamp
            last_signal = signal.timestamp
            next_side = "long" if intent.side == "buy" else "short"
            target_qty = float(intent.target_qty or config.qty or 1.0)

            if intent.close_position:
                if position is not None:
                    submit_fill(
                        PendingFill(
                            order_id=f"{intent.client_order_id}:close:{index}:{action_index}",
                            action="close",
                            side=position.side,
                            qty=position.qty,
                            signal_time=signal.timestamp,
                            signal_price=signal.price,
                            order_submission_time=point.timestamp,
                            reason=signal.reason,
                        ),
                        point,
                    )
            elif position is not None and position.side != next_side:
                close_qty = position.qty
                close_side = position.side
                submit_fill(
                    PendingFill(
                        order_id=f"{intent.client_order_id}:close:{index}:{action_index}",
                        action="close",
                        side=close_side,
                        qty=close_qty,
                        signal_time=signal.timestamp,
                        signal_price=signal.price,
                        order_submission_time=point.timestamp,
                        reason=signal.reason,
                    ),
                    point,
                )
                submit_fill(
                    PendingFill(
                        order_id=f"{intent.client_order_id}:open:{index}:{action_index}",
                        action="open",
                        side=next_side,
                        qty=target_qty,
                        signal_time=signal.timestamp,
                        signal_price=signal.price,
                        order_submission_time=point.timestamp,
                        reason=signal.reason,
                    ),
                    point,
                )
            elif position is None:
                submit_fill(
                    PendingFill(
                        order_id=f"{intent.client_order_id}:open:{index}:{action_index}",
                        action="open",
                        side=next_side,
                        qty=target_qty,
                        signal_time=signal.timestamp,
                        signal_price=signal.price,
                        order_submission_time=point.timestamp,
                        reason=signal.reason,
                    ),
                    point,
                )

    last_price = points[-1].close
    unrealized = 0.0 if position is None else position_pnl(position, last_price)
    total = realized + unrealized
    profit_factor = math.inf if abs(gross_loss) < 1e-12 and gross_profit > 0 else gross_profit / abs(gross_loss) if gross_loss else 0.0
    row = SweepRow(
        trader_id=str(trader["id"]),
        symbol=str(trader["symbol"]).upper(),
        brick_size=brick_size,
        execution_model=execution_model,
        slippage_per_fill=slippage if execution_model == "slippage" else 0.0,
        renko_bricks=len(renko_bricks),
        signals=signals,
        simulated_fills=simulated_fills,
        pending_fills=len(pending),
        closed_trades=closed_trades,
        wins=wins,
        losses=losses,
        flats=flats,
        gross_profit=gross_profit,
        gross_loss=gross_loss,
        realized_pnl=realized,
        unrealized_pnl=unrealized,
        total_pnl=total,
        max_drawdown=max_drawdown,
        win_rate_pct=100.0 * wins / closed_trades if closed_trades else 0.0,
        profit_factor=profit_factor,
        avg_trade_pnl=realized / closed_trades if closed_trades else 0.0,
        open_side=position.side if position else None,
        open_qty=position.qty if position else 0.0,
        open_entry_price=position.entry_price if position else None,
        first_signal=iso_z(first_signal) if first_signal is not None else None,
        last_signal=iso_z(last_signal) if last_signal is not None else None,
        first_price_ts=iso_z(points[0].timestamp),
        last_price_ts=iso_z(points[-1].timestamp),
        first_price=points[0].close,
        last_price=last_price,
    )
    return row, renko_bricks, fill_log


def write_rows(path: Path, rows: list[SweepRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(asdict(rows[0]).keys()) if rows else list(SweepRow.__annotations__))
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def safe_brick_name(brick_size: float) -> str:
    return f"{brick_size:.6f}".rstrip("0").rstrip(".").replace(".", "p")


def write_renko_bars(path: Path, bricks: list[RenkoBrick]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["timestamp", "symbol", "open", "close", "direction", "run_len", "brick_size", "trader_id"])
        for brick in bricks:
            writer.writerow(
                [
                    iso_z(brick.timestamp),
                    brick.symbol,
                    f"{brick.open:.6f}",
                    f"{brick.close:.6f}",
                    brick.direction,
                    brick.run_len,
                    f"{brick.brick_size:.6f}",
                    brick.trader_id or "",
                ]
            )


def write_fills(path: Path, fills: list[SimFill]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(SimFill.__annotations__))
        writer.writeheader()
        for fill in fills:
            writer.writerow(asdict(fill))


def main() -> int:
    args = parse_args()
    start = parse_day(args.start)
    end = parse_day(args.end)
    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    traders = selected_traders(config, args)
    if not traders:
        raise RuntimeError("No enabled live E1 renko_reversal traders matched the selection.")

    output_dir = Path(args.output_dir) if args.output_dir else Path("artifacts/backtests") / f"e1_production_parity_{start}_{end}"
    output_dir.mkdir(parents=True, exist_ok=True)
    source_csvs = parse_source_csvs(args.source_csv)

    metadata: dict[str, Any] = {
        "method": "production_parity_e1_renko_1s_close_v1",
        "promotion_eligible": False,
        "promotion_block_reason": "legacy parity sweep builds derived 1-second prices outside persisted Silver prepared-source contract fingerprints",
        "production_parity_notes": [
            "Uses price-forming Alpaca trades only.",
            "Collapses trades to one close per UTC second, matching production renko_source=1s_close input semantics.",
            "Calls the same RenkoStrategyEngine price transition used by fleet_runner after 1s-close source conversion.",
            "Models E1 reversals as two fills: close existing side, then open target side.",
            "Does not simulate broker/network outages, risk-block rejections, or latency unless those are represented in the input data.",
        ],
        "config": args.config,
        "start": args.start,
        "end": args.end,
        "feed": args.feed,
        "execution_model": args.execution_model,
        "slippage_per_fill": args.slippage if args.execution_model == "slippage" else 0.0,
        "sources": {},
    }

    all_rows: list[SweepRow] = []
    for trader in traders:
        symbol = str(trader["symbol"]).upper()
        source, points, dq_stats = load_or_fetch(symbol, start, end, args, source_csvs)
        if len(points) < 2:
            raise RuntimeError(f"{symbol}: need at least two 1-second close points.")
        metadata["sources"][symbol] = {
            "source": source,
            "points": len(points),
            "first": iso_z(points[0].timestamp),
            "last": iso_z(points[-1].timestamp),
            "data_quality": asdict(dq_stats),
        }
        rows_and_outputs = [
            simulate(trader, points, brick, args.execution_model, args.slippage) for brick in brick_grid(points, args)
        ]
        rows_and_outputs.sort(key=lambda item: (item[0].total_pnl, item[0].realized_pnl, -item[0].max_drawdown), reverse=True)
        rows = [item[0] for item in rows_and_outputs]
        all_rows.extend(rows)
        trader_dir = output_dir / symbol.lower() / str(trader["id"])
        write_rows(trader_dir / "sweep.csv", rows)
        write_rows(trader_dir / f"top{args.top}.csv", rows[: args.top])

        if args.write_renko_bars != "none":
            selected = set(row.brick_size for row in (rows if args.write_renko_bars == "all" else rows[: args.top]))
            outputs_by_brick = {row.brick_size: (bars, fills) for row, bars, fills in rows_and_outputs if row.brick_size in selected}
            for brick, (bars, fills) in outputs_by_brick.items():
                write_renko_bars(trader_dir / "renko_bars" / f"brick_{safe_brick_name(brick)}.csv", bars)
                write_fills(trader_dir / "fills" / f"brick_{safe_brick_name(brick)}.csv", fills)

        best = rows[0]
        print(
            f"{symbol} {trader['id']}: best brick={best.brick_size:.6f} "
            f"total={best.total_pnl:.2f} realized={best.realized_pnl:.2f} "
            f"signals={best.signals} fills={best.simulated_fills}",
            flush=True,
        )

    all_rows.sort(key=lambda row: (row.symbol, row.trader_id, -row.total_pnl))
    write_rows(output_dir / "combined_sweep.csv", all_rows)
    (output_dir / "metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"output={output_dir}", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
