from __future__ import annotations

import argparse
import csv
import json
import math
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv


DATA_BASE_URL = "https://data.alpaca.markets"
EASTERN = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class PricePoint:
    timestamp: datetime
    close: float


@dataclass
class SweepResult:
    symbol: str
    brick_size: float
    brick_pct_of_ltp: float
    renko_bricks: int
    entries: int
    closed_trades: int
    wins: int
    losses: int
    flats: int
    gross_profit: float
    gross_loss: float
    net_pnl: float
    return_pct: float
    max_drawdown: float
    max_drawdown_pct: float
    win_rate_pct: float
    profit_factor: float
    avg_trade_pnl: float
    trades_per_day: float
    open_position: bool
    final_forced_exit: bool
    neighbor_count: int = 0
    neighbor_profitability_pct: float = 0.0
    neighbor_avg_return_pct: float = 0.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Sweep Renko brick sizes for an E1 reversal strategy using "
            "1-second closes aggregated from Alpaca stock trades."
        )
    )
    parser.add_argument("--symbol", default="AAPL")
    parser.add_argument("--start", default="2026-01-01", help="Backtest start date, YYYY-MM-DD.")
    parser.add_argument("--end", default=str(date.today()), help="Backtest end date, YYYY-MM-DD.")
    parser.add_argument("--feed", default="sip", choices=["sip", "iex", "boats", "otc"])
    parser.add_argument("--csv", help="Use an existing timestamp,close CSV instead of fetching Alpaca trades.")
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--min-pct", type=float, default=0.05, help="Minimum brick size as percent of last price.")
    parser.add_argument("--max-pct", type=float, default=1.50, help="Maximum brick size as percent of last price.")
    parser.add_argument("--step-pct", type=float, default=0.01, help="Brick-size percent grid step.")
    parser.add_argument("--brick-sizes", default="", help="Comma-separated absolute brick sizes, e.g. 0.25,0.30.")
    parser.add_argument("--initial-capital", type=float, default=10000.0)
    parser.add_argument("--fixed-notional", type=float, default=10000.0)
    parser.add_argument("--slippage-ticks", type=float, default=3.0)
    parser.add_argument("--tick-size", type=float, default=0.01)
    parser.add_argument("--whole-shares", action="store_true")
    parser.add_argument("--entry-start", default="09:31:00", help="Entry session start in America/New_York.")
    parser.add_argument("--entry-end", default="16:00:00", help="Entry session end in America/New_York.")
    parser.add_argument("--anchor-mode", default="grid", choices=["grid", "first_price"])
    parser.add_argument("--leave-open", action="store_true", help="Do not force-close the final open position.")
    parser.add_argument("--max-pages-per-day", type=int, default=0, help="Debug/data cap; 0 means unlimited.")
    parser.add_argument("--request-sleep", type=float, default=0.02)
    parser.add_argument("--top", type=int, default=25)
    return parser.parse_args()


def parse_day(raw: str) -> date:
    return datetime.strptime(raw, "%Y-%m-%d").date()


def parse_hms(raw: str) -> tuple[int, int, int]:
    parts = raw.split(":")
    if len(parts) != 3:
        raise ValueError(f"Expected HH:MM:SS time, got {raw!r}")
    return int(parts[0]), int(parts[1]), int(parts[2])


def parse_timestamp(raw: str) -> datetime:
    ts = raw.strip()
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    parsed = datetime.fromisoformat(ts)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso_z(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def alpaca_headers() -> dict[str, str]:
    load_dotenv()
    key = first_env("APCA_API_KEY_ID", "ALPACA_API_KEY_ID", "ALPACA_API_KEY")
    secret = first_env("APCA_API_SECRET_KEY", "ALPACA_API_SECRET_KEY", "ALPACA_SECRET_KEY")
    if not key or not secret:
        raise RuntimeError("Missing Alpaca API credentials in environment or .env.")
    return {
        "APCA-API-KEY-ID": key,
        "APCA-API-SECRET-KEY": secret,
    }


def first_env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def default_output_dir(symbol: str, start: date, end: date) -> Path:
    return Path("artifacts") / "backtests" / f"{symbol.lower()}_e1_reversal_1s_{start}_{end}"


def cache_path(symbol: str, feed: str, start: date, end: date) -> Path:
    return Path("artifacts") / "market_data" / f"{symbol.upper()}_{feed}_{start}_{end}_1s_close.csv"


def daily_cache_path(symbol: str, feed: str, day: date) -> Path:
    return Path("artifacts") / "market_data" / "daily" / f"{symbol.upper()}_{feed}_{day}_1s_close.csv"


def load_csv_prices(path: Path) -> list[PricePoint]:
    rows: list[PricePoint] = []
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise RuntimeError(f"{path} has no CSV header.")
        time_key = first_present(reader.fieldnames, ["timestamp", "t", "time", "datetime"])
        close_key = first_present(reader.fieldnames, ["close", "c", "price", "p"])
        if not time_key or not close_key:
            raise RuntimeError(f"{path} must have timestamp and close/price columns.")
        for row in reader:
            if not row.get(time_key) or not row.get(close_key):
                continue
            rows.append(PricePoint(parse_timestamp(row[time_key]), float(row[close_key])))
    rows.sort(key=lambda point: point.timestamp)
    return rows


def first_present(names: list[str], candidates: list[str]) -> str | None:
    lowered = {name.lower(): name for name in names}
    for candidate in candidates:
        if candidate.lower() in lowered:
            return lowered[candidate.lower()]
    return None


def write_prices_csv(path: Path, points: list[PricePoint]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["timestamp", "close"])
        for point in points:
            writer.writerow([iso_z(point.timestamp), f"{point.close:.6f}"])


def session_bounds_utc(day: date, start_hms: str, end_hms: str) -> tuple[datetime, datetime]:
    sh, sm, ss = parse_hms(start_hms)
    eh, em, es = parse_hms(end_hms)
    start = datetime(day.year, day.month, day.day, sh, sm, ss, tzinfo=EASTERN)
    end = datetime(day.year, day.month, day.day, eh, em, es, tzinfo=EASTERN)
    return start.astimezone(timezone.utc), end.astimezone(timezone.utc)


def fetch_alpaca_trades_as_1s_closes(
    symbol: str,
    start: date,
    end: date,
    feed: str,
    request_sleep: float,
    max_pages_per_day: int,
) -> list[PricePoint]:
    headers = alpaca_headers()
    points: list[PricePoint] = []
    current = start
    while current <= end:
        day_cache = daily_cache_path(symbol, feed, current)
        if day_cache.exists():
            day_points = load_csv_prices(day_cache)
            print(f"{current}: loaded {len(day_points)} cached 1s closes", flush=True)
        else:
            day_points = fetch_alpaca_trade_day(
                symbol=symbol,
                day=current,
                feed=feed,
                headers=headers,
                request_sleep=request_sleep,
                max_pages=max_pages_per_day,
            )
            write_prices_csv(day_cache, day_points)
        points.extend(day_points)
        print(f"{current}: {len(day_points)} 1s closes", flush=True)
        current += timedelta(days=1)
    points.sort(key=lambda point: point.timestamp)
    return dedupe_points(points)


def fetch_alpaca_trade_day(
    symbol: str,
    day: date,
    feed: str,
    headers: dict[str, str],
    request_sleep: float,
    max_pages: int,
) -> list[PricePoint]:
    start_ts, end_ts = session_bounds_utc(day, "09:30:00", "16:00:00")
    url = f"{DATA_BASE_URL}/v2/stocks/{symbol.upper()}/trades"
    params: dict[str, object] = {
        "start": iso_z(start_ts),
        "end": iso_z(end_ts),
        "feed": feed,
        "limit": 10000,
        "sort": "asc",
    }
    by_second: dict[datetime, float] = {}
    pages = 0
    while True:
        payload = get_json_with_retries(url, headers=headers, params=params)
        trades = payload.get("trades") or []
        for trade in trades:
            raw_ts = trade.get("t")
            raw_price = trade.get("p")
            if raw_ts is None or raw_price is None:
                continue
            bucket = parse_timestamp(raw_ts).replace(microsecond=0)
            by_second[bucket] = float(raw_price)
        pages += 1
        if pages == 1 or pages % 25 == 0:
            print(f"{day}: fetched {pages} trade pages, {len(by_second)} 1s buckets", flush=True)
        token = payload.get("next_page_token")
        if not token:
            break
        if max_pages and pages >= max_pages:
            break
        params["page_token"] = token
        if request_sleep > 0:
            time.sleep(request_sleep)
    return [PricePoint(ts, close) for ts, close in sorted(by_second.items())]


def get_json_with_retries(url: str, *, headers: dict[str, str], params: dict[str, object], attempts: int = 5) -> dict:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=60)
            if response.status_code in {408, 429, 500, 502, 503, 504} and attempt < attempts:
                time.sleep(min(2.0 * attempt, 10.0))
                continue
            if response.status_code >= 400:
                raise RuntimeError(f"Alpaca trades request failed {response.status_code}: {response.text[:500]}")
            return response.json()
        except requests.RequestException as exc:
            last_error = exc
            if attempt >= attempts:
                break
            time.sleep(min(2.0 * attempt, 10.0))
    raise RuntimeError(f"Alpaca trades request failed after {attempts} attempts: {last_error}") from last_error


def dedupe_points(points: list[PricePoint]) -> list[PricePoint]:
    out: list[PricePoint] = []
    for point in points:
        if out and out[-1].timestamp == point.timestamp:
            out[-1] = point
        else:
            out.append(point)
    return out


def brick_grid(points: list[PricePoint], args: argparse.Namespace) -> list[float]:
    if args.brick_sizes.strip():
        return sorted({round(float(raw), 6) for raw in args.brick_sizes.split(",") if raw.strip()})
    ltp = points[-1].close
    values: list[float] = []
    pct = args.min_pct
    while pct <= args.max_pct + 1e-12:
        values.append(round(ltp * pct / 100.0, 6))
        pct += args.step_pct
    return sorted({value for value in values if value > 0})


def initial_brick_close(price: float, brick_size: float, anchor_mode: str) -> float:
    if anchor_mode == "first_price":
        return price
    return math.floor(price / brick_size) * brick_size


def in_entry_session(ts: datetime, start_hms: str, end_hms: str) -> bool:
    local = ts.astimezone(EASTERN)
    sh, sm, ss = parse_hms(start_hms)
    eh, em, es = parse_hms(end_hms)
    current = local.hour * 3600 + local.minute * 60 + local.second
    start = sh * 3600 + sm * 60 + ss
    end = eh * 3600 + em * 60 + es
    return start <= current < end if start <= end else current >= start or current < end


def trade_qty(notional: float, signal_price: float, whole_shares: bool) -> float:
    qty = notional / signal_price if signal_price > 0 else 0.0
    return math.floor(qty) if whole_shares else qty


def simulate(points: list[PricePoint], brick_size: float, args: argparse.Namespace, trading_days: int) -> SweepResult:
    symbol = args.symbol.upper()
    slippage = args.slippage_ticks * args.tick_size
    ltp = points[-1].close
    last_close = initial_brick_close(points[0].close, brick_size, args.anchor_mode)
    run_dir = 0
    run_len = 0
    bricks = 0

    side = 0
    qty = 0.0
    entry_fill = 0.0
    entries = 0
    closed = 0
    wins = 0
    losses = 0
    flats = 0
    gross_profit = 0.0
    gross_loss = 0.0
    realized = 0.0
    peak_equity = 0.0
    max_drawdown = 0.0
    last_trade_ts: datetime | None = None

    def mark_equity(price: float) -> None:
        nonlocal peak_equity, max_drawdown
        unrealized = (price - entry_fill) * side * qty if side else 0.0
        equity = realized + unrealized
        peak_equity = max(peak_equity, equity)
        max_drawdown = max(max_drawdown, peak_equity - equity)

    def close_position(fill_price: float) -> None:
        nonlocal side, qty, entry_fill, closed, wins, losses, flats, gross_profit, gross_loss, realized
        if side == 0:
            return
        pnl = (fill_price - entry_fill) * side * qty
        realized += pnl
        closed += 1
        if pnl > 0:
            wins += 1
            gross_profit += pnl
        elif pnl < 0:
            losses += 1
            gross_loss += pnl
        else:
            flats += 1
        side = 0
        qty = 0.0
        entry_fill = 0.0

    for point in points[1:]:
        mark_equity(point.close)
        while True:
            move = point.close - last_close
            direction = 0
            close_bricks = 1
            if run_dir == 1:
                if move >= brick_size:
                    direction = 1
                elif move <= -2.0 * brick_size:
                    direction = -1
                    close_bricks = 2
            elif run_dir == -1:
                if move <= -brick_size:
                    direction = -1
                elif move >= 2.0 * brick_size:
                    direction = 1
                    close_bricks = 2
            else:
                if move >= brick_size:
                    direction = 1
                elif move <= -brick_size:
                    direction = -1
            if direction == 0:
                break

            last_close += direction * brick_size * close_bricks
            bricks += 1
            if direction == run_dir:
                run_len += 1
            else:
                run_dir = direction
                run_len = 1

            can_trade_clock_bar = last_trade_ts is None or point.timestamp != last_trade_ts
            if run_len != 1 or not can_trade_clock_bar or not in_entry_session(point.timestamp, args.entry_start, args.entry_end):
                continue

            if direction == 1 and side <= 0:
                fill = last_close + slippage
                close_position(fill)
                qty = trade_qty(args.fixed_notional, last_close, args.whole_shares)
                if qty > 0:
                    side = 1
                    entry_fill = fill
                    entries += 1
                    last_trade_ts = point.timestamp
            elif direction == -1 and side >= 0:
                fill = last_close - slippage
                close_position(fill)
                qty = trade_qty(args.fixed_notional, last_close, args.whole_shares)
                if qty > 0:
                    side = -1
                    entry_fill = fill
                    entries += 1
                    last_trade_ts = point.timestamp
            mark_equity(last_close)

    final_forced_exit = False
    if side and not args.leave_open:
        final_fill = points[-1].close - slippage if side == 1 else points[-1].close + slippage
        close_position(final_fill)
        final_forced_exit = True

    mark_equity(points[-1].close)
    win_rate = 100.0 * wins / closed if closed else 0.0
    profit_factor = gross_profit / abs(gross_loss) if gross_loss < 0 else (math.inf if gross_profit > 0 else 0.0)
    avg = realized / closed if closed else 0.0
    return SweepResult(
        symbol=symbol,
        brick_size=brick_size,
        brick_pct_of_ltp=100.0 * brick_size / ltp,
        renko_bricks=bricks,
        entries=entries,
        closed_trades=closed,
        wins=wins,
        losses=losses,
        flats=flats,
        gross_profit=gross_profit,
        gross_loss=gross_loss,
        net_pnl=realized,
        return_pct=100.0 * realized / args.initial_capital,
        max_drawdown=max_drawdown,
        max_drawdown_pct=100.0 * max_drawdown / args.initial_capital,
        win_rate_pct=win_rate,
        profit_factor=profit_factor,
        avg_trade_pnl=avg,
        trades_per_day=closed / max(1, trading_days),
        open_position=side != 0,
        final_forced_exit=final_forced_exit,
    )


def add_neighbor_stats(rows: list[SweepResult], width: int = 2) -> None:
    by_brick = sorted(rows, key=lambda row: row.brick_size)
    for idx, row in enumerate(by_brick):
        neighbors = [
            other
            for other in by_brick[max(0, idx - width) : min(len(by_brick), idx + width + 1)]
            if other is not row
        ]
        row.neighbor_count = len(neighbors)
        if neighbors:
            row.neighbor_profitability_pct = 100.0 * sum(1 for other in neighbors if other.net_pnl > 0) / len(neighbors)
            row.neighbor_avg_return_pct = sum(other.return_pct for other in neighbors) / len(neighbors)


def write_results(path: Path, rows: list[SweepResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "symbol",
        "brick_size",
        "brick_pct_of_ltp",
        "renko_bricks",
        "entries",
        "closed_trades",
        "wins",
        "losses",
        "flats",
        "win_rate_pct",
        "profit_factor",
        "gross_profit",
        "gross_loss",
        "net_pnl",
        "return_pct",
        "avg_trade_pnl",
        "max_drawdown",
        "max_drawdown_pct",
        "trades_per_day",
        "neighbor_count",
        "neighbor_profitability_pct",
        "neighbor_avg_return_pct",
        "open_position",
        "final_forced_exit",
    ]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: format_value(getattr(row, name)) for name in fieldnames})


def heatmap_color(value: float) -> str:
    value = min(100.0, max(0.0, value))
    if value <= 50.0:
        ratio = value / 50.0
        red, green, blue = 220, int(70 + ratio * 115), 70
    else:
        ratio = (value - 50.0) / 50.0
        red, green, blue = int(220 - ratio * 165), 185, 80
    return f"#{red:02x}{green:02x}{blue:02x}"


def write_neighbor_heatmap(path: Path, rows: list[SweepResult], *, title: str, columns: int = 24) -> None:
    ordered = sorted(rows, key=lambda row: row.brick_pct_of_ltp)
    if not ordered:
        return

    cell_w = 54
    cell_h = 52
    margin_x = 54
    start_y = 118
    row_count = math.ceil(len(ordered) / columns)
    width = margin_x * 2 + columns * cell_w
    height = start_y + row_count * cell_h + 82

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as out:
        out.write(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">\n')
        out.write('<rect width="100%" height="100%" fill="#f7faf7"/>\n')
        out.write(
            "<style>"
            "text{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Arial,sans-serif;fill:#18211d}"
            ".title{font-size:22px;font-weight:780}.sub{font-size:12px;fill:#5a665f}"
            ".cell-main{font-size:9px;font-weight:760}.cell-sub{font-size:8px;font-weight:650}"
            ".axis{font-size:10px;font-weight:700;fill:#516059}.legend{font-size:10px;font-weight:700}"
            "</style>\n"
        )
        out.write(f'<text x="28" y="34" class="title">{title}</text>\n')
        out.write(
            '<text x="28" y="56" class="sub">'
            "Cells are ordered by brick size. Color = neighbor profitability. "
            "Cell text: brick %, neighbor %, return %."
            "</text>\n"
        )

        legend_x = 28
        legend_y = 78
        legend_w = 260
        for i in range(101):
            color = heatmap_color(float(i))
            x = legend_x + i * (legend_w / 101.0)
            out.write(f'<rect x="{x:.2f}" y="{legend_y}" width="{legend_w / 101.0 + 0.6:.2f}" height="12" fill="{color}"/>\n')
        out.write(f'<text x="{legend_x}" y="{legend_y + 30}" class="legend">0% neighbors profitable</text>\n')
        out.write(f'<text x="{legend_x + legend_w}" y="{legend_y + 30}" text-anchor="end" class="legend">100%</text>\n')

        for idx, row in enumerate(ordered):
            grid_row = idx // columns
            grid_col = idx % columns
            x = margin_x + grid_col * cell_w
            y = start_y + grid_row * cell_h
            color = heatmap_color(row.neighbor_profitability_pct)
            stroke = "#14532d" if row.net_pnl > 0 else "#7f1d1d"
            stroke_w = 1.8 if row.net_pnl > 0 else 1.0
            out.write(
                f'<rect x="{x}" y="{y}" width="{cell_w - 4}" height="{cell_h - 4}" '
                f'rx="5" fill="{color}" stroke="{stroke}" stroke-width="{stroke_w}">\n'
            )
            out.write(
                "<title>"
                f"Brick ${row.brick_size:.4f} ({row.brick_pct_of_ltp:.3f}%). "
                f"Neighbor profitability {row.neighbor_profitability_pct:.1f}%. "
                f"Neighbor avg return {row.neighbor_avg_return_pct:.2f}%. "
                f"Return {row.return_pct:.2f}%. Trades {row.closed_trades}."
                "</title>\n"
            )
            out.write("</rect>\n")
            text_x = x + (cell_w - 4) / 2
            out.write(f'<text x="{text_x:.1f}" y="{y + 14}" text-anchor="middle" class="cell-main">{row.brick_pct_of_ltp:.2f}%</text>\n')
            out.write(f'<text x="{text_x:.1f}" y="{y + 28}" text-anchor="middle" class="cell-sub">N {row.neighbor_profitability_pct:.0f}%</text>\n')
            out.write(f'<text x="{text_x:.1f}" y="{y + 41}" text-anchor="middle" class="cell-sub">{row.return_pct:+.1f}%</text>\n')

        out.write(f'<text x="{margin_x}" y="{height - 24}" class="axis">Green outline = positive net PnL. Red outline = negative net PnL.</text>\n')
        out.write("</svg>\n")


def format_value(value: object) -> object:
    if isinstance(value, float):
        if math.isinf(value):
            return "inf"
        return f"{value:.6f}"
    return value


def count_trading_days(points: list[PricePoint]) -> int:
    return len({point.timestamp.astimezone(EASTERN).date() for point in points})


def main() -> int:
    args = parse_args()
    args.symbol = args.symbol.upper()
    start = parse_day(args.start)
    end = parse_day(args.end)
    if start > end:
        raise RuntimeError("--start must be <= --end")

    out_dir = Path(args.output_dir) if args.output_dir else default_output_dir(args.symbol, start, end)
    if args.csv:
        source_path = Path(args.csv)
        points = load_csv_prices(source_path)
    else:
        source_path = cache_path(args.symbol, args.feed, start, end)
        if source_path.exists():
            points = load_csv_prices(source_path)
        else:
            points = fetch_alpaca_trades_as_1s_closes(
                symbol=args.symbol,
                start=start,
                end=end,
                feed=args.feed,
                request_sleep=args.request_sleep,
                max_pages_per_day=args.max_pages_per_day,
            )
            write_prices_csv(source_path, points)

    if len(points) < 2:
        raise RuntimeError("Need at least two 1-second close points to backtest.")

    trading_days = count_trading_days(points)
    rows = [simulate(points, brick, args, trading_days) for brick in brick_grid(points, args)]
    add_neighbor_stats(rows)
    ranked = sorted(rows, key=lambda row: (row.net_pnl, row.neighbor_avg_return_pct, row.closed_trades), reverse=True)

    write_results(out_dir / "renko_reversal_brick_sweep.csv", ranked)
    write_results(out_dir / f"top{args.top}_brick_sizes.csv", ranked[: args.top])
    write_neighbor_heatmap(
        out_dir / "neighbor_profitability_heatmap.svg",
        rows,
        title=f"{args.symbol} E1 Reversal Neighbor Profitability Heatmap",
    )
    metadata = {
        "method": "e1_reversal_1s_trade_aggregated_v1",
        "symbol": args.symbol,
        "source": str(source_path),
        "start": args.start,
        "end": args.end,
        "feed": args.feed,
        "price_points": len(points),
        "trading_days": trading_days,
        "first_timestamp": iso_z(points[0].timestamp),
        "last_timestamp": iso_z(points[-1].timestamp),
        "ltp_reference": points[-1].close,
        "initial_capital": args.initial_capital,
        "fixed_notional": args.fixed_notional,
        "slippage_per_order": args.slippage_ticks * args.tick_size,
        "entry_session_eastern": f"{args.entry_start}-{args.entry_end}",
        "anchor_mode": args.anchor_mode,
        "leave_open": args.leave_open,
        "best_by_net_pnl": ranked[0].__dict__,
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "metadata.json").write_text(json.dumps(metadata, indent=2, default=str) + "\n")

    print(f"source={source_path}", flush=True)
    print(f"output={out_dir}", flush=True)
    print(f"price_points={len(points)} trading_days={trading_days}", flush=True)
    print("top brick sizes by net PnL:", flush=True)
    for row in ranked[: args.top]:
        pf = "inf" if math.isinf(row.profit_factor) else f"{row.profit_factor:.2f}"
        print(
            f"{row.brick_size:.4f} ({row.brick_pct_of_ltp:.3f}%) "
            f"net=${row.net_pnl:.2f} return={row.return_pct:.2f}% "
            f"dd={row.max_drawdown_pct:.2f}% trades={row.closed_trades} "
            f"win={row.win_rate_pct:.1f}% pf={pf} "
            f"neighbor_avg={row.neighbor_avg_return_pct:.2f}%",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
