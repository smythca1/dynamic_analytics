from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from aapl_e1_reversal_brick_sweep import (
    PricePoint,
    cache_path,
    fetch_alpaca_trades_as_1s_closes,
    iso_z,
    load_csv_prices,
    write_prices_csv,
)


EASTERN = ZoneInfo("America/New_York")


@dataclass
class SweepResult:
    symbol: str
    mode: str
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
    session_force_exits: int
    neighbor_count: int = 0
    neighbor_profitability_pct: float = 0.0
    neighbor_avg_return_pct: float = 0.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run E1 Renko reversal brick-size sweeps for multiple symbols and session modes."
    )
    parser.add_argument("--symbols", default="AAPL,NVDA,TSLA")
    parser.add_argument("--start", default="2026-07-07")
    parser.add_argument("--end", default="2026-07-20")
    parser.add_argument("--feed", default="sip", choices=["sip", "iex", "boats", "otc"])
    parser.add_argument(
        "--source-csv",
        action="append",
        default=[],
        help="Override a symbol source with SYMBOL=path or SYMBOL=path+path. May be repeated.",
    )
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--min-pct", type=float, default=0.05)
    parser.add_argument("--max-pct", type=float, default=1.50)
    parser.add_argument("--step-pct", type=float, default=0.01)
    parser.add_argument("--brick-sizes", default="")
    parser.add_argument("--initial-capital", type=float, default=10000.0)
    parser.add_argument("--fixed-notional", type=float, default=10000.0)
    parser.add_argument("--slippage-ticks", type=float, default=3.0)
    parser.add_argument("--tick-size", type=float, default=0.01)
    parser.add_argument("--whole-shares", action="store_true")
    parser.add_argument("--entry-start", default="09:30:00")
    parser.add_argument("--entry-end", default="15:45:00")
    parser.add_argument("--force-flat", default="15:45:00")
    parser.add_argument("--anchor-mode", default="grid", choices=["grid", "first_price"])
    parser.add_argument("--request-sleep", type=float, default=0.02)
    parser.add_argument("--max-pages-per-day", type=int, default=0)
    parser.add_argument("--top", type=int, default=15)
    return parser.parse_args()


def parse_day(raw: str) -> date:
    return datetime.strptime(raw, "%Y-%m-%d").date()


def parse_hms(raw: str) -> tuple[int, int, int]:
    parts = raw.split(":")
    if len(parts) != 3:
        raise ValueError(f"Expected HH:MM:SS, got {raw!r}")
    return int(parts[0]), int(parts[1]), int(parts[2])


def seconds_since_midnight(ts: datetime) -> int:
    local = ts.astimezone(EASTERN)
    return local.hour * 3600 + local.minute * 60 + local.second


def hms_to_seconds(raw: str) -> int:
    h, m, s = parse_hms(raw)
    return h * 3600 + m * 60 + s


def in_entry_session(ts: datetime, start_hms: str, end_hms: str) -> bool:
    current = seconds_since_midnight(ts)
    start = hms_to_seconds(start_hms)
    end = hms_to_seconds(end_hms)
    if start <= end:
        return start <= current < end
    return current >= start or current < end


def force_flat_due(ts: datetime, force_flat_hms: str) -> bool:
    return seconds_since_midnight(ts) >= hms_to_seconds(force_flat_hms)


def initial_brick_close(price: float, brick_size: float, anchor_mode: str) -> float:
    if anchor_mode == "first_price":
        return price
    return math.floor(price / brick_size) * brick_size


def trade_qty(notional: float, signal_price: float, whole_shares: bool) -> float:
    if signal_price <= 0:
        return 0.0
    qty = notional / signal_price
    return math.floor(qty) if whole_shares else qty


def close_fill(price: float, side: int, slippage: float) -> float:
    if side > 0:
        return price - slippage
    if side < 0:
        return price + slippage
    return price


def brick_grid(points: list[PricePoint], args: argparse.Namespace) -> list[float]:
    if args.brick_sizes.strip():
        return sorted({round(float(raw), 6) for raw in args.brick_sizes.split(",") if raw.strip()})
    ltp = points[-1].close
    values: list[float] = []
    pct = args.min_pct
    while pct <= args.max_pct + 1e-12:
        values.append(round(ltp * pct / 100.0, 6))
        pct += args.step_pct
    return sorted({value for value in values if value > 0.0})


def count_trading_days(points: list[PricePoint]) -> int:
    return len({point.timestamp.astimezone(EASTERN).date() for point in points})


def dedupe_points(points: list[PricePoint]) -> list[PricePoint]:
    out: list[PricePoint] = []
    for point in sorted(points, key=lambda item: item.timestamp):
        if out and out[-1].timestamp == point.timestamp:
            out[-1] = point
        else:
            out.append(point)
    return out


def simulate(
    *,
    symbol: str,
    mode: str,
    points: list[PricePoint],
    brick_size: float,
    trading_days: int,
    args: argparse.Namespace,
) -> SweepResult:
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
    force_flat_dates: set[date] = set()
    session_force_exits = 0

    allow_overnight = mode == "overnight"

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
        local_date = point.timestamp.astimezone(EASTERN).date()
        if (
            side
            and not allow_overnight
            and local_date not in force_flat_dates
            and force_flat_due(point.timestamp, args.force_flat)
        ):
            close_position(close_fill(point.close, side, slippage))
            session_force_exits += 1
            force_flat_dates.add(local_date)
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
            if (
                run_len != 1
                or not can_trade_clock_bar
                or not in_entry_session(point.timestamp, args.entry_start, args.entry_end)
            ):
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
    if side:
        close_position(close_fill(points[-1].close, side, slippage))
        final_forced_exit = True

    mark_equity(points[-1].close)
    win_rate = 100.0 * wins / closed if closed else 0.0
    profit_factor = gross_profit / abs(gross_loss) if gross_loss < 0 else (math.inf if gross_profit > 0 else 0.0)
    avg = realized / closed if closed else 0.0
    return SweepResult(
        symbol=symbol,
        mode=mode,
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
        open_position=False,
        final_forced_exit=final_forced_exit,
        session_force_exits=session_force_exits,
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


def format_value(value: object) -> object:
    if isinstance(value, float):
        if math.isinf(value):
            return "inf"
        return f"{value:.6f}"
    return value


def write_results(path: Path, rows: list[SweepResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "symbol",
        "mode",
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
        "session_force_exits",
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
        red, green, blue = 218, int(76 + ratio * 110), 74
    else:
        ratio = (value - 50.0) / 50.0
        red, green, blue = int(218 - ratio * 164), 186, 82
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
            "Cells ordered by brick size. Color = neighbor profitability. "
            "Cell text: brick %, neighbor %, return %."
            "</text>\n"
        )
        legend_x = 28
        legend_y = 78
        legend_w = 260
        for i in range(101):
            color = heatmap_color(float(i))
            x = legend_x + i * (legend_w / 101.0)
            out.write(
                f'<rect x="{x:.2f}" y="{legend_y}" width="{legend_w / 101.0 + 0.6:.2f}" '
                f'height="12" fill="{color}"/>\n'
            )
        out.write(f'<text x="{legend_x}" y="{legend_y + 30}" class="legend">0% neighbors profitable</text>\n')
        out.write(f'<text x="{legend_x + legend_w}" y="{legend_y + 30}" text-anchor="end" class="legend">100%</text>\n')

        for idx, row in enumerate(ordered):
            grid_row = idx // columns
            grid_col = idx % columns
            x = margin_x + grid_col * cell_w
            y = start_y + grid_row * cell_h
            stroke = "#14532d" if row.net_pnl > 0 else "#7f1d1d"
            stroke_w = 1.8 if row.net_pnl > 0 else 1.0
            out.write(
                f'<rect x="{x}" y="{y}" width="{cell_w - 4}" height="{cell_h - 4}" '
                f'rx="5" fill="{heatmap_color(row.neighbor_profitability_pct)}" '
                f'stroke="{stroke}" stroke-width="{stroke_w}">\n'
            )
            out.write(
                "<title>"
                f"Brick ${row.brick_size:.4f} ({row.brick_pct_of_ltp:.3f}%). "
                f"Neighbor profitability {row.neighbor_profitability_pct:.1f}%. "
                f"Neighbor avg return {row.neighbor_avg_return_pct:.2f}%. "
                f"Return {row.return_pct:.2f}%. Trades/day {row.trades_per_day:.2f}."
                "</title>\n"
            )
            out.write("</rect>\n")
            text_x = x + (cell_w - 4) / 2
            out.write(
                f'<text x="{text_x:.1f}" y="{y + 14}" text-anchor="middle" '
                f'class="cell-main">{row.brick_pct_of_ltp:.2f}%</text>\n'
            )
            out.write(
                f'<text x="{text_x:.1f}" y="{y + 28}" text-anchor="middle" '
                f'class="cell-sub">N {row.neighbor_profitability_pct:.0f}%</text>\n'
            )
            out.write(
                f'<text x="{text_x:.1f}" y="{y + 41}" text-anchor="middle" '
                f'class="cell-sub">{row.return_pct:+.1f}%</text>\n'
            )
        out.write(
            f'<text x="{margin_x}" y="{height - 24}" class="axis">'
            "Green outline = positive net PnL. Red outline = negative net PnL."
            "</text>\n"
        )
        out.write("</svg>\n")


def parse_source_csv_overrides(raw_items: list[str]) -> dict[str, list[Path]]:
    overrides: dict[str, list[Path]] = {}
    for item in raw_items:
        if "=" not in item:
            raise ValueError(f"--source-csv must look like SYMBOL=path or SYMBOL=path+path, got {item!r}")
        raw_symbol, raw_paths = item.split("=", 1)
        symbol = raw_symbol.strip().upper()
        paths = [Path(part) for part in raw_paths.split("+") if part.strip()]
        if not symbol or not paths:
            raise ValueError(f"--source-csv must include a symbol and at least one path, got {item!r}")
        overrides[symbol] = paths
    return overrides


def load_or_fetch(
    symbol: str,
    start: date,
    end: date,
    args: argparse.Namespace,
    source_csvs: dict[str, list[Path]],
) -> tuple[str, list[PricePoint]]:
    if symbol in source_csvs:
        points: list[PricePoint] = []
        for path in source_csvs[symbol]:
            points.extend(load_csv_prices(path))
        return "+".join(str(path) for path in source_csvs[symbol]), dedupe_points(points)

    source_path = cache_path(symbol, args.feed, start, end)
    if source_path.exists():
        return str(source_path), load_csv_prices(source_path)
    points = fetch_alpaca_trades_as_1s_closes(
        symbol=symbol,
        start=start,
        end=end,
        feed=args.feed,
        request_sleep=args.request_sleep,
        max_pages_per_day=args.max_pages_per_day,
    )
    write_prices_csv(source_path, points)
    return str(source_path), points


def md_row(row: SweepResult) -> str:
    pf = "inf" if math.isinf(row.profit_factor) else f"{row.profit_factor:.2f}"
    return (
        f"| {row.symbol} | {row.mode} | ${row.brick_size:.4f} | {row.brick_pct_of_ltp:.3f}% | "
        f"${row.net_pnl:.2f} | {row.return_pct:.2f}% | {row.neighbor_profitability_pct:.1f}% | "
        f"{row.win_rate_pct:.1f}% | {row.trades_per_day:.2f} | {pf} | {row.closed_trades} |"
    )


def write_markdown_summary(
    path: Path,
    *,
    args: argparse.Namespace,
    start: date,
    end: date,
    all_ranked: dict[tuple[str, str], list[SweepResult]],
    source_info: dict[str, dict[str, object]],
) -> None:
    best_rows = [rows[0] for rows in all_ranked.values() if rows]
    lines = [
        "# E1 Renko Reversal 10-Day Brick Sweep",
        "",
        f"Window: `{start}` through `{end}` regular sessions.",
        f"Entry window: `{args.entry_start}` to `{args.entry_end}` America/New_York.",
        f"Intraday-flat mode force-flats at `{args.force_flat}`. Overnight mode keeps positions across sessions and closes any final open position at the last backtest price.",
        f"Grid: `{args.min_pct:.2f}%` to `{args.max_pct:.2f}%` of each symbol's ending price, step `{args.step_pct:.2f}%`.",
        f"Capital model: `${args.fixed_notional:,.0f}` notional per trade, `${args.initial_capital:,.0f}` return denominator, slippage `${args.slippage_ticks * args.tick_size:.2f}` per order.",
        "",
        "## Best by Net PnL",
        "",
        "| Symbol | Mode | Brick | Brick % LTP | Net PnL | Return | Neighbor Profitability | Win Rate | Trades/Day | Profit Factor | Trades |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    lines.extend(md_row(row) for row in sorted(best_rows, key=lambda item: (item.symbol, item.mode)))
    lines.extend(["", "## Top Candidates"])
    for key in sorted(all_ranked):
        rows = all_ranked[key]
        if not rows:
            continue
        symbol, mode = key
        lines.extend(
            [
                "",
                f"### {symbol} {mode}",
                "",
                "| Rank | Brick | Brick % LTP | Net PnL | Return | Neighbor Profitability | Win Rate | Trades/Day | Profit Factor | Trades |",
                "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for idx, row in enumerate(rows[: args.top], start=1):
            pf = "inf" if math.isinf(row.profit_factor) else f"{row.profit_factor:.2f}"
            lines.append(
                f"| {idx} | ${row.brick_size:.4f} | {row.brick_pct_of_ltp:.3f}% | "
                f"${row.net_pnl:.2f} | {row.return_pct:.2f}% | {row.neighbor_profitability_pct:.1f}% | "
                f"{row.win_rate_pct:.1f}% | {row.trades_per_day:.2f} | {pf} | {row.closed_trades} |"
            )
    lines.extend(["", "## Data Sources", ""])
    lines.append("| Symbol | Source | Points | Trading Days | First Timestamp | Last Timestamp | LTP |")
    lines.append("|---|---|---:|---:|---|---|---:|")
    for symbol in sorted(source_info):
        info = source_info[symbol]
        lines.append(
            f"| {symbol} | `{info['source']}` | {info['points']} | {info['trading_days']} | "
            f"`{info['first_timestamp']}` | `{info['last_timestamp']}` | {float(info['ltp']):.4f} |"
        )
    path.write_text("\n".join(lines) + "\n")


def main() -> int:
    args = parse_args()
    start = parse_day(args.start)
    end = parse_day(args.end)
    if start > end:
        raise RuntimeError("--start must be <= --end")

    symbols = [part.strip().upper() for part in args.symbols.split(",") if part.strip()]
    if not symbols:
        raise RuntimeError("--symbols produced no symbols")

    out_dir = Path(args.output_dir) if args.output_dir else Path("artifacts") / "backtests" / (
        f"e1_reversal_10d_{start}_{end}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    source_csvs = parse_source_csv_overrides(args.source_csv)

    modes = ["intraday_flat", "overnight"]
    all_ranked: dict[tuple[str, str], list[SweepResult]] = {}
    source_info: dict[str, dict[str, object]] = {}

    for symbol in symbols:
        source_path, points = load_or_fetch(symbol, start, end, args, source_csvs)
        if len(points) < 2:
            raise RuntimeError(f"{symbol}: need at least two 1-second close points")
        trading_days = count_trading_days(points)
        source_info[symbol] = {
            "source": str(source_path),
            "points": len(points),
            "trading_days": trading_days,
            "first_timestamp": iso_z(points[0].timestamp),
            "last_timestamp": iso_z(points[-1].timestamp),
            "ltp": points[-1].close,
        }
        bricks = brick_grid(points, args)
        for mode in modes:
            rows = [
                simulate(
                    symbol=symbol,
                    mode=mode,
                    points=points,
                    brick_size=brick,
                    trading_days=trading_days,
                    args=args,
                )
                for brick in bricks
            ]
            add_neighbor_stats(rows)
            ranked = sorted(
                rows,
                key=lambda row: (row.net_pnl, row.neighbor_avg_return_pct, row.closed_trades),
                reverse=True,
            )
            all_ranked[(symbol, mode)] = ranked
            mode_dir = out_dir / symbol.lower() / mode
            write_results(mode_dir / "renko_reversal_brick_sweep.csv", ranked)
            write_results(mode_dir / f"top{args.top}_brick_sizes.csv", ranked[: args.top])
            write_neighbor_heatmap(
                mode_dir / "neighbor_profitability_heatmap.svg",
                rows,
                title=f"{symbol} E1 Reversal {mode.replace('_', ' ').title()}",
            )
            best = ranked[0]
            pf = "inf" if math.isinf(best.profit_factor) else f"{best.profit_factor:.2f}"
            print(
                f"{symbol} {mode}: best brick ${best.brick_size:.4f} "
                f"({best.brick_pct_of_ltp:.3f}%) net=${best.net_pnl:.2f} "
                f"neighbor={best.neighbor_profitability_pct:.1f}% win={best.win_rate_pct:.1f}% "
                f"tpd={best.trades_per_day:.2f} pf={pf}",
                flush=True,
            )

    best_rows = [rows[0] for rows in all_ranked.values() if rows]
    combined_rows: list[SweepResult] = []
    for rows in all_ranked.values():
        combined_rows.extend(rows)
    write_results(out_dir / "best_by_symbol_mode.csv", sorted(best_rows, key=lambda row: (row.symbol, row.mode)))
    write_results(out_dir / "combined_full_sweep.csv", combined_rows)
    write_markdown_summary(
        out_dir / "SUMMARY.md",
        args=args,
        start=start,
        end=end,
        all_ranked=all_ranked,
        source_info=source_info,
    )
    metadata = {
        "method": "e1_reversal_1s_trade_aggregated_multi_symbol_v1",
        "symbols": symbols,
        "modes": modes,
        "start": str(start),
        "end": str(end),
        "feed": args.feed,
        "entry_session_eastern": f"{args.entry_start}-{args.entry_end}",
        "force_flat_eastern": args.force_flat,
        "grid": {
            "min_pct": args.min_pct,
            "max_pct": args.max_pct,
            "step_pct": args.step_pct,
            "brick_sizes": args.brick_sizes,
        },
        "initial_capital": args.initial_capital,
        "fixed_notional": args.fixed_notional,
        "slippage_per_order": args.slippage_ticks * args.tick_size,
        "anchor_mode": args.anchor_mode,
        "sources": source_info,
        "best_by_symbol_mode": [row.__dict__ for row in sorted(best_rows, key=lambda item: (item.symbol, item.mode))],
    }
    (out_dir / "metadata.json").write_text(json.dumps(metadata, indent=2, default=str) + "\n")
    print(f"output={out_dir}", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1)
