from __future__ import annotations

import argparse
import csv
import json
import math
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


EASTERN = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class Point:
    timestamp: datetime
    close: float


@dataclass(frozen=True)
class Variant:
    name: str
    window_seconds: int
    entry_z: float
    exit_z: float
    stop_z: float
    efficiency_max: float
    trend_exit_efficiency: float
    max_hold_seconds: int
    entry_start: str
    entry_end: str
    lunch_block: bool


@dataclass
class Trade:
    entry_time: datetime
    exit_time: datetime
    side: int
    entry_fill: float
    exit_fill: float
    qty: float
    pnl: float
    reason: str
    entry_z: float
    exit_z: float
    entry_efficiency: float
    exit_efficiency: float


@dataclass
class Result:
    variant: Variant
    points: int
    trading_days: int
    entries: int = 0
    closed_trades: int = 0
    wins: int = 0
    losses: int = 0
    flats: int = 0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    net_pnl: float = 0.0
    max_drawdown: float = 0.0
    exposure_seconds: int = 0
    blocked_warmup: int = 0
    blocked_session: int = 0
    blocked_lunch: int = 0
    blocked_efficiency: int = 0
    forced_session_exits: int = 0
    trend_exits: int = 0
    stop_exits: int = 0
    time_exits: int = 0
    mean_exits: int = 0

    @property
    def win_rate_pct(self) -> float:
        return 100.0 * self.wins / self.closed_trades if self.closed_trades else 0.0

    @property
    def profit_factor(self) -> float:
        if self.gross_loss < 0:
            return self.gross_profit / abs(self.gross_loss)
        return math.inf if self.gross_profit > 0 else 0.0

    @property
    def avg_trade_pnl(self) -> float:
        return self.net_pnl / self.closed_trades if self.closed_trades else 0.0

    @property
    def trades_per_day(self) -> float:
        return self.closed_trades / max(1, self.trading_days)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest a chop-native QQQ rolling mean-reversion trader.")
    parser.add_argument("--csv", required=True, help="CSV with timestamp and close/price columns.")
    parser.add_argument("--symbol", default="QQQ")
    parser.add_argument("--output-dir", default="artifacts/backtests/qqq_chop_mean_reversion")
    parser.add_argument("--initial-capital", type=float, default=10000.0)
    parser.add_argument("--fixed-notional", type=float, default=10000.0)
    parser.add_argument("--slippage", type=float, default=0.01, help="Dollars per fill.")
    parser.add_argument("--include-extended-hours", action="store_true", help="Keep premarket/after-hours points in the simulation.")
    parser.add_argument("--top", type=int, default=25)
    return parser.parse_args()


def parse_ts(raw: str) -> datetime:
    value = raw.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    ts = datetime.fromisoformat(value)
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def first_present(names: list[str], candidates: list[str]) -> str | None:
    lowered = {name.lower(): name for name in names}
    for candidate in candidates:
        if candidate.lower() in lowered:
            return lowered[candidate.lower()]
    return None


def load_points(path: Path) -> list[Point]:
    points: list[Point] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise RuntimeError(f"{path} has no CSV header.")
        time_key = first_present(reader.fieldnames, ["timestamp", "time", "datetime", "t"])
        close_key = first_present(reader.fieldnames, ["close", "price", "c", "p"])
        if not time_key or not close_key:
            raise RuntimeError(f"{path} must include timestamp and close/price columns.")
        for row in reader:
            if not row.get(time_key) or not row.get(close_key):
                continue
            points.append(Point(parse_ts(row[time_key]), float(row[close_key])))
    points.sort(key=lambda point: point.timestamp)
    deduped: list[Point] = []
    for point in points:
        if deduped and deduped[-1].timestamp == point.timestamp:
            deduped[-1] = point
        else:
            deduped.append(point)
    if len(deduped) < 2:
        raise RuntimeError(f"Need at least two price points in {path}.")
    return deduped


def parse_hms(raw: str) -> time:
    hour, minute, second = (int(part) for part in raw.split(":"))
    return time(hour=hour, minute=minute, second=second)


def in_entry_window(ts: datetime, start: str, end: str, lunch_block: bool) -> tuple[bool, str]:
    local_time = ts.astimezone(EASTERN).time()
    if local_time < parse_hms(start) or local_time >= parse_hms(end):
        return False, "session"
    if lunch_block and parse_hms("11:30:00") <= local_time < parse_hms("13:30:00"):
        return False, "lunch"
    return True, ""


def is_session_close(ts: datetime) -> bool:
    return ts.astimezone(EASTERN).time() >= parse_hms("15:58:00")


def is_regular_session(ts: datetime) -> bool:
    local_time = ts.astimezone(EASTERN).time()
    return parse_hms("09:30:00") <= local_time < parse_hms("16:00:00")


def trading_day_count(points: list[Point]) -> int:
    return len({point.timestamp.astimezone(EASTERN).date().isoformat() for point in points})


def precompute_stats(points: list[Point], windows: set[int]) -> dict[int, dict[str, list[float]]]:
    out: dict[int, dict[str, list[float]]] = {}
    timestamps = [point.timestamp for point in points]
    closes = [point.close for point in points]

    for seconds in sorted(windows):
        means: list[float] = []
        stdevs: list[float] = []
        efficiencies: list[float] = []
        warm: list[float] = []
        q: deque[tuple[datetime, float]] = deque()
        abs_moves: deque[tuple[datetime, float]] = deque()
        sum_close = 0.0
        sum_close_sq = 0.0
        sum_abs_moves = 0.0
        prev_close: float | None = None

        for idx, point in enumerate(points):
            q.append((point.timestamp, point.close))
            sum_close += point.close
            sum_close_sq += point.close * point.close
            if prev_close is not None:
                move = abs(point.close - prev_close)
                abs_moves.append((point.timestamp, move))
                sum_abs_moves += move
            cutoff = point.timestamp - timedelta(seconds=seconds)
            while q and q[0][0] < cutoff:
                _, old = q.popleft()
                sum_close -= old
                sum_close_sq -= old * old
            while abs_moves and abs_moves[0][0] <= cutoff:
                _, old_move = abs_moves.popleft()
                sum_abs_moves -= old_move

            count = len(q)
            mean = sum_close / count if count else point.close
            variance = max(0.0, (sum_close_sq / count) - mean * mean) if count else 0.0
            stdev = math.sqrt(variance)
            window_ready = 1.0 if count >= max(30, min(seconds // 2, seconds - 1)) else 0.0
            if q and q[0][0] <= cutoff + timedelta(seconds=1) and idx > 0:
                displacement = abs(point.close - q[0][1])
                efficiency = displacement / sum_abs_moves if sum_abs_moves > 0 else 0.0
            else:
                efficiency = 0.0
            means.append(mean)
            stdevs.append(stdev)
            efficiencies.append(efficiency)
            warm.append(window_ready)
            prev_close = closes[idx]

        out[seconds] = {
            "mean": means,
            "stdev": stdevs,
            "efficiency": efficiencies,
            "warm": warm,
        }
    return out


def trade_qty(notional: float, price: float) -> float:
    return notional / price if price > 0 else 0.0


def slippage_fill(price: float, side: int, entering: bool, slippage: float) -> float:
    if entering:
        return price + slippage if side > 0 else price - slippage
    return price - slippage if side > 0 else price + slippage


def simulate(
    points: list[Point],
    stats: dict[int, dict[str, list[float]]],
    variant: Variant,
    *,
    fixed_notional: float,
    slippage: float,
) -> tuple[Result, list[Trade]]:
    result = Result(variant=variant, points=len(points), trading_days=trading_day_count(points))
    stat = stats[variant.window_seconds]
    side = 0
    qty = 0.0
    entry_fill = 0.0
    entry_time: datetime | None = None
    entry_z = 0.0
    entry_eff = 0.0
    realized = 0.0
    peak_equity = 0.0
    max_drawdown = 0.0
    trades: list[Trade] = []
    last_ts = points[0].timestamp

    def mark_equity(price: float) -> None:
        nonlocal peak_equity, max_drawdown
        unrealized = (price - entry_fill) * side * qty if side else 0.0
        equity = realized + unrealized
        peak_equity = max(peak_equity, equity)
        max_drawdown = max(max_drawdown, peak_equity - equity)

    def close_position(point: Point, z_score: float, efficiency: float, reason: str) -> None:
        nonlocal side, qty, entry_fill, entry_time, entry_z, entry_eff, realized
        if side == 0 or entry_time is None:
            return
        exit_fill = slippage_fill(point.close, side, entering=False, slippage=slippage)
        pnl = (exit_fill - entry_fill) * side * qty
        realized += pnl
        result.closed_trades += 1
        if pnl > 0:
            result.wins += 1
            result.gross_profit += pnl
        elif pnl < 0:
            result.losses += 1
            result.gross_loss += pnl
        else:
            result.flats += 1
        if reason == "mean":
            result.mean_exits += 1
        elif reason == "stop":
            result.stop_exits += 1
        elif reason == "trend":
            result.trend_exits += 1
        elif reason == "time":
            result.time_exits += 1
        elif reason == "session":
            result.forced_session_exits += 1
        trades.append(
            Trade(
                entry_time=entry_time,
                exit_time=point.timestamp,
                side=side,
                entry_fill=entry_fill,
                exit_fill=exit_fill,
                qty=qty,
                pnl=pnl,
                reason=reason,
                entry_z=entry_z,
                exit_z=z_score,
                entry_efficiency=entry_eff,
                exit_efficiency=efficiency,
            )
        )
        side = 0
        qty = 0.0
        entry_fill = 0.0
        entry_time = None
        entry_z = 0.0
        entry_eff = 0.0

    for idx, point in enumerate(points):
        elapsed = max(0, int((point.timestamp - last_ts).total_seconds()))
        if side:
            result.exposure_seconds += elapsed
        last_ts = point.timestamp

        mean = stat["mean"][idx]
        stdev = stat["stdev"][idx]
        efficiency = stat["efficiency"][idx]
        warm = stat["warm"][idx] > 0.0
        z_score = (point.close - mean) / stdev if stdev > 1e-12 else 0.0
        mark_equity(point.close)

        if side:
            held_seconds = int((point.timestamp - entry_time).total_seconds()) if entry_time else 0
            if is_session_close(point.timestamp):
                close_position(point, z_score, efficiency, "session")
                mark_equity(point.close)
                continue
            if side > 0 and z_score >= -variant.exit_z:
                close_position(point, z_score, efficiency, "mean")
                mark_equity(point.close)
                continue
            if side < 0 and z_score <= variant.exit_z:
                close_position(point, z_score, efficiency, "mean")
                mark_equity(point.close)
                continue
            if side > 0 and z_score <= -variant.stop_z:
                close_position(point, z_score, efficiency, "stop")
                mark_equity(point.close)
                continue
            if side < 0 and z_score >= variant.stop_z:
                close_position(point, z_score, efficiency, "stop")
                mark_equity(point.close)
                continue
            if efficiency >= variant.trend_exit_efficiency:
                close_position(point, z_score, efficiency, "trend")
                mark_equity(point.close)
                continue
            if held_seconds >= variant.max_hold_seconds:
                close_position(point, z_score, efficiency, "time")
                mark_equity(point.close)
                continue
            continue

        allowed, block_reason = in_entry_window(point.timestamp, variant.entry_start, variant.entry_end, variant.lunch_block)
        if not warm:
            result.blocked_warmup += 1
            continue
        if not allowed:
            if block_reason == "lunch":
                result.blocked_lunch += 1
            else:
                result.blocked_session += 1
            continue
        if efficiency > variant.efficiency_max:
            result.blocked_efficiency += 1
            continue

        entry_side = 0
        if z_score <= -variant.entry_z:
            entry_side = 1
        elif z_score >= variant.entry_z:
            entry_side = -1
        if entry_side == 0:
            continue

        fill = slippage_fill(point.close, entry_side, entering=True, slippage=slippage)
        qty = trade_qty(fixed_notional, point.close)
        if qty <= 0:
            continue
        side = entry_side
        entry_fill = fill
        entry_time = point.timestamp
        entry_z = z_score
        entry_eff = efficiency
        result.entries += 1
        mark_equity(point.close)

    if side:
        point = points[-1]
        mean = stat["mean"][-1]
        stdev = stat["stdev"][-1]
        efficiency = stat["efficiency"][-1]
        z_score = (point.close - mean) / stdev if stdev > 1e-12 else 0.0
        close_position(point, z_score, efficiency, "final")

    mark_equity(points[-1].close)
    result.net_pnl = realized
    result.max_drawdown = max_drawdown
    return result, trades


def build_variants() -> list[Variant]:
    variants: list[Variant] = []
    for window in (180, 300, 600, 900):
        for entry_z in (1.0, 1.25, 1.5, 1.75, 2.0):
            for efficiency_max in (0.15, 0.25, 0.35, 0.45):
                for max_hold in (180, 300, 600):
                    variants.append(
                        Variant(
                            name=(
                                f"mr_w{window}_z{str(entry_z).replace('.', 'p')}"
                                f"_eff{str(efficiency_max).replace('.', 'p')}_hold{max_hold}"
                            ),
                            window_seconds=window,
                            entry_z=entry_z,
                            exit_z=0.10,
                            stop_z=max(entry_z + 1.0, 2.5),
                            efficiency_max=efficiency_max,
                            trend_exit_efficiency=0.65,
                            max_hold_seconds=max_hold,
                            entry_start="09:35:00",
                            entry_end="15:45:00",
                            lunch_block=False,
                        )
                    )
    for base in list(variants):
        if base.window_seconds in {300, 600} and base.entry_z in {1.25, 1.5} and base.efficiency_max in {0.25, 0.35}:
            variants.append(
                Variant(
                    name=f"{base.name}_no_lunch",
                    window_seconds=base.window_seconds,
                    entry_z=base.entry_z,
                    exit_z=base.exit_z,
                    stop_z=base.stop_z,
                    efficiency_max=base.efficiency_max,
                    trend_exit_efficiency=base.trend_exit_efficiency,
                    max_hold_seconds=base.max_hold_seconds,
                    entry_start=base.entry_start,
                    entry_end=base.entry_end,
                    lunch_block=True,
                )
            )
    return variants


def buy_hold_metrics(points: list[Point], initial_capital: float, notional: float) -> dict[str, float | str]:
    first = points[0].close
    last = points[-1].close
    qty = trade_qty(notional, first)
    pnl = (last - first) * qty
    return {
        "first_timestamp": points[0].timestamp.isoformat(),
        "last_timestamp": points[-1].timestamp.isoformat(),
        "first_close": first,
        "last_close": last,
        "pnl": pnl,
        "return_pct": 100.0 * pnl / initial_capital,
        "asset_return_pct": 100.0 * (last - first) / first if first else 0.0,
    }


def row_for_result(result: Result, initial_capital: float) -> dict[str, object]:
    return {
        "variant": result.variant.name,
        "window_seconds": result.variant.window_seconds,
        "entry_z": result.variant.entry_z,
        "exit_z": result.variant.exit_z,
        "stop_z": result.variant.stop_z,
        "efficiency_max": result.variant.efficiency_max,
        "trend_exit_efficiency": result.variant.trend_exit_efficiency,
        "max_hold_seconds": result.variant.max_hold_seconds,
        "lunch_block": result.variant.lunch_block,
        "entries": result.entries,
        "closed_trades": result.closed_trades,
        "wins": result.wins,
        "losses": result.losses,
        "win_rate_pct": result.win_rate_pct,
        "profit_factor": result.profit_factor,
        "net_pnl": result.net_pnl,
        "return_pct": 100.0 * result.net_pnl / initial_capital,
        "max_drawdown": result.max_drawdown,
        "max_drawdown_pct": 100.0 * result.max_drawdown / initial_capital,
        "avg_trade_pnl": result.avg_trade_pnl,
        "trades_per_day": result.trades_per_day,
        "exposure_minutes": result.exposure_seconds / 60.0,
        "blocked_warmup": result.blocked_warmup,
        "blocked_session": result.blocked_session,
        "blocked_lunch": result.blocked_lunch,
        "blocked_efficiency": result.blocked_efficiency,
        "mean_exits": result.mean_exits,
        "stop_exits": result.stop_exits,
        "trend_exits": result.trend_exits,
        "time_exits": result.time_exits,
        "forced_session_exits": result.forced_session_exits,
    }


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_trades(path: Path, trades_by_variant: dict[str, list[Trade]]) -> None:
    rows: list[dict[str, object]] = []
    for variant, trades in trades_by_variant.items():
        for trade in trades:
            rows.append(
                {
                    "variant": variant,
                    "entry_time": trade.entry_time.isoformat(),
                    "exit_time": trade.exit_time.isoformat(),
                    "side": "long" if trade.side > 0 else "short",
                    "entry_fill": trade.entry_fill,
                    "exit_fill": trade.exit_fill,
                    "qty": trade.qty,
                    "pnl": trade.pnl,
                    "reason": trade.reason,
                    "entry_z": trade.entry_z,
                    "exit_z": trade.exit_z,
                    "entry_efficiency": trade.entry_efficiency,
                    "exit_efficiency": trade.exit_efficiency,
                }
            )
    if not rows:
        return
    write_csv(path, rows)


def fmt(value: object, decimals: int = 2) -> str:
    if isinstance(value, float):
        if math.isinf(value):
            return "inf"
        return f"{value:.{decimals}f}"
    return str(value)


def write_markdown(path: Path, rows: list[dict[str, object]], metadata: dict[str, object]) -> None:
    ranked = sorted(rows, key=lambda row: (float(row["net_pnl"]), -float(row["max_drawdown"])), reverse=True)
    top = ranked[0]
    buy_hold = metadata["buy_hold"]
    lines = [
        "# QQQ Chop Mean-Reversion Backtest",
        "",
        f"Source: `{metadata['source']}`",
        f"Period: `{metadata['first_timestamp']}` to `{metadata['last_timestamp']}`",
        f"Price points / raw points / trading days: `{metadata['price_points']}` / `{metadata['raw_price_points']}` / `{metadata['trading_days']}`",
        f"Include extended hours: `{metadata['include_extended_hours']}`",
        f"Initial capital / fixed notional: `${metadata['initial_capital']}` / `${metadata['fixed_notional']}`",
        f"Slippage: `${metadata['slippage_per_fill']}` per fill",
        "",
        "## Best Variant",
        "",
        "| Variant | Net PnL | Return % | Max DD % | Trades | Win % | PF | Avg Trade |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
        (
            f"| {top['variant']} | {fmt(top['net_pnl'])} | {fmt(top['return_pct'])} | "
            f"{fmt(top['max_drawdown_pct'])} | {top['closed_trades']} | {fmt(top['win_rate_pct'])} | "
            f"{fmt(top['profit_factor'])} | {fmt(top['avg_trade_pnl'])} |"
        ),
        "",
        "## Buy And Hold Reference",
        "",
        "| Reference | PnL | Return % | Asset Move % | First Close | Last Close |",
        "|---|---:|---:|---:|---:|---:|",
        (
            f"| QQQ buy and hold | {fmt(buy_hold['pnl'])} | {fmt(buy_hold['return_pct'])} | "
            f"{fmt(buy_hold['asset_return_pct'])} | {fmt(buy_hold['first_close'])} | {fmt(buy_hold['last_close'])} |"
        ),
        "",
        "## Ranked Variants",
        "",
        "| Rank | Variant | Net PnL | Return % | Max DD % | Trades | Win % | PF | Window | Entry Z | Eff Max | Hold |",
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for idx, row in enumerate(ranked[:25], start=1):
        lines.append(
            "| "
            f"{idx} | {row['variant']} | {fmt(row['net_pnl'])} | {fmt(row['return_pct'])} | "
            f"{fmt(row['max_drawdown_pct'])} | {row['closed_trades']} | {fmt(row['win_rate_pct'])} | "
            f"{fmt(row['profit_factor'])} | {row['window_seconds']} | {row['entry_z']} | "
            f"{row['efficiency_max']} | {row['max_hold_seconds']} |"
        )
    lines.extend(
        [
            "",
            "## Strategy Definition",
            "",
            "- Trade only intraday, with no overnight position.",
            "- Compute rolling mean, rolling standard deviation, and price efficiency over the selected window.",
            "- Only enter when efficiency is below the configured threshold, meaning the tape is choppy rather than directional.",
            "- Fade extension from the mean: long below the mean, short above the mean.",
            "- Exit at mean reversion, hard z-score stop, directional/trend escape, max hold, or end-of-session flatten.",
            "",
            "## Data Caveat",
            "",
            "- This is a tick-derived 1-second close backtest. It does not prove broker-fill parity until the same logic is run through the live event replay harness.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def heat_color(value: float, max_abs: float) -> str:
    if max_abs <= 0:
        return "#f2f4f1"
    ratio = min(1.0, abs(value) / max_abs)
    if value >= 0:
        red = int(230 - 135 * ratio)
        green = int(245 - 55 * ratio)
        blue = int(232 - 100 * ratio)
    else:
        red = int(248 - 20 * ratio)
        green = int(226 - 135 * ratio)
        blue = int(226 - 130 * ratio)
    return f"#{red:02x}{green:02x}{blue:02x}"


def write_heatmap(path: Path, rows: list[dict[str, object]], *, metric: str, title: str) -> None:
    filtered = [row for row in rows if not bool(row["lunch_block"])]
    windows = sorted({int(row["window_seconds"]) for row in filtered})
    entry_zs = sorted({float(row["entry_z"]) for row in filtered})
    effs = sorted({float(row["efficiency_max"]) for row in filtered})
    best_by_cell: dict[tuple[int, float], dict[str, object]] = {}
    for row in filtered:
        key = (int(row["window_seconds"]), float(row["entry_z"]))
        old = best_by_cell.get(key)
        if old is None or float(row[metric]) > float(old[metric]):
            best_by_cell[key] = row

    cell_w = 122
    cell_h = 70
    margin = 28
    top = 94
    left = 86
    width = left + len(windows) * cell_w + margin
    height = top + len(entry_zs) * cell_h + 58
    max_abs = max(abs(float(row[metric])) for row in filtered) or 1.0
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#fbfcf8"/>',
        "<style>text{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Arial,sans-serif;fill:#17211d}.title{font-size:22px;font-weight:760}.sub{font-size:12px;fill:#5f6c65}.axis{font-size:12px;font-weight:720}.metric{font-size:14px;font-weight:760}.small{font-size:10px;fill:#4e5a54}</style>",
        f'<text x="{margin}" y="36" class="title">{title}</text>',
        f'<text x="{margin}" y="58" class="sub">Each cell shows the best {metric} across efficiency gates and hold times for that window / entry-z pair.</text>',
    ]
    for col, window in enumerate(windows):
        x = left + col * cell_w
        parts.append(f'<text x="{x + 8}" y="{top - 18}" class="axis">{window}s</text>')
    for row_idx, entry_z in enumerate(entry_zs):
        y = top + row_idx * cell_h
        parts.append(f'<text x="{margin}" y="{y + 38}" class="axis">z {entry_z:g}</text>')
        for col, window in enumerate(windows):
            x = left + col * cell_w
            item = best_by_cell.get((window, entry_z))
            if item is None:
                fill = "#eef1ed"
                label = "n/a"
                detail = ""
            else:
                value = float(item[metric])
                fill = heat_color(value, max_abs)
                label = f"{value:+.2f}" if metric == "net_pnl" else f"{value:.2f}"
                detail = f"eff {float(item['efficiency_max']):.2f} hold {int(item['max_hold_seconds'])}"
            parts.append(f'<rect x="{x}" y="{y}" width="{cell_w - 10}" height="{cell_h - 10}" rx="6" fill="{fill}" stroke="#d5ded8"/>')
            parts.append(f'<text x="{x + 10}" y="{y + 27}" class="metric">{label}</text>')
            parts.append(f'<text x="{x + 10}" y="{y + 48}" class="small">{detail}</text>')
    parts.append("</svg>")
    path.write_text("\n".join(parts) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    csv_path = Path(args.csv)
    points = load_points(csv_path)
    raw_points = len(points)
    if not args.include_extended_hours:
        points = [point for point in points if is_regular_session(point.timestamp)]
    if len(points) < 2:
        raise RuntimeError("Need at least two regular-session price points after filtering.")
    variants = build_variants()
    windows = {variant.window_seconds for variant in variants}
    stats = precompute_stats(points, windows)

    results: list[Result] = []
    trades_by_variant: dict[str, list[Trade]] = {}
    for variant in variants:
        result, trades = simulate(
            points,
            stats,
            variant,
            fixed_notional=args.fixed_notional,
            slippage=args.slippage,
        )
        results.append(result)
        trades_by_variant[variant.name] = trades

    rows = sorted(
        [row_for_result(result, args.initial_capital) for result in results],
        key=lambda row: (float(row["net_pnl"]), -float(row["max_drawdown"])),
        reverse=True,
    )
    top_rows = rows[: args.top]
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    metadata: dict[str, object] = {
        "method": "chop_mean_reversion_v1",
        "source": str(csv_path),
        "symbol": args.symbol.upper(),
        "first_timestamp": points[0].timestamp.isoformat(),
        "last_timestamp": points[-1].timestamp.isoformat(),
        "price_points": len(points),
        "raw_price_points": raw_points,
        "include_extended_hours": bool(args.include_extended_hours),
        "trading_days": trading_day_count(points),
        "initial_capital": args.initial_capital,
        "fixed_notional": args.fixed_notional,
        "slippage_per_fill": args.slippage,
        "buy_hold": buy_hold_metrics(points, args.initial_capital, args.fixed_notional),
        "variant_count": len(rows),
    }

    write_csv(out_dir / "qqq_chop_mean_reversion_results.csv", rows)
    write_csv(out_dir / "qqq_chop_mean_reversion_top.csv", top_rows)
    write_trades(out_dir / "qqq_chop_mean_reversion_top_trades.csv", {row["variant"]: trades_by_variant[str(row["variant"])] for row in top_rows[:5]})
    write_markdown(out_dir / "QQQ_CHOP_MEAN_REVERSION_SUMMARY.md", rows, metadata)
    write_heatmap(out_dir / "qqq_chop_mean_reversion_pnl_heatmap.svg", rows, metric="net_pnl", title="QQQ Chop Mean-Reversion Net PnL")
    write_heatmap(out_dir / "qqq_chop_mean_reversion_drawdown_heatmap.svg", rows, metric="max_drawdown_pct", title="QQQ Chop Mean-Reversion Max Drawdown")
    (out_dir / "summary.json").write_text(
        json.dumps({"metadata": metadata, "top": top_rows[:10]}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    best = rows[0]
    print(
        json.dumps(
            {
                "output_dir": str(out_dir),
                "best_variant": best["variant"],
                "best_net_pnl": best["net_pnl"],
                "best_return_pct": best["return_pct"],
                "best_max_drawdown_pct": best["max_drawdown_pct"],
                "best_trades": best["closed_trades"],
                "buy_hold_return_pct": metadata["buy_hold"]["return_pct"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
