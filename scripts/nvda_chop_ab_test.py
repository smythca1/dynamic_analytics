from __future__ import annotations

import argparse
import csv
import json
import math
from bisect import bisect_left
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


EASTERN = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class Point:
    timestamp: datetime
    close: float


@dataclass(frozen=True)
class Brick:
    timestamp: datetime
    close: float
    direction: int
    run_len: int


@dataclass(frozen=True)
class Variant:
    name: str
    group: str
    description: str
    brick_size: float
    efficiency_seconds: int | None = None
    efficiency_threshold: float | None = None
    renko_chop: bool = False
    conditional_e2: bool = False
    cooldown: bool = False
    loss_e2: bool = False
    windows: tuple[tuple[str, str], ...] = (("09:31:00", "15:59:59"),)


@dataclass
class Result:
    variant: Variant
    price_points: int
    trading_days: int
    bricks: int = 0
    entries: int = 0
    closed_trades: int = 0
    wins: int = 0
    losses: int = 0
    flats: int = 0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    realized_pnl: float = 0.0
    final_equity: float = 0.0
    max_drawdown: float = 0.0
    blocked_entries: int = 0
    blocked_efficiency: int = 0
    blocked_renko_chop: int = 0
    blocked_conditional_e2: int = 0
    blocked_cooldown: int = 0
    blocked_session: int = 0
    open_position_side: int = 0
    final_forced_exit: bool = True

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
        return self.realized_pnl / self.closed_trades if self.closed_trades else 0.0

    @property
    def trades_per_day(self) -> float:
        return self.closed_trades / max(1, self.trading_days)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="A/B test NVDA production E1 reversal trader chop reducers.")
    parser.add_argument("--csv", default="artifacts/market_data/NVDA_sip_2026-06-01_2026-06-29_1s_close.csv")
    parser.add_argument("--output-dir", default="artifacts/backtests/nvda_chop_ab_20260601_20260629")
    parser.add_argument("--symbol", default="NVDA")
    parser.add_argument("--brick-size", type=float, default=1.149851)
    parser.add_argument("--initial-capital", type=float, default=10000.0)
    parser.add_argument("--fixed-notional", type=float, default=10000.0)
    parser.add_argument("--slippage", type=float, default=0.03)
    return parser.parse_args()


def parse_ts(raw: str) -> datetime:
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


def parse_hms(raw: str) -> int:
    hour, minute, second = (int(part) for part in raw.split(":"))
    return hour * 3600 + minute * 60 + second


def load_points(path: Path) -> list[Point]:
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        points = [Point(timestamp=parse_ts(row["timestamp"]), close=float(row["close"])) for row in reader]
    if len(points) < 2:
        raise RuntimeError(f"Need at least two price points in {path}.")
    return points


def trading_day_count(points: list[Point]) -> int:
    return len({point.timestamp.astimezone(EASTERN).date().isoformat() for point in points})


def in_windows(ts: datetime, windows: tuple[tuple[str, str], ...]) -> bool:
    local = ts.astimezone(EASTERN)
    current = local.hour * 3600 + local.minute * 60 + local.second
    for start, end in windows:
        start_seconds = parse_hms(start)
        end_seconds = parse_hms(end)
        if start_seconds <= end_seconds:
            if start_seconds <= current < end_seconds:
                return True
        elif current >= start_seconds or current < end_seconds:
            return True
    return False


def initial_brick_close(price: float, brick_size: float) -> float:
    return math.floor(price / brick_size) * brick_size


def precompute_efficiency(points: list[Point], windows: set[int]) -> dict[int, list[float]]:
    timestamps = [point.timestamp for point in points]
    abs_prefix = [0.0]
    for prev, current in zip(points, points[1:]):
        abs_prefix.append(abs_prefix[-1] + abs(current.close - prev.close))

    out: dict[int, list[float]] = {}
    for seconds in windows:
        values: list[float] = []
        for idx, point in enumerate(points):
            cutoff = point.timestamp - timedelta(seconds=seconds)
            start = bisect_left(timestamps, cutoff)
            if start >= idx:
                values.append(0.0)
                continue
            displacement = abs(point.close - points[start].close)
            travel = abs_prefix[idx] - abs_prefix[start]
            values.append(displacement / travel if travel > 0 else 0.0)
        out[seconds] = values
    return out


def recent_brick_stats(bricks: list[Brick], lookback: int = 8) -> tuple[int, float]:
    recent = bricks[-lookback:]
    if len(recent) < lookback:
        return 0, 1.0
    directions = [brick.direction for brick in recent]
    flips = sum(1 for prev, current in zip(directions, directions[1:]) if current != prev)
    net_ratio = abs(sum(directions)) / len(directions)
    return flips, net_ratio


def is_renko_choppy(bricks: list[Brick], *, lookback: int = 8, max_flips: int = 4, min_net_ratio: float = 0.35) -> bool:
    flips, net_ratio = recent_brick_stats(bricks, lookback)
    return len(bricks) >= lookback and (flips >= max_flips or net_ratio < min_net_ratio)


def renko_regime(bricks: list[Brick]) -> str:
    if len(bricks) < 8:
        return "normal"
    flips, net_ratio = recent_brick_stats(bricks, 8)
    if flips >= 4 or net_ratio < 0.35:
        return "very_choppy"
    if flips >= 3 or net_ratio < 0.50:
        return "choppy"
    return "normal"


def trade_qty(notional: float, price: float) -> float:
    return notional / price if price > 0 else 0.0


def simulate(points: list[Point], variant: Variant, *, initial_capital: float, fixed_notional: float, slippage: float) -> Result:
    result = Result(variant=variant, price_points=len(points), trading_days=trading_day_count(points))
    efficiency = precompute_efficiency(
        points,
        {variant.efficiency_seconds} if variant.efficiency_seconds is not None else set(),
    )

    last_close = initial_brick_close(points[0].close, variant.brick_size)
    run_dir = 0
    run_len = 0
    bricks: list[Brick] = []
    side = 0
    qty = 0.0
    entry_fill = 0.0
    realized = 0.0
    peak_equity = 0.0
    max_drawdown = 0.0
    last_trade_ts: datetime | None = None
    cooldown_until: datetime | None = None
    entry_times: list[datetime] = []
    consecutive_losses = 0

    def mark_equity(price: float) -> None:
        nonlocal peak_equity, max_drawdown
        unrealized = (price - entry_fill) * side * qty if side else 0.0
        equity = realized + unrealized
        peak_equity = max(peak_equity, equity)
        max_drawdown = max(max_drawdown, peak_equity - equity)

    def close_position(fill_price: float) -> None:
        nonlocal side, qty, entry_fill, realized, consecutive_losses
        if side == 0:
            return
        pnl = (fill_price - entry_fill) * side * qty
        realized += pnl
        result.closed_trades += 1
        if pnl > 0:
            result.wins += 1
            result.gross_profit += pnl
            consecutive_losses = 0
        elif pnl < 0:
            result.losses += 1
            result.gross_loss += pnl
            consecutive_losses += 1
        else:
            result.flats += 1
            consecutive_losses = 0
        side = 0
        qty = 0.0
        entry_fill = 0.0

    def block_entry(idx: int, point: Point, direction: int, current_run_len: int) -> bool:
        if not in_windows(point.timestamp, variant.windows):
            result.blocked_entries += 1
            result.blocked_session += 1
            return True
        if variant.efficiency_seconds is not None and variant.efficiency_threshold is not None:
            score = efficiency[variant.efficiency_seconds][idx]
            if score < variant.efficiency_threshold:
                result.blocked_entries += 1
                result.blocked_efficiency += 1
                return True
        if variant.renko_chop and is_renko_choppy(bricks):
            result.blocked_entries += 1
            result.blocked_renko_chop += 1
            return True
        if variant.conditional_e2:
            regime = renko_regime(bricks)
            if regime == "very_choppy":
                result.blocked_entries += 1
                result.blocked_conditional_e2 += 1
                return True
            if regime == "choppy" and current_run_len < 2:
                result.blocked_entries += 1
                result.blocked_conditional_e2 += 1
                return True
            if regime == "normal" and current_run_len != 1:
                return True
        elif current_run_len != 1:
            return True
        if variant.cooldown and cooldown_until is not None and point.timestamp < cooldown_until:
            result.blocked_entries += 1
            result.blocked_cooldown += 1
            return True
        if variant.loss_e2 and consecutive_losses >= 2 and current_run_len < 2:
            result.blocked_entries += 1
            result.blocked_cooldown += 1
            return True
        if (direction == 1 and side > 0) or (direction == -1 and side < 0):
            return True
        return False

    for idx, point in enumerate(points[1:], start=1):
        mark_equity(point.close)
        while True:
            move = point.close - last_close
            direction = 0
            close_bricks = 1
            if run_dir == 1:
                if move >= variant.brick_size:
                    direction = 1
                elif move <= -2.0 * variant.brick_size:
                    direction = -1
                    close_bricks = 2
            elif run_dir == -1:
                if move <= -variant.brick_size:
                    direction = -1
                elif move >= 2.0 * variant.brick_size:
                    direction = 1
                    close_bricks = 2
            else:
                if move >= variant.brick_size:
                    direction = 1
                elif move <= -variant.brick_size:
                    direction = -1
            if direction == 0:
                break

            last_close += direction * variant.brick_size * close_bricks
            if direction == run_dir:
                run_len += 1
            else:
                run_dir = direction
                run_len = 1
            brick = Brick(timestamp=point.timestamp, close=last_close, direction=direction, run_len=run_len)
            bricks.append(brick)
            result.bricks += 1

            can_trade_clock_bar = last_trade_ts is None or point.timestamp != last_trade_ts
            if not can_trade_clock_bar or block_entry(idx, point, direction, run_len):
                continue

            if direction == 1 and side <= 0:
                fill = last_close + slippage
                close_position(fill)
                new_qty = trade_qty(fixed_notional, last_close)
                if new_qty > 0:
                    side = 1
                    qty = new_qty
                    entry_fill = fill
                    result.entries += 1
                    last_trade_ts = point.timestamp
                    entry_times.append(point.timestamp)
            elif direction == -1 and side >= 0:
                fill = last_close - slippage
                close_position(fill)
                new_qty = trade_qty(fixed_notional, last_close)
                if new_qty > 0:
                    side = -1
                    qty = new_qty
                    entry_fill = fill
                    result.entries += 1
                    last_trade_ts = point.timestamp
                    entry_times.append(point.timestamp)

            if variant.cooldown and entry_times:
                window_start = point.timestamp - timedelta(minutes=10)
                entry_times = [ts for ts in entry_times if ts >= window_start]
                if len(entry_times) >= 3:
                    cooldown_until = point.timestamp + timedelta(minutes=20)
                    entry_times = []
            mark_equity(last_close)

    if side:
        final_fill = points[-1].close - slippage if side == 1 else points[-1].close + slippage
        close_position(final_fill)
    mark_equity(points[-1].close)
    result.realized_pnl = realized
    result.final_equity = realized
    result.max_drawdown = max_drawdown
    result.open_position_side = side
    return result


def variants(base_brick: float) -> list[Variant]:
    regular = (("09:31:00", "15:59:59"),)
    return [
        Variant("A_current_production", "A_baseline", "Current NVDA production E1 reversal, brick 1.149851.", base_brick),
        Variant("B_efficiency_180s_ge_0p35", "efficiency", "Entry only when 3-minute price efficiency >= 0.35.", base_brick, efficiency_seconds=180, efficiency_threshold=0.35),
        Variant("B_efficiency_180s_ge_0p40", "efficiency", "Entry only when 3-minute price efficiency >= 0.40.", base_brick, efficiency_seconds=180, efficiency_threshold=0.40),
        Variant("B_efficiency_300s_ge_0p35", "efficiency", "Entry only when 5-minute price efficiency >= 0.35.", base_brick, efficiency_seconds=300, efficiency_threshold=0.35),
        Variant("B_efficiency_300s_ge_0p40", "efficiency", "Entry only when 5-minute price efficiency >= 0.40.", base_brick, efficiency_seconds=300, efficiency_threshold=0.40),
        Variant("B_efficiency_300s_ge_0p45", "efficiency", "Entry only when 5-minute price efficiency >= 0.45.", base_brick, efficiency_seconds=300, efficiency_threshold=0.45),
        Variant("B_renko_chop_last8", "renko_chop", "Block entries when last 8 bricks have 4+ flips or net/gross < 0.35.", base_brick, renko_chop=True),
        Variant("B_conditional_e2_in_chop", "conditional_e2", "E1 in normal regime, E2 in choppy regime, skip very choppy regime.", base_brick, conditional_e2=True),
        Variant("B_cooldown_3rev_10m_pause20m", "cooldown", "After 3 entries in 10 minutes, pause new entries for 20 minutes.", base_brick, cooldown=True),
        Variant("B_cooldown_plus_loss_e2", "cooldown", "Cooldown plus require E2 after two consecutive closed losses.", base_brick, cooldown=True, loss_e2=True),
        Variant("B_start_0935", "time", "Delay entries until 09:35 ET.", base_brick, windows=(("09:35:00", "15:59:59"),)),
        Variant("B_start_0945", "time", "Delay entries until 09:45 ET.", base_brick, windows=(("09:45:00", "15:59:59"),)),
        Variant("B_no_lunch_1130_1330", "time", "Block entries during 11:30-13:30 ET.", base_brick, windows=(("09:31:00", "11:30:00"), ("13:30:00", "15:59:59"))),
        Variant("B_start0935_no_lunch", "time", "Start 09:35 ET and block 11:30-13:30 ET.", base_brick, windows=(("09:35:00", "11:30:00"), ("13:30:00", "15:59:59"))),
        Variant("B_start0945_no_lunch", "time", "Start 09:45 ET and block 11:30-13:30 ET.", base_brick, windows=(("09:45:00", "11:30:00"), ("13:30:00", "15:59:59"))),
        Variant("B_larger_brick_1p10x", "larger_brick", "Use 1.10x current production brick size.", base_brick * 1.10),
        Variant("B_larger_brick_1p25x", "larger_brick", "Use 1.25x current production brick size.", base_brick * 1.25),
        Variant("B_larger_brick_1p50x", "larger_brick", "Use 1.50x current production brick size.", base_brick * 1.50),
    ]


def result_row(result: Result, baseline: Result, initial_capital: float) -> dict[str, object]:
    pnl_delta = result.realized_pnl - baseline.realized_pnl
    return {
        "variant": result.variant.name,
        "group": result.variant.group,
        "description": result.variant.description,
        "brick_size": result.variant.brick_size,
        "bricks": result.bricks,
        "entries": result.entries,
        "closed_trades": result.closed_trades,
        "wins": result.wins,
        "losses": result.losses,
        "win_rate_pct": result.win_rate_pct,
        "profit_factor": result.profit_factor,
        "net_pnl": result.realized_pnl,
        "return_pct": 100.0 * result.realized_pnl / initial_capital,
        "pnl_delta_vs_A": pnl_delta,
        "return_delta_pct_points_vs_A": 100.0 * pnl_delta / initial_capital,
        "max_drawdown": result.max_drawdown,
        "max_drawdown_pct": 100.0 * result.max_drawdown / initial_capital,
        "drawdown_delta_vs_A": result.max_drawdown - baseline.max_drawdown,
        "avg_trade_pnl": result.avg_trade_pnl,
        "trades_per_day": result.trades_per_day,
        "blocked_entries": result.blocked_entries,
        "blocked_efficiency": result.blocked_efficiency,
        "blocked_renko_chop": result.blocked_renko_chop,
        "blocked_conditional_e2": result.blocked_conditional_e2,
        "blocked_cooldown": result.blocked_cooldown,
        "blocked_session": result.blocked_session,
    }


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def fmt(value: object, decimals: int = 2) -> str:
    if isinstance(value, float):
        if math.isinf(value):
            return "inf"
        return f"{value:.{decimals}f}"
    return str(value)


def write_markdown(path: Path, rows: list[dict[str, object]], metadata: dict[str, object]) -> None:
    ranked = sorted(rows, key=lambda row: float(row["net_pnl"]), reverse=True)
    baseline = next(row for row in rows if row["variant"] == "A_current_production")
    lines = [
        "# NVDA Chop A/B Test",
        "",
        f"Source: `{metadata['source']}`",
        f"Period: `{metadata['first_timestamp']}` to `{metadata['last_timestamp']}`",
        f"Production brick: `{metadata['production_brick_size']}`",
        f"Initial capital / fixed notional: `${metadata['initial_capital']}` / `${metadata['fixed_notional']}`",
        f"Slippage: `${metadata['slippage_per_order']}` per fill",
        "",
        "## Baseline",
        "",
        "| Variant | Net PnL | Return % | Max DD % | Trades | Win % | PF |",
        "|---|---:|---:|---:|---:|---:|---:|",
        f"| {baseline['variant']} | {fmt(baseline['net_pnl'])} | {fmt(baseline['return_pct'])} | {fmt(baseline['max_drawdown_pct'])} | {baseline['closed_trades']} | {fmt(baseline['win_rate_pct'])} | {fmt(baseline['profit_factor'])} |",
        "",
        "## Ranked Variants",
        "",
        "| Rank | Variant | Group | Net PnL | Delta vs A | Return % | Max DD % | Trades | Blocked |",
        "|---:|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for idx, row in enumerate(ranked, start=1):
        lines.append(
            "| "
            f"{idx} | {row['variant']} | {row['group']} | {fmt(row['net_pnl'])} | "
            f"{fmt(row['pnl_delta_vs_A'])} | {fmt(row['return_pct'])} | {fmt(row['max_drawdown_pct'])} | "
            f"{row['closed_trades']} | {row['blocked_entries']} |"
        )
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- A is the current NVDA production E1 reversal trader.")
    lines.append("- B variants change one chop-reduction family at a time, except the combined cooldown/loss-confirmation test.")
    lines.append("- This test uses the historical 1-second close dataset already in the repo, so it is a controlled backtest rather than live broker-fill PnL.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def heat_color(delta: float, max_abs: float) -> str:
    if max_abs <= 0:
        return "#f3f4f6"
    ratio = min(1.0, abs(delta) / max_abs)
    if delta >= 0:
        red = int(230 - 150 * ratio)
        green = int(245 - 50 * ratio)
        blue = int(235 - 120 * ratio)
    else:
        red = int(250 - 25 * ratio)
        green = int(230 - 145 * ratio)
        blue = int(230 - 145 * ratio)
    return f"#{red:02x}{green:02x}{blue:02x}"


def write_heatmap(path: Path, rows: list[dict[str, object]]) -> None:
    ordered = sorted(rows, key=lambda row: (str(row["group"]), -float(row["pnl_delta_vs_A"])))
    max_abs = max(abs(float(row["pnl_delta_vs_A"])) for row in ordered) or 1.0
    cell_w = 214
    cell_h = 72
    cols = 3
    margin = 26
    top = 86
    width = margin * 2 + cols * cell_w
    height = top + math.ceil(len(ordered) / cols) * cell_h + 52
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#fbfcf8"/>',
        "<style>text{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Arial,sans-serif;fill:#17211d}.title{font-size:22px;font-weight:760}.sub{font-size:12px;fill:#5f6c65}.name{font-size:11px;font-weight:750}.metric{font-size:14px;font-weight:760}.small{font-size:10px;fill:#4e5a54}</style>",
        '<text x="26" y="36" class="title">NVDA A/B Chop-Reducer Delta vs Current Production</text>',
        '<text x="26" y="58" class="sub">Color and value show net PnL delta versus A baseline on the June 2026 production-selection dataset.</text>',
    ]
    for idx, row in enumerate(ordered):
        col = idx % cols
        line = idx // cols
        x = margin + col * cell_w
        y = top + line * cell_h
        delta = float(row["pnl_delta_vs_A"])
        fill = heat_color(delta, max_abs)
        parts.append(f'<rect x="{x}" y="{y}" width="{cell_w - 10}" height="{cell_h - 10}" rx="6" fill="{fill}" stroke="#d5ded8"/>')
        parts.append(f'<text x="{x + 10}" y="{y + 19}" class="name">{row["variant"]}</text>')
        parts.append(f'<text x="{x + 10}" y="{y + 41}" class="metric">{delta:+.2f}</text>')
        parts.append(
            f'<text x="{x + 10}" y="{y + 57}" class="small">ret {float(row["return_pct"]):.2f}% | dd {float(row["max_drawdown_pct"]):.2f}% | trades {row["closed_trades"]}</text>'
        )
    parts.append("</svg>")
    path.write_text("\n".join(parts) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    points = load_points(Path(args.csv))
    results = [
        simulate(
            points,
            variant,
            initial_capital=args.initial_capital,
            fixed_notional=args.fixed_notional,
            slippage=args.slippage,
        )
        for variant in variants(args.brick_size)
    ]
    baseline = next(result for result in results if result.variant.name == "A_current_production")
    rows = [result_row(result, baseline, args.initial_capital) for result in results]
    rows_ranked = sorted(rows, key=lambda row: float(row["net_pnl"]), reverse=True)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    metadata = {
        "method": "nvda_e1_reversal_chop_ab_v1",
        "source": args.csv,
        "symbol": args.symbol.upper(),
        "first_timestamp": points[0].timestamp.isoformat(),
        "last_timestamp": points[-1].timestamp.isoformat(),
        "price_points": len(points),
        "trading_days": trading_day_count(points),
        "production_brick_size": args.brick_size,
        "initial_capital": args.initial_capital,
        "fixed_notional": args.fixed_notional,
        "slippage_per_order": args.slippage,
        "baseline_variant": baseline.variant.name,
    }
    write_csv(out_dir / "nvda_chop_ab_results.csv", rows_ranked)
    write_csv(out_dir / "nvda_chop_ab_results_unsorted.csv", rows)
    (out_dir / "metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (out_dir / "summary.json").write_text(
        json.dumps({"metadata": metadata, "results": rows_ranked}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_markdown(out_dir / "NVDA_CHOP_AB_SUMMARY.md", rows, metadata)
    write_heatmap(out_dir / "nvda_chop_ab_delta_heatmap.svg", rows)
    print(json.dumps({"metadata": metadata, "top": rows_ranked[:8], "output_dir": str(out_dir)}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
