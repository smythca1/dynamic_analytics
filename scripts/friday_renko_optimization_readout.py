from __future__ import annotations

import argparse
import csv
import html
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


METRICS = (
    ("net_pnl_dollars", "Net P&L", "higher"),
    ("average_trade_pnl_dollars", "Expectancy", "higher"),
    ("profit_factor", "Profit Factor", "higher"),
    ("max_drawdown_dollars", "Max Drawdown", "lower"),
    ("closed_trades", "Trade Count", "higher"),
    ("win_rate_pct", "Win Rate", "higher"),
)

DEFAULT_FLOOR = 0.16
FLOOR = DEFAULT_FLOOR
DEFAULT_MIN_TRADES = 10
BRICK_STEP = 0.02
NEIGHBOR_WINDOW = 0.04
EPSILON = 1e-9


@dataclass(frozen=True)
class RunInputs:
    root: Path
    reversal_dir: Path
    tp_sl_dir: Path
    output_dir: Path
    min_trades: int = DEFAULT_MIN_TRADES
    floor: float = DEFAULT_FLOOR
    previous_root: Path | None = None
    previous_reversal_dir: Path | None = None
    previous_tp_sl_dir: Path | None = None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build Friday Renko optimizer readout and heatmaps.")
    parser.add_argument("--root", required=True)
    parser.add_argument("--reversal-id", default="friday_reversal_20260905")
    parser.add_argument("--tp-sl-id", default="friday_tp_sl_20260905")
    parser.add_argument("--output", default="readout")
    parser.add_argument("--min-trades", type=int, default=DEFAULT_MIN_TRADES)
    parser.add_argument("--floor", type=float, default=DEFAULT_FLOOR)
    parser.add_argument("--previous-root")
    parser.add_argument("--previous-reversal-id", default="friday_reversal_20260905")
    parser.add_argument("--previous-tp-sl-id", default="friday_tp_sl_20260905")
    args = parser.parse_args(argv)
    if args.min_trades < 1:
        parser.error("--min-trades must be >= 1")
    if args.floor <= 0:
        parser.error("--floor must be > 0")

    root = Path(args.root)
    previous_root = Path(args.previous_root) if args.previous_root else None
    inputs = RunInputs(
        root=root,
        reversal_dir=root / "optimizations" / args.reversal_id,
        tp_sl_dir=root / "optimizations" / args.tp_sl_id,
        output_dir=root / args.output,
        min_trades=args.min_trades,
        floor=args.floor,
        previous_root=previous_root,
        previous_reversal_dir=previous_root / "optimizations" / args.previous_reversal_id if previous_root else None,
        previous_tp_sl_dir=previous_root / "optimizations" / args.previous_tp_sl_id if previous_root else None,
    )
    inputs.output_dir.mkdir(parents=True, exist_ok=True)
    heatmap_dir = inputs.output_dir / "heatmaps"
    heatmap_dir.mkdir(parents=True, exist_ok=True)

    reversal_rows = load_rows(inputs.reversal_dir / "all_candidates.csv")
    tp_sl_rows = load_rows(inputs.tp_sl_dir / "all_candidates.csv")
    all_rows = reversal_rows + tp_sl_rows
    heatmap_manifest: list[dict[str, Any]] = []
    recommendations: dict[tuple[str, str], dict[str, Any]] = {}

    for strategy, rows in (("Reversal", reversal_rows), ("TP/SL", tp_sl_rows)):
        for symbol in sorted({str(row.get("symbol")) for row in rows if row.get("symbol")}):
            symbol_rows = [row for row in rows if row.get("symbol") == symbol]
            regimes = distinct_regimes(symbol_rows, strategy=strategy, min_trades=inputs.min_trades, floor=inputs.floor)
            current = first(row for row in symbol_rows if str(row.get("candidate_type", "")).startswith("CURRENT_PRODUCTION"))
            proposed = regimes[0]["leader"] if regimes else None
            recommendations[(symbol, strategy)] = {
                "symbol": symbol,
                "strategy": strategy,
                "current": current,
                "proposed": proposed,
                "regimes": regimes,
                "rows": symbol_rows,
                "min_trades": inputs.min_trades,
                "floor": inputs.floor,
                "decision": decision(current, proposed, min_trades=inputs.min_trades),
            }
            heatmap_manifest.extend(
                write_heatmaps(
                    heatmap_dir,
                    symbol=symbol,
                    strategy=strategy,
                    rows=symbol_rows,
                    proposed=proposed,
                    current=current,
                    min_trades=inputs.min_trades,
                    floor=inputs.floor,
                )
            )

    previous_recommendations = previous_floor_recommendations(inputs)
    write_csv(inputs.output_dir / "top_regimes.csv", regime_csv_rows(recommendations))
    write_csv(inputs.output_dir / "current_vs_proposed.csv", comparison_csv_rows(recommendations))
    write_csv(inputs.output_dir / "three_way_comparison.csv", three_way_comparison_rows(recommendations, previous_recommendations))
    write_csv(inputs.output_dir / "reopened_region_candidates.csv", reopened_region_rows(recommendations, floor=inputs.floor))
    write_csv(inputs.output_dir / "executive_summary.csv", executive_summary_rows(recommendations, previous_recommendations))
    write_json(inputs.output_dir / "heatmap_manifest.json", heatmap_manifest)
    write_json(inputs.output_dir / "run_summary.json", run_summary(inputs, all_rows, heatmap_manifest, recommendations, previous_recommendations))
    (inputs.output_dir / "report.md").write_text(report_markdown(inputs, recommendations, heatmap_manifest, previous_recommendations), encoding="utf-8")
    return 0


def load_rows(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def distinct_regimes(
    rows: list[dict[str, Any]],
    *,
    strategy: str,
    min_trades: int = DEFAULT_MIN_TRADES,
    floor: float = DEFAULT_FLOOR,
) -> list[dict[str, Any]]:
    grid = [
        row
        for row in rows
        if row.get("candidate_type") == "BRICK_PERCENT_GRID"
        and row.get("candidate_status") == "COMPLETED"
        and num(row.get("brick_pct")) >= floor - EPSILON
    ]
    eligible_by_family: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in grid:
        if credible_regime_member(row, min_trades=min_trades):
            eligible_by_family[regime_family(row, strategy=strategy)].append(row)

    regimes: list[dict[str, Any]] = []
    for family, family_rows in eligible_by_family.items():
        sorted_rows = sorted(family_rows, key=lambda item: num(item.get("brick_pct")))
        for cluster in contiguous_clusters(sorted_rows):
            leader = max(cluster, key=rank_key)
            neighbor_rows = neighbor_rows_for(rows, row=leader, strategy=strategy, floor=floor)
            regimes.append(
                {
                    "leader": leader,
                    "members": tuple(cluster),
                    "family": family,
                    "neighbor_rows": tuple(neighbor_rows),
                    "neighbor_strength": neighbor_strength(leader, neighbor_rows),
                    "boundary_truncated": num(leader.get("brick_pct")) <= floor + NEIGHBOR_WINDOW + EPSILON,
                    "floor": floor,
                }
            )
    regimes.sort(key=regime_rank_key, reverse=True)
    return [with_regime_metadata(regime, min_trades=min_trades, floor=floor) for regime in regimes[:3]]


def credible_regime_member(row: dict[str, Any], *, min_trades: int = DEFAULT_MIN_TRADES) -> bool:
    return (
        num(row.get("closed_trades")) >= min_trades
        and num(row.get("net_pnl_dollars")) > 0
        and num(row.get("profit_factor")) > 1
    )


def contiguous_clusters(rows: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    if not rows:
        return []
    clusters: list[list[dict[str, Any]]] = [[rows[0]]]
    previous = num(rows[0].get("brick_pct"))
    for row in rows[1:]:
        brick = num(row.get("brick_pct"))
        if brick - previous <= NEIGHBOR_WINDOW + EPSILON:
            clusters[-1].append(row)
        else:
            clusters.append([row])
        previous = brick
    return clusters


def neighbor_rows_for(
    rows: list[dict[str, Any]],
    *,
    row: dict[str, Any],
    strategy: str,
    floor: float = DEFAULT_FLOOR,
) -> list[dict[str, Any]]:
    family = regime_family(row, strategy=strategy)
    center = num(row.get("brick_pct"))
    return sorted(
        [
            item
            for item in rows
            if item.get("candidate_type") == "BRICK_PERCENT_GRID"
            and item.get("candidate_status") == "COMPLETED"
            and regime_family(item, strategy=strategy) == family
            and abs(num(item.get("brick_pct")) - center) <= NEIGHBOR_WINDOW + EPSILON
            and num(item.get("brick_pct")) >= floor - EPSILON
        ],
        key=lambda item: num(item.get("brick_pct")),
    )


def with_regime_metadata(regime: dict[str, Any], *, min_trades: int = DEFAULT_MIN_TRADES, floor: float = DEFAULT_FLOOR) -> dict[str, Any]:
    leader = regime["leader"]
    members = list(regime["members"])
    member_count = len(members)
    start = num(members[0].get("brick_pct"))
    end = num(members[-1].get("brick_pct"))
    enriched = dict(regime)
    enriched.update(
        {
            "member_count": member_count,
            "range_start_pct": start,
            "range_end_pct": end,
            "confidence": confidence(regime),
            "trade_count_quality": trade_count_quality(leader, min_trades=min_trades),
            "session_consistency": session_consistency(leader),
            "one_session_dependence": one_session_dependence(leader),
            "primary_weakness": primary_weakness(regime, floor=floor),
            "neighbor_summary": neighbor_summary(regime, floor=floor),
        }
    )
    return enriched


def regime_rank_key(regime: dict[str, Any]) -> tuple[float, float, float, float, float, float, float]:
    leader = regime["leader"]
    return (
        num(leader.get("robustness_score")),
        num(leader.get("return_on_allocated_capital_pct"), leader.get("strategy_return_pct")),
        num(leader.get("net_pnl_dollars")),
        num(leader.get("profit_factor")),
        -num(leader.get("max_drawdown_pct")),
        min(num(leader.get("closed_trades")), 1000.0),
        float(len(regime.get("members") or ())),
    )


def rank_key(row: dict[str, Any]) -> tuple[float, float, float, float, float]:
    return (
        num(row.get("robustness_score")),
        num(row.get("return_on_allocated_capital_pct"), row.get("strategy_return_pct")),
        num(row.get("net_pnl_dollars")),
        num(row.get("profit_factor")),
        -num(row.get("max_drawdown_pct")),
    )


def regime_family(row: dict[str, Any], *, strategy: str) -> tuple[Any, ...]:
    if strategy == "Reversal":
        return ("E1",)
    return (
        row.get("entry_confirmation"),
        row.get("take_profit_bricks"),
        row.get("stop_loss_bricks"),
    )


def neighbor_strength(row: dict[str, Any] | None, neighbor_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None = None) -> str:
    if row is None:
        return "Weak"
    score = num(row.get("robustness_score"))
    local = num(row.get("local_profitable_pct"))
    if neighbor_rows:
        profitable = sum(1 for item in neighbor_rows if num(item.get("net_pnl_dollars")) > 0 and num(item.get("profit_factor")) > 1)
        local = max(local, profitable / len(neighbor_rows) * 100.0)
    if score >= 0.75 and local >= 80:
        return "Strong"
    if score >= 0.55 and local >= 60:
        return "Moderate"
    return "Weak"


def confidence(regime: dict[str, Any]) -> str:
    leader = regime["leader"]
    score = 0
    if regime["neighbor_strength"] == "Strong":
        score += 2
    elif regime["neighbor_strength"] == "Moderate":
        score += 1
    if len(regime.get("members") or ()) >= 3:
        score += 1
    if num(leader.get("closed_trades")) >= 20:
        score += 1
    if num(leader.get("profit_factor")) >= 1.5:
        score += 1
    if session_consistency_ratio(leader) >= 0.6:
        score += 1
    if regime.get("boundary_truncated"):
        score -= 1
    if one_session_dependence(leader) == "yes":
        score -= 1
    if score >= 5:
        return "High"
    if score >= 3:
        return "Medium"
    return "Low"


def trade_count_quality(row: dict[str, Any], *, min_trades: int = DEFAULT_MIN_TRADES) -> str:
    trades = num(row.get("closed_trades"))
    if trades < min_trades:
        return "INSUFFICIENT SAMPLE"
    if trades >= 25:
        return "promotion-quality"
    return "minimum promotion-quality"


def session_consistency(row: dict[str, Any] | None) -> str:
    if not row:
        return "UNKNOWN"
    sessions = int(num(row.get("session_count")))
    winners = int(num(row.get("profitable_sessions")))
    losers = int(num(row.get("losing_sessions")))
    if sessions <= 0:
        return "UNKNOWN"
    return f"{winners}/{sessions} profitable sessions, {losers} losing"


def session_consistency_ratio(row: dict[str, Any]) -> float:
    sessions = num(row.get("session_count"))
    if sessions <= 0:
        return 0.0
    return num(row.get("profitable_sessions")) / sessions


def one_session_dependence(row: dict[str, Any] | None) -> str:
    if not row:
        return "UNKNOWN"
    sessions = num(row.get("session_count"))
    winners = num(row.get("profitable_sessions"))
    if sessions <= 1:
        return "UNKNOWN"
    if num(row.get("net_pnl_dollars")) > 0 and winners <= 1:
        return "yes"
    if winners <= sessions / 2 and num(row.get("profit_factor")) < 1.25:
        return "possible"
    return "no"


def primary_weakness(regime: dict[str, Any], *, floor: float = DEFAULT_FLOOR) -> str:
    leader = regime["leader"]
    if regime.get("boundary_truncated"):
        return f"truncated lower neighbor evidence at {floor:.2f}% floor"
    if one_session_dependence(leader) in {"yes", "possible"}:
        return "session consistency is weak"
    if regime["neighbor_strength"] == "Weak":
        return "weak neighboring-brick support"
    if num(leader.get("closed_trades")) < 20:
        return "thin trade sample"
    if num(leader.get("profit_factor")) < 1.5:
        return "modest profit factor"
    if num(leader.get("max_drawdown_pct")) > 10:
        return "elevated drawdown"
    return "none material in this sample"


def neighbor_summary(regime: dict[str, Any], *, floor: float = DEFAULT_FLOOR) -> str:
    leader = regime["leader"]
    center = num(leader.get("brick_pct"))
    by_brick = {round(num(row.get("brick_pct")), 2): row for row in regime.get("neighbor_rows") or ()}
    parts = []
    for offset in (-0.04, -0.02, 0.0, 0.02, 0.04):
        brick = round(center + offset, 2)
        if brick < floor - EPSILON:
            parts.append(f"{brick:.2f}% below floor")
            continue
        row = by_brick.get(brick)
        if row is None:
            parts.append(f"{brick:.2f}% missing")
            continue
        parts.append(f"{brick:.2f}% {money(num(row.get('net_pnl_dollars')))} PF {pf(row.get('profit_factor'))}")
    return "; ".join(parts)


def decision(current: dict[str, Any] | None, proposed: dict[str, Any] | None, *, min_trades: int = DEFAULT_MIN_TRADES) -> str:
    if proposed is None:
        if current and num(current.get("net_pnl_dollars")) > 0 and num(current.get("profit_factor")) > 1:
            return "KEEP CURRENT"
        return "DISABLE / NO ACCEPTABLE REGIME"
    if num(proposed.get("closed_trades")) < min_trades:
        return "HOLD / NEED MORE EVIDENCE"
    if current is None:
        return "PROMOTE NEW REGIME"

    current_good = num(current.get("net_pnl_dollars")) > 0 and num(current.get("profit_factor")) > 1 and num(current.get("closed_trades")) >= min_trades
    pnl_delta = num(proposed.get("net_pnl_dollars")) - num(current.get("net_pnl_dollars"))
    return_delta = num(proposed.get("return_on_allocated_capital_pct"), proposed.get("strategy_return_pct")) - num(
        current.get("return_on_allocated_capital_pct"), current.get("strategy_return_pct")
    )
    dd_ratio = safe_ratio(num(proposed.get("max_drawdown_dollars")), num(current.get("max_drawdown_dollars")))
    meaningful = pnl_delta >= 25.0 and return_delta >= 0.25 and dd_ratio <= 1.5
    if meaningful:
        return "PROMOTE NEW REGIME"
    if current_good:
        return "KEEP CURRENT"
    return "HOLD / NEED MORE EVIDENCE"


def write_heatmaps(
    heatmap_dir: Path,
    *,
    symbol: str,
    strategy: str,
    rows: list[dict[str, Any]],
    proposed: dict[str, Any] | None,
    current: dict[str, Any] | None,
    min_trades: int = DEFAULT_MIN_TRADES,
    floor: float = DEFAULT_FLOOR,
) -> list[dict[str, Any]]:
    manifest: list[dict[str, Any]] = []
    grid = [row for row in rows if row.get("candidate_type") == "BRICK_PERCENT_GRID" and row.get("candidate_status") == "COMPLETED"]
    if strategy == "Reversal":
        for metric, label, direction in METRICS:
            path = heatmap_dir / f"{symbol.lower()}_reversal_{metric}.svg"
            write_reversal_strip(
                path,
                symbol=symbol,
                metric=metric,
                label=label,
                direction=direction,
                rows=grid,
                proposed=proposed,
                current=current,
                min_trades=min_trades,
                floor=floor,
            )
            manifest.append({"symbol": symbol, "strategy": strategy, "metric": metric, "artifact_path": str(path)})
        return manifest

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in grid:
        grouped[(str(row.get("entry_confirmation")), str(row.get("take_profit_bricks")))].append(row)
    for metric, label, direction in METRICS:
        for (entry, tp), group_rows in sorted(grouped.items()):
            path = heatmap_dir / f"{symbol.lower()}_tp_sl_{entry.lower()}_tp{tp}_{metric}.svg"
            write_tp_sl_heatmap(
                path,
                symbol=symbol,
                metric=metric,
                label=label,
                direction=direction,
                rows=group_rows,
                entry=entry,
                tp=tp,
                proposed=proposed,
                current=current,
                min_trades=min_trades,
                floor=floor,
            )
            manifest.append({"symbol": symbol, "strategy": strategy, "metric": metric, "entry": entry, "tp": tp, "artifact_path": str(path)})
    return manifest


def write_reversal_strip(
    path: Path,
    *,
    symbol: str,
    metric: str,
    label: str,
    direction: str,
    rows: list[dict[str, Any]],
    proposed: dict[str, Any] | None,
    current: dict[str, Any] | None,
    min_trades: int = DEFAULT_MIN_TRADES,
    floor: float = DEFAULT_FLOOR,
) -> None:
    bricks = sorted({num(row.get("brick_pct")) for row in rows})
    by_brick = {num(row.get("brick_pct")): row for row in rows}
    cell = 14
    left = 90
    top = 50
    width = left + cell * len(bricks) + 30
    height = 145
    values = [num(by_brick[brick].get(metric)) for brick in bricks if brick in by_brick]
    lo, hi = scale(values)
    proposed_brick = num(proposed.get("brick_pct")) if proposed else None
    current_brick = num(current.get("brick_pct")) if current else None
    parts = svg_header(width, height)
    parts.append(f'<text x="12" y="24" class="title">{escape(symbol)} Reversal - {escape(label)}</text>')
    parts.append(f'<text x="12" y="42" class="small">{floor:.2f}% floor; x-axis is Renko brick size %; blue outline marks {floor:.2f}-0.16% review region.</text>')
    for i, brick in enumerate(bricks):
        row = by_brick[brick]
        x = left + i * cell
        color = color_for(num(row.get(metric)), lo, hi, direction=direction)
        parts.append(f'<rect x="{x}" y="{top}" width="{cell}" height="{cell * 2}" fill="{color}" stroke="#d4d4d4"/>')
        if reopened_region_brick(brick, floor=floor):
            parts.append(f'<rect x="{x + 1}" y="{top + 1}" width="{cell - 2}" height="{cell * 2 - 2}" fill="none" stroke="#0284c7" stroke-width="1.5"/>')
        if num(row.get("closed_trades")) < min_trades:
            parts.append(f'<path d="M{x},{top + cell * 2} L{x + cell},{top}" stroke="#555" stroke-width="1"/>')
        if proposed_brick is not None and abs(proposed_brick - brick) < 0.0001:
            parts.append(f'<rect x="{x + 1}" y="{top + 1}" width="{cell - 2}" height="{cell * 2 - 2}" fill="none" stroke="#111" stroke-width="2"/>')
        if current_brick is not None and abs(current_brick - brick) < 0.0001:
            parts.append(f'<circle cx="{x + cell / 2}" cy="{top + cell}" r="4" fill="#2563eb"/>')
        if i % 5 == 0:
            parts.append(f'<text x="{x - 5}" y="{top + 47}" class="tick" transform="rotate(45 {x - 5},{top + 47})">{brick:.2f}</text>')
    parts.append(f'<text x="12" y="{top + 18}" class="axis">SL</text>')
    parts.append(f'<text x="{left}" y="{height - 16}" class="small">blue dot=current when on grid; black outline=proposed; current below floor is noted in report</text>')
    parts.append("</svg>")
    path.write_text("\n".join(parts), encoding="utf-8")


def write_tp_sl_heatmap(
    path: Path,
    *,
    symbol: str,
    metric: str,
    label: str,
    direction: str,
    rows: list[dict[str, Any]],
    entry: str,
    tp: str,
    proposed: dict[str, Any] | None,
    current: dict[str, Any] | None,
    min_trades: int = DEFAULT_MIN_TRADES,
    floor: float = DEFAULT_FLOOR,
) -> None:
    bricks = sorted({num(row.get("brick_pct")) for row in rows})
    sl_values = sorted({str(row.get("stop_loss_bricks")) for row in rows}, key=lambda item: num(item))
    by_cell = {(num(row.get("brick_pct")), str(row.get("stop_loss_bricks"))): row for row in rows}
    cell_w = 14
    cell_h = 22
    left = 95
    top = 55
    width = left + cell_w * len(bricks) + 30
    height = top + cell_h * len(sl_values) + 78
    values = [num(row.get(metric)) for row in rows]
    lo, hi = scale(values)
    proposed_key = tp_sl_marker_key(proposed)
    current_key = tp_sl_marker_key(current)
    parts = svg_header(width, height)
    parts.append(f'<text x="12" y="24" class="title">{escape(symbol)} TP/SL {escape(entry)} TP{escape(tp)} - {escape(label)}</text>')
    parts.append(f'<text x="12" y="42" class="small">Rows are stop-loss bricks; columns are Renko brick size %. Blue outline marks {floor:.2f}-0.16% review region.</text>')
    for y_index, sl in enumerate(sl_values):
        y = top + y_index * cell_h
        parts.append(f'<text x="24" y="{y + 15}" class="axis">SL{escape(sl)}</text>')
        for x_index, brick in enumerate(bricks):
            x = left + x_index * cell_w
            row = by_cell.get((brick, sl))
            if row is None:
                continue
            color = color_for(num(row.get(metric)), lo, hi, direction=direction)
            parts.append(f'<rect x="{x}" y="{y}" width="{cell_w}" height="{cell_h}" fill="{color}" stroke="#d4d4d4"/>')
            if reopened_region_brick(brick, floor=floor):
                parts.append(f'<rect x="{x + 1}" y="{y + 1}" width="{cell_w - 2}" height="{cell_h - 2}" fill="none" stroke="#0284c7" stroke-width="1.5"/>')
            if num(row.get("closed_trades")) < min_trades:
                parts.append(f'<path d="M{x},{y + cell_h} L{x + cell_w},{y}" stroke="#555" stroke-width="1"/>')
            cell_key = (entry, tp, sl, round(brick, 4))
            if proposed_key == cell_key:
                parts.append(f'<rect x="{x + 1}" y="{y + 1}" width="{cell_w - 2}" height="{cell_h - 2}" fill="none" stroke="#111" stroke-width="2"/>')
            if current_key == cell_key:
                parts.append(f'<circle cx="{x + cell_w / 2}" cy="{y + cell_h / 2}" r="4" fill="#2563eb"/>')
    for i, brick in enumerate(bricks):
        if i % 5 == 0:
            x = left + i * cell_w
            parts.append(f'<text x="{x - 5}" y="{height - 42}" class="tick" transform="rotate(45 {x - 5},{height - 42})">{brick:.2f}</text>')
    parts.append(f'<text x="{left}" y="{height - 12}" class="small">blue dot=current when on grid; black outline=proposed; current below floor is noted in report</text>')
    parts.append("</svg>")
    path.write_text("\n".join(parts), encoding="utf-8")


def tp_sl_marker_key(row: dict[str, Any] | None) -> tuple[str, str, str, float] | None:
    if not row:
        return None
    return (
        str(row.get("entry_confirmation")),
        str(row.get("take_profit_bricks")),
        str(row.get("stop_loss_bricks")),
        round(num(row.get("brick_pct")), 4),
    )


def report_markdown(
    inputs: RunInputs,
    recommendations: dict[tuple[str, str], dict[str, Any]],
    heatmaps: list[dict[str, Any]],
    previous_recommendations: dict[tuple[str, str], dict[str, Any]] | None = None,
) -> str:
    sessions = session_dates(inputs)
    lookback_label = f"{len(sessions)}-session" if sessions else "multi-session"
    lines = [
        f"# Friday Renko Optimization - Controlled {inputs.floor:.2f}% Floor Rerun",
        "",
        "Research only. No deployment, production config, service, state, broker order, or broker position changes were made.",
        "",
        "Sessions used: " + ", ".join(f"`{item}`" for item in sessions) + ".",
        "",
        f"Candidate floor: new candidates use `{inputs.floor:.2f}%` minimum Renko brick size. Current deployed benchmarks are replayed unchanged, including below-floor incumbents.",
        "",
        f"Promotion evidence floor: new candidates must have at least `{inputs.min_trades}` closed trades. This is a weak {lookback_label} promotion screen, not strong proof.",
        "",
        f"Execution-economics note: the `{inputs.floor:.2f}%` floor is a guardrail. These trade-based backtests do not reconstruct exact historical bid/ask spread, slippage, or fees.",
        "",
        "## Final Consolidated Readout",
        "",
    ]
    if previous_recommendations:
        lines.extend(executive_summary_table(recommendations, previous_recommendations))
        lines.extend(explicit_questions(recommendations, previous_recommendations, floor=inputs.floor))
        lines.append("")
    lines.extend(consolidated_table(recommendations, return_label=f"{len(sessions)}-Session Return" if sessions else "Return"))
    lines.extend(fleet_findings(recommendations))
    for symbol, strategy in sorted(recommendations):
        payload = recommendations[(symbol, strategy)]
        previous_payload = previous_recommendations.get((symbol, strategy)) if previous_recommendations else None
        lines.extend(section(symbol, strategy, payload, heatmaps, previous_payload=previous_payload, floor=inputs.floor))
    lines.extend(
        [
            "## Limitations",
            "",
            f"- {lookback_label.title()} research is still a short sample; promotion-quality trade count does not make it a long-horizon validation.",
            f"- The {inputs.floor:.2f}% floor is an execution-economics guardrail, not a proven optimal spread/slippage threshold.",
            "- Historical bid/ask spread, fees, partial fills, and broker queue position are not reconstructed from the canonical SIP research source.",
            "- Backtest execution uses the configured canonical model from the optimizer artifacts; live fills can differ.",
            f"- Boundary regimes at {inputs.floor:.2f}% have truncated lower-neighbor evidence and should be treated with lower confidence.",
            "",
            "## Artifacts",
            "",
            f"- Reversal optimizer: `{inputs.reversal_dir}`",
            f"- TP/SL optimizer: `{inputs.tp_sl_dir}`",
            f"- Heatmap manifest: `{inputs.output_dir / 'heatmap_manifest.json'}`",
            f"- Regime table: `{inputs.output_dir / 'top_regimes.csv'}`",
            f"- Current vs proposed table: `{inputs.output_dir / 'current_vs_proposed.csv'}`",
            f"- Three-way comparison table: `{inputs.output_dir / 'three_way_comparison.csv'}`",
            f"- Reopened-region candidate table: `{inputs.output_dir / 'reopened_region_candidates.csv'}`",
        ]
    )
    return "\n".join(lines) + "\n"


def section(
    symbol: str,
    strategy: str,
    payload: dict[str, Any],
    heatmaps: list[dict[str, Any]],
    *,
    previous_payload: dict[str, Any] | None = None,
    floor: float = DEFAULT_FLOOR,
) -> list[str]:
    current = payload["current"]
    proposed = payload["proposed"]
    regimes = payload["regimes"]
    rows = payload["rows"]
    lines = [f"## {symbol} {strategy}", ""]
    lines.append("| Rank | Regime / Brick Range | Representative Brick | Entry | TP | SL | Return | Net P&L | Max DD | Trades | Win % | PF | Expectancy | Neighbor Strength | Confidence |")
    lines.append("| ---- | -------------------- | -------------------: | ----- | -- | -: | -----: | ------: | -----: | -----: | ----: | -: | ---------: | ----------------- | ---------- |")
    if not regimes:
        lines.append("| - | No credible eligible regime | - | - | - | - | - | - | - | - | - | - | - | - | - |")
    for idx, regime in enumerate(regimes, start=1):
        row = regime["leader"]
        lines.append(
            "| "
            + " | ".join(
                [
                    str(idx),
                    regime_range_text(regime),
                    f"{num(row.get('brick_pct')):.2f}%",
                    str(row.get("entry_confirmation") or "E1"),
                    clean(row.get("take_profit_bricks")),
                    clean(row.get("stop_loss_bricks")),
                    fmt(num(row.get("return_on_allocated_capital_pct"), row.get("strategy_return_pct")), "%"),
                    money(num(row.get("net_pnl_dollars"))),
                    money(num(row.get("max_drawdown_dollars"))),
                    str(int(num(row.get("closed_trades")))),
                    fmt(num(row.get("win_rate_pct")), "%"),
                    pf(row.get("profit_factor")),
                    money(num(row.get("average_trade_pnl_dollars"))),
                    regime["neighbor_strength"],
                    regime["confidence"],
                ]
            )
            + " |"
        )
    lines.append("")
    if len(regimes) < 3:
        lines.append(f"Only {len(regimes)} credible distinct regime(s) met the profitability, PF, and {payload['min_trades']}-trade evidence filters.")
        lines.append("")
    for idx, regime in enumerate(regimes, start=1):
        lines.extend(regime_explanation(idx, regime, min_trades=payload["min_trades"]))
    lines.append("")
    lines.append(f"Decision: `{payload['decision']}`")
    lines.append("")
    if previous_payload:
        lines.extend(three_way_table(current, previous_payload.get("proposed"), proposed))
        lines.append("")
    lines.extend(compare_table(current, proposed))
    lines.append("")
    lines.extend(reopened_region_report(rows, strategy=strategy, floor=floor, min_trades=payload["min_trades"]))
    lines.append("")
    lines.extend(interpretation(strategy=strategy, current=current, proposed=proposed, regimes=regimes, rows=rows, min_trades=payload["min_trades"], floor=floor))
    lines.append("")
    lines.extend(candidates_to_avoid(rows, strategy=strategy, min_trades=payload["min_trades"], floor=floor))
    symbol_heatmaps = [item for item in heatmaps if item["symbol"] == symbol and item["strategy"] == strategy]
    lines.append("")
    lines.append("Heatmaps:")
    for item in symbol_heatmaps[:12]:
        lines.append(f"- `{item['artifact_path']}`")
    if len(symbol_heatmaps) > 12:
        lines.append(f"- plus {len(symbol_heatmaps) - 12} additional detailed heatmaps in manifest")
    lines.append("")
    return lines


def compare_table(current: dict[str, Any] | None, proposed: dict[str, Any] | None) -> list[str]:
    fields = [
        ("Brick", "brick_pct", "%"),
        ("Entry", "entry_confirmation", ""),
        ("TP", "take_profit_bricks", ""),
        ("SL", "stop_loss_bricks", ""),
        ("Return", "return_on_allocated_capital_pct", "%"),
        ("Net P&L", "net_pnl_dollars", "$"),
        ("Max DD", "max_drawdown_dollars", "$"),
        ("Trades", "closed_trades", ""),
        ("Win Rate", "win_rate_pct", "%"),
        ("Profit Factor", "profit_factor", ""),
        ("Expectancy", "average_trade_pnl_dollars", "$"),
        ("Neighbor Robustness", "robustness_score", ""),
        ("Session Consistency", "session_consistency", ""),
    ]
    lines = [
        "| Metric | CURRENT | PROPOSED | Difference |",
        "| ------ | ------: | -------: | ---------: |",
    ]
    for label, key, unit in fields:
        c = session_consistency(current) if key == "session_consistency" else field_value(current, key)
        p = session_consistency(proposed) if key == "session_consistency" else field_value(proposed, key)
        lines.append(f"| {label} | {render(c, unit)} | {render(p, unit)} | {render_diff(c, p, unit)} |")
    return lines


def three_way_table(
    current: dict[str, Any] | None,
    previous: dict[str, Any] | None,
    proposed: dict[str, Any] | None,
) -> list[str]:
    fields = [
        ("Brick", "brick_pct", "%"),
        ("Entry", "entry_confirmation", ""),
        ("TP", "take_profit_bricks", ""),
        ("SL", "stop_loss_bricks", ""),
        ("Net Return", "return_on_allocated_capital_pct", "%"),
        ("Net P&L", "net_pnl_dollars", "$"),
        ("Max DD", "max_drawdown_dollars", "$"),
        ("Trades", "closed_trades", ""),
        ("Win Rate", "win_rate_pct", "%"),
        ("Profit Factor", "profit_factor", ""),
        ("Expectancy", "average_trade_pnl_dollars", "$"),
        ("Neighbor Robustness", "robustness_score", ""),
        ("Session Consistency", "session_consistency", ""),
    ]
    lines = [
        "| Metric | Current | 0.16% Run | 0.12% Run |",
        "| ------ | ------: | --------: | --------: |",
    ]
    for label, key, unit in fields:
        c = session_consistency(current) if key == "session_consistency" else field_value(current, key)
        p16 = session_consistency(previous) if key == "session_consistency" else field_value(previous, key)
        p12 = session_consistency(proposed) if key == "session_consistency" else field_value(proposed, key)
        lines.append(f"| {label} | {render(c, unit)} | {render(p16, unit)} | {render(p12, unit)} |")
    return lines


def reopened_region_report(
    rows: list[dict[str, Any]],
    *,
    strategy: str,
    floor: float,
    min_trades: int,
) -> list[str]:
    lines = [f"Reopened {floor:.2f}-0.16% region:"]
    lines.append("| Brick | Candidates | Eligible | Profitable | Min Trades | Best Cell | Worst Net P&L | Median Net P&L |")
    lines.append("| ----: | ---------: | -------: | ---------: | ---------: | --------- | ------------: | -------------: |")
    for brick in review_bricks(floor):
        brick_rows = [
            row
            for row in rows
            if row.get("candidate_type") == "BRICK_PERCENT_GRID"
            and row.get("candidate_status") == "COMPLETED"
            and abs(num(row.get("brick_pct")) - brick) < 0.0001
        ]
        if not brick_rows:
            lines.append(f"| {brick:.2f}% | 0 | 0 | 0 | 0 | missing | UNKNOWN | UNKNOWN |")
            continue
        eligible = [row for row in brick_rows if row.get("robustness_eligible") in {True, "True", "true"}]
        profitable = [row for row in brick_rows if num(row.get("net_pnl_dollars")) > 0]
        enough_trades = [row for row in brick_rows if num(row.get("closed_trades")) >= min_trades]
        best = max(brick_rows, key=rank_key)
        pnls = sorted(num(row.get("net_pnl_dollars")) for row in brick_rows)
        median_pnl = pnls[len(pnls) // 2] if len(pnls) % 2 else (pnls[len(pnls) // 2 - 1] + pnls[len(pnls) // 2]) / 2
        lines.append(
            "| "
            + " | ".join(
                [
                    f"{brick:.2f}%",
                    str(len(brick_rows)),
                    str(len(eligible)),
                    str(len(profitable)),
                    str(len(enough_trades)),
                    cell_summary(best),
                    money(min(pnls)),
                    money(median_pnl),
                ]
            )
            + " |"
        )
    lines.append(f"Full {strategy} candidate rows for this region are in `reopened_region_candidates.csv`.")
    return lines


def interpretation(
    *,
    strategy: str,
    current: dict[str, Any] | None,
    proposed: dict[str, Any] | None,
    regimes: list[dict[str, Any]],
    rows: list[dict[str, Any]],
    min_trades: int = DEFAULT_MIN_TRADES,
    floor: float = DEFAULT_FLOOR,
) -> list[str]:
    leader = regimes[0]["leader"] if regimes else None
    lines = ["Interpretation:"]
    lines.append(f"- Strongest profitable plateau: {describe_regime(regimes[0]) if regimes else 'none found under the eligibility filters'}")
    lines.append(f"- Recommended candidate broad-regime status: {neighbor_strength(leader) if leader else 'not applicable'}")
    lines.append(f"- Small-brick behavior: {landscape_slice(rows, floor, 0.30, min_trades=min_trades)}")
    lines.append(f"- Mid-range behavior: {landscape_slice(rows, 0.32, 1.00, min_trades=min_trades)}")
    lines.append(f"- Large-brick behavior: {landscape_slice(rows, 1.02, 2.00, min_trades=min_trades)}")
    lines.append(f"- Performance clusters at {floor:.2f}% floor: {yes_no(leader is not None and num(leader.get('brick_pct')) <= floor + EPSILON)}")
    lines.append(f"- Current region: {current_region(current, min_trades=min_trades, floor=floor)}")
    lines.append("- Isolated high-P&L cells: rejected when local profitability/robustness filters failed; see all_candidates.csv for rows with isolated_spike_flag.")
    lines.append(f"- Trade-count collapse: see `{strategy}` trade-count heatmaps; candidates below {min_trades} closed trades are hatched and labeled insufficient sample by eligibility.")
    lines.append(f"- Expectancy trend: inspect expectancy heatmaps; proposed expectancy is {money(num(proposed.get('average_trade_pnl_dollars')) if proposed else 0)}.")
    lines.append(f"- PF trend: inspect PF heatmaps; proposed PF is {pf(proposed.get('profit_factor') if proposed else None)}.")
    lines.append(f"- Drawdown: proposed max drawdown is {money(num(proposed.get('max_drawdown_dollars')) if proposed else 0)}.")
    lines.append(f"- One-session dependence: {one_session_dependence(proposed)}.")
    lines.append("- Regime-shift evidence: based on current-vs-proposed deltas plus plateau/neighbor strength; marginal improvements default to keep/hold.")
    return lines


def describe_regime(regime: dict[str, Any]) -> str:
    row = regime["leader"]
    return (
        f"{regime_range_text(regime)} represented by {num(row.get('brick_pct')):.2f}% "
        f"{row.get('entry_confirmation')} TP{clean(row.get('take_profit_bricks'))} SL{clean(row.get('stop_loss_bricks'))} "
        f"with {regime['neighbor_strength'].lower()} neighbor evidence and {regime['confidence'].lower()} confidence"
    )


def current_region(current: dict[str, Any] | None, *, min_trades: int = DEFAULT_MIN_TRADES, floor: float = DEFAULT_FLOOR) -> str:
    if not current:
        return "no current benchmark row found"
    sample = "INSUFFICIENT SAMPLE" if num(current.get("closed_trades")) < min_trades else "promotion-quality sample"
    floor_note = f"below {floor:.2f}% floor" if num(current.get("brick_pct")) < floor else f"inside/above {floor:.2f}% floor"
    return f"{floor_note}, {sample}, net {money(num(current.get('net_pnl_dollars')))}, PF {pf(current.get('profit_factor'))}"


def regime_range_text(regime: dict[str, Any]) -> str:
    start = num(regime.get("range_start_pct"))
    end = num(regime.get("range_end_pct"))
    count = int(num(regime.get("member_count")))
    text = f"{start:.2f}%" if abs(start - end) < EPSILON else f"{start:.2f}-{end:.2f}%"
    if count > 1:
        text += f" ({count} cells)"
    if regime.get("boundary_truncated"):
        text += " boundary-truncated"
    return text


def regime_explanation(rank: int, regime: dict[str, Any], *, min_trades: int = DEFAULT_MIN_TRADES) -> list[str]:
    leader = regime["leader"]
    return [
        f"Rank {rank} regime details:",
        f"- Why it ranks here: robustness {fmt(num(leader.get('robustness_score')), '')}, return {fmt(num(leader.get('return_on_allocated_capital_pct'), leader.get('strategy_return_pct')), '%')}, PF {pf(leader.get('profit_factor'))}, and {trade_count_quality(leader, min_trades=min_trades)} trade count.",
        f"- Strongest representative candidate: {num(leader.get('brick_pct')):.2f}% {leader.get('entry_confirmation')} TP{clean(leader.get('take_profit_bricks'))} SL{clean(leader.get('stop_loss_bricks'))}, candidate `{leader.get('candidate_id')}`.",
        f"- Neighboring-brick performance: {regime['neighbor_summary']}.",
        f"- Session consistency: {regime['session_consistency']}.",
        f"- Trade-count quality: {regime['trade_count_quality']} with {int(num(leader.get('closed_trades')))} closed trades.",
        f"- Primary weakness: {regime['primary_weakness']}.",
        f"- One-session dependence: {regime['one_session_dependence']}.",
        f"- Confidence: {regime['confidence']}.",
        "",
    ]


def landscape_slice(rows: list[dict[str, Any]], start: float, end: float, *, min_trades: int = DEFAULT_MIN_TRADES) -> str:
    grid = [
        row
        for row in rows
        if row.get("candidate_type") == "BRICK_PERCENT_GRID"
        and row.get("candidate_status") == "COMPLETED"
        and start - EPSILON <= num(row.get("brick_pct")) <= end + EPSILON
    ]
    if not grid:
        return f"no candidates in {start:.2f}-{end:.2f}%"
    profitable = [row for row in grid if num(row.get("net_pnl_dollars")) > 0]
    enough_trades = [row for row in grid if num(row.get("closed_trades")) >= min_trades]
    best = max(grid, key=rank_key)
    avg_trades = sum(num(row.get("closed_trades")) for row in grid) / len(grid)
    return (
        f"{len(profitable)}/{len(grid)} profitable, {len(enough_trades)}/{len(grid)} with >= {min_trades} trades, "
        f"average trades {avg_trades:.1f}, best {num(best.get('brick_pct')):.2f}% "
        f"net {money(num(best.get('net_pnl_dollars')))} PF {pf(best.get('profit_factor'))}"
    )


def candidates_to_avoid(rows: list[dict[str, Any]], *, strategy: str, min_trades: int = DEFAULT_MIN_TRADES, floor: float = DEFAULT_FLOOR) -> list[str]:
    grid = [
        row
        for row in rows
        if row.get("candidate_type") == "BRICK_PERCENT_GRID"
        and row.get("candidate_status") == "COMPLETED"
        and num(row.get("brick_pct")) >= floor - EPSILON
    ]
    avoid: list[tuple[str, dict[str, Any]]] = []
    for row in sorted(grid, key=lambda item: num(item.get("net_pnl_dollars")), reverse=True):
        reasons = avoid_reasons(row, min_trades=min_trades)
        if reasons:
            avoid.append((", ".join(reasons), row))
        if len(avoid) >= 5:
            break
    lines = ["Candidates to avoid:"]
    if not avoid:
        lines.append("- No high-headline candidates met the avoid filters in this symbol/strategy landscape.")
        return lines
    for reasons, row in avoid:
        lines.append(
            f"- {num(row.get('brick_pct')):.2f}% {row.get('entry_confirmation')} TP{clean(row.get('take_profit_bricks'))} "
            f"SL{clean(row.get('stop_loss_bricks'))}: net {money(num(row.get('net_pnl_dollars')))}, "
            f"trades {int(num(row.get('closed_trades')))}, PF {pf(row.get('profit_factor'))}; avoid because {reasons}."
        )
    return lines


def avoid_reasons(row: dict[str, Any], *, min_trades: int = DEFAULT_MIN_TRADES) -> list[str]:
    reasons: list[str] = []
    if num(row.get("closed_trades")) < min_trades:
        reasons.append(f"fewer than {min_trades} trades")
    if row.get("isolated_spike_flag") in {True, "True", "true"}:
        reasons.append("isolated spike")
    if num(row.get("local_profitable_pct")) < 60:
        reasons.append("weak neighbors")
    if num(row.get("max_drawdown_pct")) > 25:
        reasons.append("excessive drawdown")
    if one_session_dependence(row) in {"yes", "possible"}:
        reasons.append("one-session dependence")
    if num(row.get("average_trade_pnl_dollars")) <= 0:
        reasons.append("poor expectancy")
    if num(row.get("profit_factor")) <= 1:
        reasons.append("weak PF")
    if long_short_unstable(row):
        reasons.append("unstable long/short performance")
    return reasons


def long_short_unstable(row: dict[str, Any]) -> bool:
    long_pnl = num(row.get("long_pnl_dollars"))
    short_pnl = num(row.get("short_pnl_dollars"))
    total = abs(long_pnl) + abs(short_pnl)
    if total <= 0:
        return False
    return (long_pnl < 0 < short_pnl and abs(long_pnl) / total > 0.35) or (short_pnl < 0 < long_pnl and abs(short_pnl) / total > 0.35)


def consolidated_table(recommendations: dict[tuple[str, str], dict[str, Any]], *, return_label: str = "Return") -> list[str]:
    lines = [
        f"| Symbol | Strategy | Current | Rank #1 Regime | Rank #2 Regime | Rank #3 Regime | {return_label} | Net P&L | Max DD | Trades | PF | Expectancy | Robustness | Confidence | Action |",
        "| ------ | -------- | ------- | -------------- | -------------- | -------------- | -----------: | ------: | -----: | -----: | -: | ---------: | ---------- | ---------- | ------ |",
    ]
    for symbol, strategy in sorted(recommendations):
        payload = recommendations[(symbol, strategy)]
        regimes = payload["regimes"]
        proposed = payload["proposed"] or {}
        regime_cells = [brief_regime(regimes[index]) if len(regimes) > index else "-" for index in range(3)]
        lines.append(
            "| "
            + " | ".join(
                [
                    symbol,
                    strategy,
                    brief_current(payload["current"]),
                    *regime_cells,
                    render(field_value(proposed, "return_on_allocated_capital_pct") or field_value(proposed, "strategy_return_pct"), "%"),
                    render(field_value(proposed, "net_pnl_dollars"), "$"),
                    render(field_value(proposed, "max_drawdown_dollars"), "$"),
                    render(field_value(proposed, "closed_trades"), ""),
                    pf(field_value(proposed, "profit_factor")),
                    render(field_value(proposed, "average_trade_pnl_dollars"), "$"),
                    render(field_value(proposed, "robustness_score"), ""),
                    regimes[0]["confidence"] if regimes else "NONE",
                    payload["decision"],
                ]
            )
            + " |"
        )
    lines.append("")
    return lines


def fleet_findings(recommendations: dict[tuple[str, str], dict[str, Any]]) -> list[str]:
    ranked = [
        (payload["regimes"][0], symbol, strategy, payload)
        for (symbol, strategy), payload in recommendations.items()
        if payload.get("regimes")
    ]
    ranked.sort(key=lambda item: regime_rank_key(item[0]), reverse=True)
    lines = ["## Strongest Regimes For Next Week", ""]
    if not ranked:
        lines.append("- No credible regimes were found across the fleet.")
    for regime, symbol, strategy, _payload in ranked[:6]:
        row = regime["leader"]
        lines.append(
            f"- {symbol} {strategy}: {brief_regime(regime)}, net {money(num(row.get('net_pnl_dollars')))}, "
            f"PF {pf(row.get('profit_factor'))}, confidence {regime['confidence']}."
        )
    lines.extend(["", "## Meaningful Renko Regime Shifts", ""])
    shifts = [shift_description(symbol, strategy, payload) for (symbol, strategy), payload in sorted(recommendations.items())]
    lines.extend(f"- {item}" for item in shifts)
    lines.extend(["", "## KEEP CURRENT Cases", ""])
    keeps = [f"{symbol} {strategy}" for (symbol, strategy), payload in sorted(recommendations.items()) if payload["decision"] == "KEEP CURRENT"]
    lines.append("- " + (", ".join(keeps) if keeps else "None."))
    lines.extend(["", "## Top 3 Alternatives And Near-Ties", ""])
    alternatives = []
    for symbol, strategy in sorted(recommendations):
        regimes = recommendations[(symbol, strategy)]["regimes"]
        if len(regimes) >= 2:
            alternatives.append(f"{symbol} {strategy}: Rank #2 {brief_regime(regimes[1])}; near-tie={yes_no(is_near_tie(regimes[0], regimes[1]))}")
        if len(regimes) >= 3:
            alternatives.append(f"{symbol} {strategy}: Rank #3 {brief_regime(regimes[2])}")
    lines.extend(f"- {item}" for item in alternatives[:12])
    if not alternatives:
        lines.append("- No credible Rank #2/#3 alternatives.")
    lines.append("")
    return lines


def previous_floor_recommendations(inputs: RunInputs) -> dict[tuple[str, str], dict[str, Any]]:
    if not inputs.previous_reversal_dir or not inputs.previous_tp_sl_dir:
        return {}
    if not (inputs.previous_reversal_dir / "all_candidates.csv").exists() or not (inputs.previous_tp_sl_dir / "all_candidates.csv").exists():
        return {}
    previous: dict[tuple[str, str], dict[str, Any]] = {}
    for strategy, directory in (("Reversal", inputs.previous_reversal_dir), ("TP/SL", inputs.previous_tp_sl_dir)):
        rows = load_rows(directory / "all_candidates.csv")
        for symbol in sorted({str(row.get("symbol")) for row in rows if row.get("symbol")}):
            symbol_rows = [row for row in rows if row.get("symbol") == symbol]
            regimes = distinct_regimes(symbol_rows, strategy=strategy, min_trades=inputs.min_trades, floor=DEFAULT_FLOOR)
            current = first(row for row in symbol_rows if str(row.get("candidate_type", "")).startswith("CURRENT_PRODUCTION"))
            proposed = regimes[0]["leader"] if regimes else None
            previous[(symbol, strategy)] = {
                "symbol": symbol,
                "strategy": strategy,
                "current": current,
                "proposed": proposed,
                "regimes": regimes,
                "rows": symbol_rows,
                "min_trades": inputs.min_trades,
                "floor": DEFAULT_FLOOR,
                "decision": decision(current, proposed, min_trades=inputs.min_trades),
            }
    published_decisions = previous_published_decisions(inputs.previous_root)
    for key, published_decision in published_decisions.items():
        if key in previous:
            previous[key]["published_decision"] = published_decision
            previous[key]["decision"] = published_decision
    return previous


def previous_published_decisions(previous_root: Path | None) -> dict[tuple[str, str], str]:
    if not previous_root:
        return {}
    summary_path = previous_root / "readout" / "run_summary.json"
    if not summary_path.exists():
        return {}
    with summary_path.open() as handle:
        summary = json.load(handle)
    decisions = summary.get("decisions") or {}
    parsed: dict[tuple[str, str], str] = {}
    for label, value in decisions.items():
        parts = str(label).split(" ", 1)
        if len(parts) == 2:
            parsed[(parts[0], parts[1])] = str(value)
    return parsed


def executive_summary_table(
    recommendations: dict[tuple[str, str], dict[str, Any]],
    previous_recommendations: dict[tuple[str, str], dict[str, Any]],
) -> list[str]:
    lines = [
        "## Executive Summary",
        "",
        "| Strategy | Current | Best >=0.16% | Best >=0.12% | Decision | Why |",
        "| -------- | ------- | ----------- | ----------- | -------- | --- |",
    ]
    for key in sorted(recommendations):
        payload = recommendations[key]
        previous = previous_recommendations.get(key, {})
        lines.append(
            "| "
            + " | ".join(
                [
                    f"{key[0]} {key[1]}",
                    brief_current(payload.get("current")),
                    brief_candidate(previous.get("proposed")),
                    brief_candidate(payload.get("proposed")),
                    payload["decision"],
                    why_summary(payload, previous),
                ]
            )
            + " |"
        )
    lines.append("")
    return lines


def explicit_questions(
    recommendations: dict[tuple[str, str], dict[str, Any]],
    previous_recommendations: dict[tuple[str, str], dict[str, Any]],
    *,
    floor: float,
) -> list[str]:
    changed = [f"{symbol} {strategy}" for (symbol, strategy), payload in sorted(recommendations.items()) if recommendation_changed(payload, previous_recommendations.get((symbol, strategy)))]
    low_region = [f"{symbol} {strategy}" for (symbol, strategy), payload in sorted(recommendations.items()) if proposed_in_reopened_region(payload, floor=floor)]
    robust_low = [
        f"{symbol} {strategy}"
        for (symbol, strategy), payload in sorted(recommendations.items())
        if proposed_in_reopened_region(payload, floor=floor) and (payload.get("regimes") or [{}])[0].get("neighbor_strength") in {"Moderate", "Strong"}
    ]
    any_promote = any(payload["decision"] == "PROMOTE NEW REGIME" for payload in recommendations.values())
    question_answers = [
        ("Did lowering the floor from 0.16% to 0.12% materially change any recommendation?", ", ".join(changed) if changed else "No."),
        ("Which strategies benefit from the newly eligible 0.12-0.14% region?", ", ".join(low_region) if low_region else "None selected as the best eligible regime."),
        ("Are those improvements broad/robust or isolated?", ", ".join(robust_low) + " show moderate/strong neighbor evidence." if robust_low else "No selected low-region improvement shows broad enough evidence in this readout."),
        ("Does AAPL TP/SL still deserve to be disabled?", decision_answer(recommendations, ("AAPL", "TP/SL"))),
        ("Does AAPL Reversal still justify keeping the 0.11% incumbent?", decision_answer(recommendations, ("AAPL", "Reversal"))),
        ("Does NVDA Reversal have a stronger regime below 0.16%?", low_region_answer(recommendations, ("NVDA", "Reversal"), floor=floor)),
        ("Does NVDA TP/SL have a stronger regime below 0.16%?", low_region_answer(recommendations, ("NVDA", "TP/SL"), floor=floor)),
        ("Does TSLA Reversal have a stronger regime below 0.16%?", low_region_answer(recommendations, ("TSLA", "Reversal"), floor=floor)),
        ("Does TSLA TP/SL's 0.20% candidate remain the strongest credible challenger?", tsla_tp_sl_answer(recommendations)),
        ("Based only on this controlled experiment, should ANY production strategy configuration change?", "Yes: at least one strategy is marked PROMOTE NEW REGIME." if any_promote else "No production configuration change is supported by this controlled readout."),
    ]
    lines = ["## Required Questions", ""]
    for index, (question, answer) in enumerate(question_answers, start=1):
        lines.append(f"{index}. {question} {answer}")
    return lines


def brief_current(row: dict[str, Any] | None) -> str:
    if not row:
        return "missing"
    return (
        f"{num(row.get('brick_pct')):.2f}% {row.get('entry_confirmation')} "
        f"TP{clean(row.get('take_profit_bricks'))} SL{clean(row.get('stop_loss_bricks'))}"
    )


def brief_candidate(row: dict[str, Any] | None) -> str:
    if not row:
        return "-"
    return (
        f"{num(row.get('brick_pct')):.2f}% {row.get('entry_confirmation')} "
        f"TP{clean(row.get('take_profit_bricks'))} SL{clean(row.get('stop_loss_bricks'))}, "
        f"net {money(num(row.get('net_pnl_dollars')))}, PF {pf(row.get('profit_factor'))}"
    )


def brief_regime(regime: dict[str, Any]) -> str:
    row = regime["leader"]
    return (
        f"{regime_range_text(regime)} rep {num(row.get('brick_pct')):.2f}% "
        f"{row.get('entry_confirmation')} TP{clean(row.get('take_profit_bricks'))} SL{clean(row.get('stop_loss_bricks'))}"
    )


def cell_summary(row: dict[str, Any] | None) -> str:
    if not row:
        return "missing"
    return (
        f"{row.get('entry_confirmation')} TP{clean(row.get('take_profit_bricks'))} SL{clean(row.get('stop_loss_bricks'))}, "
        f"net {money(num(row.get('net_pnl_dollars')))}, trades {int(num(row.get('closed_trades')))}, PF {pf(row.get('profit_factor'))}"
    )


def why_summary(payload: dict[str, Any], previous_payload: dict[str, Any] | None) -> str:
    proposed = payload.get("proposed")
    previous = previous_payload.get("proposed") if previous_payload else None
    if not proposed:
        return "No eligible profitable plateau met the filters."
    reasons = [
        f"{num(proposed.get('closed_trades')):.0f} trades",
        f"PF {pf(proposed.get('profit_factor'))}",
        f"neighbor {fmt(num(proposed.get('robustness_score')), '')}",
    ]
    if previous and abs(num(previous.get("brick_pct")) - num(proposed.get("brick_pct"))) > EPSILON:
        reasons.append(f"previous best was {num(previous.get('brick_pct')):.2f}%")
    if proposed_in_reopened_region(payload, floor=num(payload.get("floor"), DEFAULT_FLOOR)):
        reasons.append("best came from reopened low-brick region")
    return ", ".join(reasons) + "."


def recommendation_changed(payload: dict[str, Any], previous_payload: dict[str, Any] | None) -> bool:
    if not previous_payload:
        return False
    if payload.get("decision") != previous_payload.get("decision"):
        return True
    current = payload.get("proposed") or {}
    previous = previous_payload.get("proposed") or {}
    return candidate_identity(current) != candidate_identity(previous)


def candidate_identity(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        round(num(row.get("brick_pct")), 4),
        row.get("entry_confirmation"),
        clean(row.get("take_profit_bricks")),
        clean(row.get("stop_loss_bricks")),
    )


def proposed_in_reopened_region(payload: dict[str, Any], *, floor: float) -> bool:
    proposed = payload.get("proposed")
    if not proposed:
        return False
    brick = num(proposed.get("brick_pct"))
    return floor - EPSILON <= brick < DEFAULT_FLOOR - EPSILON


def decision_answer(recommendations: dict[tuple[str, str], dict[str, Any]], key: tuple[str, str]) -> str:
    payload = recommendations.get(key)
    if not payload:
        return "No result found."
    return f"Decision is `{payload['decision']}`."


def low_region_answer(recommendations: dict[tuple[str, str], dict[str, Any]], key: tuple[str, str], *, floor: float) -> str:
    payload = recommendations.get(key)
    if not payload:
        return "No result found."
    if proposed_in_reopened_region(payload, floor=floor):
        return f"Yes: best eligible regime is {brief_candidate(payload.get('proposed'))}."
    return "No: the selected eligible regime is not below 0.16%."


def tsla_tp_sl_answer(recommendations: dict[tuple[str, str], dict[str, Any]]) -> str:
    payload = recommendations.get(("TSLA", "TP/SL"))
    proposed = payload.get("proposed") if payload else None
    if proposed and abs(num(proposed.get("brick_pct")) - 0.20) < EPSILON:
        return "Yes: the selected credible challenger remains 0.20%."
    if proposed:
        return f"No: selected challenger is {brief_candidate(proposed)}."
    return "No eligible TSLA TP/SL challenger was selected."


def shift_description(symbol: str, strategy: str, payload: dict[str, Any]) -> str:
    current = payload.get("current")
    proposed = payload.get("proposed")
    if not current or not proposed:
        return f"{symbol} {strategy}: insufficient current/proposed evidence."
    delta = num(proposed.get("brick_pct")) - num(current.get("brick_pct"))
    return_delta = num(proposed.get("return_on_allocated_capital_pct"), proposed.get("strategy_return_pct")) - num(
        current.get("return_on_allocated_capital_pct"), current.get("strategy_return_pct")
    )
    label = "material" if abs(delta) >= 0.10 and abs(return_delta) >= 0.25 else "not material"
    return f"{symbol} {strategy}: current {num(current.get('brick_pct')):.2f}% vs Rank #1 {num(proposed.get('brick_pct')):.2f}% ({label}; return delta {fmt(return_delta, ' pp')})."


def is_near_tie(first_regime: dict[str, Any], second_regime: dict[str, Any]) -> bool:
    first_row = first_regime["leader"]
    second_row = second_regime["leader"]
    pnl_gap = abs(num(first_row.get("net_pnl_dollars")) - num(second_row.get("net_pnl_dollars")))
    return_gap = abs(
        num(first_row.get("return_on_allocated_capital_pct"), first_row.get("strategy_return_pct"))
        - num(second_row.get("return_on_allocated_capital_pct"), second_row.get("strategy_return_pct"))
    )
    return pnl_gap <= 25.0 and return_gap <= 0.25 and first_regime["confidence"] == second_regime["confidence"]


def regime_csv_rows(recommendations: dict[tuple[str, str], dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (symbol, strategy), payload in sorted(recommendations.items()):
        for rank, regime in enumerate(payload["regimes"], start=1):
            row = regime["leader"]
            rows.append(
                {
                    "symbol": symbol,
                    "strategy": strategy,
                    "rank": rank,
                    "regime_range": regime_range_text(regime),
                    "representative_brick_pct": row.get("brick_pct"),
                    "member_count": regime["member_count"],
                    "neighbor_strength": regime["neighbor_strength"],
                    "confidence": regime["confidence"],
                    "session_consistency": regime["session_consistency"],
                    "one_session_dependence": regime["one_session_dependence"],
                    "primary_weakness": regime["primary_weakness"],
                    "neighbor_summary": regime["neighbor_summary"],
                    **row,
                }
            )
    return rows


def comparison_csv_rows(recommendations: dict[tuple[str, str], dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for (symbol, strategy), payload in sorted(recommendations.items()):
        current = payload["current"] or {}
        proposed = payload["proposed"] or {}
        rows.append(
            {
                "symbol": symbol,
                "strategy": strategy,
                "decision": payload["decision"],
                "current_candidate_id": current.get("candidate_id"),
                "proposed_candidate_id": proposed.get("candidate_id"),
                "current_brick_pct": current.get("brick_pct"),
                "proposed_brick_pct": proposed.get("brick_pct"),
                "current_net_pnl_dollars": current.get("net_pnl_dollars"),
                "proposed_net_pnl_dollars": proposed.get("net_pnl_dollars"),
                "current_closed_trades": current.get("closed_trades"),
                "proposed_closed_trades": proposed.get("closed_trades"),
                "current_profit_factor": current.get("profit_factor"),
                "proposed_profit_factor": proposed.get("profit_factor"),
                "current_session_consistency": session_consistency(current),
                "proposed_session_consistency": session_consistency(proposed),
                "proposed_neighbor_robustness": proposed.get("robustness_score"),
                "proposed_confidence": payload["regimes"][0]["confidence"] if payload["regimes"] else None,
            }
        )
    return rows


def three_way_comparison_rows(
    recommendations: dict[tuple[str, str], dict[str, Any]],
    previous_recommendations: dict[tuple[str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    fields = [
        ("brick_pct", ""),
        ("entry_confirmation", ""),
        ("take_profit_bricks", ""),
        ("stop_loss_bricks", ""),
        ("return_on_allocated_capital_pct", ""),
        ("net_pnl_dollars", ""),
        ("max_drawdown_dollars", ""),
        ("closed_trades", ""),
        ("win_rate_pct", ""),
        ("profit_factor", ""),
        ("average_trade_pnl_dollars", ""),
        ("robustness_score", ""),
    ]
    for key in sorted(recommendations):
        payload = recommendations[key]
        previous = previous_recommendations.get(key, {})
        current = payload.get("current") or {}
        floor016 = previous.get("proposed") or {}
        floor012 = payload.get("proposed") or {}
        row = {
            "symbol": key[0],
            "strategy": key[1],
            "decision": payload.get("decision"),
            "current_session_consistency": session_consistency(current),
            "floor016_session_consistency": session_consistency(floor016),
            "floor012_session_consistency": session_consistency(floor012),
            "recommendation_changed_vs_floor016": recommendation_changed(payload, previous),
            "floor012_winner_from_reopened_region": proposed_in_reopened_region(payload, floor=num(payload.get("floor"), DEFAULT_FLOOR)),
        }
        for field, _unit in fields:
            row[f"current_{field}"] = current.get(field)
            row[f"floor016_{field}"] = floor016.get(field)
            row[f"floor012_{field}"] = floor012.get(field)
        rows.append(row)
    return rows


def reopened_region_rows(recommendations: dict[tuple[str, str], dict[str, Any]], *, floor: float) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (symbol, strategy), payload in sorted(recommendations.items()):
        for row in sorted(payload["rows"], key=lambda item: (num(item.get("brick_pct")), str(item.get("entry_confirmation")), num(item.get("take_profit_bricks")), num(item.get("stop_loss_bricks")))):
            brick = num(row.get("brick_pct"))
            if (
                row.get("candidate_type") == "BRICK_PERCENT_GRID"
                and row.get("candidate_status") == "COMPLETED"
                and reopened_region_brick(brick, floor=floor)
            ):
                rows.append(
                    {
                        "symbol": symbol,
                        "strategy": strategy,
                        "brick_pct": row.get("brick_pct"),
                        "entry_confirmation": row.get("entry_confirmation"),
                        "take_profit_bricks": row.get("take_profit_bricks"),
                        "stop_loss_bricks": row.get("stop_loss_bricks"),
                        "return_on_allocated_capital_pct": row.get("return_on_allocated_capital_pct") or row.get("strategy_return_pct"),
                        "net_pnl_dollars": row.get("net_pnl_dollars"),
                        "max_drawdown_dollars": row.get("max_drawdown_dollars"),
                        "closed_trades": row.get("closed_trades"),
                        "win_rate_pct": row.get("win_rate_pct"),
                        "profit_factor": row.get("profit_factor"),
                        "average_trade_pnl_dollars": row.get("average_trade_pnl_dollars"),
                        "session_count": row.get("session_count"),
                        "profitable_sessions": row.get("profitable_sessions"),
                        "losing_sessions": row.get("losing_sessions"),
                        "session_consistency": session_consistency(row),
                        "robustness_score": row.get("robustness_score"),
                        "local_profitable_pct": row.get("local_profitable_pct"),
                        "isolated_spike_flag": row.get("isolated_spike_flag"),
                        "robustness_eligible": row.get("robustness_eligible"),
                        "eligibility_reasons": row.get("eligibility_reasons"),
                        "candidate_id": row.get("candidate_id"),
                    }
                )
    return rows


def executive_summary_rows(
    recommendations: dict[tuple[str, str], dict[str, Any]],
    previous_recommendations: dict[tuple[str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in sorted(recommendations):
        payload = recommendations[key]
        previous = previous_recommendations.get(key, {})
        rows.append(
            {
                "strategy": f"{key[0]} {key[1]}",
                "current": brief_current(payload.get("current")),
                "best_ge_0p16": brief_candidate(previous.get("proposed")),
                "best_ge_0p12": brief_candidate(payload.get("proposed")),
                "decision": payload.get("decision"),
                "why": why_summary(payload, previous),
                "recommendation_changed_vs_floor016": recommendation_changed(payload, previous),
                "floor012_winner_from_reopened_region": proposed_in_reopened_region(payload, floor=num(payload.get("floor"), DEFAULT_FLOOR)),
            }
        )
    return rows


def session_dates(inputs: RunInputs) -> list[str]:
    sessions = []
    for directory in (inputs.reversal_dir, inputs.tp_sl_dir):
        manifest_path = directory / "optimization_manifest.json"
        if not manifest_path.exists():
            continue
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        for session in payload.get("session_dates") or []:
            if session not in sessions:
                sessions.append(str(session))
    return sorted(sessions)


def run_summary(
    inputs: RunInputs,
    rows: list[dict[str, Any]],
    heatmaps: list[dict[str, Any]],
    recommendations: dict[tuple[str, str], dict[str, Any]],
    previous_recommendations: dict[tuple[str, str], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    completed = [row for row in rows if row.get("candidate_status") == "COMPLETED"]
    failed = [row for row in rows if row.get("candidate_status") == "FAILED"]
    grid = [row for row in completed if row.get("candidate_type") == "BRICK_PERCENT_GRID"]
    return {
        "root": str(inputs.root),
        "sessions": session_dates(inputs),
        "candidate_floor_pct": inputs.floor,
        "previous_floor_pct": DEFAULT_FLOOR if previous_recommendations else None,
        "previous_root": str(inputs.previous_root) if inputs.previous_root else None,
        "minimum_closed_trades": inputs.min_trades,
        "completed_candidates": len(completed),
        "failed_candidates": len(failed),
        "grid_candidates_below_floor": sum(1 for row in grid if num(row.get("brick_pct")) < inputs.floor),
        "reopened_region_grid_candidates": sum(1 for row in grid if reopened_region_brick(num(row.get("brick_pct")), floor=inputs.floor)),
        "current_benchmarks_below_floor": sum(
            1 for row in completed if str(row.get("candidate_type", "")).startswith("CURRENT_PRODUCTION") and num(row.get("brick_pct")) < inputs.floor
        ),
        "top_regime_count": sum(len(payload["regimes"]) for payload in recommendations.values()),
        "heatmap_count": len(heatmaps),
        "decisions": {f"{symbol} {strategy}": payload["decision"] for (symbol, strategy), payload in recommendations.items()},
        "recommendation_changes_vs_previous_floor": {
            f"{symbol} {strategy}": recommendation_changed(payload, (previous_recommendations or {}).get((symbol, strategy)))
            for (symbol, strategy), payload in recommendations.items()
        },
    }


def svg_header(width: int, height: int) -> list[str]:
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        "<style>",
        ".title{font:600 15px Arial,sans-serif;fill:#111827}.small{font:11px Arial,sans-serif;fill:#4b5563}.axis{font:12px Arial,sans-serif;fill:#111827}.tick{font:10px Arial,sans-serif;fill:#4b5563}",
        "</style>",
        '<rect width="100%" height="100%" fill="#ffffff"/>',
    ]


def color_for(value: float, lo: float, hi: float, *, direction: str) -> str:
    if not math.isfinite(value):
        return "#f3f4f6"
    if hi <= lo:
        t = 0.5
    else:
        t = (value - lo) / (hi - lo)
    if direction == "lower":
        t = 1.0 - t
    t = max(0.0, min(1.0, t))
    if t < 0.5:
        a = t / 0.5
        return mix((185, 28, 28), (255, 255, 255), a)
    a = (t - 0.5) / 0.5
    return mix((255, 255, 255), (22, 101, 52), a)


def mix(a: tuple[int, int, int], b: tuple[int, int, int], t: float) -> str:
    rgb = tuple(round(a[i] + (b[i] - a[i]) * t) for i in range(3))
    return "#{:02x}{:02x}{:02x}".format(*rgb)


def scale(values: list[float]) -> tuple[float, float]:
    finite = sorted(value for value in values if math.isfinite(value))
    if not finite:
        return 0.0, 1.0
    return finite[0], finite[-1]


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    keys = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def first(rows: Any) -> dict[str, Any] | None:
    for row in rows:
        return row
    return None


def field_value(row: dict[str, Any] | None, key: str) -> Any:
    if not row:
        return None
    return row.get(key)


def render(value: Any, unit: str) -> str:
    if value in {None, "", "UNKNOWN"}:
        return "UNKNOWN"
    if unit == "$":
        return money(num(value))
    if unit == "%":
        return fmt(num(value), "%")
    if str(value) in {"Infinity", "inf"}:
        return "Infinity"
    if isinstance(value, str) and not looks_numeric(value):
        return value
    return fmt(num(value), "")


def render_diff(current: Any, proposed: Any, unit: str) -> str:
    if current in {None, "", "UNKNOWN"} or proposed in {None, "", "UNKNOWN"}:
        return "UNKNOWN"
    if isinstance(current, str) and not looks_numeric(current):
        return "changed" if current != proposed else "same"
    delta = num(proposed) - num(current)
    if unit == "$":
        return money(delta)
    if unit == "%":
        return fmt(delta, " pp")
    return fmt(delta, "")


def clean(value: Any) -> str:
    if value in {None, "", "UNKNOWN"}:
        return "-"
    return str(value)


def money(value: float) -> str:
    return f"${value:,.2f}"


def fmt(value: float, suffix: str) -> str:
    if not math.isfinite(value):
        return "Infinity" if value > 0 else "UNKNOWN"
    return f"{value:,.2f}{suffix}"


def pf(value: Any) -> str:
    if value in {None, "", "UNKNOWN"}:
        return "UNKNOWN"
    if str(value) in {"Infinity", "inf"}:
        return "Infinity"
    return fmt(num(value), "")


def num(*values: Any) -> float:
    for value in values:
        if value in {None, "", "UNKNOWN"}:
            continue
        if str(value) in {"Infinity", "inf"}:
            return math.inf
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0


def review_bricks(floor: float) -> list[float]:
    bricks: list[float] = []
    current = floor
    while current <= DEFAULT_FLOOR + EPSILON:
        bricks.append(round(current, 2))
        current += BRICK_STEP
    if not any(abs(item - DEFAULT_FLOOR) < EPSILON for item in bricks):
        bricks.append(DEFAULT_FLOOR)
    return sorted(set(bricks))


def reopened_region_brick(brick: float, *, floor: float) -> bool:
    return floor - EPSILON <= brick <= DEFAULT_FLOOR + EPSILON


def safe_ratio(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return math.inf if numerator > 0 else 0.0
    return numerator / denominator


def looks_numeric(value: str) -> bool:
    try:
        float(value)
        return True
    except ValueError:
        return value in {"Infinity", "inf"}


def yes_no(value: bool) -> str:
    return "yes" if value else "no"


def escape(value: Any) -> str:
    return html.escape(str(value))


if __name__ == "__main__":
    raise SystemExit(main())
