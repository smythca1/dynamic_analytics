from __future__ import annotations

import argparse
import html
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


EASTERN = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class Brick:
    timestamp: datetime
    open: float
    close: float
    direction: int
    run_len: int
    brick_size: float


@dataclass(frozen=True)
class TradeMarker:
    timestamp: datetime
    price: float
    side: str
    broker_side: str
    reason: str
    source: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render Renko bricks with live trade/fill markers as SVG.")
    parser.add_argument("--renko-dir", required=True)
    parser.add_argument("--event-log")
    parser.add_argument("--trader-id")
    parser.add_argument("--fills")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--title", default="")
    parser.add_argument("--x-axis", choices=("time", "index"), default="time")
    return parser.parse_args()


def parse_timestamp(raw: str) -> datetime:
    value = raw.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError(f"Timestamp is missing timezone: {raw}")
    return parsed


def load_bricks(path: Path) -> list[Brick]:
    bricks: list[Brick] = []
    for file in sorted(path.glob("*.jsonl")):
        if not file.stem[:4].isdigit():
            continue
        with file.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                payload = json.loads(line)
                bricks.append(
                    Brick(
                        timestamp=parse_timestamp(payload["timestamp"]),
                        open=float(payload["open"]),
                        close=float(payload["close"]),
                        direction=int(payload["direction"]),
                        run_len=int(payload["run_len"]),
                        brick_size=float(payload["brick_size"]),
                    )
                )
    bricks.sort(key=lambda brick: brick.timestamp)
    return bricks


def load_signal_markers(path: Path, trader_id: str) -> list[TradeMarker]:
    markers: list[TradeMarker] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("event_type") != "signal":
                continue
            payload = event.get("payload")
            if not isinstance(payload, dict) or payload.get("trader_id") != trader_id:
                continue
            markers.append(
                TradeMarker(
                    timestamp=parse_timestamp(payload["timestamp"]),
                    price=float(payload["price"]),
                    side=str(payload["side"]),
                    broker_side=str(payload["broker_side"]),
                    reason=str(payload["reason"]),
                    source="signal",
                )
            )
    markers.sort(key=lambda marker: marker.timestamp)
    return markers


def load_fill_markers(path: Path, symbol: str) -> list[TradeMarker]:
    markers: list[TradeMarker] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            payload = json.loads(line)
            if str(payload.get("symbol", "")).upper() != symbol:
                continue
            side = str(payload["side"])
            qty = payload.get("qty", "?")
            order_id = str(payload.get("order_id", ""))[:8]
            markers.append(
                TradeMarker(
                    timestamp=parse_timestamp(payload["transaction_time"]),
                    price=float(payload["price"]),
                    side=side,
                    broker_side=side,
                    reason=f"Alpaca fill qty {qty} order {order_id}",
                    source="fill",
                )
            )
    markers.sort(key=lambda marker: marker.timestamp)
    return markers


def local_day(ts: datetime) -> str:
    return ts.astimezone(EASTERN).date().isoformat()


def local_time(ts: datetime) -> str:
    return ts.astimezone(EASTERN).strftime("%H:%M:%S")


def x_for_index(index: int, count: int, left: int, width: int) -> float:
    if count <= 1:
        return left + width / 2
    return left + (index / (count - 1)) * width


def x_for_time(ts: datetime, start: datetime, end: datetime, left: int, width: int) -> float:
    span = (end - start).total_seconds()
    if span <= 0:
        return left + width / 2
    return left + ((ts - start).total_seconds() / span) * width


def y_for_price(price: float, min_price: float, max_price: float, top: int, height: int) -> float:
    if math.isclose(max_price, min_price):
        return top + height / 2
    return top + (max_price - price) / (max_price - min_price) * height


def marker_index(marker: TradeMarker, bricks: list[Brick]) -> int:
    best_index = 0
    best_score = float("inf")
    for idx, brick in enumerate(bricks):
        time_delta = abs((brick.timestamp - marker.timestamp).total_seconds())
        price_delta = abs(brick.close - marker.price) * 10.0
        score = time_delta + price_delta
        if score < best_score:
            best_score = score
            best_index = idx
    return best_index


def marker_label(marker: TradeMarker) -> str:
    if marker.broker_side == "sell_short":
        return "SHORT"
    if marker.broker_side == "sell":
        return "SELL"
    if marker.broker_side == "buy":
        return "BUY"
    return marker.broker_side.upper()


def render_svg(
    path: Path,
    title: str,
    bricks: list[Brick],
    markers: list[TradeMarker],
    marker_source_label: str,
    x_axis: str,
) -> None:
    if not bricks:
        return

    width = 1480
    height = 760
    left = 76
    right = 34
    top = 84
    bottom = 92
    plot_w = width - left - right
    plot_h = height - top - bottom
    prices = [brick.open for brick in bricks] + [brick.close for brick in bricks] + [marker.price for marker in markers]
    pad = max((max(prices) - min(prices)) * 0.06, bricks[0].brick_size * 2)
    min_price = min(prices) - pad
    max_price = max(prices) + pad
    start_ts = min([brick.timestamp for brick in bricks] + [marker.timestamp for marker in markers])
    end_ts = max([brick.timestamp for brick in bricks] + [marker.timestamp for marker in markers])

    day_label_x: dict[str, float] = {}
    for idx, brick in enumerate(bricks):
        day = local_day(brick.timestamp)
        if day not in day_label_x:
            if x_axis == "time":
                day_label_x[day] = x_for_time(brick.timestamp, start_ts, end_ts, left, plot_w)
            else:
                day_label_x[day] = x_for_index(idx, len(bricks), left, plot_w)

    parts: list[str] = []
    parts.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">')
    parts.append('<rect width="100%" height="100%" fill="#fbfcf8"/>')
    parts.append(
        "<style>"
        "text{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Arial,sans-serif;fill:#1d2521}"
        ".title{font-size:22px;font-weight:760}.sub{font-size:12px;fill:#5d6862}"
        ".axis{font-size:11px;fill:#66736d}.price{font-size:11px;font-weight:650;fill:#4d5a54}"
        ".legend{font-size:12px;font-weight:700}.label{font-size:10px;font-weight:760}"
        "</style>"
    )
    parts.append(f'<text x="28" y="36" class="title">{html.escape(title)}</text>')
    parts.append(
        f'<text x="28" y="58" class="sub">{len(bricks)} Renko bricks, {len(markers)} {html.escape(marker_source_label)}. '
        f'X-axis = {html.escape(x_axis)}. Green = up brick/buy, red = down brick/sell/short. Times are America/New_York.</text>'
    )
    parts.append(f'<rect x="{left}" y="{top}" width="{plot_w}" height="{plot_h}" fill="#ffffff" stroke="#d9e0dc"/>')

    grid_count = 6
    for i in range(grid_count + 1):
        price = min_price + (max_price - min_price) * i / grid_count
        y = y_for_price(price, min_price, max_price, top, plot_h)
        parts.append(f'<line x1="{left}" y1="{y:.2f}" x2="{left + plot_w}" y2="{y:.2f}" stroke="#edf1ee"/>')
        parts.append(f'<text x="{left - 10}" y="{y + 4:.2f}" text-anchor="end" class="price">{price:.2f}</text>')

    for day, x in day_label_x.items():
        parts.append(f'<line x1="{x:.2f}" y1="{top}" x2="{x:.2f}" y2="{top + plot_h}" stroke="#eef2ef"/>')
        parts.append(f'<text x="{x:.2f}" y="{top + plot_h + 24}" text-anchor="middle" class="axis">{day[5:]}</text>')

    brick_w = max(2.0, min(16.0, plot_w / max(len(bricks), 1) * 0.68))
    for idx, brick in enumerate(bricks):
        if x_axis == "time":
            x = x_for_time(brick.timestamp, start_ts, end_ts, left, plot_w)
        else:
            x = x_for_index(idx, len(bricks), left, plot_w)
        y_open = y_for_price(brick.open, min_price, max_price, top, plot_h)
        y_close = y_for_price(brick.close, min_price, max_price, top, plot_h)
        y = min(y_open, y_close)
        h = max(1.8, abs(y_close - y_open))
        color = "#2f9e6d" if brick.direction > 0 else "#cf4e4a"
        parts.append(
            f'<rect x="{x - brick_w / 2:.2f}" y="{y:.2f}" width="{brick_w:.2f}" height="{h:.2f}" '
            f'fill="{color}" opacity="0.84">'
            f'<title>{local_day(brick.timestamp)} {local_time(brick.timestamp)} close {brick.close:.4f} run {brick.run_len}</title>'
            "</rect>"
        )

    for marker in markers:
        if x_axis == "time":
            x = x_for_time(marker.timestamp, start_ts, end_ts, left, plot_w)
        else:
            idx = marker_index(marker, bricks)
            x = x_for_index(idx, len(bricks), left, plot_w)
        y = y_for_price(marker.price, min_price, max_price, top, plot_h)
        is_buy = marker.broker_side == "buy"
        color = "#087f5b" if is_buy else "#b4232f"
        label = marker_label(marker)
        if is_buy:
            points = f"{x:.2f},{y - 14:.2f} {x - 9:.2f},{y + 6:.2f} {x + 9:.2f},{y + 6:.2f}"
            text_y = y - 18
        else:
            points = f"{x:.2f},{y + 14:.2f} {x - 9:.2f},{y - 6:.2f} {x + 9:.2f},{y - 6:.2f}"
            text_y = y + 30
        parts.append(
            f'<polygon points="{points}" fill="{color}" stroke="#111827" stroke-width="0.8">'
            f'<title>{label} {local_day(marker.timestamp)} {local_time(marker.timestamp)} '
            f'{marker.price:.4f} {html.escape(marker.reason)}</title></polygon>'
        )
        parts.append(f'<text x="{x:.2f}" y="{text_y:.2f}" text-anchor="middle" class="label" fill="{color}">{label}</text>')

    parts.append('<rect x="1080" y="24" width="16" height="10" fill="#2f9e6d"/><text x="1104" y="34" class="legend">Up brick / buy marker</text>')
    parts.append('<rect x="1250" y="24" width="16" height="10" fill="#cf4e4a"/><text x="1274" y="34" class="legend">Down brick / sell or short marker</text>')
    parts.append(f'<text x="{left}" y="{height - 28}" class="sub">Brick size: {bricks[0].brick_size:.4f}. Source: production live Renko archive and {html.escape(marker_source_label)}.</text>')
    parts.append("</svg>")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(parts) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    symbol = str(args.symbol).upper()
    base_title = args.title or f"{symbol} Live Renko With Trades"
    output_dir = Path(args.output_dir)
    bricks = load_bricks(Path(args.renko_dir))
    markers: list[TradeMarker] = []
    marker_source_names: list[str] = []
    output_kind = "trades"
    if args.fills:
        markers.extend(load_fill_markers(Path(args.fills), symbol))
        marker_source_names.append("Alpaca FILL activities")
        output_kind = "fills"
    if args.event_log:
        if not args.trader_id:
            raise RuntimeError("--trader-id is required when --event-log is provided.")
        markers.extend(load_signal_markers(Path(args.event_log), args.trader_id))
        marker_source_names.append("live signal events")
        output_kind = "signals" if output_kind == "trades" else "trade_markers"
    if not marker_source_names:
        raise RuntimeError("Provide --fills, --event-log, or both.")
    markers.sort(key=lambda marker: marker.timestamp)
    marker_source_label = " and ".join(marker_source_names)

    if not bricks:
        raise RuntimeError("No Renko bricks loaded.")

    generated: list[Path] = []
    combined_path = output_dir / f"{symbol.lower()}_renko_{output_kind}_combined.svg"
    render_svg(combined_path, base_title, bricks, markers, marker_source_label, args.x_axis)
    generated.append(combined_path)

    for day in sorted({local_day(brick.timestamp) for brick in bricks}):
        day_bricks = [brick for brick in bricks if local_day(brick.timestamp) == day]
        day_markers = [marker for marker in markers if local_day(marker.timestamp) == day]
        day_path = output_dir / f"{symbol.lower()}_renko_{output_kind}_{day}.svg"
        render_svg(
            day_path,
            f"{base_title} - {day}",
            day_bricks,
            day_markers,
            marker_source_label,
            args.x_axis,
        )
        generated.append(day_path)

    index_parts = [
        "<!doctype html>",
        '<html lang="en">',
        "<head>",
        '<meta charset="utf-8">',
        f"<title>{html.escape(base_title)}</title>",
        "<style>",
        "body{margin:0;background:#f6f8f5;color:#1d2521;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Arial,sans-serif}",
        "main{max-width:1540px;margin:0 auto;padding:24px}",
        "h1{font-size:24px;margin:0 0 6px}p{margin:0 0 18px;color:#5d6862}",
        "section{margin:0 0 32px}img{display:block;width:100%;height:auto;border:1px solid #d9e0dc;background:white}",
        "h2{font-size:16px;margin:0 0 8px}",
        "</style>",
        "</head>",
        "<body><main>",
        f"<h1>{html.escape(base_title)}</h1>",
        f"<p>{len(bricks)} Renko bricks and {len(markers)} {html.escape(marker_source_label)} from production archives. X-axis: {html.escape(args.x_axis)}.</p>",
    ]
    for path in generated:
        title = "Combined" if path == combined_path else path.stem.rsplit("_", 1)[-1]
        index_parts.append("<section>")
        index_parts.append(f"<h2>{html.escape(title)}</h2>")
        index_parts.append(f'<img src="{html.escape(path.name)}" alt="{html.escape(title)}">')
        index_parts.append("</section>")
    index_parts.append("</main></body></html>")
    (output_dir / f"{symbol.lower()}_renko_{output_kind}_index.html").write_text(
        "\n".join(index_parts) + "\n",
        encoding="utf-8",
    )

    summary = {
        "symbol": symbol,
        "trader_id": args.trader_id,
        "brick_count": len(bricks),
        "marker_count": len(markers),
        "marker_source": marker_source_label,
        "x_axis": args.x_axis,
        "first_brick": bricks[0].timestamp.isoformat(),
        "last_brick": bricks[-1].timestamp.isoformat(),
        "outputs": sorted(str(path) for path in output_dir.glob(f"{symbol.lower()}_renko_{output_kind}*")),
    }
    (output_dir / f"{symbol.lower()}_renko_{output_kind}_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
