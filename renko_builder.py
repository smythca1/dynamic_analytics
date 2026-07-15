from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Sequence

import pandas as pd


@dataclass
class RenkoBuildState:
    brick_size: float
    last_brick_close: float | None = None
    run_dir: int = 0
    run_len: int = 0
    bricks: list[dict[str, object]] = field(default_factory=list)
    anchor_mode: str = "first_price"
    anchor_price: float | None = None


def update_renko_build_state(
    state: RenkoBuildState,
    timestamps: Sequence[object],
    prices: Sequence[float],
) -> RenkoBuildState:
    if state.brick_size <= 0:
        raise ValueError("brick_size must be > 0.")
    if len(timestamps) != len(prices):
        raise ValueError("timestamps and prices must have matching lengths.")
    if len(prices) == 0:
        return state

    start_index = 0
    if state.last_brick_close is None:
        state.last_brick_close = initial_renko_close(float(prices[0]), state)
        start_index = 1

    for ts, price in zip(timestamps[start_index:], prices[start_index:]):
        while True:
            move = float(price) - float(state.last_brick_close)
            if state.run_dir == 1:
                if move >= state.brick_size:
                    _append_brick(state, ts, direction=1)
                    continue
                if move <= -2.0 * state.brick_size:
                    _append_brick(state, ts, direction=-1, bricks=2)
                    continue
            elif state.run_dir == -1:
                if move <= -state.brick_size:
                    _append_brick(state, ts, direction=-1)
                    continue
                if move >= 2.0 * state.brick_size:
                    _append_brick(state, ts, direction=1, bricks=2)
                    continue
            else:
                if move >= state.brick_size:
                    _append_brick(state, ts, direction=1)
                    continue
                if move <= -state.brick_size:
                    _append_brick(state, ts, direction=-1)
                    continue
            break

    return state


def _append_brick(state: RenkoBuildState, timestamp: object, *, direction: int, bricks: int = 1) -> None:
    state.last_brick_close = float(state.last_brick_close) + (direction * state.brick_size * bricks)
    if direction == state.run_dir:
        state.run_len += 1
    else:
        state.run_dir = direction
        state.run_len = 1
    state.bricks.append(
        {
            "timestamp": pd.Timestamp(timestamp),
            "brick_close": float(state.last_brick_close),
            "direction": direction,
            "run_len": state.run_len,
        }
    )


def initial_renko_close(price: float, state: RenkoBuildState) -> float:
    anchor_mode = str(state.anchor_mode or "first_price").lower().strip()
    if anchor_mode in {"first_price", "first", "price"}:
        return float(price)

    anchor_price = 0.0 if state.anchor_price is None else float(state.anchor_price)
    if anchor_mode in {"grid", "zero_grid", "anchored_grid"}:
        return anchor_price + (math.floor((float(price) - anchor_price) / state.brick_size) * state.brick_size)

    raise ValueError(f"Unsupported Renko anchor_mode: {state.anchor_mode}")


def build_renko_from_prices(
    timestamps: Sequence[object],
    prices: Sequence[float],
    *,
    brick_size: float,
    anchor_mode: str = "first_price",
    anchor_price: float | None = None,
) -> pd.DataFrame:
    state = RenkoBuildState(brick_size=brick_size, anchor_mode=anchor_mode, anchor_price=anchor_price)
    update_renko_build_state(state, timestamps, prices)
    renko = pd.DataFrame(state.bricks)
    return renko.reset_index(drop=True) if not renko.empty else renko
