# Canonical Renko Specification

This is the canonical Renko contract for historical backtests, paper trading,
and live trading.

## Inputs

Input is a chronological stream of `(timestamp, price)` events. Production may
derive those events from raw ticks or completed 1-second closes, but Renko
construction sees only this event stream.

Duplicate or out-of-order input is a data-quality issue and must be reported by
the caller. The Renko builder does not sort or dedupe internally.

## Anchor

When no brick has been initialized, the first input price sets
`last_brick_close` and emits no brick.

`anchor_mode = first_price`:

- `last_brick_close = first_price`

`anchor_mode = grid`:

- `last_brick_close = anchor_price + floor((first_price - anchor_price) / brick_size) * brick_size`
- default `anchor_price = 0.0`

## Brick Emission

Each input event can emit zero, one, or many bricks. All bricks emitted from the
same input event carry that event timestamp.

Let `move = price - last_brick_close`.

When `run_dir == 0`:

- emit an up brick while `move >= brick_size`
- emit a down brick while `move <= -brick_size`

When `run_dir == 1`:

- continuation up emits one brick at `last_brick_close + brick_size`
- reversal down requires a two-box move and emits one brick at
  `last_brick_close - 2 * brick_size`

When `run_dir == -1`:

- continuation down emits one brick at `last_brick_close - brick_size`
- reversal up requires a two-box move and emits one brick at
  `last_brick_close + 2 * brick_size`

After a reversal brick, additional continuation bricks from the same input event
are allowed if the remaining move still crosses continuation thresholds.

## Run State

Each emitted brick updates `last_brick_close`.

If `direction == run_dir`, increment `run_len`.

If `direction != run_dir`, set `run_dir = direction` and `run_len = 1`.

The skipped box in a two-box reversal is not emitted and does not increment
`run_len`.

## Boundary Behavior

Threshold equality emits a brick. Floating-point comparisons use a small
absolute epsilon of `1e-12` to make Python and C++ deterministic at binary
rounding boundaries. Values more than `1e-12` inside the threshold must not
emit.

## Restart Determinism

Persisted state must include:

- `brick_size`
- `last_brick_close`
- `run_dir`
- `run_len`
- `anchor_mode`
- `anchor_price`

Restoring that state and continuing with the remaining input stream must produce
the same subsequent bricks as a single uninterrupted run.
