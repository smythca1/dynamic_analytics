# Production Version Contract

Date: 2026-07-21

This repo has one canonical Renko construction specification and one live trading
path. Backtests must state whether they target the current live E1 reversal
profiles or the older continuation-case TP/SL research case.

## Current Production Baseline

Live execution stack:

- Live fleet runner: `fleet_runner.py`
- Strategy engine: `strategy_engine.py`
- Renko builder: `renko_builder.py`
- Event dataclasses and JSON shape: `trading_events.py`
- Execution adapter: `execution_engine.py`
- Broker adapter: `alpaca_trader.py`
- Live fleet config: `configs/traders.live.example.json`
- Live tick archive root: `artifacts/live/ticks/YYYY-MM-DD/SYMBOL.jsonl`
- Live event log: `artifacts/live/fleet_live_events.jsonl`
- Deploy target: `trading-fleet-live.service` plus `mission-control.service`

Aligned backtester:

- Current live E1 production-parity sweep: `scripts/e1_renko_production_parity_sweep.py`
- Historical continuation-case TP/SL backtester: `validated_renko_backtest.cpp`
- Canonical Renko rule spec: `RENKO_SPEC.md`

The passing local baseline is:

```bash
make compile
make test
```

## Canonical Renko Construction

All Python and C++ Renko implementations must match `RENKO_SPEC.md`.

Required semantics:

- First input anchors and emits no brick.
- `grid` anchoring uses the configured grid; `first_price` anchors exactly to
  the first observed price.
- Continuations emit every full brick crossed by one market event.
- Reversals require a two-box move from the prior brick close; the skipped
  reversal box is not emitted.
- A large reversal may emit the first reversal brick and then additional
  continuation bricks from the same market event.
- Equality at the threshold emits; comparisons use the documented epsilon.
- Multiple bricks from one market event carry that event timestamp.
- Restart/snapshot state must reproduce the same logical stream.

The parity harness is `tests/test_renko_cpp_parity.py`, which compiles
`validated_renko_backtest.cpp` in parity mode and compares:

```text
timestamp, brick_close, direction, run_len
```

## Current Live Backtest Case: E1 Reversal

The production AAPL/NVDA/TSLA profiles are `renko_reversal` E1 strategies. Their
production-parity sweep is `scripts/e1_renko_production_parity_sweep.py`.

Data and Renko rules:

- Aggregate stored ticks to 1-second closes before Renko construction.
- Use grid anchoring unless a task explicitly states otherwise.
- Use the same `RenkoStrategyEngine` transition called by `fleet_runner.py`
  after live `renko_source=1s_close` conversion.

Strategy rules:

- Enter on the configured E1 Renko reversal/continuation signal emitted by
  `RenkoStrategyEngine`.
- E1 reversals are modeled as two brokerage fills: close the existing side, then
  open the target side.
- Backtests must distinguish `signal_price`, `order_submission_time`,
  `fill_price`, and `fill_time`.
- Supported fill models are `idealized`, `slippage`, and `next_event`.
  `idealized` is optimistic and fills at the signal threshold. `next_event`
  fills only from the next chronological market event after signal generation.
- Data-quality statistics must be written for each run, including sorting and
  duplicate timestamp repairs.

## Historical Backtest Case: Continuation TP/SL

`validated_renko_backtest.cpp` implements the older continuation-case TP/SL
strategy. It remains useful for that research case and for C++ Renko parity, but
it is not a production-equivalent backtest for the current live E1 reversal
profiles.

TP/SL semantics:

- `exit_management=strategy` exits are strategy-managed and evaluated on Renko
  brick closes.
- `exit_management=oco` exits are broker-managed after entry submission and are
  raw-market-price/order-book dependent. A Renko-close TP/SL backtest is not
  equivalent to production OCO execution.
- Do not infer TP/SL ordering from OHLC bars.

## Live 1S Renko Source

Live configs can set:

```json
{
  "data": {
    "renko_source": "1s_close"
  }
}
```

That makes `fleet_runner.py` send only completed 1-second close prices into the
Renko engine. This matches the current backtest source assumption as closely as
the Alpaca SIP feed allows.

## Removed For Clean Slate

These paths are intentionally not part of the current contract:

- `backtester_v2.py`
- `production_replay_validate.py`
- `parity_gate.py`
- `scripts/run_raw_renko_sweep.py`
- `scripts/compare_tradingview_overlay.py`
- `_legacy_quarantine/`
- `artifacts/backtests/`

Do not use old artifact folders, raw-tick sweep outputs, or legacy replay results
as deployment evidence.
