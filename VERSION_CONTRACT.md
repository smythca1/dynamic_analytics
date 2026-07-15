# Production Version Contract

Date: 2026-06-22

This repo now has one active Renko backtest standard and one live trading path.
Older raw-tick backtest/replay code and generated artifacts have been removed to
avoid mixing incompatible assumptions.

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

- `validated_renko_backtest.cpp`

The passing local baseline is:

```bash
make compile
make test
```

## Canonical Backtest Case: Continuation Case

The canonical Renko TP/SL backtest logic is named `continuation_case`.

Data and Renko rules:

- Aggregate stored ticks to 1-second closes before Renko construction.
- Use TradingView-style Renko reversal behavior:
  - one brick continues an existing run,
  - reversal requires a two-brick move from the previous brick close.
- Use grid anchoring unless a task explicitly states otherwise.
- The raw tick mode in `validated_renko_backtest.cpp` is research-only and must
  be explicitly requested with `--source-mode tick`.

Strategy rules:

- Enter only while flat, on the exact `E` brick of a fresh same-direction run.
- While in a position, ignore opposing entry conditions until TP or SL exits the
  open trade.
- If a brick exits a trade, do not also enter a new trade on that same brick.
- Fill entries and exits at the Renko brick close.
- Intraday backtests must force-flat and count open positions at the configured
  flat time, defaulting to 19:59:00 UTC for the current US regular-session
  profiles.
- Use explicit slippage only when requested by the run configuration.
- Report neighbor profitability, fixed return, gross return, drawdown, trade
  count, and validation flags.

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
