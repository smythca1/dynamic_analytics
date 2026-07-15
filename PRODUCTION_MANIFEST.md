# Production Manifest

Date: 2026-06-22

This repo has been cleaned after the Renko backtester alignment. Old generated
backtest artifacts, raw-tick backtest code, replay validators, and quarantined
research files are intentionally removed.

## Keep In Production Root

Runtime and execution:

- `fleet_runner.py`
- `strategy_engine.py`
- `renko_builder.py`
- `trading_events.py`
- `execution_engine.py`
- `alpaca_trader.py`
- `monitoring_app.py`

Current aligned backtester:

- `validated_renko_backtest.cpp`

Deployment and config:

- `deploy-trading`
- `trading-fleet-live.service`
- `mission-control.service`
- `requirements.txt`
- `configs/traders.live.example.json`
- `.env`
- `.env.example`

Operations:

- `scripts/manual_flatten_profiles.py`
- `scripts/check_syntax.py`
- `scripts/prune_to_production.py`

Tests:

- `tests/test_alpaca_trader.py`
- `tests/test_execution_engine.py`
- `tests/test_renko_anchor.py`
- `tests/test_strategy_engine.py`

Docs and controls:

- `VERSION_CONTRACT.md`
- `PRODUCTION_MANIFEST.md`
- `Makefile`

## Removed

These are no longer part of the active repo:

- `artifacts/backtests/`
- `_legacy_quarantine/`
- `backtester_v2.py`
- `production_replay_validate.py`
- `parity_gate.py`
- `scripts/run_raw_renko_sweep.py`
- `scripts/compare_tradingview_overlay.py`
- Tests for the removed modules

## Baseline

Run:

```bash
make compile
make test
```

No stale backtest artifact should be used as evidence. New backtests should be
generated from `validated_renko_backtest.cpp` using the 1-second-close
continuation-case standard described in `VERSION_CONTRACT.md`.
