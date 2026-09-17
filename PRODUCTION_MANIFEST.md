# Production Manifest

Date: 2026-07-21

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

Backtesting and validation:

- `validated_renko_backtest.cpp`
- `scripts/e1_renko_production_parity_sweep.py`
- `RENKO_SPEC.md`

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
- `tests/test_renko_golden_vectors.py`
- `tests/test_renko_cpp_parity.py`
- `tests/test_e1_sweep_execution_models.py`
- `tests/test_strategy_engine.py`

Docs and controls:

- `VERSION_CONTRACT.md`
- `PRODUCTION_MANIFEST.md`
- `STRATEGY_LIFECYCLE.md`
- `CURRENT_STRATEGIES.md`
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

No stale backtest artifact should be used as evidence. New production E1
reversal sweeps should be generated from
`scripts/e1_renko_production_parity_sweep.py` using the canonical Renko standard
in `RENKO_SPEC.md`. `validated_renko_backtest.cpp` remains in production root for
the historical continuation-case TP/SL backtester and for Python/C++ Renko
parity validation.
