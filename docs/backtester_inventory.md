# Backtester Inventory And Production-Parity Architecture

## 1. Current production execution path

`fleet_runner.py` is the live production path. It loads trader config from `configs/traders.live.example.json`, builds a `RenkoStrategyEngine` or `MeanReversionStrategyEngine`, restores engine state from `artifacts/live/state`, and uses `AlpacaExecutionEngine` only when the runner is started with `--execute-real`.

Alpaca SIP trade flow:

1. Receive a WebSocket trade message from `wss://stream.data.alpaca.markets/v2/{feed}`.
2. Convert it with `MarketTick.from_alpaca_trade()` in `trading_events.py`.
3. Archive the raw tick with `archive_tick()` before filtering.
4. Filter eligible trades with `is_price_forming_trade()`.
5. Convert eligible trades to the configured Renko source with `renko_source_ticks()`.
6. Call `runtime.engine.on_tick()` to update Renko and strategy state.
7. Emit Renko bricks, signals, and order intents.
8. Pass signals/intents through `handle_action()`.
9. Submit real orders through `AlpacaExecutionEngine.execute()` and `AlpacaTrader.submit_order()` when live execution is enabled.
10. Split reversals into close/open broker legs in `alpaca_trader.py` when an opposite broker position exists.

Production also runs a force-flat scheduler that can submit broker force-flat orders independently of a market-data tick.

## 2. Current backtesting path

The closest current backtest path is `scripts/e1_renko_production_parity_sweep.py`. It uses `RenkoStrategyEngine`, but it has its own one-second source construction, simulated execution, fill ledger, and PnL logic.

Other current or historical paths:

- `scripts/live_signal_parity.py`: replays archived ticks and compares signal sequences only.
- `validated_renko_backtest.cpp`: historical continuation TP/SL research path plus C++ Renko parity mode.
- `scripts/aapl_e1_reversal_brick_sweep.py`: hand-rolled E1 Renko/accounting.
- `scripts/e1_reversal_multi_symbol_brick_report.py`: hand-rolled multi-symbol E1 Renko/accounting.
- `scripts/qqq_chop_mean_reversion_backtest.py`: hand-rolled mean-reversion strategy simulation.
- `scripts/july_strategy_replay_vs_buy_hold.py`: uses production strategy engines but custom immediate-fill accounting.

## 3. Duplicate or conflicting logic

Trade filtering is canonical in `trading_events.py`, but duplicated in:

- `scripts/ticks_to_filtered_1s_csv.py`
- `scripts/rebuild_renko_from_ticks.py`

One-second sampling is implemented separately in:

- `fleet_runner.py`
- `scripts/live_signal_parity.py`
- `scripts/ticks_to_filtered_1s_csv.py`
- `scripts/rebuild_renko_from_ticks.py`
- `scripts/e1_renko_production_parity_sweep.py`
- `validated_renko_backtest.cpp`

Renko construction is canonical in `renko_builder.py`, but reimplemented in:

- `validated_renko_backtest.cpp`
- `scripts/aapl_e1_reversal_brick_sweep.py`
- `scripts/e1_reversal_multi_symbol_brick_report.py`
- `scripts/nvda_chop_ab_test.py`

Position accounting and PnL are duplicated in several scripts, including:

- `scripts/e1_renko_production_parity_sweep.py`
- `scripts/aapl_e1_reversal_brick_sweep.py`
- `scripts/e1_reversal_multi_symbol_brick_report.py`
- `scripts/july_strategy_replay_vs_buy_hold.py`
- `scripts/july_trader_vs_buy_hold_report.py`
- `scripts/export_alpaca_fills_pnl.py`

## 4. Root causes of production/backtest divergence

- Backtests often start from derived one-second CSVs instead of immutable raw SIP trades.
- Some historical fetch paths do not apply the same price-forming trade condition filter.
- Batch scripts may flush the final pending second, while production emits a completed second only after a later eligible trade arrives.
- Some scripts regular-hours-filter data before strategy/Renko state updates, while production session rules are handled in the strategy engine.
- Gold/report scripts recreate Silver Renko, strategy, and PnL logic.
- Current production event logs do not explicitly log canonical one-second observations.
- Live signal IDs include UUID suffixes, so parity must compare logical event fields or add deterministic replay identifiers.
- Broker force-flat scheduler behavior is separate from tick-driven strategy behavior.

## 5. Proposed Bronze/Silver/Gold architecture

Bronze:

- Immutable raw Alpaca SIP trades.
- Raw live tick archives.
- Raw broker orders and fill activities.
- Raw live event-log snapshots.
- Exchange-calendar/session metadata.

Silver:

- Price-forming trade eligibility.
- Canonical one-second sampling.
- Renko construction.
- Strategy state transitions and signal generation.
- Simulated/live fill normalization.
- Position accounting.
- Trade reconstruction.
- Realized/unrealized PnL ledger.

Gold:

- Performance metrics.
- Reports.
- Optimizer output.
- Heatmaps.
- Parity summaries.

Gold must consume Silver ledgers and must not reconstruct Silver logic.

## 6. Canonical data contracts

Required Silver event contracts:

- `RawSipTrade`: source payload, feed, mode, symbol, SIP timestamp, logged timestamp, price, size, exchange, trade id, tape, conditions, archive path, line number.
- `EligibleTrade`: source raw trade id, eligibility decision, filter version, reject reason.
- `CanonicalObservation`: UTC second bucket, emitted sequence number, price, source eligible trade id, triggering later trade id.
- `RenkoBrickEvent`: observation id, brick sequence number, open, close, direction, run length, brick size, state before/after.
- `StrategySignalEvent`: signal sequence number, source brick/observation id, side, broker side, reason, strategy, trader id.
- `SimOrderEvent`: source signal id, order intent, order sequence, submission observation/time, execution policy.
- `FillEvent`: order id, leg, side, qty, fill observation id, fill timestamp, fill price.
- `PositionTransition`: before/after position state, fill id, realized PnL if basis is known, unknown-basis flag otherwise.
- `ClosedTrade`: entry fill id, close fill id, side, qty, entry price/time, close price/time, realized PnL.
- `PnlLedger`: cash, realized PnL, unrealized PnL, total equity, mark source, basis status.

## 7. Proposed replay and execution state machines

Market replay state machine:

1. Read ordered Bronze SIP trades.
2. Emit `RawSipTrade` events.
3. Apply canonical eligibility filter.
4. Feed eligible trades into the canonical delayed one-second sampler.
5. On each emitted observation, fill orders pending from earlier observations.
6. Feed the observation into the production strategy engine.
7. Emit Renko bricks and strategy signals.
8. Convert signals to simulated orders.
9. Queue orders for the next eligible canonical observation.

Execution state machine:

- Default fill policy is next observation.
- Orders generated by observation N cannot fill on observation N.
- Reversal orders produce close and open fill legs in deterministic order.
- Pending orders remain pending if the replay window ends before a later eligible observation.

Accounting state machine:

- Respect explicit `leg=open` and `leg=close`.
- BUY closing a short realizes PnL only against known short basis.
- BUY opening a long establishes the new long basis.
- SELL closing a long realizes PnL only against known long basis.
- SELL opening a short establishes the new short basis.
- Positions open before the replay window require explicit starting basis or must be marked basis-unknown.

## 8. Files expected to change

- `trading_events.py`: add or formalize canonical event dataclasses for observations, normalized fills, position transitions, closed trades, and PnL ledger rows.
- `fleet_runner.py`: replace process-local 1-second sampling with a shared canonical sampler and optionally log canonical observation events.
- `scripts/e1_renko_production_parity_sweep.py`: convert to a wrapper around the canonical replay engine.
- `scripts/live_signal_parity.py`: reuse canonical Bronze readers, sampler, and parity comparator.
- `scripts/ticks_to_filtered_1s_csv.py`: reuse canonical filter/sampler or clearly demote to a derived export tool.
- `scripts/rebuild_renko_from_ticks.py`: reuse canonical filter/sampler/Renko path.
- `VERSION_CONTRACT.md`: document the production-parity replay contract.
- `PRODUCTION_MANIFEST.md`: document canonical Silver modules and identify legacy/non-parity scripts.
- `tests/*`: add targeted coverage for sampler, replay, execution timing, reversal accounting, and parity reporting.

## 9. New files expected to be created

- `canonical_sampler.py`: reusable delayed one-second sampler.
- `market_data_replay.py`: Bronze readers and ordered SIP trade iterator.
- `position_accounting.py`: canonical fill-to-position/PnL accounting.
- `replay_engine.py`: production-parity replay state machine.
- `parity_report.py`: event-by-event parity comparator.
- `scripts/production_parity_replay.py`: CLI for production-parity replay.
- `scripts/fetch_alpaca_sip_trades_bronze.py`: historical raw SIP trade acquisition into Bronze.
- `tests/test_canonical_sampler.py`
- `tests/test_position_accounting.py`
- `tests/test_replay_engine.py`
- `tests/test_production_parity_report.py`

## 10. Production-sensitive files that should remain unchanged

- `alpaca_trader.py`: no live order-submission behavior changes in the first parity work.
- `execution_engine.py`: no live Alpaca execution behavior changes in the first parity work.
- `configs/traders.live.example.json`: no production config changes.
- `deploy-trading`: no deploy changes.
- `trading-fleet-live.service`: no service changes.
- `mission-control.service`: no service changes.
- `.env` and secrets: untouched.
- `validated_renko_backtest.cpp`: leave as historical TP/SL research and C++ Renko parity unless separately approved.

## 11. Migration sequence broken into small pull requests

1. Add canonical sampler and unit tests. Do not wire production yet.
2. Add canonical position-accounting ledger and reversal accounting tests.
3. Add replay engine that consumes Bronze trades and calls existing production strategy engines.
4. Convert `scripts/e1_renko_production_parity_sweep.py` to use the replay engine.
5. Convert `scripts/live_signal_parity.py` to use canonical replay components.
6. Wire `fleet_runner.py` to the shared sampler after proving output parity with existing unit tests.
7. Add Bronze historical SIP acquisition tooling.
8. Add event-level parity report.
9. Add July 20-24 benchmark harness once the full raw SIP dataset and production logs are available.
10. Update docs and mark non-parity scripts as legacy/Gold research.

## 12. Unit-test plan

- Price-forming trade eligibility for empty conditions, `@`, `@,F`, and rejected conditions.
- Delayed one-second sampler behavior.
- No synthetic observations for empty seconds.
- No final pending-second flush unless explicitly requested by a non-production export mode.
- Out-of-order trade handling.
- Existing Renko golden vectors.
- Restart determinism for Renko state.
- Strategy signal sequence from canonical observations.
- Next-observation fill timing.
- BUY+BUY and SELL+SELL reversal accounting.
- Unknown starting basis behavior.
- Deterministic repeated replay output.

## 13. Integration-test plan

- Replay synthetic raw SIP JSONL through raw, eligible, observation, Renko, signal, order, fill, position, closed-trade, and PnL ledgers.
- Replay archived QQQ raw ticks from `artifacts/production_current/ticks`.
- Compare replayed signals against `artifacts/production_current/fleet_live_events_20260720_latest.jsonl`.
- Compare replayed Renko bricks against production `renko_brick` events when matching raw inputs and starting state are available.
- Confirm tests never instantiate a real `AlpacaExecutionEngine` or submit orders.

## 14. July 20-24 production-parity test plan

The local repository does not currently contain enough raw data to reproduce the full July 20-24, 2026 benchmark. Available local data includes QQQ raw production ticks for July 15, July 16, July 17, and July 20 only, plus production event logs through July 21 snapshots and derived CSVs mostly through July 20.

Benchmark plan:

1. Acquire Bronze raw SIP trades for every benchmark symbol and completed session from July 20, 2026 through July 24, 2026.
2. Capture matching production event logs, broker order records, and broker fill records.
3. Identify explicit starting Renko, strategy, and position state for the benchmark window.
4. Replay raw SIP trades through the canonical Silver pipeline.
5. Compare raw counts, eligible counts, observations, Renko bricks, signals, simulated orders, normalized fills, position transitions, closed trades, and PnL.
6. Treat current reported PnL values as outputs to verify, not as ground truth.

## 15. Acceptance criteria

- Identical canonical one-second observations for identical ordered SIP input.
- Identical Renko brick sequence.
- Identical strategy signal sequence.
- Identical position direction changes.
- Correct BUY+BUY and SELL+SELL reversal accounting.
- No same-observation look-ahead fills.
- Explicit handling of positions open before the replay window.
- Deterministic repeated runs.
- Traceable event-level parity report.
- No live Alpaca order submission during tests.

## 16. Risks and unresolved questions

- Historical Alpaca API ordering may not match live receive ordering for all equal or near-equal SIP timestamps.
- Full July 20-24 raw SIP data is not present locally.
- Force-flat scheduler behavior needs explicit modeling, especially when no trade arrives exactly at force-flat time.
- Existing production logs do not contain canonical observation IDs.
- Live UUID signal IDs cannot be compared directly.
- The canonical eligible-condition set should be confirmed before long-term contract freeze.
- Starting position basis must be provided explicitly or PnL must be marked basis-unknown.
- Some existing scripts and docs already describe parity, but their implementation still mixes Silver and Gold responsibilities.

## 17. Approval required

This document is an implementation inventory and plan. Code changes to create the canonical replay architecture require separate approval under the repository Phase 1/Phase 2 workflow.
