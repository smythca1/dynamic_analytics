# Strategy Lifecycle

Date: 2026-07-15

This repo should make strategy state visible from Git history. Every material
strategy change should be committed with an explicit lifecycle stage in the
commit subject.

## Stages

`dev`

- Hypothesis, implementation work, local experiments, disabled config entries.
- Must not have `execution_enabled: true`.
- Evidence can be incomplete, but assumptions should be written in the commit
  body or profile notes.

`test`

- Backtest, paper, replay, or limited validation work.
- Must include the dataset window, slippage assumption, sizing assumption, and
  artifact path when artifacts exist.
- Any live-facing config should remain disabled unless explicitly marked as a
  controlled test.

`prod`

- Live trading candidate with `enabled: true` and `execution_enabled: true`.
- Must include active start date, quantity, risk limits, and backtest or replay
  evidence in the strategy profile.
- Production promotion should follow a passing local baseline:

```bash
make compile
make test
```

`ops`

- Deployment, monitoring, environment, or manual intervention changes.
- Use this for service changes, manual flatten scripts, fleet status fixes, and
  operational documentation.

## Commit Subjects

Use one of these prefixes for strategy lifecycle commits:

```text
strategy(dev): add <symbol> <strategy hypothesis>
strategy(test): backtest <symbol> <strategy/config>
strategy(prod): promote <symbol> <strategy/config>
strategy(prod): retire <symbol> <strategy/config>
strategy(ops): deploy <fleet/service/config change>
```

Examples:

```text
strategy(test): backtest qqq chop mean reversion at 5c slippage
strategy(prod): promote qqq chop mean reversion live hedge
strategy(dev): add nvda renko efficiency gate candidate
strategy(prod): retire tsla tp/sl renko trader
strategy(ops): deploy camatron live fleet update
```

## Promotion Checklist

Before moving a strategy to `prod`, the commit should answer:

- What symbol and strategy ID changed?
- What config parameters changed?
- What data period was tested?
- What slippage, sizing, and session assumptions were used?
- What are return, max drawdown, trade count, and buy-and-hold comparison?
- Is the live config explicitly enabled for execution?
- What date does the trader become active?

## Config Signals

The live config should continue to expose lifecycle state directly:

- Production: `enabled: true`, `execution_enabled: true`, `mode: "live"`.
- Test or paper: `enabled: true`, `execution_enabled: false`, `mode: "paper"`.
- Development candidate: `enabled: false`, `execution_enabled: false`.

When a strategy changes stage, update `configs/traders.live.example.json` and
commit it with the matching `strategy(...)` prefix.
