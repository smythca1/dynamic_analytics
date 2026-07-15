# Current Strategies

Date: 2026-07-15

This file is the current human-readable strategy stage register. The executable
state remains in `configs/traders.live.example.json`; this register makes the
Git history easy to scan.

## Production

| Symbol | Trader ID | Strategy | Live Execution | Active Since |
| --- | --- | --- | --- | --- |
| AAPL | `camatron_aapl_1s_renko_1p3443_e1_reversal_1sh_20260701_live` | 1S Renko E1 reversal, long/short, overnight allowed | Yes | 2026-07-01 |
| NVDA | `camatron_nvda_1s_renko_1p149851_e1_reversal_1sh_20260701_live` | 1S Renko E1 reversal, long/short, overnight allowed | Yes | 2026-07-01 |
| TSLA | `camatron_tsla_1s_renko_2p31165_e1_reversal_1sh_20260710_live` | 1S Renko E1 reversal, long/short, overnight allowed | Yes | 2026-07-10 |

## Test

| Symbol | Trader ID | Strategy | Live Execution | Test Since |
| --- | --- | --- | --- | --- |
| QQQ | `camatron_qqq_1s_chop_mean_reversion_w180_z2_eff035_1sh_20260715_test` | 1S low-efficiency chop mean reversion, intraday force-flat | No | 2026-07-15 |

## Development

| Symbol | Trader ID | Strategy | Notes |
| --- | --- | --- | --- |
| TSLA | `candidate_tsla_renko_psar_1p90_e2_i3_a0p04_m0p6_disabled` | Renko PSAR candidate | Disabled legacy hypothesis; not production eligible without replay and parity validation. |
