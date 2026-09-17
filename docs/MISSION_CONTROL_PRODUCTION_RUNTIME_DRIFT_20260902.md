# Mission Control Production Runtime Drift 2026-09-02

Status: read-only drift note captured during the Mission Control telemetry
deployment gate. This document is not a remediation plan.

## Context

The droplet's `/root/trading-server/DEPLOYED_VERSION.json` reported deployed
commit `604fc800128586f3b244a4614e24437b7cebe03a`, deployed at
`2026-08-07T13:42:53.252819+00:00`.

During the 2026-09-02 Mission Control telemetry deployment, current production
runtime file hashes were compared against the clean telemetry deployment commit
`f915a0fe1ec62bdaa58469bfa0f632d89668af94`. Several non-telemetry runtime
files differed from the clean commit source. Because the standard deploy script
promotes the full runtime bundle, a normal full-bundle promotion would have
overwritten those drifted production files.

## Drifted Runtime Paths

| Path | Production hash observed | Clean commit hash |
| --- | --- | --- |
| `alpaca_trader.py` | `f3c337d7714b13900bd3cdf1bac82ba76e093f36921fdadcccef0488fabeaf84` | `ac9c68e4c106a2006b3b43fa536e446017eabe3ee70da607163ed78edb138bd8` |
| `execution_engine.py` | `30a8f1c2da626f333654663d176304fe845aa0f008ab0514b343c9b1a95f93f0` | `bd6f1ce359da5fe6faa1ba6a2fd6bcdc5f0d5bd4f37b352a05f18f59514de231` |
| `fleet_runner.py` | `8674e5f6785ebe2e67e5c331a39b3da95fb7c9c81eadbcbae31ce4c081b79cb3` | `b0b263bc8b1fc5d4495300dbbd38f218a4473b606ec45ccc0a0734205e1a9125` |
| `renko_builder.py` | `b0b51043575f31375cc84b90d8a5c83eb8566f8b1dcb34fad7d2abfe033d1b2c` | `c47042ef11740da36a0c5a529983ad4724243c593230c3a30d6db74d4090436e` |
| `strategy_engine.py` | `a815b4b10f7bc5886f22387a59a61f993d576195745c393156f473a3ad9a0e02` | `b089d944c1c47502ce5a60a26b323853becc25c86b50b9acde682da847bf966d` |
| `trading_events.py` | `b7127bcbc79b6c3cec3ccd7988f5c43085ee36f7aadceabf361fa0e5125da660` | `aae301169526c7a6c1a4b48a9df75b7af9b5eb05142f3ef23f360aa7b18827ae` |
| `shared/` aggregate | `41f51ad7177ebdecc94ccd00e6d619c5fa7016ec69f21a7162affdbfc9947e05` | `0331e50591bd8742f14f1835ae5cc130ce8f0149096beebb0d0fa61526f679ad` |
| `silver/` aggregate | `fc4ddfa8684729059e03c6c8660d5176e2c0e575d4c6b9e96e62797450fe09b2` | `989c3b94e6daa796d2a39c0e01ed42d89a0c316f36b6c19f8da08c08d8dca98f` |

`requirements.txt` matched the clean commit hash:
`b8d329fec4bd834acb458451ff8bf93c1136ae06a6bcd56c4c8fc43de6b10ba7`.

## Safety Decision

The telemetry deployment used a constrained Mission Control overlay instead of
the full runtime bundle. This avoided overwriting the drifted trading,
execution, Renko, strategy, shared market-data, and Silver runtime files.

Broker-state caching work must continue to avoid modifying or normalizing these
drifted paths unless a separate production drift reconciliation phase is
approved.
