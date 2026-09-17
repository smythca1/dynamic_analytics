# Mission Control Telemetry Contract V1

Status: initial read-only Mission Control exporter contract.

This contract defines the Prometheus-compatible telemetry exposed by
`monitoring_app.py` at `/metrics`. It is an observability layer only. It must
not submit, cancel, replace, or delete broker orders; must not call fleet
reconciliation; and must not mutate strategy, Renko, execution, broker
protection, or live state.

## Scope

The exporter may read:

- Fleet config JSON.
- Shared market-data health JSON.
- Shared canonical journal JSONL through a bounded tail.
- Fleet event JSONL through bounded tails.
- Persisted strategy state snapshots.
- Cached broker account, position, and open-order snapshots.

A Mission Control background broker cache refresher may call only these
read-only broker methods:

- `get_account()`
- `get_positions()`
- `get_orders(status="open", limit=100)`

The `/metrics` request path must not call Alpaca directly. It reads the latest
atomic broker cache snapshot only.

The exporter must not read full JSONL history during a scrape. First contact
with a JSONL source starts from a bounded tail window. Subsequent scrapes read
only newly appended bytes unless the file shrinks or advances beyond the tail
window.

## Broker Cache

Mission Control owns a read-only background broker cache refresher with a
production cadence bounded to approximately 2 to 5 seconds by
`MISSION_CONTROL_BROKER_CACHE_REFRESH_SECONDS` (default `3`). Broker read
timeouts are bounded by `MISSION_CONTROL_BROKER_CACHE_READ_TIMEOUT_SECONDS`
(default `2`).

Each refresh builds a complete per-fleet result off the scrape path. The visible
cache is replaced only after a complete successful fleet refresh, so `/metrics`
readers never observe partially refreshed broker state. If a refresh fails, the
last known good snapshot is preserved and `renko_broker_cache_up` becomes `0`
for that fleet until a fresh successful refresh occurs. Startup with no
successful snapshot emits `NaN` broker values, cache age `NaN`,
`renko_broker_cache_up` `0`, and a broker source error.

Broker cache freshness is measured by
`renko_broker_cache_last_success_timestamp_seconds`; cache age can be derived in
Prometheus with `time() - renko_broker_cache_last_success_timestamp_seconds`.
The exporter also emits `renko_broker_cache_age_seconds` for direct inspection.

## Label Policy

Only these data labels are allowed:

- `environment`
- `fleet`
- `strategy`
- `symbol`
- `side`
- `service`
- `event_type`

Histogram bucket samples also use Prometheus' required control label `le`.

Do not use order IDs, client order IDs, signal IDs, trade IDs, timestamps,
prices, raw exception text, file paths, or free-form reason strings as labels.

If multiple configured traders collapse to the same bounded metric identity
(`fleet`, `strategy`, `symbol`), V1 emits only one deterministic representative
series and sets
`renko_telemetry_source_error{service="fleet_config_label_collision"}` to `1`
for that identity. Selection prefers enabled traders, then execution-enabled
traders, then the newest state snapshot mtime.

Bounded `side` values are `flat`, `long`, `short`, and `unknown`.
Bounded `event_type` values are:

- `MARKET_TICK`
- `BRICK_CREATED`
- `SIGNAL_GENERATED`
- `ORDER_SUBMITTED`
- `EXECUTION_ERROR`
- `STRATEGY_BLOCKED`
- `MARKET_DATA_BLOCKED`
- `RECONCILIATION_SUCCESS`
- `RECONCILIATION_FAILURE`
- `PROTECTIVE_ORDER_SUBMITTED`
- `PROTECTIVE_ORDER_VERIFIED`
- `PROTECTIVE_ORDER_RECOVERY`
- `PROTECTION_INVARIANT_FAILURE`
- `RUNTIME_HALTED`
- `FORCE_FLAT`
- `DUPLICATE_MARKET_EVENT`
- `OTHER`

## Freshness And Readiness

Market-data staleness is interpreted against a regular-session approximation:
Monday through Friday, `09:30:00` to `16:00:00` in `America/New_York`.

During the regular session, market data is stale when canonical observation age
exceeds `MISSION_CONTROL_REGULAR_SESSION_STALE_SECONDS` or the source timestamp
is unavailable.

Outside the regular session, observation age remains visible, but
`renko_market_data_stale` does not fail solely because no new market ticks are
arriving. This avoids treating normal after-hours inactivity as a production
failure. Holiday-aware readiness is not included in V1.

Fleet readiness is `1` only when all enabled traders in the fleet have valid
state, acceptable heartbeat freshness for the current market-session state,
read-only internal-vs-broker position match, safe reconciliation state, broker
cache freshness/read success, and regular-session market-data freshness when the
market is open.

## Metric Inventory

| Metric | Type | Labels | Source |
| --- | --- | --- | --- |
| `renko_market_session_open` | gauge | `environment`, `service` | Local NY regular-session approximation |
| `renko_readiness` | gauge | `environment`, `fleet`, `service` | Derived from read-only state, broker, and market-data inputs |
| `renko_telemetry_collector_up` | gauge | `environment`, `service` | Exporter self-check |
| `renko_telemetry_scrape_duration_seconds` | gauge | `environment`, `service` | Exporter wall-clock measurement |
| `renko_telemetry_source_error` | gauge | `environment`, optional `fleet`, optional `strategy`, optional `symbol`, `service` | Exporter source read status |
| `renko_telemetry_source_last_success_timestamp_seconds` | gauge | `environment`, optional `fleet`, optional `strategy`, optional `symbol`, `service` | Last successful bounded source read |
| `renko_telemetry_jsonl_tail_truncated` | gauge | `environment`, `service` | Bounded JSONL reader |
| `renko_telemetry_jsonl_bytes_read` | gauge | `environment`, `service` | Bounded JSONL reader |
| `renko_telemetry_malformed_jsonl_lines_total` | counter | `environment`, `service` | Bounded JSONL reader |
| `renko_sip_connected` | gauge | `environment`, `service` | Shared market-data health JSON |
| `renko_sip_last_event_timestamp_seconds` | gauge | `environment`, `symbol`, `service` | Shared market-data health JSON |
| `renko_sip_reconnects_total` | counter | `environment`, `service` | Shared market-data health JSON |
| `renko_sip_ticks_total` | counter | `environment`, `symbol`, `service` | Shared market-data health JSON |
| `renko_canonical_observations_total` | counter | `environment`, `symbol`, `service` | Bounded canonical journal tail, latest sequence |
| `renko_canonical_last_observation_timestamp_seconds` | gauge | `environment`, `symbol`, `service` | Shared health JSON or bounded canonical journal tail |
| `renko_canonical_observation_age_seconds` | gauge | `environment`, `symbol`, `service` | Shared health JSON or derived from last canonical observation |
| `renko_market_data_gap_detected` | gauge | `environment`, `service` | Shared market-data health JSON and consumer health |
| `renko_market_data_stale` | gauge | `environment`, `symbol`, `service` | Derived from freshness and market-session state |
| `renko_last_price` | gauge | `environment`, `symbol`, `service` | Bounded canonical journal tail |
| `renko_trader_up` | gauge | `environment`, `fleet`, `strategy`, `symbol` | State snapshot and bounded event tail heartbeat |
| `renko_trader_last_heartbeat_timestamp_seconds` | gauge | `environment`, `fleet`, `strategy`, `symbol` | State snapshot or bounded event tail |
| `renko_strategy_armed` | gauge | `environment`, `fleet`, `strategy`, `symbol` | Config, state validity, reconciliation safety, and trader session |
| `renko_strategy_state_valid` | gauge | `environment`, `fleet`, `strategy`, `symbol` | Persisted strategy state snapshot |
| `renko_execution_enabled` | gauge | `environment`, `fleet`, `strategy`, `symbol` | Fleet config |
| `renko_position_state` | gauge | `environment`, `fleet`, `strategy`, `symbol`, `side` | Persisted strategy state snapshot |
| `renko_position_quantity` | gauge | `environment`, `fleet`, `strategy`, `symbol` | Persisted strategy state snapshot |
| `renko_position_entry_price` | gauge | `environment`, `fleet`, `strategy`, `symbol` | Persisted strategy state snapshot |
| `renko_position_unrealized_pnl` | gauge | `environment`, `fleet`, `strategy`, `symbol` | Cached broker positions snapshot |
| `renko_reconciliation_status` | gauge | `environment`, `fleet`, `strategy`, `symbol` | Read-only internal-vs-broker comparison |
| `renko_pending_orders` | gauge | `environment`, `fleet`, `strategy`, `symbol` | Cached broker open-orders snapshot |
| `renko_protective_orders_present` | gauge | `environment`, `fleet`, `strategy`, `symbol` | Cached broker open-orders snapshot |
| `renko_internal_vs_broker_position_match` | gauge | `environment`, `fleet`, `strategy`, `symbol` | Read-only internal-vs-broker comparison |
| `renko_orphan_broker_position_detected` | gauge | `environment`, `fleet`, `strategy`, `symbol` | Read-only internal-vs-broker comparison |
| `renko_orphan_internal_position_detected` | gauge | `environment`, `fleet`, `strategy`, `symbol` | Read-only internal-vs-broker comparison |
| `renko_reconciliation_last_success_timestamp_seconds` | gauge | `environment`, `fleet`, `strategy`, `symbol` | Bounded event tail |
| `renko_reconciliation_failures_total` | counter | `environment`, `fleet`, `symbol`, `service` | Bounded event tail |
| `renko_last_brick_price` | gauge | `environment`, `fleet`, `strategy`, `symbol` | Persisted Renko strategy state |
| `renko_last_brick_timestamp_seconds` | gauge | `environment`, `fleet`, `strategy`, `symbol` | Bounded event tail |
| `renko_bricks_total` | counter | `environment`, `fleet`, `strategy`, `symbol` | Persisted Renko strategy state processed-brick count |
| `renko_brick_direction` | gauge | `environment`, `fleet`, `strategy`, `symbol` | Persisted Renko strategy state |
| `renko_broker_cache_last_success_timestamp_seconds` | gauge | `environment`, `fleet`, `service` | Background broker cache refresh |
| `renko_broker_cache_age_seconds` | gauge | `environment`, `fleet`, `service` | Derived from cached broker snapshot success time |
| `renko_broker_cache_refresh_duration_seconds` | gauge | `environment`, `fleet`, `service` | Background broker refresh wall-clock duration |
| `renko_broker_cache_refresh_failures_total` | counter | `environment`, `fleet`, `service` | Background broker refresh failures |
| `renko_broker_cache_up` | gauge | `environment`, `fleet`, `service` | Broker cache freshness and latest refresh health |
| `renko_broker_connected` | gauge | `environment`, `fleet`, `service` | Fresh broker cache health |
| `renko_broker_account_equity` | gauge | `environment`, `fleet`, `service` | Cached broker account snapshot |
| `renko_broker_buying_power` | gauge | `environment`, `fleet`, `service` | Cached broker account snapshot |
| `renko_broker_cash` | gauge | `environment`, `fleet`, `service` | Cached broker account snapshot |
| `renko_broker_daytrading_buying_power` | gauge | `environment`, `fleet`, `service` | Cached broker account snapshot |
| `renko_broker_margin_used` | gauge | `environment`, `fleet`, `service` | Derived from broker account long/short market value and equity |
| `renko_broker_position_qty` | gauge | `environment`, `fleet`, `symbol`, `service` | Cached broker positions snapshot |
| `renko_broker_position_market_value` | gauge | `environment`, `fleet`, `symbol`, `service` | Cached broker positions snapshot |
| `renko_broker_open_orders` | gauge | `environment`, `fleet`, `service` | Cached broker open-orders snapshot |
| `renko_portfolio_total_equity` | gauge | `environment`, `fleet`, `service` | Cached broker account snapshot |
| `renko_portfolio_gross_exposure` | gauge | `environment`, `fleet`, `service` | Cached broker account snapshot |
| `renko_portfolio_net_exposure` | gauge | `environment`, `fleet`, `service` | Cached broker account snapshot |
| `renko_portfolio_cash` | gauge | `environment`, `fleet`, `service` | Cached broker account snapshot |
| `renko_portfolio_unrealized_pnl` | gauge | `environment`, `fleet`, `service` | Cached broker positions snapshot |
| `renko_portfolio_realized_pnl_today` | gauge | `environment`, `fleet`, `service` | Intentionally `NaN` in V1 |
| `renko_activity_events_total` | counter | `environment`, `fleet`, `strategy`, `symbol`, `side`, `event_type` | Bounded event tail |
| `renko_signals_total` | counter | `environment`, `fleet`, `strategy`, `symbol`, `side` | Bounded event tail |
| `renko_orders_submitted_total` | counter | `environment`, `fleet`, `strategy`, `symbol`, `side` | Bounded execution event tail |
| `renko_orders_filled_total` | counter | `environment`, `fleet`, `strategy`, `symbol`, `side` | Bounded execution event tail when status is filled or partially filled |
| `renko_strategy_blocks_total` | counter | `environment`, `fleet`, `strategy`, `symbol`, `side` | Bounded event tail |
| `renko_execution_errors_total` | counter | `environment`, `fleet`, `strategy`, `symbol`, `side` | Bounded event tail |
| `renko_execution_ack_latency_seconds` | histogram | `environment`, `fleet`, `strategy`, `symbol`, `side`, `le` on buckets | Bounded execution event tail |
| `renko_execution_fill_latency_seconds` | histogram | `environment`, `fleet`, `strategy`, `symbol`, `side`, `le` on buckets | Bounded execution event tail when fill timestamps exist |

## Source-Of-Truth Map

| Surface | Source of truth | Notes |
| --- | --- | --- |
| Executive readiness | Derived from all V1 read-only sources | No trading mutation or reconciliation call |
| Market data health | Shared market-data health JSON plus canonical journal tail | No websocket subscription by exporter |
| Positions | Persisted strategy state and cached broker positions snapshot | Read-only comparison only |
| Trading activity | Fleet JSONL event tails | Counters begin from bounded tail on exporter start |
| Execution quality | Execution JSONL events | Fill latency requires fill timestamps in event payloads |
| Broker/account | Background cache built from Alpaca read-only account, positions, orders GET methods | `/metrics` uses cache only; failures emit source errors and cache health gauges |
| Reconciliation | Persisted state plus cached broker positions and bounded reconciliation events | Does not call live reconciliation functions |
| Portfolio | Cached broker account and positions snapshots | Realized PnL intentionally unavailable in V1 |
| Infrastructure | Node/systemd exporters outside this app | CPU/RAM/disk/restarts are not emitted by V1 app exporter |
| Research/optimizer | Future artifact exporter | V1 does not scan optimizer outputs |

## Known Gaps

- `renko_portfolio_realized_pnl_today` is emitted as `NaN` and
  `renko_telemetry_source_error{service="canonical_accounting"}` is `1` until
  live canonical accounting state exists.
- `ORDER_PARTIAL_FILL`, exact `ORDER_FILLED`, `POSITION_OPENED`,
  `POSITION_CLOSED`, `TAKE_PROFIT_HIT`, and `STOP_LOSS_HIT` cannot be
  reconstructed completely from current bounded event tails. V1 exposes only
  events that are durably present in the bounded tail.
- Event counters reset when the mission-control process restarts and may skip
  older history by design.
- Holiday-aware market-session readiness is not included.
- Broker position/order metrics are symbol-scoped. V1 assumes one active
  strategy per fleet/symbol/account for reconciliation display.
- Broker position/account metrics can represent a preserved last known good
  snapshot while `renko_broker_cache_up` is `0`; alerting must gate on cache
  health/freshness before treating broker values as current.
- Duplicate configured traders with identical bounded `fleet`, `strategy`, and
  `symbol` labels cannot be represented separately without increasing label
  cardinality. V1 collapses them and emits a visible label-collision source
  error.

## Deployment Notes

Adding or updating `/metrics` does not require changes to fleet services, shared
market-data services, broker execution, strategy code, Renko code, production
configuration, Prometheus, or Grafana. A safe production rollout should deploy
the mission-control code only, scrape it locally, validate read-only metrics and
cache cadence, then add Prometheus and Grafana in separate approved steps.
