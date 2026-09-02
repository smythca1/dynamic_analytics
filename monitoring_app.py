from __future__ import annotations

import json
import os
from collections import deque
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, PlainTextResponse

from alpaca_trader import AlpacaTrader, load_alpaca_credentials
from mission_control_telemetry import (
    MissionControlTelemetryCollector,
    TelemetryConfig,
    render_emergency_metrics,
)


load_dotenv()

app = FastAPI(title="Trading Mission Control")


def env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value not in {None, ""} else float(default)


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


@lru_cache(maxsize=16)
def get_trader(mode: str | None, credential_profile: str | None = None) -> AlpacaTrader:
    return AlpacaTrader(
        credentials=load_alpaca_credentials(mode, profile=credential_profile),
        max_notional=env_float("MAX_NOTIONAL", 250000.0),
    )


def default_event_log_path() -> Path:
    return Path(os.getenv("LIVE_EVENT_LOG", "artifacts/live/live_events.jsonl"))


def default_fleet_config_path() -> Path:
    return Path(os.getenv("TRADER_CONFIG_PATH", "configs/traders.example.json"))


@lru_cache(maxsize=1)
def get_telemetry_collector() -> MissionControlTelemetryCollector:
    return MissionControlTelemetryCollector(TelemetryConfig.from_environment())


def load_fleet_config(path: Path | None = None) -> dict[str, Any]:
    config_path = path or default_fleet_config_path()
    if not config_path.exists():
        return {
            "fleet_id": "unconfigured",
            "source": str(config_path),
            "traders": [],
        }

    with config_path.open("r", encoding="utf-8") as handle:
        config = json.load(handle)

    traders = config.get("traders")
    if not isinstance(traders, list):
        raise ValueError(f"{config_path} must contain a top-level traders array.")

    config["source"] = str(config_path)
    return config


def tail_jsonl(path: Path, limit: int) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []

    rows: deque[dict[str, Any]] = deque(maxlen=limit)
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return list(rows)


def parse_logged_at(event: dict[str, Any]) -> datetime:
    raw = str(event.get("logged_at", ""))
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)


def event_payload(event: dict[str, Any]) -> dict[str, Any]:
    payload = event.get("payload")
    return payload if isinstance(payload, dict) else {}


def event_symbol(event: dict[str, Any]) -> str:
    payload = event_payload(event)
    intent = payload.get("intent")
    if isinstance(intent, dict) and intent.get("symbol"):
        return str(intent["symbol"]).upper()
    if payload.get("symbol"):
        return str(payload["symbol"]).upper()
    return ""


def event_strategy(event: dict[str, Any]) -> str:
    payload = event_payload(event)
    intent = payload.get("intent")
    if isinstance(intent, dict) and intent.get("strategy"):
        return str(intent["strategy"])
    if payload.get("strategy"):
        return str(payload["strategy"])
    return ""


def event_trader_id(event: dict[str, Any]) -> str:
    payload = event_payload(event)
    intent = payload.get("intent")
    if payload.get("trader_id"):
        return str(payload["trader_id"])
    if isinstance(intent, dict) and intent.get("trader_id"):
        return str(intent["trader_id"])
    return ""


def trader_mode(trader: dict[str, Any], fallback: str | None, force_mode: bool = False) -> str:
    if force_mode and fallback:
        return fallback.lower()
    return str(trader.get("mode") or fallback or os.getenv("ALPACA_TRADING_MODE") or "paper").lower()


def trader_credential_profile(trader: dict[str, Any]) -> str | None:
    value = trader.get("credential_profile")
    return str(value) if value not in {None, ""} else None


def trader_event_log_path(trader: dict[str, Any]) -> Path:
    return Path(str(trader.get("event_log") or default_event_log_path()))


def collect_events(config: dict[str, Any], limit: int) -> list[dict[str, Any]]:
    paths = {default_event_log_path()}
    for trader in config.get("traders", []):
        if isinstance(trader, dict):
            paths.add(trader_event_log_path(trader))

    events: list[dict[str, Any]] = []
    per_path_limit = max(limit, 200)
    for path in paths:
        for event in tail_jsonl(path, per_path_limit):
            event["_log_path"] = str(path)
            events.append(event)

    events.sort(key=parse_logged_at)
    return events[-limit:]


def trader_matches_event(trader: dict[str, Any], event: dict[str, Any]) -> bool:
    trader_id = str(trader.get("id", ""))
    symbol = str(trader.get("symbol", "")).upper()
    strategy_name = str(trader.get("strategy_name") or trader_id)
    explicit_trader_id = event_trader_id(event)
    if explicit_trader_id:
        return explicit_trader_id == trader_id

    event_symbol_value = event_symbol(event)
    if event_symbol_value and event_symbol_value != symbol:
        return False

    event_strategy_value = event_strategy(event)
    if event_strategy_value and event_strategy_value in {trader_id, strategy_name}:
        return True

    return bool(event_symbol_value and event_symbol_value == symbol)


def describe_strategy(trader: dict[str, Any]) -> str:
    strategy = trader.get("strategy") if isinstance(trader.get("strategy"), dict) else {}
    strategy_type = str(strategy.get("type") or "unknown")
    if strategy_type == "renko_tp_sl":
        return (
            f"Renko TP/SL {strategy.get('brick_size')} "
            f"E{strategy.get('entry_bricks')} TP{strategy.get('take_profit_bricks')} SL{strategy.get('stop_loss_bricks')}"
        )
    if strategy_type == "renko_psar":
        return (
            f"Renko PSAR {strategy.get('brick_size')} "
            f"E{strategy.get('entry_bricks')} I{strategy.get('psar_initial_stop_bricks')} "
            f"A{strategy.get('psar_af_step')} M{strategy.get('psar_af_max')}"
        )
    return strategy_type


def summarize_trader(
    trader: dict[str, Any],
    positions: list[dict[str, Any]],
    orders: list[dict[str, Any]],
    events: list[dict[str, Any]],
    fallback_mode: str | None,
    force_mode: bool = False,
) -> dict[str, Any]:
    symbol = str(trader.get("symbol", "")).upper()
    matched_positions = [position for position in positions if str(position.get("symbol", "")).upper() == symbol]
    matched_orders = [order for order in orders if str(order.get("symbol", "")).upper() == symbol]
    matched_events = [event for event in events if trader_matches_event(trader, event)]
    latest_event = matched_events[-1] if matched_events else None
    latest_signal = next((event for event in reversed(matched_events) if event.get("event_type") == "signal"), None)
    latest_execution = next((event for event in reversed(matched_events) if event.get("event_type") == "execution"), None)
    latest_brick = next((event for event in reversed(matched_events) if event.get("event_type") == "renko_brick"), None)

    market_value = sum(abs(as_float(position.get("market_value"))) for position in matched_positions)
    unrealized_pl = sum(as_float(position.get("unrealized_pl")) for position in matched_positions)
    risk = trader.get("risk") if isinstance(trader.get("risk"), dict) else {}
    max_position_notional = as_float(risk.get("max_position_notional"), default=0.0)
    max_daily_loss = as_float(risk.get("max_daily_loss"), default=0.0)
    enabled = bool(trader.get("enabled", True))

    status = "ok" if enabled else "disabled"
    alerts: list[str] = []
    if enabled and latest_event is None:
        status = "idle"
        alerts.append("no events")
    if max_position_notional and market_value > max_position_notional:
        status = "breach"
        alerts.append("position limit")
    if max_daily_loss and unrealized_pl <= -abs(max_daily_loss):
        status = "breach"
        alerts.append("loss limit")

    latest_payload = event_payload(latest_event) if latest_event else {}
    brick_payload = event_payload(latest_brick) if latest_brick else {}
    signal_payload = event_payload(latest_signal) if latest_signal else {}
    execution_payload = event_payload(latest_execution) if latest_execution else {}

    return {
        "id": trader.get("id"),
        "enabled": enabled,
        "status": status,
        "alerts": alerts,
        "symbol": symbol,
        "mode": trader_mode(trader, fallback_mode, force_mode=force_mode),
        "credential_profile": trader_credential_profile(trader),
        "broker": trader.get("broker", "alpaca"),
        "description": describe_strategy(trader),
        "session": trader.get("session", {}),
        "sizing": trader.get("sizing", {}),
        "risk": risk,
        "position_count": len(matched_positions),
        "open_order_count": len(matched_orders),
        "market_value": market_value,
        "unrealized_pl": unrealized_pl,
        "latest_event_type": latest_event.get("event_type") if latest_event else None,
        "latest_event_at": latest_event.get("logged_at") if latest_event else None,
        "latest_event": latest_payload,
        "latest_signal": signal_payload,
        "latest_execution": execution_payload,
        "latest_brick": brick_payload,
        "event_count": len(matched_events),
        "event_log": str(trader_event_log_path(trader)),
    }


def summarize_risk(account: dict[str, Any], positions: list[dict[str, Any]], orders: list[dict[str, Any]]) -> dict[str, Any]:
    equity = as_float(account.get("equity"))
    buying_power = as_float(account.get("buying_power"))
    long_market_value = abs(as_float(account.get("long_market_value")))
    short_market_value = abs(as_float(account.get("short_market_value")))
    gross_exposure = long_market_value + short_market_value
    unrealized_pl = sum(as_float(position.get("unrealized_pl")) for position in positions)
    max_position_notional = max((abs(as_float(position.get("market_value"))) for position in positions), default=0.0)
    open_order_notional = sum(
        abs(as_float(order.get("notional")))
        or abs(as_float(order.get("qty")) * as_float(order.get("limit_price") or order.get("filled_avg_price")))
        for order in orders
    )

    return {
        "mode": account.get("account_blocked") and "blocked" or "active",
        "equity": equity,
        "buying_power": buying_power,
        "cash": as_float(account.get("cash")),
        "portfolio_value": as_float(account.get("portfolio_value")),
        "long_market_value": long_market_value,
        "short_market_value": short_market_value,
        "gross_exposure": gross_exposure,
        "gross_exposure_pct": (gross_exposure / equity * 100.0) if equity else 0.0,
        "unrealized_pl": unrealized_pl,
        "open_order_notional": open_order_notional,
        "position_count": len(positions),
        "open_order_count": len(orders),
        "max_position_notional": max_position_notional,
        "max_notional_config": env_float("MAX_NOTIONAL", 250000.0),
        "pattern_day_trader": bool(account.get("pattern_day_trader")),
        "trading_blocked": bool(account.get("trading_blocked")),
        "account_blocked": bool(account.get("account_blocked")),
    }


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return DASHBOARD_HTML


@app.get("/api/account")
async def account(mode: str | None = Query(default=None, pattern="^(paper|live)?$")) -> dict[str, Any]:
    return get_trader(mode).get_account()


@app.get("/api/positions")
async def positions(mode: str | None = Query(default=None, pattern="^(paper|live)?$")) -> list[dict[str, Any]]:
    return get_trader(mode).get_positions()


@app.get("/api/orders")
async def orders(
    mode: str | None = Query(default=None, pattern="^(paper|live)?$"),
    status: str = "open",
    limit: int = 100,
) -> list[dict[str, Any]]:
    return get_trader(mode).get_orders(status=status, limit=limit)


@app.get("/api/events")
async def events(limit: int = 100, path: str | None = None) -> list[dict[str, Any]]:
    event_path = Path(path) if path else default_event_log_path()
    return tail_jsonl(event_path, max(1, min(limit, 1000)))


@app.get("/api/snapshot")
async def snapshot(
    mode: str | None = Query(default=None, pattern="^(paper|live)?$"),
    event_limit: int = 50,
) -> dict[str, Any]:
    trader = get_trader(mode)
    account_data = trader.get_account()
    position_data = trader.get_positions()
    order_data = trader.get_orders(status="open", limit=100)
    event_data = tail_jsonl(default_event_log_path(), max(1, min(event_limit, 1000)))
    return {
        "account": account_data,
        "positions": position_data,
        "orders": order_data,
        "risk": summarize_risk(account_data, position_data, order_data),
        "events": event_data,
        "event_log": str(default_event_log_path()),
    }


@app.get("/api/fleet")
async def fleet_config(path: str | None = None) -> dict[str, Any]:
    return load_fleet_config(Path(path) if path else None)


@app.get("/api/mission-control")
async def mission_control(
    mode: str | None = Query(default=None, pattern="^(paper|live)?$"),
    mode_scope: str = Query(default="configured", pattern="^(configured|force)$"),
    event_limit: int = 250,
    config_path: str | None = None,
) -> dict[str, Any]:
    config = load_fleet_config(Path(config_path) if config_path else None)
    traders_config = [trader for trader in config.get("traders", []) if isinstance(trader, dict)]
    force_mode = mode_scope == "force" and mode is not None
    account_keys = sorted(
        {
            (
                trader_mode(trader, mode, force_mode=force_mode),
                trader_credential_profile(trader),
            )
            for trader in traders_config
        }
        or {(mode or "paper", None)},
        key=lambda item: (item[0], item[1] or ""),
    )
    if mode:
        account_keys = sorted({*account_keys, (mode, None)}, key=lambda item: (item[0], item[1] or ""))
    modes = sorted({account_mode for account_mode, _profile in account_keys})

    accounts: dict[str, dict[str, Any]] = {}
    positions_by_mode: dict[str, list[dict[str, Any]]] = {}
    orders_by_mode: dict[str, list[dict[str, Any]]] = {}
    risk_by_mode: dict[str, dict[str, Any]] = {}

    for current_mode, current_profile in account_keys:
        account_key = f"{current_mode}:{current_profile or 'default'}"
        trader_client = get_trader(current_mode, current_profile)
        account_data = trader_client.get_account()
        position_data = trader_client.get_positions()
        order_data = trader_client.get_orders(status="open", limit=100)
        accounts[account_key] = account_data
        positions_by_mode[account_key] = position_data
        orders_by_mode[account_key] = order_data
        risk_by_mode[account_key] = summarize_risk(account_data, position_data, order_data)

    event_data = collect_events(config, max(1, min(event_limit, 2000)))
    trader_summaries = []
    for trader in traders_config:
        current_mode = trader_mode(trader, mode, force_mode=force_mode)
        account_key = f"{current_mode}:{trader_credential_profile(trader) or 'default'}"
        trader_summaries.append(
            summarize_trader(
                trader=trader,
                positions=positions_by_mode.get(account_key, []),
                orders=orders_by_mode.get(account_key, []),
                events=event_data,
                fallback_mode=mode,
                force_mode=force_mode,
            )
        )

    primary_mode = mode if mode in modes else (modes[0] if modes else "paper")
    primary_account_key = next(
        (
            f"{trader_mode(trader, mode, force_mode=force_mode)}:{trader_credential_profile(trader) or 'default'}"
            for trader in traders_config
            if trader_mode(trader, mode, force_mode=force_mode) == primary_mode
        ),
        f"{primary_mode}:default",
    )
    return {
        "config": {
            "fleet_id": config.get("fleet_id", "default"),
            "source": config.get("source"),
            "trader_count": len(trader_summaries),
        },
        "mode": primary_mode,
        "primary_account_key": primary_account_key,
        "mode_scope": mode_scope,
        "modes": modes,
        "accounts": accounts,
        "positions_by_mode": positions_by_mode,
        "orders_by_mode": orders_by_mode,
        "risk_by_mode": risk_by_mode,
        "traders": trader_summaries,
        "events": event_data,
        "event_logs": sorted({str(default_event_log_path()), *(summary["event_log"] for summary in trader_summaries)}),
        "server_time": datetime.now(tz=timezone.utc).isoformat(),
    }


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics() -> PlainTextResponse:
    try:
        body = get_telemetry_collector().collect_prometheus(broker_reader_for=get_trader)
    except Exception:
        body = render_emergency_metrics(os.getenv("MISSION_CONTROL_ENVIRONMENT") or "production")
    return PlainTextResponse(
        body,
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )


DASHBOARD_HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Mission Control</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f4f6f8;
      --panel: #ffffff;
      --text: #15171a;
      --muted: #667085;
      --line: #d9dde4;
      --good: #087443;
      --bad: #b42318;
      --warn: #b54708;
      --accent: #155eef;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 18px 24px;
      border-bottom: 1px solid var(--line);
      background: var(--panel);
      position: sticky;
      top: 0;
      z-index: 2;
    }
    h1 { margin: 0; font-size: 19px; letter-spacing: 0; }
    .titlebar { display: grid; gap: 2px; }
    .subtitle { color: var(--muted); font-size: 13px; }
    main { padding: 20px 24px 28px; display: grid; gap: 18px; }
    .toolbar { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
    select, button {
      height: 34px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: white;
      color: var(--text);
      padding: 0 10px;
      font: inherit;
    }
    button { cursor: pointer; border-color: #b8c0cc; }
    .status { color: var(--muted); font-size: 13px; min-width: 210px; text-align: right; }
    .grid { display: grid; gap: 12px; grid-template-columns: repeat(4, minmax(160px, 1fr)); }
    .trader-grid { display: grid; gap: 12px; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); }
    .metric, section {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
    }
    .metric { padding: 14px; min-height: 82px; }
    .label { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
    .value { margin-top: 6px; font-size: 24px; font-weight: 650; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .subvalue { margin-top: 2px; color: var(--muted); font-size: 13px; }
    .trader {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 14px;
      display: grid;
      gap: 12px;
      min-height: 228px;
    }
    .trader-head { display: flex; justify-content: space-between; gap: 10px; align-items: start; }
    .trader-title { display: grid; gap: 2px; }
    .trader-title strong { font-size: 16px; }
    .pill {
      display: inline-flex;
      align-items: center;
      min-height: 24px;
      padding: 2px 8px;
      border-radius: 999px;
      border: 1px solid var(--line);
      font-size: 12px;
      color: var(--muted);
      white-space: nowrap;
    }
    .pill.ok { color: var(--good); border-color: #99d8bb; background: #f0fdf4; }
    .pill.idle, .pill.warn { color: var(--warn); border-color: #fedf89; background: #fffbeb; }
    .pill.breach { color: var(--bad); border-color: #fecdca; background: #fef3f2; }
    .pill.disabled { color: var(--muted); background: #f2f4f7; }
    .kv { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px 14px; }
    .kv div { min-width: 0; }
    .kv span { display: block; color: var(--muted); font-size: 12px; }
    .kv strong { display: block; margin-top: 2px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    section { overflow: hidden; }
    section h2 {
      margin: 0;
      padding: 12px 14px;
      font-size: 14px;
      border-bottom: 1px solid var(--line);
      background: #fbfcfd;
    }
    .table-wrap { overflow: auto; }
    table { width: 100%; border-collapse: collapse; min-width: 760px; }
    th, td { padding: 10px 12px; border-bottom: 1px solid var(--line); text-align: right; white-space: nowrap; }
    th:first-child, td:first-child { text-align: left; }
    th { color: var(--muted); font-weight: 600; font-size: 12px; background: #fbfcfd; }
    tr:last-child td { border-bottom: 0; }
    .good { color: var(--good); }
    .bad { color: var(--bad); }
    .warn { color: var(--warn); }
    .events { display: grid; gap: 8px; padding: 12px; max-height: 460px; overflow: auto; }
    .event {
      display: grid;
      grid-template-columns: 148px 130px 1fr;
      gap: 10px;
      padding: 9px 10px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #fff;
      align-items: start;
    }
    code { color: #344054; white-space: pre-wrap; overflow-wrap: anywhere; }
    @media (max-width: 980px) {
      header { align-items: flex-start; flex-direction: column; }
      .status { text-align: left; }
      .grid { grid-template-columns: repeat(2, minmax(150px, 1fr)); }
      main { padding: 14px; }
      .event { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <header>
    <div class="titlebar">
      <h1>Mission Control</h1>
      <div class="subtitle" id="fleet">Fleet loading</div>
    </div>
    <div class="toolbar">
      <select id="scope">
        <option value="configured">Configured Fleet</option>
        <option value="force-paper">Force Paper</option>
        <option value="force-live">Force Live</option>
      </select>
      <button id="refresh">Refresh</button>
      <span class="status" id="status">Loading</span>
    </div>
  </header>
  <main>
    <div class="grid" id="metrics"></div>
    <div class="trader-grid" id="traders"></div>
    <section>
      <h2>Positions</h2>
      <div class="table-wrap"><table id="positions"></table></div>
    </section>
    <section>
      <h2>Open Orders</h2>
      <div class="table-wrap"><table id="orders"></table></div>
    </section>
    <section>
      <h2>Recent Engine Events</h2>
      <div class="events" id="events"></div>
    </section>
  </main>
  <script>
    const fmt = new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 });
    const money = new Intl.NumberFormat(undefined, { style: "currency", currency: "USD", maximumFractionDigits: 2 });
    const pct = value => `${fmt.format(Number(value || 0))}%`;
    const cssPnl = value => Number(value || 0) >= 0 ? "good" : "bad";
    const el = id => document.getElementById(id);

    function metric(label, value, subvalue = "", className = "") {
      return `<div class="metric"><div class="label">${label}</div><div class="value ${className}">${value}</div><div class="subvalue">${subvalue}</div></div>`;
    }

    function activeRisk(data) {
      return data.risk_by_mode[data.primary_account_key] || data.risk_by_mode[data.mode] || Object.values(data.risk_by_mode)[0] || {};
    }

    function activePositions(data) {
      return data.positions_by_mode[data.primary_account_key] || data.positions_by_mode[data.mode] || [];
    }

    function activeOrders(data) {
      return data.orders_by_mode[data.primary_account_key] || data.orders_by_mode[data.mode] || [];
    }

    function renderMetrics(data) {
      const r = activeRisk(data);
      const enabled = data.traders.filter(t => t.enabled).length;
      const breaches = data.traders.filter(t => t.status === "breach").length;
      el("metrics").innerHTML = [
        metric("Equity", money.format(r.equity), `Cash ${money.format(r.cash)}`),
        metric("Buying Power", money.format(r.buying_power), `Portfolio ${money.format(r.portfolio_value)}`),
        metric("Gross Exposure", money.format(r.gross_exposure), pct(r.gross_exposure_pct), r.gross_exposure_pct > 100 ? "warn" : ""),
        metric("Unrealized P/L", money.format(r.unrealized_pl), `${r.position_count} positions`, cssPnl(r.unrealized_pl)),
        metric("Fleet", `${enabled}/${data.traders.length}`, `${breaches} breaches`, breaches ? "bad" : ""),
        metric("Open Orders", fmt.format(r.open_order_count), money.format(r.open_order_notional)),
        metric("Max Position", money.format(r.max_position_notional), `Limit ${money.format(r.max_notional_config)}`, r.max_position_notional > r.max_notional_config ? "bad" : ""),
        metric("Server Time", new Date(data.server_time).toLocaleTimeString(), data.mode.toUpperCase())
      ].join("");
    }

    function renderTraders(traders) {
      el("traders").innerHTML = traders.map(t => {
        const session = t.session || {};
        const sizing = t.sizing || {};
        const latestBrick = t.latest_brick || {};
        const latestSignal = t.latest_signal || {};
        const latestExecution = t.latest_execution || {};
        const lastEventTime = t.latest_event_at ? new Date(t.latest_event_at).toLocaleTimeString() : "none";
        const notional = sizing.notional ? money.format(Number(sizing.notional)) : (sizing.qty ? `${fmt.format(sizing.qty)} sh` : "unset");
        const alerts = t.alerts && t.alerts.length ? ` · ${t.alerts.join(", ")}` : "";
        return `
          <article class="trader">
            <div class="trader-head">
              <div class="trader-title">
                <strong>${t.symbol} <span class="pill">${t.mode}${t.credential_profile ? `:${t.credential_profile}` : ""}</span></strong>
                <span class="subvalue">${t.id || ""}</span>
              </div>
              <span class="pill ${t.status}">${t.status}${alerts}</span>
            </div>
            <div class="subvalue">${t.description}</div>
            <div class="kv">
              <div><span>Position</span><strong>${money.format(t.market_value || 0)}</strong></div>
              <div><span>Open P/L</span><strong class="${cssPnl(t.unrealized_pl)}">${money.format(t.unrealized_pl || 0)}</strong></div>
              <div><span>Orders</span><strong>${fmt.format(t.open_order_count || 0)}</strong></div>
              <div><span>Size</span><strong>${notional}</strong></div>
              <div><span>Session</span><strong>${session.entry_start || "open"} / ${session.force_flat || "none"}</strong></div>
              <div><span>Overnight</span><strong>${session.allow_overnight ? "allowed" : "flat"}</strong></div>
              <div><span>Last Brick</span><strong>${latestBrick.close ? `${money.format(latestBrick.close)} · run ${latestBrick.run_len}` : "none"}</strong></div>
              <div><span>Last Event</span><strong>${lastEventTime}</strong></div>
              <div><span>Signal</span><strong>${latestSignal.reason || "none"}</strong></div>
              <div><span>Execution</span><strong>${latestExecution.status || "none"}</strong></div>
            </div>
          </article>`;
      }).join("") || "<section><h2>No Traders Configured</h2></section>";
    }

    function table(headers, rows) {
      return `<thead><tr>${headers.map(h => `<th>${h}</th>`).join("")}</tr></thead><tbody>${rows.join("") || `<tr><td colspan="${headers.length}">None</td></tr>`}</tbody>`;
    }

    function renderPositions(positions) {
      const rows = positions.map(p => `
        <tr>
          <td>${p.symbol || ""}</td>
          <td>${p.side || ""}</td>
          <td>${fmt.format(Number(p.qty || 0))}</td>
          <td>${money.format(Number(p.avg_entry_price || 0))}</td>
          <td>${money.format(Number(p.current_price || 0))}</td>
          <td>${money.format(Number(p.market_value || 0))}</td>
          <td class="${cssPnl(p.unrealized_pl)}">${money.format(Number(p.unrealized_pl || 0))}</td>
          <td class="${cssPnl(p.unrealized_plpc)}">${pct(Number(p.unrealized_plpc || 0) * 100)}</td>
        </tr>`);
      el("positions").innerHTML = table(["Symbol", "Side", "Qty", "Avg", "Last", "Value", "P/L", "P/L %"], rows);
    }

    function renderOrders(orders) {
      const rows = orders.map(o => `
        <tr>
          <td>${o.symbol || ""}</td>
          <td>${o.side || ""}</td>
          <td>${o.type || ""}</td>
          <td>${o.status || ""}</td>
          <td>${fmt.format(Number(o.qty || 0))}</td>
          <td>${o.notional ? money.format(Number(o.notional)) : ""}</td>
          <td>${o.limit_price ? money.format(Number(o.limit_price)) : ""}</td>
          <td>${o.client_order_id || ""}</td>
        </tr>`);
      el("orders").innerHTML = table(["Symbol", "Side", "Type", "Status", "Qty", "Notional", "Limit", "Client ID"], rows);
    }

    function summarizePayload(payload) {
      if (payload && payload.symbol) return payload.symbol;
      if (payload && payload.intent) return `${payload.intent.symbol} ${payload.intent.side} ${payload.intent.reason}`;
      if (payload && payload.reason) return payload.reason;
      return "";
    }

    function renderEvents(events) {
      el("events").innerHTML = events.slice().reverse().map(event => `
        <div class="event">
          <div>${new Date(event.logged_at).toLocaleTimeString()}</div>
          <strong>${event.event_type}</strong>
          <code>${summarizePayload(event.payload) || JSON.stringify(event.payload)}</code>
        </div>`).join("") || "<div class='event'>No events</div>";
    }

    async function load() {
      const scope = el("scope").value;
      const force = scope.startsWith("force-");
      const modeScope = force ? "force" : "configured";
      const params = new URLSearchParams({ mode_scope: modeScope, event_limit: "250" });
      if (force) params.set("mode", scope.replace("force-", ""));
      el("status").textContent = "Loading";
      try {
        const res = await fetch(`/api/mission-control?${params.toString()}`);
        if (!res.ok) throw new Error(await res.text());
        const data = await res.json();
        el("fleet").textContent = `${data.config.fleet_id} · ${data.config.trader_count} traders · ${data.config.source}`;
        renderMetrics(data);
        renderTraders(data.traders);
        renderPositions(activePositions(data));
        renderOrders(activeOrders(data));
        renderEvents(data.events);
        el("status").textContent = `${scope.toUpperCase()} updated ${new Date().toLocaleTimeString()}`;
      } catch (err) {
        el("status").textContent = String(err);
      }
    }

    el("refresh").addEventListener("click", load);
    el("scope").addEventListener("change", load);
    load();
    setInterval(load, 5000);
  </script>
</body>
</html>
"""
