import csv
import json
import math
import re
import statistics
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path("artifacts/production_health/20260908")
SNAPSHOT_PATH = ROOT / "remote_snapshot.json"


def parse_ts(value):
    if value in (None, ""):
        return None
    raw = str(value)
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw).astimezone(timezone.utc)
    except Exception:
        return None


def write_json(path, payload):
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")


def write_csv(path, rows, fields):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def money(value):
    if value in (None, ""):
        return ""
    try:
        return f"${float(value):,.2f}"
    except Exception:
        return str(value)


def pct(value):
    if value in (None, ""):
        return ""
    try:
        return f"{float(value):g}%"
    except Exception:
        return str(value)


def strategy_family(row):
    if row.get("strategy_type") == "renko_tp_sl":
        return "TP/SL"
    if row.get("strategy_type") == "renko_reversal":
        return "Reversal"
    return str(row.get("strategy_type") or "")


def entry(row):
    return f"E{row.get('entry_bricks')}" if row.get("entry_bricks") not in (None, "") else ""


def active_strategies(snapshot):
    return [row for row in snapshot["strategies"] if row.get("enabled") is True and row.get("execution_enabled") is True]


def state_by_trader(snapshot):
    return {row.get("trader_id"): row for row in snapshot["states"] if row.get("trader_id")}


def account_name(strategy):
    return "megatron" if str(strategy.get("account_namespace") or "").lower() == "megatron" else "camatron"


def broker_account(snapshot, strategy):
    return snapshot.get("broker", {}).get("accounts", {}).get(account_name(strategy), {})


def broker_positions(snapshot, strategy):
    symbol = str(strategy.get("symbol") or "").upper()
    return [row for row in broker_account(snapshot, strategy).get("target_positions", []) if str(row.get("symbol") or "").upper() == symbol]


def broker_orders(snapshot, strategy):
    symbol = str(strategy.get("symbol") or "").upper()
    return [row for row in broker_account(snapshot, strategy).get("target_open_orders_nested", []) if str(row.get("symbol") or "").upper() == symbol]


def canonical_trade_state(state):
    if not isinstance(state, dict):
        return {}
    snapshot = state.get("snapshot") if isinstance(state.get("snapshot"), dict) else state
    engine = snapshot.get("engine") if isinstance(snapshot.get("engine"), dict) else {}
    trade = engine.get("trade") if isinstance(engine.get("trade"), dict) else None
    if isinstance(trade, dict):
        return trade
    trade = snapshot.get("trade") if isinstance(snapshot.get("trade"), dict) else None
    if isinstance(trade, dict):
        return trade
    return snapshot


def state_position_label(state):
    trade = canonical_trade_state(state)
    position = trade.get("position")
    if not position:
        return "flat"
    qty = abs(float(position.get("qty") or position.get("quantity") or 0))
    side = str(position.get("side") or "")
    return f"{side} {qty:g}"


def broker_position_label(rows):
    if not rows:
        return "flat"
    row = rows[0]
    return f"{row.get('side')} {abs(float(row.get('qty') or 0)):g}"


def positions_match(state, broker_rows):
    trade = canonical_trade_state(state)
    position = trade.get("position")
    if not position and not broker_rows:
        return True
    if not position or len(broker_rows) != 1:
        return False
    broker = broker_rows[0]
    return (
        str(position.get("side") or "").lower() == str(broker.get("side") or "").lower()
        and abs(abs(float(position.get("qty") or position.get("quantity") or 0)) - abs(float(broker.get("qty") or 0))) < 1e-9
    )


def protection_status(state, orders):
    if not state:
        return "missing state"
    trade = canonical_trade_state(state)
    protection = trade.get("protection_state")
    if isinstance(protection, dict):
        payload = protection.get("payload") if isinstance(protection.get("payload"), dict) else {}
        issues = payload.get("issues") if isinstance(payload, dict) else None
        issue_codes = ",".join(str(issue.get("code")) for issue in issues or [] if isinstance(issue, dict))
        base = protection.get("state") or payload.get("state")
        if issue_codes:
            return f"{base}; issues={issue_codes}"
        stop = protection.get("stop_price") or protection.get("stop_loss_stop_price") or protection.get("stop_loss_trigger_price")
        lineage = [
            f"parent={protection.get('parent_order_id')}" if protection.get("parent_order_id") else "",
            f"tp={protection.get('take_profit_order_id')}" if protection.get("take_profit_order_id") else "",
            f"sl={protection.get('stop_loss_order_id')}" if protection.get("stop_loss_order_id") else "",
        ]
        lineage_text = "; ".join(item for item in lineage if item)
        if lineage_text:
            return f"{base}; {lineage_text}"
        return f"{base}; stop={stop}" if stop else str(base)
    if orders:
        return f"{len(orders)} broker orders; lifecycle={trade.get('tp_sl_lifecycle_state')}"
    return str(trade.get("tp_sl_lifecycle_state") or "none")


def recursive_orders(order):
    if not isinstance(order, dict):
        return
    yield order, order
    for leg in order.get("legs") or []:
        if isinstance(leg, dict):
            yield leg, order


def infer_trader_id(client_order_id, active_ids):
    text = str(client_order_id or "")
    matches = [tid for tid in active_ids if text.startswith(tid[: min(len(tid), 42)]) or text.startswith(tid.split("_qty1_")[0][:42])]
    if matches:
        return sorted(matches, key=len, reverse=True)[0]
    for tid in active_ids:
        prefix = "_".join(tid.split("_")[:6])
        if prefix and text.startswith(prefix):
            return tid
    return ""


def build_order_map(account, active_ids):
    out = {}
    for order in account.get("recent_orders_since_previous_session") or []:
        for item, parent in recursive_orders(order):
            oid = str(item.get("id") or "")
            if not oid:
                continue
            parent_client = parent.get("client_order_id")
            client = item.get("client_order_id") or parent_client
            out[oid] = {
                "client_order_id": client,
                "parent_client_order_id": parent_client,
                "trader_id": infer_trader_id(parent_client or client, active_ids),
                "submitted_at": item.get("submitted_at"),
                "filled_at": item.get("filled_at"),
                "order_side": item.get("side"),
                "position_intent": item.get("position_intent"),
                "order_type": item.get("type") or item.get("order_type"),
            }
    return out


def signed_qty(side, qty):
    side = str(side or "").lower()
    qty = float(qty or 0)
    return qty if side == "buy" else -qty


def fifo_performance(rows):
    lots = []
    realized = 0.0
    trades = []
    long_pnl = 0.0
    short_pnl = 0.0
    for row in sorted(rows, key=lambda r: (str(r.get("transaction_time")), str(r.get("activity_id")))):
        qty = signed_qty(row.get("side"), row.get("qty"))
        price = float(row.get("price") or 0)
        remaining = qty
        while abs(remaining) > 1e-9 and lots and (lots[0]["qty"] > 0) != (remaining > 0):
            lot = lots[0]
            close_qty = min(abs(remaining), abs(lot["qty"]))
            if lot["qty"] > 0:
                pnl = (price - lot["price"]) * close_qty
                long_pnl += pnl
            else:
                pnl = (lot["price"] - price) * close_qty
                short_pnl += pnl
            realized += pnl
            trades.append(
                {
                    "entry_time": lot["time"],
                    "exit_time": row.get("transaction_time"),
                    "side": "long" if lot["qty"] > 0 else "short",
                    "qty": close_qty,
                    "entry_price": lot["price"],
                    "exit_price": price,
                    "pnl": pnl,
                }
            )
            lot["qty"] += close_qty if lot["qty"] < 0 else -close_qty
            remaining += close_qty if remaining < 0 else -close_qty
            if abs(lot["qty"]) < 1e-9:
                lots.pop(0)
        if abs(remaining) > 1e-9:
            lots.append({"qty": remaining, "price": price, "time": row.get("transaction_time")})
    wins = [t["pnl"] for t in trades if t["pnl"] > 0]
    losses = [t["pnl"] for t in trades if t["pnl"] <= 0]
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    curve = []
    total = 0.0
    for trade in trades:
        total += trade["pnl"]
        curve.append(total)
    max_dd = 0.0
    peak = 0.0
    for value in curve:
        peak = max(peak, value)
        max_dd = min(max_dd, value - peak)
    holds = []
    for trade in trades:
        start = parse_ts(trade["entry_time"])
        end = parse_ts(trade["exit_time"])
        if start and end:
            holds.append((end - start).total_seconds())
    return {
        "closed_trades": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "win_pct": (len(wins) / len(trades) * 100) if trades else None,
        "gross_pnl": realized,
        "costs": 0.0,
        "net_pnl": realized,
        "profit_factor": (gross_win / gross_loss) if gross_loss else (None if not gross_win else "inf"),
        "expectancy": realized / len(trades) if trades else None,
        "avg_winner": statistics.mean(wins) if wins else None,
        "avg_loser": statistics.mean(losses) if losses else None,
        "largest_winner": max(wins) if wins else None,
        "largest_loser": min(losses) if losses else None,
        "max_realized_drawdown": abs(max_dd),
        "long_pnl": long_pnl,
        "short_pnl": short_pnl,
        "avg_holding_seconds": statistics.mean(holds) if holds else None,
        "open_qty": sum(lot["qty"] for lot in lots),
        "open_lots": lots,
        "trades": trades,
    }


def live_performance(snapshot, active):
    active_ids = [row["id"] for row in active]
    rows = []
    fill_details = []
    for account_name, account in snapshot.get("broker", {}).get("accounts", {}).items():
        order_map = build_order_map(account, active_ids)
        for fill in account.get("fills_since_previous_session") or []:
            oid = str(fill.get("order_id") or "")
            meta = order_map.get(oid, {})
            trader_id = meta.get("trader_id") or infer_trader_id(meta.get("parent_client_order_id") or meta.get("client_order_id"), active_ids)
            fill_details.append(
                {
                    "account": account_name,
                    "trader_id": trader_id or "UNATTRIBUTED",
                    "symbol": str(fill.get("symbol") or "").upper(),
                    "side": str(fill.get("side") or "").lower(),
                    "qty": float(fill.get("qty") or 0),
                    "price": float(fill.get("price") or 0),
                    "transaction_time": fill.get("transaction_time"),
                    "order_id": oid,
                    "activity_id": fill.get("id"),
                    "client_order_id": meta.get("client_order_id"),
                    "parent_client_order_id": meta.get("parent_client_order_id"),
                    "submitted_at": meta.get("submitted_at"),
                    "position_intent": meta.get("position_intent"),
                }
            )
    by_trader = {}
    for fill in fill_details:
        by_trader.setdefault(fill["trader_id"], []).append(fill)
    strategy_by_id = {row["id"]: row for row in active}
    for trader_id, fills in sorted(by_trader.items()):
        perf = fifo_performance(fills)
        strategy = strategy_by_id.get(trader_id, {})
        first = min((parse_ts(row.get("transaction_time")) for row in fills if parse_ts(row.get("transaction_time"))), default=None)
        last = max((parse_ts(row.get("transaction_time")) for row in fills if parse_ts(row.get("transaction_time"))), default=None)
        hours = ((last - first).total_seconds() / 3600) if first and last and last > first else None
        rows.append(
            {
                "Engine": strategy.get("engine") or "",
                "Symbol": fills[0]["symbol"],
                "Strategy": strategy_family(strategy) if strategy else "UNATTRIBUTED",
                "Trader ID": trader_id,
                "Closed Trades": perf["closed_trades"],
                "Wins": perf["wins"],
                "Losses": perf["losses"],
                "Win %": perf["win_pct"],
                "Gross P&L": perf["gross_pnl"],
                "Costs": perf["costs"],
                "Net P&L": perf["net_pnl"],
                "Return": "",
                "PF": perf["profit_factor"],
                "Expectancy": perf["expectancy"],
                "Avg Winner": perf["avg_winner"],
                "Avg Loser": perf["avg_loser"],
                "Largest Winner": perf["largest_winner"],
                "Largest Loser": perf["largest_loser"],
                "Max Realized Drawdown": perf["max_realized_drawdown"],
                "Long P&L": perf["long_pnl"],
                "Short P&L": perf["short_pnl"],
                "Avg Holding Seconds": perf["avg_holding_seconds"],
                "Trades Per Hour": perf["closed_trades"] / hours if hours else "",
                "Open Qty From Fills": perf["open_qty"],
                "Fill Count": len(fills),
            }
        )
    return rows, fill_details


def latency_summary(rows, group_key):
    grouped = {}
    for row in rows:
        grouped.setdefault(row.get(group_key) or "", []).append(row)
    out = []
    for key, values in sorted(grouped.items()):
        for metric in ("event_to_submitted_ms", "signal_log_to_submitted_ms", "submitted_to_execution_log_ms", "submitted_to_filled_ms"):
            nums = [float(row[metric]) for row in values if row.get(metric) is not None]
            out.append(
                {
                    group_key: key,
                    "Metric": metric,
                    "Samples": len(nums),
                    "p50": statistics.median(nums) if nums else "",
                    "p90": percentile(nums, 90) if len(nums) >= 10 else "",
                    "p95": percentile(nums, 95) if len(nums) >= 20 else "",
                    "min": min(nums) if nums else "",
                    "max": max(nums) if nums else "",
                    "Assessment": "INSUFFICIENT DATA" if len(nums) < 10 else "MEASURED",
                }
            )
    return out


def percentile(values, p):
    if not values:
        return None
    ordered = sorted(values)
    rank = (len(ordered) - 1) * p / 100
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return ordered[low]
    return ordered[low] + (ordered[high] - ordered[low]) * (rank - low)


def build():
    snapshot = json.loads(SNAPSHOT_PATH.read_text())
    active = active_strategies(snapshot)
    states = state_by_trader(snapshot)

    write_json(ROOT / "deployed_camatron_config.json", snapshot["configs"]["camatron"])
    write_json(ROOT / "deployed_megatron_config.json", snapshot["configs"]["megatron"])
    write_json(ROOT / "deployed_market_data_config.json", snapshot["configs"]["market_data"])
    write_json(ROOT / "broker_state.json", snapshot["broker"])

    strategy_rows = []
    broker_rows = []
    state_summary = {"captured_at": snapshot["captured_at"], "active_strategy_count": len(active), "active_states": [], "legacy_nonflat_states": []}
    critical_mismatches = []
    for strategy in active:
        state = states.get(strategy["id"])
        positions = broker_positions(snapshot, strategy)
        orders = broker_orders(snapshot, strategy)
        match = positions_match(state, positions)
        protection = protection_status(state, orders)
        if not match:
            critical_mismatches.append(f"{strategy['engine']} {strategy['symbol']} state={state_position_label(state)} broker={broker_position_label(positions)}")
        strategy_rows.append(
            {
                "Engine": strategy["engine"],
                "Symbol": strategy["symbol"],
                "Strategy": strategy_family(strategy),
                "Brick": strategy.get("brick_pct_configured"),
                "Entry": entry(strategy),
                "TP": strategy.get("take_profit_bricks") or "",
                "SL": strategy.get("stop_loss_bricks") or "",
                "Qty": strategy.get("qty"),
                "Runtime State": state_position_label(state),
                "State Path": state.get("path") if state else "MISSING",
                "Lifecycle": state.get("tp_sl_lifecycle_state") if state else "MISSING",
                "Protection": protection,
                "Trader ID": strategy["id"],
            }
        )
        broker_rows.append(
            {
                "Engine": strategy["engine"],
                "Symbol": strategy["symbol"],
                "Local Position": state_position_label(state),
                "Broker Position": broker_position_label(positions),
                "Open Orders": len(orders),
                "Protection": protection,
                "Match": "YES" if match else "NO",
            }
        )
        state_summary["active_states"].append(
            {
                "strategy": strategy,
                "state": state,
                "broker_positions": positions,
                "broker_open_orders": orders,
                "position_match": match,
            }
        )
    active_ids = {row["id"] for row in active}
    for state in snapshot["states"]:
        trade = canonical_trade_state(state)
        if state.get("trader_id") not in active_ids and trade.get("position") is not None:
            state_summary["legacy_nonflat_states"].append(
                {
                    "trader_id": state.get("trader_id"),
                    "symbol": state.get("symbol"),
                    "position": trade.get("position"),
                    "lifecycle": trade.get("tp_sl_lifecycle_state"),
                    "path": state.get("path"),
                    "mtime_utc": (state.get("metadata") or {}).get("mtime_utc"),
                }
            )
    write_json(ROOT / "state_summary.json", state_summary)
    write_csv(ROOT / "strategies.csv", strategy_rows, ["Engine", "Symbol", "Strategy", "Brick", "Entry", "TP", "SL", "Qty", "Runtime State", "State Path", "Lifecycle", "Protection", "Trader ID"])
    write_csv(ROOT / "broker_reconciliation.csv", broker_rows, ["Engine", "Symbol", "Local Position", "Broker Position", "Open Orders", "Protection", "Match"])

    service_rows = []
    for name, service in snapshot["services"].items():
        service_rows.append(
            {
                "Component": name,
                "Status": f"{service.get('ActiveState')}/{service.get('SubState')}",
                "Uptime": service.get("ExecMainStartTimestamp") or service.get("NextElapseUSecRealtime"),
                "Restarts": service.get("NRestarts") or "",
                "Key Health Evidence": f"pid={service.get('MainPID')}; result={service.get('Result')}; restart={service.get('Restart')}; mem={service.get('MemoryCurrent')}",
                "Assessment": "RED" if service.get("ActiveState") == "failed" else "GREEN" if service.get("ActiveState") == "active" else "YELLOW",
            }
        )
    write_csv(ROOT / "service_health.csv", service_rows, ["Component", "Status", "Uptime", "Restarts", "Key Health Evidence", "Assessment"])

    perf_rows, fill_details = live_performance(snapshot, active)
    write_csv(
        ROOT / "live_performance.csv",
        perf_rows,
        ["Engine", "Symbol", "Strategy", "Trader ID", "Closed Trades", "Wins", "Losses", "Win %", "Gross P&L", "Costs", "Net P&L", "Return", "PF", "Expectancy", "Avg Winner", "Avg Loser", "Largest Winner", "Largest Loser", "Max Realized Drawdown", "Long P&L", "Short P&L", "Avg Holding Seconds", "Trades Per Hour", "Open Qty From Fills", "Fill Count"],
    )
    write_csv(ROOT / "broker_fills_attributed.csv", fill_details, ["account", "trader_id", "symbol", "side", "qty", "price", "transaction_time", "order_id", "activity_id", "client_order_id", "parent_client_order_id", "submitted_at", "position_intent"])

    execution_quality = latency_summary(snapshot.get("execution_rows", []), "symbol")
    write_csv(ROOT / "execution_quality.csv", execution_quality, ["symbol", "Metric", "Samples", "p50", "p90", "p95", "min", "max", "Assessment"])

    parity_rows = []
    for strategy in active:
        parity_rows.append(
            {
                "Symbol": strategy["symbol"],
                "Strategy": strategy_family(strategy),
                "Backtest Trades": "NOT_RUN_STOP_CONDITION",
                "Live Trades": "",
                "Backtest Net": "NOT_RUN_STOP_CONDITION",
                "Live Net": "",
                "Trade Parity": "NOT ASSESSED",
                "P&L Parity": "NOT ASSESSED",
                "Limitation": "Fleet services failed and broker/state mismatch found before replay; raw_archive_enabled=false so raw SIP tick parity is unavailable from market-data archive.",
            }
        )
    write_csv(ROOT / "live_vs_backtest_parity.csv", parity_rows, ["Symbol", "Strategy", "Backtest Trades", "Live Trades", "Backtest Net", "Live Net", "Trade Parity", "P&L Parity", "Limitation"])

    anomalies = [
        {"Timestamp": snapshot["captured_at"], "Component": "services", "Severity": "critical", "Evidence": "trading-fleet-live.service failed; trading-fleet-megatron-live.service failed"},
    ]
    for mismatch in critical_mismatches:
        anomalies.append({"Timestamp": snapshot["captured_at"], "Component": "broker_reconciliation", "Severity": "critical", "Evidence": mismatch})
    for engine, log in snapshot["events"].items():
        counts = log.get("counts_by_type", {})
        for event_type in ("critical_protection_invariant_violation", "stream_error", "risk_block"):
            if counts.get(event_type):
                anomalies.append({"Timestamp": snapshot["captured_at"], "Component": engine, "Severity": "critical" if "critical" in event_type else "warning", "Evidence": f"{event_type} count in captured log tail: {counts[event_type]}"})
    for unit in ("trading-fleet-live.service", "trading-fleet-megatron-live.service"):
        tail = snapshot["journal_tail"].get(unit, {}).get("stdout", "")
        for line in tail.splitlines()[-80:]:
            if "BrokerEngineStateMismatch" in line or "ALPACA RESPONSE: 403" in line or "Failed with result" in line:
                anomalies.append({"Timestamp": line[:15], "Component": unit, "Severity": "critical" if "BrokerEngineStateMismatch" in line or "Failed with result" in line else "warning", "Evidence": line})
    write_csv(ROOT / "anomalies.csv", anomalies, ["Timestamp", "Component", "Severity", "Evidence"])

    hashes = []
    for row in snapshot["critical_file_hashes"]:
        hashes.append(
            {
                "File": row.get("relative_path") or row.get("path"),
                "Path": row.get("path"),
                "Hash": row.get("sha256") if row.get("exists") else "MISSING",
                "Git Commit / Version": "deployed file hash; /root/trading-server is not a git checkout",
                "mtime_utc": row.get("mtime_utc", ""),
            }
        )
    for line in snapshot.get("effective_systemd_hashes", {}).get("stdout", "").splitlines():
        match = re.match(r"([0-9a-f]{64})\s+(\S+)", line)
        if match:
            hashes.append({"File": Path(match.group(2)).name + " effective-systemd", "Path": match.group(2), "Hash": match.group(1), "Git Commit / Version": "effective systemd unit/drop-in hash", "mtime_utc": ""})
    write_csv(ROOT / "config_hashes.csv", hashes, ["File", "Path", "Hash", "Git Commit / Version", "mtime_utc"])

    health = snapshot["market_data"]["health"]
    journal_current = snapshot["market_data"]["journal_stats_current"]["by_symbol"]
    sip_green = all(journal_current.get(symbol, {}).get("observations", 0) > 0 for symbol in ("AAPL", "NVDA", "TSLA")) and health.get("journal_healthy") is True
    scorecard = [
        {"Area": "Services", "Status": "RED", "Evidence": "Both fleet services failed; market-data remains active", "Concern": "Trading engines are down"},
        {"Area": "SIP Data", "Status": "GREEN" if sip_green else "YELLOW", "Evidence": "Canonical journal has current observations for AAPL/NVDA/TSLA; no health gaps/drops reported", "Concern": "raw_archive_enabled=false limits raw SIP parity"},
        {"Area": "Renko Processing", "Status": "RED", "Evidence": "Fleet state mtimes stopped near 13:30-13:32 after service failures", "Concern": "Renko processing is not advancing after crashes"},
        {"Area": "Strategy Processing", "Status": "RED", "Evidence": "Camatron failed at 13:32:31; Megatron failed at 13:30:07", "Concern": "Active strategies are not running"},
        {"Area": "Broker Reconciliation", "Status": "RED", "Evidence": "; ".join(critical_mismatches) if critical_mismatches else "No mismatch", "Concern": "Local state does not match broker truth" if critical_mismatches else ""},
        {"Area": "Protection", "Status": "RED", "Evidence": "Camatron AAPL/TSLA state reports missing_exit_plan/protection verification failure; Megatron AAPL orphan protection caused crash", "Concern": "Protection lifecycle failures"},
        {"Area": "Execution", "Status": "RED", "Evidence": "Recent order sequence preceded protection/reconciliation failures; sample too small for percentiles", "Concern": "Execution cannot be considered healthy while engines are failed"},
        {"Area": "Live/Backtest Parity", "Status": "RED", "Evidence": "Replay intentionally not run after stop-condition mismatch", "Concern": "Parity cannot be validated against failed live state"},
        {"Area": "Trading Performance", "Status": "YELLOW", "Evidence": "Broker fills captured; realized P&L computed where attribution was possible", "Concern": "Small/incomplete sample and open/unrealized positions"},
        {"Area": "Infrastructure", "Status": "YELLOW", "Evidence": "Disk/memory captured; market data alive; retention timer enabled", "Concern": "Retention may remove parity inputs"},
    ]
    write_csv(ROOT / "scorecard.csv", scorecard, ["Area", "Status", "Evidence", "Concern"])

    attributed_perf_rows = [
        row
        for row in perf_rows
        if row.get("Trader ID") != "UNATTRIBUTED" and int(row.get("Closed Trades") or 0) > 0
    ]
    total_realized = sum(float(row.get("Net P&L") or 0) for row in perf_rows if row.get("Trader ID") != "UNATTRIBUTED")
    strongest = max(attributed_perf_rows, key=lambda r: float(r.get("Net P&L") or 0), default={})
    weakest = min(attributed_perf_rows, key=lambda r: float(r.get("Net P&L") or 0), default={})

    md = [
        "# Production Trader Health & Performance Audit - 2026-09-08",
        "",
        "Read-only audit. No deployment, restart, reconciliation, config/state edit, order cancel/submit, position close, protection edit, or monitoring edit was performed.",
        "",
        f"Captured at: `{snapshot['captured_at']}`",
        f"Host: `{snapshot['host'].get('stdout', '').strip()}`",
        "",
        "## Critical Findings",
        "",
        "- `trading-fleet-live.service` is `failed/failed`, PID 0.",
        "- `trading-fleet-megatron-live.service` is `failed/failed`, PID 0.",
        "- Camatron AAPL state says `short 1`, broker says `flat`.",
        "- Camatron TSLA state says `long 1`, broker says `flat`.",
        "- Camatron AAPL/TSLA protection state shows `missing_exit_plan` and `PROTECTION_FAILED` evidence.",
        "- Megatron crashed at the open on AAPL orphan broker-native protection while broker was flat.",
        "- Per stop condition, production-config replay was not run after broker/state disagreement was found.",
        "",
        "## Production Truth",
        "",
        "| Engine | Symbol | Strategy | Brick | Entry | TP | SL | Qty | Runtime State | Lifecycle |",
        "| --- | --- | --- | ---: | --- | ---: | ---: | ---: | --- | --- |",
    ]
    for row in strategy_rows:
        md.append("| " + " | ".join([str(row["Engine"]), str(row["Symbol"]), str(row["Strategy"]), pct(row["Brick"]), str(row["Entry"]), str(row["TP"]), str(row["SL"]), str(row["Qty"]), str(row["Runtime State"]), str(row["Lifecycle"])]) + " |")
    md.extend(["", "## Service & Infrastructure Health", "", "| Component | Status | Uptime/Start | Restarts | Evidence | Assessment |", "| --- | --- | --- | ---: | --- | --- |"])
    for row in service_rows:
        md.append("| " + " | ".join([row["Component"], row["Status"], str(row["Uptime"]), str(row["Restarts"]), row["Key Health Evidence"], row["Assessment"]]) + " |")
    md.extend(["", "## SIP / Market Data Health", ""])
    for symbol, stats_row in journal_current.items():
        md.append(f"- {symbol}: latest canonical observation `{stats_row.get('last_observation')}`, observations since current session start `{stats_row.get('observations')}`, max gap `{stats_row.get('max_observation_gap_seconds')}` seconds.")
    md.extend(
        [
            f"- Contract: `{snapshot['configs']['market_data'].get('canonical_source')}`.",
            f"- SIP connected: `{health.get('sip_connected')}`; subscription acknowledged: `{health.get('subscription_acknowledged')}`; journal healthy: `{health.get('journal_healthy')}`.",
            f"- raw_archive_enabled: `{snapshot['configs']['market_data'].get('raw_archive_enabled')}`. This prevents raw SIP tick parity from the market-data archive; only canonical 1-second journal parity can be assessed from this source.",
            "",
            "## Broker Reconciliation",
            "",
            "| Engine | Symbol | Local Position | Broker Position | Open Orders | Protection | Match |",
            "| --- | --- | ---: | ---: | ---: | --- | --- |",
        ]
    )
    for row in broker_rows:
        md.append("| " + " | ".join([row["Engine"], row["Symbol"], row["Local Position"], row["Broker Position"], str(row["Open Orders"]), row["Protection"], row["Match"]]) + " |")
    md.extend(
        [
            "",
            "## End-To-End Execution Audit",
            "",
            "End-to-end execution evidence exists in event logs and broker fills, but the chain is not healthy. Camatron submitted bracket orders around `2026-09-08T13:31-13:32Z`; protection verification failed with `missing_exit_plan`, then the service failed. Megatron failed at `2026-09-08T13:30:07Z` on orphan protection for AAPL while broker was flat.",
            "",
            "Latency samples from captured execution rows are too small for p90/p95 interpretation. See `execution_quality.csv`.",
            "",
            "## Live Trading Performance",
            "",
            f"Current Production P&L, realized only and excluding unattributed fills: `{money(total_realized)}`. Open/unrealized P&L is separate in `broker_state.json`.",
            "",
            "| Engine | Symbol | Strategy | Closed Trades | Wins | Losses | Net P&L | PF | Expectancy |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | --- | ---: |",
        ]
    )
    for row in perf_rows:
        md.append("| " + " | ".join([str(row["Engine"]), str(row["Symbol"]), str(row["Strategy"]), str(row["Closed Trades"]), str(row["Wins"]), str(row["Losses"]), money(row["Net P&L"]), str(row["PF"]), money(row["Expectancy"])]) + " |")
    md.extend(
        [
            "",
            "## Live vs Production-Parity Backtest",
            "",
            "Not run. Stop condition triggered by failed fleet services plus broker/local-state disagreement. Running parity after this point would risk producing a misleading validation against an unhealthy live state. The intended replay source remains the canonical 1-second production journal with exact deployed configs; this is distinct from raw SIP tick parity, which is limited by `raw_archive_enabled=false`.",
            "",
            "## Anomaly Review",
            "",
            f"- Anomaly rows written: `{len(anomalies)}` in `anomalies.csv`.",
            "- Most important anomalies are the two failed fleet services, Camatron AAPL/TSLA broker-state mismatches, protection verification failures, and Megatron AAPL orphan-protection crash.",
            "",
            "## Final Health Scorecard",
            "",
            "| Area | Status | Evidence | Concern |",
            "| --- | --- | --- | --- |",
        ]
    )
    for row in scorecard:
        md.append("| " + " | ".join([row["Area"], row["Status"], row["Evidence"], row["Concern"]]) + " |")
    md.extend(
        [
            "",
            "## PRODUCTION VERDICT",
            "",
            "**UNHEALTHY — TRADING SHOULD BE REVIEWED IMMEDIATELY**",
            "",
            "Concrete basis: both trading fleet services are failed, Camatron active strategy state disagrees with broker truth for AAPL and TSLA, and protection lifecycle evidence shows missing exit-plan/protection failures. Market data itself is still running, but the strategy engines are not healthy.",
            "",
            "### Current Production P&L",
            "",
            f"Total realized P&L from attributed fills in the captured broker window: `{money(total_realized)}`. See `live_performance.csv` for breakdown. Open/unrealized P&L remains broker truth only and is not counted as realized performance.",
            "",
            "### Strongest Strategy So Far",
            "",
            f"{strongest.get('Trader ID', 'INSUFFICIENT DATA')} with attributed net `{money(strongest.get('Net P&L'))}`. Evidence is too small/unstable for a strategy-quality conclusion.",
            "",
            "### Weakest Strategy So Far",
            "",
            f"{weakest.get('Trader ID', 'INSUFFICIENT DATA')} with attributed net `{money(weakest.get('Net P&L'))}`. Given the service/protection failures, weakness may be structural rather than statistical.",
            "",
            "### Live/Backtest Parity",
            "",
            "Not validated because the stop condition fired. The canonical 1-second replay path remains available, but raw SIP tick parity is not available from the production market-data raw archive because `raw_archive_enabled=false`.",
            "",
            "### Execution Quality",
            "",
            "Insufficient clean sample for percentile conclusions. The meaningful execution finding is not latency; it is protection/reconciliation failure after live orders.",
            "",
            "### Highest-Priority Concern",
            "",
            "Both live strategy fleet services are failed after broker/protection invariants fired.",
            "",
            "### Recommended Next Step",
            "",
            "Review production immediately using the captured broker/state evidence before any restart, reconciliation, or strategy resumption decision.",
        ]
    )
    (ROOT / "production_health.md").write_text("\n".join(md) + "\n")


if __name__ == "__main__":
    build()
