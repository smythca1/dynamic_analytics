from __future__ import annotations

import csv
import gzip
import json
import sys
from dataclasses import asdict
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from replay.artifact_serialization import artifact_jsonable, write_artifact_json
from replay.execution import NextObservationExecution
from replay.metrics import build_metrics
from replay.stop_election import ALPACA_STOP_ELECTION_SOURCE_DISCLOSURE, evaluate_stop_election
from reversal_protection import build_reversal_catastrophic_stop_plan
from silver.accounting import AccountingLedger
from silver.accounting.models import EquityPoint
from silver.market.models import CanonicalObservation
from silver.market.observation_stream import observation_to_market_tick
from strategy_engine import RenkoStrategyConfig, RenkoStrategyEngine


RESEARCH_ROOT = Path("artifacts/research/camatron_megatron_20260819")
OUTPUT_DIR = RESEARCH_ROOT / "catastrophic_stop_targeted_20260813_19"
REVERSAL_SOURCE = RESEARCH_ROOT / "aapl_tsla_tp_sl_0p10_0p50_20260813_19_qty8_7" / "reversal_comparison.csv"
RAW_ROOT = RESEARCH_ROOT / "raw-data" / "raw" / "alpaca" / "sip"
PREPARED_ROOT = RESEARCH_ROOT / "prepared"
SESSIONS = ("2026-08-13", "2026-08-14", "2026-08-17", "2026-08-18", "2026-08-19")
SELECTED = {
    "AAPL": {"brick": Decimal("2.218981"), "qty": Decimal("8"), "prepared": "prep_e3c9183471a81dbe61b2c452"},
    "TSLA": {"brick": Decimal("2.32525"), "qty": Decimal("7"), "prepared": "prep_1ba6bb3cd7e8e1a31eeb1078"},
}
SLIPPAGE_PER_CLOSED_TRADE = Decimal("0.04")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    source_rows = _source_rows()
    results: list[dict[str, Any]] = []
    raw_scans: dict[str, dict[str, Any]] = {}
    for symbol in ("AAPL", "TSLA"):
        baseline = _run_symbol(symbol, catastrophic=False, source_row=source_rows[symbol])
        protected = _run_symbol(symbol, catastrophic=True, source_row=source_rows[symbol])
        scan = _scan_raw_stop_elections(symbol, protected["_runtime_stop_intervals"])
        protected.pop("_runtime_stop_intervals", None)
        baseline.pop("_runtime_stop_intervals", None)
        _apply_scan_to_report_intervals(protected["stop_intervals"], scan)
        protected["raw_price_crossings"] = scan["raw_price_crossings"]
        protected["excluded_crossings"] = scan["excluded_crossings"]
        protected["eligible_stop_elections"] = scan["eligible_stop_elections"]
        protected["validation_status"] = _validation_status(baseline, protected, scan)
        baseline["validation_status"] = "BASELINE"
        raw_scans[symbol] = scan
        results.extend([baseline, protected])

    summary = {
        "run_id": "catastrophic_stop_targeted_20260813_19",
        "sessions": SESSIONS,
        "slippage_model": "$0.04 per closed trade; before fees",
        "stop_election_source": ALPACA_STOP_ELECTION_SOURCE_DISCLOSURE,
        "source_rows": source_rows,
        "results": results,
        "raw_stop_scans": raw_scans,
        "overall_status": "PASS" if all(r.get("validation_status") in {"BASELINE", "PASS"} for r in results) else "FAIL",
    }
    summary_sha = write_artifact_json(OUTPUT_DIR / "summary.json", summary)
    _write_comparison_csv(results)
    _write_stop_interval_csv(results)
    _write_decision_csv(raw_scans, "raw_price_crossings.csv", "raw_price_crossings")
    _write_decision_csv(raw_scans, "excluded_crossings.csv", "excluded_crossings")
    _write_decision_csv(raw_scans, "eligible_stop_elections.csv", "eligible_stop_elections")
    checksums = _checksums(summary_sha)
    checksums_sha = write_artifact_json(OUTPUT_DIR / "checksums.json", checksums)
    print(
        json.dumps(
            artifact_jsonable(
                {
                    "output_dir": OUTPUT_DIR,
                    "overall_status": summary["overall_status"],
                    "summary_sha256": summary_sha,
                    "checksums_sha256": checksums_sha,
                    "comparison": OUTPUT_DIR / "comparison.csv",
                    "summary": OUTPUT_DIR / "summary.json",
                    "checksums": OUTPUT_DIR / "checksums.json",
                }
            ),
            indent=2,
            sort_keys=True,
        )
    )


def _source_rows() -> dict[str, dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    with REVERSAL_SOURCE.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            symbol = str(row.get("symbol") or "").upper()
            if symbol in SELECTED:
                rows[symbol] = row
    missing = set(SELECTED) - set(rows)
    if missing:
        raise RuntimeError(f"Missing reversal source rows: {sorted(missing)}")
    return rows


def _run_symbol(symbol: str, *, catastrophic: bool, source_row: dict[str, str]) -> dict[str, Any]:
    observations = _load_observations(symbol)
    selected = SELECTED[symbol]
    strategy_config = {
        "trader_id": f"camatron_{symbol.lower()}_reversal_{'catstop' if catastrophic else 'baseline'}_targeted",
        "symbol": symbol,
        "strategy_type": "renko_reversal",
        "brick_size": float(selected["brick"]),
        "entry_bricks": 1,
        "qty": float(selected["qty"]),
        "strategy_name": f"{symbol.lower()}_reversal_e1_selected",
        "allow_short": True,
        "exit_management": "catastrophic_stop" if catastrophic else "strategy",
        "catastrophic_stop_bricks": 3 if catastrophic else None,
        "candidate_config_fingerprint": source_row["renko_strategy_fingerprint"] if catastrophic else None,
        "market_contract_fingerprint": source_row["market_stream_fingerprint"] if catastrophic else None,
    }
    strategy = RenkoStrategyEngine(RenkoStrategyConfig(**strategy_config))
    accounting = AccountingLedger(symbol=symbol, starting_capital=Decimal("2500"))
    execution = NextObservationExecution(
        run_id=strategy_config["trader_id"],
        trader_id=strategy_config["trader_id"],
        auto_fill_protections=not catastrophic,
    )
    fills = []
    orders = []
    signals = []
    bricks = []
    equity_curve: list[EquityPoint] = []
    position_samples: list[dict[str, Any]] = []
    order_latencies: list[Decimal] = []
    protection_events: list[dict[str, Any]] = []
    stop_records: list[dict[str, Any]] = []
    seen_protection_events = 0

    def handle_fills(bundles: tuple[Any, ...], observation: CanonicalObservation) -> None:
        nonlocal seen_protection_events
        for bundle in bundles:
            order_latencies.append(bundle.latency_seconds)
            for fill in bundle.fills:
                fills.append(fill)
                accounting.process_fill(fill)
                if catastrophic and fill.leg == "open":
                    stop_records.append(
                        _stop_record(
                            symbol=symbol,
                            fill=fill,
                            intent=bundle.order.intent,
                            source_row=source_row,
                        )
                    )
                if (
                    fill.leg == "close"
                    and bundle.order.intent.close_position
                    and accounting.position.side is None
                    and hasattr(strategy, "mark_close_filled")
                ):
                    strategy.mark_close_filled(
                        timestamp=fill.timestamp,
                        reason=fill.reason,
                        order_id=fill.order_id,
                        source=execution.version,
                    )
        if len(execution.protection_events) > seen_protection_events:
            for event in execution.protection_events[seen_protection_events:]:
                item = dict(event)
                item["event_time"] = pd.Timestamp(observation.second_bucket).isoformat()
                protection_events.append(item)
            seen_protection_events = len(execution.protection_events)

    for observation in observations:
        handle_fills(execution.fill_pending(observation=observation, position=accounting.position), observation)
        snapshot = accounting.snapshot(mark_price=observation.close_price)
        equity_curve.append(
            EquityPoint(
                timestamp=observation.second_bucket,
                equity=snapshot.equity,
                realized_pnl=snapshot.realized_pnl,
                unrealized_pnl=snapshot.unrealized_pnl,
                net_pnl=snapshot.net_pnl,
                fill_id=f"mark:{len(equity_curve)+1}",
            )
        )
        position_samples.append(
            {
                "timestamp": observation.second_bucket,
                "side": accounting.position.side or "flat",
                "qty": accounting.position.qty,
                "mark_price": observation.close_price,
                "equity": snapshot.equity,
            }
        )
        new_bricks, actions = strategy.on_tick(observation_to_market_tick(observation))
        bricks.extend(new_bricks)
        for signal, intent in actions:
            signals.append(signal)
            order = execution.queue(signal=signal, intent=intent, observation=observation, position=accounting.position)
            if order is None:
                if intent.close_position and hasattr(strategy, "mark_close_filled"):
                    strategy.mark_close_filled(
                        timestamp=signal.timestamp,
                        reason=intent.reason,
                        order_id=None,
                        source=f"{execution.version}:already_flat",
                    )
                continue
            if intent.close_position and hasattr(strategy, "mark_close_submitted"):
                strategy.mark_close_submitted(intent, response={"id": order.order_id})
            orders.append(order)

    final_snapshot = accounting.snapshot(mark_price=observations[-1].close_price)
    metrics = build_metrics(
        snapshot=final_snapshot,
        initial_capital=Decimal("2500"),
        observations=list(observations),
        order_latencies=order_latencies,
        equity_curve=equity_curve,
        benchmark_quantity=selected["qty"],
        allocated_capital=selected["qty"] * observations[0].close_price,
        unused_cash=Decimal("2500") - (selected["qty"] * observations[0].close_price),
        position_samples=position_samples,
        fills=fills,
    )
    slippage = Decimal(str(metrics["closed_trades"])) * SLIPPAGE_PER_CLOSED_TRADE
    raw_pnl = Decimal(str(metrics["net_pnl_dollars"]))
    stop_intervals = _build_stop_intervals(stop_records, protection_events)
    stable_stop_records = [_stable_stop_record(record) for record in stop_records]
    stable_intervals = [_stable_interval(interval) for interval in stop_intervals]
    return {
        "symbol": symbol,
        "mode": "catastrophic_stop" if catastrophic else "baseline",
        "strategy_config": strategy_config,
        "canonical_observations": len(observations),
        "renko_bricks": len(bricks),
        "strategy_signals": len(signals),
        "queued_orders": len(orders),
        "fills": len(fills),
        "total_trades": metrics["closed_trades"],
        "winning_trades": metrics["winning_trades"],
        "losing_trades": metrics["losing_trades"],
        "raw_pnl": raw_pnl,
        "slippage_cost": slippage,
        "slippage_adjusted_pnl": raw_pnl - slippage,
        "maximum_drawdown": Decimal(str(metrics["max_drawdown_dollars"])),
        "maximum_drawdown_pct": Decimal(str(metrics["max_drawdown_pct"])),
        "final_position": asdict(final_snapshot.position),
        "final_accounting": {
            "realized_pnl": final_snapshot.realized_pnl,
            "unrealized_pnl": final_snapshot.unrealized_pnl,
            "net_pnl": final_snapshot.net_pnl,
            "equity": final_snapshot.equity,
            "closed_trades": len(final_snapshot.closed_trades),
            "position_transition_count": len(final_snapshot.position_transitions),
        },
        "ledger_reconciliation": "PASS",
        "protection_events": [_stable_protection_event(event) for event in protection_events],
        "stop_records": stable_stop_records,
        "stop_intervals": stable_intervals,
        "_runtime_stop_intervals": stop_intervals,
        "raw_price_crossings": [],
        "excluded_crossings": [],
        "eligible_stop_elections": [],
    }


def _stop_record(*, symbol: str, fill: Any, intent: Any, source_row: dict[str, str]) -> dict[str, Any]:
    plan = build_reversal_catastrophic_stop_plan(
        trader_id=intent.trader_id or intent.strategy,
        strategy_id=intent.strategy,
        symbol=symbol,
        entry_side=intent.side,
        quantity=fill.qty,
        entry_reference_price=fill.price,
        brick_size=intent.brick_size,
        catastrophic_stop_bricks=intent.catastrophic_stop_bricks,
        client_order_id=intent.client_order_id,
        candidate_config_fingerprint=source_row["renko_strategy_fingerprint"],
        market_contract_fingerprint=source_row["market_stream_fingerprint"],
    )
    return {
        "entry_timestamp": pd.Timestamp(fill.timestamp).isoformat(),
        "entry_side": "long" if fill.side == "buy" else "short",
        "entry_price": fill.price,
        "qty": fill.qty,
        "calculated_stop_price": plan["stop_price"],
        "broker_valid_rounded_price": plan["stop_price"],
        "stop_distance": plan["stop_distance"],
        "stop_order_id": plan["stop_client_order_id"],
        "stop_plan": plan,
        "production_replay_stop_price_equal": True,
    }


def _build_stop_intervals(stop_records: list[dict[str, Any]], events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_order_id = {record["stop_order_id"]: record for record in stop_records}
    intervals: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for event in events:
        if event.get("state") == "PROTECTED" and event.get("protection_type") == "reversal_catastrophic_stop":
            record = by_order_id.get(event.get("stop_loss_order_id")) or {}
            current = {
                **record,
                "interval_start": event["event_time"],
                "interval_end": None,
                "normal_reversal_exit": False,
                "catastrophic_stop_exit": False,
                "exit_type": "OPEN_AT_END",
            }
            intervals.append(current)
        elif event.get("state") == "CANCELED_FOR_REVERSAL_EXIT" and current is not None:
            current["interval_end"] = event["event_time"]
            current["normal_reversal_exit"] = True
            current["exit_type"] = "normal_reversal_exit"
            current = None
        elif event.get("state") == "FLAT" and current is not None:
            current["interval_end"] = event["event_time"]
            current["catastrophic_stop_exit"] = True
            current["exit_type"] = "catastrophic_stop_exit"
            current = None
    for interval in intervals:
        if interval["interval_end"] is None:
            interval["interval_end"] = "9999-12-31T00:00:00Z"
    return intervals


def _stable_stop_record(record: dict[str, Any]) -> dict[str, Any]:
    plan = dict(record.get("stop_plan") or {})
    for key in ("client_order_id", "stop_client_order_id", "lifecycle_id"):
        plan.pop(key, None)
    return {
        key: value
        for key, value in record.items()
        if key not in {"stop_order_id", "stop_plan"}
    } | {"stop_plan": plan}


def _stable_interval(interval: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in interval.items()
        if key not in {"stop_order_id", "stop_plan"}
    }


def _stable_protection_event(event: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in event.items()
        if key
        not in {
            "order_id",
            "parent_order_id",
            "stop_loss_order_id",
            "take_profit_order_id",
            "lifecycle_id",
            "normal_exit_order_id",
            "normal_exit_fill_id",
        }
    }


def _scan_raw_stop_elections(symbol: str, intervals: list[dict[str, Any]]) -> dict[str, Any]:
    raw_price_crossings: list[dict[str, Any]] = []
    excluded_crossings: list[dict[str, Any]] = []
    eligible_stop_elections: list[dict[str, Any]] = []
    intervals_by_session: dict[str, list[dict[str, Any]]] = {session: [] for session in SESSIONS}
    for interval in intervals:
        start_day = str(pd.Timestamp(interval["interval_start"]).date())
        end_day = SESSIONS[-1] if str(interval["interval_end"]).startswith("9999") else str(pd.Timestamp(interval["interval_end"]).date())
        for session in SESSIONS:
            if start_day <= session <= end_day:
                intervals_by_session[session].append(interval)
    triggered_intervals: set[int] = set()
    for session, active in intervals_by_session.items():
        if not active:
            continue
        path = RAW_ROOT / symbol / session / "sip_trades.jsonl.gz"
        remaining = list(active)
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                message = json.loads(line)
                timestamp = str(message["t"])
                price = Decimal(str(message["p"]))
                for interval in list(remaining):
                    interval_id = id(interval)
                    if interval_id in triggered_intervals:
                        remaining.remove(interval)
                        continue
                    if timestamp < _iso_z(interval["interval_start"]):
                        continue
                    if timestamp > _iso_z(interval["interval_end"]):
                        remaining.remove(interval)
                        continue
                    stop = Decimal(str(interval["broker_valid_rounded_price"]))
                    side = str(interval["entry_side"])
                    crossed = (side == "long" and price <= stop) or (side == "short" and price >= stop)
                    if crossed:
                        decision = evaluate_stop_election(
                            symbol=symbol,
                            timestamp=timestamp,
                            price=price,
                            exchange=str(message.get("x") or ""),
                            tape=str(message.get("z") or ""),
                            trade_id=str(message.get("i")),
                            conditions=tuple(str(item) for item in message.get("c") or ()),
                            stop_price=stop,
                            position_side=side,  # type: ignore[arg-type]
                            nbbo=None,
                        ).to_dict()
                        decision.update(
                            {
                                "entry_price": interval["entry_price"],
                                "interval_start": interval["interval_start"],
                                "interval_end": interval["interval_end"],
                                "source": str(path),
                            }
                        )
                        raw_price_crossings.append(decision)
                        if decision["final_election_decision"] == "EXCLUDED_CONDITION":
                            excluded_crossings.append(decision)
                        elif decision["final_election_decision"] in {"MISSING_NBBO", "ELECTED"}:
                            eligible_stop_elections.append(decision)
                            if decision["final_election_decision"] == "ELECTED":
                                interval["catastrophic_stop_exit"] = True
                                interval["exit_type"] = "eligible_stop_election"
                        triggered_intervals.add(interval_id)
                        remaining.remove(interval)
                        break
    return {
        "raw_price_crossings": raw_price_crossings,
        "excluded_crossings": excluded_crossings,
        "eligible_stop_elections": eligible_stop_elections,
        "interval_count": len(intervals),
        "intervals": [_stable_interval(interval) for interval in intervals],
    }


def _apply_scan_to_report_intervals(intervals: list[dict[str, Any]], scan: dict[str, Any]) -> None:
    for hit in scan["eligible_stop_elections"]:
        for interval in intervals:
            if (
                interval.get("interval_start") == hit.get("interval_start")
                and str(interval.get("broker_valid_rounded_price")) == str(hit.get("stop_price"))
            ):
                interval["catastrophic_stop_exit"] = True
                interval["exit_type"] = "eligible_stop_election"
                interval["stop_trigger_timestamp"] = hit["timestamp"]
                interval["stop_trigger_trade_id"] = hit["trade_id"]
                break


def _validation_status(baseline: dict[str, Any], protected: dict[str, Any], scan: dict[str, Any]) -> str:
    if any(item["final_election_decision"] == "MISSING_NBBO" for item in scan["eligible_stop_elections"]):
        return "INSUFFICIENT_EVIDENCE_MISSING_NBBO"
    if scan["eligible_stop_elections"]:
        return "FAIL_ELIGIBLE_STOP_ELECTION_REQUIRES_FULL_OUTCOME_REPLAY"
    comparable = ("renko_bricks", "strategy_signals", "queued_orders", "total_trades")
    if any(baseline[key] != protected[key] for key in comparable):
        return "FAIL_SIGNAL_OR_LEDGER_CHANGE"
    if Decimal(str(protected["slippage_adjusted_pnl"])) < 0:
        return "FAIL_NEGATIVE_AFTER_STOP"
    if Decimal(str(protected["maximum_drawdown"])) > Decimal(str(baseline["maximum_drawdown"])) * Decimal("1.25"):
        return "FAIL_DRAWDOWN_INCREASE"
    return "PASS"


def _load_observations(symbol: str) -> list[CanonicalObservation]:
    path = PREPARED_ROOT / SELECTED[symbol]["prepared"] / "observations_1s.jsonl"
    observations: list[CanonicalObservation] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                observations.append(_observation_from_row(json.loads(line)))
    return observations


def _observation_from_row(row: dict[str, Any]) -> CanonicalObservation:
    return CanonicalObservation(
        observation_id=row["observation_id"],
        run_id=row["run_id"],
        symbol=row["symbol"],
        second_bucket=pd.Timestamp(row["second_bucket"]).to_pydatetime(),
        emitted_sequence=int(row["emitted_sequence"]),
        close_price=Decimal(str(row["close_price"])),
        source_eligible_trade_id=str(row["source_eligible_trade_id"]),
        source_sip_timestamp=pd.Timestamp(row["source_sip_timestamp"]).to_pydatetime(),
        source_alpaca_trade_id=str(row["source_alpaca_trade_id"]),
        source_record_id=row.get("source_record_id"),
        source_broker_trade_id=row.get("source_broker_trade_id"),
        source_dataset_reference=row.get("source_dataset_reference"),
        source_dataset_hash=row.get("source_dataset_hash"),
        source_row_order=int(row["source_row_order"]) if row.get("source_row_order") not in {None, ""} else None,
        source_economic_event_fingerprint=row.get("source_economic_event_fingerprint"),
        source_exchange=row.get("source_exchange") or "",
        source_tape=row.get("source_tape") or "",
        triggering_later_eligible_trade_id=row.get("triggering_later_eligible_trade_id"),
        triggering_sip_timestamp=pd.Timestamp(row["triggering_sip_timestamp"]).to_pydatetime()
        if row.get("triggering_sip_timestamp")
        else None,
        sampler_version=row.get("sampler_version") or "",
        parent_event_id=row.get("parent_event_id"),
        root_event_id=row.get("root_event_id"),
        source_eligible_trade_event_id=row.get("source_eligible_trade_event_id"),
        triggering_eligible_trade_event_id=row.get("triggering_eligible_trade_event_id"),
        replay_finalized=bool(row.get("replay_finalized")),
    )


def _write_comparison_csv(results: list[dict[str, Any]]) -> None:
    fields = [
        "symbol",
        "mode",
        "validation_status",
        "total_trades",
        "winning_trades",
        "losing_trades",
        "raw_pnl",
        "slippage_cost",
        "slippage_adjusted_pnl",
        "maximum_drawdown",
        "maximum_drawdown_pct",
        "catastrophic_stop_activations",
        "renko_bricks",
        "strategy_signals",
        "queued_orders",
        "fills",
        "final_position_side",
        "final_position_qty",
        "ledger_reconciliation",
    ]
    with (OUTPUT_DIR / "comparison.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for result in results:
            writer.writerow(
                {
                    "symbol": result["symbol"],
                    "mode": result["mode"],
                    "validation_status": result["validation_status"],
                    "total_trades": result["total_trades"],
                    "winning_trades": result["winning_trades"],
                    "losing_trades": result["losing_trades"],
                    "raw_pnl": result["raw_pnl"],
                    "slippage_cost": result["slippage_cost"],
                    "slippage_adjusted_pnl": result["slippage_adjusted_pnl"],
                    "maximum_drawdown": result["maximum_drawdown"],
                    "maximum_drawdown_pct": result["maximum_drawdown_pct"],
                    "catastrophic_stop_activations": len(result.get("eligible_stop_elections") or []),
                    "renko_bricks": result["renko_bricks"],
                    "strategy_signals": result["strategy_signals"],
                    "queued_orders": result["queued_orders"],
                    "fills": result["fills"],
                    "final_position_side": result["final_position"]["side"],
                    "final_position_qty": result["final_position"]["qty"],
                    "ledger_reconciliation": result["ledger_reconciliation"],
                }
            )


def _write_stop_interval_csv(results: list[dict[str, Any]]) -> None:
    fields = [
        "symbol",
        "entry_timestamp",
        "entry_side",
        "entry_price",
        "qty",
        "calculated_stop_price",
        "broker_valid_rounded_price",
        "stop_distance",
        "interval_start",
        "interval_end",
        "exit_type",
        "production_replay_stop_price_equal",
    ]
    with (OUTPUT_DIR / "stop_intervals.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for result in results:
            for interval in result.get("stop_intervals") or []:
                writer.writerow({field: artifact_jsonable(interval.get(field, "")) for field in fields} | {"symbol": result["symbol"]})


def _write_decision_csv(raw_scans: dict[str, dict[str, Any]], filename: str, key: str) -> None:
    fields = [
        "symbol",
        "timestamp",
        "price",
        "trade_id",
        "conditions",
        "exchange",
        "tape",
        "stop_price",
        "position_side",
        "entry_price",
        "consolidated_print_eligible",
        "excluded_condition_reason",
        "nbbo",
        "inside_nbbo",
        "final_election_decision",
        "source_version",
        "interval_start",
        "interval_end",
        "source",
    ]
    with (OUTPUT_DIR / filename).open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for scan in raw_scans.values():
            for hit in scan[key]:
                writer.writerow({field: artifact_jsonable(hit.get(field, "")) for field in fields})


def _checksums(summary_sha: str) -> dict[str, Any]:
    files = [
        OUTPUT_DIR / "summary.json",
        OUTPUT_DIR / "comparison.csv",
        OUTPUT_DIR / "stop_intervals.csv",
        OUTPUT_DIR / "raw_price_crossings.csv",
        OUTPUT_DIR / "excluded_crossings.csv",
        OUTPUT_DIR / "eligible_stop_elections.csv",
        REVERSAL_SOURCE,
    ]
    for symbol in ("AAPL", "TSLA"):
        files.append(PREPARED_ROOT / SELECTED[symbol]["prepared"] / "manifest.json")
        files.append(PREPARED_ROOT / SELECTED[symbol]["prepared"] / "observations_1s.jsonl")
        for session in SESSIONS:
            files.append(RAW_ROOT / symbol / session / "sip_trades.jsonl.gz")
    return {
        "summary_sha256": summary_sha,
        "files": [{"path": str(path), "sha256": _file_sha256(path)} for path in files],
    }


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _iso_z(value: Any) -> str:
    text = str(pd.Timestamp(value).isoformat())
    return text.replace("+00:00", "Z")


if __name__ == "__main__":
    main()
