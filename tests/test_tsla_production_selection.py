from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import FrozenInstanceError, asdict, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest import mock

import pandas as pd

from exit_plan import build_renko_tp_sl_exit_plan
from optimization.models import OptimizationConfig, TraderSelection
from optimization.runner import _execute_candidate, run_candidate, run_optimization
from optimization.tsla_production_selection import (
    EXIT_PAIRS, FrozenCandidate, RegimePoint, ResearchPartition, ResearchTemporaryFiles,
    SelectionRules, SourceEvidence, _prepared, approved_universe_config, canonical_json,
    concentration_metrics, distinct_regimes, evaluate_holdout, fingerprint, freeze_finalists as _freeze_finalists,
    generate_challengers, neighborhood, prepare_validated_dataset, source_fingerprint, validate_session,
    select_finalists,
    evaluate_training,
    CALIBRATION_POLICY, calibrate_live_fills, calibrated_friction, slippage_distribution,
    validate_calibration_partition, calibrate_broker_export, published_fee_fallback,
)
from optimization.market_calendar import nyse_session_window
from replay.execution import (ExecutionFriction, NextObservationExecution, ResearchFeeLedger,
                              RawSipExecution, RawExecutionAssumptions, raw_trade_groups, EquityRegulatoryFees,
                              InvalidProtectionGeometry, SIDE_FAMILY_BACKTEST_PROFILE)
from replay.metrics import build_metrics
from silver.accounting import AccountingLedger
from silver.accounting.models import EquityPoint, NormalizedFill, PositionState
from silver.market import CanonicalObservation, RawSipTrade
from strategy_engine import RenkoStrategyConfig, RenkoStrategyEngine
from trading_events import OrderIntent, Signal


D = Decimal
TRAIN = date(2026, 9, 21)
HOLDOUT = date(2026, 9, 22)


def synthetic_training_results(candidates):
    return {c.identity: {"candidate_identity": c.identity, "candidate_status": "COMPLETED",
                         "source_combined_hash": c.training_identity} for c in candidates}


def freeze_finalists(candidates, partition, rules, evidence):
    # Older holdout fixtures model completed training without running a sweep.
    return _freeze_finalists(candidates, partition, rules, evidence,
                             training_results=synthetic_training_results(candidates))


def source(day=TRAIN, prices=(100, 101, 100, 99, 98, 97, 98, 99, 100, 101, 100, 99, 98, 97, 96), seconds=None):
    start = nyse_session_window(day).start_utc + timedelta(minutes=1)
    return tuple(RawSipTrade(start + timedelta(seconds=offset), "TSLA", price, 1,
                             trade_id=f"{day}:{i}", conditions=("@",), source_sequence=i, row_order=i)
                 for i, (offset, price) in enumerate(zip(seconds or range(len(prices)), prices)))


def evidence(rows, day=TRAIN, **kwargs):
    window = nyse_session_window(day)
    base = SourceEvidence("synthetic:complete", source_fingerprint(rows), len(rows), window.start_utc,
                          window.end_utc, True, "synthetic_fixture")
    return replace(base, **kwargs)


def dataset(day=TRAIN, **kwargs):
    rows = source(day, **kwargs)
    return prepare_validated_dataset((day,), {day: (rows, evidence(rows, day), None)})


def trader(**kwargs):
    config = RenkoStrategyConfig(symbol="TSLA", brick_size=1, entry_bricks=1,
                                strategy_type="renko_tp_sl", take_profit_bricks=1, stop_loss_bricks=1,
                                trader_id="synthetic_tsla", strategy_name="synthetic_tsla", qty=1,
                                renko_anchor_mode="grid", renko_anchor_price=0,
                                entry_start="09:31:00", entry_end="15:59:00", force_flat="15:59:30")
    config = replace(config, **kwargs)
    return TraderSelection("synthetic_tsla", "TSLA", config.strategy_name, config.strategy_type,
                           asdict(config), {}, "synthetic-config")


def config(**kwargs):
    return replace(OptimizationConfig("integrity", "unused.json", (TRAIN,)), **kwargs)


def observation(price="100", index=0):
    timestamp = nyse_session_window(TRAIN).start_utc + timedelta(minutes=1, seconds=index)
    return CanonicalObservation(str(index), "synthetic", "TSLA", timestamp, index, D(price),
                                f"eligible:{index}", timestamp, f"trade:{index}")


def entry(side="buy", *, bracket=False):
    obs = observation()
    signal = Signal("signal", "entry", pd.Timestamp(obs.second_bucket), "TSLA",
                    "long" if side == "buy" else "short", side, 100, "entry", "synthetic", "synthetic_tsla")
    plan = build_renko_tp_sl_exit_plan(
        trader_id="synthetic_tsla", strategy_id="synthetic", symbol="TSLA", entry_side=side,
        quantity=1, entry_reference_type="signal_price", entry_reference_price=100, brick_size=1,
        take_profit_bricks=2, stop_loss_bricks=1, time_in_force="day", order_class="bracket",
        client_order_id="entry", candidate_config_fingerprint="config", market_contract_fingerprint="market",
    ).to_dict() if bracket else None
    intent = OrderIntent("entry", signal.timestamp, "TSLA", side, "entry", "market", "synthetic", "entry", qty=1,
                         exit_plan=plan, exit_management="bracket" if bracket else "strategy")
    return signal, intent


class EmpiricalCalibrationTests(unittest.TestCase):
    def sample(self, side="buy", price="101", **changes):
        row = dict(account="test-live", activity_id="fill", order_id="order", symbol="TSLA", side=side,
                   fill_price=price, reference_price="100", qty="1", origin="live",
                   timestamp="2026-09-08T14:00:01Z", reference_timestamp="2026-09-08T14:00:00Z",
                   provenance={"broker-export": "fixture-hash"}, reference_policy=CALIBRATION_POLICY)
        return {**row, **changes}

    def test_all_four_legs_signed_adverse_and_favorable_floor(self):
        for leg, side, adverse, favorable in (("long_entry", "buy", "101", "99"),
                ("short_exit", "buy", "101", "99"), ("short_entry", "sell", "99", "101"),
                ("long_exit", "sell", "99", "101")):
            result = calibrate_live_fills([self.sample(side, adverse, leg=leg),
                                          self.sample(side, favorable, leg=leg, activity_id="favorable")])
            self.assertEqual(result["signed"]["mean"], "0")
            self.assertEqual(D(result["base"]), D(".5"))
            self.assertEqual(D(result["stress"]), D(1))
            self.assertEqual([D(row["adverse_component"]) for row in result["observations"]], [D(0), D(1)])

    def test_mean_quantiles_order_dedup_and_decimal_context(self):
        from decimal import localcontext
        rows = [self.sample(price=str(100 + value), activity_id=str(value), qty=str(value + 1)) for value in range(4)]
        first = calibrate_live_fills(rows)
        self.assertEqual(first, calibrate_live_fills(list(reversed(rows))))
        with localcontext() as context:
            context.prec = 8
            self.assertEqual(first, calibrate_live_fills(rows))
        self.assertEqual(D(first["base"]), D("1.5"))
        self.assertEqual(D(first["adverse"]["median"]), D("1.5"))
        self.assertEqual(D(first["adverse"]["p75"]), D("2.25"))
        self.assertEqual(D(first["adverse"]["p90"]), D("2.7"))
        duplicate = calibrate_live_fills(rows + rows)
        self.assertEqual(duplicate["duplicates"], 4)
        self.assertEqual(duplicate["base"], first["base"])
        with self.assertRaisesRegex(ValueError, "Conflicting duplicate"):
            calibrate_live_fills([rows[0], {**rows[0], "fill_price": "101"}])

    def test_empty_invalid_provenance_and_future_reference_fail(self):
        with self.assertRaises(ValueError):
            calibrate_live_fills([])
        for changes in ({"symbol": "QQQ"}, {"origin": "paper"}, {"side": "unknown"},
                        {"qty": "0"}, {"fill_price": "NaN"}, {"reference_price": "-1"}, {"fill_price": None},
                        {"timestamp": "2026-09-08T14:00:01"},
                        {"provenance": {}}, {"reference_policy": "picked_best"},
                        {"reference_timestamp": "2026-09-08T14:00:02Z"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                calibrate_live_fills([self.sample(**changes)])
        with self.assertRaisesRegex(ValueError, "holdout"):
            calibrate_live_fills([self.sample()], holdout_sessions=(date(2026, 9, 8),))

    def test_frozen_costs_tampering_zero_base_and_holdout_dates(self):
        calibration = calibrate_live_fills([self.sample()])
        friction = calibrated_friction(calibration, fee_per_fill=".02")
        self.assertEqual(friction.stress().fee_per_fill, friction.fee_per_fill)
        self.assertEqual(friction.calibration_evidence_hash, calibration["identity"])
        validate_calibration_partition(friction, ResearchPartition((TRAIN,), (HOLDOUT,)))
        with self.assertRaisesRegex(ValueError, "holdout"):
            validate_calibration_partition(replace(friction, calibration_sessions=(str(HOLDOUT),)), ResearchPartition((TRAIN,), (HOLDOUT,)))
        with self.assertRaisesRegex(ValueError, "modified"):
            calibrated_friction({**calibration, "base": "9"})
        with self.assertRaisesRegex(ValueError, "positive BASE"):
            calibrated_friction(calibrate_live_fills([self.sample(price="99")]))

    def test_stress_cannot_generate_universe_and_holdout_cannot_recalibrate(self):
        calibration = calibrate_live_fills([self.sample()])
        friction = calibrated_friction(calibration, fee_per_fill=".02")
        cfg = config(execution_friction=friction)
        partition = ResearchPartition((TRAIN,), (HOLDOUT,))
        training = dataset()
        candidates = generate_challengers(cfg, trader(exit_management="bracket", candidate_config_fingerprint="config",
                                                      market_contract_fingerprint="market"), training, partition)
        candidate = candidates[0]
        initial_identity = candidate.identity
        finalists = freeze_finalists((candidate,), partition, SelectionRules(), {"fixture": True})
        changed = replace(cfg, execution_friction=ExecutionFriction(slippage_per_share=99, fee_per_fill=99))
        for stress in (False, True):
            with mock.patch("optimization.tsla_production_selection.run_candidate") as run:
                evaluate_holdout(finalists, dataset(HOLDOUT), changed, stress=stress)
                actual = run.call_args.kwargs["config"].execution_friction
                self.assertEqual(actual, friction.stress() if stress else friction)
                self.assertEqual(actual.fee_per_fill, D(".02"))
        self.assertEqual(candidate.identity, initial_identity)
        with self.assertRaisesRegex(ValueError, "STRESS"):
            generate_challengers(replace(cfg, execution_friction=friction.stress()), trader(), training, partition)
        with self.assertRaisesRegex(ValueError, "holdout"):
            generate_challengers(replace(cfg, execution_friction=replace(friction, calibration_sessions=(str(HOLDOUT),))), trader(), training, partition)

    def test_certification_no_longer_requires_exact_broker_parity_but_discloses_fees(self):
        from scripts.run_tsla_production_selection import certification_gates
        calibration = calibrate_live_fills([self.sample()])
        friction = calibrated_friction(calibration)
        gates, blockers = certification_gates(True, calibration, friction)
        self.assertTrue(all("broker" not in blocker.lower() for blocker in blockers))
        self.assertTrue(any("fees" in blocker.lower() for blocker in blockers))
        self.assertEqual(dict((area, status) for area, status, _ in gates)["Conservative replay execution"], "PASS")

    def test_fixed_charge_rejects_mixed_or_inconsistent_assumptions(self):
        for kwargs in ({"slippage_per_share": 0}, {"slippage_per_share": ".1", "slippage_bps": 1},
                       {"slippage_per_share": ".1", "stress_slippage_per_share": ".3"},
                       {"slippage_per_share": ".15", "base_slippage_per_share": ".1"}):
            with self.assertRaises(ValueError):
                ExecutionFriction(application="PER_FILL_CHARGE_V1", **kwargs)

    def test_broker_export_normalization_exclusions_and_reference_selection(self):
        import gzip
        from hashlib import sha256
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            day = root / "2026-09-08"
            day.mkdir()
            raw = [dict(S="TSLA", t=f"2026-09-08T13:31:0{second}Z", p=price, s=100, x="V", z="C", c=["@"], i=second)
                   for second, price in ((0, 100), (1, 99), (2, 98))]
            path = day / "sip_trades.jsonl.gz"
            path.write_bytes(gzip.compress("".join(json.dumps(row) + "\n" for row in raw).encode()))
            manifest = dict(sha256=sha256(path.read_bytes()).hexdigest(), symbol="TSLA", feed="sip", raw_record_count=3,
                            pagination={"pages": [{"records": 3, "next_page_token_present": False}]},
                            request_parameters={"start": "2026-09-08T13:30:00Z", "end": "2026-09-08T20:00:00Z"})
            (day / "sip_trades.jsonl.manifest.json").write_text(json.dumps(manifest))
            fill = dict(id="fill", order_id="order", activity_type="FILL", symbol="TSLA", side="sell_short",
                        qty="1", price="97.9", transaction_time="2026-09-08T13:31:02.1Z")
            order = dict(id="order", client_order_id="client", symbol="TSLA", side="sell", source="access_key",
                         type="market", filled_avg_price="97.9", filled_qty="1")
            broker = {"accounts": {"live": {"base_url": "https://api.alpaca.markets", "recent_fills": [fill, fill], "recent_orders": [order]}}}
            event = {"event_type": "execution", "payload": {"mode": "real", "intent": {"symbol": "TSLA", "client_order_id": "client", "timestamp": "2026-09-08T13:31:00Z"}}}
            broker_path, events_path = root / "broker.json", root / "events.json"
            broker_path.write_text(json.dumps(broker))
            events_path.write_text(json.dumps({"events": {"live": {"events": [event]}}}))
            result = calibrate_broker_export(broker_path, events_path, root)
            self.assertEqual(result["found"], 2)
            self.assertEqual(result["duplicates"], 1)
            row = result["calibration"]["observations"][0]
            self.assertEqual(row["reference_price"], "98")
            self.assertEqual(row["broker_activity_side"], "sell_short")
            self.assertEqual(D(result["calibration"]["base"]), D(".1"))
            self.assertEqual(result, calibrate_broker_export(broker_path, events_path, root))
            manifest["raw_record_count"] = 4
            (day / "sip_trades.jsonl.manifest.json").write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, "count"):
                calibrate_broker_export(broker_path, events_path, root)


class RegulatoryFeeTests(unittest.TestCase):
    def friction(self):
        sample = EmpiricalCalibrationTests().sample(price="100.03495")
        return published_fee_fallback(calibrated_friction(calibrate_live_fills([sample])))

    def fill(self, side="sell", qty="1", price="350", **changes):
        values = dict(timestamp="2026-09-10T14:00:00Z", symbol="TSLA", trader_id="test", source="test",
                      side=side, qty=qty, price=price, leg="open", fill_id="fill", order_id="order")
        return NormalizedFill(**{**values, **changes})

    def test_exact_components_buy_sell_short_and_fractional_quantities(self):
        schedule = EquityRegulatoryFees()
        for side in ("buy", "sell", "sell_short"):
            with self.subTest(side=side):
                parts = schedule.components(self.fill(side, qty="0.123456789", price="350.1234"))
                selling = side != "buy"
                self.assertEqual(parts["REG"], D(".123456789") * D("350.1234") * D(".0000206") if selling else 0)
                self.assertEqual(parts["TAF"], D(".123456789") * D(".000195") if selling else 0)
                self.assertEqual(parts["CAT"], D(".123456789") * D(".000003"))

    def test_extended_fee_window_boundaries_and_all_25_experiment_dates(self):
        schedule = EquityRegulatoryFees()
        self.assertEqual((schedule.valid_from, schedule.valid_through), ("2026-08-07", "2026-09-25"))
        partition = ResearchPartition.completed(datetime(2026, 9, 26, tzinfo=timezone.utc))
        self.assertEqual((len(partition.optimization), len(partition.holdout)), (20, 5))
        self.assertEqual((partition.optimization[0], partition.optimization[-1]), (date(2026, 8, 21), date(2026, 9, 18)))
        self.assertEqual(partition.holdout, tuple(date(2026, 9, day) for day in range(21, 26)))
        # Date-only synthetic fills: no market data or holdout performance is opened.
        days = {*map(str, partition.optimization + partition.holdout), "2026-08-07", "2026-09-12", "2026-09-13"}
        for day in sorted(days):
            for side in ("buy", "sell", "sell_short"):
                with self.subTest(day=day, side=side):
                    fill = self.fill(side, qty="0.123456789", price="350.1234", timestamp=f"{day}T14:00:00Z")
                    self.assertEqual(schedule.components(fill), schedule.components(replace(fill, timestamp="2026-09-10T14:00:00Z")))
        schedule.components(self.fill(timestamp="2026-09-26T03:59:59Z"))
        for timestamp in ("2026-08-06T14:00:00Z", "2026-09-26T04:00:00Z", "2026-09-26T14:00:00Z"):
            with self.subTest(timestamp=timestamp), self.assertRaisesRegex(ValueError, "outside approved"):
                schedule.components(self.fill(timestamp=timestamp))

    def test_fee_window_changes_identity_without_changing_other_assumptions(self):
        friction = self.friction()
        old = replace(friction, regulatory_fees=replace(friction.regulatory_fees, valid_through="2026-09-12"))
        current_payload = asdict(friction)
        old_payload = asdict(old)
        self.assertNotEqual(fingerprint(current_payload), fingerprint(old_payload))
        restored = ExecutionFriction(**json.loads(canonical_json(current_payload)))
        self.assertEqual(fingerprint(asdict(restored)), fingerprint(current_payload))
        old_payload["regulatory_fees"]["valid_through"] = "2026-09-25"
        self.assertEqual(old_payload, current_payload)
        self.assertEqual(friction.slippage_per_share, D(".03495"))
        self.assertEqual(friction.stress().slippage_per_share, D(".06990"))
        self.assertEqual(friction.regulatory_fees, friction.stress().regulatory_fees)

    def test_daily_rounding_charges_increments_not_each_fill(self):
        fees = ResearchFeeLedger(self.friction())
        for index in range(100):
            fees.record(self.fill("buy", fill_id=f"b{index}", order_id=f"b{index}"), PositionState("TSLA"))
            fees.record(self.fill("sell_short", fill_id=f"s{index}", order_id=f"s{index}"), PositionState("TSLA"))
            if index == 0:
                self.assertEqual(fees.total, D(".03"))
        self.assertEqual(fees.total, D(".76"))
        self.assertEqual(fees.regulatory_totals, {"REG": D(".73"), "TAF": D(".02"), "CAT": D(".01")})
        daily = fees.fee_evidence()["daily"][0]
        self.assertEqual(D(daily["unrounded"]["REG"]), D(".721"))
        self.assertEqual(sum(map(D, daily["rounded"].values())), fees.total)
        self.assertEqual(fees.friction_total, D(200) * D(".03495"))

    def test_taf_partial_fills_share_cap_even_across_dates(self):
        for same_order in (True, False):
            fees = ResearchFeeLedger(self.friction())
            for index, day in enumerate(("2026-09-10", "2026-09-11")):
                fees.record(self.fill("sell_short", qty="30000", fill_id=str(index),
                    order_id="shared" if same_order else str(index), timestamp=f"{day}T14:00:00Z"), PositionState("TSLA"))
            self.assertEqual(fees.regulatory_totals["TAF"], D("9.79") if same_order else D("11.70"))
        parts = EquityRegulatoryFees().components(self.fill(qty="60000"))
        self.assertEqual(parts["TAF"], D("9.79"))

    def test_fee_dates_are_new_york_and_daily_totals_reset(self):
        fees = ResearchFeeLedger(self.friction())
        for index, timestamp in enumerate(("2026-09-10T23:59:59Z", "2026-09-11T00:00:01Z", "2026-09-11T14:00:00Z")):
            fees.record(self.fill("buy", timestamp=timestamp, fill_id=str(index)), PositionState("TSLA"))
        daily = fees.fee_evidence()["daily"]
        self.assertEqual([row["session"] for row in daily], ["2026-09-10", "2026-09-11"])
        self.assertEqual(D(daily[0]["unrounded"]["CAT"]), D(".000006"))
        self.assertEqual(fees.total, D(".02"))

    def test_invalid_schedule_and_duplicate_conflicts_fail_without_charge(self):
        friction = self.friction()
        fees = ResearchFeeLedger(friction)
        fill = self.fill("buy")
        fees.record(fill, PositionState("TSLA"))
        original = (fees.total, fees.friction_total, fees.fee_evidence())
        fees.record(fill, PositionState("TSLA"))
        self.assertEqual((fees.total, fees.friction_total, fees.fee_evidence()), original)
        for bad in (replace(fill, price=D(351)), replace(fill, fill_id="other", side="sell"),
                    replace(fill, fill_id="outside", timestamp="2026-09-26T14:00:00Z"),
                    replace(fill, fill_id="symbol", symbol="QQQ"), replace(fill, fill_id="no-order", order_id=None)):
            with self.assertRaises(ValueError):
                fees.record(bad, PositionState("TSLA"))
            self.assertEqual((fees.total, fees.friction_total, fees.fee_evidence()), original)
        for kwargs in ({"fee_per_fill": ".01"}, {"fee_per_share": ".01"}, {"application": "PRICE_ADJUSTMENT"}):
            with self.assertRaises(ValueError):
                replace(friction, **kwargs)
        for kwargs in ({"taf_order_cap": "-1"}, {"reg_rate": "NaN"}, {"cat_per_share": "Infinity"},
                       {"aggregation": "per_fill_rounding"}, {"valid_from": "2026-09-26"}):
            with self.assertRaises(ValueError):
                EquityRegulatoryFees(**kwargs)

    def test_execution_profile_allows_same_order_sell_reversal_fee_family(self):
        friction = self.friction()
        ledger = AccountingLedger(symbol="TSLA", starting_capital=1000)
        entry = self.fill("buy", leg="open", fill_id="entry", order_id="entry")
        before = ledger.position
        ledger.process_fill(entry)
        ResearchFeeLedger(friction).record(entry, before)

        close = self.fill("sell", leg="close", fill_id="close", order_id="reversal")
        open_short = self.fill("sell_short", leg="open", fill_id="open", order_id="reversal")

        strict = ResearchFeeLedger(friction)
        before = ledger.position
        ledger.process_fill(close)
        strict.record(close, before)
        before = ledger.position
        ledger.process_fill(open_short)
        with self.assertRaisesRegex(ValueError, "Order side changed"):
            strict.record(open_short, before)

        ledger = AccountingLedger(symbol="TSLA", starting_capital=1000)
        profiled = ResearchFeeLedger(friction, execution_profile=SIDE_FAMILY_BACKTEST_PROFILE)
        for fill in (entry, close, open_short):
            before = ledger.position
            ledger.process_fill(fill)
            profiled.record(fill, before)
        self.assertEqual(profiled.order_sides[("standalone_tsla", "reversal")], "sell")
        self.assertEqual(profiled.fee_evidence()["execution_profile"]["regulatory_order_side_policy"], "SIDE_FAMILY")

    def test_long_short_wins_losses_single_count_accounting_and_stress(self):
        for short in (False, True):
            for profit in (-10, 0, 10):
                totals = []
                for friction in (self.friction(), self.friction().stress()):
                    ledger = AccountingLedger(symbol="TSLA", starting_capital=1000)
                    fees = ResearchFeeLedger(friction)
                    for leg, side, price in (("open", "sell_short" if short else "buy", D(350)),
                            ("close", "buy" if short else "sell", D(350) + profit * (-1 if short else 1))):
                        fill = self.fill(side, price=price, leg=leg, fill_id=leg, order_id=leg)
                        before = ledger.position
                        ledger.process_fill(fill)
                        fees.record(fill, before)
                        fees.record(fill, before)
                    snapshot = fees.snapshot(ledger.snapshot())
                    self.assertEqual(ledger.realized_pnl, D(profit))
                    self.assertEqual(fees.total, D(".03"))
                    self.assertEqual(fees.friction_total, 2 * friction.slippage_per_share)
                    self.assertEqual(snapshot.net_pnl, D(profit) - fees.total - fees.friction_total)
                    self.assertEqual(snapshot.closed_trades[0].pnl, snapshot.net_pnl)
                    self.assertEqual(fees.open_fees, 0)
                    self.assertEqual(fees.open_friction, 0)
                    totals.append(snapshot.net_pnl)
                self.assertEqual(totals[0] - totals[1], D(".06990"))

    def test_partial_close_and_empty_ledger_conserve_fees(self):
        ledger = AccountingLedger(symbol="TSLA", starting_capital=1000)
        fees = ResearchFeeLedger(self.friction())
        self.assertEqual(fees.total, 0)
        self.assertEqual(fees.fee_evidence()["daily"], [])
        for index, (leg, side, qty, price) in enumerate((("open", "buy", "2.5", "350"),
                ("close", "sell", "1.25", "351"), ("close", "sell", "1.25", "349"))):
            fill = self.fill(side, qty, price, leg=leg, fill_id=str(index), order_id=leg)
            before = ledger.position
            ledger.process_fill(fill)
            fees.record(fill, before)
        snapshot = fees.snapshot(ledger.snapshot())
        self.assertEqual(fees.total, D(".04"))
        self.assertEqual(fees.friction_total, D(5) * D(".03495"))
        self.assertEqual(sum(trade.pnl for trade in snapshot.closed_trades), snapshot.net_pnl)
        self.assertEqual(snapshot.net_pnl, -fees.total - fees.friction_total)
        self.assertEqual(fees.open_fees, 0)

    def test_frozen_fee_holdout_roundtrip_and_candidate_identity(self):
        training_day, holdout_day = date(2026, 9, 10), date(2026, 9, 11)
        friction = self.friction()
        self.assertEqual(ExecutionFriction(**json.loads(canonical_json(asdict(friction)))), friction)
        cfg = config(session_dates=(training_day,), execution_friction=friction)
        partition = ResearchPartition((training_day,), (holdout_day,))
        training = dataset(training_day)
        selected = trader(exit_management="bracket", candidate_config_fingerprint="config", market_contract_fingerprint="market")
        candidates = generate_challengers(cfg, selected, training, partition)
        candidate = candidates[0]
        frozen = freeze_finalists((candidate,), partition, SelectionRules(), {"synthetic": True})
        for stress in (False, True):
            with mock.patch("optimization.tsla_production_selection.run_candidate") as run:
                evaluate_holdout(frozen, dataset(holdout_day), replace(cfg, execution_friction=ExecutionFriction()), stress=stress)
                actual = run.call_args.kwargs["config"].execution_friction
                self.assertEqual(actual, friction.stress() if stress else friction)
        changed = replace(friction, regulatory_fees=replace(friction.regulatory_fees, taf_order_cap="10"))
        alternate = generate_challengers(replace(cfg, execution_friction=changed), selected, training, partition)[0]
        self.assertNotEqual(candidate.identity, alternate.identity)

    def test_approved_fallback_certifies_unknown_commission_but_rejects_changed_fees(self):
        from scripts.run_tsla_production_selection import certification_gates
        calibration = calibrate_live_fills([EmpiricalCalibrationTests().sample(price="100.03495")])
        friction = published_fee_fallback(calibrated_friction(calibration))
        gates, blockers = certification_gates(True, calibration, friction)
        self.assertEqual(blockers, [])
        self.assertEqual({area: status for area, status, _ in gates}["Regulatory fees"], "PASS")
        self.assertIn("UNVERIFIED", friction.regulatory_fees.commission_status)
        for changed in (replace(friction, regulatory_fees=None),
                        replace(friction, regulatory_fees=replace(friction.regulatory_fees, reg_rate=0))):
            self.assertTrue(certification_gates(True, calibration, changed)[1])
        self.assertTrue(certification_gates(False, calibration, friction)[1])

    def test_candidate_runner_regulatory_fees_repeat_and_frozen_holdout_stress(self):
        training_day, holdout_day = date(2026, 9, 10), date(2026, 9, 11)
        datasets = []
        for day in (training_day, holdout_day):
            rows = tuple(replace(item, tape="C") for item in source(day,
                prices=(100, 101, 101, 101, 99, 101, 101), seconds=(0, 1, 2, 2.1, 2.5, 2.9, 3)))
            datasets.append(prepare_validated_dataset((day,), {day: (rows, evidence(rows, day), None)}))
        training, holdout = datasets
        cfg = config(session_dates=(training_day,), execution_friction=self.friction())
        partition = ResearchPartition((training_day,), (holdout_day,))
        native = trader(exit_management="bracket", candidate_config_fingerprint="config", market_contract_fingerprint="market")
        candidate = next(item for item in generate_challengers(cfg, native, training, partition)
                         if item.spec().brick_pct == D("0.66") and item.spec().strategy_parameters.entry_bricks == 1
                         and item.spec().strategy_parameters.take_profit_bricks == 2 and item.spec().strategy_parameters.stop_loss_bricks == 1)
        first = evaluate_training(candidate, training, cfg)
        repeat = evaluate_training(candidate, training, cfg)
        self.assertEqual(first.status, "COMPLETED", first.error_message)
        self.assertEqual(first.row, repeat.row)
        self.assertEqual(first.row["closed_trades"], 1)
        self.assertEqual(first.row["fees_dollars"], .03)
        self.assertEqual(first.row["regulatory_fees_dollars"], .03)
        self.assertEqual(first.row["commission_dollars"], 0)
        self.assertEqual(first.row["pre_friction_gross_pnl_dollars"], -2)
        self.assertAlmostEqual(first.row["net_pnl_dollars"], -2.0999)
        finalists = freeze_finalists((candidate,), partition, SelectionRules(), {"synthetic": True})
        base = evaluate_holdout(finalists, holdout, cfg)[0]
        stress = evaluate_holdout(finalists, holdout, cfg, stress=True)[0]
        self.assertEqual(base.status, "COMPLETED", base.error_message)
        self.assertEqual(stress.status, "COMPLETED", stress.error_message)
        self.assertEqual(base.row["transaction_fee_evidence"], stress.row["transaction_fee_evidence"])
        self.assertEqual(base.row["fees_dollars"], stress.row["fees_dollars"])
        self.assertAlmostEqual(stress.row["net_pnl_dollars"], -2.1698)
        self.assertNotEqual(base.row["configuration_hash"], stress.row["configuration_hash"])

    def test_certification_loads_pinned_calibration_without_recalibrating(self):
        from scripts.run_tsla_production_selection import ROOT, load_frozen_calibration
        path = ROOT / "artifacts/research/tsla_empirical_friction_20260925_final/calibration.json"
        with mock.patch("optimization.tsla_production_selection.calibrate_live_fills", side_effect=AssertionError("recalibration")), \
             mock.patch("optimization.tsla_production_selection.calibrate_broker_export", side_effect=AssertionError("raw access")):
            audit, friction = load_frozen_calibration(path)
            self.assertEqual(friction.slippage_per_share, D(".03495"))
            self.assertEqual(friction.stress().slippage_per_share, D(".06990"))
            self.assertEqual(friction.calibration_evidence_hash, audit["calibration"]["identity"])
            with self.assertRaisesRegex(ValueError, "holdout"):
                load_frozen_calibration(path, ("2026-09-08",))
        with tempfile.TemporaryDirectory() as directory:
            modified = Path(directory) / "calibration.json"
            modified.write_bytes(path.read_bytes() + b" ")
            with self.assertRaisesRegex(ValueError, "checksum"):
                load_frozen_calibration(modified)


class RawExecutionParityTests(unittest.TestCase):
    def test_fixed_charge_all_paths_preserves_ideal_prices_and_stress_only_costs_more(self):
        base = ExecutionFriction(slippage_per_share=".1", application="PER_FILL_CHARGE_V1", fee_per_fill=".02")
        stress = base.stress()
        self.assertEqual(stress.slippage_per_share, base.slippage_per_share * 2)
        self.assertEqual(stress.fee(D(1)), base.fee(D(1)))
        self.assertEqual(stress.stress(), stress)
        for side, exit_mark, ideal_exit, reason, when in (
            ("buy", 103, 102, "TP", 1.5), ("sell", 97, 98, "TP", 1.5),
            ("buy", 97, 97, "SL", 1.5), ("sell", 104, 104, "SL", 1.5),
            ("buy", 100, 100, "FORCE_FLAT", 23310), ("sell", 100, 100, "FORCE_FLAT", 23310),
        ):
            results = []
            for friction in (base, stress):
                exe, acc, fees = self.setup_execution(side, friction)
                entered, _ = self.process(exe, acc, fees, 1, 100)
                self.assertEqual(entered[0].price, D(100))
                self.assertEqual(fees.snapshot(acc.snapshot(mark_price=D(100))).net_pnl, -friction.slippage_per_share - D(".02"))
                exited, _ = self.process(exe, acc, fees, when, exit_mark)
                self.assertEqual(exited[0].price, D(ideal_exit))
                proof = exe.trade_evidence(acc, fees)[0]
                self.assertEqual(proof["exit_reason"], reason)
                gross = D(ideal_exit - 100) * (1 if side == "buy" else -1)
                self.assertEqual(D(proof["pre_friction_gross_pnl"]), gross)
                self.assertEqual(D(proof["execution_friction"]), friction.slippage_per_share * 2)
                self.assertEqual(D(proof["fees"]), D(".04"))
                net = fees.snapshot(acc.snapshot()).net_pnl
                self.assertEqual(net, gross - friction.slippage_per_share * 2 - D(".04"))
                self.assertEqual(D(proof["net_pnl"]), net)
                results.append(net)
            self.assertEqual(results[0] - results[1], D(".2"))

    def setup_execution(self, side="buy", friction=None, **assumptions):
        cfg = trader(exit_management="bracket", candidate_config_fingerprint="config",
                     market_contract_fingerprint="market").strategy_config
        strategy = RenkoStrategyEngine(RenkoStrategyConfig(**cfg))
        execution = RawSipExecution(run_id="raw-test", trader_id="synthetic_tsla", strategy=strategy,
                                    friction=friction, assumptions=RawExecutionAssumptions(**assumptions))
        accounting = AccountingLedger(symbol="TSLA", starting_capital=D(1000))
        fees = ResearchFeeLedger(execution.friction)
        signal, intent = entry(side, bracket=True)
        obs = observation()
        obs = replace(obs, triggering_sip_timestamp=obs.second_bucket + timedelta(milliseconds=100))
        execution.queue(signal=signal, intent=intent, observation=obs, position=accounting.position)
        return execution, accounting, fees

    def process(self, execution, accounting, fees, seconds, low, high=None):
        timestamp = observation().second_bucket + timedelta(seconds=seconds)
        bundles, mark = execution.process_interval(timestamp=timestamp, low=low, high=low if high is None else high,
                                                   position=accounting.position)
        fills = []
        for bundle in bundles:
            for fill in bundle.fills:
                before = accounting.position
                accounting.process_fill(fill)
                fees.record(fill, before)
                fills.append(fill)
        return fills, mark

    def test_entry_uses_availability_not_bucket_time(self):
        exe, acc, fees = self.setup_execution()
        self.assertFalse(self.process(exe, acc, fees, .05, 100)[0])
        self.assertFalse(self.process(exe, acc, fees, .1, 100)[0])
        fills, _ = self.process(exe, acc, fees, .101, 100)
        self.assertEqual(len(fills), 1)
        self.assertEqual(fills[0].timestamp, observation().second_bucket + timedelta(milliseconds=101))

    def test_intrasecond_stop_closes_without_next_canonical_evaluation(self):
        exe, acc, fees = self.setup_execution()
        self.process(exe, acc, fees, 1, 100)
        fills, _ = self.process(exe, acc, fees, 1.5, 98)
        self.assertEqual(fills[0].reason, "stop_loss_broker_native")
        self.assertIsNone(acc.position.side)
        self.assertFalse(self.process(exe, acc, fees, 2, 100)[0])
        self.assertFalse(exe.active_protections)

    def test_long_and_short_unknown_order_both_hit_stop_first(self):
        for side, expected in (("buy", D(98)), ("sell", D(103))):
            with self.subTest(side=side):
                exe, acc, fees = self.setup_execution(side)
                self.process(exe, acc, fees, 1, 100)
                fills, _ = self.process(exe, acc, fees, 1.5, 98, 103)
                self.assertEqual(fills[0].reason, "stop_loss_broker_native")
                self.assertEqual(fills[0].price, expected)
                self.assertEqual(len(acc.closed_trades), 1)

    def test_ambiguous_entry_interval_can_stop_after_immediate_activation(self):
        for side, low, high in (("buy", 98, 100), ("sell", 100, 103)):
            exe, acc, fees = self.setup_execution(side)
            fills, _ = self.process(exe, acc, fees, 1, low, high)
            self.assertEqual([fill.leg for fill in fills], ["open", "close"])
            self.assertEqual(fills[1].reason, "stop_loss_broker_native")
            self.assertIsNone(acc.position.side)

    def test_known_order_does_not_look_ahead_to_later_stop(self):
        for side, first, later in (("buy", 103, 98), ("sell", 97, 103)):
            exe, acc, fees = self.setup_execution(side)
            self.process(exe, acc, fees, 1, 100)
            fills, _ = self.process(exe, acc, fees, 1.25, first)
            self.assertEqual(fills[0].reason, "take_profit_broker_native")
            self.assertFalse(self.process(exe, acc, fees, 1.5, later)[0])

    def test_tp_sl_and_adverse_gap_friction_all_directions(self):
        friction = ExecutionFriction(slippage_per_share=D(".1"), fee_per_fill=D(".02"))
        for side, price, reason, exit_price in (
            ("buy", 103, "TP", "102"), ("buy", 97, "SL", "96.9"),
            ("sell", 97, "TP", "98"), ("sell", 104, "SL", "104.1"),
        ):
            with self.subTest(side=side, reason=reason):
                exe, acc, fees = self.setup_execution(side, friction)
                entered, _ = self.process(exe, acc, fees, 1, 100)
                self.assertEqual(entered[0].price, D("100.1") if side == "buy" else D("99.9"))
                exited, _ = self.process(exe, acc, fees, 1.5, price)
                self.assertEqual(exited[0].price, D(exit_price))
                proof = exe.trade_evidence(acc, fees)[0]
                self.assertEqual(proof["exit_reason"], reason)
                self.assertEqual(D(proof["fees"]), D(".04"))
                self.assertEqual(D(proof["gross_pnl"]) - D(proof["fees"]), D(proof["net_pnl"]))
                self.assertEqual(proof["hold_seconds"], .5)
                self.assertIsNotNone(proof["tp_price"])
                self.assertIsNotNone(proof["sl_price"])

    def test_tp_touch_and_friction_threshold_do_not_grant_fill(self):
        for side, limit in (("buy", 102), ("sell", 98)):
            exe, acc, fees = self.setup_execution(side, ExecutionFriction(slippage_per_share=D(".1")))
            self.process(exe, acc, fees, 1, 100)
            self.assertFalse(self.process(exe, acc, fees, 1.1, limit)[0])
            price = D(limit) + (D(".05") if side == "buy" else D("-.05"))
            self.assertFalse(self.process(exe, acc, fees, 1.2, price)[0])

    def test_ack_delay_keeps_exposure_and_records_missed_crossing(self):
        exe, acc, fees = self.setup_execution(protection_ack_delay_ms=500)
        self.process(exe, acc, fees, 1, 100)
        self.assertFalse(exe.active_protections)
        self.assertFalse(self.process(exe, acc, fees, 1.25, 98)[0])
        self.assertEqual(acc.snapshot(mark_price=D(98)).unrealized_pnl, D(-2))
        self.assertIn("EXPOSED_MARK", [event["state"] for event in exe.protection_events])
        self.assertTrue(self.process(exe, acc, fees, 1.5, 98)[0])

    def test_ack_timeout_fails_without_inventing_recovery(self):
        exe, acc, fees = self.setup_execution(protection_ack_delay_ms=3000)
        self.process(exe, acc, fees, 1, 100)
        with self.assertRaisesRegex(ValueError, "acknowledgement timeout"):
            self.process(exe, acc, fees, 3, 98)

    def test_verifier_rejects_gap_outside_valid_exit_plan(self):
        exe, acc, fees = self.setup_execution()
        with self.assertRaises(InvalidProtectionGeometry):
            self.process(exe, acc, fees, 1, 103)

    def test_long_short_geometry_boundaries(self):
        for side, tp, sl in (("buy", 102, 99), ("sell", 98, 101)):
            direction = "long" if side == "buy" else "short"
            for fill, issue in ((100, None), (tp, "take_profit"),
                    (tp + (1 if side == "buy" else -1), "take_profit"),
                    (sl, "stop_loss"), (sl + (-1 if side == "buy" else 1), "stop_loss")):
                with self.subTest(side=side, fill=fill):
                    exe, acc, fees = self.setup_execution(side)
                    if issue is None:
                        self.assertEqual(self.process(exe, acc, fees, 1, fill)[0][0].price, D(fill))
                        continue
                    with self.assertRaises(InvalidProtectionGeometry) as raised:
                        self.process(exe, acc, fees, 1, fill)
                    proof = raised.exception.evidence
                    self.assertEqual(proof["issue_codes"], [f"invalid_{direction}_{issue}_price"])
                    self.assertEqual(D(proof["modeled_entry_fill"]), D(fill))
                    self.assertEqual(D(proof["intended_tp"]), D(tp))
                    self.assertEqual(D(proof["intended_sl"]), D(sl))
                    self.assertEqual(proof["signal_reference_price"], "100")
                    self.assertFalse(exe.active_protections)

    def test_non_geometry_verification_issues_remain_runtime_errors(self):
        from types import SimpleNamespace
        exe, acc, fees = self.setup_execution()
        verification = SimpleNamespace(protected=False, issues=[
            SimpleNamespace(code="invalid_long_take_profit_price"), SimpleNamespace(code="config_mismatch")])
        with mock.patch("replay.execution.verify_broker_native_protection", return_value=verification):
            with self.assertRaises(ValueError) as raised:
                self.process(exe, acc, fees, 1, 103)
        self.assertNotIsInstance(raised.exception, InvalidProtectionGeometry)

    def test_force_flat_before_exact_after_long_short_with_friction(self):
        deadline = (15 * 3600 + 59 * 60 + 30) - (9 * 3600 + 31 * 60)
        for side in ("buy", "sell"):
            exe, acc, fees = self.setup_execution(side, ExecutionFriction(slippage_per_share=D(".1")))
            self.process(exe, acc, fees, 1, 100)
            self.assertFalse(self.process(exe, acc, fees, deadline - .001, 100)[0])
            fills, _ = self.process(exe, acc, fees, deadline, 100)
            self.assertEqual(fills[0].reason, "session_force_flat")
            self.assertEqual(fills[0].price, D("99.9") if side == "buy" else D("100.1"))
            self.assertEqual(exe.trade_evidence(acc, fees)[0]["exit_reason"], "FORCE_FLAT")
            self.assertFalse(self.process(exe, acc, fees, deadline + .001, 100)[0])

    def test_force_flat_simultaneous_stop_uses_adverse_market_price_once(self):
        deadline = 23310
        for side, price in (("buy", 97), ("sell", 104)):
            exe, acc, fees = self.setup_execution(side)
            self.process(exe, acc, fees, 1, 100)
            fills, _ = self.process(exe, acc, fees, deadline, price)
            self.assertEqual(len(fills), 1)
            self.assertEqual(fills[0].reason, "session_force_flat")
            self.assertEqual(fills[0].price, D(price))
            self.assertFalse(exe.active_protections)

    def test_force_flat_simultaneous_tp_cannot_gain_market_price_improvement(self):
        for side, price, expected in (("buy", 104, 102), ("sell", 96, 98)):
            exe, acc, fees = self.setup_execution(side)
            self.process(exe, acc, fees, 1, 100)
            fills, _ = self.process(exe, acc, fees, 23310, price)
            self.assertEqual(len(fills), 1)
            self.assertEqual(fills[0].reason, "take_profit_broker_native")
            self.assertEqual(fills[0].price, D(expected))
            self.assertFalse(exe.pending_orders)

    def test_force_flat_delay_and_dst_are_explicit(self):
        exe, acc, fees = self.setup_execution(force_flat_delay_ms=500)
        self.process(exe, acc, fees, 1, 100)
        self.assertFalse(self.process(exe, acc, fees, 23310, 100)[0])
        self.assertTrue(self.process(exe, acc, fees, 23310.5, 100)[0])
        deadline = exe._deadline(pd.Timestamp("2026-11-02T15:00:00Z"))
        self.assertEqual(deadline, pd.Timestamp("2026-11-02T20:59:30.5Z"))

    def test_entry_available_after_production_cutoff_is_rejected(self):
        exe, acc, fees = self.setup_execution()
        exe._pending.clear()
        signal, intent = entry(bracket=True)
        obs = replace(observation(), triggering_sip_timestamp=pd.Timestamp("2026-09-21T19:59:00.001Z"))
        with self.assertRaisesRegex(ValueError, "entry window"):
            exe.queue(signal=signal, intent=intent, observation=obs, position=acc.position)

    def test_invalid_delay_assumptions_and_wrong_native_strategy_rejected(self):
        for value in (-1, 1.5, True):
            with self.assertRaises(ValueError):
                RawExecutionAssumptions(protection_ack_delay_ms=value)
        with self.assertRaisesRegex(ValueError, "bracket"):
            RawSipExecution(run_id="invalid", trader_id="test",
                            strategy=RenkoStrategyEngine(RenkoStrategyConfig(**trader().strategy_config)))

    def test_force_flat_cancels_pending_ack_and_unfilled_entry(self):
        exe, acc, fees = self.setup_execution(protection_ack_delay_ms=1000)
        # Parent submitted earlier, fills immediately before the deadline.
        self.process(exe, acc, fees, 23309.5, 100)
        fills, _ = self.process(exe, acc, fees, 23310, 100)
        self.assertEqual(fills[0].reason, "session_force_flat")
        exe, acc, fees = self.setup_execution()
        self.assertFalse(self.process(exe, acc, fees, 23310, 100)[0])
        self.assertFalse(exe.pending_orders)

    def test_missing_force_flat_price_and_overnight_exposure_fail(self):
        exe, acc, fees = self.setup_execution()
        self.process(exe, acc, fees, 1, 100)
        with self.assertRaisesRegex(ValueError, "Unresolved exposure"):
            exe.finish(nyse_session_window(TRAIN).end_utc, acc.position)
        with self.assertRaisesRegex(ValueError, "session boundary"):
            exe.advance_clock(nyse_session_window(HOLDOUT).start_utc, acc.position)

    def test_force_flat_submission_does_not_use_stale_price(self):
        exe, acc, fees = self.setup_execution()
        self.process(exe, acc, fees, 1, 100)
        exe.advance_clock(observation().second_bucket + timedelta(seconds=23310), acc.position)
        self.assertEqual(acc.position.side, "long")
        fills, _ = self.process(exe, acc, fees, 23310.5, 98)
        self.assertEqual(fills[0].price, D(98))
        self.assertEqual(fills[0].timestamp, observation().second_bucket + timedelta(seconds=23310.5))

    def test_orphan_and_partial_quantity_are_rejected(self):
        exe, acc, fees = self.setup_execution()
        self.process(exe, acc, fees, 1, 100)
        with self.assertRaisesRegex(ValueError, "Orphan"):
            exe.process_interval(timestamp=observation(index=2).second_bucket, low=100, high=100,
                                 position=PositionState.flat("TSLA"))
        with self.assertRaisesRegex(ValueError, "quantity or side"):
            exe.process_interval(timestamp=observation(index=2).second_bucket, low=100, high=100,
                                 position=replace(acc.position, qty=D(".5")))

    def test_excluded_print_cannot_trigger_native_exit(self):
        exe, acc, fees = self.setup_execution()
        self.process(exe, acc, fees, 1, 100)
        tick = replace(source(prices=(98,), seconds=(2,))[0], tape="C", conditions=("I",))
        self.assertFalse(exe.process_trade_group((tick,), acc.position)[0])
        self.assertEqual(acc.position.side, "long")

    def test_equal_timestamp_prices_group_without_claiming_row_order(self):
        rows = source(prices=(103, 98), seconds=(2, 2))
        self.assertEqual(list(raw_trade_groups(rows)), [rows])
        with self.assertRaisesRegex(ValueError, "chronologically"):
            list(raw_trade_groups(source(seconds=tuple(reversed(range(15))))))

    def test_raw_validated_candidate_repeat_and_frozen_holdout_assumptions(self):
        datasets = []
        for day in (TRAIN, HOLDOUT):
            rows = tuple(replace(item, tape="C") for item in source(day,
                prices=(100, 101, 101, 101, 99, 101, 101), seconds=(0, 1, 2, 2.1, 2.5, 2.9, 3)))
            datasets.append(prepare_validated_dataset((day,), {day: (rows, evidence(rows, day), None)}))
        training, holdout = datasets
        partition = ResearchPartition((TRAIN,), (HOLDOUT,))
        cfg = config(execution_model="RAW_SIP_NATIVE_V1", raw_execution=RawExecutionAssumptions(protection_ack_delay_ms=50))
        native = trader(exit_management="bracket", candidate_config_fingerprint="config", market_contract_fingerprint="market")
        candidates = generate_challengers(cfg, native, training, partition)
        candidate = next(item for item in candidates if item.spec().brick_pct == D("0.66")
                         and item.spec().strategy_parameters.entry_bricks == 1
                         and item.spec().strategy_parameters.take_profit_bricks == 2
                         and item.spec().strategy_parameters.stop_loss_bricks == 1)
        first = evaluate_training(candidate, training, cfg)
        second = evaluate_training(candidate, training, cfg)
        self.assertEqual(first.status, "COMPLETED", first.error_message)
        self.assertEqual(first.row, second.row)
        proof = first.row["raw_execution_evidence"]
        self.assertEqual(proof["completed_trades"][0]["exit_reason"], "SL")
        finalists = freeze_finalists((candidate,), partition, training, {"synthetic": True})
        changed = replace(cfg, execution_model="NEXT_CANONICAL_OBSERVATION_V1",
                          raw_execution=RawExecutionAssumptions(protection_ack_delay_ms=1000))
        held = evaluate_holdout(finalists, holdout, changed)[0]
        self.assertEqual(held.status, "COMPLETED", held.error_message)
        self.assertEqual(held.row["raw_execution_evidence"]["assumptions"]["protection_ack_delay_ms"], 50)
        self.assertEqual(held.spec.brick_size_dollars, first.spec.brick_size_dollars)
        with self.assertRaisesRegex(ValueError, "Raw execution evidence"):
            replace(training, raw_trades=training.raw_trades[:-1]).verify()

    def test_candidate_fixed_charge_base_stress_accounting_and_identity(self):
        datasets = []
        for day in (TRAIN, HOLDOUT):
            rows = tuple(replace(item, tape="C") for item in source(day,
                prices=(100, 101, 101, 101, 99, 101, 101), seconds=(0, 1, 2, 2.1, 2.5, 2.9, 3)))
            datasets.append(prepare_validated_dataset((day,), {day: (rows, evidence(rows, day), None)}))
        training, holdout = datasets
        partition = ResearchPartition((TRAIN,), (HOLDOUT,))
        friction = ExecutionFriction(slippage_per_share=".1", application="PER_FILL_CHARGE_V1", fee_per_fill=".02",
                                     calibration_evidence_hash="synthetic", calibration_sessions=("2026-09-08",))
        cfg = config(execution_friction=friction)
        native = trader(exit_management="bracket", candidate_config_fingerprint="config", market_contract_fingerprint="market")
        candidate = next(item for item in generate_challengers(cfg, native, training, partition)
                         if item.spec().brick_pct == D("0.66") and item.spec().strategy_parameters.entry_bricks == 1
                         and item.spec().strategy_parameters.take_profit_bricks == 2 and item.spec().strategy_parameters.stop_loss_bricks == 1)
        first = evaluate_training(candidate, training, cfg)
        second = evaluate_training(candidate, training, cfg)
        self.assertEqual(first.status, "COMPLETED", first.error_message)
        self.assertEqual(first.row, second.row)
        self.assertEqual(first.row["pre_friction_gross_pnl_dollars"], -2)
        self.assertEqual(first.row["execution_friction_dollars"], .2)
        self.assertEqual(first.row["fees_dollars"], .04)
        self.assertAlmostEqual(first.row["net_pnl_dollars"], -2.24)
        finalists = freeze_finalists((candidate,), partition, SelectionRules(), {"synthetic": True})
        base = evaluate_holdout(finalists, holdout, cfg)[0]
        stress = evaluate_holdout(finalists, holdout, cfg, stress=True)[0]
        self.assertEqual(stress.status, "COMPLETED", stress.error_message)
        self.assertAlmostEqual(stress.row["net_pnl_dollars"], -2.44)
        self.assertEqual(base.spec, stress.spec)
        self.assertNotEqual(base.row["configuration_hash"], stress.row["configuration_hash"])
        self.assertEqual(base.row["fees_dollars"], stress.row["fees_dollars"])


class DataIntegrityTests(unittest.TestCase):
    def test_completed_session_close_holiday_and_dst_boundaries(self):
        before = ResearchPartition.completed(datetime(2026, 9, 25, 19, 59, 59, tzinfo=timezone.utc))
        after = ResearchPartition.completed(datetime(2026, 9, 25, 20, tzinfo=timezone.utc))
        self.assertEqual(before.holdout[-1], date(2026, 9, 24))
        self.assertEqual(after.holdout[-1], date(2026, 9, 25))
        self.assertNotIn(date(2026, 9, 7), after.optimization)
        self.assertEqual(len(after.optimization), 20)
        self.assertEqual(len(after.holdout), 5)
        self.assertEqual(nyse_session_window(date(2026, 11, 2)).start_utc.hour, 14)
        early = ResearchPartition.completed(datetime(2026, 11, 27, 22, tzinfo=timezone.utc))
        self.assertNotIn(date(2026, 11, 27), early.optimization + early.holdout)

    def test_partition_rejects_overlap_reverse_and_partial_sessions(self):
        for training, holdout in (((TRAIN,), (TRAIN,)), ((HOLDOUT,), (TRAIN,)), ((date(2026, 11, 27),), (date(2026, 11, 30),))):
            with self.assertRaises(ValueError):
                ResearchPartition(training, holdout)

    def test_legitimate_no_update_and_rejected_trade_do_not_require_23400_rows(self):
        rows = source(prices=(100, 500, 101, 102), seconds=(0, 1, 10, 20))
        rows = (rows[0], replace(rows[1], conditions=("I",)), *rows[2:])
        report, obs = validate_session(TRAIN, rows, evidence(rows))
        self.assertEqual(report.status, "PASS")
        self.assertEqual([item.close_price for item in obs], [D(100), D(101)])
        self.assertEqual(report.pending_final_seconds, 1)
        self.assertTrue(all(gap[3] == "LEGITIMATE_NO_EMISSION" for gap in report.gaps))

    def test_missing_canonical_second_fails_despite_equal_span(self):
        rows = source()
        _, obs = validate_session(TRAIN, rows, evidence(rows))
        report, _ = validate_session(TRAIN, rows, evidence(rows), obs[:2] + obs[3:])
        self.assertEqual(report.status, "FAIL")
        self.assertIn("canonical_progression_mismatch", report.failures)
        self.assertIn("INVALID_MISSING_CANONICAL_OBSERVATION", [gap[3] for gap in report.gaps])

    def test_removed_source_row_fails_independent_manifest(self):
        rows = source()
        report, _ = validate_session(TRAIN, rows[:2] + rows[3:], evidence(rows))
        self.assertEqual(report.status, "FAIL")
        self.assertIn("source_hash_or_count_mismatch", report.failures)

    def test_unverified_source_fails_instead_of_claiming_legitimate_silence(self):
        rows = source()
        proof = evidence(rows, acquisition_complete=False)
        report, _ = validate_session(TRAIN, rows, proof)
        self.assertEqual(report.status, "FAIL")
        self.assertTrue(all(gap[3] == "UNVERIFIED_OR_INVALID" for gap in report.gaps))
        with self.assertRaisesRegex(ValueError, "failed integrity"):
            prepare_validated_dataset((TRAIN,), {TRAIN: (rows, proof, None)})

    def test_backfill_pass_and_changed_canonical_price(self):
        rows = source()
        report, obs = validate_session(TRAIN, rows, evidence(rows, backfilled=True))
        self.assertEqual(report.status, "BACKFILLED_AND_PASS")
        changed = (replace(obs[0], close_price=D(999)), *obs[1:])
        self.assertEqual(validate_session(TRAIN, rows, evidence(rows), changed)[0].status, "FAIL")

    def test_dataset_identity_determinism_and_tamper_detection(self):
        first, second = dataset(), dataset()
        self.assertEqual(first, second)
        self.assertNotEqual(first.identity, dataset(prices=(101, 102, 103)).identity)
        with self.assertRaisesRegex(ValueError, "fingerprint"):
            replace(first, observations=first.observations[1:]).verify()

    def test_future_rows_cannot_enter_training_preparation(self):
        rows = source(HOLDOUT)
        with self.assertRaises(ValueError):
            prepare_validated_dataset((TRAIN,), {HOLDOUT: (rows, evidence(rows, HOLDOUT), None)})
        with self.assertRaises(ValueError):
            prepare_validated_dataset((TRAIN,), {TRAIN: (rows, evidence(rows), None)})

    def test_source_order_duplicates_and_wrong_window_fail(self):
        rows = source()
        for invalid in (tuple(reversed(rows)), (rows[0], *rows), (replace(rows[0], symbol="AAPL"), *rows[1:])):
            self.assertEqual(validate_session(TRAIN, invalid, evidence(invalid))[0].status, "FAIL")
        self.assertEqual(validate_session(TRAIN, rows, evidence(rows, requested_start=rows[0].sip_timestamp))[0].status, "FAIL")


class FrozenResearchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.training = dataset()
        cls.partition = ResearchPartition((TRAIN,), (HOLDOUT,))
        cls.config = config()
        cls.candidates = generate_challengers(cls.config, trader(), cls.training, cls.partition)

    def test_exact_510_unique_approved_candidates_without_evaluation(self):
        with mock.patch("optimization.tsla_production_selection.run_candidate", side_effect=AssertionError("No sweep")):
            candidates = generate_challengers(self.config, trader(), self.training, self.partition)
        self.assertEqual(len(candidates), 510)
        self.assertEqual(len({item.identity for item in candidates}), 510)
        specs = [item.spec() for item in candidates]
        self.assertEqual({item.strategy_parameters.entry_confirmation for item in specs}, {"E1", "E2"})
        self.assertEqual({(item.strategy_parameters.take_profit_bricks, item.strategy_parameters.stop_loss_bricks)
                          for item in specs}, set(EXIT_PAIRS))
        self.assertEqual({item.strategy_parameters.strategy_type for item in specs}, {"renko_tp_sl"})
        self.assertEqual(sorted({item.brick_pct for item in specs}), [D(i) / 100 for i in range(16, 67)])
        self.assertEqual(min(item.brick_pct for item in specs), D("0.16"))
        self.assertEqual(max(item.brick_pct for item in specs), D("0.66"))

    def test_holdout_prices_and_settings_cannot_recalibrate_frozen_candidate(self):
        candidate = self.candidates[0]
        finalists = freeze_finalists((candidate,), self.partition, SelectionRules(), {"synthetic": True})
        altered_config = config(initial_capital=999999, execution_friction=ExecutionFriction(slippage_per_share=10))
        for prices in ((100, 101, 102), (900, 800, 700)):
            with mock.patch("optimization.tsla_production_selection.run_candidate", return_value="checked") as run:
                evaluate_holdout(finalists, dataset(HOLDOUT, prices=prices), altered_config)
            self.assertEqual(run.call_args.kwargs["spec"], candidate.spec())
            self.assertEqual(run.call_args.kwargs["config"].initial_capital, self.config.initial_capital)
            self.assertEqual(run.call_args.kwargs["config"].execution_friction, self.config.execution_friction)
        self.assertEqual(candidate.spec().brick_size_dollars, D("0.16"))

    def test_immutable_payload_and_tampering_rejected(self):
        candidate = self.candidates[0]
        with self.assertRaises(FrozenInstanceError):
            candidate.identity = "changed"
        thawed = candidate.spec()
        thawed.trader.strategy_config["qty"] = 500
        self.assertEqual(candidate.spec().trader.strategy_config["qty"], 1)
        with self.assertRaises(ValueError):
            replace(candidate, spec_json=candidate.spec_json + " ").spec()

    def test_training_holdout_and_cost_identity_are_isolated(self):
        with self.assertRaises(ValueError):
            generate_challengers(self.config, trader(), dataset(HOLDOUT), self.partition)
        changed = generate_challengers(config(execution_friction=ExecutionFriction(half_spread="0.01")),
                                       trader(), self.training, self.partition)
        self.assertNotEqual(changed[0].identity, self.candidates[0].identity)

    def test_unverified_tsla_entry_points_fail_closed(self):
        candidate = self.candidates[0]
        result = run_candidate(config=self.config, spec=candidate.spec(), prepared=_prepared(self.training),
                               observations=self.training.observations)
        self.assertEqual(result.status, "FAILED")
        self.assertIn("validated dataset", result.error_message)
        with mock.patch("optimization.runner._load_configured_renko_traders", return_value=(trader(),)), \
             mock.patch("optimization.runner.prepare_source") as prepare:
            with self.assertRaisesRegex(ValueError, "legacy full-sweep"):
                run_optimization(self.config)
            prepare.assert_not_called()
        self.assertEqual(evaluate_training(candidate, self.training, self.config).status, "COMPLETED")

    def test_real_holdout_evaluation_reuses_candidate_without_mutation(self):
        candidate = self.candidates[0]
        frozen = freeze_finalists((candidate,), self.partition, SelectionRules(), {"fixture": True})
        before = candidate.spec_json
        results = evaluate_holdout(frozen, dataset(HOLDOUT), config())
        self.assertEqual(results[0].status, "COMPLETED")
        self.assertEqual(results[0].spec.brick_size_dollars, D("0.16"))
        self.assertEqual(candidate.spec_json, before)

    def test_bracket_lifecycle_ids_repeat_exactly(self):
        universe = generate_challengers(self.config, trader(exit_management="bracket",
                                        candidate_config_fingerprint="synthetic", market_contract_fingerprint="synthetic"),
                                        self.training, self.partition)
        spec = next(item.spec() for item in universe if item.spec().brick_pct == D("0.66")
                    and item.spec().strategy_parameters.entry_confirmation == "E1"
                    and item.spec().strategy_parameters.take_profit_bricks == 1)
        with tempfile.TemporaryDirectory() as root:
            paths = [Path(root) / name for name in ("one", "two")]
            for path in paths:
                path.mkdir()
                _execute_candidate(config=approved_universe_config(self.config), spec=spec, prepared=_prepared(self.training),
                                   observations=self.training.observations, selected_output_dir=path)
            for name in ("signals.jsonl", "orders.jsonl", "fills.jsonl", "closed_trades.jsonl"):
                self.assertEqual((paths[0] / name).read_bytes(), (paths[1] / name).read_bytes())

    def test_synthetic_selection_freezes_one_center_and_rejects_spikes(self):
        points = tuple(RegimePoint(D(index) / 100, "E2", 2, 1, D(1000 if index == 37 else 10), D(2), 50)
                       for index in range(36, 45))
        results = synthetic_training_results(self.candidates)
        selected = select_finalists(self.training, self.candidates, points, self.partition, training_results=results)
        self.assertEqual(len(selected.candidates), 1)
        self.assertEqual(selected.candidates[0].spec().brick_pct, D("0.40"))
        self.assertEqual(selected, select_finalists(self.training, self.candidates, points, self.partition, training_results=results))
        with self.assertRaises(ValueError):
            select_finalists(self.training, self.candidates, points[:1], self.partition)

    def test_deterministic_trades_entries_exits_pnl_metrics_and_cached_production_path(self):
        # One synthetic fixed strategy, not an optimization or ranking run.
        candidate = next(item for item in self.candidates if item.spec().brick_pct == D("0.66")
                         and item.spec().strategy_parameters.entry_confirmation == "E1"
                         and item.spec().strategy_parameters.take_profit_bricks == 1)
        with tempfile.TemporaryDirectory() as root:
            paths = [Path(root) / name for name in ("first", "repeat", "cached")]
            results = []
            for path, cached in zip(paths, (False, False, True)):
                path.mkdir()
                results.append(_execute_candidate(config=self.config, spec=candidate.spec(), prepared=_prepared(self.training),
                                                   observations=self.training.observations, selected_output_dir=path,
                                                   use_cached_renko_stream=cached))
            self.assertEqual(results[0], results[1])
            self.assertEqual(results[0], results[2])
            self.assertGreater(results[0]["closed_trades"], 0)
            for name in ("signals.jsonl", "orders.jsonl", "fills.jsonl", "closed_trades.jsonl", "equity_curve.csv"):
                self.assertEqual((paths[0] / name).read_bytes(), (paths[1] / name).read_bytes())
                self.assertEqual((paths[0] / name).read_bytes(), (paths[2] / name).read_bytes())


class ExecutionIntegrityTests(unittest.TestCase):
    def test_all_four_sides_receive_adverse_friction(self):
        friction = ExecutionFriction(slippage_per_share="0.10", half_spread="0.05", slippage_bps="10")
        for side, leg, position in (
            ("buy", "open", PositionState.flat("TSLA")),
            ("sell", "close", PositionState("TSLA", "long", D(1), D(100))),
            ("sell", "open", PositionState.flat("TSLA")),
            ("buy", "close", PositionState("TSLA", "short", D(1), D(100))),
        ):
            execution = NextObservationExecution(run_id="cost", trader_id="synthetic_tsla", friction=friction)
            signal, intent = entry(side)
            intent = replace(intent, close_position=leg == "close")
            execution.queue(signal=signal, intent=intent, observation=observation(), position=position)
            fill = execution.fill_pending(observation=observation(index=1), position=position)[0].fills[0]
            self.assertEqual(fill.price, D("100.25") if side == "buy" else D("99.75"))
            self.assertEqual(fill.leg, leg)

    def test_same_timestamp_other_id_and_earlier_observation_cannot_fill(self):
        execution = NextObservationExecution(run_id="clock", trader_id="synthetic_tsla")
        signal, intent = entry()
        execution.queue(signal=signal, intent=intent, observation=observation(), position=PositionState.flat("TSLA"))
        for obs in (replace(observation(), observation_id="different"), observation(index=-1)):
            self.assertEqual(execution.fill_pending(observation=obs, position=PositionState.flat("TSLA")), ())
        self.assertEqual(len(execution.fill_pending(observation=observation(index=1), position=PositionState.flat("TSLA"))), 1)

    def test_stop_gaps_and_tp_limits_for_long_and_short(self):
        for side, final_price, expected, reason in (("buy", "95", "94.9", "stop_loss_broker_native"),
                                                   ("sell", "105", "105.1", "stop_loss_broker_native"),
                                                   ("buy", "110", "102", "take_profit_broker_native"),
                                                   ("sell", "90", "98", "take_profit_broker_native")):
            execution = NextObservationExecution(run_id="gap", trader_id="synthetic_tsla",
                                                friction=ExecutionFriction(slippage_per_share="0.1"), conservative_protections=True)
            signal, intent = entry(side, bracket=True)
            ledger = AccountingLedger(symbol="TSLA", starting_capital=1000)
            execution.queue(signal=signal, intent=intent, observation=observation(), position=ledger.position)
            fill = execution.fill_pending(observation=observation(index=1), position=ledger.position)[0].fills[0]
            ledger.process_fill(fill)
            close = execution.fill_pending(observation=observation(final_price, 2), position=ledger.position)[0].fills[0]
            self.assertEqual(close.price, D(expected))
            self.assertEqual(close.reason, reason)
            self.assertEqual(execution.active_protections, ())

    def test_intrasecond_native_stop_is_a_material_certification_limitation(self):
        executions = [NextObservationExecution(run_id="native-limit", trader_id="synthetic_tsla", conservative_protections=True)
                      for _ in range(2)]
        positions = []
        for execution in executions:
            signal, intent = entry(bracket=True)
            execution.queue(signal=signal, intent=intent, observation=observation(), position=PositionState.flat("TSLA"))
            fill = execution.fill_pending(observation=observation(index=1), position=PositionState.flat("TSLA"))[0].fills[0]
            ledger = AccountingLedger(symbol="TSLA")
            ledger.process_fill(fill)
            positions.append(ledger.position)
        from trading_events import MarketTick
        tick = MarketTick(pd.Timestamp(observation(index=1).second_bucket) + pd.Timedelta(milliseconds=500),
                          "TSLA", 98, 1, conditions=("@",))
        raw_exit = executions[0].fill_active_protections_on_raw_trade(tick=tick, position=positions[0])
        close_only_exit = executions[1].fill_pending(observation=observation("100", 2), position=positions[1])
        self.assertEqual(raw_exit[0].fills[0].reason, "stop_loss_broker_native")
        self.assertEqual(close_only_exit, ())

    def test_conservative_mode_rejects_unsupported_execution(self):
        for kwargs in ({"order_type": "limit"}, {"stop_loss_order_type": "stop_limit"}, {"exit_management": "oco"}):
            execution = NextObservationExecution(run_id="unsupported", trader_id="synthetic_tsla", conservative_protections=True)
            signal, intent = entry()
            with self.assertRaises(ValueError):
                execution.queue(signal=signal, intent=replace(intent, **kwargs), observation=observation(), position=PositionState.flat("TSLA"))

    def test_force_flat_close_cancels_stale_bracket_before_reentry(self):
        execution = NextObservationExecution(run_id="flat", trader_id="synthetic_tsla", conservative_protections=True)
        signal, intent = entry(bracket=True)
        ledger = AccountingLedger(symbol="TSLA")
        execution.queue(signal=signal, intent=intent, observation=observation(), position=ledger.position)
        ledger.process_fill(execution.fill_pending(observation=observation(index=1), position=ledger.position)[0].fills[0])
        exit_signal = replace(signal, signal_type="exit", timestamp=pd.Timestamp(observation(index=1).second_bucket), broker_side="sell")
        exit_intent = replace(intent, side="sell", signal_type="exit", timestamp=exit_signal.timestamp,
                              close_position=True, exit_plan=None, reason="session_force_flat")
        execution.queue(signal=exit_signal, intent=exit_intent, observation=observation(index=1), position=ledger.position)
        ledger.process_fill(execution.fill_pending(observation=observation(index=2), position=ledger.position)[0].fills[0])
        self.assertIsNone(ledger.position.side)
        self.assertEqual(execution.active_protections, ())

    def test_stress_is_worse_and_zero_baseline_is_unavailable(self):
        self.assertIsNone(ExecutionFriction().stress())
        base = ExecutionFriction(slippage_bps=5, fee_per_share="0.01")
        stress = base.stress()
        self.assertGreater(stress.price(D(100), "buy"), base.price(D(100), "buy"))
        self.assertLess(stress.price(D(100), "sell"), base.price(D(100), "sell"))
        self.assertGreater(stress.fee(D(1)), base.fee(D(1)))
        for value in ("-1", "NaN", "Infinity"):
            with self.assertRaises(ValueError):
                ExecutionFriction(slippage_per_share=value)


class AccountingIntegrityTests(unittest.TestCase):
    def test_partial_close_fixed_charge_allocated_once(self):
        ledger = AccountingLedger(symbol="TSLA", starting_capital=1000)
        fees = ResearchFeeLedger(ExecutionFriction(slippage_per_share=".1", application="PER_FILL_CHARGE_V1", fee_per_fill=1))
        for index, (leg, side, qty, price) in enumerate((("open", "buy", 2, 100), ("close", "sell", 1, 110), ("close", "sell", 1, 110))):
            fill = NormalizedFill("2026-09-21T14:00:00Z", "TSLA", "test", side, qty, price, leg=leg, fill_id=str(index))
            before = ledger.position
            ledger.process_fill(fill)
            fees.record(fill, before)
            if index == 0:
                snapshot = fees.snapshot(ledger.snapshot(mark_price=D(99)))
                self.assertEqual(snapshot.equity, D("996.8"))
        snapshot = fees.snapshot(ledger.snapshot())
        self.assertEqual(snapshot.net_pnl, D("16.6"))
        self.assertEqual([trade.pnl for trade in snapshot.closed_trades], [D("8.3"), D("8.3")])
        self.assertEqual(fees.total, D(3))
        self.assertEqual(fees.friction_total, D(".4"))
        self.assertEqual(fees.open_friction, 0)

    def roundtrips(self, profits, fee="0", sides=None):
        ledger = AccountingLedger(symbol="TSLA", starting_capital=1000)
        fees = ResearchFeeLedger(ExecutionFriction(fee_per_share=fee))
        for index, profit in enumerate(profits):
            side = (sides or ["long"] * len(profits))[index]
            for leg, action, price in (("open", "buy" if side == "long" else "sell_short", D(100)),
                                       ("close", "sell" if side == "long" else "buy", D(100) + D(profit) * (1 if side == "long" else -1))):
                fill = NormalizedFill(datetime(2026, 9, 21, 14, index, 0 if leg == "open" else 1, tzinfo=timezone.utc),
                                      "TSLA", "test", action, 1, price, fill_id=f"{index}:{leg}", leg=leg)
                before = ledger.position
                ledger.process_fill(fill)
                fees.record(fill, before)
        return ledger, fees, fees.snapshot(ledger.snapshot())

    def test_hand_calculated_pnl_fees_win_loss_pf_expectancy_direction(self):
        ledger, fees, snapshot = self.roundtrips((10, -4), fee="0.5", sides=("long", "short"))
        self.assertEqual(ledger.realized_pnl, D(6))
        self.assertEqual(fees.total, D(2))
        self.assertEqual(snapshot.net_pnl, D(4))
        self.assertEqual([trade.pnl for trade in snapshot.closed_trades], [D(9), D(-5)])
        self.assertEqual(snapshot.profit_factor, D("1.8"))
        self.assertEqual(snapshot.win_rate, D(50))
        self.assertEqual(snapshot.average_trade_pnl_dollars, D(2))
        self.assertEqual(snapshot.average_winner_dollars, D(9))
        self.assertEqual(snapshot.average_loser_dollars, D(-5))
        metrics = build_metrics(snapshot=snapshot, initial_capital=D(1000), allocated_capital=D(100),
                                observations=[], order_latencies=[])
        self.assertEqual(metrics["strategy_return_pct"], 0.4)
        self.assertEqual(metrics["return_on_allocated_capital_pct"], 4)
        concentrations = concentration_metrics(snapshot.closed_trades, {TRAIN: D(4)})
        self.assertEqual(concentrations["long_pct"], D(225))
        self.assertEqual(concentrations["short_pct"], D(-125))

    def test_no_trades_all_wins_all_losses_zero_and_negative_total(self):
        for profits, expected_pf in (((), None), ((2, 3), D("Infinity")), ((-2, -3), D(0)), ((2, -2), D(1)), ((0,), None)):
            _, _, snapshot = self.roundtrips(profits)
            self.assertEqual(snapshot.profit_factor, expected_pf)
            metrics = concentration_metrics(snapshot.closed_trades, {TRAIN: snapshot.net_pnl})
            if snapshot.net_pnl <= 0:
                self.assertIsNone(metrics["top_three_trades_pct"])
                self.assertIsNone(metrics["long_pct"])
            self.assertNotIn("NaN", canonical_json(metrics))

    def test_partial_close_allocates_entry_fees_and_open_fees_reduce_equity(self):
        ledger = AccountingLedger(symbol="TSLA", starting_capital=1000)
        fees = ResearchFeeLedger(ExecutionFriction(fee_per_fill=1))
        for index, (leg, side, qty, price) in enumerate((("open", "buy", 2, 100), ("close", "sell", 1, 110), ("close", "sell", 1, 110))):
            fill = NormalizedFill("2026-09-21T14:00:00Z", "TSLA", "test", side, qty, price, leg=leg, fill_id=str(index))
            before = ledger.position
            ledger.process_fill(fill)
            fees.record(fill, before)
            if index == 0:
                snapshot = fees.snapshot(ledger.snapshot())
                self.assertEqual(snapshot.equity, D(999))
                self.assertEqual(snapshot.equity_curve[-1].equity, D(999))
                self.assertEqual(snapshot.max_drawdown_dollars, D(1))
        snapshot = fees.snapshot(ledger.snapshot())
        self.assertEqual([trade.pnl for trade in snapshot.closed_trades], [D("8.5"), D("8.5")])
        self.assertEqual(snapshot.net_pnl, D(17))


class RegimeIntegrityTests(unittest.TestCase):
    def points(self, profitable=range(36, 45)):
        return tuple(RegimePoint(D(index) / 100, "E2", 2, 1, D(1000 if index == 37 else 10 if index in profitable else -5),
                                 D(2 if index in profitable else "0.5"), 50) for index in range(32, 49))

    def test_known_plateau_center_is_not_peak(self):
        regime = distinct_regimes(self.points())[0]
        self.assertEqual(regime["strength"], "STRONG PLATEAU")
        self.assertEqual((regime["lower"], regime["upper"]), (D("0.36"), D("0.44")))
        self.assertEqual(regime["center"].brick, D("0.40"))
        self.assertEqual(regime["exact_maximum"].brick, D("0.37"))
        self.assertEqual(len(regime["neighbors"]), 9)

    def test_spike_weak_and_moderate(self):
        self.assertEqual(distinct_regimes(self.points({37}))[0]["strength"], "OPTIMIZATION SPIKE")
        self.assertEqual(distinct_regimes(self.points({37, 38}))[0]["strength"], "WEAK")
        self.assertEqual(distinct_regimes(self.points(range(36, 42)))[0]["strength"], "MODERATE PLATEAU")

    def test_distinct_families_not_three_adjacent_rows(self):
        points = self.points()
        self.assertEqual(len(distinct_regimes(points)), 1)
        other = tuple(replace(point, entry="E1") for point in points)
        regimes = distinct_regimes(points + other)
        self.assertEqual(len(regimes), 2)
        self.assertNotEqual(regimes[0]["family"], regimes[1]["family"])
        self.assertEqual(regimes, distinct_regimes(tuple(reversed(points + other))))

    def test_bounds_family_isolation_missing_neighbor_and_even_center(self):
        points = tuple(RegimePoint(D(index) / 100, "E1", 1, 1, D(10), D(2), 50) for index in range(16, 22))
        self.assertEqual(len(neighborhood(points, points[0])), 5)
        regime = distinct_regimes(points)[0]
        self.assertEqual(regime["center"].brick, D("0.18"))
        self.assertTrue(regime["boundary_truncated"])
        with self.assertRaises(ValueError):
            distinct_regimes(points + (points[0],))


class SafetyIntegrityTests(unittest.TestCase):
    def test_cleanup_only_owned_files_after_success(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            temporary = ResearchTemporaryFiles(root / "new_research")
            owned = temporary.create("dataset.bin", b"12345")
            preserved = temporary.root / "report.json"
            preserved.write_text("evidence")
            with self.assertRaises(ValueError):
                temporary.cleanup(validated_successfully=False)
            self.assertTrue(owned.exists())
            result = temporary.cleanup(validated_successfully=True)
            self.assertEqual(result["space_reclaimed"], 5)
            self.assertTrue(preserved.exists())

    def test_cleanup_rejects_existing_roots_symlinks_and_tampering(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            with self.assertRaises(FileExistsError):
                ResearchTemporaryFiles(root)
            with self.assertRaises(ValueError):
                ResearchTemporaryFiles(root / "live")
            temporary = ResearchTemporaryFiles(root / "new_research")
            with self.assertRaises(ValueError):
                temporary.create("../outside", b"x")
            owned = temporary.create("data", b"x")
            owned.write_bytes(b"changed")
            with self.assertRaises(ValueError):
                temporary.cleanup(validated_successfully=True)
            self.assertTrue(owned.exists())

    def test_cleanup_rejects_symlink_replacement_and_preserves_external_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            external = root / "existing"
            external.write_text("preserve")
            temporary = ResearchTemporaryFiles(root / "new_research")
            owned = temporary.create("data", b"x")
            owned.unlink()
            owned.symlink_to(external)
            with self.assertRaises(ValueError):
                temporary.cleanup(validated_successfully=True)
            self.assertEqual(external.read_text(), "preserve")

    def test_research_module_has_no_live_execution_or_broker_calls(self):
        import ast
        import optimization.tsla_production_selection as module
        tree = ast.parse(Path(module.__file__).read_text())
        imports = [node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
        self.assertNotIn("alpaca_trader", imports)
        self.assertNotIn("execution_engine", imports)
        names = [node.func.attr for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)]
        self.assertFalse(set(names) & {"submit_order", "cancel_order", "replace_order", "run_optimization"})


if __name__ == "__main__":
    unittest.main()
