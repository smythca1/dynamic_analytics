from __future__ import annotations

import csv
import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest import mock

import pandas as pd

from optimization.cli import main as optimization_main
from optimization.models import OptimizationConfig, SessionWindow
from optimization.recommendations import build_recommendations
from optimization.robustness import annotate_robustness
from optimization.runner import (
    _build_cached_renko_stream,
    _candidate_batches,
    _execute_candidate,
    brick_grid,
    build_candidate_specs,
    load_enabled_renko_traders,
    run_optimization,
    strategy_parameter_grid_for,
)
from optimization.source_inventory import StageRequest, remote_source_path, stage_sources
from optimization.source_preparation import load_prepared_observations, prepare_source
from replay.execution import NextObservationExecution
from replay.metrics import build_capital_metrics, build_metrics
from silver.accounting import AccountingLedger
from silver.accounting.models import AccountingSnapshot, EquityPoint, NormalizedFill, PositionState
from strategy_engine import TP_SL_LIFECYCLE_CLOSE_SUBMITTED, TP_SL_LIFECYCLE_EXIT_PENDING, TP_SL_LIFECYCLE_FLAT, RenkoStrategyConfig, RenkoStrategyEngine
from trading_events import MarketTick, OrderIntent, Signal


SESSIONS = (date(2026, 7, 20),)


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def reference_capital_metrics(
    *,
    initial_capital: Decimal,
    allocated_capital: Decimal | None,
    unused_cash: Decimal | None,
    quantity: Decimal | None,
    net_pnl: Decimal,
    position_samples: list[dict],
    fills: list[NormalizedFill],
) -> dict:
    deployed_values: list[Decimal] = []
    cash_values: list[Decimal] = []
    position_values: list[Decimal] = []
    exposure_values: list[Decimal] = []
    long_count = 0
    short_count = 0
    flat_count = 0
    for sample in position_samples:
        qty = Decimal(str(sample.get("qty", "0")))
        mark_price = Decimal(str(sample.get("mark_price", "0")))
        equity = Decimal(str(sample.get("equity", initial_capital)))
        side = sample.get("side")
        position_value = abs(qty) * mark_price
        deployed_values.append(position_value)
        position_values.append(position_value)
        cash_values.append(equity - position_value)
        exposure_values.append(position_value / equity * Decimal("100") if equity else Decimal("0"))
        if qty == 0 or side in {None, "", "flat"}:
            flat_count += 1
        elif side == "long":
            long_count += 1
        elif side == "short":
            short_count += 1
    sample_count = Decimal(len(position_samples)) if position_samples else Decimal("0")
    turnover = sum((fill.qty * fill.price for fill in fills), Decimal("0"))
    average_trade_size = turnover / Decimal(len(fills)) if fills else None
    return {
        "average_deployed_capital_dollars": reference_num_or_unknown(reference_avg(deployed_values)),
        "maximum_deployed_capital_dollars": reference_num_or_unknown(max(deployed_values) if deployed_values else None),
        "minimum_deployed_capital_dollars": reference_num_or_unknown(min(deployed_values) if deployed_values else None),
        "average_cash_balance_dollars": reference_num_or_unknown(reference_avg(cash_values)),
        "minimum_cash_balance_dollars": reference_num_or_unknown(min(cash_values) if cash_values else None),
        "average_position_value_dollars": reference_num_or_unknown(reference_avg(position_values)),
        "maximum_position_value_dollars": reference_num_or_unknown(max(position_values) if position_values else None),
        "average_exposure_pct": reference_num_or_unknown(reference_avg(exposure_values)),
        "maximum_exposure_pct": reference_num_or_unknown(max(exposure_values) if exposure_values else None),
        "long_exposure_pct": reference_num_or_unknown(Decimal(long_count) / sample_count * Decimal("100") if sample_count else None),
        "short_exposure_pct": reference_num_or_unknown(Decimal(short_count) / sample_count * Decimal("100") if sample_count else None),
        "flat_exposure_pct": reference_num_or_unknown(Decimal(flat_count) / sample_count * Decimal("100") if sample_count else None),
        "capital_utilization_pct": reference_num_or_unknown(allocated_capital / initial_capital * Decimal("100") if allocated_capital is not None and initial_capital else None),
        "pnl_per_share_dollars": reference_num_or_unknown(net_pnl / quantity if quantity else None),
        "pnl_on_allocated_capital_dollars": reference_num(net_pnl),
        "return_on_allocated_capital_pct": reference_num_or_unknown(net_pnl / allocated_capital * Decimal("100") if allocated_capital is not None and allocated_capital else None),
        "return_on_total_account_equity_pct": reference_num_or_unknown(net_pnl / initial_capital * Decimal("100") if initial_capital else None),
        "turnover_dollars": reference_num(turnover),
        "turnover_multiple": reference_num_or_unknown(turnover / allocated_capital if allocated_capital is not None and allocated_capital else None),
        "average_trade_size_dollars": reference_num_or_unknown(average_trade_size),
    }


def reference_avg(values: list[Decimal]) -> Decimal | None:
    if not values:
        return None
    return sum(values, Decimal("0")) / Decimal(len(values))


def reference_num(value: Decimal | int | float) -> int | float:
    if isinstance(value, Decimal):
        if not value.is_finite():
            return str(value)
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    return value


def reference_num_or_unknown(value):
    return "UNKNOWN" if value is None else reference_num(value)


def source_rows(symbol: str = "AAPL") -> list[dict]:
    prices = [100, 101, 99, 101, 102, 98, 103, 97, 104]
    return [
        {
            "timestamp": f"2026-07-20T13:30:0{index}.100Z",
            "symbol": symbol,
            "price": price,
            "size": 1,
            "exchange": "unit",
            "tape": "C",
            "trade_id": str(index),
            "conditions": ["@"],
            "source_sequence": index,
        }
        for index, price in enumerate(prices)
    ]


def price_source_rows(prices: list[float | int], symbol: str = "AAPL") -> list[dict]:
    return [
        {
            "timestamp": f"2026-07-20T13:31:{index:02d}.100Z",
            "symbol": symbol,
            "price": price,
            "size": 1,
            "exchange": "unit",
            "tape": "C",
            "trade_id": str(index),
            "conditions": ["@"],
            "source_sequence": index,
        }
        for index, price in enumerate(prices)
    ]


def config_payload() -> dict:
    return {
        "traders": [
            {
                "id": "aapl_renko",
                "enabled": True,
                "symbol": "AAPL",
                "strategy_name": "aapl_renko_test",
                "strategy": {
                    "type": "renko_reversal",
                    "brick_size": 1.0,
                    "entry_bricks": 1,
                    "brick_sizing_mode": "fixed_dollars",
                    "allow_short": True,
                    "renko_anchor_mode": "grid",
                    "renko_anchor_price": 0.0,
                },
                "sizing": {"qty": 1},
                "session": {"entry_start": "09:31:00", "entry_end": "15:59:59", "allow_overnight": True},
                "data": {"renko_source": "1s_close"},
                "profile": {"renko_brick_pct": 1.0, "ltp_reference": 100},
            },
            {
                "id": "qqq_chop",
                "enabled": True,
                "symbol": "QQQ",
                "strategy": {
                    "type": "chop_mean_reversion",
                    "window_seconds": 3600,
                    "entry_z": 3,
                    "stop_z": 5,
                    "efficiency_max": 0.35,
                    "trend_exit_efficiency": 0.5,
                    "max_hold_seconds": 600,
                },
                "sizing": {"qty": 1},
                "session": {},
            },
            {
                "id": "disabled_tsla",
                "enabled": False,
                "symbol": "TSLA",
                "strategy": {"type": "renko_reversal", "brick_size": 2, "entry_bricks": 1},
                "sizing": {"qty": 1},
            },
        ]
    }


class OptimizationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.config_path = self.root / "traders.json"
        self.config_path.write_text(json.dumps(config_payload()), encoding="utf-8")
        self.source_root = self.root / "bronze"
        self.source_path = self.source_root / "AAPL" / "2026-07-20" / "sip_trades.jsonl"
        write_jsonl(self.source_path, source_rows("AAPL"))

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def opt_config(
        self,
        *,
        preflight_only: bool = False,
        candidate_pcts=("1.00", "1.01", "1.02"),
        position_sizing_mode: str = "PRODUCTION_CONFIG_QUANTITY_V1",
        parameter_grid_mode: str = "BRICK_ONLY_V1",
        initial_capital: str = "10000",
        process_count: int | str = 1,
        candidate_batch_size: int = 1,
        resume: bool = False,
        retry_failed: bool = False,
        selected_entry_confirmations=None,
        selected_take_profit_bricks=None,
        selected_stop_loss_bricks=None,
        include_current_production: bool = True,
    ) -> OptimizationConfig:
        return OptimizationConfig(
            optimization_id="opt-test",
            production_config_path=str(self.config_path),
            session_dates=SESSIONS,
            source_root=str(self.source_root),
            prepared_output_root=str(self.root / "prepared"),
            output_directory=str(self.root / "optimizations"),
            candidate_pcts=None if candidate_pcts is None else tuple(Decimal(item) for item in candidate_pcts),
            preflight_only=preflight_only,
            minimum_closed_trades=1,
            position_sizing_mode=position_sizing_mode,
            parameter_grid_mode=parameter_grid_mode,
            initial_capital=initial_capital,
            process_count=process_count,
            candidate_batch_size=candidate_batch_size,
            checkpoint_every_candidates=1,
            progress_every_seconds=1,
            resume=resume,
            retry_failed=retry_failed,
            selected_entry_confirmations=selected_entry_confirmations,
            selected_take_profit_bricks=selected_take_profit_bricks,
            selected_stop_loss_bricks=selected_stop_loss_bricks,
            include_current_production=include_current_production,
        )

    def test_remote_source_path_construction(self) -> None:
        self.assertEqual(remote_source_path("/root/ticks", "aapl", date(2026, 7, 20)), "/root/ticks/2026-07-20/AAPL.jsonl")

    def test_stage_data_dry_run_performs_no_scp(self) -> None:
        calls = []
        result = stage_sources(
            StageRequest(host="droplet", symbols=("AAPL",), sessions=SESSIONS, output_root=str(self.root), dry_run=True),
            runner=lambda *args, **kwargs: calls.append(args),
        )
        self.assertEqual(calls, [])
        self.assertEqual(result.skipped, 1)

    def test_existing_identical_local_file_is_reused(self) -> None:
        from optimization.source_inventory import inspect_source_file, write_source_metadata

        info = inspect_source_file(self.source_path, symbol="AAPL", session=SESSIONS[0])
        write_source_metadata(info, remote_source_path="/remote/AAPL.jsonl")
        result = stage_sources(
            StageRequest(host="droplet", symbols=("AAPL",), sessions=SESSIONS, output_root=str(self.source_root)),
            runner=mock.Mock(side_effect=AssertionError("scp should not run")),
        )
        self.assertEqual(result.reused, 1)

    def test_existing_different_local_file_is_not_silently_overwritten(self) -> None:
        with self.assertRaises(FileExistsError):
            stage_sources(StageRequest(host="droplet", symbols=("AAPL",), sessions=SESSIONS, output_root=str(self.source_root)))

    def test_stage_metadata_hash_and_row_counts(self) -> None:
        target_root = self.root / "stage"

        def fake_runner(command, check):
            Path(command[-1]).write_text(self.source_path.read_text(encoding="utf-8"), encoding="utf-8")

        result = stage_sources(StageRequest(host="droplet", symbols=("AAPL",), sessions=SESSIONS, output_root=str(target_root)), runner=fake_runner)
        info = result.files[0]
        self.assertEqual(info.row_count, len(source_rows()))
        self.assertTrue(info.sha256)
        self.assertTrue(Path(info.path).with_suffix(".metadata.json").exists())

    def test_incomplete_copy_cleanup(self) -> None:
        target_root = self.root / "broken"

        def fake_runner(command, check):
            Path(command[-1]).write_text("partial", encoding="utf-8")
            raise RuntimeError("copy failed")

        with self.assertRaises(RuntimeError):
            stage_sources(StageRequest(host="droplet", symbols=("AAPL",), sessions=SESSIONS, output_root=str(target_root)), runner=fake_runner)
        self.assertFalse((target_root / "AAPL" / "2026-07-20" / "sip_trades.jsonl.tmp").exists())

    def test_no_order_execution_imports_in_staging_module(self) -> None:
        text = Path("optimization/source_inventory.py").read_text(encoding="utf-8")
        self.assertNotIn("alpaca", text.lower())

    def test_raw_sip_is_filtered_sampled_once_per_preparation(self) -> None:
        with mock.patch("optimization.source_preparation.stream_historical_source", wraps=__import__("optimization.source_preparation").source_preparation.stream_historical_source) as wrapped:
            prepare_source(
                symbol="AAPL",
                trader_id="aapl_renko",
                session_dates=SESSIONS,
                source_root=str(self.source_root),
                output_root=str(self.root / "prepared"),
                run_id="prep",
            )
        self.assertEqual(wrapped.call_count, 1)

    def test_prepared_observations_match_canonical_sampler_output(self) -> None:
        prepared = prepare_source(
            symbol="AAPL",
            trader_id="aapl_renko",
            session_dates=SESSIONS,
            source_root=str(self.source_root),
            output_root=str(self.root / "prepared"),
            run_id="prep",
        )
        observations = load_prepared_observations(prepared.observations_path)
        self.assertEqual(prepared.observation_count, len(observations))
        self.assertGreater(len(observations), 0)
        self.assertEqual(observations[0].symbol, "AAPL")

    def test_cache_key_changes_when_source_hash_changes(self) -> None:
        first = prepare_source(symbol="AAPL", trader_id="aapl_renko", session_dates=SESSIONS, source_root=str(self.source_root), output_root=str(self.root / "prepared"), run_id="prep")
        write_jsonl(self.source_path, source_rows("AAPL") + [source_rows("AAPL")[-1] | {"trade_id": "extra", "source_sequence": 999}])
        second = prepare_source(symbol="AAPL", trader_id="aapl_renko", session_dates=SESSIONS, source_root=str(self.source_root), output_root=str(self.root / "prepared"), run_id="prep")
        self.assertNotEqual(first.preparation_id, second.preparation_id)

    def test_cache_reuse_when_semantic_inputs_match(self) -> None:
        first = prepare_source(symbol="AAPL", trader_id="aapl_renko", session_dates=SESSIONS, source_root=str(self.source_root), output_root=str(self.root / "prepared"), run_id="prep")
        second = prepare_source(symbol="AAPL", trader_id="aapl_renko", session_dates=SESSIONS, source_root=str(self.source_root), output_root=str(self.root / "prepared"), run_id="prep")
        self.assertEqual(first.preparation_id, second.preparation_id)
        self.assertTrue(second.reused)

    def test_cache_key_changes_when_session_policy_changes(self) -> None:
        first = prepare_source(symbol="AAPL", trader_id="aapl_renko", session_dates=SESSIONS, source_root=str(self.source_root), output_root=str(self.root / "prepared"), run_id="prep")
        shifted_window = (
            SessionWindow(
                session_date=SESSIONS[0],
                start_utc=datetime(2026, 7, 20, 13, 31, tzinfo=timezone.utc),
                end_utc=datetime(2026, 7, 20, 20, 0, tzinfo=timezone.utc),
            ),
        )
        second = prepare_source(
            symbol="AAPL",
            trader_id="aapl_renko",
            session_dates=SESSIONS,
            source_root=str(self.source_root),
            output_root=str(self.root / "prepared"),
            run_id="prep",
            session_windows=shifted_window,
        )
        self.assertNotEqual(first.preparation_id, second.preparation_id)

    def test_raw_source_is_referenced_not_copied_into_preparation(self) -> None:
        prepared = prepare_source(symbol="AAPL", trader_id="aapl_renko", session_dates=SESSIONS, source_root=str(self.source_root), output_root=str(self.root / "prepared"), run_id="prep")
        names = {path.name for path in Path(prepared.output_directory).iterdir()}
        self.assertNotIn("sip_trades.jsonl", names)
        self.assertIn("observations_1s.jsonl", names)

    def test_exact_296_decimal_grid_values(self) -> None:
        values = brick_grid(Decimal("0.05"), Decimal("3.00"), Decimal("0.01"))
        self.assertEqual(len(values), 296)
        self.assertEqual(values[0], Decimal("0.05"))
        self.assertEqual(values[-1], Decimal("3.00"))
        self.assertEqual(values[1], Decimal("0.06"))

    def test_current_production_candidate_added_separately(self) -> None:
        prepared = prepare_source(symbol="AAPL", trader_id="aapl_renko", session_dates=SESSIONS, source_root=str(self.source_root), output_root=str(self.root / "prepared"), run_id="prep")
        trader = load_enabled_renko_traders(str(self.config_path), ("all-enabled-renko",))[0]
        specs = build_candidate_specs(config=self.opt_config(candidate_pcts=("1.00",)), trader=trader, prepared=prepared, observations=load_prepared_observations(prepared.observations_path))
        self.assertEqual([spec.candidate_type for spec in specs], ["BRICK_PERCENT_GRID", "CURRENT_PRODUCTION"])

    def test_candidates_do_not_share_state_and_are_deterministic(self) -> None:
        result = run_optimization(self.opt_config(candidate_pcts=("1.00", "2.00")))
        second = run_optimization(self.opt_config(candidate_pcts=("1.00", "2.00")))
        self.assertEqual(result.candidate_rows, second.candidate_rows)
        grid = [row for row in result.candidate_rows if row["candidate_type"] == "BRICK_PERCENT_GRID"]
        self.assertEqual(len(grid), 2)
        self.assertNotEqual(grid[0]["candidate_id"], grid[1]["candidate_id"])

    def test_existing_production_renko_strategy_engine_is_used(self) -> None:
        with mock.patch("optimization.runner.RenkoStrategyEngine", wraps=RenkoStrategyEngine) as wrapped:
            run_optimization(self.opt_config(candidate_pcts=("1.00",)))
        self.assertGreaterEqual(wrapped.call_count, 1)

    def test_canonical_execution_and_accounting_are_used(self) -> None:
        with mock.patch("optimization.runner.NextObservationExecution", wraps=NextObservationExecution) as execution:
            with mock.patch("optimization.runner.AccountingLedger", wraps=AccountingLedger) as accounting:
                run_optimization(self.opt_config(candidate_pcts=("1.00",)))
        self.assertGreaterEqual(execution.call_count, 1)
        self.assertGreaterEqual(accounting.call_count, 1)

    def test_no_alternative_pnl_implementation_exists(self) -> None:
        text = "\n".join(path.read_text(encoding="utf-8") for path in Path("optimization").glob("*.py"))
        self.assertNotIn("realized_pnl +=", text)
        self.assertNotIn("gross_profit +=", text)

    def test_candidate_rows_use_explicit_units_and_unknowns(self) -> None:
        result = run_optimization(self.opt_config(candidate_pcts=("1.00",)))
        row = result.candidate_rows[0]
        self.assertIn("net_pnl_dollars", row)
        self.assertIn("strategy_return_pct", row)
        self.assertEqual(row["slippage_bps"], "UNKNOWN")
        self.assertEqual(row["fees_dollars"], "UNKNOWN")

    def test_buy_hold_and_drawdown_come_from_canonical_metrics(self) -> None:
        result = run_optimization(self.opt_config(candidate_pcts=("1.00",)))
        row = result.candidate_rows[0]
        self.assertIn("buy_hold_pnl_dollars", row)
        self.assertIn("max_drawdown_dollars", row)

    def test_current_production_baseline_preserves_fixed_dollar_brick(self) -> None:
        result = run_optimization(self.opt_config(candidate_pcts=("1.00",)))
        current = [row for row in result.candidate_rows if row["candidate_type"] == "CURRENT_PRODUCTION"][0]
        self.assertEqual(current["brick_size_dollars"], 1)
        self.assertEqual(current["reference_price_policy"], "CURRENT_PRODUCTION_FIXED_DOLLARS")

    def test_whole_share_floor_quantity_and_unused_cash(self) -> None:
        result = run_optimization(
            self.opt_config(candidate_pcts=("1.00",), position_sizing_mode="CAPITAL_ALLOCATED_WHOLE_SHARES_V1", initial_capital="10050")
        )
        row = result.candidate_rows[0]
        self.assertEqual(row["position_sizing_mode"], "CAPITAL_ALLOCATED_WHOLE_SHARES_V1")
        self.assertEqual(row["production_config_quantity"], 1)
        self.assertEqual(row["research_quantity"], 100)
        self.assertEqual(row["quantity"], 100)
        self.assertEqual(row["allocated_capital_dollars"], 10000)
        self.assertEqual(row["unused_cash_dollars"], 50)
        self.assertLessEqual(Decimal(str(row["allocated_capital_dollars"])), Decimal("10050"))

    def test_candidate_replay_receives_research_quantity_without_mutating_config(self) -> None:
        before = self.config_path.read_text(encoding="utf-8")
        result = run_optimization(self.opt_config(candidate_pcts=("1.00",), position_sizing_mode="CAPITAL_ALLOCATED_WHOLE_SHARES_V1"))
        after = self.config_path.read_text(encoding="utf-8")
        self.assertEqual(before, after)
        row = result.candidate_rows[0]
        self.assertEqual(row["research_quantity"], 100)
        self.assertEqual(row["benchmark_quantity"], 100)

    def test_buy_hold_uses_same_whole_share_quantity_and_unused_cash(self) -> None:
        result = run_optimization(
            self.opt_config(candidate_pcts=("1.00",), position_sizing_mode="CAPITAL_ALLOCATED_WHOLE_SHARES_V1", initial_capital="10050")
        )
        row = result.candidate_rows[0]
        self.assertEqual(row["benchmark_quantity"], row["research_quantity"])
        self.assertEqual(row["benchmark_entry_value_dollars"], row["allocated_capital_dollars"])
        self.assertEqual(row["buy_hold_ending_equity_dollars"], row["unused_cash_dollars"] + row["benchmark_exit_value_dollars"])

    def test_capital_metric_fields_are_populated(self) -> None:
        result = run_optimization(self.opt_config(candidate_pcts=("1.00",), position_sizing_mode="CAPITAL_ALLOCATED_WHOLE_SHARES_V1"))
        row = result.candidate_rows[0]
        for field in (
            "average_deployed_capital_dollars",
            "maximum_deployed_capital_dollars",
            "minimum_deployed_capital_dollars",
            "average_cash_balance_dollars",
            "minimum_cash_balance_dollars",
            "average_position_value_dollars",
            "maximum_position_value_dollars",
            "average_exposure_pct",
            "maximum_exposure_pct",
            "capital_utilization_pct",
            "pnl_per_share_dollars",
            "return_on_allocated_capital_pct",
            "return_on_total_account_equity_pct",
            "turnover_dollars",
            "turnover_multiple",
            "average_trade_size_dollars",
        ):
            self.assertIn(field, row)

    def test_capital_metrics_flat_and_long_short_deployed_capital(self) -> None:
        snapshot = AccountingSnapshot(
            position=PositionState.flat("AAPL"),
            realized_pnl=Decimal("10"),
            unrealized_pnl=Decimal("0"),
            net_pnl=Decimal("10"),
            equity=Decimal("10010"),
        )
        observations = load_prepared_observations(
            prepare_source(
                symbol="AAPL",
                trader_id="aapl_renko",
                session_dates=SESSIONS,
                source_root=str(self.source_root),
                output_root=str(self.root / "prepared"),
                run_id="prep",
            ).observations_path
        )
        metrics = build_metrics(
            snapshot=snapshot,
            initial_capital=Decimal("10000"),
            observations=list(observations),
            order_latencies=[],
            benchmark_quantity=Decimal("100"),
            allocated_capital=Decimal("10000"),
            unused_cash=Decimal("0"),
            position_samples=[
                {"side": "flat", "qty": Decimal("0"), "mark_price": Decimal("100"), "equity": Decimal("10000")},
                {"side": "long", "qty": Decimal("100"), "mark_price": Decimal("101"), "equity": Decimal("10010")},
                {"side": "short", "qty": Decimal("100"), "mark_price": Decimal("99"), "equity": Decimal("10020")},
            ],
            fills=[
                NormalizedFill(timestamp="2026-07-20T13:30:01Z", symbol="AAPL", trader_id="t", side="buy", qty=100, price=101, leg="open"),
                NormalizedFill(timestamp="2026-07-20T13:30:02Z", symbol="AAPL", trader_id="t", side="sell", qty=100, price=102, leg="close"),
            ],
        )
        self.assertEqual(metrics["flat_exposure_pct"], 100 / 3)
        self.assertEqual(metrics["long_exposure_pct"], 100 / 3)
        self.assertEqual(metrics["short_exposure_pct"], 100 / 3)
        self.assertEqual(metrics["turnover_dollars"], 20300)
        self.assertEqual(metrics["average_trade_size_dollars"], 10150)

    def test_capital_metrics_one_pass_matches_reference_semantics(self) -> None:
        fills = [
            NormalizedFill(timestamp="2026-07-20T13:30:01Z", symbol="AAPL", trader_id="t", side="buy", qty="1.25", price="100.125", leg="open"),
            NormalizedFill(timestamp="2026-07-20T13:30:02Z", symbol="AAPL", trader_id="t", side="sell", qty="1.25", price="100.875", leg="close"),
            NormalizedFill(timestamp="2026-07-20T13:30:03Z", symbol="AAPL", trader_id="t", side="sell_short", qty="0.75", price="99.333333333333333333", leg="open"),
        ]
        cases = [
            {
                "name": "empty samples",
                "samples": [],
                "net_pnl": Decimal("0"),
                "quantity": None,
                "allocated_capital": None,
                "fills": [],
            },
            {
                "name": "one sample",
                "samples": [{"side": "long", "qty": Decimal("2"), "mark_price": Decimal("101.25"), "equity": Decimal("10012.50")}],
                "net_pnl": Decimal("12.50"),
            },
            {
                "name": "all flat samples",
                "samples": [
                    {"side": "flat", "qty": Decimal("0"), "mark_price": Decimal("100"), "equity": Decimal("10000")},
                    {"side": "", "qty": Decimal("0"), "mark_price": Decimal("101"), "equity": Decimal("10001")},
                    {"side": None, "qty": Decimal("0"), "mark_price": Decimal("102"), "equity": Decimal("9999")},
                ],
                "net_pnl": Decimal("-1.00"),
            },
            {
                "name": "all long samples",
                "samples": [
                    {"side": "long", "qty": Decimal("3"), "mark_price": Decimal("100"), "equity": Decimal("10000")},
                    {"side": "long", "qty": Decimal("4"), "mark_price": Decimal("101"), "equity": Decimal("10008")},
                ],
                "net_pnl": Decimal("8"),
            },
            {
                "name": "all short samples",
                "samples": [
                    {"side": "short", "qty": Decimal("-3"), "mark_price": Decimal("100"), "equity": Decimal("10000")},
                    {"side": "short", "qty": Decimal("-5"), "mark_price": Decimal("99.5"), "equity": Decimal("10020")},
                ],
                "net_pnl": Decimal("20"),
            },
            {
                "name": "alternating long short flat",
                "samples": [
                    {"side": "long", "qty": Decimal("7"), "mark_price": Decimal("100.10"), "equity": Decimal("10010")},
                    {"side": "short", "qty": Decimal("-6"), "mark_price": Decimal("99.90"), "equity": Decimal("10015")},
                    {"side": "flat", "qty": Decimal("0"), "mark_price": Decimal("100.00"), "equity": Decimal("10012")},
                    {"side": "long", "qty": Decimal("2"), "mark_price": Decimal("101.333"), "equity": Decimal("10021.5")},
                ],
                "net_pnl": Decimal("-15.25"),
            },
            {
                "name": "varying equity cash and deployed capital",
                "samples": [
                    {"side": "long", "qty": Decimal("10.5"), "mark_price": Decimal("98.125"), "equity": Decimal("9980.75")},
                    {"side": "short", "qty": Decimal("-8.25"), "mark_price": Decimal("102.875"), "equity": Decimal("10050.125")},
                    {"side": "flat", "qty": Decimal("0"), "mark_price": Decimal("101.5"), "equity": Decimal("9990.625")},
                ],
                "net_pnl": Decimal("50.125"),
                "allocated_capital": Decimal("8750.25"),
            },
            {
                "name": "decimal edge cases with many fractional digits",
                "samples": [
                    {
                        "side": "long",
                        "qty": Decimal("0.123456789123456789"),
                        "mark_price": Decimal("123.456789987654321"),
                        "equity": Decimal("10000.000000000000001"),
                    },
                    {
                        "side": "short",
                        "qty": Decimal("-0.987654321987654321"),
                        "mark_price": Decimal("98.7654321123456789"),
                        "equity": Decimal("9999.999999999999999"),
                    },
                ],
                "net_pnl": Decimal("0.123456789123456789"),
                "quantity": Decimal("0.123456789123456789"),
            },
            {
                "name": "large synthetic sample set",
                "samples": [
                    {
                        "side": ("long" if index % 3 == 0 else "short" if index % 3 == 1 else "flat"),
                        "qty": Decimal("0") if index % 3 == 2 else Decimal(index % 17 + 1) / Decimal("7") * (Decimal("-1") if index % 3 == 1 else Decimal("1")),
                        "mark_price": Decimal("100") + Decimal(index % 29) / Decimal("13"),
                        "equity": Decimal("10000") + Decimal(index % 101) / Decimal("9"),
                    }
                    for index in range(5000)
                ],
                "net_pnl": Decimal("123.4567"),
                "allocated_capital": Decimal("9999.99"),
            },
        ]
        for case in cases:
            with self.subTest(case["name"]):
                kwargs = {
                    "initial_capital": Decimal("10000"),
                    "allocated_capital": case.get("allocated_capital", Decimal("9000.50")),
                    "unused_cash": Decimal("999.50"),
                    "quantity": case.get("quantity", Decimal("10")),
                    "net_pnl": case["net_pnl"],
                    "position_samples": case["samples"],
                    "fills": case.get("fills", fills),
                }
                self.assertEqual(build_capital_metrics(**kwargs), reference_capital_metrics(**kwargs))

    def test_capital_allocated_pnl_scales_from_one_share_run(self) -> None:
        legacy = run_optimization(self.opt_config(candidate_pcts=("1.00",))).candidate_rows[0]
        capital = run_optimization(self.opt_config(candidate_pcts=("1.00",), position_sizing_mode="CAPITAL_ALLOCATED_WHOLE_SHARES_V1")).candidate_rows[0]
        self.assertEqual(capital["research_quantity"], 100)
        self.assertAlmostEqual(capital["net_pnl_dollars"], legacy["net_pnl_dollars"] * 100)

    def test_exact_281_value_v2_grid_and_846_row_total(self) -> None:
        values = brick_grid(Decimal("0.20"), Decimal("3.00"), Decimal("0.01"))
        self.assertEqual(len(values), 281)
        self.assertEqual(values[0], Decimal("0.20"))
        self.assertEqual(values[-1], Decimal("3.00"))
        self.assertEqual(3 * (len(values) + 1), 846)

    def test_pr8b_parameter_grid_and_total_candidate_count(self) -> None:
        config = self.opt_config(
            candidate_pcts=None,
            position_sizing_mode="CAPITAL_ALLOCATED_WHOLE_SHARES_V1",
            parameter_grid_mode="RENKO_TP_SL_ENTRY_TP_SL_GRID_V1",
        )
        values = brick_grid(Decimal("0.20"), Decimal("3.00"), Decimal("0.01"))
        parameters = strategy_parameter_grid_for(config)
        self.assertEqual(len(values), 281)
        self.assertEqual(len(parameters), 18)
        self.assertEqual({parameter.entry_bricks for parameter in parameters}, {1, 2})
        self.assertEqual({parameter.take_profit_bricks for parameter in parameters}, {1, 2, 3})
        self.assertEqual({parameter.stop_loss_bricks for parameter in parameters}, {1, 2, 3})
        self.assertEqual(3 * (len(values) * len(parameters) + 1), 15177)

    def test_filtered_tp_sl_candidate_grid_supports_exact_audit_rerun(self) -> None:
        result = run_optimization(
            self.opt_config(
                candidate_pcts=("0.27",),
                position_sizing_mode="CAPITAL_ALLOCATED_WHOLE_SHARES_V1",
                parameter_grid_mode="RENKO_TP_SL_ENTRY_TP_SL_GRID_V1",
                selected_entry_confirmations=("E1",),
                selected_take_profit_bricks=(2,),
                selected_stop_loss_bricks=(3,),
                include_current_production=False,
            )
        )
        self.assertEqual(len(result.candidate_rows), 1)
        row = result.candidate_rows[0]
        self.assertEqual(row["entry_confirmation"], "E1")
        self.assertEqual(row["take_profit_bricks"], 2)
        self.assertEqual(row["stop_loss_bricks"], 3)
        self.assertEqual(row["candidate_type"], "BRICK_PERCENT_GRID")

    def test_filtered_tp_sl_grid_supports_e3_entry_confirmation(self) -> None:
        result = run_optimization(
            self.opt_config(
                candidate_pcts=("0.27",),
                position_sizing_mode="CAPITAL_ALLOCATED_WHOLE_SHARES_V1",
                parameter_grid_mode="RENKO_TP_SL_ENTRY_TP_SL_GRID_V1",
                selected_entry_confirmations=("E3",),
                selected_take_profit_bricks=(2,),
                selected_stop_loss_bricks=(3,),
                include_current_production=False,
            )
        )
        self.assertEqual(len(result.candidate_rows), 1)
        row = result.candidate_rows[0]
        self.assertEqual(row["entry_confirmation"], "E3")
        self.assertEqual(row["entry_bricks"], 3)
        self.assertEqual(row["take_profit_bricks"], 2)
        self.assertEqual(row["stop_loss_bricks"], 3)
        self.assertEqual(row["candidate_type"], "BRICK_PERCENT_GRID")

    def test_pr8b_reversal_baseline_tp_sl_not_applicable(self) -> None:
        result = run_optimization(
            self.opt_config(
                candidate_pcts=("1.00",),
                position_sizing_mode="CAPITAL_ALLOCATED_WHOLE_SHARES_V1",
                parameter_grid_mode="RENKO_TP_SL_ENTRY_TP_SL_GRID_V1",
            )
        )
        current = [row for row in result.candidate_rows if row["candidate_type"] == "CURRENT_PRODUCTION_REVERSAL_CAPITAL_ALLOCATED"][0]
        self.assertEqual(current["strategy_type"], "renko_reversal")
        self.assertEqual(current["strategy_family"], "DIRECT_REVERSAL_BASELINE")
        self.assertEqual(current["take_profit_confirmation"], "NOT_APPLICABLE")
        self.assertEqual(current["stop_loss_confirmation"], "NOT_APPLICABLE")
        self.assertEqual(current["take_profit_bricks"], "UNKNOWN")
        self.assertEqual(current["stop_loss_bricks"], "UNKNOWN")
        self.assertTrue(current["direct_reversal_enabled"])
        self.assertFalse(current["exits_flat_before_reentry"])

    def test_pr8b_every_grid_candidate_uses_renko_tp_sl(self) -> None:
        result = run_optimization(
            self.opt_config(
                candidate_pcts=("1.00",),
                position_sizing_mode="CAPITAL_ALLOCATED_WHOLE_SHARES_V1",
                parameter_grid_mode="RENKO_TP_SL_ENTRY_TP_SL_GRID_V1",
            )
        )
        grid = [row for row in result.candidate_rows if row["candidate_type"] == "BRICK_PERCENT_GRID"]
        self.assertEqual(len(grid), 18)
        self.assertTrue(all(row["strategy_type"] == "renko_tp_sl" for row in grid))
        self.assertTrue(all(row["strategy_family"] == "FLATTENING_TP_SL_STRATEGY" for row in grid))
        self.assertTrue(all(row["direct_reversal_enabled"] is False for row in grid))
        self.assertTrue(all(row["exits_flat_before_reentry"] is True for row in grid))
        self.assertEqual(len({row["candidate_id"] for row in result.candidate_rows}), 19)

    def test_pr8b_tp_sl_fields_change_tp_sl_identity_not_reversal_identity(self) -> None:
        prepared = prepare_source(symbol="AAPL", trader_id="aapl_renko", session_dates=SESSIONS, source_root=str(self.source_root), output_root=str(self.root / "prepared"), run_id="prep")
        observations = load_prepared_observations(prepared.observations_path)
        trader = load_enabled_renko_traders(str(self.config_path), ("all-enabled-renko",))[0]
        specs = build_candidate_specs(
            config=self.opt_config(
                candidate_pcts=("1.00",),
                position_sizing_mode="CAPITAL_ALLOCATED_WHOLE_SHARES_V1",
                parameter_grid_mode="RENKO_TP_SL_ENTRY_TP_SL_GRID_V1",
            ),
            trader=trader,
            prepared=prepared,
            observations=observations,
        )
        current = [spec for spec in specs if spec.candidate_type == "CURRENT_PRODUCTION_REVERSAL_CAPITAL_ALLOCATED"][0]
        grid_ids = {spec.candidate_id for spec in specs if spec.candidate_type == "BRICK_PERCENT_GRID"}
        self.assertEqual(len(grid_ids), 18)
        self.assertEqual(current.strategy_parameters.identity_parts(), ("DIRECT_REVERSAL_BASELINE", "renko_reversal", 1))

    def test_tp_sl_batches_keep_all_parameter_variants_for_a_brick_together(self) -> None:
        prepared = prepare_source(symbol="AAPL", trader_id="aapl_renko", session_dates=SESSIONS, source_root=str(self.source_root), output_root=str(self.root / "prepared"), run_id="prep")
        observations = load_prepared_observations(prepared.observations_path)
        trader = load_enabled_renko_traders(str(self.config_path), ("all-enabled-renko",))[0]
        config = self.opt_config(
            candidate_pcts=("1.00", "1.02"),
            position_sizing_mode="CAPITAL_ALLOCATED_WHOLE_SHARES_V1",
            parameter_grid_mode="RENKO_TP_SL_ENTRY_TP_SL_GRID_V1",
            include_current_production=False,
            candidate_batch_size=25,
        )
        specs = list(build_candidate_specs(config=config, trader=trader, prepared=prepared, observations=observations))
        batches = _candidate_batches(config=config, specs=specs)
        self.assertEqual([len(batch) for batch in batches], [18, 18])
        self.assertEqual(len({spec.brick_pct for spec in batches[0]}), 1)
        self.assertEqual(len({spec.brick_pct for spec in batches[1]}), 1)

    def test_cached_renko_stream_path_matches_original_per_candidate_path(self) -> None:
        write_jsonl(self.source_path, price_source_rows([100, 101, 100, 99, 98, 97, 98, 99, 100, 101, 100, 99, 98, 97, 96]))
        prepared = prepare_source(symbol="AAPL", trader_id="aapl_renko", session_dates=SESSIONS, source_root=str(self.source_root), output_root=str(self.root / "prepared"), run_id="prep")
        observations = load_prepared_observations(prepared.observations_path)
        trader = load_enabled_renko_traders(str(self.config_path), ("all-enabled-renko",))[0]
        config = self.opt_config(
            candidate_pcts=("1.00", "1.50", "2.00"),
            parameter_grid_mode="RENKO_TP_SL_ENTRY_TP_SL_GRID_V1",
            selected_entry_confirmations=("E1", "E2"),
            selected_take_profit_bricks=(1, 2),
            selected_stop_loss_bricks=(1, 3),
            include_current_production=False,
        )
        specs = build_candidate_specs(config=config, trader=trader, prepared=prepared, observations=observations)
        selected = [
            specs[0],
            next(spec for spec in specs if spec.brick_pct == Decimal("1.50") and spec.strategy_parameters.entry_confirmation == "E2"),
            specs[-1],
        ]
        streams = {}
        for spec in selected:
            streams.setdefault(
                str(spec.brick_size_dollars),
                _build_cached_renko_stream(config=config, spec=spec, prepared=prepared, observations=observations),
            )
            uncached = _execute_candidate(
                config=config,
                spec=spec,
                prepared=prepared,
                observations=observations,
                use_cached_renko_stream=False,
            )
            cached = _execute_candidate(
                config=config,
                spec=spec,
                prepared=prepared,
                observations=observations,
                renko_stream=streams[str(spec.brick_size_dollars)],
            )
            self.assertEqual(cached, uncached)

    def test_pr8b_renko_strategy_behavior_distinctions(self) -> None:
        reversal = RenkoStrategyEngine(
            RenkoStrategyConfig(
                symbol="AAPL",
                strategy_type="renko_reversal",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=1,
                stop_loss_bricks=1,
                qty=1,
                renko_anchor_mode="grid",
                renko_anchor_price=0.0,
                allow_overnight=True,
            )
        )
        tp_sl = RenkoStrategyEngine(
            RenkoStrategyConfig(
                symbol="AAPL",
                strategy_type="renko_tp_sl",
                brick_size=1.0,
                entry_bricks=1,
                take_profit_bricks=1,
                stop_loss_bricks=1,
                qty=1,
                renko_anchor_mode="grid",
                renko_anchor_price=0.0,
                allow_overnight=True,
            )
        )
        prices = [100.0, 101.0, 99.0]
        reversal_actions = []
        tp_sl_actions = []
        for offset, price in enumerate(prices):
            tick = MarketTick(pd.Timestamp("2026-07-20T14:00:00Z") + pd.Timedelta(seconds=offset), "AAPL", price, 1)
            reversal_actions.extend(reversal.on_tick(tick)[1])
            tp_sl_actions.extend(tp_sl.on_tick(tick)[1])
        self.assertEqual(reversal_actions[-1][1].qty, 2)
        self.assertEqual(reversal_actions[-1][1].target_qty, 1)
        self.assertTrue(reversal.position is not None)
        self.assertEqual(tp_sl_actions[-1][0].signal_type, "exit")
        self.assertTrue(tp_sl_actions[-1][1].close_position)
        self.assertIsNotNone(tp_sl.position)
        self.assertEqual(tp_sl.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_EXIT_PENDING)

    def test_optimizer_tp_sl_candidate_completes_multiple_cycles_and_reenters_after_tp_and_sl(self) -> None:
        write_jsonl(self.source_path, price_source_rows([100, 101, 100, 99, 98, 97, 98, 99, 100, 101, 100, 99, 98, 97, 96]))
        result = run_optimization(
            self.opt_config(
                candidate_pcts=("1.00",),
                parameter_grid_mode="RENKO_TP_SL_ENTRY_TP_SL_GRID_V1",
                selected_entry_confirmations=("E1",),
                selected_take_profit_bricks=(1,),
                selected_stop_loss_bricks=(1,),
                include_current_production=False,
            )
        )
        row = result.candidate_rows[0]
        self.assertEqual(row["candidate_status"], "COMPLETED")
        self.assertEqual(row["closed_trades"], 3)
        self.assertEqual(row["strategy_signals"], 6)
        self.assertEqual(row["queued_orders"], 6)
        self.assertEqual(row["simulated_fills"], 6)
        self.assertEqual(row["pending_orders_at_end"], 0)
        self.assertEqual(row["max_position_quantity"], 1)
        self.assertEqual(row["take_profit_exit_count"], 2)
        self.assertEqual(row["stop_loss_exit_count"], 1)
        self.assertEqual(json.loads(row["tp_sl_exit_reason_counts"]), {"stop_loss_1_bricks": 1, "take_profit_1_bricks": 2})

        target = Path(str(row["selected_output_directory"]))
        self.assertTrue((target / "renko_bricks.jsonl").exists())
        signals = read_jsonl(target / "signals.jsonl")
        orders = read_jsonl(target / "orders.jsonl")
        fills = read_jsonl(target / "fills.jsonl")
        closed_trades = read_jsonl(target / "closed_trades.jsonl")
        transitions = read_jsonl(target / "position_transitions.jsonl")
        self.assertEqual([signal["signal_type"] for signal in signals], ["entry", "exit", "entry", "exit", "entry", "exit"])
        self.assertEqual(len({order["order_id"] for order in orders}), len(orders))
        self.assertTrue(all(Decimal(str(order["qty"])) > 0 for order in orders))
        self.assertTrue(all(order["intent"]["target_qty"] == 0 for order in orders if order["intent"]["close_position"]))
        self.assertEqual([trade["reason"] for trade in closed_trades], ["stop_loss_1_bricks", "take_profit_1_bricks", "take_profit_1_bricks"])

        orders_by_id = {order["order_id"]: order for order in orders}
        for fill in fills:
            order = orders_by_id[fill["order_id"]]
            self.assertNotEqual(fill["timestamp"], order["queued_at"])
            self.assertGreater(Decimal(str(fill["qty"])), Decimal("0"))
            if fill["leg"] == "close":
                self.assertIn(fill["reason"], {"take_profit_1_bricks", "stop_loss_1_bricks"})
        for transition in transitions:
            for key in ("before", "after"):
                qty = Decimal(str(transition[key]["qty"]))
                self.assertIn(qty, {Decimal("0"), Decimal("1.0")})

    def test_optimizer_invokes_tp_sl_close_lifecycle_hooks_at_submission_and_flat_fill(self) -> None:
        write_jsonl(self.source_path, price_source_rows([100, 101, 100, 99, 98, 97, 98, 99, 100, 101, 100, 99, 98, 97, 96]))

        class RecordingStrategy(RenkoStrategyEngine):
            instances: list["RecordingStrategy"] = []

            def __init__(self, config):
                super().__init__(config)
                self.close_submissions = []
                self.close_fills = []
                RecordingStrategy.instances.append(self)

            def mark_close_submitted(self, intent, response=None):
                self.close_submissions.append({"state_before": self.tp_sl_lifecycle_state, "reason": intent.reason})
                super().mark_close_submitted(intent, response=response)

            def mark_close_filled(self, *, timestamp=None, reason=None, order_id=None, source="reconciliation"):
                self.close_fills.append({"state_before": self.tp_sl_lifecycle_state, "reason": reason, "source": source})
                super().mark_close_filled(timestamp=timestamp, reason=reason, order_id=order_id, source=source)

        with mock.patch("optimization.runner.RenkoStrategyEngine", RecordingStrategy):
            result = run_optimization(
                self.opt_config(
                    candidate_pcts=("1.00",),
                    parameter_grid_mode="RENKO_TP_SL_ENTRY_TP_SL_GRID_V1",
                    selected_entry_confirmations=("E1",),
                    selected_take_profit_bricks=(1,),
                    selected_stop_loss_bricks=(1,),
                    include_current_production=False,
                )
            )
        row = result.candidate_rows[0]
        strategy = RecordingStrategy.instances[0]
        self.assertEqual(row["closed_trades"], 3)
        self.assertEqual(len(strategy.close_submissions), row["closed_trades"])
        self.assertEqual(len(strategy.close_fills), row["closed_trades"])
        self.assertTrue(all(item["state_before"] == TP_SL_LIFECYCLE_EXIT_PENDING for item in strategy.close_submissions))
        self.assertTrue(all(item["state_before"] == TP_SL_LIFECYCLE_CLOSE_SUBMITTED for item in strategy.close_fills))
        self.assertTrue(all(item["reason"] in {"take_profit_1_bricks", "stop_loss_1_bricks"} for item in strategy.close_fills))
        self.assertEqual(strategy.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_FLAT)

    def test_optimizer_already_flat_tp_sl_close_reconciles_without_zero_size_order_or_deadlock(self) -> None:
        class AlreadyFlatCloseStrategy:
            instances: list["AlreadyFlatCloseStrategy"] = []

            def __init__(self, config):
                self.config = config
                self.emitted = False
                self.tp_sl_lifecycle_state = TP_SL_LIFECYCLE_EXIT_PENDING
                self.filled_calls = []
                AlreadyFlatCloseStrategy.instances.append(self)

            def on_tick(self, tick):
                if self.emitted:
                    return [], []
                self.emitted = True
                signal = Signal(
                    signal_id="unstable",
                    signal_type="exit",
                    timestamp=tick.timestamp,
                    symbol=tick.symbol,
                    side="long",
                    broker_side="sell",
                    price=tick.price,
                    reason="take_profit_1_bricks",
                    strategy="fake",
                    trader_id="aapl_renko",
                )
                intent = OrderIntent(
                    client_order_id="unstable",
                    timestamp=tick.timestamp,
                    symbol=tick.symbol,
                    side="sell",
                    signal_type="exit",
                    order_type="market",
                    strategy="fake",
                    reason="take_profit_1_bricks",
                    qty=None,
                    target_qty=0,
                    close_position=True,
                    trader_id="aapl_renko",
                    signal_id=signal.signal_id,
                    lifecycle_state=TP_SL_LIFECYCLE_EXIT_PENDING,
                )
                return [], [(signal, intent)]

            def mark_close_filled(self, *, timestamp=None, reason=None, order_id=None, source="reconciliation"):
                self.filled_calls.append({"timestamp": timestamp, "reason": reason, "order_id": order_id, "source": source})
                self.tp_sl_lifecycle_state = TP_SL_LIFECYCLE_FLAT

            def mark_close_submitted(self, intent, response=None):
                raise AssertionError("already-flat close must not be submitted")

        write_jsonl(self.source_path, price_source_rows([100, 100, 100]))
        with mock.patch("optimization.runner.RenkoStrategyEngine", AlreadyFlatCloseStrategy):
            result = run_optimization(
                self.opt_config(
                    candidate_pcts=("1.00",),
                    parameter_grid_mode="RENKO_TP_SL_ENTRY_TP_SL_GRID_V1",
                    selected_entry_confirmations=("E1",),
                    selected_take_profit_bricks=(1,),
                    selected_stop_loss_bricks=(1,),
                    include_current_production=False,
                )
            )
        row = result.candidate_rows[0]
        strategy = AlreadyFlatCloseStrategy.instances[0]
        self.assertEqual(row["queued_orders"], 0)
        self.assertEqual(row["simulated_fills"], 0)
        self.assertEqual(row["closed_trades"], 0)
        self.assertEqual(row["pending_orders_at_end"], 0)
        self.assertEqual(len(strategy.filled_calls), 1)
        self.assertEqual(strategy.filled_calls[0]["reason"], "take_profit_1_bricks")
        self.assertIn("already_flat", strategy.filled_calls[0]["source"])
        self.assertEqual(strategy.tp_sl_lifecycle_state, TP_SL_LIFECYCLE_FLAT)

    def test_optimizer_tp_sl_repeat_is_deterministic_for_signals_fills_trades_pnl_and_metrics(self) -> None:
        write_jsonl(self.source_path, price_source_rows([100, 101, 102, 103, 102, 101, 100, 99, 98, 99, 100, 101, 102, 103]))
        config = self.opt_config(
            candidate_pcts=("1.00",),
            parameter_grid_mode="RENKO_TP_SL_ENTRY_TP_SL_GRID_V1",
            selected_entry_confirmations=("E1",),
            selected_take_profit_bricks=(1,),
            selected_stop_loss_bricks=(1,),
            include_current_production=False,
        )
        first = run_optimization(config)
        first_row = first.candidate_rows[0]
        first_target = Path(str(first_row["selected_output_directory"]))
        first_payload = {
            "row": first_row,
            "signals": read_jsonl(first_target / "signals.jsonl"),
            "fills": read_jsonl(first_target / "fills.jsonl"),
            "closed_trades": read_jsonl(first_target / "closed_trades.jsonl"),
        }
        second = run_optimization(config)
        second_row = second.candidate_rows[0]
        second_target = Path(str(second_row["selected_output_directory"]))
        second_payload = {
            "row": second_row,
            "signals": read_jsonl(second_target / "signals.jsonl"),
            "fills": read_jsonl(second_target / "fills.jsonl"),
            "closed_trades": read_jsonl(second_target / "closed_trades.jsonl"),
        }
        self.assertEqual(first_payload, second_payload)
        self.assertEqual(first_row["closed_trades"], 3)
        self.assertEqual(first_row["net_pnl_dollars"], 3)
        self.assertEqual(first_row["profit_factor"], "Infinity")

    def test_pr8b_brick_robustness_neighbors_keep_parameters_fixed(self) -> None:
        rows = []
        for pct in ("0.99", "1.00", "1.01"):
            for tp in (1, 2):
                rows.append(
                    {
                        "trader_id": "t",
                        "candidate_id": f"tp{tp}-{pct}",
                        "candidate_type": "BRICK_PERCENT_GRID",
                        "candidate_status": "COMPLETED",
                        "brick_pct": pct,
                        "entry_confirmation": "E1",
                        "take_profit_bricks": tp,
                        "stop_loss_bricks": 1,
                        "net_pnl_dollars": 10 if tp == 1 else 100,
                        "strategy_return_pct": 1,
                        "profit_factor": 2,
                        "closed_trades": 11,
                        "max_drawdown_pct": 1,
                        "strategy_outperformance_dollars": 1,
                    }
                )
        annotated = annotate_robustness(rows, minimum_closed_trades=10, max_drawdown_pct_threshold=Decimal("25"), local_profitable_pct_threshold=Decimal("60"))
        center = [row for row in annotated if row["candidate_id"] == "tp1-1.00"][0]
        self.assertEqual(center["local_neighbor_count"], 3)
        self.assertEqual(center["tp_parameter_neighbor_count"], 1)
        self.assertEqual(center["lower_neighbor_candidate_id"], "tp1-0.99")
        self.assertEqual(center["upper_neighbor_candidate_id"], "tp1-1.01")

    def test_current_production_capital_allocated_baseline_type(self) -> None:
        result = run_optimization(self.opt_config(candidate_pcts=("1.00",), position_sizing_mode="CAPITAL_ALLOCATED_WHOLE_SHARES_V1"))
        current = [row for row in result.candidate_rows if row["candidate_type"] == "CURRENT_PRODUCTION_CAPITAL_ALLOCATED"][0]
        self.assertEqual(current["brick_size_dollars"], 1)
        self.assertEqual(current["research_quantity"], 100)

    def test_e1_only_structured_config(self) -> None:
        traders = load_enabled_renko_traders(str(self.config_path), ("all-enabled-renko",))
        self.assertEqual([trader.symbol for trader in traders], ["AAPL"])
        self.assertTrue(all(trader.strategy_type == "renko_reversal" for trader in traders))
        self.assertTrue(all(int(trader.strategy_config["entry_bricks"]) == 1 for trader in traders))

    def test_neighborhood_boundaries_and_neighbors(self) -> None:
        rows = [
            {"trader_id": "t", "candidate_id": f"c{i}", "candidate_type": "BRICK_PERCENT_GRID", "candidate_status": "COMPLETED", "brick_pct": float(Decimal("1.00") + Decimal(i) / Decimal("100")), "net_pnl_dollars": i, "strategy_return_pct": i, "profit_factor": 2, "closed_trades": 11, "max_drawdown_pct": 1, "strategy_outperformance_dollars": i}
            for i in range(-5, 6)
        ]
        annotated = annotate_robustness(rows, minimum_closed_trades=10, max_drawdown_pct_threshold=Decimal("25"), local_profitable_pct_threshold=Decimal("60"))
        center = [row for row in annotated if row["brick_pct"] == 1.0][0]
        self.assertEqual(center["local_neighbor_count"], 11)
        self.assertTrue(center["lower_neighbor_candidate_id"])
        self.assertTrue(center["upper_neighbor_candidate_id"])

    def test_isolated_spike_and_stable_plateau_detection(self) -> None:
        rows = [
            {"trader_id": "t", "candidate_id": "low", "candidate_type": "BRICK_PERCENT_GRID", "candidate_status": "COMPLETED", "brick_pct": 0.99, "net_pnl_dollars": -1, "strategy_return_pct": -1, "profit_factor": 0, "closed_trades": 11, "max_drawdown_pct": 1, "strategy_outperformance_dollars": -1},
            {"trader_id": "t", "candidate_id": "spike", "candidate_type": "BRICK_PERCENT_GRID", "candidate_status": "COMPLETED", "brick_pct": 1.00, "net_pnl_dollars": 100, "strategy_return_pct": 1, "profit_factor": 2, "closed_trades": 11, "max_drawdown_pct": 1, "strategy_outperformance_dollars": 100},
            {"trader_id": "t", "candidate_id": "low2", "candidate_type": "BRICK_PERCENT_GRID", "candidate_status": "COMPLETED", "brick_pct": 1.01, "net_pnl_dollars": -1, "strategy_return_pct": -1, "profit_factor": 0, "closed_trades": 11, "max_drawdown_pct": 1, "strategy_outperformance_dollars": -1},
        ]
        center = [row for row in annotate_robustness(rows, minimum_closed_trades=10, max_drawdown_pct_threshold=Decimal("25"), local_profitable_pct_threshold=Decimal("60")) if row["candidate_id"] == "spike"][0]
        self.assertTrue(center["isolated_spike_flag"])
        self.assertFalse(center["robustness_eligible"])

    def test_minimum_trade_eligibility(self) -> None:
        row = {"trader_id": "t", "candidate_id": "c", "candidate_type": "BRICK_PERCENT_GRID", "candidate_status": "COMPLETED", "brick_pct": 1, "net_pnl_dollars": 10, "strategy_return_pct": 1, "profit_factor": 2, "closed_trades": 1, "max_drawdown_pct": 1, "strategy_outperformance_dollars": 1}
        annotated = annotate_robustness([row], minimum_closed_trades=10, max_drawdown_pct_threshold=Decimal("25"), local_profitable_pct_threshold=Decimal("60"))[0]
        self.assertIn("below_minimum_closed_trades", annotated["eligibility_reasons"])

    def test_recommendation_does_not_automatically_choose_highest_pnl(self) -> None:
        rows = [
            {"trader_id": "t", "candidate_id": "highest", "candidate_type": "BRICK_PERCENT_GRID", "candidate_status": "COMPLETED", "brick_pct": 1, "net_pnl_dollars": 100, "strategy_return_pct": 10, "return_on_allocated_capital_pct": 10, "profit_factor": 2, "closed_trades": 20, "max_drawdown_pct": 20, "robustness_eligible": True, "robustness_score": 0.1},
            {"trader_id": "t", "candidate_id": "robust", "candidate_type": "BRICK_PERCENT_GRID", "candidate_status": "COMPLETED", "brick_pct": 2, "net_pnl_dollars": 80, "strategy_return_pct": 8, "return_on_allocated_capital_pct": 8, "profit_factor": 2, "closed_trades": 20, "max_drawdown_pct": 5, "robustness_eligible": True, "robustness_score": 0.9},
            {"trader_id": "t", "candidate_id": "third", "candidate_type": "BRICK_PERCENT_GRID", "candidate_status": "COMPLETED", "brick_pct": 3, "net_pnl_dollars": 70, "strategy_return_pct": 7, "return_on_allocated_capital_pct": 7, "profit_factor": 2, "closed_trades": 20, "max_drawdown_pct": 7, "robustness_eligible": True, "robustness_score": 0.7},
        ]
        recommendations = build_recommendations(rows)
        self.assertEqual(recommendations["t"]["highest_net_pnl"]["candidate_id"], "highest")
        self.assertEqual(recommendations["t"]["recommended_robust"]["candidate_id"], "robust")
        self.assertEqual(len(recommendations["t"]["top_three_primary"]), 3)

    def test_sensitivity_index_and_execution_cost_warning(self) -> None:
        result = run_optimization(
            self.opt_config(candidate_pcts=("0.50", "1.00", "1.50"), position_sizing_mode="CAPITAL_ALLOCATED_WHOLE_SHARES_V1")
        )
        grid = [row for row in result.candidate_rows if row["candidate_type"] == "BRICK_PERCENT_GRID"]
        self.assertTrue(all(row["sensitivity_index"] != "UNKNOWN" for row in grid))
        self.assertTrue(all(row["sensitivity_classification"] in {"LOW", "MODERATE", "HIGH", "VERY_HIGH"} for row in grid))
        self.assertTrue(any(row["execution_cost_sensitive"] in {True, False} for row in grid))

    def test_output_row_counts_and_deterministic_ordering(self) -> None:
        result = run_optimization(self.opt_config(candidate_pcts=("1.00", "1.01", "1.02")))
        out = Path(result.output_directory)
        with (out / "all_candidates.csv").open(newline="") as handle:
            rows = list(csv.DictReader(handle))
        self.assertEqual(len(rows), 4)
        second = run_optimization(self.opt_config(candidate_pcts=("1.00", "1.01", "1.02")))
        self.assertEqual(result.candidate_rows, second.candidate_rows)
        self.assertTrue((out / "heatmap_data.csv").exists())
        self.assertTrue((out / "asset_summary.csv").exists())
        self.assertTrue((out / "top_three_by_asset.csv").exists())

    def test_lightweight_summary_output_no_full_audit(self) -> None:
        result = run_optimization(self.opt_config(candidate_pcts=("1.00",)))
        names = {path.name for path in Path(result.output_directory).iterdir()}
        self.assertIn("all_candidates.csv", names)
        self.assertNotIn("raw_trade_references.jsonl", names)

    def test_selected_trades_outputs_only_for_selected_candidates(self) -> None:
        result = run_optimization(self.opt_config(candidate_pcts=("0.50", "1.00", "1.50", "2.00")))
        selected = [row for row in result.candidate_rows if row.get("selected_output_directory")]
        self.assertGreaterEqual(len(selected), 1)
        self.assertLess(len(selected), len(result.candidate_rows))
        selected_root = Path(result.output_directory) / "selected_runs"
        self.assertTrue(selected_root.exists())
        for row in selected:
            target = Path(str(row["selected_output_directory"]))
            self.assertTrue((target / "summary.json").exists())
            self.assertTrue((target / "signals.jsonl").exists())
            self.assertTrue((target / "orders.jsonl").exists())
            self.assertTrue((target / "fills.jsonl").exists())
            self.assertTrue((target / "position_transitions.jsonl").exists())
            self.assertTrue((target / "closed_trades.jsonl").exists())
            self.assertTrue((target / "equity_curve.csv").exists())

    def test_recommendation_ids_exist_in_candidate_output(self) -> None:
        result = run_optimization(self.opt_config(candidate_pcts=("1.00", "1.01", "1.02")))
        ids = {row["candidate_id"] for row in result.candidate_rows}
        for payload in result.recommendations.values():
            for item in payload.values():
                if isinstance(item, dict) and item.get("candidate_id"):
                    self.assertIn(item["candidate_id"], ids)

    def test_cross_asset_percentages_are_not_summed(self) -> None:
        result = run_optimization(self.opt_config(candidate_pcts=("1.00",)))
        self.assertIn("Percent returns are averaged", result.cross_asset_summary["note"])

    def test_failed_candidate_does_not_abort_remaining_candidates(self) -> None:
        from optimization import runner

        original = runner._execute_candidate
        calls = {"count": 0}

        def fail_once_then_run(**kwargs):
            calls["count"] += 1
            if calls["count"] == 1:
                raise RuntimeError("boom")
            return original(**kwargs)

        with mock.patch("optimization.runner._execute_candidate", side_effect=fail_once_then_run):
            result = run_optimization(self.opt_config(candidate_pcts=("1.00",)))
        self.assertEqual(len(result.candidate_rows), 2)
        self.assertEqual(result.candidate_rows[0]["candidate_status"], "FAILED")
        self.assertEqual(result.candidate_rows[1]["candidate_status"], "COMPLETED")

    def test_source_files_are_not_reread_for_every_candidate(self) -> None:
        with mock.patch("optimization.runner.prepare_source", wraps=__import__("optimization.runner").runner.prepare_source) as wrapped:
            run_optimization(self.opt_config(candidate_pcts=("1.00", "1.01", "1.02")))
        self.assertEqual(wrapped.call_count, 1)

    def test_process_count_two_preserves_deterministic_output_order(self) -> None:
        serial = run_optimization(self.opt_config(candidate_pcts=("1.00", "1.01")))
        parallel = run_optimization(self.opt_config(candidate_pcts=("1.00", "1.01"), process_count=2))
        fields = ["candidate_type", "trader_id", "brick_pct", "candidate_status", "net_pnl_dollars", "configuration_hash", "replay_fingerprint"]
        self.assertEqual(
            [{field: row.get(field) for field in fields} for row in serial.candidate_rows],
            [{field: row.get(field) for field in fields} for row in parallel.candidate_rows],
        )

    def test_process_auto_resolves_conservative_worker_count(self) -> None:
        from optimization.runner import resolve_process_plan

        plan = resolve_process_plan(self.opt_config(process_count="auto"))
        self.assertGreaterEqual(plan["logical_cpu_count"], 1)
        self.assertGreaterEqual(plan["selected_process_count"], 1)
        self.assertLessEqual(plan["selected_process_count"], max(1, plan["logical_cpu_count"] - 2))

    def test_live_trading_host_refuses_multiprocess_optimization(self) -> None:
        with self.assertRaises(ValueError):
            OptimizationConfig(
                optimization_id="live-refusal",
                production_config_path=str(self.config_path),
                session_dates=SESSIONS,
                source_root=str(self.source_root),
                host_role="live-trading",
                process_count=2,
            )

    def test_parallel_jobs_do_not_embed_observation_tuple_per_candidate(self) -> None:
        from optimization import runner

        seen_batches = []

        class FakePool:
            def __init__(self, *args, **kwargs):
                self.kwargs = kwargs

            def __enter__(self):
                runner._initialize_candidate_worker(
                    self.kwargs["initargs"][0],
                    self.kwargs["initargs"][1],
                    self.kwargs["initargs"][2],
                )
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def map(self, fn, batches):
                for batch in batches:
                    seen_batches.append(batch)
                    yield fn(batch)

        with mock.patch("optimization.runner.ProcessPoolExecutor", FakePool):
            result = run_optimization(self.opt_config(candidate_pcts=("1.00", "1.01"), process_count=2, candidate_batch_size=2))
        self.assertEqual(len(result.candidate_rows), 3)
        self.assertTrue(seen_batches)
        self.assertTrue(all(hasattr(item, "candidate_id") for batch in seen_batches for item in batch))

    def test_completed_candidates_checkpoint_and_progress_are_written(self) -> None:
        result = run_optimization(self.opt_config(candidate_pcts=("1.00", "1.01")))
        out = Path(result.output_directory)
        completed = out / "completed_candidates.jsonl"
        progress = out / "progress.json"
        self.assertTrue(completed.exists())
        self.assertTrue(progress.exists())
        self.assertEqual(len([line for line in completed.read_text(encoding="utf-8").splitlines() if line]), len(result.candidate_rows))
        payload = json.loads(progress.read_text(encoding="utf-8"))
        self.assertEqual(payload["status"], "COMPLETED")
        self.assertEqual(payload["completed_candidates"], len(result.candidate_rows))

    def test_resume_skips_completed_candidates(self) -> None:
        config = self.opt_config(candidate_pcts=("1.00", "1.01"))
        first = run_optimization(config)
        resumed = run_optimization(self.opt_config(candidate_pcts=("1.00", "1.01"), resume=True))
        self.assertEqual(first.candidate_rows, resumed.candidate_rows)
        completed_path = Path(resumed.output_directory) / "completed_candidates.jsonl"
        records = [json.loads(line) for line in completed_path.read_text(encoding="utf-8").splitlines() if line]
        self.assertEqual(len({row["candidate_id"] for row in records}), len(first.candidate_rows))

    def test_preflight_performs_no_full_sweep(self) -> None:
        result = run_optimization(self.opt_config(preflight_only=True))
        self.assertEqual(result.candidate_rows, tuple())
        self.assertEqual(result.preflight["total_candidate_count"], 4)

    def test_cli_preflight(self) -> None:
        rc = optimization_main(
            [
                "run",
                "--config",
                str(self.config_path),
                "--sessions",
                "2026-07-20",
                "--source-root",
                str(self.source_root),
                "--optimization-id",
                "cli-preflight",
                "--output",
                str(self.root / "cli-out"),
                "--preflight-only",
                "--candidate-pcts",
                "1.00",
            ]
        )
        self.assertEqual(rc, 0)


if __name__ == "__main__":
    unittest.main()
