from __future__ import annotations

import json
import tempfile
import unittest
from copy import deepcopy
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd

from fleet_runner import TraderRuntime, build_runtimes
from optimization.models import PreparedSource
from optimization.runner import promotion_contract_guard
from optimization.source_preparation import load_prepared_observations, prepare_source
from renko_builder import RenkoBuildState, update_renko_build_state
from shared.config.trader_config import TraderConfigError, load_production_renko_config, validate_renko_trader_source
from silver.market import CANONICAL_RENKO_SOURCE, CanonicalMarketObservationAdapter, CanonicalOneSecondSipSampler, RawSipTrade
from silver.market.models import second_bucket
from strategy_engine import RenkoStrategyConfig, RenkoStrategyEngine
from trading_events import MarketTick


def raw(
    timestamp: str,
    price: str,
    trade_id: str,
    sequence: int,
    *,
    symbol: str = "AAPL",
    conditions: tuple[str, ...] = ("@",),
) -> RawSipTrade:
    return RawSipTrade(
        sip_timestamp=timestamp,
        receive_timestamp=timestamp,
        symbol=symbol,
        price=price,
        quantity="1",
        exchange="unit",
        tape="C",
        trade_id=trade_id,
        broker_trade_id=trade_id,
        source_record_id=f"source:{symbol}:{sequence}",
        source_dataset_reference="unit-dataset",
        source_dataset_hash="unit-hash",
        source_row_order=sequence,
        conditions=conditions,
        source="unit",
        source_sequence=sequence,
        row_order=sequence,
        trader_id="canonical",
    )


def observation_key(observation) -> tuple[str, str, str, str | None]:
    return (
        pd.Timestamp(observation.second_bucket).isoformat(),
        str(Decimal(str(observation.close_price)).normalize()),
        observation.source_broker_trade_id,
        observation.source_record_id,
    )


def brick_key(brick) -> tuple[str, float, int, int]:
    return (pd.Timestamp(brick.timestamp).isoformat(), round(float(brick.close), 6), int(brick.direction), int(brick.run_len))


def signal_key(signal) -> tuple[str, str, str, str, float]:
    return (pd.Timestamp(signal.timestamp).isoformat(), signal.signal_type, signal.side, signal.reason, round(float(signal.price), 6))


def intent_key(intent) -> tuple[str, str, str, str, str]:
    return (pd.Timestamp(intent.timestamp).isoformat(), intent.signal_type, intent.side, intent.reason, intent.order_type)


def run_engine(observations, *, qty: float = 1.0, trader_id: str = "a") -> tuple[list, list, list]:
    engine = RenkoStrategyEngine(
        RenkoStrategyConfig(
            trader_id=trader_id,
            symbol="AAPL",
            strategy_type="renko_reversal",
            brick_size=1.0,
            entry_bricks=1,
            qty=qty,
            renko_anchor_mode="grid",
            renko_anchor_price=0.0,
            entry_start="09:30:00",
            entry_end="16:00:00",
            allow_overnight=True,
        )
    )
    bricks = []
    signals = []
    intents = []
    for observation in observations:
        new_bricks, actions = engine.on_tick(
            MarketTick(
                timestamp=pd.Timestamp(observation.second_bucket),
                symbol=observation.symbol,
                price=float(observation.close_price),
                size=1,
                trade_id=observation.source_broker_trade_id,
            )
        )
        bricks.extend(new_bricks)
        for signal, intent in actions:
            signals.append(signal)
            intents.append(intent)
    return bricks, signals, intents


class SystemWideRenkoContractTests(unittest.TestCase):
    def test_second_bucket_floors_python_datetime_microseconds(self) -> None:
        source = datetime(2026, 8, 24, 14, 30, 15, 987654, tzinfo=timezone.utc)
        bucket = second_bucket(source)
        self.assertEqual(bucket, datetime(2026, 8, 24, 14, 30, 15, tzinfo=timezone.utc))
        self.assertEqual(source.microsecond, 987654)

    def test_second_bucket_floors_pandas_timestamp_nanoseconds(self) -> None:
        bucket = second_bucket(pd.Timestamp("2026-08-24T14:30:15.987654321Z"))
        self.assertEqual(pd.Timestamp(bucket).isoformat(), "2026-08-24T14:30:15+00:00")
        self.assertEqual(pd.Timestamp(bucket).nanosecond, 0)
        self.assertEqual(pd.Timestamp(bucket).microsecond, 0)

    def test_second_bucket_collapses_nanosecond_timestamps_inside_one_second(self) -> None:
        buckets = {
            second_bucket(pd.Timestamp(f"2026-08-24T14:30:15.{suffix}Z"))
            for suffix in ("000000001", "111111111", "999999999")
        }
        self.assertEqual(len(buckets), 1)
        self.assertEqual(pd.Timestamp(next(iter(buckets))).isoformat(), "2026-08-24T14:30:15+00:00")

    def test_second_bucket_preserves_exact_second_boundary(self) -> None:
        bucket = second_bucket(pd.Timestamp("2026-08-24T14:30:15Z"))
        self.assertEqual(pd.Timestamp(bucket).isoformat(), "2026-08-24T14:30:15+00:00")

    def test_second_bucket_normalizes_timezone_aware_inputs_to_same_instant(self) -> None:
        utc_bucket = second_bucket(datetime(2026, 8, 24, 18, 30, 15, 123456, tzinfo=timezone.utc))
        eastern_bucket = second_bucket(
            pd.Timestamp(datetime(2026, 8, 24, 14, 30, 15, 987654, tzinfo=ZoneInfo("America/New_York")))
        )
        self.assertEqual(utc_bucket, eastern_bucket)

    def test_second_bucket_is_idempotent_and_monotonic_across_seconds(self) -> None:
        first = second_bucket(pd.Timestamp("2026-08-24T14:30:15.999999999Z"))
        second = second_bucket(pd.Timestamp("2026-08-24T14:30:16.000000001Z"))
        self.assertEqual(second_bucket(first), first)
        self.assertLess(first, second)

    def test_sampler_emits_one_canonical_close_per_symbol_second_for_nanosecond_sip_trades(self) -> None:
        sampler = CanonicalOneSecondSipSampler(run_id="canonical")
        trades = [
            raw("2026-08-24T14:30:15.000000001Z", "100", "t0", 0),
            raw("2026-08-24T14:30:15.500000000Z", "101", "t1", 1),
            raw("2026-08-24T14:30:15.999999999Z", "102", "t2", 2),
            raw("2026-08-24T14:30:16.000000001Z", "103", "t3", 3),
            raw("2026-08-24T14:30:16.999999999Z", "104", "t4", 4),
            raw("2026-08-24T14:30:17.000000001Z", "105", "t5", 5),
        ]

        observations = []
        for trade in trades:
            observations.extend(sampler.process_trade(trade).observations)

        keys = [(item.symbol, pd.Timestamp(item.second_bucket).isoformat()) for item in observations]
        self.assertEqual(keys, [("AAPL", "2026-08-24T14:30:15+00:00"), ("AAPL", "2026-08-24T14:30:16+00:00")])
        self.assertEqual([str(item.close_price) for item in observations], ["102", "104"])
        self.assertEqual(len(keys), len(set(keys)))

    def test_production_adapter_nanosecond_ticks_do_not_emit_multiple_closes_for_same_second(self) -> None:
        adapter = CanonicalMarketObservationAdapter(run_id="canonical")
        ticks = [
            MarketTick(pd.Timestamp("2026-08-24T14:30:15.000000001Z"), "AAPL", 100.0, 1, trade_id="t0", conditions=("@",)),
            MarketTick(pd.Timestamp("2026-08-24T14:30:15.500000000Z"), "AAPL", 101.0, 1, trade_id="t1", conditions=("@",)),
            MarketTick(pd.Timestamp("2026-08-24T14:30:15.999999999Z"), "AAPL", 102.0, 1, trade_id="t2", conditions=("@",)),
            MarketTick(pd.Timestamp("2026-08-24T14:30:16.000000001Z"), "AAPL", 103.0, 1, trade_id="t3", conditions=("@",)),
            MarketTick(pd.Timestamp("2026-08-24T14:30:17.000000001Z"), "AAPL", 104.0, 1, trade_id="t4", conditions=("@",)),
        ]

        observations = []
        for index, tick in enumerate(ticks):
            source_event = SimpleNamespace(
                source_record_id=f"source:AAPL:{index}",
                source_sequence=index,
                archive_path="unit-dataset",
                broker_trade_id=tick.trade_id,
            )
            observations.extend(adapter.process_tick(tick, trader_id="canonical", source_event=source_event).observations)

        timestamps = [pd.Timestamp(item.second_bucket) for item in observations]
        self.assertEqual([item.isoformat() for item in timestamps], ["2026-08-24T14:30:15+00:00", "2026-08-24T14:30:16+00:00"])
        self.assertTrue(all(item.nanosecond == 0 and item.microsecond == 0 for item in timestamps))

    def test_every_checked_in_renko_config_uses_canonical_source(self) -> None:
        rows = []
        for path in sorted(Path("configs").rglob("*.json")):
            data = json.loads(path.read_text(encoding="utf-8"))
            for trader in data.get("traders", []):
                if not isinstance(trader, dict):
                    continue
                strategy = trader.get("strategy") if isinstance(trader.get("strategy"), dict) else {}
                if str(strategy.get("type", "")).startswith("renko"):
                    source = (trader.get("data") or {}).get("renko_source")
                    rows.append((str(path), trader.get("id"), source))
        self.assertTrue(rows)
        self.assertTrue(all(source == CANONICAL_RENKO_SOURCE for _, _, source in rows), rows)

    def test_missing_and_noncanonical_renko_sources_are_rejected(self) -> None:
        trader = {
            "id": "bad",
            "symbol": "AAPL",
            "strategy": {"type": "renko_reversal", "brick_size": 1, "entry_bricks": 1},
            "sizing": {"qty": 1},
        }
        with self.assertRaises(TraderConfigError):
            validate_renko_trader_source(trader)
        trader["data"] = {"renko_source": "ticks"}
        with self.assertRaises(TraderConfigError):
            validate_renko_trader_source(trader)

    def test_promotable_paths_produce_identical_observations_bricks_signals_and_presizing_intents(self) -> None:
        trades = [
            raw("2026-07-20T13:30:00.100Z", "100", "t0", 0),
            raw("2026-07-20T13:30:00.900Z", "101", "t1", 1),
            raw("2026-07-20T13:30:01.100Z", "102", "t2", 2),
            raw("2026-07-20T13:30:02.100Z", "99", "t3", 3),
            raw("2026-07-20T13:30:03.100Z", "101", "t4", 4),
        ]
        sampler = CanonicalOneSecondSipSampler(run_id="canonical")
        replay_observations = []
        for trade in trades:
            replay_observations.extend(sampler.process_trade(trade).observations)

        adapter = CanonicalMarketObservationAdapter(run_id="canonical")
        production_observations = []
        for trade in trades:
            tick = MarketTick(pd.Timestamp(trade.sip_timestamp), trade.symbol, float(trade.price), float(trade.quantity), trade_id=trade.trade_id, conditions=trade.conditions)
            source_event = SimpleNamespace(source_record_id=trade.source_record_id, source_sequence=trade.source_sequence, archive_path=trade.source_dataset_reference, broker_trade_id=trade.broker_trade_id)
            production_observations.extend(adapter.process_tick(tick, trader_id="canonical", source_event=source_event).observations)

        self.assertEqual([observation_key(item) for item in production_observations], [observation_key(item) for item in replay_observations])

        first_bricks, first_signals, first_intents = run_engine(production_observations, qty=1, trader_id="acct1")
        second_bricks, second_signals, second_intents = run_engine(replay_observations, qty=9, trader_id="acct2")
        self.assertEqual([brick_key(item) for item in first_bricks], [brick_key(item) for item in second_bricks])
        self.assertEqual([signal_key(item) for item in first_signals], [signal_key(item) for item in second_signals])
        self.assertEqual([intent_key(item) for item in first_intents], [intent_key(item) for item in second_intents])

    def test_optimizer_preparation_observations_match_canonical_sampler(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "bronze" / "AAPL" / "2026-07-20" / "sip_trades.jsonl"
            source.parent.mkdir(parents=True)
            rows = [
                {"timestamp": "2026-07-20T13:30:00.100Z", "symbol": "AAPL", "price": 100, "size": 1, "trade_id": "t0", "conditions": ["@"], "source_sequence": 0},
                {"timestamp": "2026-07-20T13:30:00.900Z", "symbol": "AAPL", "price": 101, "size": 1, "trade_id": "t1", "conditions": ["@"], "source_sequence": 1},
                {"timestamp": "2026-07-20T13:30:01.100Z", "symbol": "AAPL", "price": 102, "size": 1, "trade_id": "t2", "conditions": ["@"], "source_sequence": 2},
            ]
            source.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
            prepared = prepare_source(symbol="AAPL", trader_id="aapl", session_dates=(date(2026, 7, 20),), source_root=str(root / "bronze"), output_root=str(root / "prepared"), run_id="prep")
            observations = load_prepared_observations(prepared.observations_path)
        self.assertEqual([str(item.close_price) for item in observations], ["101"])
        self.assertTrue(prepared.approved_canonical_path)
        self.assertTrue(prepared.market_stream_contract.get("fingerprint"))

    def test_sampler_checkpoint_continuation_matches_uninterrupted_processing(self) -> None:
        trades = [raw("2026-07-20T13:30:00.100Z", "100", "t0", 0), raw("2026-07-20T13:30:01.100Z", "101", "t1", 1)]
        uninterrupted = CanonicalOneSecondSipSampler(run_id="resume")
        uninterrupted_rows = []
        for trade in trades:
            uninterrupted_rows.extend(uninterrupted.process_trade(trade).observations)

        first = CanonicalOneSecondSipSampler(run_id="resume")
        first.process_trade(trades[0])
        snapshot = first.snapshot(scope_key=("canonical", "AAPL"))
        resumed = CanonicalOneSecondSipSampler(run_id="resume")
        resumed.restore_snapshot(snapshot)
        resumed_rows = list(resumed.process_trade(trades[1]).observations)

        self.assertEqual([observation_key(item) for item in resumed_rows], [observation_key(item) for item in uninterrupted_rows])

    def test_missing_or_mismatched_contract_metadata_blocks_promotion(self) -> None:
        target = load_production_renko_config("configs/traders.live.example.json", "camatron_aapl_1s_renko_1p3443_e1_reversal_1sh_20260701_live")
        legacy_row = {"candidate_status": "COMPLETED"}
        result = promotion_contract_guard(candidate_row=legacy_row, target_contract_fingerprints=target.metadata["contract_fingerprints"], session_coverage_complete=True)
        self.assertFalse(result["promotable"])
        self.assertIn("missing_promotable_contract_metadata", result["reasons"])

        row = {
            "candidate_status": "COMPLETED",
            "approved_canonical_path": True,
            "promotable_contract_metadata": True,
            "market_stream_fingerprint": target.metadata["contract_fingerprints"]["market_stream"]["fingerprint"],
            "renko_strategy_fingerprint": "wrong",
        }
        result = promotion_contract_guard(candidate_row=row, target_contract_fingerprints=target.metadata["contract_fingerprints"], session_coverage_complete=False)
        self.assertFalse(result["promotable"])
        self.assertIn("incomplete_session_coverage", result["reasons"])
        self.assertIn("renko_strategy_fingerprint_mismatch", result["reasons"])

    def test_existing_state_without_contract_fingerprint_blocks_enabled_renko_startup(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = root / "state"
            state.mkdir()
            state_path = state / "legacy.json"
            state_path.write_text(
                json.dumps(
                    {
                        "engine": {
                            "symbol": "AAPL",
                            "strategy_type": "renko_reversal",
                            "renko": {"brick_size": 1, "last_brick_close": 100, "run_dir": 0, "run_len": 0},
                            "trade": {},
                        }
                    }
                ),
                encoding="utf-8",
            )
            config = {
                "traders": [
                    {
                        "id": "legacy",
                        "enabled": True,
                        "execution_enabled": False,
                        "symbol": "AAPL",
                        "strategy": {"type": "renko_reversal", "brick_size": 1, "entry_bricks": 1},
                        "sizing": {"qty": 1},
                        "data": {"feed": "sip", "renko_source": "1s_close"},
                    }
                ]
            }
            args = SimpleNamespace(
                force_mode=None,
                force_feed=None,
                market_data_source=None,
                market_data_profile=None,
                data_credential_profile=None,
                log_output=str(root / "events.jsonl"),
                execute_real=False,
                archive_root=str(root),
                evidence_enabled=False,
                shadow_sampler_enabled=False,
            )
            with self.assertRaisesRegex(RuntimeError, "no canonical Renko contract fingerprints"):
                build_runtimes(config, args)

    def test_qqq_non_renko_builds_without_renko_source_requirement(self) -> None:
        config = {
            "traders": [
                {
                    "id": "qqq",
                    "enabled": True,
                    "execution_enabled": False,
                    "symbol": "QQQ",
                    "strategy": {
                        "type": "chop_mean_reversion",
                        "window_seconds": 10,
                        "entry_z": 3,
                        "stop_z": 5,
                        "efficiency_max": 0.35,
                        "trend_exit_efficiency": 0.65,
                        "max_hold_seconds": 60,
                    },
                    "sizing": {"qty": 1},
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmp:
            args = SimpleNamespace(
                force_mode=None,
                force_feed=None,
                market_data_source=None,
                market_data_profile=None,
                data_credential_profile=None,
                log_output=str(Path(tmp) / "events.jsonl"),
                execute_real=False,
                archive_root=tmp,
                evidence_enabled=False,
                shadow_sampler_enabled=False,
            )
            runtimes = build_runtimes(config, args)
        self.assertEqual(runtimes[0].renko_source, "ticks")
        self.assertIsNone(runtimes[0].market_adapter)


if __name__ == "__main__":
    unittest.main()
