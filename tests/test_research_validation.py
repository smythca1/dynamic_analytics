from __future__ import annotations

import asyncio
import gzip
import json
import tempfile
import unittest
from unittest import mock
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from optimization.coverage import validate_coverage
from optimization.historical_sip import (
    HistoricalSipBackfillRequest,
    backfill_historical_sip,
    reject_production_tick_root,
)
from optimization.market_calendar import latest_completed_nyse_sessions, nyse_session_window
from optimization.mean_reversion_runner import MeanReversionResearchConfig, run_mean_reversion_research
from optimization.models import OptimizationConfig
from optimization.overlap import compare_overlap
from optimization.parity_harness import run_deterministic_parity
from optimization.runner import load_enabled_renko_traders, run_optimization
from optimization.shadow_capture import ShadowCaptureRequest, capture_shadow_session
from replay.historical_source import read_historical_source
from replay.models import ReplayConfig
from shared.config.trader_config import load_production_renko_config
from silver.market import CanonicalObservation


class Response:
    def __init__(self, payload: dict, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code
        self.text = json.dumps(payload)

    def json(self) -> dict:
        return self._payload


class FakeSession:
    def __init__(self) -> None:
        self.calls = []
        self.pages = [
            {"trades": [_alpaca_trade("2026-08-13T13:30:00.100Z", 100, "1")], "next_page_token": "next"},
            {"trades": [_alpaca_trade("2026-08-13T13:30:01.100Z", 101, "2")]},
        ]

    def get(self, url, *, headers, params, timeout):
        self.calls.append({"url": url, "headers": headers, "params": dict(params), "timeout": timeout})
        return Response(self.pages.pop(0))


def _alpaca_trade(ts: str, price: float, trade_id: str, symbol: str = "AAPL") -> dict:
    return {"t": ts, "S": symbol, "p": price, "s": 1, "x": "V", "z": "C", "c": ["@"], "i": trade_id}


def _write_gzip_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def _session_rows(symbol: str = "AAPL", step_seconds: int = 300) -> list[dict]:
    window = nyse_session_window(date(2026, 8, 13))
    rows = []
    ts = window.start_utc
    index = 0
    while ts <= window.end_utc:
        rows.append(_alpaca_trade(ts.isoformat().replace("+00:00", "Z"), 100 + (index % 3), str(index), symbol=symbol))
        ts += timedelta(seconds=step_seconds)
        index += 1
    return rows


class ResearchValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_historical_backfill_paginates_and_writes_manifest_without_credentials(self) -> None:
        request = HistoricalSipBackfillRequest(
            symbols=("AAPL",),
            sessions=(date(2026, 8, 13),),
            output_root=str(self.root / "research"),
            env_file=None,
            min_free_gb=0,
        )
        session = FakeSession()
        with mock.patch.dict("os.environ", {"APCA_API_KEY_ID": "key", "APCA_API_SECRET_KEY": "secret"}):
            result = backfill_historical_sip(request, session_factory=lambda: session)
        self.assertEqual(result.files[0].raw_record_count, 2)
        raw_path = Path(result.files[0].raw_path)
        self.assertTrue(raw_path.name.endswith(".jsonl.gz"))
        manifest = json.loads(Path(result.files[0].manifest_path).read_text(encoding="utf-8"))
        self.assertEqual(manifest["provider"], "alpaca")
        self.assertEqual(manifest["feed"], "sip")
        self.assertEqual(manifest["pagination"]["page_count"], 2)
        self.assertNotIn("key", json.dumps(manifest))
        self.assertNotIn("secret", json.dumps(manifest))
        self.assertEqual(session.calls[1]["params"]["page_token"], "next")

    def test_rejects_production_tick_root(self) -> None:
        with self.assertRaises(ValueError):
            reject_production_tick_root("/root/trading-server/artifacts/live/ticks")
        with self.assertRaises(ValueError):
            reject_production_tick_root("artifacts/live/ticks/research")

    def test_shadow_capture_rejects_production_artifact_roots(self) -> None:
        async def empty_stream():
            if False:
                yield {}

        request = ShadowCaptureRequest(
            symbols=("AAPL",),
            session_date=date(2026, 8, 20),
            output_root="artifacts/live/ticks",
            min_free_gb=0,
        )
        with self.assertRaises(ValueError):
            asyncio.run(capture_shadow_session(request, message_source=empty_stream()))

    def test_shadow_capture_writes_research_raw_canonical_and_manifest_without_credentials(self) -> None:
        async def stream():
            yield _alpaca_trade("2026-08-20T13:30:01.100Z", 100, "1")
            yield _alpaca_trade("2026-08-20T13:30:02.100Z", 101, "2")

        started_after_open = datetime(2026, 8, 20, 14, 50, tzinfo=timezone.utc)
        request = ShadowCaptureRequest(
            symbols=("AAPL",),
            session_date=date(2026, 8, 20),
            output_root=str(self.root / "research" / "shadow"),
            run_id="unit-shadow",
            min_free_gb=0,
            started_at_override=started_after_open,
        )
        with mock.patch.dict("os.environ", {"APCA_API_KEY_ID": "key", "APCA_API_SECRET_KEY": "secret"}):
            result = asyncio.run(capture_shadow_session(request, message_source=stream(), now=lambda: started_after_open))
        self.assertEqual(result.completeness, "PARTIAL_STARTED_AFTER_OPEN")
        self.assertEqual(result.raw_counts["AAPL"], 2)
        self.assertEqual(result.canonical_counts["AAPL"], 1)
        manifest = json.loads(Path(result.manifest_path).read_text(encoding="utf-8"))
        self.assertEqual(manifest["canonical_adapter"], "CanonicalMarketObservationAdapter")
        self.assertFalse(manifest["broker_order_endpoints_enabled"])
        self.assertFalse(manifest["production_state_mutation"])
        self.assertEqual(manifest["market_data_endpoint"], "alpaca_data_stream")
        self.assertIn("AAPL", manifest["market_stream_fingerprints"])
        self.assertNotIn("key", json.dumps(manifest))
        self.assertNotIn("secret", json.dumps(manifest))
        raw_path = Path(manifest["paths"]["AAPL"]["raw"])
        canonical_path = Path(manifest["paths"]["AAPL"]["canonical_1s_close"])
        raw_row = json.loads(raw_path.read_text(encoding="utf-8").splitlines()[0])
        canonical_row = json.loads(canonical_path.read_text(encoding="utf-8").splitlines()[0])
        self.assertEqual(raw_row["source_sequence"], 1)
        self.assertTrue(raw_row["eligible"])
        self.assertEqual(canonical_row["observation"]["second_bucket"], "2026-08-20T13:30:01+00:00")
        self.assertEqual(canonical_row["observation"]["source_alpaca_trade_id"], "1")
        self.assertEqual(len(manifest["checksums"]["AAPL"]["raw"]), 64)

    def test_shadow_capture_module_does_not_import_broker_trading_paths(self) -> None:
        text = Path("optimization/shadow_capture.py").read_text(encoding="utf-8")
        for forbidden in ("TradingClient", "submit_order", "/v2/orders", "AlpacaExecutionEngine", "execution_engine", "alpaca_trader"):
            self.assertNotIn(forbidden, text)

    def test_gzip_historical_source_is_supported(self) -> None:
        path = self.root / "AAPL" / "2026-08-13" / "sip_trades.jsonl.gz"
        _write_gzip_jsonl(path, [_alpaca_trade("2026-08-13T13:30:00.100Z", 100, "1")])
        result = read_historical_source(
            ReplayConfig(
                run_id="gzip",
                trader_id="aapl",
                symbol="AAPL",
                start=datetime(2026, 8, 13, 13, 30, tzinfo=timezone.utc),
                end=datetime(2026, 8, 13, 20, 0, tzinfo=timezone.utc),
                timezone="UTC",
                session_mode="regular",
                strategy_config={"symbol": "AAPL", "brick_size": 1, "entry_bricks": 1, "qty": 1},
                initial_capital="10000",
                brick_mode="BRICK_DOLLARS",
                brick_size_dollars="1",
                source_data_path=str(path),
            )
        )
        self.assertEqual(result.raw_trades_in_window, 1)

    def test_coverage_passes_complete_stream_and_fails_partial_stream(self) -> None:
        complete = self.root / "AAPL" / "2026-08-13" / "sip_trades.jsonl.gz"
        partial = self.root / "AAPL" / "2026-08-13-partial" / "sip_trades.jsonl.gz"
        _write_gzip_jsonl(complete, _session_rows())
        _write_gzip_jsonl(partial, _session_rows()[:3])
        report = validate_coverage(symbol="AAPL", session_date=date(2026, 8, 13), source_path=complete)
        self.assertEqual(report.status, "PASS")
        failed = validate_coverage(symbol="AAPL", session_date=date(2026, 8, 13), source_path=partial)
        self.assertEqual(failed.status, "FAIL")
        self.assertIn("early_last_trade", failed.failure_reasons)

    def test_overlap_exact_match(self) -> None:
        left = self.root / "live" / "AAPL" / "2026-08-13" / "sip_trades.jsonl.gz"
        right = self.root / "hist" / "AAPL" / "2026-08-13" / "sip_trades.jsonl.gz"
        rows = _session_rows(step_seconds=60)
        _write_gzip_jsonl(left, rows)
        _write_gzip_jsonl(right, rows)
        report = compare_overlap(symbol="AAPL", session_date=date(2026, 8, 13), live_path=left, historical_path=right)
        self.assertEqual(report.status, "PASS")
        self.assertEqual(report.raw_exact_match_percentage, 100.0)
        self.assertEqual(report.canonical_exact_match_percentage, 100.0)

    def test_deterministic_parity_for_renko_and_mean_reversion(self) -> None:
        observations = tuple(
            CanonicalObservation(
                observation_id=f"obs-{idx}",
                run_id="test",
                symbol="AAPL",
                second_bucket=datetime(2026, 8, 13, 13, 31, idx, tzinfo=timezone.utc),
                emitted_sequence=idx,
                close_price=price,
                source_eligible_trade_id=f"elig-{idx}",
                source_sip_timestamp=datetime(2026, 8, 13, 13, 31, idx, tzinfo=timezone.utc),
                source_alpaca_trade_id=f"trade-{idx}",
            )
            for idx, price in enumerate([100, 101, 99, 102, 98, 103, 97, 104], start=1)
        )
        renko = run_deterministic_parity(
            trader_id="aapl",
            strategy_config={
                "symbol": "AAPL",
                "brick_size": 1.0,
                "entry_bricks": 1,
                "strategy_type": "renko_reversal",
                "qty": 1,
                "allow_overnight": True,
                "renko_anchor_mode": "grid",
                "renko_anchor_price": 0.0,
            },
            observations=observations,
        )
        self.assertEqual(renko.status, "PASS")
        qqq_start = datetime(2026, 8, 13, 13, 31, tzinfo=timezone.utc)
        qqq_obs = tuple(
            CanonicalObservation(
                observation_id=f"q-{idx}",
                run_id="test",
                symbol="QQQ",
                second_bucket=qqq_start + timedelta(seconds=idx),
                emitted_sequence=idx,
                close_price=price,
                source_eligible_trade_id=f"elig-q-{idx}",
                source_sip_timestamp=qqq_start + timedelta(seconds=idx),
                source_alpaca_trade_id=f"qtrade-{idx}",
            )
            for idx, price in enumerate([100, 100.1, 99.9, 100.2, 99.8, 100.4, 99.6, 100.5] * 8, start=1)
        )
        mean = run_deterministic_parity(
            trader_id="qqq",
            strategy_config={
                "symbol": "QQQ",
                "window_seconds": 30,
                "entry_z": 1.0,
                "exit_z": 0.1,
                "stop_z": 2.5,
                "efficiency_max": 1.0,
                "trend_exit_efficiency": 1.5,
                "max_hold_seconds": 60,
                "qty": 1,
                "allow_overnight": True,
            },
            observations=qqq_obs,
        )
        self.assertEqual(mean.status, "PASS")

    def test_tsla_single_strategy_kernel_has_deterministic_strategy_parity(self) -> None:
        loaded = load_production_renko_config(
            "configs/traders.live.example.json",
            "camatron_tsla_1s_renko_0p42_e2_tp1_sl3_qty1_20260917_live",
        )
        brick = loaded.brick_audit.effective_brick_dollars
        base = Decimal("327.50")
        prices = [
            base,
            base + brick,
            base + (brick * 2),
            base + (brick * 3),
            base + (brick * 4),
            base + (brick * 3),
            base + (brick * 2),
            base + brick,
            base,
            base - brick,
            base - (brick * 2),
            base - (brick * 3),
            base - (brick * 4),
            base - (brick * 5),
        ]
        start = datetime(2026, 9, 14, 13, 31, tzinfo=timezone.utc)
        observations = tuple(
            CanonicalObservation(
                observation_id=f"tsla-kernel-{idx}",
                run_id="tsla-kernel-strategy-parity",
                symbol="TSLA",
                second_bucket=start + timedelta(seconds=idx),
                emitted_sequence=idx,
                close_price=price,
                source_eligible_trade_id=f"eligible-{idx}",
                source_sip_timestamp=start + timedelta(seconds=idx),
                source_alpaca_trade_id=f"trade-{idx}",
            )
            for idx, price in enumerate(prices, start=1)
        )

        report = run_deterministic_parity(
            trader_id=loaded.strategy_config.trader_id,
            strategy_config=loaded.strategy_config,
            observations=observations,
        )

        self.assertEqual(report.status, "PASS")
        self.assertTrue(report.observation_parity)
        self.assertTrue(report.brick_parity)
        self.assertTrue(report.signal_parity)
        self.assertTrue(report.order_intent_parity)
        self.assertTrue(report.lifecycle_parity)
        self.assertTrue(report.identical_final_state)
        self.assertEqual(report.first_divergence, None)

    def test_optimizer_can_include_disabled_research_tp_sl_when_explicit(self) -> None:
        config_path = self.root / "megatron.json"
        config_path.write_text(
            json.dumps(
                {
                    "traders": [
                        {
                            "id": "megatron_nvda",
                            "enabled": False,
                            "symbol": "NVDA",
                            "strategy_name": "nvda_tp_sl",
                            "strategy": {
                                "type": "renko_tp_sl",
                                "brick_size": 1.0,
                                "entry_bricks": 1,
                                "take_profit_bricks": 2,
                                "stop_loss_bricks": 2,
                                "renko_anchor_mode": "grid",
                                "renko_anchor_price": 0.0,
                            },
                            "sizing": {"qty": 1},
                            "session": {"allow_overnight": True},
                            "data": {"renko_source": "1s_close"},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        traders = load_enabled_renko_traders(
            str(config_path),
            ("megatron_nvda",),
            include_disabled_research_traders=True,
            included_strategy_types=("renko_tp_sl",),
        )
        self.assertEqual(len(traders), 1)
        source = self.root / "bronze" / "NVDA" / "2026-08-13" / "sip_trades.jsonl.gz"
        _write_gzip_jsonl(source, _session_rows(symbol="NVDA", step_seconds=60))
        config = OptimizationConfig(
            optimization_id="tp-sl",
            production_config_path=str(config_path),
            session_dates=(date(2026, 8, 13),),
            source_root=str(self.root / "bronze"),
            prepared_output_root=str(self.root / "prepared"),
            output_directory=str(self.root / "out"),
            selected_trader_ids=("megatron_nvda",),
            parameter_grid_mode="RENKO_TP_SL_ENTRY_TP_SL_GRID_V1",
            selected_entry_confirmations=("E1",),
            selected_take_profit_bricks=(2,),
            selected_stop_loss_bricks=(2,),
            candidate_pcts=("1.0",),
            include_disabled_research_traders=True,
            minimum_closed_trades=0,
        )
        result = run_optimization(config)
        self.assertGreaterEqual(len(result.candidate_rows), 1)
        self.assertTrue(any(row["strategy_type"] == "renko_tp_sl" for row in result.candidate_rows))

    def test_mean_reversion_research_uses_production_engine_and_existing_grid(self) -> None:
        config_path = self.root / "traders.json"
        config_path.write_text(
            json.dumps(
                {
                    "traders": [
                        {
                            "id": "qqq_live",
                            "enabled": True,
                            "symbol": "QQQ",
                            "strategy_name": "qqq_current",
                            "strategy": {
                                "type": "chop_mean_reversion",
                                "window_seconds": 3600,
                                "entry_z": 3,
                                "exit_z": 0.10,
                                "stop_z": 4,
                                "efficiency_max": 0.35,
                                "trend_exit_efficiency": 0.65,
                                "max_hold_seconds": 600,
                            },
                            "sizing": {"qty": 1},
                            "session": {"entry_start": "09:31:00", "entry_end": "15:59:59", "allow_overnight": True},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        source = self.root / "bronze" / "QQQ" / "2026-08-13" / "sip_trades.jsonl.gz"
        _write_gzip_jsonl(source, _session_rows(symbol="QQQ", step_seconds=60))
        result = run_mean_reversion_research(
            MeanReversionResearchConfig(
                research_id="qqq-mr",
                config_path=str(config_path),
                trader_id="qqq_live",
                session_dates=(date(2026, 8, 13),),
                source_root=str(self.root / "bronze"),
                prepared_output_root=str(self.root / "prepared-mr"),
                output_directory=str(self.root / "mr-out"),
                include_grid=True,
                include_current=True,
            )
        )
        self.assertIsNotNone(result.incumbent)
        self.assertIsNotNone(result.best_raw_candidate)
        self.assertTrue(Path(result.output_directory, "candidates.csv").exists())
        self.assertTrue(all(row["strategy_type"] == "chop_mean_reversion" for row in result.rows))

    def test_latest_completed_window_does_not_include_incomplete_august_20(self) -> None:
        sessions = latest_completed_nyse_sessions(now=datetime(2026, 8, 20, 0, 18, tzinfo=timezone.utc), count=5)
        self.assertEqual(
            sessions,
            (date(2026, 8, 13), date(2026, 8, 14), date(2026, 8, 17), date(2026, 8, 18), date(2026, 8, 19)),
        )


if __name__ == "__main__":
    unittest.main()
