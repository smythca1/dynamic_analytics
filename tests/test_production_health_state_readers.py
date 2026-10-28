from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]


def load_module(name: str, relative_path: str):
    path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class ProductionHealthStateReaderTests(unittest.TestCase):
    def _reconciliation_fixture(self, *, positions, fills=None, direct_positions=None, positions_error=None):
        fleet = load_module("fleet_runner_reconciliation_regression", "fleet_runner.py")

        class Trader:
            def __init__(self):
                self.direct_positions = list(direct_positions or [])
                self.positions = list(positions)
                self.fills = list(fills or [])

            def _get_position(self, symbol):
                if self.direct_positions:
                    value = self.direct_positions.pop(0)
                    return value
                return None

            def get_positions(self):
                if positions_error:
                    raise RuntimeError(positions_error)
                if self.positions:
                    return self.positions.pop(0)
                return []

            def get_orders(self, **kwargs):
                return []

            def get_recent_fills(self, symbol, limit=100):
                return self.fills

        class Engine:
            def __init__(self):
                self.config = SimpleNamespace(strategy_type="renko_reversal", exit_management="strategy")
                self.reconciliation_unsafe = False
                self.position = {
                    "symbol": "TSLA",
                    "side": "short",
                    "entry_price": 393.89,
                    "qty": 5,
                }
                self.restored = []
                self.external_close = []

            def trade_state_snapshot(self):
                return {"position": self.position, "force_flat_dates": []}

            def mark_reconciliation_unsafe(self, reason, evidence=None):
                self.reconciliation_unsafe = True
                return {"reason": reason, "new": True}

            def mark_reconciliation_safe(self, reason, evidence=None):
                self.reconciliation_unsafe = False
                return {"reason": reason, "new": False}

            def restore_trade_state(self, state):
                self.restored.append(state)
                self.position = state.get("position")

            def mark_external_close_filled(self, **kwargs):
                self.external_close.append(kwargs)
                self.position = None

        class Log:
            def __init__(self):
                self.events = []

            def write(self, name, payload):
                self.events.append((name, payload))

        trader = Trader()
        engine = Engine()
        execution = fleet.AlpacaExecutionEngine.__new__(fleet.AlpacaExecutionEngine)
        execution.trader = trader
        runtime = fleet.TraderRuntime(
            trader_id="camatron_tsla_test",
            symbol="TSLA",
            mode="live",
            credential_profile="test",
            feed="sip",
            enabled=True,
            execution_enabled=True,
            max_position_notional=10000,
            max_daily_loss=250,
            engine=engine,
            execution=execution,
            log=Log(),
        )
        return fleet, runtime, engine

    def test_reconciliation_does_not_clear_on_empty_observation_without_close_fill(self):
        fleet, runtime, engine = self._reconciliation_fixture(
            positions=[[]],
            direct_positions=[None],
        )

        with mock.patch.object(fleet, "archive_engine_state"):
            with self.assertRaises(fleet.BrokerEngineStateMismatch):
                fleet.reconcile_engine_with_broker(Path(tempfile.mkdtemp()), runtime)

        self.assertEqual(len(engine.restored), 0)
        self.assertIsNotNone(engine.position)
        self.assertTrue(engine.reconciliation_unsafe)
        self.assertTrue(any(name == "critical_runtime_halt" for name, _ in runtime.log.events))

    def test_reconciliation_accepts_legitimate_flatten_with_close_fill(self):
        fleet, runtime, engine = self._reconciliation_fixture(
            positions=[[]],
            direct_positions=[None],
            fills=[{"symbol": "TSLA", "side": "buy", "qty": "5", "price": "410.00", "order_id": "close-1"}],
        )

        with mock.patch.object(fleet, "archive_engine_state"):
            fleet.reconcile_engine_with_broker(Path(tempfile.mkdtemp()), runtime)

        self.assertEqual(len(engine.external_close), 1)
        self.assertIsNone(engine.position)
        self.assertEqual(len(engine.restored), 0)

    def test_reconciliation_halts_and_preserves_position_on_stale_positions_endpoint(self):
        fleet, runtime, engine = self._reconciliation_fixture(
            positions=[],
            direct_positions=[None],
            positions_error="timeout",
        )

        with mock.patch.object(fleet, "archive_engine_state"):
            with self.assertRaises(fleet.BrokerEngineStateMismatch):
                fleet.reconcile_engine_with_broker(Path(tempfile.mkdtemp()), runtime)

        self.assertIsNotNone(engine.position)
        self.assertEqual(engine.restored, [])
        self.assertTrue(engine.reconciliation_unsafe)

    def test_build_health_reports_nested_open_position(self) -> None:
        health = load_module(
            "production_health_build_health_artifacts",
            "artifacts/production_health/20260908/build_health_artifacts.py",
        )
        state = {
            "engine": {
                "trade": {
                    "position": {
                        "symbol": "NVDA",
                        "side": "long",
                        "quantity": 1,
                    }
                }
            }
        }

        self.assertEqual(health.state_position_label(state), "long 1")
        self.assertTrue(health.positions_match(state, [{"symbol": "NVDA", "side": "long", "qty": "1"}]))

    def test_build_health_reports_nested_active_protection(self) -> None:
        health = load_module(
            "production_health_build_health_artifacts_protection",
            "artifacts/production_health/20260908/build_health_artifacts.py",
        )
        state = {
            "engine": {
                "trade": {
                    "position": {"symbol": "NVDA", "side": "long", "qty": 1},
                    "protection_state": {
                        "state": "OPEN",
                        "parent_order_id": "parent-1",
                        "take_profit_order_id": "tp-1",
                        "stop_loss_order_id": "sl-1",
                    },
                }
            }
        }

        status = health.protection_status(state, [])

        self.assertIn("OPEN", status)
        self.assertIn("parent=parent-1", status)
        self.assertIn("tp=tp-1", status)
        self.assertIn("sl=sl-1", status)

    def test_build_health_reports_nested_flat_position(self) -> None:
        health = load_module(
            "production_health_build_health_artifacts_flat",
            "artifacts/production_health/20260908/build_health_artifacts.py",
        )
        state = {"engine": {"trade": {"position": None, "pending_exit": None, "protection_state": None}}}

        self.assertEqual(health.state_position_label(state), "flat")
        self.assertTrue(health.positions_match(state, []))

    def test_final_verify_state_meta_uses_engine_trade(self) -> None:
        verifier = load_module(
            "production_health_final_verify",
            "artifacts/production_health/20260908/final_verify.py",
        )
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "state.json"
            path.write_text(
                json.dumps(
                    {
                        "saved_at": "2026-09-10T19:13:20.521509+00:00",
                        "engine": {
                            "trade": {
                                "position": {"symbol": "NVDA", "side": "long", "quantity": 1},
                                "pending_exit": None,
                                "protection_state": {"state": "OPEN", "parent_order_id": "parent-1"},
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            meta = verifier.state_meta(path)

        self.assertEqual(meta["position_side"], "long")
        self.assertEqual(meta["position_qty"], 1)
        self.assertEqual(meta["protection_state"]["parent_order_id"], "parent-1")

    def test_shared_sip_validation_rejects_nested_open_position(self) -> None:
        validation = load_module(
            "production_post_fix_shared_sip_validation",
            "scripts/post_fix_shared_sip_validation_20260825.py",
        )
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            trader_id = "camatron_nvda"
            state_dir = root / "artifacts/live/state"
            state_dir.mkdir(parents=True)
            (state_dir / f"{trader_id}.json").write_text(
                json.dumps(
                    {
                        "contract_fingerprints": {"market_stream": {"fingerprint": "fp-nvda"}},
                        "engine": {
                            "trade": {
                                "position": {"symbol": "NVDA", "side": "long", "qty": 1},
                                "pending_exit": None,
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            market_cfg = SimpleNamespace(contract_fingerprints={"NVDA": "fp-nvda"})

            with mock.patch.object(validation, "ROOT", root), mock.patch.object(
                validation, "EXPECTED_CAMATRON", {trader_id: {"symbol": "NVDA", "market_fingerprint": "fp-nvda"}}
            ), mock.patch.object(validation, "EXPECTED_MEGATRON", {}):
                with self.assertRaisesRegex(RuntimeError, "state has position"):
                    validation.validate_state_files(market_cfg)


if __name__ == "__main__":
    unittest.main()
