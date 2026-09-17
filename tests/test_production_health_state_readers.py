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
