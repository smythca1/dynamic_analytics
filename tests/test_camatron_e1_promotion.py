from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path

from scripts.prepare_camatron_e1_promotion import (
    TARGETS,
    apply_approved_targets,
    exposure_report,
    migration_plan,
    validate_complete_migration,
    validate_preflight,
    write_artifacts,
)


OLD_VALUES = {
    "AAPL": (1.3443, 1),
    "NVDA": (1.149851, 1),
    "TSLA": (2.31165, 1),
}


def old_style_config() -> dict:
    data = json.loads(Path("configs/traders.live.example.json").read_text(encoding="utf-8"))
    copied = copy.deepcopy(data)
    for target in TARGETS:
        for trader in copied["traders"]:
            if trader["id"] != target.trader_id:
                continue
            brick, qty = OLD_VALUES[target.symbol]
            trader["strategy"]["brick_size"] = brick
            trader["sizing"]["qty"] = qty
            trader["risk"]["max_position_notional"] = 1000
            trader["profile"]["brick_size"] = brick
            trader["profile"]["live_qty"] = qty
    return copied


def clean_preflight() -> dict:
    assets = {}
    for target, price in zip(TARGETS, (311.34, 216.29, 326.0)):
        assets[target.symbol] = {
            "broker_position": {"symbol": target.symbol, "side": "long", "qty": "1", "current_price": str(price)},
            "strategy_position": {"symbol": target.symbol, "side": "long", "qty": 1},
            "open_orders": [],
            "reference_price": price,
        }
    return {
        "account": {
            "equity": "3536.88",
            "cash": "1843.25",
            "effective_buying_power": "9763.16",
            "buying_power": "9763.16",
            "daytrading_buying_power": "9763.16",
            "regt_buying_power": "4540.13",
            "maintenance_margin": "256.09",
            "shorting_enabled": True,
            "trading_blocked": False,
            "transfers_blocked": False,
            "account_blocked": False,
        },
        "target_assets": assets,
        "open_orders": [],
    }


class CamatronE1PromotionTests(unittest.TestCase):
    def test_only_three_intended_trader_records_change(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "old.json"
            config_path.write_text(json.dumps(old_style_config()), encoding="utf-8")
            current = json.loads(config_path.read_text(encoding="utf-8"))
            proposed, rows = apply_approved_targets(config_path, current)

        changed = [
            after["id"]
            for before, after in zip(current["traders"], proposed["traders"])
            if before != after
        ]

        self.assertEqual(set(changed), {target.trader_id for target in TARGETS})
        self.assertEqual({row["new_quantity"] for row in rows}, {8, 11, 7})
        self.assertEqual({row["new_strategy_type"] for row in rows}, {"renko_reversal"})
        self.assertEqual({row["new_entry_bricks"] for row in rows}, {1})
        self.assertEqual({row["new_risk_cap"] for row in rows}, {3500})

    def test_exact_approved_values_and_rollback_artifacts_are_written(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "old.json"
            out = Path(temp_dir) / "promotion"
            config_path.write_text(json.dumps(old_style_config()), encoding="utf-8")
            manifest = write_artifacts(config_path, out)
            proposed = json.loads((out / "proposed_config.json").read_text(encoding="utf-8"))
            rollback = json.loads((out / "rollback_config.json").read_text(encoding="utf-8"))
            plan_text = (out / "migration_plan.json").read_text(encoding="utf-8")

        for target in TARGETS:
            proposed_trader = next(trader for trader in proposed["traders"] if trader["id"] == target.trader_id)
            rollback_trader = next(trader for trader in rollback["traders"] if trader["id"] == target.trader_id)
            self.assertEqual(proposed_trader["strategy"]["brick_size"], target.brick_size)
            self.assertEqual(proposed_trader["sizing"]["qty"], target.quantity)
            self.assertEqual(proposed_trader["risk"]["max_position_notional"], 3500)
            self.assertEqual(rollback_trader["strategy"]["brick_size"], OLD_VALUES[target.symbol][0])
            self.assertEqual(rollback_trader["sizing"]["qty"], 1)
        self.assertNotEqual(manifest["current_config_hash"], manifest["proposed_config_hash"])
        self.assertIn("RESET_RENKO_ON_BRICK_CHANGE_PRESERVE_POSITION_V1", plan_text)

    def test_preflight_blocks_open_orders_reconciliation_failures_and_missing_prices(self) -> None:
        preflight = clean_preflight()
        rows = [{"symbol": target.symbol, "intended_first_reversal_quantity": target.quantity + 1} for target in TARGETS]
        self.assertEqual(validate_preflight(preflight, rows), [])

        with_open_order = copy.deepcopy(preflight)
        with_open_order["target_assets"]["AAPL"]["open_orders"] = [{"id": "order-1"}]
        self.assertIn("AAPL: open orders block promotion", validate_preflight(with_open_order, rows))

        with_disagreement = copy.deepcopy(preflight)
        with_disagreement["target_assets"]["NVDA"]["strategy_position"]["qty"] = 2
        self.assertIn("NVDA: broker and strategy positions disagree", validate_preflight(with_disagreement, rows))

        missing_price = copy.deepcopy(preflight)
        missing_price["target_assets"]["TSLA"]["reference_price"] = 0
        missing_price["target_assets"]["TSLA"]["broker_position"]["current_price"] = ""
        self.assertIn("TSLA: missing bootstrap reference price", validate_preflight(missing_price, rows))

    def test_concurrent_reversal_exposure_and_buying_power_are_evaluated(self) -> None:
        preflight = clean_preflight()
        rows = [{"symbol": target.symbol, "intended_first_reversal_quantity": target.quantity + 1} for target in TARGETS]
        exposure = exposure_report(preflight, rows)

        self.assertAlmostEqual(exposure["combined_target_gross_exposure"], 7151.91, places=2)
        self.assertAlmostEqual(exposure["combined_concurrent_reversal_order_notional"], 8005.54, places=2)
        self.assertAlmostEqual(exposure["effective_buying_power_headroom"], 1757.62, places=2)
        self.assertLess(exposure["regt_buying_power_headroom"], 0)

    def test_partial_migration_failure_fails_atomic_promotion(self) -> None:
        results = {"AAPL": {"status": "ok"}, "NVDA": {"status": "failed"}, "TSLA": {"status": "ok"}}

        self.assertEqual(validate_complete_migration(results), ["NVDA: migration did not complete"])

    def test_rollback_plan_uses_safe_brick_change_semantics(self) -> None:
        rows = [
            {
                "symbol": target.symbol,
                "trader_id": target.trader_id,
                "old_brick_size": target.brick_size,
                "new_brick_size": OLD_VALUES[target.symbol][0],
                "old_quantity": target.quantity,
                "new_quantity": 1,
            }
            for target in TARGETS
        ]

        plan = migration_plan(rows)

        self.assertTrue(all(item["state_reset_required"] for item in plan))
        self.assertTrue(all(item["preserve_broker_position"] for item in plan))
        self.assertTrue(all(item["bootstrap_without_signal"] for item in plan))


if __name__ == "__main__":
    unittest.main()
