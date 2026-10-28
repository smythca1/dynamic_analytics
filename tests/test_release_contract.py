from __future__ import annotations

import copy
import tempfile
import unittest
from pathlib import Path

from shared.deployment.release_contract import (
    build_release_contract,
    evaluate_broker_snapshot,
    evaluate_checkpoint_snapshot,
    plan_change,
    validate_historical_checkpoint_authorizations,
    validate_release_contract,
)


class ReleaseContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.traders = {"traders": [{
            "id": "tsla-qty8",
            "enabled": True,
            "symbol": "TSLA",
            "sizing": {"qty": 8},
            "strategy": {"type": "renko_reversal"},
        }]}
        self.market = {"symbols": ["TSLA", "SPXC"], "contract_fingerprints": {"TSLA": "t", "SPXC": "s"}}
        (self.root / "traders.json").write_text("{}\n", encoding="utf-8")
        (self.root / "market.json").write_text("{}\n", encoding="utf-8")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def contract(self, traders=None, market=None, retired=()):
        return build_release_contract(
            commit="a" * 40,
            config_paths=[self.root / "traders.json", self.root / "market.json"],
            trader_config=traders or self.traders,
            market_config=market or self.market,
            service_dependencies={"trading-fleet-live.service": ["TSLA"]},
            retired_symbols=retired,
        )

    def test_quantity_only_change_is_a_trader_change(self):
        old = self.contract()
        traders = copy.deepcopy(self.traders)
        traders["traders"][0]["sizing"]["qty"] = 5
        new = self.contract(traders=traders)
        self.assertEqual(plan_change(old, new)["changed_traders"], ["tsla-qty8"])
        self.assertTrue(plan_change(old, new)["broker_preflight_required"])

    def test_removed_symbol_is_reported_and_retirement_is_fail_closed(self):
        old = self.contract()
        market = {"symbols": ["SPXC"], "contract_fingerprints": {"SPXC": "s"}}
        traders = {"traders": []}
        new = self.contract(traders=traders, market=market, retired=("TSLA",))
        plan = plan_change(old, new)
        self.assertEqual(plan["removed_market_symbols"], ["TSLA"])
        with self.assertRaisesRegex(ValueError, "resurrect retired"):
            self.contract(retired=("TSLA",))

    def test_enabled_symbol_must_have_shared_contract(self):
        with self.assertRaisesRegex(ValueError, "absent from shared"):
            self.contract(market={"symbols": ["SPXC"], "contract_fingerprints": {"SPXC": "s"}})

    def test_changed_fingerprint_requires_shared_service_impact(self):
        old = self.contract()
        market = {"symbols": ["TSLA", "SPXC"], "contract_fingerprints": {"TSLA": "new", "SPXC": "s"}}
        new = self.contract(market=market)
        self.assertTrue(plan_change(old, new)["market_contract_changed"])
        self.assertTrue(plan_change(old, new)["checkpoint_preflight_required"])

    def test_legacy_contract_remains_validatable_only_when_complete(self):
        contract = self.contract()
        validate_release_contract(contract)
        with self.assertRaisesRegex(ValueError, "immutable git commit"):
            validate_release_contract({**contract, "git_commit": ""})

    def test_broker_flat_changed_trader_passes(self):
        result = evaluate_broker_snapshot(
            {"account_id": "acct", "api_health": "ok", "complete": True, "stale": False,
             "positions": [], "open_orders_nested": []},
            expected_account_id="acct", affected_symbols=["TSLA"], require_flat_symbols=["TSLA"],
        )
        self.assertEqual(result["status"], "PASS")

    def test_broker_exposure_and_nested_order_block_retirement(self):
        result = evaluate_broker_snapshot(
            {"account_id": "acct", "api_health": "ok", "complete": True, "stale": False,
             "positions": [{"symbol": "MRVL", "qty": "1"}],
             "open_orders_nested": [{"symbol": "MRVL", "status": "partially_filled"}]},
            expected_account_id="acct", affected_symbols=["MRVL"], require_flat_symbols=["MRVL"],
        )
        self.assertEqual(result["status"], "BLOCKED")
        self.assertEqual(len(result["errors"]), 2)

    def test_missing_or_wrong_account_fails_closed(self):
        result = evaluate_broker_snapshot(
            {"account_id": "other", "api_health": "ok", "complete": False,
             "positions": [], "open_orders_nested": []},
            expected_account_id="acct", affected_symbols=["TSLA"],
        )
        self.assertEqual(result["status"], "BLOCKED")

    def test_incompatible_active_checkpoint_is_rejected_and_retired_is_historical(self):
        result = evaluate_checkpoint_snapshot(
            {"tsla-qty8": {"trader_id": "tsla-qty8", "symbol": "TSLA", "quantity": 5,
                           "position": {"qty": 0}},
             "old-mrvl": {"trader_id": "old-mrvl"}},
            candidate_traders=[{"id": "tsla-qty8", "symbol": "TSLA", "quantity": 8}],
            broker_positions={"TSLA": 0, "MRVL": 0}, retired_trader_ids=["old-mrvl"],
        )
        self.assertEqual(result["status"], "BLOCKED")
        self.assertEqual(len(result["errors"]), 1)
        self.assertEqual(result["historical"], [{"trader_id": "old-mrvl", "classification": "historical_non_restorable"}])

    def test_retired_checkpoint_is_safe_when_broker_flat(self):
        result = evaluate_checkpoint_snapshot(
            {"old-mrvl": {"trader_id": "old-mrvl", "symbol": "MRVL", "quantity": 1}},
            candidate_traders=[], broker_positions={"MRVL": 0}, retired_trader_ids=["old-mrvl"],
        )
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["historical"][0]["classification"], "historical_non_restorable")

    def test_replaced_checkpoint_is_historical_and_new_identity_can_initialize_flat(self):
        result = evaluate_checkpoint_snapshot(
            {"tsla-qty5": {"trader_id": "tsla-qty5", "symbol": "TSLA", "quantity": 5}, "tsla-qty8": None},
            candidate_traders=[{"id": "tsla-qty8", "symbol": "TSLA", "quantity": 8}],
            broker_positions={"TSLA": 0}, retired_trader_ids=["tsla-qty5"],
        )
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["historical"][0]["trader_id"], "tsla-qty5")

    def test_unknown_checkpoint_identity_is_rejected(self):
        result = evaluate_checkpoint_snapshot(
            {"unknown": {"trader_id": "unknown", "symbol": "TSLA"}},
            candidate_traders=[{"id": "tsla-qty8", "symbol": "TSLA", "quantity": 8}],
            broker_positions={"TSLA": 0}, retired_trader_ids=[],
        )
        self.assertEqual(result["status"], "BLOCKED")
        self.assertIn("authorized release consumers", result["errors"][0])

    def test_unchanged_independent_consumer_checkpoint_is_active(self):
        result = evaluate_checkpoint_snapshot(
            {"spxc": {"trader_id": "spxc", "symbol": "SPXC", "quantity": 2, "position": {"qty": 0}}},
            candidate_traders=[{"id": "spxc", "symbol": "SPXC", "quantity": 2}],
            broker_positions={"SPXC": 0}, retired_trader_ids=[],
        )
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["historical"], [])

    def test_missing_checkpoint_is_safe_only_when_broker_flat(self):
        result = evaluate_checkpoint_snapshot(
            {"tsla-qty8": None},
            candidate_traders=[{"id": "tsla-qty8", "symbol": "TSLA", "quantity": 8}],
            broker_positions={"TSLA": 0},
        )
        self.assertEqual(result["status"], "PASS")

    def _historical_record(self, **overrides):
        record = {
            "trader_id": "tsla-qty5-old",
            "checkpoint_path": "artifacts/live/state/tsla-qty5-old.json",
            "sha256": "a" * 64,
            "symbol": "TSLA",
            "strategy_type": "renko_reversal",
            "strategy_identity": "canonical_e1",
            "classification": "verified_historical",
            "restoration": "historical_non_restorable",
            "provenance": {"release_commit": "b" * 40},
            "reviewed_by": "operator",
            "reviewed_at_utc": "2026-10-28T18:00:00Z",
            "broker_evidence": {"position_flat": True, "open_orders": 0, "verified_at_utc": "2026-10-28T18:00:00Z"},
        }
        record.update(overrides)
        return record

    def test_verified_historical_authorization_matches_exact_inventory(self):
        record = self._historical_record()
        result = validate_historical_checkpoint_authorizations(
            [record],
            [{"path": record["checkpoint_path"], "sha256": record["sha256"], "trader_id": record["trader_id"],
              "symbol": "TSLA", "strategy_type": "renko_reversal", "strategy_identity": "canonical_e1"}],
        )
        self.assertEqual(result["status"], "PASS")

    def test_legacy_authorization_requires_explicit_uncertainty(self):
        record = self._historical_record(classification="legacy_authorized_non_restorable", provenance=None,
                                         provenance_uncertainty="original release unavailable; operator reviewed")
        self.assertEqual(validate_historical_checkpoint_authorizations([record], [])["status"], "PASS")

    def test_historical_authorization_rejects_tampering_duplicates_and_active_conflict(self):
        record = self._historical_record()
        tampered = validate_historical_checkpoint_authorizations(
            [record], [{"path": record["checkpoint_path"], "sha256": "c" * 64, "trader_id": record["trader_id"],
                        "symbol": "TSLA", "strategy_type": "renko_reversal", "strategy_identity": "canonical_e1"}],
        )
        self.assertEqual(tampered["status"], "BLOCKED")
        self.assertEqual(validate_historical_checkpoint_authorizations([record, record], [])["status"], "BLOCKED")
        self.assertEqual(validate_historical_checkpoint_authorizations([record], [], active_trader_ids=[record["trader_id"]])["status"], "BLOCKED")


if __name__ == "__main__":
    unittest.main()
