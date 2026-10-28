from __future__ import annotations

import copy
import tempfile
import unittest
from pathlib import Path

from shared.deployment.release_contract import (
    build_release_contract,
    plan_change,
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


if __name__ == "__main__":
    unittest.main()
