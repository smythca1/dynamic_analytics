from __future__ import annotations

import copy
import json
import unittest
from pathlib import Path

from silver.market.shared_service import (
    SharedMarketDataConfig,
    validate_shared_market_data_config,
)


ROOT = Path(__file__).resolve().parents[1]


def load_json(relative_path: str) -> dict:
    return json.loads((ROOT / relative_path).read_text(encoding="utf-8"))


class MarketDataDeploymentPreflightTests(unittest.TestCase):
    MRVL_FINGERPRINT = "5f5acfd09a4c805c756e5ce6447d5f1ce31b6ddcb6a8130586e57d88b4774dea"

    def setUp(self) -> None:
        self.shared_payload = load_json("configs/market-data.live.example.json")
        self.fleet_configs = [
            load_json("configs/traders.live.example.json"),
            load_json("configs/accounts/spxc.live.json"),
            load_json("configs/accounts/megatron.live.json"),
        ]

    def payload_with_explicit_mrvl_contract(self) -> dict:
        payload = copy.deepcopy(self.shared_payload)
        payload["symbols"].append("MRVL")
        payload["contract_fingerprints"]["MRVL"] = self.MRVL_FINGERPRINT
        return payload

    def test_missing_mrvl_fingerprint_is_rejected_before_shared_config_creation(self) -> None:
        payload = self.payload_with_explicit_mrvl_contract()
        del payload["contract_fingerprints"]["MRVL"]

        with self.assertRaisesRegex(ValueError, "missing contract fingerprints.*MRVL"):
            SharedMarketDataConfig.from_mapping(payload)

    def test_absent_fingerprint_map_is_rejected(self) -> None:
        payload = copy.deepcopy(self.shared_payload)
        payload.pop("contract_fingerprints")

        with self.assertRaisesRegex(ValueError, "missing contract fingerprints"):
            SharedMarketDataConfig.from_mapping(payload)

    def test_incorrect_fingerprint_value_is_rejected(self) -> None:
        payload = self.payload_with_explicit_mrvl_contract()
        payload["contract_fingerprints"]["MRVL"] = "october-12-bad-fingerprint"

        with self.assertRaisesRegex(ValueError, "do not match canonical contracts"):
            SharedMarketDataConfig.from_mapping(payload)

    def test_valid_complete_configuration_passes(self) -> None:
        config = SharedMarketDataConfig.from_mapping(self.shared_payload)

        validate_shared_market_data_config(config, self.fleet_configs)

    def test_newly_enabled_trader_absent_from_shared_configuration_is_rejected(self) -> None:
        fleet = copy.deepcopy(self.fleet_configs[0])
        enabled_trader = next(trader for trader in fleet["traders"] if trader.get("enabled") and trader.get("execution_enabled"))
        newly_enabled = copy.deepcopy(enabled_trader)
        newly_enabled["id"] = "newly-enabled-absent-symbol"
        newly_enabled["symbol"] = "AMD"
        fleet["traders"].append(newly_enabled)
        config = SharedMarketDataConfig.from_mapping(self.shared_payload)

        with self.assertRaisesRegex(ValueError, "missing enabled symbols.*AMD"):
            validate_shared_market_data_config(config, [fleet, self.fleet_configs[1]])

    def test_october_12_mrvl_failure_is_blocked_by_preflight(self) -> None:
        payload = self.payload_with_explicit_mrvl_contract()
        del payload["contract_fingerprints"]["MRVL"]

        with self.assertRaisesRegex(ValueError, "missing contract fingerprints.*MRVL"):
            config = SharedMarketDataConfig.from_mapping(payload)
            validate_shared_market_data_config(config, self.fleet_configs)


if __name__ == "__main__":
    unittest.main()
