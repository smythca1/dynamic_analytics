import json
import unittest
from copy import deepcopy
from pathlib import Path

import pandas as pd

from shared.config.trader_config import TraderConfigError, production_consumer_identity, validate_production_fleet_config
from silver.market.ipc_consumer import SharedCanonicalStreamConsumer


class MultiTraderProductionTests(unittest.TestCase):
    def _three_trader_config(self):
        tsla_config = json.loads(Path("configs/traders.live.example.json").read_text())
        spxc_config = json.loads(Path("configs/accounts/spxc.live.json").read_text())
        tsla = deepcopy(next(item for item in tsla_config["traders"] if item.get("enabled")))
        spxc = deepcopy(spxc_config["traders"][0])
        mrvl = deepcopy(tsla)
        mrvl.update({"id": "camatron_mrvl_reversal_test_live", "symbol": "MRVL"})
        mrvl["sizing"]["qty"] = 1
        mrvl["strategy"]["brick_size"] = 1.0
        mrvl["profile"]["renko_brick_pct"] = "0.40"
        return {"fleet_id": "test-three-trader", "traders": [tsla, spxc, mrvl]}

    def test_three_distinct_traders_share_generic_contract(self):
        validate_production_fleet_config(self._three_trader_config())

    def test_duplicate_symbol_fails_closed(self):
        config = self._three_trader_config()
        config["traders"][2]["symbol"] = "TSLA"
        with self.assertRaisesRegex(TraderConfigError, "symbols must be unique"):
            validate_production_fleet_config(config)

    def test_consumer_identity_is_deterministic_and_trader_scoped(self):
        config = self._three_trader_config()
        identities = [production_consumer_identity(trader) for trader in config["traders"]]
        self.assertEqual(identities[0], "camatron:" + config["traders"][0]["id"])
        self.assertEqual(len(identities), len(set(identities)))

    def test_duplicate_consumer_identity_fails_closed(self):
        config = self._three_trader_config()
        config["traders"][2]["id"] = config["traders"][0]["id"]
        config["traders"][2]["symbol"] = "NVDA"
        with self.assertRaisesRegex(TraderConfigError, "trader ids must be unique"):
            validate_production_fleet_config(config)

    def test_symbol_local_gap_does_not_block_other_symbol(self):
        consumer = SharedCanonicalStreamConsumer(
            socket_path="/tmp/unused.sock",
            identity="test",
            expected_contract_fingerprints={},
            state_path="/tmp/unused-state.json",
            symbols=("TSLA", "SPXC", "MRVL"),
        )
        consumer.catchup_complete = True
        consumer.last_observation_at_by_symbol["TSLA"] = pd.Timestamp.now(tz="UTC")
        consumer.last_observation_at_by_symbol["SPXC"] = pd.Timestamp.now(tz="UTC")
        consumer.last_observation_at_by_symbol["MRVL"] = pd.Timestamp.now(tz="UTC")
        consumer.startup_ready_symbols.update({"TSLA", "SPXC", "MRVL"})
        now = pd.Timestamp.now(tz="UTC").isoformat()
        for symbol in ("TSLA", "SPXC", "MRVL"):
            consumer.symbol_health[symbol] = {
                "symbol": symbol,
                "journal_healthy": True,
                "gap_count": 0,
                "last_raw_trade": now,
                "canonical_flush_expected": False,
            }
        consumer.gapped_symbols.add("SPXC")
        self.assertIsNone(consumer.signal_block_reason("TSLA"))
        self.assertEqual(consumer.signal_block_reason("SPXC"), "shared_market_data_gap")
        self.assertIsNone(consumer.signal_block_reason("MRVL"))

    def _ready_consumer(self):
        consumer = SharedCanonicalStreamConsumer(
            socket_path="/tmp/unused.sock",
            identity="test",
            expected_contract_fingerprints={},
            state_path="/tmp/unused-state.json",
            symbols=("TSLA", "SPXC"),
            freshness_threshold_seconds=5.0,
        )
        consumer.catchup_complete = True
        consumer.startup_ready_symbols.update({"TSLA", "SPXC"})
        return consumer

    def test_sparse_active_symbol_is_quiet_healthy(self):
        consumer = self._ready_consumer()
        consumer.symbol_health["SPXC"] = {
            "symbol": "SPXC",
            "journal_healthy": True,
            "gap_count": 0,
            "last_raw_trade": pd.Timestamp.now(tz="UTC").isoformat(),
            "last_eligible_trade": None,
            "canonical_flush_expected": False,
        }
        self.assertIsNone(consumer.signal_block_reason("SPXC"))

    def test_later_eligible_second_without_publication_blocks_symbol(self):
        consumer = self._ready_consumer()
        consumer.symbol_health["SPXC"] = {
            "symbol": "SPXC",
            "journal_healthy": True,
            "gap_count": 0,
            "last_raw_trade": pd.Timestamp.now(tz="UTC").isoformat(),
            "last_eligible_trade": pd.Timestamp.now(tz="UTC").isoformat(),
            "canonical_flush_expected": True,
            "canonical_flush_expected_at": (pd.Timestamp.now(tz="UTC") - pd.Timedelta(seconds=10)).isoformat(),
        }
        self.assertEqual(
            consumer.signal_block_reason("SPXC"),
            "shared_market_data_canonical_emission_failed",
        )

    def test_symbol_raw_quiet_does_not_block_other_symbol(self):
        consumer = self._ready_consumer()
        old = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(seconds=10)).isoformat()
        consumer.symbol_health["SPXC"] = {
            "symbol": "SPXC",
            "journal_healthy": True,
            "gap_count": 0,
            "last_raw_trade": old,
            "canonical_flush_expected": False,
        }
        consumer.symbol_health["TSLA"] = {
            "symbol": "TSLA",
            "journal_healthy": True,
            "gap_count": 0,
            "last_raw_trade": pd.Timestamp.now(tz="UTC").isoformat(),
            "canonical_flush_expected": False,
        }
        self.assertIsNone(consumer.signal_block_reason("SPXC"))
        self.assertIsNone(consumer.signal_block_reason("TSLA"))

    def test_explicit_symbol_delivery_failure_blocks_only_symbol(self):
        consumer = self._ready_consumer()
        consumer.symbol_health["SPXC"] = {
            "symbol": "SPXC",
            "journal_healthy": True,
            "gap_count": 0,
            "symbol_delivery_failure": "subscription_rejected",
            "canonical_flush_expected": False,
        }
        consumer.symbol_health["TSLA"] = {
            "symbol": "TSLA",
            "journal_healthy": True,
            "gap_count": 0,
            "canonical_flush_expected": False,
        }
        self.assertEqual(
            consumer.signal_block_reason("SPXC"),
            "shared_market_data_symbol_delivery_failed_subscription_rejected",
        )
        self.assertIsNone(consumer.signal_block_reason("TSLA"))

    def test_startup_requires_two_live_observations(self):
        consumer = self._ready_consumer()
        consumer.startup_ready_symbols.clear()
        consumer.symbol_health["SPXC"] = {
            "symbol": "SPXC",
            "journal_healthy": True,
            "gap_count": 0,
            "last_raw_trade": pd.Timestamp.now(tz="UTC").isoformat(),
            "canonical_flush_expected": False,
        }
        self.assertEqual(consumer.signal_block_reason("SPXC"), "shared_market_data_not_ready")


if __name__ == "__main__":
    unittest.main()
