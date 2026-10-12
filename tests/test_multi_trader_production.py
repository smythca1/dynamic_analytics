import json
import unittest
from copy import deepcopy
from pathlib import Path

import pandas as pd

from shared.config.trader_config import TraderConfigError, validate_production_fleet_config
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
        consumer.gapped_symbols.add("SPXC")
        self.assertIsNone(consumer.signal_block_reason("TSLA"))
        self.assertEqual(consumer.signal_block_reason("SPXC"), "shared_market_data_gap")
        self.assertIsNone(consumer.signal_block_reason("MRVL"))


if __name__ == "__main__":
    unittest.main()
