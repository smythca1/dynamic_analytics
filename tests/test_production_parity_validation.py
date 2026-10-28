from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from optimization.source_preparation import load_prepared_observations
from replay.engine import run_replay
from replay.models import ReplayConfig
from scripts.validate_production_parity import run_production_parity
from silver.market.contract import market_stream_contract, renko_strategy_contract
from silver.market.models import CanonicalObservation
from strategy_engine import RenkoStrategyConfig, RenkoStrategyEngine


class ProductionParityValidationTests(unittest.TestCase):
    def _fixture(self, symbol: str, root: Path):
        strategy = dict(RenkoStrategyConfig(
            trader_id="parity-trader", symbol=symbol, brick_size=1.0, entry_bricks=1,
            strategy_type="renko_reversal", strategy_name="parity-fixture", qty=1.0,
            renko_anchor_mode="grid", renko_anchor_price=100.0, entry_start="09:31:00",
            entry_end="15:59:59", allow_overnight=True,
        ).__dict__)
        start = datetime(2026, 7, 20, 13, 30, tzinfo=timezone.utc)
        observations = [CanonicalObservation(
            observation_id=f"{symbol.lower()}-obs-{index}", run_id="parity-fixture", symbol=symbol,
            second_bucket=start + timedelta(minutes=index), emitted_sequence=index + 1,
            close_price=Decimal(price), source_eligible_trade_id=f"eligible-{index}",
            source_sip_timestamp=start + timedelta(minutes=index), source_alpaca_trade_id=f"trade-{index}",
        ) for index, price in enumerate(("100", "102", "100", "98", "100"))]
        source = root / f"{symbol.lower()}-observations.jsonl"
        source.write_text("".join(json.dumps(asdict(item), default=str, sort_keys=True) + "\n" for item in observations))
        checkpoint = root / f"{symbol.lower()}-checkpoint.json"
        snapshot = RenkoStrategyEngine(RenkoStrategyConfig(**strategy)).snapshot()
        checkpoint.write_text(json.dumps({"saved_at": start.isoformat(), "trader_id": "parity-trader",
                                          "symbol": symbol, "engine": snapshot}, default=str, sort_keys=True))
        loaded = tuple(load_prepared_observations(source))
        market = market_stream_contract(symbol=symbol, session_timezone="America/New_York")
        contract = renko_strategy_contract(strategy)
        replay = run_replay(ReplayConfig(
            run_id="fixture-replay", trader_id="parity-trader", symbol=symbol,
            start=loaded[0].second_bucket, end=loaded[-1].second_bucket, timezone="UTC",
            session_mode="configured", strategy_config=strategy, initial_capital="10000",
            brick_mode="BRICK_DOLLARS", brick_size_dollars="1", initial_strategy_state=snapshot,
            initial_renko_state=snapshot["renko"], start_state_path=str(checkpoint),
            start_state_completeness="complete", source_data_path=str(source),
            source_semantic_mode="prepared_canonical_observations",
            production_config={"market_contract_fingerprint": market.fingerprint,
                               "strategy_contract_fingerprint": contract.fingerprint},
            output_directory=str(root / "fixture-replay-output"), output_level="AUDIT",
        ))
        rows: list[dict[str, object]] = []
        for observation in replay.ledgers.canonical_observations:
            rows.append({"evidence_type": "production_observation", "symbol": symbol,
                         "trader_id": "parity-trader", "payload": {
                             "observation_bucket_timestamp": observation.second_bucket.isoformat(),
                             "close_price": str(observation.close_price),
                             "source_trade_identity": observation.source_record_id}})
        for brick in replay.ledgers.renko_bricks:
            rows.append({"evidence_type": "renko_brick", "symbol": symbol, "trader_id": "parity-trader",
                         "payload": {"brick_logical_timestamp": brick.timestamp.isoformat(),
                                     "source_observation_bucket_timestamp": None,
                                     "open": str(brick.open), "close": str(brick.close), "direction": brick.direction,
                                     "run_length": brick.run_len, "effective_brick_size": str(brick.brick_size)}})
        for signal in replay.ledgers.strategy_signals:
            rows.append({"evidence_type": "strategy_signal", "symbol": symbol, "trader_id": "parity-trader",
                         "payload": {"logical_signal_timestamp": signal.timestamp.isoformat(),
                                     "source_brick_timestamp": None, "side": signal.side,
                                     "reason": signal.reason, "strategy": signal.strategy,
                                     "signal_type": signal.signal_type,
                                     "target_direction": getattr(signal, "target_direction", None)}})
        for transition in replay.ledgers.position_transitions:
            rows.append({"evidence_type": "position_direction", "symbol": symbol, "trader_id": "parity-trader",
                         "payload": {"logical_transition_timestamp": transition.timestamp.isoformat(),
                                     "before_direction": transition.before.side,
                                     "after_direction": transition.after.side,
                                     "triggering_signal_sequence": getattr(transition, "triggering_signal_sequence", None)}})
        evidence = root / f"{symbol.lower()}-evidence.jsonl"
        evidence.write_text("".join(json.dumps(row, sort_keys=True, default=str) + "\n" for row in rows))
        return source, checkpoint, evidence, {"strategy": strategy, "market": market, "contract": contract}

    def _run(self, root: Path, symbol: str, evidence: Path | None = None):
        source, checkpoint, default_evidence, metadata = self._fixture(symbol, root)
        result = run_production_parity(
            source_path=source, evidence_path=evidence or default_evidence, start_state_path=checkpoint,
            strategy_config=metadata["strategy"], symbol=symbol, trader_id="parity-trader",
            run_id=f"{symbol.lower()}-run", output_directory=root / f"output-{symbol.lower()}",
            expected_market_fingerprint=metadata["market"].fingerprint,
            expected_strategy_fingerprint=metadata["contract"].fingerprint)
        return result, source, checkpoint, default_evidence

    def test_symbol_agnostic_exact_replay_and_determinism(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for symbol in ("TSLA", "MRVL"):
                first, _, _, _ = self._run(root, symbol)
                second, _, _, _ = self._run(root, symbol)
                self.assertEqual(first.classification, "PASS")
                self.assertEqual(first.parity.classification, "LOGIC_PARITY_VALIDATED")
                self.assertEqual(first.replay_fingerprint, second.replay_fingerprint)
                self.assertEqual(first.parity.renko_bricks.matched_count,
                                 first.parity.renko_bricks.replay_event_count)
                self.assertEqual(first.parity.strategy_signals.matched_count,
                                 first.parity.strategy_signals.replay_event_count)

    def test_first_deliberate_divergence_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result, _, _, evidence = self._run(root, "TSLA")
            self.assertEqual(result.classification, "PASS")
            rows = [json.loads(line) for line in evidence.read_text().splitlines()]
            brick = next(row for row in rows if row["evidence_type"] == "renko_brick")
            brick["payload"]["close"] = "999"
            divergent = root / "divergent.jsonl"
            divergent.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows))
            failed, _, _, _ = self._run(root, "TSLA", divergent)
            self.assertEqual(failed.classification, "FAIL")
            self.assertEqual(failed.parity.renko_bricks.first_mismatch_sequence, 1)

    def test_missing_initialization_or_evidence_is_incomplete(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, _, _, metadata = self._fixture("TSLA", root)
            result = run_production_parity(
                source_path=source, evidence_path=None, start_state_path=None,
                strategy_config=metadata["strategy"], symbol="TSLA", trader_id="parity-trader",
                run_id="incomplete", output_directory=root / "out",
                expected_market_fingerprint=metadata["market"].fingerprint,
                expected_strategy_fingerprint=metadata["contract"].fingerprint)
            self.assertEqual(result.classification, "INCOMPLETE")
            self.assertTrue(result.limitations)


if __name__ == "__main__":
    unittest.main()
