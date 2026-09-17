from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import scripts.friday_qty1_canary_orchestration_20260828 as canary
import scripts.post_fix_shared_sip_validation_20260825 as validation


class QuantityOneCanaryOrchestrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def write_json(self, path: Path, payload: dict) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
        return path

    def activate_at(self, time_et: str) -> tuple[dict, mock.Mock, mock.Mock]:
        broker = {"accounts": {"camatron": {}, "megatron": {}}}
        start = mock.Mock(side_effect=lambda unit: {"unit": unit, "before": {"NRestarts": "0"}, "after": {"ActiveState": "active", "NRestarts": "0"}})
        assert_flat = mock.Mock()
        with mock.patch.object(canary, "TARGET_DAY", "2026-08-31"), mock.patch.object(
            canary, "et_now", return_value=canary.pd.Timestamp(f"2026-08-31 {time_et}", tz=canary.ET)
        ), mock.patch.object(canary.r, "broker_snapshot", return_value=broker), mock.patch.object(
            canary.r, "assert_broker_flat", assert_flat
        ), mock.patch.object(
            canary, "start_fleet_once", start
        ), mock.patch.object(
            canary, "service_snapshot", return_value={}
        ):
            result = canary.activate(self.root / "activation")
        return result, start, assert_flat

    def sip_socket_snapshot(self, rows: list[str], *, shared_pid: str = "100") -> dict:
        with mock.patch.object(validation, "_resolve_sip_websocket_peer_ips", return_value=({"34.145.195.91"}, None)), mock.patch.object(
            validation, "systemctl_show", return_value={"MainPID": shared_pid}
        ), mock.patch.object(
            validation, "run", return_value=SimpleNamespace(stdout="\n".join(rows))
        ):
            return validation.tcp_python_443()

    def test_manifest_skips_declared_mutable_state_byte_checks_only(self) -> None:
        immutable = self.write_json(self.root / "immutable.py", {"version": 1})
        mutable = self.write_json(self.root / "state.json", {"position": None})
        manifest = self.write_json(
            self.root / "manifest.json",
            {
                "mutable_state_paths": [str(mutable)],
                "sha256": {
                    str(immutable): canary.sha256(immutable),
                    str(mutable): "stale-state-checksum",
                },
            },
        )

        result = canary.verify_manifest(manifest)

        self.assertEqual(result["actual"][str(immutable)], canary.sha256(immutable))
        self.assertEqual(result["mutable_paths_skipped_for_byte_checksum"], [str(mutable)])

    def test_manifest_still_rejects_immutable_mismatch(self) -> None:
        immutable = self.write_json(self.root / "immutable.py", {"version": 1})
        manifest = self.write_json(
            self.root / "manifest.json",
            {"mutable_state_paths": [], "sha256": {str(immutable): "bad"}},
        )

        with self.assertRaisesRegex(RuntimeError, "manifest checksum mismatches"):
            canary.verify_manifest(manifest)

    def test_warmup_intent_payload_is_execution_ineligible_with_lineage(self) -> None:
        runtime = SimpleNamespace(trader_id="t1", symbol="AAPL")
        signal = SimpleNamespace(strategy="s1", symbol="AAPL", side="long", broker_side="buy", price=101.25)
        intent = SimpleNamespace(qty=1, signal_type="entry")
        event = {
            "sequence": 42,
            "source_timestamp": "2026-08-28T13:33:20.123456Z",
            "observation_timestamp": "2026-08-28T13:33:20Z",
            "event_id": "event-42",
        }

        payload = canary.warmup_intent_payload(runtime, signal, intent, event)

        self.assertFalse(payload["execution_eligible"])
        self.assertEqual(payload["reason"], "pre_activation_warmup")
        self.assertEqual(payload["source_sequence"], 42)
        self.assertEqual(payload["source_timestamp"], "2026-08-28T13:33:20.123456Z")
        self.assertEqual(payload["quantity"], 1)

    def test_flat_state_validation_rejects_nested_engine_pending_intent(self) -> None:
        state = self.write_json(
            self.root / "state.json",
            {
                "contract_fingerprints": {
                    "market_stream": {"fingerprint": "fp-aapl"},
                },
                "canonical_market_checkpoints": {"processed_event_ids": {"AAPL": "event-1"}},
                "engine": {"trade": {"position": None, "pending_exit": None, "pending_order_intent": {"side": "buy"}}},
            },
        )
        market_cfg = SimpleNamespace(contract_fingerprints={"AAPL": "fp-aapl"})

        with mock.patch.object(canary.r, "EXPECTED_CAMATRON", {"t1": {"symbol": "AAPL", "market_fingerprint": "fp-aapl"}}), mock.patch.object(
            canary.r, "EXPECTED_MEGATRON", {}
        ):
            with self.assertRaisesRegex(RuntimeError, "pending_order_intent"):
                canary.validate_flat_state_payload(state, "t1", market_cfg)

    def test_interrupted_commit_marker_fails_closed_before_state_write(self) -> None:
        marker = self.write_json(self.root / canary.STATE_COMMIT_MARKER, {"status": "started"})

        with self.assertRaisesRegex(RuntimeError, "interrupted prior state commit detected"):
            canary.commit_staged_states(self.root, {"camatron": self.root, "megatron": self.root}, {}, {})

        self.assertEqual(json.loads(marker.read_text(encoding="utf-8"))["status"], "started")

    def test_stage_live_states_copies_current_files_to_staging_area(self) -> None:
        live = self.root / "artifacts/live/state/camatron_aapl.json"
        meg = self.root / "artifacts/live/megatron/state/megatron_tsla.json"
        self.write_json(live, {"state": "live"})
        self.write_json(meg, {"state": "meg"})
        with mock.patch.object(canary, "ROOT", self.root), mock.patch.object(canary.r, "EXPECTED_CAMATRON", {"camatron_aapl": {}}), mock.patch.object(
            canary.r, "EXPECTED_MEGATRON", {"megatron_tsla": {}}
        ):
            result = canary.stage_live_states(self.root / "evidence", "unit")

        copied = {Path(item["staged"]).name: item for item in result["copied"]}
        self.assertEqual(set(copied), {"camatron_aapl.json", "megatron_tsla.json"})
        for item in copied.values():
            self.assertEqual(item["source_sha256"], item["staged_sha256"])

    def test_stage_live_states_records_missing_new_regime_state(self) -> None:
        live = self.root / "artifacts/live/state/camatron_aapl.json"
        self.write_json(live, {"state": "live"})
        with mock.patch.object(canary, "ROOT", self.root), mock.patch.object(
            canary.r, "EXPECTED_CAMATRON", {"camatron_aapl": {}, "camatron_new": {}}
        ), mock.patch.object(canary.r, "EXPECTED_MEGATRON", {"megatron_new": {}}):
            result = canary.stage_live_states(self.root / "evidence", "unit")

        by_trader = {item["trader_id"]: item for item in result["copied"]}
        self.assertEqual(by_trader["camatron_aapl"]["source_sha256"], by_trader["camatron_aapl"]["staged_sha256"])
        self.assertEqual(by_trader["camatron_new"]["status"], "missing_new_regime_state")
        self.assertEqual(by_trader["megatron_new"]["status"], "missing_new_regime_state")

    def test_activate_rejects_before_0935_et(self) -> None:
        with mock.patch.object(canary, "TARGET_DAY", "2026-08-31"), mock.patch.object(
            canary, "et_now", return_value=canary.pd.Timestamp("2026-08-31 09:34:59", tz=canary.ET)
        ), mock.patch.object(canary, "start_fleet_once") as start:
            with self.assertRaisesRegex(RuntimeError, "outside approved window"):
                canary.activate(self.root / "activation")

        start.assert_not_called()

    def test_activate_allows_at_0935_et(self) -> None:
        result, start, assert_flat = self.activate_at("09:35:00")

        self.assertEqual(result["status"], "activated")
        self.assertEqual(start.call_count, 2)
        assert_flat.assert_called_once()

    def test_activate_allows_after_1000_before_1530_et(self) -> None:
        result, start, assert_flat = self.activate_at("10:01:00")

        self.assertEqual(result["status"], "activated")
        self.assertEqual(start.call_count, 2)
        assert_flat.assert_called_once()

    def test_activate_rejects_at_1530_et(self) -> None:
        with mock.patch.object(canary, "TARGET_DAY", "2026-08-31"), mock.patch.object(
            canary, "et_now", return_value=canary.pd.Timestamp("2026-08-31 15:30:00", tz=canary.ET)
        ), mock.patch.object(canary, "start_fleet_once") as start:
            with self.assertRaisesRegex(RuntimeError, "outside approved window"):
                canary.activate(self.root / "activation")

        start.assert_not_called()

    def test_activate_still_requires_broker_flatness(self) -> None:
        broker = {"accounts": {"camatron": {"target_positions": [{"symbol": "AAPL"}]}}}
        with mock.patch.object(canary, "TARGET_DAY", "2026-08-31"), mock.patch.object(
            canary, "et_now", return_value=canary.pd.Timestamp("2026-08-31 10:01:00", tz=canary.ET)
        ), mock.patch.object(canary.r, "broker_snapshot", return_value=broker), mock.patch.object(
            canary.r, "assert_broker_flat", side_effect=RuntimeError("target positions are not flat")
        ), mock.patch.object(
            canary, "start_fleet_once"
        ) as start:
            with self.assertRaisesRegex(RuntimeError, "target positions are not flat"):
                canary.activate(self.root / "activation")

        start.assert_not_called()

    def test_tp_sl_session_validation_uses_effective_source_contract(self) -> None:
        config = self.write_json(
            self.root / "config.json",
            {
                "traders": [
                    {
                        "id": "tp_sl",
                        "enabled": True,
                        "execution_enabled": True,
                        "symbol": "TSLA",
                        "data": {
                            "market_data_source": "shared_canonical_stream",
                            "renko_source": "1s_close",
                            "feed": "sip",
                        },
                        "strategy": {
                            "type": "renko_tp_sl",
                            "brick_size": 1.0,
                            "entry_bricks": 1,
                            "take_profit_bricks": 3,
                            "stop_loss_bricks": 1,
                            "exit_management": "bracket",
                            "candidate_config_fingerprint": "config-tsla",
                            "market_contract_fingerprint": "market-tsla",
                            "protection": {"emergency_flatten_enabled": True},
                        },
                        "session": {
                            "timezone": "America/New_York",
                            "entry_start": "09:31:00",
                            "entry_end": "15:59:59",
                            "force_flat": None,
                            "allow_overnight": True,
                        },
                        "sizing": {"qty": 1},
                    }
                ]
            },
        )

        result = validation.validate_expected_trader(
            config,
            {
                "tp_sl": {
                    "symbol": "TSLA",
                    "type": "renko_tp_sl",
                    "entry_bricks": 1,
                    "take_profit_bricks": 3,
                    "stop_loss_bricks": 1,
                    "qty": 1,
                    "exit_management": "bracket",
                }
            },
            reject_qqq=False,
        )

        self.assertEqual(result[0]["id"], "tp_sl")

    def test_sip_socket_count_ignores_broker_https_connection(self) -> None:
        snapshot = self.sip_socket_snapshot(
            [
                'ESTAB 0 0 157.230.54.46:44766 34.145.195.91:443 users:(("python",pid=100,fd=8))',
                'ESTAB 0 0 157.230.54.46:48292 35.221.23.121:443 users:(("python",pid=200,fd=7))',
            ]
        )

        self.assertEqual(snapshot["python_443_count"], 1)
        self.assertEqual(snapshot["sip_websocket_count"], 1)
        self.assertEqual(snapshot["ignored_python_443_count"], 1)
        self.assertTrue(snapshot["sip_websocket_invariant_ok"])

    def test_sip_socket_count_detects_duplicate_sip_connections(self) -> None:
        snapshot = self.sip_socket_snapshot(
            [
                'ESTAB 0 0 157.230.54.46:44766 34.145.195.91:443 users:(("python",pid=100,fd=8))',
                'ESTAB 0 0 157.230.54.46:48292 34.145.195.91:443 users:(("python",pid=200,fd=7))',
            ]
        )

        self.assertEqual(snapshot["python_443_count"], 2)
        self.assertEqual(snapshot["sip_websocket_count"], 2)
        self.assertFalse(snapshot["sip_websocket_invariant_ok"])

    def test_sip_socket_count_detects_zero_sip_connections(self) -> None:
        snapshot = self.sip_socket_snapshot(
            [
                'ESTAB 0 0 157.230.54.46:48292 35.221.23.121:443 users:(("python",pid=200,fd=7))',
            ]
        )

        self.assertEqual(snapshot["python_443_count"], 0)
        self.assertEqual(snapshot["sip_websocket_count"], 0)
        self.assertEqual(snapshot["ignored_python_443_count"], 1)
        self.assertFalse(snapshot["sip_websocket_invariant_ok"])

    def test_sip_socket_count_requires_shared_service_ownership(self) -> None:
        snapshot = self.sip_socket_snapshot(
            [
                'ESTAB 0 0 157.230.54.46:48292 34.145.195.91:443 users:(("python",pid=200,fd=7))',
            ]
        )

        self.assertEqual(snapshot["sip_websocket_count"], 1)
        self.assertEqual(snapshot["python_443_count"], 0)
        self.assertFalse(snapshot["sip_websocket_owned_by_shared_service"])
        self.assertFalse(snapshot["sip_websocket_invariant_ok"])


if __name__ == "__main__":
    unittest.main()
