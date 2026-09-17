from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts.renko_cutover_migration import MigrationError, build_parser, run_migration


class RenkoCutoverMigrationTests(unittest.TestCase):
    def run_args(self, root: Path, *, output_name: str = "stage", broker_position: dict | None = None, brick_size: float = 10.0, rows: list[dict] | None = None):
        config = self.write_config(root, brick_size=brick_size)
        state = self.write_flat_state(root, brick_size=brick_size)
        broker = self.write_broker_snapshot(root, broker_position=broker_position)
        sip = self.write_sip(root, rows=rows)
        return build_parser().parse_args(
            [
                "--mode",
                "megatron-flat-boundary",
                "--account",
                "megatron",
                "--trader-id",
                "unit_megatron_aapl",
                "--symbol",
                "AAPL",
                "--cutoff",
                "2026-08-10T13:30:10Z",
                "--config",
                str(config),
                "--state",
                str(state),
                "--broker-snapshot",
                str(broker),
                "--sip-file",
                str(sip),
                "--session-date",
                "2026-08-10",
                "--output-dir",
                str(root / output_name),
                "--old-renko-source",
                "ticks",
            ]
        )

    def write_config(self, root: Path, *, brick_size: float) -> Path:
        path = root / "target.json"
        path.write_text(
            json.dumps(
                {
                    "traders": [
                        {
                            "id": "unit_megatron_aapl",
                            "enabled": True,
                            "execution_enabled": False,
                            "symbol": "AAPL",
                            "account_namespace": "megatron",
                            "strategy": {
                                "type": "renko_tp_sl",
                                "brick_size": brick_size,
                                "entry_bricks": 1,
                                "take_profit_bricks": 2,
                                "stop_loss_bricks": 1,
                                "renko_anchor_mode": "first_price",
                            },
                            "sizing": {"qty": 1},
                            "session": {"timezone": "America/New_York", "allow_overnight": True},
                            "data": {"feed": "sip", "renko_source": "1s_close"},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        return path

    def write_flat_state(self, root: Path, *, brick_size: float) -> Path:
        path = root / "state.json"
        path.write_text(
            json.dumps(
                {
                    "configuration_hash": "old-raw-ticks",
                    "engine": {
                        "symbol": "AAPL",
                        "strategy_type": "renko_tp_sl",
                        "strategy_name": "unit",
                        "renko": {
                            "brick_size": brick_size,
                            "last_brick_close": None,
                            "run_dir": 0,
                            "run_len": 0,
                            "processed_bricks": 0,
                            "anchor_mode": "first_price",
                            "anchor_price": None,
                        },
                        "trade": {
                            "position": None,
                            "tp_sl_lifecycle_state": "FLAT",
                            "pending_exit": None,
                            "last_closed_exit": {"order_id": "old"},
                            "force_flat_dates": [],
                        },
                    },
                }
            ),
            encoding="utf-8",
        )
        return path

    def write_broker_snapshot(self, root: Path, *, broker_position: dict | None) -> Path:
        path = root / "broker.json"
        positions = [broker_position] if broker_position else []
        path.write_text(
            json.dumps(
                {
                    "accounts": {
                        "megatron": {
                            "account": {"id": "acct-megatron"},
                            "positions": positions,
                            "open_orders": [],
                            "recent_orders": [],
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        return path

    def write_sip(self, root: Path, *, rows: list[dict] | None = None) -> Path:
        path = root / "bronze" / "AAPL" / "2026-08-10" / "sip_trades.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        if rows is None:
            rows = [
                {"timestamp": "2026-08-10T13:30:00.100Z", "symbol": "AAPL", "price": 100.0, "size": 1, "trade_id": "t0", "conditions": ["@"], "source_sequence": 0},
                {"timestamp": "2026-08-10T13:30:01.100Z", "symbol": "AAPL", "price": 100.1, "size": 1, "trade_id": "t1", "conditions": ["@"], "source_sequence": 1},
                {"timestamp": "2026-08-10T13:30:02.100Z", "symbol": "AAPL", "price": 100.2, "size": 1, "trade_id": "t2", "conditions": ["@"], "source_sequence": 2},
            ]
        path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")
        return path

    def test_flat_boundary_staging_is_byte_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = run_migration(self.run_args(root, output_name="stage-a"))
            second = run_migration(self.run_args(root, output_name="stage-b"))

            self.assertEqual(first["status"], "STAGED")
            self.assertEqual(
                (root / "stage-a" / "state.json").read_bytes(),
                (root / "stage-b" / "state.json").read_bytes(),
            )
            staged = json.loads((root / "stage-a" / "state.json").read_text(encoding="utf-8"))
            self.assertEqual(staged["migration_marker"]["type"], "CONTRACT_MIGRATION_FLAT")
            self.assertEqual(staged["engine"]["trade"]["position"], None)
            self.assertEqual(staged["pending_orders_state"]["status"], "VERIFIED_NONE")

    def test_validation_only_writes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args = self.run_args(root)
            args.validation_only = True
            result = run_migration(args)
            self.assertEqual(result["status"], "VALIDATION_ONLY_PASS")
            self.assertFalse((root / "stage").exists())

    def test_flat_boundary_refuses_nonflat_broker_position(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args = self.run_args(
                root,
                broker_position={"symbol": "AAPL", "qty": "-1", "side": "short", "avg_entry_price": "100.00"},
            )
            with self.assertRaisesRegex(MigrationError, "requires broker flat"):
                run_migration(args)

    def test_refuses_live_or_relative_output_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args = self.run_args(root)
            args.output_dir = "relative/stage"
            with self.assertRaisesRegex(MigrationError, "absolute path"):
                run_migration(args)

            args = self.run_args(root)
            args.output_dir = f"{root}/artifacts/live/stage"
            with self.assertRaisesRegex(MigrationError, "live production artifacts"):
                run_migration(args)

    def test_checkpoint_validation_fails_when_runtime_restore_drops_processed_bricks(self) -> None:
        rows = [
            {"timestamp": "2026-08-10T13:30:00.100Z", "symbol": "AAPL", "price": 100.0, "size": 1, "trade_id": "t0", "conditions": ["@"], "source_sequence": 0},
            {"timestamp": "2026-08-10T13:30:01.100Z", "symbol": "AAPL", "price": 100.0, "size": 1, "trade_id": "t1", "conditions": ["@"], "source_sequence": 1},
            {"timestamp": "2026-08-10T13:30:02.100Z", "symbol": "AAPL", "price": 102.0, "size": 1, "trade_id": "t2", "conditions": ["@"], "source_sequence": 2},
            {"timestamp": "2026-08-10T13:30:03.100Z", "symbol": "AAPL", "price": 103.0, "size": 1, "trade_id": "t3", "conditions": ["@"], "source_sequence": 3},
            {"timestamp": "2026-08-10T13:30:04.100Z", "symbol": "AAPL", "price": 104.0, "size": 1, "trade_id": "t4", "conditions": ["@"], "source_sequence": 4},
            {"timestamp": "2026-08-10T13:30:05.100Z", "symbol": "AAPL", "price": 105.0, "size": 1, "trade_id": "t5", "conditions": ["@"], "source_sequence": 5},
            {"timestamp": "2026-08-10T13:30:06.100Z", "symbol": "AAPL", "price": 106.0, "size": 1, "trade_id": "t6", "conditions": ["@"], "source_sequence": 6},
            {"timestamp": "2026-08-10T13:30:07.100Z", "symbol": "AAPL", "price": 107.0, "size": 1, "trade_id": "t7", "conditions": ["@"], "source_sequence": 7},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            args = self.run_args(root, brick_size=1.0, rows=rows)
            with self.assertRaisesRegex(MigrationError, "processed_bricks"):
                run_migration(args)

    def test_camatron_continuation_refuses_engine_state_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / "camatron.json"
            config.write_text(
                json.dumps(
                    {
                        "traders": [
                            {
                                "id": "unit_camatron_aapl",
                                "enabled": True,
                                "execution_enabled": False,
                                "symbol": "AAPL",
                                "strategy": {
                                    "type": "renko_reversal",
                                    "brick_size": 1.0,
                                    "entry_bricks": 1,
                                    "renko_anchor_mode": "grid",
                                    "renko_anchor_price": 0.0,
                                },
                                "sizing": {"qty": 1},
                                "session": {"timezone": "America/New_York", "allow_overnight": True},
                                "data": {"feed": "sip", "renko_source": "1s_close"},
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            state = root / "state.json"
            state.write_text(
                json.dumps(
                    {
                        "engine": {
                            "symbol": "AAPL",
                            "strategy_type": "renko_reversal",
                            "strategy_name": "unit",
                            "renko": {
                                "brick_size": 1.0,
                                "last_brick_close": 999.0,
                                "run_dir": 1,
                                "run_len": 1,
                                "processed_bricks": 1,
                                "anchor_mode": "grid",
                                "anchor_price": 0.0,
                            },
                            "trade": {
                                "position": {"symbol": "AAPL", "side": "long", "entry_time": "2026-08-10T13:30:01+00:00", "entry_price": 999.0, "qty": 1},
                                "tp_sl_lifecycle_state": "POSITION_OPEN",
                                "pending_exit": None,
                                "force_flat_dates": [],
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )
            broker = root / "broker.json"
            broker.write_text(
                json.dumps(
                    {
                        "accounts": {
                            "camatron": {
                                "account": {"id": "acct-camatron"},
                                "positions": [{"symbol": "AAPL", "qty": "1", "side": "long", "avg_entry_price": "999.0"}],
                                "open_orders": [],
                                "recent_orders": [],
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            sip = self.write_sip(root)
            args = build_parser().parse_args(
                [
                    "--mode",
                    "camatron-continuation",
                    "--account",
                    "camatron",
                    "--trader-id",
                    "unit_camatron_aapl",
                    "--symbol",
                    "AAPL",
                    "--cutoff",
                    "2026-08-10T13:30:10Z",
                    "--config",
                    str(config),
                    "--state",
                    str(state),
                    "--broker-snapshot",
                    str(broker),
                    "--sip-file",
                    str(sip),
                    "--session-date",
                    "2026-08-10",
                    "--output-dir",
                    str(root / "stage"),
                    "--old-renko-source",
                    "1s_close",
                ]
            )

            with self.assertRaisesRegex(MigrationError, "exact continuation mismatch"):
                run_migration(args)


if __name__ == "__main__":
    unittest.main()
