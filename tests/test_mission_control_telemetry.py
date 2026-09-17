from __future__ import annotations

import asyncio
import ast
import json
import re
import threading
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from mission_control_telemetry import (
    ALLOWED_DATA_LABELS,
    BoundedJsonlTail,
    BrokerSnapshotBackgroundCollector,
    BrokerSnapshotCache,
    MissionControlTelemetryCollector,
    TelemetryConfig,
)


class GuardedBroker:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def get_account(self) -> dict[str, str]:
        self.calls.append("get_account")
        return {
            "equity": "10000",
            "buying_power": "20000",
            "cash": "5000",
            "daytrading_buying_power": "40000",
            "long_market_value": "306",
            "short_market_value": "0",
        }

    def get_positions(self) -> list[dict[str, str]]:
        self.calls.append("get_positions")
        return [{"symbol": "AAPL", "side": "long", "qty": "3", "market_value": "306", "unrealized_pl": "6"}]

    def get_orders(self, *, status: str, limit: int) -> list[dict[str, str]]:
        self.calls.append(f"get_orders:{status}:{limit}")
        return [{"symbol": "AAPL", "type": "stop", "order_class": "", "qty": "3"}]

    def submit_order(self, *args: object, **kwargs: object) -> None:
        raise AssertionError("broker mutator called")

    def cancel_order(self, *args: object, **kwargs: object) -> None:
        raise AssertionError("broker mutator called")

    def replace_order(self, *args: object, **kwargs: object) -> None:
        raise AssertionError("broker mutator called")

    def submit_oco_exit_order(self, *args: object, **kwargs: object) -> None:
        raise AssertionError("broker mutator called")

    def submit_emergency_flatten(self, *args: object, **kwargs: object) -> None:
        raise AssertionError("broker mutator called")


class RaisingBrokerFactory:
    def __call__(self, mode: str | None, profile: str | None) -> object:
        raise RuntimeError("broker unavailable")


class StaticBroker:
    def __init__(self, *, qty: str = "3", equity: str = "10000", fail_on: str | None = None) -> None:
        self.calls: list[str] = []
        self.qty = qty
        self.equity = equity
        self.fail_on = fail_on

    def get_account(self) -> dict[str, str]:
        self.calls.append("get_account")
        if self.fail_on == "get_account":
            raise RuntimeError("account failed")
        return {
            "equity": self.equity,
            "buying_power": "20000",
            "cash": "5000",
            "daytrading_buying_power": "40000",
            "long_market_value": "306",
            "short_market_value": "0",
        }

    def get_positions(self) -> list[dict[str, str]]:
        self.calls.append("get_positions")
        if self.fail_on == "get_positions":
            raise RuntimeError("positions failed")
        return [{"symbol": "AAPL", "side": "long", "qty": self.qty, "market_value": "306", "unrealized_pl": "6"}]

    def get_orders(self, *, status: str, limit: int) -> list[dict[str, str]]:
        self.calls.append(f"get_orders:{status}:{limit}")
        if self.fail_on == "get_orders":
            raise RuntimeError("orders failed")
        return [{"symbol": "AAPL", "type": "stop", "order_class": "", "qty": self.qty}]

    def submit_order(self, *args: object, **kwargs: object) -> None:
        raise AssertionError("broker mutator called")

    def cancel_order(self, *args: object, **kwargs: object) -> None:
        raise AssertionError("broker mutator called")

    def replace_order(self, *args: object, **kwargs: object) -> None:
        raise AssertionError("broker mutator called")

    def delete_order(self, *args: object, **kwargs: object) -> None:
        raise AssertionError("broker mutator called")

    def close_position(self, *args: object, **kwargs: object) -> None:
        raise AssertionError("broker mutator called")

    def close_all_positions(self, *args: object, **kwargs: object) -> None:
        raise AssertionError("broker mutator called")


class BlockingBroker(StaticBroker):
    def __init__(self, *, started: threading.Event, release: threading.Event, qty: str = "5") -> None:
        super().__init__(qty=qty, equity="11000")
        self.started = started
        self.release = release

    def get_positions(self) -> list[dict[str, str]]:
        self.calls.append("get_positions")
        self.started.set()
        self.release.wait(timeout=2.0)
        return [{"symbol": "AAPL", "side": "long", "qty": self.qty, "market_value": "510", "unrealized_pl": "10"}]


class MissionControlTelemetryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_collects_metrics_from_read_only_sources(self) -> None:
        paths = self._healthy_workspace()
        broker = GuardedBroker()
        collector = MissionControlTelemetryCollector(paths["config"])

        output = collector.collect_prometheus(
            broker_reader_for=lambda mode, profile: broker,
            now=datetime(2026, 9, 1, 14, 30, 4, tzinfo=timezone.utc),
        )

        self.assertIn('renko_sip_connected{environment="test",service="shared_market_data"} 1', output)
        self.assertIn('renko_readiness{environment="test",fleet="fleet-a",service="readiness"} 1', output)
        self.assertIn('renko_strategy_armed{environment="test",fleet="fleet-a",strategy="strategy_a",symbol="AAPL"} 1', output)
        self.assertIn('renko_internal_vs_broker_position_match{environment="test",fleet="fleet-a",strategy="strategy_a",symbol="AAPL"} 1', output)
        self.assertIn('renko_orders_submitted_total{environment="test",fleet="fleet-a",strategy="strategy_a",symbol="AAPL",side="long"} 1', output)
        self.assertIn('renko_orders_filled_total{environment="test",fleet="fleet-a",strategy="strategy_a",symbol="AAPL",side="long"} 1', output)
        self.assertIn('renko_execution_ack_latency_seconds_count{environment="test",fleet="fleet-a",strategy="strategy_a",symbol="AAPL",side="long"} 1', output)
        self.assertIn('renko_execution_fill_latency_seconds_count{environment="test",fleet="fleet-a",strategy="strategy_a",symbol="AAPL",side="long"} 1', output)
        self.assertIn('renko_portfolio_realized_pnl_today{environment="test",fleet="fleet-a",service="broker"} NaN', output)
        self.assertIn('renko_telemetry_source_error{environment="test",fleet="fleet-a",service="canonical_accounting"} 1', output)
        self.assertEqual(["get_account", "get_positions", "get_orders:open:100"], broker.calls)

    def test_missing_and_malformed_sources_fail_visibly(self) -> None:
        config_path = self.root / "traders.json"
        health_path = self.root / "health.json"
        journal_path = self.root / "missing-journal.jsonl"
        event_log = self.root / "events.jsonl"
        self._write_json(
            config_path,
            {
                "fleet_id": "fleet-a",
                "traders": [
                    {
                        "id": "trader-a",
                        "enabled": True,
                        "execution_enabled": True,
                        "symbol": "AAPL",
                        "mode": "live",
                        "event_log": str(event_log),
                        "strategy_name": "strategy_a",
                        "strategy": {"type": "renko_reversal", "brick_size": 1.5},
                    }
                ],
            },
        )
        health_path.write_text("{bad json\n", encoding="utf-8")
        event_log.write_text("{bad json\n", encoding="utf-8")
        config = TelemetryConfig(
            environment="test",
            fleet_config_paths=(config_path,),
            archive_root=self.root,
            market_data_health_path=health_path,
            canonical_journal_path=journal_path,
            event_tail_max_bytes=4096,
            event_tail_max_lines=100,
        )

        output = MissionControlTelemetryCollector(config).collect_prometheus(
            broker_reader_for=RaisingBrokerFactory(),
            now=datetime(2026, 9, 1, 14, 30, 4, tzinfo=timezone.utc),
        )

        self.assertIn('renko_telemetry_source_error{environment="test",service="market_data_health"} 1', output)
        self.assertIn('renko_telemetry_source_error{environment="test",service="canonical_journal"} 1', output)
        self.assertIn('renko_telemetry_source_error{environment="test",service="event_log"} 1', output)
        self.assertIn('renko_telemetry_source_error{environment="test",fleet="fleet-a",service="broker"} 1', output)
        self.assertIn('renko_telemetry_source_error{environment="test",fleet="fleet-a",strategy="strategy_a",symbol="AAPL",service="state_snapshot"} 1', output)
        self.assertIn('renko_strategy_state_valid{environment="test",fleet="fleet-a",strategy="strategy_a",symbol="AAPL"} 0', output)
        self.assertIn('renko_broker_connected{environment="test",fleet="fleet-a",service="broker"} 0', output)
        self.assertIn('renko_readiness{environment="test",fleet="fleet-a",service="readiness"} 0', output)

    def test_after_hours_market_data_age_is_visible_but_not_stale_failure(self) -> None:
        paths = self._healthy_workspace(observation_age_seconds=90_000.0)
        output = MissionControlTelemetryCollector(paths["config"]).collect_prometheus(
            broker_reader_for=lambda mode, profile: GuardedBroker(),
            now=datetime(2026, 9, 5, 18, 0, 0, tzinfo=timezone.utc),
        )

        self.assertIn('renko_market_session_open{environment="test",service="market_calendar"} 0', output)
        self.assertIn('renko_market_data_stale{environment="test",symbol="AAPL",service="shared_market_data"} 0', output)
        self.assertIn('renko_canonical_observation_age_seconds{environment="test",symbol="AAPL",service="shared_market_data"} 90000', output)

    def test_bounded_jsonl_tail_does_not_scan_full_history(self) -> None:
        path = self.root / "events.jsonl"
        old_event = {"event_type": "signal", "payload": {"pad": "x" * 2000}}
        recent_event = {"event_type": "execution", "payload": {"trader_id": "trader-a"}}
        old_line = json.dumps(old_event) + "\n"
        recent_line = json.dumps(recent_event) + "\n"
        path.write_text(old_line + recent_line, encoding="utf-8")

        result = BoundedJsonlTail(max_bytes=len(recent_line) + 20, max_lines=10).read_new(path)

        self.assertTrue(result.truncated)
        self.assertEqual((recent_event,), result.events)
        self.assertLess(result.bytes_read, len(old_line) + len(recent_line))

    def test_output_labels_stay_within_contract(self) -> None:
        paths = self._healthy_workspace()
        output = MissionControlTelemetryCollector(paths["config"]).collect_prometheus(
            broker_reader_for=lambda mode, profile: GuardedBroker(),
            now=datetime(2026, 9, 1, 14, 30, 4, tzinfo=timezone.utc),
        )
        allowed = set(ALLOWED_DATA_LABELS) | {"le"}
        samples: set[str] = set()
        duplicates: list[str] = []
        for line in output.splitlines():
            if not line or line.startswith("#"):
                continue
            identity = line.rsplit(" ", 1)[0]
            if identity in samples:
                duplicates.append(identity)
            samples.add(identity)
        for label_block in re.findall(r"\{([^}]*)\}", output):
            for item in label_block.split(","):
                self.assertIn(item.split("=", 1)[0], allowed)
        self.assertEqual([], duplicates)

    def test_duplicate_strategy_labels_are_collapsed_and_visible(self) -> None:
        paths = self._healthy_workspace()
        config = paths["config"]
        config_path = config.fleet_config_paths[0]
        payload = json.loads(config_path.read_text(encoding="utf-8"))
        duplicate = dict(payload["traders"][0])
        duplicate["id"] = "trader-duplicate"
        duplicate["enabled"] = False
        payload["traders"].insert(0, duplicate)
        config_path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")

        output = MissionControlTelemetryCollector(config).collect_prometheus(
            broker_reader_for=lambda mode, profile: GuardedBroker(),
            now=datetime(2026, 9, 1, 14, 30, 4, tzinfo=timezone.utc),
        )

        samples: set[str] = set()
        duplicates: list[str] = []
        for line in output.splitlines():
            if not line or line.startswith("#"):
                continue
            identity = line.rsplit(" ", 1)[0]
            if identity in samples:
                duplicates.append(identity)
            samples.add(identity)
        self.assertEqual([], duplicates)
        self.assertIn(
            'renko_telemetry_source_error{environment="test",fleet="fleet-a",strategy="strategy_a",symbol="AAPL",service="fleet_config_label_collision"} 1',
            output,
        )
        self.assertIn('renko_trader_up{environment="test",fleet="fleet-a",strategy="strategy_a",symbol="AAPL"} 1', output)

    def test_metrics_can_read_empty_cache_without_broker_gets(self) -> None:
        paths = self._healthy_workspace()
        config = paths["config"]
        cache = BrokerSnapshotCache()
        forbidden_calls: list[str] = []

        def forbidden_broker_reader(mode: str | None, profile: str | None) -> object:
            forbidden_calls.append(f"{mode}:{profile}")
            raise AssertionError("metrics called broker reader")

        output = MissionControlTelemetryCollector(config).collect_prometheus(
            broker_reader_for=forbidden_broker_reader,
            broker_snapshot_provider=self._cache_provider(cache, config),
            now=datetime(2026, 9, 1, 14, 30, 4, tzinfo=timezone.utc),
        )

        self.assertEqual([], forbidden_calls)
        self.assertIn('renko_broker_cache_up{environment="test",fleet="fleet-a",service="broker_cache"} 0', output)
        self.assertIn('renko_broker_connected{environment="test",fleet="fleet-a",service="broker"} 0', output)
        self.assertIn('renko_telemetry_source_error{environment="test",fleet="fleet-a",service="broker"} 1', output)
        self.assertIn('renko_readiness{environment="test",fleet="fleet-a",service="readiness"} 0', output)

    def test_metrics_route_uses_cache_not_get_trader(self) -> None:
        import monitoring_app

        paths = self._healthy_workspace()
        config = paths["config"]
        cache = BrokerSnapshotCache()
        collector = MissionControlTelemetryCollector(config)
        with (
            patch.object(monitoring_app, "get_telemetry_config", return_value=config),
            patch.object(monitoring_app, "get_broker_snapshot_cache", return_value=cache),
            patch.object(monitoring_app, "get_telemetry_collector", return_value=collector),
            patch.object(monitoring_app, "get_trader", side_effect=AssertionError("route called get_trader")) as get_trader,
        ):
            response = asyncio.run(monitoring_app.metrics())

        self.assertFalse(get_trader.called)
        body = response.body.decode("utf-8")
        self.assertIn('renko_broker_cache_up{environment="test",fleet="fleet-a",service="broker_cache"} 0', body)

    def test_background_refresh_uses_only_read_only_broker_gets(self) -> None:
        paths = self._healthy_workspace()
        config = paths["config"]
        broker = StaticBroker()
        cache = BrokerSnapshotCache()
        refresher = BrokerSnapshotBackgroundCollector(
            cache=cache,
            config=config,
            broker_reader_for=lambda mode, profile: broker,
        )

        refresher.refresh_once(now=datetime(2026, 9, 1, 14, 30, 4, tzinfo=timezone.utc))

        self.assertEqual(["get_account", "get_positions", "get_orders:open:100"], broker.calls)
        output = MissionControlTelemetryCollector(config).collect_prometheus(
            broker_snapshot_provider=self._cache_provider(cache, config),
            now=datetime(2026, 9, 1, 14, 30, 5, tzinfo=timezone.utc),
        )
        self.assertIn('renko_broker_cache_up{environment="test",fleet="fleet-a",service="broker_cache"} 1', output)
        self.assertIn('renko_broker_cache_refresh_failures_total{environment="test",fleet="fleet-a",service="broker_cache"} 0', output)
        self.assertIn('renko_broker_position_qty{environment="test",fleet="fleet-a",symbol="AAPL",service="broker"} 3', output)

    def test_no_broker_or_reconciliation_mutator_call_sites_are_introduced(self) -> None:
        mutators = {
            "submit_order",
            "cancel_order",
            "replace_order",
            "delete_order",
            "cancel_all_orders",
            "close_position",
            "close_all_positions",
            "submit_oco_exit_order",
            "submit_emergency_flatten",
            "reconcile",
            "reconcile_state",
            "recover_missing_protection",
            "recover_reversal_protection",
        }
        for path in [Path("mission_control_telemetry.py"), Path("monitoring_app.py")]:
            tree = ast.parse(path.read_text(encoding="utf-8"))
            called = sorted(
                {
                    node.func.attr
                    for node in ast.walk(tree)
                    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                }
            )
            self.assertEqual([], sorted(mutators.intersection(called)), msg=str(path))

    def test_stale_cache_is_visible_and_not_current_healthy(self) -> None:
        paths = self._healthy_workspace()
        config = paths["config"]
        cache = BrokerSnapshotCache()
        broker = StaticBroker()
        BrokerSnapshotBackgroundCollector(
            cache=cache,
            config=config,
            broker_reader_for=lambda mode, profile: broker,
        ).refresh_once(now=datetime.fromtimestamp(100, tz=timezone.utc))

        output = MissionControlTelemetryCollector(config).collect_prometheus(
            broker_snapshot_provider=self._cache_provider(cache, config),
            now=datetime.fromtimestamp(120, tz=timezone.utc),
        )

        self.assertIn('renko_broker_cache_age_seconds{environment="test",fleet="fleet-a",service="broker_cache"} 20', output)
        self.assertIn('renko_broker_cache_up{environment="test",fleet="fleet-a",service="broker_cache"} 0', output)
        self.assertIn('renko_broker_connected{environment="test",fleet="fleet-a",service="broker"} 0', output)
        self.assertIn('renko_telemetry_source_error{environment="test",fleet="fleet-a",service="broker"} 1', output)
        self.assertIn('renko_broker_position_qty{environment="test",fleet="fleet-a",symbol="AAPL",service="broker"} 3', output)

    def test_failed_refresh_retains_last_known_good_snapshot(self) -> None:
        paths = self._healthy_workspace()
        config = paths["config"]
        cache = BrokerSnapshotCache()
        BrokerSnapshotBackgroundCollector(
            cache=cache,
            config=config,
            broker_reader_for=lambda mode, profile: StaticBroker(qty="3", equity="10000"),
        ).refresh_once(now=datetime.fromtimestamp(100, tz=timezone.utc))
        failing_broker = StaticBroker(qty="5", equity="11000", fail_on="get_positions")

        BrokerSnapshotBackgroundCollector(
            cache=cache,
            config=config,
            broker_reader_for=lambda mode, profile: failing_broker,
        ).refresh_once(now=datetime.fromtimestamp(101, tz=timezone.utc))

        output = MissionControlTelemetryCollector(config).collect_prometheus(
            broker_snapshot_provider=self._cache_provider(cache, config),
            now=datetime.fromtimestamp(102, tz=timezone.utc),
        )
        self.assertEqual(["get_account", "get_positions"], failing_broker.calls)
        self.assertIn('renko_broker_cache_refresh_failures_total{environment="test",fleet="fleet-a",service="broker_cache"} 1', output)
        self.assertIn('renko_broker_cache_up{environment="test",fleet="fleet-a",service="broker_cache"} 0', output)
        self.assertIn('renko_broker_account_equity{environment="test",fleet="fleet-a",service="broker"} 10000', output)
        self.assertIn('renko_broker_position_qty{environment="test",fleet="fleet-a",symbol="AAPL",service="broker"} 3', output)

    def test_cache_replacement_is_atomic_during_slow_refresh(self) -> None:
        paths = self._healthy_workspace()
        config = paths["config"]
        cache = BrokerSnapshotCache()
        BrokerSnapshotBackgroundCollector(
            cache=cache,
            config=config,
            broker_reader_for=lambda mode, profile: StaticBroker(qty="3", equity="10000"),
        ).refresh_once(now=datetime.fromtimestamp(100, tz=timezone.utc))
        started = threading.Event()
        release = threading.Event()
        blocking_broker = BlockingBroker(started=started, release=release, qty="5")
        refresher = BrokerSnapshotBackgroundCollector(
            cache=cache,
            config=config,
            broker_reader_for=lambda mode, profile: blocking_broker,
        )
        thread = threading.Thread(target=lambda: refresher.refresh_once(now=datetime.fromtimestamp(101, tz=timezone.utc)))
        thread.start()
        self.assertTrue(started.wait(timeout=1.0))
        try:
            before_release_started = time.perf_counter()
            output = MissionControlTelemetryCollector(config).collect_prometheus(
                broker_snapshot_provider=self._cache_provider(cache, config),
                now=datetime.fromtimestamp(101, tz=timezone.utc),
            )
            before_release_duration = time.perf_counter() - before_release_started
            self.assertLess(before_release_duration, 0.2)
            self.assertIn('renko_broker_account_equity{environment="test",fleet="fleet-a",service="broker"} 10000', output)
            self.assertIn('renko_broker_position_qty{environment="test",fleet="fleet-a",symbol="AAPL",service="broker"} 3', output)
        finally:
            release.set()
            thread.join(timeout=1.0)

        output = MissionControlTelemetryCollector(config).collect_prometheus(
            broker_snapshot_provider=self._cache_provider(cache, config),
            now=datetime.fromtimestamp(102, tz=timezone.utc),
        )
        self.assertIn('renko_broker_account_equity{environment="test",fleet="fleet-a",service="broker"} 11000', output)
        self.assertIn('renko_broker_position_qty{environment="test",fleet="fleet-a",symbol="AAPL",service="broker"} 5', output)

    def test_background_timeout_marks_failure_without_blocking_metrics(self) -> None:
        paths = self._healthy_workspace()
        config = paths["config"]
        cache = BrokerSnapshotCache()
        started = threading.Event()
        release = threading.Event()
        broker = BlockingBroker(started=started, release=release)
        refresher = BrokerSnapshotBackgroundCollector(
            cache=cache,
            config=config,
            broker_reader_for=lambda mode, profile: broker,
            refresh_interval_seconds=0.05,
            read_timeout_seconds=0.01,
        )
        refresher.start()
        try:
            self.assertTrue(started.wait(timeout=1.0))
            time.sleep(0.05)
            started_at = time.perf_counter()
            output = MissionControlTelemetryCollector(config).collect_prometheus(
                broker_snapshot_provider=self._cache_provider(cache, config),
                now=datetime(2026, 9, 1, 14, 30, 5, tzinfo=timezone.utc),
            )
            self.assertLess(time.perf_counter() - started_at, 0.2)
            self.assertIn('renko_broker_cache_up{environment="test",fleet="fleet-a",service="broker_cache"} 0', output)
            self.assertIn('renko_broker_cache_refresh_failures_total{environment="test",fleet="fleet-a",service="broker_cache"} 1', output)
            self.assertIn('renko_broker_connected{environment="test",fleet="fleet-a",service="broker"} 0', output)
        finally:
            release.set()
            refresher.stop()

    def test_simultaneous_metrics_scrapes_do_not_call_broker(self) -> None:
        paths = self._healthy_workspace()
        config = paths["config"]
        cache = BrokerSnapshotCache()
        BrokerSnapshotBackgroundCollector(
            cache=cache,
            config=config,
            broker_reader_for=lambda mode, profile: StaticBroker(),
        ).refresh_once(now=datetime(2026, 9, 1, 14, 30, 4, tzinfo=timezone.utc))
        forbidden_calls: list[str] = []

        def forbidden_broker_reader(mode: str | None, profile: str | None) -> object:
            forbidden_calls.append(f"{mode}:{profile}")
            raise AssertionError("metrics called broker reader")

        def scrape() -> str:
            return MissionControlTelemetryCollector(config).collect_prometheus(
                broker_reader_for=forbidden_broker_reader,
                broker_snapshot_provider=self._cache_provider(cache, config),
                now=datetime(2026, 9, 1, 14, 30, 5, tzinfo=timezone.utc),
            )

        with ThreadPoolExecutor(max_workers=8) as executor:
            outputs = list(executor.map(lambda _: scrape(), range(16)))

        self.assertEqual([], forbidden_calls)
        for output in outputs:
            self.assertIn('renko_broker_cache_up{environment="test",fleet="fleet-a",service="broker_cache"} 1', output)

    def test_metrics_route_returns_emergency_payload_on_collector_failure(self) -> None:
        import monitoring_app

        with patch.object(monitoring_app, "get_telemetry_collector", side_effect=RuntimeError("boom")):
            response = asyncio.run(monitoring_app.metrics())

        body = response.body.decode("utf-8")
        self.assertEqual("text/plain; version=0.0.4; charset=utf-8", response.media_type)
        self.assertIn('renko_telemetry_collector_up{environment="production",service="collector"} 0', body)
        self.assertIn('renko_telemetry_source_error{environment="production",service="collector"} 1', body)

    def _healthy_workspace(self, *, observation_age_seconds: float = 2.0) -> dict[str, object]:
        config_path = self.root / "traders.json"
        health_path = self.root / "health.json"
        journal_path = self.root / "canonical.jsonl"
        event_log = self.root / "events.jsonl"
        state_path = self.root / "state/trader-a.json"
        self._write_json(
            config_path,
            {
                "fleet_id": "fleet-a",
                "traders": [
                    {
                        "id": "trader-a",
                        "enabled": True,
                        "execution_enabled": True,
                        "symbol": "AAPL",
                        "mode": "live",
                        "event_log": str(event_log),
                        "strategy_name": "strategy_a",
                        "strategy": {"type": "renko_reversal", "brick_size": 1.5},
                        "session": {
                            "timezone": "America/New_York",
                            "entry_start": "09:31:00",
                            "entry_end": "15:59:59",
                            "active_start_date": "2026-09-01",
                            "active_end_date": None,
                        },
                    }
                ],
            },
        )
        self._write_json(
            health_path,
            {
                "sip_connected": True,
                "upstream_reconnect_count": 2,
                "gap_count": 0,
                "raw_trade_count_by_symbol": {"AAPL": 12},
                "last_raw_trade_by_symbol": {"AAPL": "2026-09-01T14:30:00+00:00"},
                "last_canonical_observation_by_symbol": {"AAPL": "2026-09-01T14:30:00+00:00"},
                "observation_age_seconds": {"AAPL": observation_age_seconds},
                "consumers": {},
            },
        )
        canonical = {
            "type": "journal_record",
            "payload": {
                "type": "observation",
                "symbol": "AAPL",
                "sequence": 42,
                "observation_timestamp": "2026-09-01T14:30:00+00:00",
                "close_price": "102",
            },
        }
        journal_path.write_text(json.dumps(canonical) + "\n", encoding="utf-8")
        self._write_json(
            state_path,
            {
                "saved_at": "2026-09-01T14:30:02+00:00",
                "trader_id": "trader-a",
                "symbol": "AAPL",
                "configured_brick_size": 1.5,
                "engine": {
                    "trader_id": "trader-a",
                    "symbol": "AAPL",
                    "strategy_name": "strategy_a",
                    "strategy_type": "renko_reversal",
                    "renko": {
                        "brick_size": 1.5,
                        "last_brick_close": 101.5,
                        "run_dir": 1,
                        "processed_bricks": 7,
                    },
                    "trade": {
                        "position": {
                            "symbol": "AAPL",
                            "side": "long",
                            "entry_time": "2026-09-01T14:00:00+00:00",
                            "entry_price": 100.0,
                            "qty": 3,
                        },
                        "reconciliation_unsafe": False,
                    },
                },
            },
        )
        events = [
            {
                "event_type": "renko_brick",
                "logged_at": "2026-09-01T14:30:01+00:00",
                "payload": {"trader_id": "trader-a", "symbol": "AAPL", "side": "long"},
            },
            {
                "event_type": "signal",
                "logged_at": "2026-09-01T14:30:02+00:00",
                "payload": {"trader_id": "trader-a", "symbol": "AAPL", "side": "long"},
            },
            {
                "event_type": "execution",
                "logged_at": "2026-09-01T14:30:03+00:00",
                "payload": {
                    "timestamp": "2026-09-01T14:30:03+00:00",
                    "status": "filled",
                    "intent": {
                        "timestamp": "2026-09-01T14:30:02.500000+00:00",
                        "trader_id": "trader-a",
                        "symbol": "AAPL",
                        "side": "buy",
                    },
                    "response": {"filled_at": "2026-09-01T14:30:04+00:00"},
                },
            },
            {
                "event_type": "state_reconciled",
                "logged_at": "2026-09-01T14:30:03+00:00",
                "payload": {"trader_id": "trader-a", "symbol": "AAPL"},
            },
        ]
        event_log.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
        config = TelemetryConfig(
            environment="test",
            fleet_config_paths=(config_path,),
            archive_root=self.root,
            market_data_health_path=health_path,
            canonical_journal_path=journal_path,
            event_tail_max_bytes=4096,
            event_tail_max_lines=100,
        )
        return {"config": config}

    def _cache_provider(self, cache: BrokerSnapshotCache, config: TelemetryConfig):
        return lambda specs, now: cache.read_for_specs(
            specs,
            now,
            stale_after_seconds=config.broker_cache_stale_seconds,
        )

    def _write_json(self, path: Path, payload: object) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
