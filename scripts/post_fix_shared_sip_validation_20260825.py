from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import re
import socket
import stat
import statistics
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv

from alpaca_trader import AlpacaTrader, load_alpaca_credentials
from shared.config.trader_config import build_strategy_config
from silver.market import SharedMarketDataConfig
from silver.market.canonical_journal import CanonicalJournal
from silver.market.ipc_protocol import read_message, write_message


TARGET_DAY = "2026-08-27"
ET = "America/New_York"
ROOT = Path("/root/trading-server")
CONFIG_PATH = ROOT / "configs/market-data.live.example.json"
CAMATRON_CONFIG = ROOT / "configs/traders.live.example.json"
MEGATRON_CONFIG = ROOT / "configs/accounts/megatron.live.json"
EVIDENCE_ROOT = ROOT / "artifacts/shared_sip_validation/20260827"
ACTIVATION_DEADLINE_LABEL = "end_of_09:40_ET"
ACTIVATION_DEADLINE_TIME = "09:41:00"
TIMER_UNIT = "post-fix-shared-sip-validation-20260825.timer"
SERVICE_UNIT = "post-fix-shared-sip-validation-20260825.service"
MARKET_SERVICE = "trading-market-data-live.service"
CAMATRON_SERVICE = "trading-fleet-live.service"
MEGATRON_SERVICE = "trading-fleet-megatron-live.service"
ALPACA_SIP_WEBSOCKET_HOST = "stream.data.alpaca.markets"
REQUIRED_TP_SL_SESSION = {
    "entry_start": "09:30:15",
    "entry_end": "15:50:15",
    "force_flat": "15:59:30",
    "allow_overnight": False,
}

EXPECTED_SOURCE_SHA = {
    "trading_events.py": "b7127bcbc79b6c3cec3ccd7988f5c43085ee36f7aadceabf361fa0e5125da660",
    "silver/market/live_event_stream.py": "33d1f24d5097faca11454a5ac97b6bf4d1a7bce903eb86972c2103bcbe3658ec",
    "silver/market/shared_service.py": "507a83db1e0b882a536e0fd3e85470fd93a8d0ebe0f3782a16a5795c86d1a620",
    "silver/market/models.py": "6ee236b882fdab43007ca1b3271e0b0633c31ec7c5fce759fb60970638f3f21f",
}

EXPECTED_CAMATRON = {
    "camatron_aapl_1s_renko_0p10_e2_tp3_sl3_qty1_20260904_live": {
        "symbol": "AAPL",
        "type": "renko_tp_sl",
        "brick_pct": 0.10,
        "effective_brick_size": 0.30397,
        "entry_bricks": 2,
        "take_profit_bricks": 3,
        "stop_loss_bricks": 3,
        "qty": 1,
        "exit_management": "bracket",
        "market_fingerprint": "0761d66b121034dea1c4ab89569a5578084f60e072f7281d38d7a7ea7cdd580a",
    },
    "camatron_nvda_1s_renko_0p22_e1_tp3_sl3_qty1_20260904_live": {
        "symbol": "NVDA",
        "type": "renko_tp_sl",
        "brick_pct": 0.22,
        "effective_brick_size": 0.495704,
        "entry_bricks": 1,
        "take_profit_bricks": 3,
        "stop_loss_bricks": 3,
        "qty": 1,
        "exit_management": "bracket",
        "market_fingerprint": "83f01df8ed0b3ba105c94d1e042e5d63cf1f888b94d9e70d4ab45236605625d3",
    },
    "camatron_tsla_1s_renko_0p10_e2_tp2_sl3_qty1_20260904_live": {
        "symbol": "TSLA",
        "type": "renko_tp_sl",
        "brick_pct": 0.10,
        "effective_brick_size": 0.3275,
        "entry_bricks": 2,
        "take_profit_bricks": 2,
        "stop_loss_bricks": 3,
        "qty": 1,
        "exit_management": "bracket",
        "market_fingerprint": "6ff4bc9a603abbde9999a767009a94304646032f458978c6d71cfc31e01b656d",
    },
}

EXPECTED_MEGATRON = {
    "megatron_aapl_1s_renko_0p12_e1_reversal_qty1_20260904_live": {
        "symbol": "AAPL",
        "type": "renko_reversal",
        "brick_pct": 0.12,
        "effective_brick_size": 0.364764,
        "entry_bricks": 1,
        "qty": 1,
        "exit_management": "catastrophic_stop",
        "catastrophic_stop_bricks": 3,
        "market_fingerprint": "0761d66b121034dea1c4ab89569a5578084f60e072f7281d38d7a7ea7cdd580a",
    },
    "megatron_nvda_1s_renko_0p20_e1_reversal_qty1_20260904_live": {
        "symbol": "NVDA",
        "type": "renko_reversal",
        "brick_pct": 0.20,
        "effective_brick_size": 0.45064,
        "entry_bricks": 1,
        "qty": 1,
        "exit_management": "catastrophic_stop",
        "catastrophic_stop_bricks": 3,
        "market_fingerprint": "83f01df8ed0b3ba105c94d1e042e5d63cf1f888b94d9e70d4ab45236605625d3",
    },
    "megatron_tsla_1s_renko_0p54_e1_reversal_qty1_20260904_live": {
        "symbol": "TSLA",
        "type": "renko_reversal",
        "brick_pct": 0.54,
        "effective_brick_size": 1.7685,
        "entry_bricks": 1,
        "qty": 1,
        "exit_management": "catastrophic_stop",
        "catastrophic_stop_bricks": 3,
        "market_fingerprint": "6ff4bc9a603abbde9999a767009a94304646032f458978c6d71cfc31e01b656d",
    },
}


def utc_now() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


def et_now() -> pd.Timestamp:
    return utc_now().tz_convert(ET)


def iso_now() -> str:
    return utc_now().isoformat()


def json_default(value: Any) -> str:
    return str(value)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=json_default) + "\n", encoding="utf-8")


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True, default=json_default) + "\n")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(args: list[str], *, check: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, text=True, capture_output=True, check=check)


def systemctl_show(unit: str) -> dict[str, str]:
    fields = [
        "ActiveState",
        "SubState",
        "MainPID",
        "NRestarts",
        "Result",
        "InvocationID",
        "ExecMainStartTimestamp",
        "ExecMainStartTimestampMonotonic",
        "ActiveEnterTimestamp",
        "ActiveEnterTimestampMonotonic",
    ]
    proc = run(["systemctl", "show", unit, *[f"-p{field}" for field in fields]])
    return dict(line.split("=", 1) for line in proc.stdout.splitlines() if "=" in line)


def service_inactive(unit: str) -> bool:
    return systemctl_show(unit).get("ActiveState") == "inactive"


def stop_timer_after_completion() -> None:
    run(["systemctl", "disable", "--now", TIMER_UNIT])
    run(["systemctl", "reset-failed", TIMER_UNIT, SERVICE_UNIT])


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    data = sorted(float(value) for value in values)
    if len(data) == 1:
        return data[0]
    pos = (len(data) - 1) * (pct / 100.0)
    lo = int(pos)
    hi = min(lo + 1, len(data) - 1)
    return data[lo] * (1 - (pos - lo)) + data[hi] * (pos - lo)


def stats(values: list[float]) -> dict[str, Any]:
    return {
        "count": len(values),
        "p50": percentile(values, 50),
        "p95": percentile(values, 95),
        "p99": percentile(values, 99),
        "max": max(values) if values else None,
    }


def payload_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=json_default).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


@dataclass(frozen=True)
class TransientSourceStalenessLimits:
    runtime_signal_block_threshold_seconds: float = 5.0
    max_episode_source_age_seconds: float = 15.0
    max_cumulative_stale_seconds: float = 30.0
    max_stale_duty_cycle: float = 0.05
    rolling_window_seconds: float = 600.0
    final_healthy_seconds: float = 30.0


@dataclass
class TransientSourceEpisode:
    symbol: str
    started_at: str
    started_monotonic: float
    max_source_freshness_age_seconds: float
    total_exceedance_seconds: float = 0.0
    max_rolling_cumulative_exceedance_seconds: float = 0.0
    max_rolling_stale_duty_cycle: float = 0.0
    local_raw_ingress_liveness: list[dict[str, Any]] = field(default_factory=list)
    local_canonical_publication_liveness: list[dict[str, Any]] = field(default_factory=list)
    consumer_signal_blocks: list[dict[str, Any]] = field(default_factory=list)
    recovery_observation_id: str | None = None
    recovery_source_timestamp: str | None = None
    recovery_source_age_seconds: float | None = None
    ended_at: str | None = None
    final_classification: str = "SOURCE_STALE_SIGNAL_BLOCKED"
    failure_reason: str | None = None

    def to_report(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "episode_start": self.started_at,
            "episode_end": self.ended_at,
            "maximum_source_freshness_age_seconds": self.max_source_freshness_age_seconds,
            "total_exceedance_time_above_5s": self.total_exceedance_seconds,
            "rolling_cumulative_exceedance_seconds": self.max_rolling_cumulative_exceedance_seconds,
            "rolling_stale_duty_cycle": self.max_rolling_stale_duty_cycle,
            "local_raw_ingress_liveness": self.local_raw_ingress_liveness,
            "local_canonical_publication_liveness": self.local_canonical_publication_liveness,
            "consumer_signal_block_reasons": self.consumer_signal_blocks,
            "recovery_observation_id": self.recovery_observation_id,
            "recovery_source_timestamp": self.recovery_source_timestamp,
            "recovery_source_age_seconds": self.recovery_source_age_seconds,
            "final_classification": self.final_classification,
            "failure_reason": self.failure_reason,
        }


@dataclass
class SymbolTransientState:
    symbol: str
    state: str = "HEALTHY"
    healthy_since: float | None = None
    healthy_since_wall: str | None = None
    current_episode: TransientSourceEpisode | None = None
    episodes: list[TransientSourceEpisode] = field(default_factory=list)
    stale_intervals: deque[tuple[float, float, float]] = field(default_factory=deque)
    last_sample_monotonic: float | None = None
    last_raw_count: int | None = None
    last_raw_advance_monotonic: float | None = None
    last_eligible_count: int | None = None
    fatal_violations: list[dict[str, Any]] = field(default_factory=list)


class TransientSourceStalenessTracker:
    def __init__(self, symbols: list[str], limits: TransientSourceStalenessLimits | None = None) -> None:
        self.limits = limits or TransientSourceStalenessLimits()
        self.states = {symbol.upper(): SymbolTransientState(symbol=symbol.upper()) for symbol in symbols}

    def update(
        self,
        *,
        symbol: str,
        now_monotonic: float,
        now_iso: str,
        source_freshness_age_seconds: float | None,
        raw_trade_count: int | None,
        eligible_raw_trade_count: int | None,
        canonical_publication_liveness_age_seconds: float | None,
        sip_healthy: bool,
        consumer_block_reasons: dict[str, str | None],
        latest_event: dict[str, Any] | None,
        order_intent_accepted: bool = False,
    ) -> list[str]:
        symbol = symbol.upper()
        state = self.states[symbol]
        failures: list[str] = []
        threshold = self.limits.runtime_signal_block_threshold_seconds
        source_age = float(source_freshness_age_seconds) if source_freshness_age_seconds is not None else float("inf")
        raw_count = int(raw_trade_count) if raw_trade_count is not None else None
        eligible_count = int(eligible_raw_trade_count) if eligible_raw_trade_count is not None else None
        pub_age = (
            float(canonical_publication_liveness_age_seconds)
            if canonical_publication_liveness_age_seconds is not None
            else float("inf")
        )

        if state.healthy_since is None and source_age <= threshold:
            state.healthy_since = now_monotonic
            state.healthy_since_wall = now_iso

        if raw_count is not None and (state.last_raw_count is None or raw_count > state.last_raw_count):
            state.last_raw_advance_monotonic = now_monotonic
        if raw_count is not None:
            state.last_raw_count = raw_count

        elapsed = 0.0 if state.last_sample_monotonic is None else max(0.0, now_monotonic - state.last_sample_monotonic)
        state.last_sample_monotonic = now_monotonic

        current_eligible_input = eligible_count is not None and state.last_eligible_count is not None and eligible_count > state.last_eligible_count
        if eligible_count is not None:
            state.last_eligible_count = eligible_count

        raw_liveness_age = (
            0.0 if state.last_raw_advance_monotonic is None else max(0.0, now_monotonic - state.last_raw_advance_monotonic)
        )
        raw_ingress_healthy = raw_count is not None and raw_liveness_age <= threshold
        canonical_publication_healthy = pub_age <= threshold or not current_eligible_input

        if not sip_healthy:
            failures.append("sip_authentication_or_subscription_lost")
        if source_age > threshold and not raw_ingress_healthy:
            failures.append("raw_local_ingress_stale")
        if current_eligible_input and pub_age > threshold:
            failures.append("local_canonical_publication_stale_despite_current_eligible_input")

        stale = source_age > threshold
        if stale:
            entering_episode = state.current_episode is None
            if state.current_episode is None:
                state.current_episode = TransientSourceEpisode(
                    symbol=symbol,
                    started_at=now_iso,
                    started_monotonic=now_monotonic,
                    max_source_freshness_age_seconds=source_age,
                )
                state.state = "SOURCE_STALE_SIGNAL_BLOCKED"
                state.healthy_since = None
                state.healthy_since_wall = None
            episode = state.current_episode
            episode.max_source_freshness_age_seconds = max(episode.max_source_freshness_age_seconds, source_age)
            episode.local_raw_ingress_liveness.append(
                {
                    "timestamp": now_iso,
                    "healthy": raw_ingress_healthy,
                    "raw_trade_count": raw_count,
                    "seconds_since_raw_count_advance": raw_liveness_age,
                }
            )
            episode.local_canonical_publication_liveness.append(
                {
                    "timestamp": now_iso,
                    "healthy": canonical_publication_healthy,
                    "current_eligible_input": current_eligible_input,
                    "publication_liveness_age_seconds": pub_age,
                }
            )
            episode.consumer_signal_blocks.append(
                {
                    "timestamp": now_iso,
                    "consumer_block_reasons": consumer_block_reasons,
                    "both_consumers_blocked": all(consumer_block_reasons.values()),
                }
            )
            if elapsed and not entering_episode:
                state.stale_intervals.append((now_monotonic - elapsed, now_monotonic, elapsed))
                episode.total_exceedance_seconds += elapsed if elapsed else 0.0
            rolling_start = now_monotonic - self.limits.rolling_window_seconds
            while state.stale_intervals and state.stale_intervals[0][1] <= rolling_start:
                state.stale_intervals.popleft()
            rolling_cumulative = sum(
                max(0.0, min(end, now_monotonic) - max(start, rolling_start))
                for start, end, _duration in state.stale_intervals
            )
            duty_cycle = rolling_cumulative / self.limits.rolling_window_seconds
            episode.max_rolling_cumulative_exceedance_seconds = max(
                episode.max_rolling_cumulative_exceedance_seconds,
                rolling_cumulative,
            )
            episode.max_rolling_stale_duty_cycle = max(episode.max_rolling_stale_duty_cycle, duty_cycle)
            if source_age > self.limits.max_episode_source_age_seconds:
                failures.append("source_freshness_age_above_15s")
            if rolling_cumulative > self.limits.max_cumulative_stale_seconds:
                failures.append("rolling_cumulative_source_stale_above_30s")
            if duty_cycle > self.limits.max_stale_duty_cycle:
                failures.append("rolling_source_stale_duty_cycle_above_5pct")
            if not all(consumer_block_reasons.values()):
                failures.append("consumer_missing_source_stale_signal_block")
            if order_intent_accepted:
                failures.append("order_intent_accepted_while_source_stale")
        else:
            if state.current_episode is not None:
                if elapsed:
                    state.current_episode.total_exceedance_seconds += elapsed
                    state.stale_intervals.append((now_monotonic - elapsed, now_monotonic, elapsed))
                    rolling_start = now_monotonic - self.limits.rolling_window_seconds
                    while state.stale_intervals and state.stale_intervals[0][1] <= rolling_start:
                        state.stale_intervals.popleft()
                    rolling_cumulative = sum(
                        max(0.0, min(end, now_monotonic) - max(start, rolling_start))
                        for start, end, _duration in state.stale_intervals
                    )
                    duty_cycle = rolling_cumulative / self.limits.rolling_window_seconds
                    state.current_episode.max_rolling_cumulative_exceedance_seconds = max(
                        state.current_episode.max_rolling_cumulative_exceedance_seconds,
                        rolling_cumulative,
                    )
                    state.current_episode.max_rolling_stale_duty_cycle = max(
                        state.current_episode.max_rolling_stale_duty_cycle,
                        duty_cycle,
                    )
                    if rolling_cumulative > self.limits.max_cumulative_stale_seconds:
                        failures.append("rolling_cumulative_source_stale_above_30s")
                    if duty_cycle > self.limits.max_stale_duty_cycle:
                        failures.append("rolling_source_stale_duty_cycle_above_5pct")
                event = latest_event or {}
                if str(event.get("delivery_phase") or "live") != "live":
                    failures.append("stale_recovery_from_replayed_or_historical_event")
                if not event.get("event_id"):
                    failures.append("stale_recovery_without_current_eligible_canonical_observation")
                if event.get("eligible") is False:
                    failures.append("stale_recovery_from_ineligible_event")
                if event.get("late") is True or event.get("out_of_order") is True:
                    failures.append("stale_recovery_from_late_or_out_of_order_event")
                state.current_episode.ended_at = now_iso
                state.current_episode.recovery_observation_id = event.get("event_id")
                state.current_episode.recovery_source_timestamp = event.get("source_timestamp") or event.get("observation_timestamp")
                state.current_episode.recovery_source_age_seconds = source_age
                state.current_episode.final_classification = "HEALTHY_RECOVERED"
                state.episodes.append(state.current_episode)
                state.current_episode = None
                state.state = "HEALTHY_RECOVERED"
            else:
                state.state = "HEALTHY"
            if state.healthy_since is None:
                state.healthy_since = now_monotonic
                state.healthy_since_wall = now_iso

        if failures:
            state.state = "FATAL"
            failure = {"timestamp": now_iso, "symbol": symbol, "failure_reasons": failures}
            state.fatal_violations.append(failure)
            if state.current_episode is not None and state.current_episode.failure_reason is None:
                state.current_episode.failure_reason = ";".join(failures)
                state.current_episode.final_classification = "FATAL"
        return [f"{symbol} transient source-staleness policy failure: {reason}" for reason in failures]

    def activation_failures(self, *, now_monotonic: float) -> list[str]:
        failures: list[str] = []
        for symbol, state in self.states.items():
            if state.fatal_violations:
                failures.append(f"{symbol} fatal transient source-staleness violations: {state.fatal_violations[:3]}")
            if state.current_episode is not None or state.state in {"SOURCE_STALE_SIGNAL_BLOCKED", "RECOVERING", "FATAL"}:
                failures.append(f"{symbol} stale or fatal at activation boundary: {state.state}")
            healthy_for = 0.0 if state.healthy_since is None else now_monotonic - state.healthy_since
            if healthy_for < self.limits.final_healthy_seconds:
                failures.append(f"{symbol} final continuously source-fresh period below 30s: {healthy_for:.3f}s")
        return failures

    def summary(self, *, now_monotonic: float) -> dict[str, Any]:
        by_symbol = {}
        for symbol, state in self.states.items():
            episodes = list(state.episodes)
            if state.current_episode is not None:
                episodes.append(state.current_episode)
            healthy_for = 0.0 if state.healthy_since is None else max(0.0, now_monotonic - state.healthy_since)
            reported_state = "FATAL" if state.fatal_violations else state.state
            by_symbol[symbol] = {
                "state": reported_state,
                "healthy_since": state.healthy_since_wall,
                "continuously_healthy_seconds": healthy_for,
                "source_stale_signal_blocked_time_seconds": sum(ep.total_exceedance_seconds for ep in episodes),
                "recovery_time_seconds": 0.0,
                "maximum_episode_source_freshness_age_seconds": max(
                    [ep.max_source_freshness_age_seconds for ep in episodes] or [0.0]
                ),
                "maximum_rolling_cumulative_exceedance_seconds": max(
                    [ep.max_rolling_cumulative_exceedance_seconds for ep in episodes] or [0.0]
                ),
                "maximum_rolling_stale_duty_cycle": max([ep.max_rolling_stale_duty_cycle for ep in episodes] or [0.0]),
                "episodes": [ep.to_report() for ep in episodes],
                "fatal_violations": list(state.fatal_violations),
            }
        return {
            "states": ["HEALTHY", "SOURCE_STALE_SIGNAL_BLOCKED", "RECOVERING", "HEALTHY_RECOVERED", "FATAL"],
            "limits": {
                "runtime_signal_block_threshold_seconds": self.limits.runtime_signal_block_threshold_seconds,
                "max_episode_source_age_seconds": self.limits.max_episode_source_age_seconds,
                "max_cumulative_stale_seconds": self.limits.max_cumulative_stale_seconds,
                "max_stale_duty_cycle": self.limits.max_stale_duty_cycle,
                "rolling_window_seconds": self.limits.rolling_window_seconds,
                "final_healthy_seconds": self.limits.final_healthy_seconds,
            },
            "by_symbol": by_symbol,
        }


def service_snapshot() -> dict[str, dict[str, str]]:
    return {unit: systemctl_show(unit) for unit in [MARKET_SERVICE, CAMATRON_SERVICE, MEGATRON_SERVICE]}


def _resolve_sip_websocket_peer_ips(host: str = ALPACA_SIP_WEBSOCKET_HOST) -> tuple[set[str], str | None]:
    try:
        addresses = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
    except OSError as exc:
        return set(), str(exc)
    return {str(address[4][0]) for address in addresses if address and len(address) >= 5}, None


def _peer_ip_from_ss_row(row: str) -> str | None:
    parts = row.split()
    if len(parts) < 5:
        return None
    peer = parts[4]
    if not peer.endswith(":443"):
        return None
    return peer.rsplit(":", 1)[0].strip("[]")


def _python_pids_from_ss_row(row: str) -> list[int]:
    return [int(match) for match in re.findall(r"pid=(\d+)", row)]


def tcp_python_443() -> dict[str, Any]:
    proc = run(["ss", "-ntp"])
    rows = [line for line in proc.stdout.splitlines() if ":443" in line and "python" in line]
    sip_peer_ips, resolution_error = _resolve_sip_websocket_peer_ips()
    shared_pid = int(systemctl_show(MARKET_SERVICE).get("MainPID") or 0)
    sip_rows = [row for row in rows if _peer_ip_from_ss_row(row) in sip_peer_ips]
    ignored_rows = [row for row in rows if row not in sip_rows]
    shared_owned_sip_rows = [row for row in sip_rows if shared_pid > 0 and shared_pid in _python_pids_from_ss_row(row)]
    invariant_ok = len(sip_rows) == 1 and len(shared_owned_sip_rows) == 1
    # Existing gates consume python_443_count == 1. Keep that strict by folding
    # ownership into the legacy count while exposing the raw classification below.
    invariant_count = len(sip_rows) if invariant_ok or len(sip_rows) != 1 else 0
    return {
        "python_443_count": invariant_count,
        "python_443_rows": sip_rows[:20],
        "raw_python_443_count": len(rows),
        "raw_python_443_rows": rows[:20],
        "ignored_python_443_count": len(ignored_rows),
        "ignored_python_443_rows": ignored_rows[:20],
        "sip_websocket_host": ALPACA_SIP_WEBSOCKET_HOST,
        "sip_websocket_peer_ips": sorted(sip_peer_ips),
        "sip_websocket_resolution_error": resolution_error,
        "sip_websocket_count": len(sip_rows),
        "sip_websocket_rows": sip_rows[:20],
        "sip_websocket_shared_service_pid": shared_pid,
        "sip_websocket_owned_by_shared_service": len(shared_owned_sip_rows) == 1,
        "sip_websocket_invariant_ok": invariant_ok,
    }


def process_snapshot() -> list[str]:
    proc = run(["pgrep", "-af", "silver.market.shared_service|fleet_runner|alpaca_trader|trading-fleet"])
    rows = [line for line in proc.stdout.splitlines() if line.strip()]
    return [line for line in rows if "post_fix_shared_sip_validation_20260825.py" not in line]


def path_metadata(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False}
    st = path.lstat()
    return {
        "path": str(path),
        "exists": True,
        "mode": stat.filemode(st.st_mode),
        "is_socket": stat.S_ISSOCK(st.st_mode),
        "is_regular": stat.S_ISREG(st.st_mode),
        "size": st.st_size,
        "uid": st.st_uid,
        "gid": st.st_gid,
        "mtime": st.st_mtime,
        "ctime": st.st_ctime,
    }


def health_metadata(path: Path) -> dict[str, Any]:
    meta = path_metadata(path)
    if not meta.get("exists"):
        return meta
    with contextlib.suppress(Exception):
        health = read_json(path)
        meta["health_timestamp"] = health.get("timestamp")
        meta["sip_connected"] = health.get("sip_connected")
        meta["authenticated"] = health.get("authenticated")
        meta["subscription_acknowledged"] = health.get("subscription_acknowledged")
        meta["raw_archive_enabled"] = health.get("raw_archive_enabled")
        meta["nonessential_canonical_archive_enabled"] = health.get("nonessential_canonical_archive_enabled")
        meta["canonical_journal_enabled"] = health.get("canonical_journal_enabled")
    return meta


def journal_tail(unit: str, lines: int = 80) -> str:
    return run(["journalctl", "-u", unit, "-n", str(lines), "--no-pager"]).stdout


def readiness_snapshot(cfg: SharedMarketDataConfig, *, started_after_epoch: float) -> dict[str, Any]:
    service = systemctl_show(MARKET_SERVICE)
    return {
        "timestamp": iso_now(),
        "started_after_epoch": started_after_epoch,
        "service": service,
        "socket": path_metadata(Path(cfg.socket_path)),
        "health": health_metadata(Path(cfg.health_path)),
        "recent_journal": journal_tail(MARKET_SERVICE),
    }


def assert_market_service_ready(
    cfg: SharedMarketDataConfig,
    *,
    evidence_dir: Path,
    started_after_epoch: float,
    timeout_seconds: float = 30.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last: dict[str, Any] = {}
    readiness_path = evidence_dir / "shared_service_readiness.json"
    while time.monotonic() < deadline:
        last = readiness_snapshot(cfg, started_after_epoch=started_after_epoch)
        service = last["service"]
        socket = last["socket"]
        health = last["health"]
        main_pid = int(service.get("MainPID") or 0)
        failures: list[str] = []
        if service.get("ActiveState") == "failed" or service.get("Result") not in {"", "success"}:
            failures.append(f"service failed: {service}")
        if service.get("ActiveState") != "active":
            failures.append(f"service not active: {service.get('ActiveState')}")
        if service.get("SubState") != "running":
            failures.append(f"service not running: {service.get('SubState')}")
        if main_pid <= 0:
            failures.append(f"nonzero MainPID not established: {service.get('MainPID')}")
        if socket.get("exists") and socket.get("is_regular"):
            failures.append(f"IPC path is a regular file: {socket}")
        if not socket.get("exists"):
            failures.append(f"IPC socket missing: {socket.get('path')}")
        elif not socket.get("is_socket"):
            failures.append(f"IPC path is not a Unix socket: {socket}")
        elif float(socket.get("ctime") or 0) < started_after_epoch - 1.0:
            failures.append(f"IPC socket predates current start: {socket}")
        if not health.get("exists"):
            failures.append(f"health file missing: {health.get('path')}")
        elif float(health.get("mtime") or 0) < started_after_epoch - 1.0:
            failures.append(f"health file predates current start: {health}")
        if not service.get("InvocationID"):
            failures.append(f"missing systemd InvocationID: {service}")
        last["ready"] = not failures
        last["failures"] = failures
        write_json(readiness_path, last)
        if not failures:
            return last
        if any("service failed" in failure or "regular file" in failure for failure in failures):
            break
        time.sleep(0.5)
    final = readiness_snapshot(cfg, started_after_epoch=started_after_epoch)
    final["ready"] = False
    final["failures"] = last.get("failures") or ["readiness timeout"]
    final["timeout_seconds"] = timeout_seconds
    write_json(readiness_path, final)
    raise RuntimeError(f"shared service readiness failed: {final['failures']}")


def _broker_account_snapshot(name: str, profile: str | None, symbols: list[str]) -> tuple[str, dict[str, Any]]:
    trader = AlpacaTrader(load_alpaca_credentials("live", profile=profile), max_notional=1_000_000)
    account = trader.get_account()
    positions = trader.get_positions()
    orders = trader.get_open_orders_nested(500)
    symbol_set = set(symbols)
    return name, {
        "profile": profile or "default_live",
        "account_id": account.get("id"),
        "status": account.get("status"),
        "trading_blocked": account.get("trading_blocked"),
        "account_blocked": account.get("account_blocked"),
        "transfers_blocked": account.get("transfers_blocked"),
        "target_symbols": symbols,
        "target_positions": [p for p in positions if str(p.get("symbol", "")).upper() in symbol_set],
        "target_open_orders_nested": [o for o in orders if str(o.get("symbol", "")).upper() in symbol_set],
        "all_open_order_count": len(orders),
    }


def broker_snapshot(*, timeout_seconds: float = 20.0) -> dict[str, Any]:
    load_dotenv(ROOT / ".env")
    snapshot: dict[str, Any] = {"captured_at": iso_now(), "timeout_seconds": timeout_seconds, "accounts": {}}
    targets = [
        ("camatron", None, ["AAPL", "NVDA", "TSLA"]),
        ("megatron", "megatron", ["AAPL", "TSLA"]),
    ]
    executor = ThreadPoolExecutor(max_workers=len(targets))
    try:
        futures = [executor.submit(_broker_account_snapshot, *target) for target in targets]
        for future in futures:
            try:
                name, account_snapshot = future.result(timeout=timeout_seconds)
            except FuturesTimeoutError as exc:
                raise RuntimeError(f"broker snapshot timed out after {timeout_seconds}s; refusing to assume flat") from exc
            snapshot["accounts"][name] = account_snapshot
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
    return snapshot


def assert_broker_flat(snapshot: dict[str, Any]) -> None:
    for account in snapshot["accounts"].values():
        if str(account.get("status", "")).upper() != "ACTIVE":
            raise RuntimeError(f"broker account not active: {account}")
        if account.get("account_blocked") or account.get("trading_blocked"):
            raise RuntimeError(f"broker account blocked: {account}")
        if account.get("target_positions"):
            raise RuntimeError(f"target positions are not flat: {account['target_positions']}")
        if account.get("target_open_orders_nested"):
            raise RuntimeError(f"target open orders remain: {account['target_open_orders_nested']}")


def validate_expected_trader(path: Path, expected: dict[str, dict[str, Any]], *, reject_qqq: bool) -> list[dict[str, Any]]:
    cfg = read_json(path)
    enabled = [trader for trader in cfg.get("traders", []) if trader.get("enabled")]
    enabled_by_id = {str(trader.get("id")): trader for trader in enabled}
    if set(enabled_by_id) != set(expected):
        raise RuntimeError(f"{path} enabled trader mismatch: {sorted(enabled_by_id)} != {sorted(expected)}")
    if reject_qqq and any(str(trader.get("symbol", "")).upper() == "QQQ" and trader.get("enabled") for trader in cfg.get("traders", [])):
        raise RuntimeError("QQQ is enabled")
    summaries: list[dict[str, Any]] = []
    for trader_id, required in expected.items():
        trader = enabled_by_id[trader_id]
        strategy = trader.get("strategy") or {}
        session = trader.get("session") or {}
        sizing = trader.get("sizing") or {}
        data = trader.get("data") or {}
        profile = trader.get("profile") or {}
        if not trader.get("execution_enabled"):
            raise RuntimeError(f"{trader_id} execution_enabled is false")
        if str(data.get("market_data_source")) != "shared_canonical_stream":
            raise RuntimeError(f"{trader_id} does not use shared_canonical_stream")
        if str(data.get("renko_source")) != "1s_close" or str(data.get("feed")) != "sip":
            raise RuntimeError(f"{trader_id} does not use SIP 1s_close")
        if int(sizing.get("qty")) != int(required["qty"]):
            raise RuntimeError(f"{trader_id} qty mismatch")
        for field in ["symbol", "type", "entry_bricks", "exit_management"]:
            if str((strategy if field not in {"symbol"} else trader).get(field)) != str(required[field]):
                raise RuntimeError(f"{trader_id} {field} mismatch")
        strategy_config = build_strategy_config(trader)
        if "brick_pct" in required:
            configured_pct = strategy.get("brick_pct") or strategy.get("brick_size_ltp_percent") or profile.get("renko_brick_pct")
            if configured_pct is None or abs(float(configured_pct) - float(required["brick_pct"])) > 1e-9:
                raise RuntimeError(f"{trader_id} brick_pct mismatch: {configured_pct} != {required['brick_pct']}")
        if "effective_brick_size" in required:
            if abs(float(strategy_config.brick_size) - float(required["effective_brick_size"])) > 1e-9:
                raise RuntimeError(
                    f"{trader_id} effective brick mismatch: {strategy_config.brick_size} != {required['effective_brick_size']}"
                )
        for field in ["brick_size", "take_profit_bricks", "stop_loss_bricks", "catastrophic_stop_bricks"]:
            if field not in required:
                continue
            if strategy.get(field) is None:
                raise RuntimeError(f"{trader_id} missing {field}")
            if abs(float(strategy.get(field)) - float(required[field])) > 1e-9:
                raise RuntimeError(f"{trader_id} {field} mismatch: {strategy.get(field)} != {required[field]}")
        if required["type"] == "renko_tp_sl" and str(required["exit_management"]) == "bracket":
            for field, value in REQUIRED_TP_SL_SESSION.items():
                actual = getattr(strategy_config, field)
                if actual != value:
                    raise RuntimeError(f"{trader_id} effective TP/SL session {field} mismatch: {actual} != {value}")
        if "candidate_id" in required and profile.get("candidate_id") != required["candidate_id"]:
            raise RuntimeError(f"{trader_id} candidate_id mismatch")
        protection = strategy.get("protection") or {}
        if required["exit_management"] in {"bracket", "catastrophic_stop"} and protection.get("emergency_flatten_enabled") is not True:
            raise RuntimeError(f"{trader_id} emergency flatten is not enabled")
        summaries.append(
            {
                "id": trader_id,
                "symbol": trader.get("symbol"),
                "strategy": strategy,
                "qty": sizing.get("qty"),
                "data": data,
                "candidate_id": profile.get("candidate_id"),
            }
        )
    return summaries


def validate_state_files(market_cfg: SharedMarketDataConfig) -> list[dict[str, Any]]:
    paths = {
        **{tid: ROOT / "artifacts/live/state" / f"{tid}.json" for tid in EXPECTED_CAMATRON},
        **{tid: ROOT / "artifacts/live/megatron/state" / f"{tid}.json" for tid in EXPECTED_MEGATRON},
    }
    expected = {**EXPECTED_CAMATRON, **EXPECTED_MEGATRON}
    summaries = []
    for trader_id, path in paths.items():
        if not path.exists():
            raise RuntimeError(f"missing state file {path}")
        payload = read_json(path)
        snapshot = payload.get("snapshot") if isinstance(payload.get("snapshot"), dict) else payload
        engine = snapshot.get("engine") if isinstance(snapshot.get("engine"), dict) else {}
        trade = engine.get("trade") if isinstance(engine.get("trade"), dict) else None
        if not isinstance(trade, dict):
            trade = snapshot.get("trade") if isinstance(snapshot.get("trade"), dict) else snapshot
        if trade.get("position") is not None:
            raise RuntimeError(f"{trader_id} state has position")
        if trade.get("pending_exit") is not None:
            raise RuntimeError(f"{trader_id} state has pending_exit")
        if bool(trade.get("reconciliation_unsafe")):
            raise RuntimeError(f"{trader_id} state reconciliation_unsafe")
        fingerprints = snapshot.get("contract_fingerprints") or trade.get("contract_fingerprints") or {}
        market = fingerprints.get("market_stream") or {}
        if market.get("fingerprint") != expected[trader_id]["market_fingerprint"]:
            raise RuntimeError(f"{trader_id} state market fingerprint mismatch")
        symbol = expected[trader_id]["symbol"]
        if market_cfg.contract_fingerprints.get(symbol) != expected[trader_id]["market_fingerprint"]:
            raise RuntimeError(f"{trader_id} market config fingerprint mismatch")
        summaries.append({"trader_id": trader_id, "path": str(path), "sha256": sha256(path), "flat": True})
    return summaries


def validate_clock() -> dict[str, Any]:
    return {
        "date_utc": run(["date", "-u", "--iso-8601=ns"]).stdout.strip(),
        "date_et": run(["bash", "-lc", f"TZ={ET} date --iso-8601=ns"]).stdout.strip(),
        "timedatectl": run(["timedatectl"]).stdout,
        "timesync_status": run(["timedatectl", "timesync-status"]).stdout,
    }


def disk_snapshot(path: Path) -> dict[str, Any]:
    usage = os.statvfs(str(path))
    return {
        "path": str(path),
        "free_bytes": usage.f_bavail * usage.f_frsize,
        "free_inodes": usage.f_favail,
    }


def latest_deployment_report_path() -> Path:
    reports = sorted(
        EVIDENCE_ROOT.glob("*_deploy_*/deployment_schedule_report.json"),
        key=lambda path: re.search(r"(\d{8}T\d{6}Z)$", path.parent.name).group(1)
        if re.search(r"(\d{8}T\d{6}Z)$", path.parent.name)
        else path.parent.name,
    )
    if not reports:
        raise RuntimeError(f"missing deployment report under {EVIDENCE_ROOT}")
    return reports[-1]


def verify_deployed_checksums_against_report() -> dict[str, Any]:
    report_path = latest_deployment_report_path()
    report = read_json(report_path)
    expected = report.get("post_deploy_sha256")
    if not isinstance(expected, dict) or not expected:
        raise RuntimeError(f"deployment report missing post_deploy_sha256: {report_path}")
    mismatches = []
    actual: dict[str, str] = {}
    for raw_path, expected_sha in expected.items():
        path = Path(raw_path)
        if not path.exists():
            mismatches.append({"path": str(path), "expected": expected_sha, "actual": "MISSING"})
            continue
        actual_sha = sha256(path)
        actual[str(path)] = actual_sha
        if actual_sha != expected_sha:
            mismatches.append({"path": str(path), "expected": expected_sha, "actual": actual_sha})
    if mismatches:
        raise RuntimeError(f"deployed checksum mismatch against {report_path}: {mismatches}")
    return {"deployment_report": str(report_path), "checksums": actual}


def preflight(evidence_dir: Path) -> dict[str, Any]:
    if et_now().date().isoformat() != TARGET_DAY:
        raise RuntimeError(f"refusing to run outside {TARGET_DAY} ET: {et_now().isoformat()}")
    deployed_checksums = verify_deployed_checksums_against_report()
    for rel, expected in EXPECTED_SOURCE_SHA.items():
        actual = sha256(ROOT / rel)
        if actual != expected:
            raise RuntimeError(f"{rel} checksum mismatch {actual} != {expected}")
    for unit in [MARKET_SERVICE, CAMATRON_SERVICE, MEGATRON_SERVICE]:
        if not service_inactive(unit):
            raise RuntimeError(f"{unit} is not inactive: {systemctl_show(unit)}")
    processes = process_snapshot()
    if processes:
        raise RuntimeError(f"unexpected shared/fleet process: {processes}")
    if Path("/run/trading/market-data/live.sock").exists() or Path("/run/trading/market-data/live.lock").exists():
        raise RuntimeError("active shared-market-data socket or lock exists")

    market_cfg = SharedMarketDataConfig.load(CONFIG_PATH)
    if market_cfg.feed != "sip" or market_cfg.canonical_source != "1s_close":
        raise RuntimeError("market data config is not SIP 1s_close")
    if market_cfg.raw_archive_enabled is not False:
        raise RuntimeError("canonical-journal-only canary requires raw_archive_enabled=false")
    if market_cfg.nonessential_canonical_archive_enabled is not False:
        raise RuntimeError("canonical-journal-only canary requires nonessential_canonical_archive_enabled=false")
    if market_cfg.canonical_journal_enabled is not True:
        raise RuntimeError("canonical journal must remain enabled")
    if set(market_cfg.symbols) != {"AAPL", "NVDA", "TSLA"}:
        raise RuntimeError(f"market data symbol mismatch: {market_cfg.symbols}")
    camatron = validate_expected_trader(CAMATRON_CONFIG, EXPECTED_CAMATRON, reject_qqq=True)
    megatron = validate_expected_trader(MEGATRON_CONFIG, EXPECTED_MEGATRON, reject_qqq=False)
    state = validate_state_files(market_cfg)
    broker = broker_snapshot()
    assert_broker_flat(broker)
    journal = CanonicalJournal(market_cfg.journal_path, max_replay_records=market_cfg.replay_limit_records)
    recovery = journal.recover()
    if not recovery.healthy:
        raise RuntimeError(f"canonical journal is unhealthy: {recovery}")
    snapshot = {
        "timestamp": iso_now(),
        "services": service_snapshot(),
        "processes": processes,
        "socket_listing": run(["ls", "-la", "/run/trading/market-data"]).stdout,
        "clock": validate_clock(),
        "disk": disk_snapshot(ROOT),
        "deployed_checksums": deployed_checksums,
        "source_checksums": {rel: sha256(ROOT / rel) for rel in EXPECTED_SOURCE_SHA},
        "config_checksums": {
            str(CONFIG_PATH): sha256(CONFIG_PATH),
            str(CAMATRON_CONFIG): sha256(CAMATRON_CONFIG),
            str(MEGATRON_CONFIG): sha256(MEGATRON_CONFIG),
        },
        "market_config": {
            "symbols": market_cfg.symbols,
            "feed": market_cfg.feed,
            "canonical_source": market_cfg.canonical_source,
            "contract_fingerprints": market_cfg.contract_fingerprints,
        },
        "enabled_profiles": {"camatron": camatron, "megatron": megatron},
        "state_files": state,
        "broker": broker,
        "journal": {
            "healthy": recovery.healthy,
            "latest_sequences": recovery.latest_sequences,
            "corrupted_records": recovery.corrupted_records,
            "oldest_sequences": journal.oldest_sequence_by_symbol(),
        },
    }
    write_json(evidence_dir / "preflight.json", snapshot)
    return snapshot


@dataclass
class Tailer:
    path: Path
    offset: int = 0

    @classmethod
    def from_end(cls, path: Path) -> "Tailer":
        return cls(path=path, offset=path.stat().st_size if path.exists() else 0)

    def read_new(self) -> list[str]:
        if not self.path.exists():
            return []
        size = self.path.stat().st_size
        if size < self.offset:
            self.offset = 0
        with self.path.open("r", encoding="utf-8") as handle:
            handle.seek(self.offset)
            lines = handle.readlines()
            self.offset = handle.tell()
        return [line for line in lines if line.strip()]


class DiagnosticConsumer:
    def __init__(self, identity: str, cfg: SharedMarketDataConfig, last_sequences: dict[str, int]) -> None:
        self.identity = identity
        self.cfg = cfg
        self.last_sequences = {str(k).upper(): int(v) for k, v in last_sequences.items()}
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.task: asyncio.Task[None] | None = None
        self.events: list[dict[str, Any]] = []
        self.gaps: list[dict[str, Any]] = []
        self.duplicates: list[dict[str, Any]] = []
        self.disconnects: list[dict[str, Any]] = []
        self.first_after_reconnect: list[dict[str, Any]] = []
        self.connected_at: str | None = None
        self.catchup_complete = False
        self.reconnect_checkpoint: dict[str, int] | None = None
        self._seen: set[tuple[str, int, str]] = set()

    async def connect(self) -> None:
        self.reader, self.writer = await asyncio.open_unix_connection(self.cfg.socket_path)
        self.connected_at = iso_now()
        self.catchup_complete = False
        await write_message(
            self.writer,
            {
                "type": "hello",
                "identity": self.identity,
                "expected_contract_fingerprints": self.cfg.contract_fingerprints,
                "last_sequences": self.last_sequences,
            },
        )
        self.task = asyncio.create_task(self._loop())

    async def _loop(self) -> None:
        assert self.reader is not None
        try:
            while True:
                message = await read_message(self.reader)
                typ = message.get("type")
                if typ == "catchup_complete":
                    self.catchup_complete = True
                    continue
                if typ == "gap":
                    self.gaps.append(message)
                    continue
                if typ != "observation":
                    continue
                symbol = str(message.get("symbol") or "").upper()
                sequence = int(message.get("sequence") or 0)
                event_id = str(message.get("event_id") or "")
                key = (symbol, sequence, event_id)
                if key in self._seen:
                    self.duplicates.append({"symbol": symbol, "sequence": sequence, "event_id": event_id})
                self._seen.add(key)
                previous = int(self.last_sequences.get(symbol, 0))
                if previous and sequence > previous + 1:
                    self.gaps.append({"type": "local_sequence_gap", "symbol": symbol, "previous": previous, "received": sequence})
                if self.reconnect_checkpoint is not None and symbol not in {x["symbol"] for x in self.first_after_reconnect}:
                    checkpoint = int(self.reconnect_checkpoint.get(symbol, 0))
                    self.first_after_reconnect.append(
                        {
                            "symbol": symbol,
                            "checkpoint": checkpoint,
                            "first_sequence": sequence,
                            "expected_if_advanced": checkpoint + 1,
                        }
                    )
                self.last_sequences[symbol] = max(previous, sequence)
                event = {
                    "symbol": symbol,
                    "sequence": sequence,
                    "event_id": event_id,
                    "hash": payload_hash(message),
                    "delivery_phase": "live" if self.catchup_complete else "replay_catchup",
                    "source_timestamp": message.get("source_timestamp"),
                    "observation_timestamp": message.get("observation_timestamp"),
                    "receive_timestamp": message.get("receive_timestamp"),
                    "received_at": iso_now(),
                }
                try:
                    received_at = pd.Timestamp(event["received_at"]).tz_convert("UTC")
                    receive_timestamp = pd.Timestamp(event["receive_timestamp"]).tz_convert("UTC")
                    observation_timestamp = pd.Timestamp(event["observation_timestamp"]).tz_convert("UTC")
                    event["local_ingress_to_consumer_seconds"] = max(0.0, (received_at - receive_timestamp).total_seconds())
                    event["upstream_exchange_to_ingress_seconds"] = max(0.0, (receive_timestamp - observation_timestamp).total_seconds())
                except Exception:
                    pass
                self.events.append(event)
                assert self.writer is not None
                ack_started = time.perf_counter()
                await write_message(self.writer, {"type": "ack", "symbol": symbol, "sequence": sequence, "event_id": event_id})
                event["ack_checkpoint_seconds"] = time.perf_counter() - ack_started
        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
            return

    def latest_event_by_symbol(self) -> dict[str, dict[str, Any]]:
        latest: dict[str, dict[str, Any]] = {}
        for event in self.events:
            latest[event["symbol"]] = event
        return latest

    def signal_block_reason_by_symbol(self, symbol: str, *, now: pd.Timestamp, threshold_seconds: float) -> str | None:
        symbol = symbol.upper()
        if self.gaps:
            return "shared_market_data_gap"
        if not self.catchup_complete:
            return "shared_market_data_not_caught_up"
        latest = self.latest_event_by_symbol().get(symbol)
        if latest is None:
            return "shared_market_data_no_observation"
        try:
            age = (now - pd.Timestamp(latest["observation_timestamp"]).tz_convert("UTC")).total_seconds()
        except Exception:
            return "shared_market_data_observation_timestamp_invalid"
        if age > threshold_seconds:
            return f"shared_market_data_stale_{age:.3f}s"
        return None

    async def disconnect(self) -> None:
        if self.writer is not None:
            self.writer.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.writer.wait_closed(), timeout=5)
        if self.task is not None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(asyncio.gather(self.task, return_exceptions=True), timeout=5)
        self.disconnects.append({"at": iso_now(), "last_sequences": dict(self.last_sequences)})

    def summary(self) -> dict[str, Any]:
        by_symbol: dict[str, int] = defaultdict(int)
        for event in self.events:
            by_symbol[event["symbol"]] += 1
        return {
            "identity": self.identity,
            "connected_at": self.connected_at,
            "catchup_complete": self.catchup_complete,
            "event_count": len(self.events),
            "by_symbol": dict(by_symbol),
            "last_sequences": dict(self.last_sequences),
            "gaps": self.gaps,
            "duplicates": self.duplicates,
            "disconnects": self.disconnects,
            "first_after_reconnect": self.first_after_reconnect,
            "first_event": self.events[0] if self.events else None,
            "last_event": self.events[-1] if self.events else None,
        }


async def wait_until(target: pd.Timestamp) -> None:
    while utc_now() < target:
        await asyncio.sleep(min(5, max(0.1, (target - utc_now()).total_seconds())))


async def wait_fresh(cfg: SharedMarketDataConfig, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        health = read_json(Path(cfg.health_path)) if Path(cfg.health_path).exists() else {}
        ages = health.get("canonical_heartbeat_age_seconds") or health.get("observation_age_seconds") or {}
        source_ages = health.get("canonical_source_freshness_age_seconds") or {}
        if (
            health.get("sip_connected")
            and health.get("authenticated")
            and health.get("subscription_acknowledged")
            and all(float(ages.get(symbol, 999999)) <= cfg.freshness_threshold_seconds for symbol in cfg.symbols)
            and all(float(source_ages.get(symbol, 999999)) <= cfg.freshness_threshold_seconds for symbol in cfg.symbols)
        ):
            return True
        await asyncio.sleep(0.5)
    return False


def latest_pipeline_metric(health: dict[str, Any], name: str) -> dict[str, Any]:
    return ((health.get("pipeline_metrics") or {}).get("durations_seconds") or {}).get(name) or {}


def latest_pipeline_high_water(health: dict[str, Any]) -> dict[str, Any]:
    return (health.get("pipeline_metrics") or {}).get("high_water") or {}


def collect_consumer_latency(consumers: dict[str, Any]) -> dict[str, Any]:
    local_latency_by_symbol: dict[str, list[float]] = defaultdict(list)
    live_local_latency_by_symbol: dict[str, list[float]] = defaultdict(list)
    replay_local_latency_by_symbol: dict[str, list[float]] = defaultdict(list)
    ack_latency_by_symbol: dict[str, list[float]] = defaultdict(list)
    local_latency_outliers: list[dict[str, Any]] = []
    for consumer in consumers.values():
        for event in consumer.events:
            symbol = str(event.get("symbol") or "").upper()
            if "local_ingress_to_consumer_seconds" in event:
                local_latency = float(event["local_ingress_to_consumer_seconds"])
                phase = str(event.get("delivery_phase") or "unknown")
                local_latency_by_symbol[symbol].append(local_latency)
                target = live_local_latency_by_symbol if phase == "live" else replay_local_latency_by_symbol
                target[symbol].append(local_latency)
                local_latency_outliers.append(
                    {
                        "identity": consumer.identity,
                        "delivery_phase": phase,
                        "symbol": symbol,
                        "sequence": event.get("sequence"),
                        "event_id": event.get("event_id"),
                        "local_ingress_to_consumer_seconds": local_latency,
                        "upstream_exchange_to_ingress_seconds": event.get("upstream_exchange_to_ingress_seconds"),
                        "observation_timestamp": event.get("observation_timestamp"),
                        "receive_timestamp": event.get("receive_timestamp"),
                        "received_at": event.get("received_at"),
                    }
                )
            if "ack_checkpoint_seconds" in event:
                ack_latency_by_symbol[symbol].append(float(event["ack_checkpoint_seconds"]))
    all_latency = [value for values in local_latency_by_symbol.values() for value in values]
    live_latency = [value for values in live_local_latency_by_symbol.values() for value in values]
    replay_latency = [value for values in replay_local_latency_by_symbol.values() for value in values]
    return {
        "local_latency_by_symbol": local_latency_by_symbol,
        "live_local_latency_by_symbol": live_local_latency_by_symbol,
        "replay_local_latency_by_symbol": replay_local_latency_by_symbol,
        "ack_latency_by_symbol": ack_latency_by_symbol,
        "all_latency": all_latency,
        "live_latency": live_latency,
        "replay_latency": replay_latency,
        "outliers": sorted(
            local_latency_outliers,
            key=lambda item: float(item.get("local_ingress_to_consumer_seconds") or 0),
            reverse=True,
        )[:50],
    }


async def active_validation(evidence_dir: Path) -> dict[str, Any]:
    cfg = SharedMarketDataConfig.load(CONFIG_PATH)
    journal = CanonicalJournal(cfg.journal_path, max_replay_records=cfg.replay_limit_records)
    initial_head = journal.latest_sequence_by_symbol()
    raw_tailers = (
        {
            symbol: Tailer.from_end(Path(cfg.archive_root) / "raw" / TARGET_DAY / f"{symbol}.jsonl")
            for symbol in cfg.symbols
        }
        if cfg.raw_archive_enabled
        else {}
    )
    journal_tailer = Tailer.from_end(Path(cfg.journal_path))
    report: dict[str, Any] = {
        "status": "running",
        "started_at": iso_now(),
        "initial_head": initial_head,
        "samples": [],
        "failures": [],
    }
    report_path = evidence_dir / "active_validation_report.json"
    samples_path = evidence_dir / "active_validation_samples.jsonl"
    write_json(report_path, report)

    open_time = pd.Timestamp(f"{TARGET_DAY} 09:30:00", tz=ET).tz_convert("UTC")
    await wait_until(open_time)
    if not await wait_fresh(cfg, timeout=90):
        raise RuntimeError(f"fresh health not established after open: {read_json(Path(cfg.health_path)) if Path(cfg.health_path).exists() else {}}")

    cam = DiagnosticConsumer("camatron", cfg, initial_head)
    meg = DiagnosticConsumer("megatron", cfg, initial_head)
    consumers = {"camatron": cam, "megatron": meg}
    await cam.connect()
    await meg.connect()

    raw_counts: dict[str, int] = defaultdict(int)
    canonical_counts: dict[str, int] = defaultdict(int)
    canonical_seconds: set[tuple[str, str]] = set()
    duplicate_seconds: list[dict[str, Any]] = []
    nanoseconds: list[dict[str, Any]] = []
    seq_prev: dict[str, int] = defaultdict(int)
    upstream_age_by_symbol: dict[str, list[float]] = defaultdict(list)
    upstream_age_by_minute: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    latest_canonical_event_by_symbol: dict[str, dict[str, Any]] = {}
    transient_tracker = TransientSourceStalenessTracker(cfg.symbols)
    service_start = systemctl_show(MARKET_SERVICE)
    failures: list[str] = []
    replays: list[dict[str, Any]] = []
    start = time.monotonic()
    next_sample = 0.0
    did_meg = False
    did_cam = False

    try:
        while time.monotonic() - start < 600:
            elapsed = time.monotonic() - start
            for symbol, tailer in raw_tailers.items():
                raw_counts[symbol] += len(tailer.read_new())
            for line in journal_tailer.read_new():
                try:
                    record = json.loads(line)
                    payload = record.get("payload", record)
                    symbol = str(payload.get("symbol") or "").upper()
                    sequence = int(payload.get("sequence") or 0)
                    obs = pd.Timestamp(payload["observation_timestamp"]).tz_convert("UTC")
                    recv = pd.Timestamp(payload.get("receive_timestamp") or payload["observation_timestamp"]).tz_convert("UTC")
                except Exception as exc:
                    failures.append(f"journal parse error: {exc}")
                    continue
                if seq_prev[symbol] and sequence != seq_prev[symbol] + 1:
                    failures.append(f"{symbol} journal sequence nonmonotonic {seq_prev[symbol]}->{sequence}")
                seq_prev[symbol] = sequence
                canonical_counts[symbol] += 1
                latest_canonical_event_by_symbol[symbol] = {
                    "symbol": symbol,
                    "sequence": sequence,
                    "event_id": str(payload.get("event_id") or ""),
                    "source_timestamp": payload.get("source_timestamp"),
                    "observation_timestamp": payload.get("observation_timestamp"),
                    "receive_timestamp": payload.get("receive_timestamp"),
                    "delivery_phase": "live",
                }
                if obs.nanosecond or obs.microsecond:
                    nanoseconds.append({"symbol": symbol, "sequence": sequence, "observation_timestamp": payload["observation_timestamp"]})
                key = (symbol, obs.isoformat())
                if key in canonical_seconds:
                    duplicate_seconds.append({"symbol": symbol, "sequence": sequence, "observation_timestamp": obs.isoformat()})
                canonical_seconds.add(key)
                upstream_age = max(0.0, (recv - obs).total_seconds())
                upstream_age_by_symbol[symbol].append(upstream_age)
                upstream_age_by_minute[symbol][obs.floor("min").isoformat()].append(upstream_age)

            if not did_meg and elapsed >= 200:
                before = meg.summary()
                checkpoint = dict(meg.last_sequences)
                await meg.disconnect()
                await asyncio.sleep(15)
                meg = DiagnosticConsumer("megatron", cfg, checkpoint)
                meg.reconnect_checkpoint = checkpoint
                await meg.connect()
                consumers["megatron"] = meg
                await asyncio.sleep(30)
                replays.append({"identity": "megatron", "checkpoint": checkpoint, "before": before, "after": meg.summary(), "new_events": len(meg.events)})
                did_meg = True
            if not did_cam and elapsed >= 400:
                before = cam.summary()
                checkpoint = dict(cam.last_sequences)
                await cam.disconnect()
                await asyncio.sleep(15)
                cam = DiagnosticConsumer("camatron", cfg, checkpoint)
                cam.reconnect_checkpoint = checkpoint
                await cam.connect()
                consumers["camatron"] = cam
                await asyncio.sleep(30)
                replays.append({"identity": "camatron", "checkpoint": checkpoint, "before": before, "after": cam.summary(), "new_events": len(cam.events)})
                did_cam = True

            health = read_json(Path(cfg.health_path)) if Path(cfg.health_path).exists() else {}
            if not cfg.raw_archive_enabled:
                raw_counts = defaultdict(
                    int,
                    {str(k).upper(): int(v) for k, v in dict(health.get("raw_trade_count_by_symbol") or {}).items()},
                )
            event_ages = health.get("observation_age_seconds") or {}
            heartbeat_ages = health.get("canonical_heartbeat_age_seconds") or event_ages
            source_ages = health.get("canonical_source_freshness_age_seconds") or {}
            publication_ages = health.get("canonical_publication_liveness_age_seconds") or {}
            health_flags = {
                key: health.get(key)
                for key in ["sip_connected", "authenticated", "subscription_acknowledged", "gap_count", "dropped_message_count", "upstream_reconnect_count"]
            }
            sip_healthy = bool(
                health.get("sip_connected")
                and health.get("authenticated")
                and health.get("subscription_acknowledged")
            )
            raw_trade_counts = {
                str(k).upper(): int(v)
                for k, v in dict(health.get("raw_trade_count_by_symbol") or raw_counts).items()
            }
            eligible_raw_trade_counts = {
                str(k).upper(): int(v)
                for k, v in dict(health.get("eligible_raw_trade_count_by_symbol") or {}).items()
            }
            now_wall = utc_now()
            now_iso = now_wall.isoformat()
            for symbol in cfg.symbols:
                source_age = source_ages.get(symbol)
                heartbeat_age = float(heartbeat_ages.get(symbol, 999999))
                source_age_float = float(source_age) if source_age is not None else None
                if source_age_float is not None and source_age_float <= cfg.freshness_threshold_seconds and heartbeat_age > cfg.freshness_threshold_seconds:
                    failures.append(f"{symbol} local canonical heartbeat stale while source fresh: {heartbeat_ages}")
                consumer_blocks = {
                    identity: consumer.signal_block_reason_by_symbol(
                        symbol,
                        now=now_wall,
                        threshold_seconds=cfg.freshness_threshold_seconds,
                    )
                    for identity, consumer in consumers.items()
                }
                failures.extend(
                    transient_tracker.update(
                        symbol=symbol,
                        now_monotonic=time.monotonic(),
                        now_iso=now_iso,
                        source_freshness_age_seconds=source_age_float,
                        raw_trade_count=raw_trade_counts.get(symbol),
                        eligible_raw_trade_count=eligible_raw_trade_counts.get(symbol),
                        canonical_publication_liveness_age_seconds=publication_ages.get(symbol),
                        sip_healthy=sip_healthy,
                        consumer_block_reasons=consumer_blocks,
                        latest_event=latest_canonical_event_by_symbol.get(symbol),
                    )
                )
            if time.monotonic() >= next_sample:
                sample = {
                    "timestamp": now_iso,
                    "elapsed_seconds": elapsed,
                    "services": service_snapshot(),
                    "ss": tcp_python_443(),
                    "health_ages": event_ages,
                    "canonical_heartbeat_ages": heartbeat_ages,
                    "canonical_source_freshness_ages": source_ages,
                    "canonical_publication_liveness_ages": publication_ages,
                    "raw_trade_counts_by_symbol": health.get("raw_trade_count_by_symbol") or {},
                    "eligible_raw_trade_counts_by_symbol": eligible_raw_trade_counts,
                    "excluded_raw_trade_counts_by_symbol": health.get("excluded_raw_trade_count_by_symbol") or {},
                    "archive_policy": {
                        "raw_archive_enabled": cfg.raw_archive_enabled,
                        "nonessential_canonical_archive_enabled": cfg.nonessential_canonical_archive_enabled,
                        "canonical_journal_enabled": cfg.canonical_journal_enabled,
                    },
                    "health_flags": health_flags,
                    "pipeline_metrics": health.get("pipeline_metrics") or {},
                    "raw_counts": dict(raw_counts),
                    "canonical_counts": dict(canonical_counts),
                    "transient_source_staleness": transient_tracker.summary(now_monotonic=time.monotonic()),
                    "consumers": {identity: consumer.summary() for identity, consumer in consumers.items()},
                }
                append_jsonl(samples_path, sample)
                report["samples"].append(sample)
                write_json(report_path, report)
                next_sample = time.monotonic() + 60
            await asyncio.sleep(1)
    finally:
        await asyncio.gather(*(consumer.disconnect() for consumer in consumers.values()), return_exceptions=True)

    service_end = systemctl_show(MARKET_SERVICE)
    overlap_maps = {
        identity: {(event["symbol"], event["sequence"]): event for event in consumer.events}
        for identity, consumer in consumers.items()
    }
    overlap_keys = set.intersection(*(set(mapping) for mapping in overlap_maps.values())) if len(overlap_maps) == 2 else set()
    mismatches = []
    for key in sorted(overlap_keys):
        left = overlap_maps["camatron"][key]
        right = overlap_maps["megatron"][key]
        if left["event_id"] != right["event_id"] or left["hash"] != right["hash"]:
            mismatches.append({"key": list(key), "camatron": left, "megatron": right})

    final_health = read_json(Path(cfg.health_path)) if Path(cfg.health_path).exists() else {}
    raw_wait = latest_pipeline_metric(final_health, "raw_archive_queue_wait_seconds")
    event_loop_lag = latest_pipeline_metric(final_health, "event_loop_lag_seconds")
    journal_write = latest_pipeline_metric(final_health, "canonical_journal_durable_write_seconds")
    high_water = latest_pipeline_high_water(final_health)
    policy = {
        "raw_archive_enabled": bool(final_health.get("raw_archive_enabled")),
        "nonessential_canonical_archive_enabled": bool(final_health.get("nonessential_canonical_archive_enabled")),
        "canonical_journal_enabled": bool(final_health.get("canonical_journal_enabled")),
    }
    consumer_latency = collect_consumer_latency(consumers)
    local_latency_by_symbol = consumer_latency["local_latency_by_symbol"]
    live_local_latency_by_symbol = consumer_latency["live_local_latency_by_symbol"]
    replay_local_latency_by_symbol = consumer_latency["replay_local_latency_by_symbol"]
    ack_latency_by_symbol = consumer_latency["ack_latency_by_symbol"]
    all_latency = consumer_latency["all_latency"]
    live_latency = consumer_latency["live_latency"]
    replay_latency = consumer_latency["replay_latency"]
    all_upstream_age = [value for values in upstream_age_by_symbol.values() for value in values]
    if tcp_python_443()["python_443_count"] != 1:
        failures.append("SIP websocket count is not exactly one")
    if any(count <= 0 for count in raw_counts.values()) or set(raw_counts) != set(cfg.symbols):
        failures.append(f"missing raw trades: {dict(raw_counts)}")
    if any(count <= 0 for count in canonical_counts.values()) or set(canonical_counts) != set(cfg.symbols):
        failures.append(f"missing canonical observations: {dict(canonical_counts)}")
    if nanoseconds:
        failures.append(f"nanosecond canonical timestamps {nanoseconds[:3]}")
    if duplicate_seconds:
        failures.append(f"duplicate canonical symbol-seconds {duplicate_seconds[:3]}")
    if mismatches:
        failures.append("consumer event/hash mismatch")
    for identity, consumer in consumers.items():
        if consumer.gaps:
            failures.append(f"{identity} gaps {consumer.gaps[:3]}")
        if consumer.duplicates:
            failures.append(f"{identity} duplicates {consumer.duplicates[:3]}")
        if not consumer.catchup_complete:
            failures.append(f"{identity} did not catch up")
    for replay in replays:
        after = replay.get("after") or {}
        first_after = after.get("first_after_reconnect") or []
        if replay.get("new_events", 0) <= 0 or not first_after:
            failures.append(f"{replay['identity']} replay did not produce catch-up evidence")
    if service_start.get("MainPID") != service_end.get("MainPID") or service_start.get("NRestarts") != service_end.get("NRestarts"):
        failures.append(f"shared service restarted: {service_start} -> {service_end}")
    for unit in [CAMATRON_SERVICE, MEGATRON_SERVICE]:
        if systemctl_show(unit).get("ActiveState") != "inactive":
            failures.append(f"{unit} became active during validation")
    local_latency_stats = stats(live_latency)
    if local_latency_stats["p95"] is None or float(local_latency_stats["p95"] or 999) >= 1.0:
        failures.append(f"local ingress-to-consumer p95 gate failed: {local_latency_stats}")
    if local_latency_stats["p99"] is None or float(local_latency_stats["p99"] or 999) >= 2.0:
        failures.append(f"local ingress-to-consumer p99 gate failed: {local_latency_stats}")
    if local_latency_stats["max"] is None or float(local_latency_stats["max"] or 999) >= 5.0:
        failures.append(f"local ingress-to-consumer max gate failed: {local_latency_stats}")
    if policy["raw_archive_enabled"] and (raw_wait.get("p99") is None or float(raw_wait.get("p99")) > 0.5):
        failures.append(f"raw archive queue wait p99 gate failed: {raw_wait}")
    if event_loop_lag.get("p99") is None or float(event_loop_lag.get("p99")) >= 0.1:
        failures.append(f"event-loop lag p99 gate failed: {event_loop_lag}")
    if event_loop_lag.get("max") is None or float(event_loop_lag.get("max")) > 1.0:
        failures.append(f"event-loop spike max gate failed: {event_loop_lag}")
    if float(((final_health.get("pipeline_metrics") or {}).get("gauges") or {}).get("event_loop_consecutive_spikes_over_500ms", 0) or 0) > 1:
        failures.append("consecutive event-loop spikes above 500ms")
    if journal_write.get("p99") is None or float(journal_write.get("p99")) > 0.1:
        failures.append(f"canonical journal write p99 gate failed: {journal_write}")
    if float(high_water.get("archive_queue_depth", 0) or 0) > 500:
        failures.append(f"archive queue high-water excessive: {high_water}")
    if final_health.get("critical_events"):
        failures.append(f"critical health events present: {final_health.get('critical_events')}")
    failures.extend(transient_tracker.activation_failures(now_monotonic=time.monotonic()))

    report.update(
        {
            "completed_at": iso_now(),
            "final_health": final_health,
            "final_services": service_snapshot(),
            "raw_counts": dict(raw_counts),
            "canonical_counts": dict(canonical_counts),
            "canonical_unique_symbol_seconds": len(canonical_seconds),
            "nanosecond_violations": nanoseconds,
            "duplicate_symbol_seconds": duplicate_seconds,
            "latency_seconds": {
                "local_ingress_to_consumer_by_symbol": {symbol: stats(values) for symbol, values in local_latency_by_symbol.items()},
                "local_ingress_to_consumer_overall": stats(all_latency),
                "live_local_ingress_to_consumer_by_symbol": {
                    symbol: stats(values) for symbol, values in live_local_latency_by_symbol.items()
                },
                "live_local_ingress_to_consumer_overall": stats(live_latency),
                "replay_catchup_local_ingress_to_consumer_by_symbol": {
                    symbol: stats(values) for symbol, values in replay_local_latency_by_symbol.items()
                },
                "replay_catchup_local_ingress_to_consumer_overall": stats(replay_latency),
                "local_ingress_to_consumer_gate_scope": "live_delivery_only",
                "local_ingress_to_consumer_outliers": consumer_latency["outliers"],
                "ack_checkpoint_by_symbol": {symbol: stats(values) for symbol, values in ack_latency_by_symbol.items()},
                "upstream_exchange_to_ingress_by_symbol": {symbol: stats(values) for symbol, values in upstream_age_by_symbol.items()},
                "upstream_exchange_to_ingress_overall": stats(all_upstream_age),
                "upstream_exchange_to_ingress_by_symbol_minute": {
                    symbol: {minute: stats(values) for minute, values in minutes.items()}
                    for symbol, minutes in upstream_age_by_minute.items()
                },
            },
            "archive_policy": policy,
            "canonical_freshness_seconds": {
                "local_publication_liveness": final_health.get("canonical_publication_liveness_age_seconds") or {},
                "local_canonical_heartbeat": final_health.get("canonical_heartbeat_age_seconds") or {},
                "market_source_freshness": final_health.get("canonical_source_freshness_age_seconds") or {},
                "latest_source_timestamp": final_health.get("last_canonical_source_timestamp_by_symbol") or {},
                "latest_sampler_admission": final_health.get("last_canonical_sampler_admission_by_symbol") or {},
                "latest_publication": final_health.get("last_canonical_publication_by_symbol") or {},
            },
            "transient_source_staleness": transient_tracker.summary(now_monotonic=time.monotonic()),
            "event_loop_original_100ms_target": {
                "target_seconds": 0.1,
                "actual_p99_seconds": event_loop_lag.get("p99"),
                "met": event_loop_lag.get("p99") is not None and float(event_loop_lag.get("p99")) < 0.1,
            },
            "pipeline_gate_metrics": {
                "raw_archive_queue_wait_seconds": raw_wait,
                "event_loop_lag_seconds": event_loop_lag,
                "canonical_journal_durable_write_seconds": journal_write,
                "high_water": high_water,
            },
            "reconnect_replay": replays,
            "consumers": {identity: consumer.summary() for identity, consumer in consumers.items()},
            "overlap": {"count": len(overlap_keys), "mismatches": mismatches[:20]},
            "failures": failures,
            "status": "passed" if not failures else "failed",
        }
    )
    write_json(report_path, report)
    if failures:
        raise RuntimeError("; ".join(failures[:8]))
    return report


def start_unit(unit: str) -> None:
    proc = run(["systemctl", "start", unit])
    if proc.returncode != 0:
        raise RuntimeError(f"failed to start {unit}: {proc.stderr or proc.stdout}")


def stop_unit(unit: str) -> None:
    run(["systemctl", "stop", unit])


def activation_deadline_et() -> pd.Timestamp:
    return pd.Timestamp(f"{TARGET_DAY} {ACTIVATION_DEADLINE_TIME}", tz=ET)


def pre_activation_preflight(evidence_dir: Path) -> dict[str, Any]:
    market_cfg = SharedMarketDataConfig.load(CONFIG_PATH)
    deployed_checksums = verify_deployed_checksums_against_report()
    services = service_snapshot()
    market = services[MARKET_SERVICE]
    if market.get("ActiveState") != "active" or market.get("SubState") != "running" or int(market.get("MainPID") or 0) <= 0:
        raise RuntimeError(f"shared market-data service is not active for handoff: {market}")
    for unit in [CAMATRON_SERVICE, MEGATRON_SERVICE]:
        if services[unit].get("ActiveState") != "inactive":
            raise RuntimeError(f"{unit} is not inactive before handoff: {services[unit]}")
    websocket = tcp_python_443()
    if websocket["python_443_count"] != 1:
        raise RuntimeError(f"SIP websocket count before handoff is not exactly one: {websocket}")
    camatron = validate_expected_trader(CAMATRON_CONFIG, EXPECTED_CAMATRON, reject_qqq=True)
    megatron = validate_expected_trader(MEGATRON_CONFIG, EXPECTED_MEGATRON, reject_qqq=False)
    state = validate_state_files(market_cfg)
    broker = broker_snapshot()
    assert_broker_flat(broker)
    snapshot = {
        "timestamp": iso_now(),
        "deployed_checksums": deployed_checksums,
        "services": services,
        "sip_websocket": websocket,
        "enabled_profiles": {"camatron": camatron, "megatron": megatron},
        "state_files": state,
        "broker": broker,
    }
    write_json(evidence_dir / "pre_activation_preflight.json", snapshot)
    return snapshot


def run_activation_handoff(
    *,
    validation: dict[str, Any] | None,
    evidence_dir: Path,
    start_fn=start_unit,
    preflight_fn=pre_activation_preflight,
    now_fn=et_now,
    service_snapshot_fn=service_snapshot,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "timestamp": iso_now(),
        "deadline_label": ACTIVATION_DEADLINE_LABEL,
        "deadline_et": activation_deadline_et().isoformat(),
        "status": "blocked",
        "started_units": [],
    }
    if not isinstance(validation, dict):
        result["reason"] = "missing_validation_result"
        write_json(evidence_dir / "activation_handoff_report.json", result)
        return result
    if validation.get("status") != "passed" or validation.get("failures"):
        result["reason"] = "validation_not_unequivocally_passed"
        result["validation_status"] = validation.get("status")
        result["validation_failures"] = validation.get("failures")
        write_json(evidence_dir / "activation_handoff_report.json", result)
        return result
    completed_at = validation.get("completed_at")
    if not completed_at:
        result["reason"] = "missing_validation_completion_timestamp"
        write_json(evidence_dir / "activation_handoff_report.json", result)
        return result
    completed_et = pd.Timestamp(completed_at).tz_convert(ET)
    result["validation_completed_at_et"] = completed_et.isoformat()
    if completed_et >= activation_deadline_et():
        result["reason"] = "validation_completed_after_09_40_et_deadline"
        write_json(evidence_dir / "activation_handoff_report.json", result)
        return result
    latest_activation = pd.Timestamp(f"{TARGET_DAY} 15:00:00", tz=ET)
    current_et = now_fn()
    result["handoff_evaluated_at_et"] = current_et.isoformat()
    if current_et >= latest_activation:
        result["reason"] = "fewer_than_60_minutes_remain_before_regular_close"
        write_json(evidence_dir / "activation_handoff_report.json", result)
        return result
    try:
        result["pre_activation_preflight"] = preflight_fn(evidence_dir)
    except Exception as exc:
        result["reason"] = "pre_activation_preflight_failed"
        result["error"] = str(exc)
        write_json(evidence_dir / "activation_handoff_report.json", result)
        return result
    for unit in [CAMATRON_SERVICE, MEGATRON_SERVICE]:
        start_fn(unit)
        result["started_units"].append(unit)
    result["status"] = "activated"
    result["reason"] = "validation_passed_before_deadline_and_pre_activation_preflight_passed"
    result["services_after_start"] = service_snapshot_fn()
    write_json(evidence_dir / "activation_handoff_report.json", result)
    return result


def verify_protection(strategy_id: str, symbol: str, strategy_type: str, broker_account: dict[str, Any]) -> dict[str, Any]:
    positions = broker_account.get("target_positions") or []
    orders = broker_account.get("target_open_orders_nested") or []
    symbol_positions = [p for p in positions if str(p.get("symbol", "")).upper() == symbol]
    symbol_orders = [o for o in orders if str(o.get("symbol", "")).upper() == symbol]
    if not symbol_positions:
        return {"strategy_id": strategy_id, "symbol": symbol, "position": None, "protected": True, "orders": symbol_orders}
    order_blob = json.dumps(symbol_orders, sort_keys=True, default=json_default).lower()
    if strategy_type == "renko_tp_sl":
        protected = "take_profit" in order_blob and "stop_loss" in order_blob
    else:
        protected = "stop" in order_blob
    return {
        "strategy_id": strategy_id,
        "symbol": symbol,
        "position": symbol_positions,
        "protected": protected,
        "orders": symbol_orders,
    }


def monitor_until_noon(evidence_dir: Path) -> dict[str, Any]:
    report_path = evidence_dir / "noon_canary_monitor_report.json"
    samples_path = evidence_dir / "noon_canary_monitor_samples.jsonl"
    end = pd.Timestamp(f"{TARGET_DAY} 15:30:00", tz=ET).tz_convert("UTC")
    service_baseline = service_snapshot()
    event_logs = {
        "camatron": ROOT / "artifacts/live/fleet_live_events.jsonl",
        "megatron": ROOT / "artifacts/live/megatron/events/fleet_events.jsonl",
    }
    log_tailers = {name: Tailer.from_end(path) for name, path in event_logs.items()}
    events_by_strategy: dict[str, list[dict[str, Any]]] = defaultdict(list)
    failures: list[str] = []
    strategies = {**EXPECTED_CAMATRON, **EXPECTED_MEGATRON}
    while utc_now() < end:
        health = read_json(Path(SharedMarketDataConfig.load(CONFIG_PATH).health_path)) if Path(SharedMarketDataConfig.load(CONFIG_PATH).health_path).exists() else {}
        broker = broker_snapshot()
        for name, tailer in log_tailers.items():
            for line in tailer.read_new():
                with contextlib.suppress(Exception):
                    payload = json.loads(line)
                    tid = str(payload.get("trader_id") or payload.get("id") or "")
                    if tid in strategies:
                        events_by_strategy[tid].append(payload)
        protections = []
        for tid, expected in strategies.items():
            account_name = "megatron" if tid.startswith("megatron_") else "camatron"
            protection = verify_protection(tid, expected["symbol"], expected["type"], broker["accounts"][account_name])
            protections.append(protection)
            if not protection["protected"]:
                failures.append(f"{tid} has unprotected broker exposure")
                stop_unit(MEGATRON_SERVICE if account_name == "megatron" else CAMATRON_SERVICE)
        ages = health.get("canonical_heartbeat_age_seconds") or health.get("observation_age_seconds") or {}
        source_ages = health.get("canonical_source_freshness_age_seconds") or {}
        if any(float(ages.get(symbol, 999999)) > 5.0 for symbol in ["AAPL", "NVDA", "TSLA"]):
            failures.append(f"shared SIP stale during canary: {ages}")
            stop_unit(CAMATRON_SERVICE)
            stop_unit(MEGATRON_SERVICE)
        if any(float(source_ages.get(symbol, 999999)) > 5.0 for symbol in ["AAPL", "NVDA", "TSLA"]):
            failures.append(f"shared SIP source stale during canary: {source_ages}")
            stop_unit(CAMATRON_SERVICE)
            stop_unit(MEGATRON_SERVICE)
        sample = {
            "timestamp": iso_now(),
            "services": service_snapshot(),
            "service_baseline": service_baseline,
            "health_ages": ages,
            "source_freshness_ages": source_ages,
            "pipeline_metrics": health.get("pipeline_metrics") or {},
            "broker": broker,
            "protections": protections,
            "event_counts_by_strategy": {tid: len(events) for tid, events in events_by_strategy.items()},
            "failures": failures[-20:],
        }
        append_jsonl(samples_path, sample)
        write_json(report_path, sample | {"status": "failed" if failures else "running"})
        if failures:
            break
        time.sleep(30)
    final_broker = broker_snapshot()
    strategy_reports = {}
    for tid, expected in strategies.items():
        events = events_by_strategy.get(tid, [])
        entries = [e for e in events if "order" in str(e.get("event_type") or e.get("type") or "").lower() or "entry" in str(e).lower()]
        signals = [e for e in events if "signal" in str(e.get("event_type") or e.get("type") or "").lower()]
        bricks = [e for e in events if "brick" in str(e.get("event_type") or e.get("type") or "").lower()]
        account_name = "megatron" if tid.startswith("megatron_") else "camatron"
        protection = verify_protection(tid, expected["symbol"], expected["type"], final_broker["accounts"][account_name])
        label = "FAIL — KEEP STOPPED" if failures else ("PASS — OPERATIONAL LIVE LIFECYCLE OBSERVED" if entries else "NO LIVE SIGNAL OBSERVED")
        strategy_reports[tid] = {
            "symbol": expected["symbol"],
            "observations_and_bricks": {"brick_events": len(bricks)},
            "signals": len(signals),
            "entry_attempts": len(entries),
            "broker_state": final_broker["accounts"][account_name],
            "protection": protection,
            "event_sample": events[-20:],
            "label": label,
        }
    final = {
        "status": "failed" if failures else "completed",
        "completed_at": iso_now(),
        "failures": failures,
        "services": service_snapshot(),
        "broker": final_broker,
        "strategy_reports": strategy_reports,
    }
    write_json(report_path, final)
    return final


async def main() -> None:
    run_id = f"post_fix_active_20260826T{utc_now().strftime('%H%M%SZ')}"
    evidence_dir = EVIDENCE_ROOT / run_id
    evidence_dir.mkdir(parents=True, exist_ok=True)
    final_report = evidence_dir / "orchestration_report.json"
    orchestration: dict[str, Any] = {"status": "running", "started_at": iso_now(), "evidence_dir": str(evidence_dir), "phases": []}
    write_json(final_report, orchestration)
    try:
        pre = preflight(evidence_dir)
        orchestration["phases"].append({"phase": "preflight", "status": "passed", "summary": pre})
        write_json(final_report, orchestration)

        market_service_start_requested_epoch = time.time()
        start_unit(MARKET_SERVICE)
        readiness = assert_market_service_ready(
            SharedMarketDataConfig.load(CONFIG_PATH),
            evidence_dir=evidence_dir,
            started_after_epoch=market_service_start_requested_epoch,
        )
        orchestration["phases"].append(
            {
                "phase": "shared_service_start",
                "status": "started",
                "services": service_snapshot(),
                "readiness": readiness,
            }
        )
        write_json(final_report, orchestration)

        validation = await active_validation(evidence_dir)
        orchestration["phases"].append({"phase": "active_validation", "status": validation["status"], "report": str(evidence_dir / "active_validation_report.json")})
        write_json(final_report, orchestration)

        handoff = run_activation_handoff(validation=validation, evidence_dir=evidence_dir)
        orchestration["phases"].append({"phase": "activation_handoff", "status": handoff["status"], "report": str(evidence_dir / "activation_handoff_report.json")})
        if handoff["status"] != "activated":
            orchestration["status"] = "activation_blocked"
            orchestration["recommendation"] = f"Fleets not started: {handoff.get('reason')}"
            write_json(final_report, orchestration)
            return

        orchestration["phases"].append({"phase": "fleet_activation", "status": "started", "services": service_snapshot(), "activated_at": iso_now()})
        write_json(final_report, orchestration)
        noon = monitor_until_noon(evidence_dir)
        orchestration["phases"].append({"phase": "noon_monitor", "status": noon["status"], "report": str(evidence_dir / "noon_canary_monitor_report.json")})
        orchestration["status"] = "completed" if noon["status"] == "completed" else "failed"
        write_json(final_report, orchestration)
    except Exception as exc:
        stop_unit(CAMATRON_SERVICE)
        stop_unit(MEGATRON_SERVICE)
        orchestration["status"] = "failed"
        orchestration["error"] = str(exc)
        orchestration["services"] = service_snapshot()
        with contextlib.suppress(Exception):
            orchestration["broker"] = broker_snapshot()
        write_json(final_report, orchestration)
        raise
    finally:
        stop_timer_after_completion()
        if service_inactive(CAMATRON_SERVICE) and service_inactive(MEGATRON_SERVICE):
            stop_unit(MARKET_SERVICE)
        orchestration["timer_cleanup"] = {
            TIMER_UNIT: systemctl_show(TIMER_UNIT),
            SERVICE_UNIT: systemctl_show(SERVICE_UNIT),
        }
        orchestration["final_services"] = service_snapshot()
        with contextlib.suppress(Exception):
            orchestration["final_broker"] = broker_snapshot()
        write_json(final_report, orchestration)


if __name__ == "__main__":
    asyncio.run(main())
