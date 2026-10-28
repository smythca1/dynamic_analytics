"""Research integrity gates. This module never submits orders or starts a sweep.

Production reuse: CanonicalOneSecondSipSampler, market_stream_contract,
effective_renko_brick_size, RenkoStrategyEngine (through runner.run_candidate),
NextObservationExecution, AccountingLedger and Silver trade metrics.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation, localcontext
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

from replay.execution import ExecutionFriction, RawExecutionAssumptions, EquityRegulatoryFees
from shared.config.trader_config import effective_renko_brick_size
from silver.market import CanonicalObservation, CanonicalOneSecondSipSampler, RawSipTrade, market_stream_contract

from .market_calendar import EASTERN, is_nyse_early_close, latest_completed_nyse_sessions, nyse_session_window
from .models import CandidateSpec, OptimizationConfig, PreparedSource, StrategyOptimizationParameters, TraderSelection
from .runner import brick_grid, build_candidate_specs, run_candidate


VERSION = "tsla.research_integrity.v1"
EXIT_PAIRS = ((1, 1), (2, 1), (2, 2), (3, 1), (3, 2))
STEP = Decimal("0.01")
LOW = Decimal("0.16")
HIGH = Decimal("0.66")
CALIBRATION_POLICY = "CANONICAL_AVAILABLE_NEXT_ELIGIBLE_OR_NATIVE_EXIT_V1"


def slippage_distribution(values):
    ordered = sorted(Decimal(str(value)) for value in values)
    if not ordered or any(not value.is_finite() for value in ordered):
        raise ValueError("Calibration requires finite nonempty observations")
    with localcontext() as context:
        context.prec = 28
        def quantile(fraction):
            index = Decimal(len(ordered) - 1) * fraction
            lower = int(index)
            return ordered[lower] + (ordered[min(lower + 1, len(ordered) - 1)] - ordered[lower]) * (index - lower)
        mean = sum(ordered, Decimal(0)) / len(ordered)
        return {"count": len(ordered), "mean": str(mean), "median": str(quantile(Decimal(".5"))),
                "p75": str(quantile(Decimal(".75"))), "p90": str(quantile(Decimal(".9"))),
                "minimum": str(ordered[0]), "maximum": str(ordered[-1]),
                "population_stddev": str((sum((v - mean) ** 2 for v in ordered) / len(ordered)).sqrt())}


def calibrate_live_fills(records, *, holdout_sessions=()):
    """Unweighted fill observations; callers supply auditable causal references."""
    from silver.events.models import utc_datetime

    unique = {}
    duplicates = 0
    for record in records:
        row = dict(record)
        required = ("account", "activity_id", "order_id", "timestamp", "reference_timestamp", "provenance")
        if any(not row.get(key) for key in required) or row.get("symbol") != "TSLA" or row.get("origin") != "live":
            raise ValueError("Missing authoritative live TSLA calibration provenance")
        if row.get("reference_policy") != CALIBRATION_POLICY or row.get("side") not in {"buy", "sell"}:
            raise ValueError("Invalid calibration side or reference policy")
        for field in ("fill_price", "reference_price", "qty"):
            try:
                value = Decimal(str(row[field]))
            except (KeyError, InvalidOperation) as exc:
                raise ValueError("Missing or invalid calibration price or quantity") from exc
            if not value.is_finite() or value <= 0:
                raise ValueError("Invalid calibration price or quantity")
            row[field] = str(value)
        for field in ("timestamp", "reference_timestamp"):
            if datetime.fromisoformat(str(row[field]).replace("Z", "+00:00")).tzinfo is None:
                raise ValueError("Calibration timestamp requires explicit timezone")
        timestamp = utc_datetime(row["timestamp"])
        reference_time = utc_datetime(row["reference_timestamp"])
        if reference_time > timestamp:
            raise ValueError("Reference is not causal")
        session = timestamp.astimezone(EASTERN).date().isoformat()
        if session in {str(day) for day in holdout_sessions}:
            raise ValueError("Calibration cannot consume holdout evidence")
        row["session"] = session
        key = (row["account"], row["activity_id"])
        if key in unique:
            if unique[key] != row:
                raise ValueError("Conflicting duplicate calibration activity")
            duplicates += 1
        unique[key] = row
    if not unique:
        raise ValueError("No valid empirical calibration observations")
    rows = [unique[key] for key in sorted(unique)]
    with localcontext() as context:
        context.prec = 28
        for row in rows:
            difference = Decimal(row["fill_price"]) - Decimal(row["reference_price"])
            signed = difference if row["side"] == "buy" else -difference
            row.update(signed_slippage=str(signed), adverse_component=str(max(signed, Decimal(0))))
        signed = slippage_distribution([row["signed_slippage"] for row in rows])
        adverse = slippage_distribution([row["adverse_component"] for row in rows])
        base = Decimal(adverse["mean"])
        stress = base * 2
    result = {"policy": CALIBRATION_POLICY, "observations": rows, "duplicates": duplicates,
              "signed": signed, "adverse": adverse, "base": str(base), "stress": str(stress),
              "sessions": sorted({row["session"] for row in rows}),
              "confidence": "LIMITED", "quantiles": "linear interpolation at (n-1)*q",
              "fee_status": "UNVERIFIED: fill evidence contains no authoritative fee schedule",
              "limitations": ["Small, selected historical sample; not a statistical tail bound",
                              "Trade-only causal references are research proxies, not NBBO or exact broker simulation"]}
    result["identity"] = fingerprint(result)
    return result


def calibrated_friction(calibration, *, fee_per_share=0, fee_per_fill=0):
    payload = {key: value for key, value in calibration.items() if key != "identity"}
    if calibration.get("identity") != fingerprint(payload):
        raise ValueError("Calibration evidence was modified")
    return ExecutionFriction(
        slippage_per_share=calibration["base"], application="PER_FILL_CHARGE_V1",
        base_slippage_per_share=calibration["base"], stress_slippage_per_share=calibration["stress"],
        calibration="EMPIRICAL_TSLA_LIMITED", calibration_evidence_hash=calibration["identity"],
        calibration_sessions=tuple(calibration["sessions"]), fee_per_share=fee_per_share,
        fee_per_fill=fee_per_fill, fee_assumption=calibration["fee_status"],
    )


def published_fee_fallback(friction: ExecutionFriction) -> ExecutionFriction:
    """Keep the accepted calibration intact; add only the approved fee policy."""
    if (friction.application != "PER_FILL_CHARGE_V1" or friction.base_slippage_per_share != Decimal(".03495")
            or friction.stress_slippage_per_share != Decimal(".06990") or not friction.calibration_evidence_hash):
        raise ValueError("Fee certification requires the previously frozen TSLA slippage calibration")
    return replace(friction, regulatory_fees=EquityRegulatoryFees(),
                   fee_assumption="PUBLISHED_SCHEDULE_FALLBACK; STANDARD_DIRECT_RETAIL_COMMISSION_ZERO; ACCOUNT_SPECIFIC_STATUS_UNVERIFIED")


def validate_calibration_partition(friction, partition):
    if friction.application != "PER_FILL_CHARGE_V1":
        return
    if not friction.calibration_evidence_hash or not friction.calibration_sessions:
        raise ValueError("Empirical research requires frozen calibration provenance")
    if any(date.fromisoformat(day) >= partition.holdout[0] for day in friction.calibration_sessions):
        raise ValueError("Calibration must precede untouched holdout")


def calibrate_broker_export(broker_path, events_path, archive_root, *, holdout_sessions=()):
    """Read-only extraction. No broker connections and no strategy evaluation."""
    from collections import Counter
    from types import SimpleNamespace
    from replay.historical_source import _read_rows, _row_to_trade, file_sha256
    from replay.execution import raw_trade_groups
    from replay.stop_election import CONSOLIDATED_TAPES, excluded_condition_reason
    from silver.events.models import utc_datetime

    broker_path, events_path, archive_root = Path(broker_path), Path(events_path), Path(archive_root)
    broker = json.loads(broker_path.read_text())
    events = json.loads(events_path.read_text())
    provenance = {str(broker_path): file_sha256(broker_path), str(events_path): file_sha256(events_path)}
    intents = {}
    for account, capture in events["events"].items():
        for event in capture.get("events", []):
            payload = event.get("payload", {})
            intent = payload.get("intent", {})
            if event.get("event_type") == "execution" and payload.get("mode") == "real" and intent.get("symbol") == "TSLA":
                intents[(account, intent.get("client_order_id"))] = intent
    candidates, excluded, seen = [], [], {}
    found = duplicates = 0
    for account, data in sorted(broker["accounts"].items()):
        orders, parents = {}, {}
        for order in data.get("recent_orders", []):
            orders[order["id"]] = order
            for child in order.get("legs") or []:
                orders[child["id"]] = child
                parents[child["id"]] = order
        for fill in data.get("recent_fills", []):
            if fill.get("symbol") != "TSLA":
                continue
            found += 1
            key = (account, fill.get("id"))
            if key in seen:
                if seen[key] != fill:
                    raise ValueError("Conflicting broker fill activity")
                duplicates += 1
                continue
            seen[key] = fill
            row = {"account": account, "activity_id": fill.get("id"), "order_id": fill.get("order_id"),
                   "timestamp": fill.get("transaction_time"), "side": "sell" if fill.get("side") == "sell_short" else fill.get("side"),
                   "broker_activity_side": fill.get("side"), "symbol": "TSLA",
                   "fill_price": fill.get("price"), "qty": fill.get("qty"), "origin": "live",
                   "reference_policy": CALIBRATION_POLICY}
            order = orders.get(fill.get("order_id"), {})
            parent = parents.get(fill.get("order_id"))
            reason = None
            if data.get("base_url") != "https://api.alpaca.markets":
                reason = "not_authoritative_live_origin"
            elif not order or not fill.get("id") or fill.get("activity_type") != "FILL":
                reason = "missing_broker_order_or_activity_provenance"
            elif (parent or order).get("source") != "access_key":
                reason = "manual_or_unknown_submission_origin"
            elif order.get("symbol") != "TSLA" or order.get("side") != row["side"]:
                reason = "order_activity_mismatch"
            elif (Decimal(str(order.get("filled_qty") or 0)) != Decimal(str(fill.get("qty") or 0))
                  or Decimal(str(order.get("filled_avg_price") or 0)) != Decimal(str(fill.get("price") or 0))):
                reason = "partial_or_inconsistent_aggregate_fill_not_supported"
            elif order.get("replaces") or order.get("replaced_by"):
                reason = "replaced_order_reference_ambiguous"
            kind = order.get("type")
            if not reason and kind == "market":
                intent = intents.get((account, order.get("client_order_id")))
                if not intent or not intent.get("timestamp"):
                    reason = "missing_canonical_signal_availability_evidence"
                else:
                    row.update(kind="market", signal_timestamp=intent["timestamp"])
            elif not reason and kind in {"limit", "stop"} and parent and parent.get("filled_at"):
                row.update(kind=kind, activation=parent["filled_at"],
                           threshold=order.get("limit_price") if kind == "limit" else order.get("stop_price"),
                           parent_order_id=parent["id"])
                if not row["threshold"] or parent["side"] == row["side"]:
                    reason = "invalid_protective_reference"
            elif not reason:
                reason = "unsupported_or_unproven_order_activation"
            if reason:
                excluded.append({**row, "exclusion": reason})
            else:
                candidates.append(row)
    days = sorted({utc_datetime(row["timestamp"]).astimezone(EASTERN).date().isoformat() for row in candidates})
    if set(days) & {str(day) for day in holdout_sessions}:
        raise ValueError("Calibration archive overlaps holdout")
    for day in days:
        rows = [row for row in candidates if utc_datetime(row["timestamp"]).astimezone(EASTERN).date().isoformat() == day]
        path = archive_root / day / "sip_trades.jsonl.gz"
        manifest_path = path.with_name("sip_trades.jsonl.manifest.json")
        if not path.exists() or not manifest_path.exists():
            for row in rows:
                row["exclusion"] = "missing_raw_reference_archive"
            continue
        manifest = json.loads(manifest_path.read_text())
        digest = file_sha256(path)
        pages = manifest.get("pagination", {}).get("pages", [])
        window = nyse_session_window(date.fromisoformat(day))
        request = manifest.get("request_parameters", {})
        if (digest != manifest.get("sha256") or manifest.get("symbol") != "TSLA" or manifest.get("feed") != "sip"
                or not pages or pages[-1].get("next_page_token_present") is not False
                or utc_datetime(request.get("start")) != window.start_utc
                or utc_datetime(request.get("end")) != window.end_utc):
            raise ValueError("Calibration source hash/completeness contract failed")
        provenance[str(path)] = digest
        provenance[str(manifest_path)] = file_sha256(manifest_path)
        sampler = CanonicalOneSecondSipSampler(run_id="calibration")
        desired = {utc_datetime(row["signal_timestamp"]) for row in rows if row["kind"] == "market"}
        available = {}
        count = 0
        previous = None
        source_config = SimpleNamespace(symbol="TSLA", trader_id="calibration", source_timezone="UTC")
        def trades():
            nonlocal count, previous
            for index, raw in _read_rows(path):
                count += 1
                timestamp = utc_datetime(raw["t"])
                if previous is not None and timestamp < previous:
                    raise ValueError("Unordered calibration source")
                previous = timestamp
                if raw.get("symbol", raw.get("S")) != "TSLA" or not window.start_utc <= timestamp < window.end_utc:
                    raise ValueError("Calibration source outside symbol/session contract")
                # Once all causal references are found, finish count/ordering validation without constructing events.
                if all(row.get("reference_price") is not None for row in rows):
                    continue
                yield _row_to_trade(raw, row_order=index, config=source_config, source_ref=str(path), source_hash=digest)
        for group in raw_trade_groups(trades()):
            timestamp = group[0].sip_timestamp
            eligible = [trade for trade in group if trade.tape in CONSOLIDATED_TAPES and excluded_condition_reason(trade.conditions) is None]
            for row in rows:
                if row.get("reference_price") is not None:
                    continue
                reference = None
                if row["kind"] == "limit":
                    reference = Decimal(str(row["threshold"]))
                    reference_time = utc_datetime(row["activation"])
                elif eligible:
                    low, high = min(t.price for t in eligible), max(t.price for t in eligible)
                    reference_time = timestamp
                    if row["kind"] == "market":
                        activation = available.get(utc_datetime(row["signal_timestamp"]))
                        if activation is not None and timestamp > activation:
                            reference = high if row["side"] == "buy" else low
                    elif timestamp > utc_datetime(row["activation"]):
                        threshold = Decimal(str(row["threshold"]))
                        if (row["side"] == "buy" and high >= threshold) or (row["side"] == "sell" and low <= threshold):
                            reference = high if row["side"] == "buy" else low
                if reference is not None:
                    row.update(reference_price=str(reference), reference_timestamp=reference_time.isoformat())
            if desired - available.keys():
                for trade in group:
                    for observation in sampler.process_trade(trade).observations:
                        if observation.second_bucket in desired:
                            available[observation.second_bucket] = observation.triggering_sip_timestamp
        if count != manifest.get("raw_record_count") or count != sum(page["records"] for page in pages):
            raise ValueError("Calibration source row count failed")
    valid = []
    for row in candidates:
        if not row.get("reference_price"):
            row.setdefault("exclusion", "no_causal_reference_found")
        elif utc_datetime(row["reference_timestamp"]) > utc_datetime(row["timestamp"]):
            row["exclusion"] = "reference_occurs_after_live_fill"
        if row.get("exclusion"):
            excluded.append(row)
        else:
            row["provenance"] = provenance
            valid.append(row)
    calibration = calibrate_live_fills(valid, holdout_sessions=holdout_sessions)
    return {"found": found, "duplicates": duplicates, "unique": len(seen), "excluded": excluded,
            "exclusion_counts": dict(sorted(Counter(row["exclusion"] for row in excluded).items())),
            "provenance": provenance, "calibration": calibration}


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False)


def fingerprint(value: Any) -> str:
    return sha256(canonical_json(value).encode()).hexdigest()


def source_fingerprint(rows: tuple[RawSipTrade, ...]) -> str:
    # Byte-identical to canonical_json(list), without materializing the raw stream.
    digest = sha256(b"[")
    separator = b""
    for row in rows:
        digest.update(separator)
        digest.update(canonical_json(asdict(row)).encode())
        separator = b","
    digest.update(b"]")
    return digest.hexdigest()


def observation_semantics(observation: CanonicalObservation) -> tuple:
    # Exclude run-specific event IDs, retain market lineage and availability time.
    return (observation.symbol, observation.second_bucket.isoformat(), str(observation.close_price),
            observation.source_sip_timestamp.isoformat(), observation.source_record_id,
            observation.source_alpaca_trade_id, observation.source_exchange, observation.source_tape,
            observation.triggering_sip_timestamp.isoformat() if observation.triggering_sip_timestamp else None,
            observation.replay_finalized, observation.emitted_sequence, observation.sampler_version)


@dataclass(frozen=True)
class SourceEvidence:
    """Independent acquisition evidence, not a completeness guess from price gaps.

    Supply the checksum/count from the completed acquisition manifest. A hash
    computed from an already truncated file cannot establish completeness.
    """

    identity: str
    expected_hash: str
    expected_rows: int
    requested_start: datetime
    requested_end: datetime
    acquisition_complete: bool
    provenance: str
    backfilled: bool = False


@dataclass(frozen=True)
class CoverageEvidence:
    session: date
    start: datetime
    end: datetime
    first_observation: datetime | None
    last_observation: datetime | None
    expected_progression: tuple[tuple, ...]
    actual_progression: tuple[tuple, ...]
    gaps: tuple[tuple[str, str, int, str], ...]
    source_hash: str
    source_rows: int
    source_identity: str
    provenance: str
    pending_final_seconds: int
    failures: tuple[str, ...]
    status: str


def validate_session(
    session: date, rows: tuple[RawSipTrade, ...], evidence: SourceEvidence,
    actual: tuple[CanonicalObservation, ...] | None = None, *,
    symbol: str = "TSLA",
    sampler: CanonicalOneSecondSipSampler | None = None,
) -> tuple[CoverageEvidence, tuple[CanonicalObservation, ...]]:
    window = nyse_session_window(session)
    sampler = sampler or CanonicalOneSecondSipSampler(run_id="integrity")
    failures: list[str] = []
    digest = source_fingerprint(rows)
    if not evidence.acquisition_complete or evidence.provenance not in {
        "historical_sip_complete", "capture_reconciled", "synthetic_fixture"
    } or not evidence.identity:
        failures.append("source_completeness_unverified")
    if evidence.requested_start != window.start_utc or evidence.requested_end != window.end_utc:
        failures.append("acquisition_window_mismatch")
    if evidence.expected_hash != digest or evidence.expected_rows != len(rows):
        failures.append("source_hash_or_count_mismatch")
    if is_nyse_early_close(session):
        failures.append("not_full_length_regular_session")
    expected: list[CanonicalObservation] = []
    previous = None
    for row in rows:
        if row.symbol != symbol or not window.start_utc <= row.sip_timestamp <= window.end_utc:
            failures.append("source_outside_symbol_or_session")
            continue
        if not row.price.is_finite() or not row.quantity.is_finite():
            failures.append("nonfinite_source_value")
            continue
        if previous is not None and row.ordering_key < previous:
            failures.append("out_of_order_source")
        previous = row.ordering_key
        result = sampler.process_trade(row)
        if any(item.diagnostic_type in {"duplicate_source_trade", "ambiguous_equal_ordering_trade",
                                        "older_prior_bucket_trade", "older_same_bucket_trade"}
               for item in result.diagnostics):
            failures.append("ambiguous_or_duplicate_source")
        expected.extend(result.observations)
    expected_tuple = tuple(expected)
    actual = expected_tuple if actual is None else actual
    exp = tuple(observation_semantics(item) for item in expected_tuple)
    act = tuple(observation_semantics(item) for item in actual)
    if exp != act:
        failures.append("canonical_progression_mismatch")
    if not expected:
        failures.append("no_canonical_observations")
    trusted_source = not failures
    gaps: list[tuple[str, str, int, str]] = []
    buckets = [item.second_bucket for item in expected if window.start_utc <= item.second_bucket <= window.end_utc]
    boundaries = [window.start_utc - timedelta(seconds=1), *buckets, window.end_utc + timedelta(seconds=1)]
    for left, right in zip(boundaries, boundaries[1:]):
        seconds = int((right - left).total_seconds()) - 1
        if seconds > 0:
            # The final occupied second is deliberately pending, never forward-filled.
            label = "LEGITIMATE_NO_EMISSION" if trusted_source else "UNVERIFIED_OR_INVALID"
            gaps.append(((left + timedelta(seconds=1)).isoformat(), (right - timedelta(seconds=1)).isoformat(), seconds, label))
    missing = set(exp) - set(act)
    for item in sorted(missing):
        gaps.append((item[1], item[1], 1, "INVALID_MISSING_CANONICAL_OBSERVATION"))
    status = "FAIL" if failures else "BACKFILLED_AND_PASS" if evidence.backfilled else "PASS"
    report = CoverageEvidence(
        session, window.start_utc, window.end_utc,
        actual[0].second_bucket if actual else None, actual[-1].second_bucket if actual else None,
        exp, act, tuple(gaps), digest, len(rows), evidence.identity, evidence.provenance,
        len(sampler.pending_diagnostics()), tuple(sorted(set(failures))), status,
    )
    return report, actual


@dataclass(frozen=True)
class ResearchPartition:
    optimization: tuple[date, ...]
    holdout: tuple[date, ...]

    def __post_init__(self) -> None:
        combined = self.optimization + self.holdout
        if not self.optimization or not self.holdout or tuple(sorted(set(combined))) != combined:
            raise ValueError("Partitions must be nonempty, unique, and strictly chronological")
        for session in combined:
            nyse_session_window(session)
            if is_nyse_early_close(session):
                raise ValueError("Only full-length sessions are eligible")

    @classmethod
    def completed(cls, now: datetime, training_count: int = 20, holdout_count: int = 5):
        if now.tzinfo is None or min(training_count, holdout_count) < 1:
            raise ValueError("An aware as-of timestamp and positive session counts are required")
        count = training_count + holdout_count
        sessions = latest_completed_nyse_sessions(now=now, count=count)
        while len([day for day in sessions if not is_nyse_early_close(day)]) < count:
            sessions = latest_completed_nyse_sessions(now=now, count=len(sessions) + 1)
        sessions = tuple(day for day in sessions if not is_nyse_early_close(day))[-count:]
        return cls(sessions[:training_count], sessions[training_count:])


@dataclass(frozen=True)
class ValidatedDataset:
    sessions: tuple[date, ...]
    observations: tuple[CanonicalObservation, ...]
    coverage: tuple[CoverageEvidence, ...]
    manifest_json: str
    identity: str
    raw_trades: tuple[RawSipTrade, ...] = ()

    def verify(self) -> None:
        if any(item.status == "FAIL" for item in self.coverage):
            raise ValueError("Failed coverage cannot enter research")
        manifest = json.loads(self.manifest_json)
        if fingerprint(manifest) != self.identity or manifest["observations_hash"] != fingerprint(
            [observation_semantics(item) for item in self.observations]
        ):
            raise ValueError("Dataset fingerprint mismatch")
        if tuple(manifest["sessions"]) != tuple(day.isoformat() for day in self.sessions):
            raise ValueError("Dataset session mismatch")
        if len(self.coverage) != len(self.sessions) or tuple(item.session for item in self.coverage) != self.sessions:
            raise ValueError("Dataset coverage mismatch")
        if manifest["coverage_hash"] != fingerprint([asdict(item) for item in self.coverage]):
            raise ValueError("Coverage evidence fingerprint mismatch")
        if manifest["observation_count"] != len(self.observations):
            raise ValueError("Observation count mismatch")
        if manifest["raw_execution_hash"] != source_fingerprint(self.raw_trades):
            raise ValueError("Raw execution evidence fingerprint mismatch")


def prepare_validated_dataset(
    sessions: tuple[date, ...],
    sources: dict[date, tuple[tuple[RawSipTrade, ...], SourceEvidence, tuple[CanonicalObservation, ...] | None]],
) -> ValidatedDataset:
    if not sessions or tuple(sorted(set(sessions))) != sessions or set(sources) != set(sessions):
        raise ValueError("Preparation takes exactly one chronological partition, never combined future data")
    sampler = CanonicalOneSecondSipSampler(run_id="integrity")
    observations: list[CanonicalObservation] = []
    reports: list[CoverageEvidence] = []
    scope = None
    for session in sessions:
        rows, evidence, actual = sources[session]
        for row in rows:
            scope = scope or row.scope_key
            if row.scope_key != scope:
                raise ValueError("One canonical source stream is required per partition")
        report, sampled = validate_session(session, rows, evidence, actual, sampler=sampler)
        if report.status == "FAIL":
            raise ValueError(f"Session {session} failed integrity: {report.failures}")
        reports.append(report)
        observations.extend(sampled)
    manifest = {
        "symbol": "TSLA", "sessions": [day.isoformat() for day in sessions],
        "market_contract": asdict(market_stream_contract(symbol="TSLA", session_timezone="America/New_York")),
        "preparation_version": VERSION, "source_rows": sum(item.source_rows for item in reports),
        "observation_count": len(observations),
        "sources": [{"identity": item.source_identity, "hash": item.source_hash, "rows": item.source_rows,
                     "provenance": item.provenance} for item in reports],
        "observations_hash": fingerprint([observation_semantics(item) for item in observations]),
        "coverage_hash": fingerprint([asdict(item) for item in reports]),
        "sampler_policy": "continuous_within_partition; NO_FLUSH; no future partition trigger",
        "raw_execution_hash": source_fingerprint(tuple(row for day in sessions for row in sources[day][0])),
    }
    dataset = ValidatedDataset(sessions, tuple(observations), tuple(reports), canonical_json(manifest), fingerprint(manifest),
                               tuple(row for day in sessions for row in sources[day][0]))
    dataset.verify()
    return dataset


def approved_universe_config(config: OptimizationConfig) -> OptimizationConfig:
    if config.execution_friction.application == "PER_FILL_CHARGE_V1":
        if config.execution_friction.slippage_per_share != config.execution_friction.base_slippage_per_share:
            raise ValueError("STRESS is finalist validation only, never candidate optimization")
        config = replace(config, execution_model="RAW_SIP_NATIVE_V1")
    return replace(config, parameter_grid_mode="RENKO_TP_SL_ENTRY_TP_SL_GRID_V1",
                   candidate_pcts=brick_grid(LOW, HIGH, STEP), selected_entry_confirmations=("E1", "E2"),
                   selected_take_profit_bricks=(1, 2, 3), selected_stop_loss_bricks=(1, 2),
                   allowed_exit_pairs=EXIT_PAIRS, include_current_production=False,
                   position_sizing_mode="PRODUCTION_CONFIG_QUANTITY_V1", conservative_protections=True)


def _prepared(dataset: ValidatedDataset) -> PreparedSource:
    manifest = json.loads(dataset.manifest_json)
    return PreparedSource(dataset.identity, "TSLA", dataset.sessions, (), dataset.identity,
                          len(dataset.observations), "", "", {}, "PASS",
                          market_stream_contract=manifest["market_contract"], promotable=False)


@dataclass(frozen=True)
class FrozenCandidate:
    spec_json: str
    assumptions_json: str
    training_identity: str
    training_sessions: tuple[date, ...]
    identity: str

    def spec(self) -> CandidateSpec:
        if self.identity != fingerprint([self.spec_json, self.assumptions_json, self.training_identity,
                                         [day.isoformat() for day in self.training_sessions]]):
            raise ValueError("Frozen candidate was modified")
        raw = json.loads(self.spec_json)
        raw["trader"] = TraderSelection(**raw["trader"])
        raw["strategy_parameters"] = StrategyOptimizationParameters(**raw["strategy_parameters"])
        for key in ("brick_pct", "brick_size_dollars", "reference_price", "production_config_quantity",
                    "research_quantity", "allocated_capital", "unused_cash"):
            raw[key] = Decimal(raw[key]) if raw[key] is not None else None
        raw["reference_price_timestamp"] = datetime.fromisoformat(raw["reference_price_timestamp"])
        return CandidateSpec(**raw)


def freeze_candidate(spec: CandidateSpec, training: ValidatedDataset, config: OptimizationConfig) -> FrozenCandidate:
    training.verify()
    if config.session_dates != training.sessions:
        raise ValueError("Candidate calibration requires the training partition only")
    if spec.reference_price_timestamp != training.observations[0].second_bucket or spec.reference_price != training.observations[0].close_price:
        raise ValueError("Candidate reference must come from the training partition")
    if spec.trader.symbol != "TSLA" or spec.strategy_parameters.strategy_type != "renko_tp_sl":
        raise ValueError("Only TSLA TP/SL candidates are supported")
    # Production itself converts an explicit percent+reference pair once to dollars.
    if spec.candidate_type == "BRICK_PERCENT_GRID":
        effective = effective_renko_brick_size({"strategy": {
            "brick_sizing_mode": "BRICK_PERCENT", "brick_pct": spec.brick_pct, "reference_price": spec.reference_price,
        }})
        if effective != spec.brick_size_dollars:
            raise ValueError("Effective brick disagrees with production conversion")
    spec_json = canonical_json(asdict(replace(spec, reference_price_policy="FROZEN_TRAINING_REFERENCE_FIXED_DOLLARS_V1")))
    assumptions = canonical_json({"initial_capital": str(config.initial_capital),
                                 "friction": asdict(config.execution_friction),
                                 "conservative_protections": config.conservative_protections,
                                 "execution_model": config.execution_model,
                                 "raw_execution": asdict(config.raw_execution),
                                 "warmup_policy": config.warmup_policy, "version": VERSION,
                                 "code_fingerprint": research_code_fingerprint(),
                                 "stress_friction": asdict(config.execution_friction.stress())
                                 if config.execution_friction.slippage_per_share > 0 else None})
    identity = fingerprint([spec_json, assumptions, training.identity, [day.isoformat() for day in training.sessions]])
    return FrozenCandidate(spec_json, assumptions, training.identity, training.sessions, identity)


def generate_challengers(config: OptimizationConfig, trader: TraderSelection, training: ValidatedDataset,
                         partition: ResearchPartition) -> tuple[FrozenCandidate, ...]:
    validate_calibration_partition(config.execution_friction, partition)
    if (getattr(training, "partition", "TRAIN") != "TRAIN" or
            training.sessions != partition.optimization or config.session_dates != partition.optimization):
        raise ValueError("Universe generation accepts training only")
    config = approved_universe_config(config)
    specs = build_candidate_specs(config=config, trader=trader, prepared=_prepared(training), observations=training.observations)
    frozen = tuple(freeze_candidate(spec, training, config) for spec in specs)
    if len(frozen) != 510 or len({item.identity for item in frozen}) != 510:
        raise ValueError("Approved universe must contain exactly 510 unique challengers")
    return frozen


def research_code_fingerprint():
    root = Path(__file__).resolve().parents[1]
    paths = set()
    for directory in ("optimization", "replay", "silver", "shared"):
        paths.update((root / directory).rglob("*.py"))
    paths.update(root / name for name in ("strategy_engine.py", "renko_builder.py", "exit_plan.py",
                                          "broker_protection.py", "trading_events.py"))
    return fingerprint({str(path.relative_to(root)): sha256(path.read_bytes()).hexdigest()
                        for path in sorted(paths)})


def evaluate_training(candidate: FrozenCandidate, training: ValidatedDataset, config: OptimizationConfig, *,
                      use_renko_cache=False, execution_cache=None, renko_stream=None, execution_diagnostics=None):
    training.verify()
    if candidate.training_identity != training.identity or candidate.training_sessions != training.sessions:
        raise ValueError("Candidate training dataset mismatch")
    assumptions = json.loads(candidate.assumptions_json)
    if getattr(training, "partition", "TRAIN") != "TRAIN":
        raise ValueError("Holdout cannot enter training evaluation")
    if assumptions["code_fingerprint"] != research_code_fingerprint():
        raise ValueError("Research code changed after candidate freezing")
    frozen_config = replace(config, session_dates=training.sessions,
                            initial_capital=Decimal(assumptions["initial_capital"]),
                            execution_friction=ExecutionFriction(**assumptions["friction"]),
                            conservative_protections=assumptions["conservative_protections"],
                            execution_model=assumptions["execution_model"],
                            raw_execution=RawExecutionAssumptions(**assumptions["raw_execution"]),
                            warmup_policy=assumptions["warmup_policy"])
    result = run_candidate(config=frozen_config, spec=candidate.spec(), prepared=_prepared(training),
                         observations=training.observations, use_cached_renko_stream=False, validated_dataset=training,
                         execution_cache=execution_cache, execution_diagnostics=execution_diagnostics,
                         renko_stream=renko_stream if renko_stream is not None else
                         training.renko_stream(frozen_config, candidate.spec()) if use_renko_cache else None)
    result.row["candidate_identity"] = candidate.identity
    if result.status == "COMPLETED_INVALID":
        evidence = result.row["invalidity_evidence"]
        evidence["candidate_identity"] = candidate.identity
        result.row["invalidity_evidence_hash"] = fingerprint(evidence)
    return result


def validated_invalid_result(row, candidate_identity, dataset_identity):
    """Fail closed on incomplete, mismatched, or inconsistent terminal evidence."""
    from replay.execution import InvalidProtectionGeometry
    try:
        evidence = row["invalidity_evidence"]
        if (row["candidate_status"] != "COMPLETED_INVALID"
                or row["candidate_identity"] != candidate_identity
                or row["source_combined_hash"] != dataset_identity
                or row["production_eligible"] is not False
                or row["result_quality"] != "EXECUTION_INVALID"
                or evidence["candidate_identity"] != candidate_identity
                or evidence["candidate_id"] != row["candidate_id"]
                or evidence["dataset_identity"] != dataset_identity
                or evidence["reason"] != "INVALID_PROTECTION_GEOMETRY"
                or fingerprint(evidence) != row["invalidity_evidence_hash"]):
            return False
        stamp = pd.Timestamp(evidence["timestamp"])
        if stamp.tz_convert("America/New_York").date().isoformat() != evidence["session"]:
            return False
        reference, fill, tp, sl = (Decimal(evidence[key]) for key in
                                  ("signal_reference_price", "modeled_entry_fill", "intended_tp", "intended_sl"))
        if not all(value.is_finite() and value > 0 for value in (reference, fill, tp, sl)):
            return False
        if evidence["direction"] == "long":
            codes = (["invalid_long_take_profit_price"] if tp <= fill else []) + (["invalid_long_stop_loss_price"] if sl >= fill else [])
        elif evidence["direction"] == "short":
            codes = (["invalid_short_take_profit_price"] if tp >= fill else []) + (["invalid_short_stop_loss_price"] if sl <= fill else [])
        else:
            return False
        friction = evidence["execution_friction"]
        if not isinstance(friction, dict) or set(friction) != set(ExecutionFriction.__dataclass_fields__):
            return False
        schedule = friction["regulatory_fees"]
        if schedule is not None and (not isinstance(schedule, dict) or set(schedule) != set(EquityRegulatoryFees.__dataclass_fields__)):
            return False
        ExecutionFriction(**friction)
        return bool(codes) and codes == evidence["issue_codes"] and evidence["violated_invariants"] == [
            InvalidProtectionGeometry.INVARIANTS[code] for code in codes]
    except (KeyError, TypeError, ValueError, ArithmeticError):
        return False


@dataclass(frozen=True)
class SelectionRules:
    minimum_trades: int = 40
    minimum_pf: Decimal = Decimal("1")
    strong_width: int = 5
    moderate_width: int = 3

    def __post_init__(self):
        if self.minimum_trades < 1 or self.minimum_pf < 1 or self.strong_width < self.moderate_width or self.moderate_width < 3:
            raise ValueError("Invalid fixed selection thresholds")


@dataclass(frozen=True)
class RegimePoint:
    brick: Decimal
    entry: str
    tp: int
    sl: int
    pnl: Decimal | None
    profit_factor: Decimal | None
    trades: int | None
    candidate_status: str = "COMPLETED"

    @property
    def family(self) -> tuple:
        return self.entry, self.tp, self.sl


def neighborhood(points: tuple[RegimePoint, ...], center: RegimePoint) -> tuple[RegimePoint, ...]:
    expected = {center.brick + STEP * i for i in range(-4, 5)}
    return tuple(sorted((point for point in points if point.family == center.family and point.brick in expected
                         and LOW <= point.brick <= HIGH), key=lambda item: item.brick))


def distinct_regimes(points: tuple[RegimePoint, ...], rules: SelectionRules = SelectionRules()) -> tuple[dict, ...]:
    keys = [(point.family, point.brick) for point in points]
    if len(set(keys)) != len(keys):
        raise ValueError("Duplicate regime rows")
    for point in points:
        if (point.entry not in {"E1", "E2"} or (point.tp, point.sl) not in EXIT_PAIRS
                or not point.brick.is_finite() or not LOW <= point.brick <= HIGH
                or point.brick % STEP or point.candidate_status not in {"COMPLETED", "COMPLETED_INVALID"}):
            raise ValueError("Invalid regime row")
        if point.candidate_status == "COMPLETED_INVALID":
            if (point.pnl, point.profit_factor, point.trades) != (None, None, None):
                raise ValueError("Invalid candidates cannot carry performance")
        elif (point.pnl is None or point.profit_factor is None or point.trades is None
              or not point.pnl.is_finite() or point.profit_factor.is_nan()):
            raise ValueError("Invalid regime performance")
    eligible = lambda point: point.candidate_status == "COMPLETED" and point.pnl > 0 and point.profit_factor > rules.minimum_pf and point.trades >= rules.minimum_trades
    regimes = []
    for family in sorted({point.family for point in points}):
        groups: list[list[RegimePoint]] = []
        for point in sorted((point for point in points if point.family == family and eligible(point)), key=lambda item: item.brick):
            if not groups or point.brick - groups[-1][-1].brick != STEP:
                groups.append([])
            groups[-1].append(point)
        for group in groups:
            midpoint = (group[0].brick + group[-1].brick) / 2
            center = min(group, key=lambda point: (abs(point.brick - midpoint), point.brick))
            neighbors = neighborhood(points, center)
            expected_count = sum(LOW <= center.brick + STEP * i <= HIGH for i in range(-4, 5))
            fraction = Decimal(sum(eligible(point) for point in neighbors)) / expected_count
            strength = "WEAK"
            if len(group) >= rules.strong_width and fraction >= Decimal("0.8"):
                strength = "STRONG PLATEAU"
            elif len(group) >= rules.moderate_width and fraction >= Decimal("0.6"):
                strength = "MODERATE PLATEAU"
            elif len(group) == 1:
                strength = "OPTIMIZATION SPIKE"
            regimes.append({"family": family, "lower": group[0].brick, "upper": group[-1].brick,
                            "center": center, "exact_maximum": max(group, key=lambda point: (point.pnl, -point.brick)),
                            "strength": strength, "neighbors": neighbors, "positive_neighbor_fraction": fraction,
                            "boundary_truncated": expected_count < 9})
    ranks = {"STRONG PLATEAU": 3, "MODERATE PLATEAU": 2, "WEAK": 1, "OPTIMIZATION SPIKE": 0}
    return tuple(sorted(regimes, key=lambda item: (-ranks[item["strength"]], -item["positive_neighbor_fraction"],
                                                  -(item["upper"] - item["lower"]), item["family"], item["lower"])))


@dataclass(frozen=True)
class FrozenFinalists:
    candidates: tuple[FrozenCandidate, ...]
    partition: ResearchPartition
    rules: SelectionRules
    selection_evidence_hash: str
    training_validity: tuple[tuple[str, str], ...] = ()


def freeze_finalists(candidates: tuple[FrozenCandidate, ...], partition: ResearchPartition,
                     rules: SelectionRules, selection_evidence: Any, *, training_results=None) -> FrozenFinalists:
    if not candidates or len(candidates) > 3 or len({item.identity for item in candidates}) != len(candidates):
        raise ValueError("Freeze one to three distinct finalists")
    if any(item.training_sessions != partition.optimization for item in candidates):
        raise ValueError("Finalists must be selected exclusively from training")
    if len({item.training_identity for item in candidates}) != 1 or len({item.assumptions_json for item in candidates}) != 1:
        raise ValueError("Finalists must share one training dataset and execution contract")
    for item in candidates:
        item.spec()
        validate_calibration_partition(ExecutionFriction(**json.loads(item.assumptions_json)["friction"]), partition)
        row = (training_results or {}).get(item.identity, {})
        if (row.get("candidate_status") != "COMPLETED" or "invalidity_evidence" in row or row.get("candidate_identity") != item.identity
                or row.get("source_combined_hash") != item.training_identity):
            raise ValueError("Finalist requires execution-valid completed training evidence")
    validity = tuple((item.identity, fingerprint(training_results[item.identity])) for item in candidates)
    return FrozenFinalists(candidates, partition, rules, fingerprint(selection_evidence), validity)


def select_finalists(training: ValidatedDataset, universe: tuple[FrozenCandidate, ...],
                     points: tuple[RegimePoint, ...], partition: ResearchPartition,
                     rules: SelectionRules = SelectionRules(), *, training_results=None) -> FrozenFinalists:
    training.verify()
    if training.sessions != partition.optimization or any(item.training_identity != training.identity for item in universe):
        raise ValueError("Selection requires the frozen training universe")
    by_key = {}
    for candidate in universe:
        spec = candidate.spec()
        key = (spec.brick_pct, spec.strategy_parameters.entry_confirmation,
               spec.strategy_parameters.take_profit_bricks, spec.strategy_parameters.stop_loss_bricks)
        if key in by_key:
            raise ValueError("Duplicate candidate specification")
        by_key[key] = candidate
    if any((point.brick, *point.family) not in by_key for point in points):
        raise ValueError("Score is outside the frozen universe")
    for point in points:
        candidate = by_key[(point.brick, *point.family)]
        row = (training_results or {}).get(candidate.identity, {})
        if (row.get("candidate_status") != point.candidate_status
                or row.get("candidate_identity") != candidate.identity
                or row.get("source_combined_hash") != candidate.training_identity):
            raise ValueError("Score requires matching training validity evidence")
        if point.candidate_status == "COMPLETED_INVALID" and not validated_invalid_result(
                row, candidate.identity, candidate.training_identity):
            raise ValueError("Invalid score requires verified invalidity evidence")
    regimes = distinct_regimes(points, rules)
    distinct = []
    for regime in regimes:
        if regime["strength"] not in {"STRONG PLATEAU", "MODERATE PLATEAU"}:
            continue
        if any(regime["family"] == previous["family"]
               and regime["lower"] <= previous["upper"] + Decimal("0.04")
               and regime["upper"] >= previous["lower"] - Decimal("0.04") for previous in distinct):
            continue
        distinct.append(regime)
    selected = tuple(by_key[(regime["center"].brick, *regime["center"].family)] for regime in distinct[:3])
    return freeze_finalists(selected, partition, rules, {
        "training": training.identity, "rules": asdict(rules), "scores": [asdict(point) for point in points],
    }, training_results=training_results)


def evaluate_holdout(finalists: FrozenFinalists, holdout: ValidatedDataset, config: OptimizationConfig, *, stress=False):
    if (tuple(identity for identity, digest in finalists.training_validity if len(digest) == 64)
            != tuple(candidate.identity for candidate in finalists.candidates)):
        raise ValueError("Holdout requires execution-valid training evidence")
    holdout.verify()
    if holdout.sessions != finalists.partition.holdout:
        raise ValueError("Holdout partition mismatch")
    results = []
    for candidate in finalists.candidates:
        assumptions = json.loads(candidate.assumptions_json)
        if assumptions["code_fingerprint"] != research_code_fingerprint():
            raise ValueError("Research code changed after candidate freezing")
        friction = ExecutionFriction(**assumptions["friction"])
        validate_calibration_partition(friction, finalists.partition)
        if stress:
            if friction.application != "PER_FILL_CHARGE_V1":
                raise ValueError("Finalist stress requires frozen empirical fixed friction")
            friction = friction.stress()
        frozen_config = replace(config, session_dates=holdout.sessions,
                                initial_capital=Decimal(assumptions["initial_capital"]),
                                execution_friction=friction,
                                conservative_protections=assumptions["conservative_protections"],
                                execution_model=assumptions["execution_model"],
                                raw_execution=RawExecutionAssumptions(**assumptions["raw_execution"]),
                                warmup_policy=assumptions["warmup_policy"])
        results.append(run_candidate(config=frozen_config, spec=candidate.spec(), prepared=_prepared(holdout),
                                     observations=holdout.observations, use_cached_renko_stream=False, validated_dataset=holdout,
                                     frozen_finalists=finalists))
    return tuple(results)


def concentration_metrics(closed_trades: tuple, session_pnl: dict[date, Decimal]) -> dict:
    known = [trade for trade in closed_trades if trade.pnl is not None]
    total = sum((trade.pnl for trade in known), Decimal("0"))
    if sum(session_pnl.values(), Decimal("0")) != total:
        raise ValueError("Session and trade P&L denominators must reconcile")
    complete = len(known) == len(closed_trades)
    ratio = lambda numerator: numerator / total * 100 if total > 0 and complete else None
    return {"closed_trade_pnl": total, "concentration_denominator": "closed-trade net P&L",
            "concentration_status": "UNDEFINED_UNKNOWN_BASIS" if not complete else "DEFINED" if total > 0 else "UNDEFINED_NONPOSITIVE_TOTAL",
            "best_session_pct": ratio(max(session_pnl.values(), default=Decimal("0"))),
            "top_three_trades_pct": ratio(sum(sorted((trade.pnl for trade in known), reverse=True)[:3], Decimal("0"))),
            "long_pct": ratio(sum((trade.pnl for trade in known if trade.side == "long"), Decimal("0"))),
            "short_pct": ratio(sum((trade.pnl for trade in known if trade.side == "short"), Decimal("0")))}


class ResearchTemporaryFiles:
    """Own only files created exclusively by this instance in a new research root."""

    def __init__(self, root: Path) -> None:
        root = root.absolute()
        if any(part in {"live", "production_current", "production_truth", "ticks", ".git"} for part in root.parts):
            raise ValueError("Production paths are forbidden")
        if root.parent.resolve() != root.parent:
            raise ValueError("Symlinked research roots are forbidden")
        root.mkdir(exist_ok=False)
        self.root = root
        self.owned: dict[Path, tuple[int, int, str]] = {}

    def create(self, name: str, data: bytes) -> Path:
        if Path(name).name != name or name in {"", ".", ".."}:
            raise ValueError("Only direct research filenames are allowed")
        path = self.root / name
        with path.open("xb") as handle:
            handle.write(data)
        stat = path.stat()
        self.owned[path] = (stat.st_dev, stat.st_ino, sha256(data).hexdigest())
        return path

    def cleanup(self, *, validated_successfully: bool) -> dict:
        if not validated_successfully:
            raise ValueError("Preserve evidence until validation succeeds")
        before = sum(path.stat().st_size for path in self.owned)
        for path, (device, inode, digest) in self.owned.items():
            stat = path.lstat()
            if path.is_symlink() or path.parent.resolve() != self.root or (stat.st_dev, stat.st_ino) != (device, inode):
                raise ValueError("Temporary file ownership changed")
            if sha256(path.read_bytes()).hexdigest() != digest:
                raise ValueError("Temporary file content changed")
        for path in self.owned:
            path.unlink()
        self.owned.clear()
        return {"disk_before_cleanup": before, "disk_after_cleanup": 0, "space_reclaimed": before,
                "scope": "owned temporary file bytes only"}
