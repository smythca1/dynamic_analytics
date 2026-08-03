from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

from .models import ReplayResult
from .output import _jsonable


@dataclass(frozen=True)
class ParityComparison:
    name: str
    production_event_count: int
    replay_event_count: int
    matched_count: int
    missing_production_count: int
    extra_replay_count: int
    exact_match_rate: float | None
    first_mismatch_timestamp: str | None = None
    first_mismatch_sequence: int | None = None
    mismatch_categories: dict[str, int] = field(default_factory=dict)
    maximum_consecutive_matching_sequence: int = 0
    evidence_status: str = "available"
    component_classification: str = "NOT_VERIFIABLE"
    economic_match_rate: float | None = None
    sequence_match_rate: float | None = None
    logical_timestamp_match_rate: float | None = None
    processing_timestamp_difference_count: int = 0


@dataclass(frozen=True)
class LogicParityReport:
    parity_run_id: str
    replay_run_id: str
    configuration_hash: str
    source_dataset_hash: str
    replay_fingerprint: str
    classification: str
    observations: ParityComparison
    renko_bricks: ParityComparison
    strategy_signals: ParityComparison
    position_direction_transitions: ParityComparison
    mismatch_files: dict[str, str] = field(default_factory=dict)
    limitations: tuple[str, ...] = ()


def compare_sampler_observation_parity(
    production_observations: list[dict[str, Any]],
    shadow_observations: list[dict[str, Any]],
) -> tuple[ParityComparison, list[dict[str, Any]]]:
    production = [_sampler_observation_key(row) for row in production_observations]
    shadow = [_sampler_observation_key(row) for row in shadow_observations]
    mismatches: list[dict[str, Any]] = []
    matched = 0
    max_streak = 0
    streak = 0
    categories: dict[str, int] = {}
    for index in range(max(len(production), len(shadow))):
        prod = production[index] if index < len(production) else None
        shd = shadow[index] if index < len(shadow) else None
        if prod is not None and shd is not None and _sampler_observations_equal(prod, shd):
            matched += 1
            streak += 1
            max_streak = max(max_streak, streak)
            continue
        streak = 0
        if prod is None:
            category = "shadow_only_observation"
        elif shd is None:
            category = "production_only_observation"
        elif prod.get("sequence") != shd.get("sequence"):
            category = "sequence_mismatch"
        elif prod.get("observation_bucket_timestamp") != shd.get("observation_bucket_timestamp"):
            category = "bucket_mismatch"
        elif not _decimal_close(prod.get("close_price"), shd.get("close_price")):
            category = "price_mismatch"
        else:
            category = "field_mismatch"
        categories[category] = categories.get(category, 0) + 1
        mismatches.append({"sequence": index + 1, "category": category, "production": prod, "shadow": shd})
    denominator = len(production)
    rate = float(matched / denominator * 100) if denominator else (100.0 if not shadow else 0.0)
    first = mismatches[0] if mismatches else None
    first_timestamp = None
    if first:
        row = first.get("production") or first.get("shadow") or {}
        first_timestamp = row.get("observation_bucket_timestamp")
    comparison = ParityComparison(
        name="production_vs_shadow_sampler",
        production_event_count=len(production),
        replay_event_count=len(shadow),
        matched_count=matched,
        missing_production_count=sum(1 for mismatch in mismatches if mismatch["category"] == "shadow_only_observation"),
        extra_replay_count=sum(1 for mismatch in mismatches if mismatch["category"] == "production_only_observation"),
        exact_match_rate=rate,
        first_mismatch_timestamp=first_timestamp,
        first_mismatch_sequence=first["sequence"] if first else None,
        mismatch_categories=categories,
        maximum_consecutive_matching_sequence=max_streak,
        component_classification=_component_classification(rate, len(production), len(shadow)),
    )
    return comparison, mismatches


def compare_logic_parity(
    *,
    parity_run_id: str,
    replay: ReplayResult,
    output_directory: str | Path,
    production_observations_path: str | Path | None = None,
    production_renko_path: str | Path | None = None,
    production_signals_path: str | Path | None = None,
    production_position_transitions_path: str | Path | None = None,
    comparison_start: Any | None = None,
    comparison_end: Any | None = None,
) -> LogicParityReport:
    output = Path(output_directory) / parity_run_id
    output.mkdir(parents=True, exist_ok=True)
    replay_observations = [_observation_key(row) for row in _window(replay.ledgers.canonical_observations, comparison_start, comparison_end)]
    replay_bricks = [_brick_key(row) for row in _window(replay.ledgers.renko_bricks, comparison_start, comparison_end)]
    replay_signals = [_signal_key(row) for row in _window(replay.ledgers.strategy_signals, comparison_start, comparison_end)]
    replay_positions = [_position_key(row) for row in _window(replay.ledgers.position_transitions, comparison_start, comparison_end)]

    comparisons = {
        "observations": _compare("observations", _window_keys(_load_observations(production_observations_path), comparison_start, comparison_end), replay_observations),
        "renko_bricks": _compare_bricks(
            _window_keys(_load_bricks(production_renko_path), comparison_start, comparison_end),
            replay_bricks,
        ),
        "strategy_signals": _compare("strategy_signals", _window_keys(_load_signals(production_signals_path), comparison_start, comparison_end), replay_signals),
        "position_direction_transitions": _compare(
            "position_direction_transitions",
            _window_keys(_load_positions(production_position_transitions_path), comparison_start, comparison_end),
            replay_positions,
        ),
    }
    mismatch_files = _write_mismatches(output, comparisons)
    limitations = tuple(
        f"{name}: {comparison[0].evidence_status}"
        for name, comparison in comparisons.items()
        if comparison[0].evidence_status != "available" or comparison[0].exact_match_rate != 100.0
    )
    classification = _classification(comparisons.values(), replay.manifest)
    report = LogicParityReport(
        parity_run_id=parity_run_id,
        replay_run_id=replay.context.run_id,
        configuration_hash=replay.context.configuration_hash,
        source_dataset_hash=replay.context.source_dataset_hash,
        replay_fingerprint=replay.context.replay_fingerprint,
        classification=classification,
        observations=comparisons["observations"][0],
        renko_bricks=comparisons["renko_bricks"][0],
        strategy_signals=comparisons["strategy_signals"][0],
        position_direction_transitions=comparisons["position_direction_transitions"][0],
        mismatch_files=mismatch_files,
        limitations=limitations,
    )
    _write_outputs(output, replay, report)
    return report


def _compare(name: str, production: list[dict[str, Any]] | None, replay: list[dict[str, Any]]) -> tuple[ParityComparison, list[dict[str, Any]]]:
    if production is None:
        return (
            ParityComparison(
                name=name,
                production_event_count=0,
                replay_event_count=len(replay),
                matched_count=0,
                missing_production_count=0,
                extra_replay_count=len(replay),
                exact_match_rate=None,
                evidence_status="production_evidence_missing",
                component_classification="NOT_VERIFIABLE",
            ),
            [],
        )
    mismatches: list[dict[str, Any]] = []
    matched = 0
    max_streak = 0
    streak = 0
    categories: dict[str, int] = {}
    for index in range(max(len(production), len(replay))):
        prod = production[index] if index < len(production) else None
        rep = replay[index] if index < len(replay) else None
        if prod == rep and prod is not None:
            matched += 1
            streak += 1
            max_streak = max(max_streak, streak)
            continue
        streak = 0
        category = "missing_production" if prod is None else "extra_replay" if rep is None else "field_mismatch"
        categories[category] = categories.get(category, 0) + 1
        mismatches.append({"sequence": index + 1, "category": category, "production": prod, "replay": rep})
    denominator = len(production)
    rate = float(matched / denominator * 100) if denominator else (100.0 if not replay else 0.0)
    first = mismatches[0] if mismatches else None
    first_timestamp = None
    if first:
        row = first.get("production") or first.get("replay") or {}
        first_timestamp = row.get("timestamp") or row.get("second_bucket")
    comparison = ParityComparison(
        name=name,
        production_event_count=len(production),
        replay_event_count=len(replay),
        matched_count=matched,
        missing_production_count=max(0, len(production) - len(replay)),
        extra_replay_count=max(0, len(replay) - len(production)),
        exact_match_rate=rate,
        first_mismatch_timestamp=first_timestamp,
        first_mismatch_sequence=first["sequence"] if first else None,
        mismatch_categories=categories,
        maximum_consecutive_matching_sequence=max_streak,
        component_classification=_component_classification(rate, len(production), len(replay)),
    )
    return comparison, mismatches


def _compare_bricks(production: list[dict[str, Any]] | None, replay: list[dict[str, Any]]) -> tuple[ParityComparison, list[dict[str, Any]]]:
    if production is None:
        return (
            ParityComparison(
                name="renko_bricks",
                production_event_count=0,
                replay_event_count=len(replay),
                matched_count=0,
                missing_production_count=0,
                extra_replay_count=len(replay),
                exact_match_rate=None,
                evidence_status="production_evidence_missing",
                component_classification="NOT_VERIFIABLE",
            ),
            [],
        )
    mismatches: list[dict[str, Any]] = []
    exact = 0
    economic = 0
    sequence = 0
    logical_ts = 0
    processing_ts_diff = 0
    categories: dict[str, int] = {}
    max_streak = 0
    streak = 0
    total = max(len(production), len(replay))
    for index in range(total):
        prod = production[index] if index < len(production) else None
        rep = replay[index] if index < len(replay) else None
        if prod is None or rep is None:
            category = "missing_production" if prod is None else "extra_replay"
            categories[category] = categories.get(category, 0) + 1
            mismatches.append({"sequence": index + 1, "category": category, "production": prod, "replay": rep})
            streak = 0
            continue
        economic_match = _brick_economic_equal(prod, rep)
        sequence_match = True
        logical_match = prod.get("brick_logical_timestamp") == rep.get("brick_logical_timestamp")
        processing_match = prod.get("production_processing_timestamp") == rep.get("production_processing_timestamp")
        if economic_match:
            economic += 1
        if sequence_match:
            sequence += 1
        if logical_match:
            logical_ts += 1
        if not processing_match and prod.get("production_processing_timestamp") and rep.get("production_processing_timestamp"):
            processing_ts_diff += 1
        if economic_match and sequence_match and logical_match:
            exact += 1
            streak += 1
            max_streak = max(max_streak, streak)
            if not processing_match and prod.get("production_processing_timestamp") and rep.get("production_processing_timestamp"):
                categories["PROCESSING_TIMESTAMP_DIFFERENCE"] = categories.get("PROCESSING_TIMESTAMP_DIFFERENCE", 0) + 1
            continue
        streak = 0
        if not economic_match:
            category = "economic_field_mismatch"
        elif not logical_match:
            category = "logical_timestamp_mismatch"
        else:
            category = "field_mismatch"
        categories[category] = categories.get(category, 0) + 1
        mismatches.append({"sequence": index + 1, "category": category, "production": prod, "replay": rep})
    denominator = len(production)
    exact_rate = float(exact / denominator * 100) if denominator else (100.0 if not replay else 0.0)
    economic_rate = float(economic / denominator * 100) if denominator else (100.0 if not replay else 0.0)
    sequence_rate = float(sequence / denominator * 100) if denominator else (100.0 if not replay else 0.0)
    logical_ts_rate = float(logical_ts / denominator * 100) if denominator else (100.0 if not replay else 0.0)
    first = mismatches[0] if mismatches else None
    first_timestamp = None
    if first:
        row = first.get("production") or first.get("replay") or {}
        first_timestamp = row.get("brick_logical_timestamp") or row.get("source_observation_bucket_timestamp")
    comparison = ParityComparison(
        name="renko_bricks",
        production_event_count=len(production),
        replay_event_count=len(replay),
        matched_count=exact,
        missing_production_count=max(0, len(production) - len(replay)),
        extra_replay_count=max(0, len(replay) - len(production)),
        exact_match_rate=exact_rate,
        first_mismatch_timestamp=first_timestamp,
        first_mismatch_sequence=first["sequence"] if first else None,
        mismatch_categories=categories,
        maximum_consecutive_matching_sequence=max_streak,
        component_classification=_renko_classification(
            production_count=len(production),
            replay_count=len(replay),
            economic_rate=economic_rate,
            sequence_rate=sequence_rate,
            logical_timestamp_rate=logical_ts_rate,
        ),
        economic_match_rate=economic_rate,
        sequence_match_rate=sequence_rate,
        logical_timestamp_match_rate=logical_ts_rate,
        processing_timestamp_difference_count=processing_ts_diff,
    )
    return comparison, mismatches


def _classification(comparisons: Iterable[tuple[ParityComparison, list[dict[str, Any]]]], manifest: dict[str, Any]) -> str:
    values = [item[0] for item in comparisons]
    if all(value.evidence_status == "available" and value.component_classification == "VALIDATED" for value in values):
        starting = manifest.get("starting_state", {})
        if starting.get("parity_verifiable"):
            return "LOGIC_PARITY_VALIDATED"
    if any(value.evidence_status == "available" and value.component_classification == "VALIDATED" for value in values):
        return "LOGIC_PARITY_PARTIAL"
    return "FULL_SESSION_RESEARCH"


def _component_classification(match_rate: float | None, production_count: int, replay_count: int) -> str:
    if match_rate is None or production_count == 0:
        return "NOT_VERIFIABLE"
    if match_rate == 100.0 and production_count == replay_count:
        return "VALIDATED"
    if match_rate > 0.0:
        return "PARTIAL"
    return "FAILED"


def _renko_classification(
    *,
    production_count: int,
    replay_count: int,
    economic_rate: float,
    sequence_rate: float,
    logical_timestamp_rate: float,
) -> str:
    if production_count == 0:
        return "NOT_VERIFIABLE"
    if economic_rate == 100.0 and sequence_rate == 100.0 and logical_timestamp_rate == 100.0 and production_count == replay_count:
        return "VALIDATED"
    if economic_rate == 100.0 and sequence_rate == 100.0 and logical_timestamp_rate == 0.0:
        return "FAILED"
    if economic_rate == 100.0 and sequence_rate == 100.0:
        return "ECONOMIC_SEQUENCE_MATCH_TIMESTAMP_UNVERIFIED"
    if economic_rate > 0.0 or sequence_rate > 0.0 or logical_timestamp_rate > 0.0:
        return "PARTIAL"
    return "FAILED"


def _write_mismatches(output: Path, comparisons: dict[str, tuple[ParityComparison, list[dict[str, Any]]]]) -> dict[str, str]:
    files: dict[str, str] = {}
    for name, (_, mismatches) in comparisons.items():
        if not mismatches:
            continue
        path = output / f"{name}_mismatches.jsonl"
        with path.open("w", encoding="utf-8") as handle:
            for row in mismatches:
                handle.write(json.dumps(_jsonable(row), sort_keys=True) + "\n")
        files[name] = str(path)
    return files


def _write_outputs(output: Path, replay: ReplayResult, report: LogicParityReport) -> None:
    payload = _jsonable(asdict(report))
    (output / "manifest.json").write_text(
        json.dumps(
            {
                "parity_run_id": report.parity_run_id,
                "replay_run_id": report.replay_run_id,
                "configuration_hash": report.configuration_hash,
                "source_dataset_hash": report.source_dataset_hash,
                "replay_fingerprint": report.replay_fingerprint,
                "replay_manifest_reference": replay.output_directory,
                "classification": report.classification,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    (output / "parity_summary.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    mismatch_summary = {
        key: value
        for key, value in payload.items()
        if key in {"observations", "renko_bricks", "strategy_signals", "position_direction_transitions", "limitations"}
    }
    (output / "mismatch_summary.json").write_text(json.dumps(mismatch_summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (output / "report.txt").write_text(_report_text(report), encoding="utf-8")


def _report_text(report: LogicParityReport) -> str:
    lines = [
        "Logic Parity Report",
        f"Parity run ID: {report.parity_run_id}",
        f"Replay run ID: {report.replay_run_id}",
        f"Configuration hash: {report.configuration_hash}",
        f"Source dataset hash: {report.source_dataset_hash}",
        f"Replay fingerprint: {report.replay_fingerprint}",
        f"Logic parity: {report.classification}",
        "Execution parity: NOT VALIDATED",
        "Simulated-versus-live PnL parity: NOT VALIDATED",
        "Slippage: UNKNOWN",
        "Spread: UNKNOWN",
    ]
    for comparison in (report.observations, report.renko_bricks, report.strategy_signals, report.position_direction_transitions):
        lines.extend(
            [
                "",
                comparison.name,
                f"production_event_count: {comparison.production_event_count}",
                f"replay_event_count: {comparison.replay_event_count}",
                f"matched_count: {comparison.matched_count}",
                f"exact_match_rate: {comparison.exact_match_rate}",
                f"first_mismatch_timestamp: {comparison.first_mismatch_timestamp}",
                f"first_mismatch_sequence: {comparison.first_mismatch_sequence}",
                f"evidence_status: {comparison.evidence_status}",
                f"component_classification: {comparison.component_classification}",
                f"economic_match_rate: {comparison.economic_match_rate}",
                f"sequence_match_rate: {comparison.sequence_match_rate}",
                f"logical_timestamp_match_rate: {comparison.logical_timestamp_match_rate}",
                f"processing_timestamp_difference_count: {comparison.processing_timestamp_difference_count}",
            ]
        )
    return "\n".join(lines) + "\n"


def _load_jsonl(path: str | Path | None) -> list[dict[str, Any]] | None:
    if path is None:
        return None
    rows: list[dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def _load_observations(path: str | Path | None) -> list[dict[str, Any]] | None:
    rows = _load_jsonl(path)
    return None if rows is None else [_observation_key(row) for row in rows]


def _load_bricks(path: str | Path | None) -> list[dict[str, Any]] | None:
    rows = _load_jsonl(path)
    return None if rows is None else [_brick_key(row) for row in rows]


def _load_signals(path: str | Path | None) -> list[dict[str, Any]] | None:
    rows = _load_jsonl(path)
    return None if rows is None else [_signal_key(row) for row in rows]


def _load_positions(path: str | Path | None) -> list[dict[str, Any]] | None:
    rows = _load_jsonl(path)
    return None if rows is None else [_position_key(row) for row in rows]


def _window(rows: Iterable[Any], start: Any | None, end: Any | None) -> list[Any]:
    if start is None and end is None:
        return list(rows)
    start_s = start.isoformat() if hasattr(start, "isoformat") else str(start) if start is not None else None
    end_s = end.isoformat() if hasattr(end, "isoformat") else str(end) if end is not None else None
    filtered = []
    for row in rows:
        ts = _timestamp(row)
        if start_s is not None and ts < start_s:
            continue
        if end_s is not None and ts > end_s:
            continue
        filtered.append(row)
    return filtered


def _window_keys(rows: list[dict[str, Any]] | None, start: Any | None, end: Any | None) -> list[dict[str, Any]] | None:
    if rows is None:
        return None
    return _window(rows, start, end)


def _timestamp(row: Any) -> str:
    value = (
        getattr(row, "observation_bucket_timestamp", None)
        or getattr(row, "brick_logical_timestamp", None)
        or getattr(row, "logical_signal_timestamp", None)
        or getattr(row, "logical_transition_timestamp", None)
        or getattr(row, "timestamp", None)
        or getattr(row, "second_bucket", None)
    )
    if value is None and isinstance(row, dict):
        value = (
            row.get("observation_bucket_timestamp")
            or row.get("brick_logical_timestamp")
            or row.get("logical_signal_timestamp")
            or row.get("logical_transition_timestamp")
            or row.get("timestamp")
            or row.get("second_bucket")
        )
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _observation_key(row: Any) -> dict[str, Any]:
    payload = _payload(row)
    return {
        "symbol": _get(row, "symbol"),
        "observation_bucket_timestamp": _iso(
            _get(payload, "observation_bucket_timestamp") or _get(row, "second_bucket") or _get(row, "timestamp")
        ),
        "close_price": _decimal_string(
            _get(payload, "close_price") or _get(row, "close_price") or _get(row, "price") or _get(row, "close")
        ),
        "source_trade_timestamp": _iso(_get(payload, "source_trade_timestamp")),
        "triggering_trade_timestamp": _iso(_get(payload, "triggering_trade_timestamp")),
        "source_record_id": _get(row, "source_record_id") or _get(payload, "source_trade_identity"),
    }


def _brick_key(row: Any) -> dict[str, Any]:
    payload = _payload(row)
    return {
        "symbol": _get(row, "symbol"),
        "brick_logical_timestamp": _iso(_get(payload, "brick_logical_timestamp") or _get(row, "timestamp")),
        "source_observation_bucket_timestamp": _iso(
            _get(payload, "source_observation_bucket_timestamp") or _get(row, "source_observation_bucket_timestamp")
        ),
        "production_processing_timestamp": _iso(_get(row, "logged_at") or _get(payload, "recorded_at")),
        "open": _decimal_string(_get(payload, "open") if _get(payload, "open") is not None else _get(row, "open")),
        "close": _decimal_string(_get(payload, "close") if _get(payload, "close") is not None else _get(row, "close")),
        "direction": int(_get(payload, "direction") if _get(payload, "direction") is not None else _get(row, "direction")),
        "run_len": int(_get(payload, "run_length") if _get(payload, "run_length") is not None else _get(row, "run_len")),
        "brick_size": _decimal_string(
            _get(payload, "effective_brick_size") if _get(payload, "effective_brick_size") is not None else _get(row, "brick_size")
        ),
    }


def _signal_key(row: Any) -> dict[str, Any]:
    payload = _payload(row)
    return {
        "logical_signal_timestamp": _iso(_get(payload, "logical_signal_timestamp") or _get(row, "timestamp")),
        "source_brick_timestamp": _iso(_get(payload, "source_brick_timestamp")),
        "symbol": _get(row, "symbol"),
        "side": _get(payload, "side") or _get(row, "side"),
        "reason": _get(payload, "reason") or _get(row, "reason"),
        "strategy": _get(payload, "strategy") or _get(row, "strategy"),
        "trader_id": _get(row, "trader_id"),
        "signal_type": _get(payload, "signal_type") or _get(row, "signal_type"),
        "target_direction": _get(payload, "target_direction"),
    }


def _position_key(row: Any) -> dict[str, Any]:
    payload = _payload(row)
    before = _get(row, "before") or {}
    after = _get(row, "after") or {}
    return {
        "logical_transition_timestamp": _iso(_get(payload, "logical_transition_timestamp") or _get(row, "timestamp")),
        "symbol": _get(row, "symbol"),
        "before_direction": _get(payload, "before_direction") or _get(before, "side"),
        "after_direction": _get(payload, "after_direction") or _get(after, "side"),
        "triggering_signal_sequence": _get(payload, "triggering_signal_sequence"),
    }


def _sampler_observation_key(row: dict[str, Any]) -> dict[str, Any]:
    payload = _payload(row)
    return {
        "sequence": int(_get(payload, "sequence") if _get(payload, "sequence") is not None else _get(row, "sequence")),
        "symbol": _get(row, "symbol"),
        "observation_bucket_timestamp": _iso(_get(payload, "observation_bucket_timestamp") or _get(row, "observation_bucket_timestamp")),
        "close_price": _decimal_string(_get(payload, "close_price") if _get(payload, "close_price") is not None else _get(row, "close_price")),
    }


def _sampler_observations_equal(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return (
        left.get("sequence") == right.get("sequence")
        and left.get("symbol") == right.get("symbol")
        and left.get("observation_bucket_timestamp") == right.get("observation_bucket_timestamp")
        and _decimal_close(left.get("close_price"), right.get("close_price"))
    )


def _payload(row: Any) -> dict[str, Any]:
    payload = _get(row, "payload")
    return payload if isinstance(payload, dict) else {}


def _get(row: Any, key: str) -> Any:
    if isinstance(row, dict):
        return row.get(key)
    return getattr(row, key, None)


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value).replace("Z", "+00:00")


def _decimal_string(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _brick_economic_equal(production: dict[str, Any], replay: dict[str, Any]) -> bool:
    keys = ("symbol", "direction", "run_len")
    if any(production.get(key) != replay.get(key) for key in keys):
        return False
    for key in ("open", "close", "brick_size"):
        if not _decimal_close(production.get(key), replay.get(key)):
            return False
    return True


def _decimal_close(left: Any, right: Any, tolerance: Decimal = Decimal("0.000000001")) -> bool:
    try:
        return abs(Decimal(str(left)) - Decimal(str(right))) <= tolerance
    except (InvalidOperation, TypeError, ValueError):
        return left == right
