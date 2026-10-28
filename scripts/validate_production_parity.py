"""Run canonical replay and compare it with retained production evidence."""
from __future__ import annotations

import argparse
import json
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from replay.engine import run_replay
from replay.models import ReplayConfig
from replay.parity import LogicParityReport, compare_logic_parity
from replay.start_state import load_replay_start_state
from optimization.source_preparation import load_prepared_observations
from silver.market.contract import market_stream_contract, renko_strategy_contract


@dataclass(frozen=True)
class ValidationResult:
    classification: str
    parity: LogicParityReport
    configuration_hash: str
    source_dataset_hash: str
    replay_fingerprint: str
    limitations: tuple[str, ...]


def load_records(path: str | Path, *, symbol: str | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    wanted = symbol.upper() if symbol else None
    with Path(path).open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            if wanted and str(row.get("symbol") or "").upper() != wanted:
                continue
            rows.append(row)
    return rows


def _validate_observations(path: str | Path, symbol: str) -> tuple[Any, ...]:
    observations = tuple(load_prepared_observations(path))
    if not observations:
        raise ValueError("canonical observation source is empty")
    if any(item.symbol != symbol.upper() for item in observations):
        raise ValueError("canonical observation source contains another symbol")
    if any(left.second_bucket > right.second_bucket for left, right in zip(observations, observations[1:])):
        raise ValueError("canonical observations are not in deterministic timestamp order")
    if len({item.observation_id for item in observations}) != len(observations):
        raise ValueError("canonical observations contain duplicate observation IDs")
    return observations


def _write_evidence_views(path: str | Path, directory: Path, symbol: str) -> dict[str, Path]:
    records = load_records(path, symbol=symbol)
    types = {
        "production_observation": "observations.jsonl",
        "observation": "observations.jsonl",
        "renko_brick": "renko_bricks.jsonl",
        "strategy_signal": "signals.jsonl",
        "position_direction": "position_transitions.jsonl",
    }
    grouped: dict[str, list[dict[str, Any]]] = {name: [] for name in set(types.values())}
    canonical_observations: list[dict[str, Any]] = []
    legacy_observations: list[dict[str, Any]] = []
    for record in records:
        evidence_type = str(record.get("evidence_type") or "")
        if evidence_type == "production_observation":
            canonical_observations.append(record)
            continue
        if evidence_type == "observation":
            legacy_observations.append(record)
            continue
        filename = types.get(evidence_type)
        if filename:
            grouped[filename].append(record)
    grouped["observations.jsonl"] = canonical_observations or legacy_observations
    paths: dict[str, Path] = {}
    for filename, rows in grouped.items():
        target = directory / filename
        target.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")
        paths[filename] = target
    return paths


def run_production_parity(
    *,
    source_path: str | Path,
    evidence_path: str | Path | None,
    start_state_path: str | Path | None,
    strategy_config: dict[str, Any],
    symbol: str,
    trader_id: str,
    run_id: str,
    output_directory: str | Path,
    expected_market_fingerprint: str | None = None,
    expected_strategy_fingerprint: str | None = None,
    initial_capital: str = "10000",
) -> ValidationResult:
    observations = _validate_observations(source_path, symbol)
    market_contract = market_stream_contract(
        symbol=symbol,
        session_timezone=str(strategy_config.get("timezone") or "America/New_York"),
    )
    strategy_contract = renko_strategy_contract(strategy_config)
    limitations: list[str] = []
    if expected_market_fingerprint and expected_market_fingerprint != market_contract.fingerprint:
        raise ValueError("market contract fingerprint does not match canonical implementation")
    if expected_strategy_fingerprint and expected_strategy_fingerprint != strategy_contract.fingerprint:
        raise ValueError("strategy contract fingerprint does not match canonical implementation")
    if not expected_market_fingerprint:
        limitations.append("market contract fingerprint was not supplied for external verification")
    if not expected_strategy_fingerprint:
        limitations.append("strategy contract fingerprint was not supplied for external verification")

    initial_renko_state = None
    initial_strategy_state = None
    initial_accounting_position = None
    start_state_completeness = "not_supplied"
    if start_state_path:
        start_state = load_replay_start_state(start_state_path)
        if start_state.symbol != symbol.upper():
            raise ValueError("starting checkpoint symbol does not match requested symbol")
        if start_state.trader_id and start_state.trader_id != trader_id:
            raise ValueError("starting checkpoint trader does not match requested trader")
        start_state_completeness = start_state.completeness
        initial_renko_state = start_state.renko_state
        initial_strategy_state = start_state.strategy_state
        initial_accounting_position = start_state.accounting_position
    else:
        limitations.append("starting checkpoint was not supplied")

    if start_state_completeness != "complete":
        limitations.append(f"starting checkpoint completeness is {start_state_completeness}")
    if evidence_path is None:
        limitations.append("production evidence was not supplied")

    config = ReplayConfig(
        run_id=run_id,
        trader_id=trader_id,
        symbol=symbol,
        start=observations[0].second_bucket,
        end=observations[-1].second_bucket,
        timezone="UTC",
        session_mode="configured",
        strategy_config=dict(strategy_config),
        initial_capital=initial_capital,
        brick_mode="BRICK_DOLLARS",
        brick_size_dollars=str(strategy_config["brick_size"]),
        initial_renko_state=initial_renko_state,
        initial_strategy_state=initial_strategy_state,
        initial_accounting_position=initial_accounting_position,
        start_state_path=str(start_state_path) if start_state_path else None,
        start_state_completeness=start_state_completeness,
        source_data_path=str(source_path),
        source_semantic_mode="prepared_canonical_observations",
        production_config={
            "market_contract_fingerprint": market_contract.fingerprint,
            "strategy_contract_fingerprint": strategy_contract.fingerprint,
        },
        output_directory=str(Path(output_directory) / "replay"),
        output_level="AUDIT",
    )
    replay = run_replay(config)
    with tempfile.TemporaryDirectory(prefix="renko-parity-views-") as temporary:
        views = _write_evidence_views(evidence_path, Path(temporary), symbol) if evidence_path else {}
        parity = compare_logic_parity(
            parity_run_id=run_id,
            replay=replay,
            output_directory=Path(output_directory) / "parity",
            production_observations_path=views.get("observations.jsonl") if evidence_path else None,
            production_renko_path=views.get("renko_bricks.jsonl") if evidence_path else None,
            production_signals_path=views.get("signals.jsonl") if evidence_path else None,
            production_position_transitions_path=views.get("position_transitions.jsonl") if evidence_path else None,
            comparison_start=config.start,
            comparison_end=config.end,
        )

    if limitations:
        classification = "INCOMPLETE"
    elif parity.classification == "LOGIC_PARITY_VALIDATED":
        classification = "PASS"
    else:
        classification = "FAIL"
    return ValidationResult(
        classification=classification,
        parity=parity,
        configuration_hash=replay.context.configuration_hash,
        source_dataset_hash=replay.context.source_dataset_hash,
        replay_fingerprint=replay.context.replay_fingerprint,
        limitations=tuple(limitations),
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, help="Prepared canonical observations JSONL")
    parser.add_argument("--evidence", help="Production evidence JSONL")
    parser.add_argument("--start-state", help="Recorded production checkpoint")
    parser.add_argument("--strategy-config", required=True, help="Verified Renko strategy configuration JSON")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--trader-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--expected-market-fingerprint")
    parser.add_argument("--expected-strategy-fingerprint")
    parser.add_argument("--initial-capital", default="10000")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    strategy_config = json.loads(Path(args.strategy_config).read_text(encoding="utf-8"))
    result = run_production_parity(
        source_path=args.source,
        evidence_path=args.evidence,
        start_state_path=args.start_state,
        strategy_config=strategy_config,
        symbol=args.symbol,
        trader_id=args.trader_id,
        run_id=args.run_id,
        output_directory=args.output,
        expected_market_fingerprint=args.expected_market_fingerprint,
        expected_strategy_fingerprint=args.expected_strategy_fingerprint,
        initial_capital=args.initial_capital,
    )
    print(json.dumps({
        "classification": result.classification,
        "configuration_hash": result.configuration_hash,
        "source_dataset_hash": result.source_dataset_hash,
        "replay_fingerprint": result.replay_fingerprint,
        "parity_classification": result.parity.classification,
        "limitations": result.limitations,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
