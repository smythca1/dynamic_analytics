from __future__ import annotations

import argparse
import json
from pathlib import Path

from strategy_engine import RenkoStrategyConfig

from .bronze import build_bronze_metadata, write_bronze_metadata
from .engine import run_replay
from .models import ReplayConfig
from .parity import compare_logic_parity
from .production_config import load_production_renko_config
from .start_state import load_replay_start_state


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run canonical historical replay V1.")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run")
    run.add_argument("--trader-id", required=True)
    run.add_argument("--source", required=True)
    run.add_argument("--start", required=True)
    run.add_argument("--end", required=True)
    run.add_argument("--symbol", required=True)
    run.add_argument("--initial-capital", required=True)
    run.add_argument("--execution", default="NEXT_CANONICAL_OBSERVATION_V1")
    run.add_argument("--output-level", default="SUMMARY", choices=["SUMMARY", "TRADES", "AUDIT"])
    run.add_argument("--output", default="artifacts/replays")
    run.add_argument("--run-id", required=True)
    run.add_argument("--brick-mode", default="BRICK_DOLLARS", choices=["BRICK_DOLLARS", "BRICK_PERCENT", "PRODUCTION_CONFIG"])
    run.add_argument("--brick-size", type=float)
    run.add_argument("--brick-pct", type=float)
    run.add_argument("--config", dest="config_path")
    run.add_argument("--entry-bricks", type=int, default=1)
    run.add_argument("--qty", type=float, default=1.0)
    run.add_argument("--strategy-type", default="renko_reversal")
    run.add_argument("--strategy-name", default="canonical_replay_e1_v1")
    run.add_argument("--renko-anchor-mode", default="grid")
    run.add_argument("--renko-anchor-price", type=float, default=0.0)
    run.add_argument("--entry-start", default="09:31:00")
    run.add_argument("--entry-end", default="15:59:59")
    run.add_argument("--allow-overnight", action="store_true")
    run.add_argument("--warmup-start")
    run.add_argument("--comparison-start")
    run.add_argument("--comparison-end")
    run.add_argument("--start-state")
    run.add_argument("--production-renko")
    run.add_argument("--production-observations")
    run.add_argument("--production-signals")
    run.add_argument("--production-position-transitions")
    run.add_argument("--parity-run-id")
    run.add_argument("--parity-output", default="artifacts/parity")
    run.add_argument("--write-bronze-metadata", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command != "run":
        raise ValueError(f"Unsupported command: {args.command}")
    if args.brick_mode == "BRICK_DOLLARS" and (args.brick_size is None or args.brick_pct is not None):
        raise ValueError("BRICK_DOLLARS requires --brick-size and rejects --brick-pct.")
    if args.brick_mode == "BRICK_PERCENT" and (args.brick_pct is None or args.brick_size is not None):
        raise ValueError("BRICK_PERCENT requires --brick-pct and rejects --brick-size.")
    if args.brick_mode == "PRODUCTION_CONFIG" and (args.brick_size is not None or args.brick_pct is not None):
        raise ValueError("PRODUCTION_CONFIG rejects --brick-size and --brick-pct.")
    production_metadata = {}
    source_config_path = None
    if args.brick_mode == "PRODUCTION_CONFIG":
        if not args.config_path:
            raise ValueError("PRODUCTION_CONFIG requires --config.")
        loaded = load_production_renko_config(args.config_path, args.trader_id)
        if loaded.strategy_config.symbol != args.symbol.upper():
            raise ValueError(f"CLI symbol {args.symbol} does not match production config symbol {loaded.strategy_config.symbol}.")
        strategy_config = loaded.strategy_config
        production_metadata = dict(loaded.metadata)
        production_metadata["production_config_hash"] = loaded.config_hash
        source_config_path = loaded.config_path
    else:
        strategy_brick_size = args.brick_size or 1.0
        strategy_config = RenkoStrategyConfig(
            trader_id=args.trader_id,
            symbol=args.symbol,
            brick_size=strategy_brick_size,
            entry_bricks=args.entry_bricks,
            strategy_type=args.strategy_type,
            strategy_name=args.strategy_name,
            qty=args.qty,
            renko_anchor_mode=args.renko_anchor_mode,
            renko_anchor_price=args.renko_anchor_price,
            entry_start=args.entry_start,
            entry_end=args.entry_end,
            allow_overnight=args.allow_overnight,
        )
    initial_renko_state = None
    initial_strategy_state = None
    initial_accounting_position = None
    start_state_path = None
    start_state_completeness = "not_supplied"
    production_evidence = {}
    if args.start_state:
        start_state = load_replay_start_state(args.start_state)
        start_state_path = args.start_state
        production_evidence["start_state_snapshot_timestamp"] = (
            start_state.snapshot_timestamp.isoformat() if start_state.snapshot_timestamp else None
        )
        production_evidence["start_state_source"] = args.start_state
        if start_state.snapshot_timestamp is not None and start_state.snapshot_timestamp <= ReplayConfig(
            run_id=args.run_id,
            trader_id=args.trader_id,
            symbol=args.symbol,
            start=args.start,
            end=args.end,
            timezone="UTC",
            session_mode="configured",
            strategy_config=strategy_config.__dict__,
            initial_capital=args.initial_capital,
            brick_mode=args.brick_mode,
            brick_size_dollars=args.brick_size,
            brick_size_ltp_percent=args.brick_pct,
            source_data_path=args.source,
        ).start:
            initial_renko_state = start_state.renko_state
            initial_strategy_state = start_state.strategy_state
            initial_accounting_position = start_state.accounting_position
            start_state_completeness = start_state.completeness
        else:
            start_state_completeness = "stale_for_replay_start"
    config = ReplayConfig(
        run_id=args.run_id,
        trader_id=args.trader_id,
        symbol=args.symbol,
        start=args.start,
        end=args.end,
        warmup_start=args.warmup_start,
        comparison_start=args.comparison_start,
        comparison_end=args.comparison_end,
        timezone="UTC",
        session_mode="configured",
        strategy_config=strategy_config.__dict__,
        brick_mode=args.brick_mode,
        brick_size_dollars=args.brick_size,
        brick_size_ltp_percent=args.brick_pct,
        initial_capital=args.initial_capital,
        initial_renko_state=initial_renko_state,
        initial_strategy_state=initial_strategy_state,
        initial_accounting_position=initial_accounting_position,
        start_state_path=start_state_path,
        start_state_completeness=start_state_completeness,
        execution_model=args.execution,
        source_data_path=args.source,
        source_semantic_mode="raw_sip_trades",
        source_config_path=source_config_path,
        production_config=production_metadata,
        production_evidence=production_evidence,
        output_directory=args.output,
        output_level=args.output_level,
    )
    result = run_replay(config)
    parity = None
    if args.write_bronze_metadata:
        metadata = build_bronze_metadata(config=config, source_path=args.source, date=config.start.date().isoformat())
        write_bronze_metadata(metadata, Path(args.source).with_suffix(".metadata.json"))
    if args.parity_run_id:
        parity = compare_logic_parity(
            parity_run_id=args.parity_run_id,
            replay=result,
            output_directory=args.parity_output,
            production_observations_path=args.production_observations,
            production_renko_path=args.production_renko,
            production_signals_path=args.production_signals,
            production_position_transitions_path=args.production_position_transitions,
            comparison_start=config.comparison_start,
            comparison_end=config.comparison_end,
        )
    print(
        json.dumps(
            {
                "run_id": result.context.run_id,
                "output_directory": result.output_directory,
                "configuration_hash": result.context.configuration_hash,
                "source_dataset_hash": result.context.source_dataset_hash,
                "replay_fingerprint": result.context.replay_fingerprint,
                "parity_classification": parity.classification if parity else None,
                "metrics": result.metrics,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
