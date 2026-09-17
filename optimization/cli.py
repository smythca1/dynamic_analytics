from __future__ import annotations

import argparse
import json
from datetime import date
from decimal import Decimal
from pathlib import Path

from .coverage import validate_coverage, write_coverage_reports
from .historical_sip import HistoricalSipBackfillRequest, backfill_historical_sip, research_raw_path
from .mean_reversion_runner import MeanReversionResearchConfig, run_mean_reversion_research
from .models import OptimizationConfig
from .overlap import compare_overlap, write_overlap_reports
from .reversal_volume_research import ReversalVolumeResearchConfig, run_reversal_volume_research
from .runner import preflight, run_optimization
from .shadow_capture import ShadowCaptureRequest, load_shadow_capture_manifest, run_shadow_capture
from .source_inventory import StageRequest, stage_sources
from .source_preparation import prepare_source


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Canonical multi-asset Renko optimization.")
    sub = parser.add_subparsers(dest="command", required=True)

    stage = sub.add_parser("stage-data")
    stage.add_argument("--host", required=True)
    stage.add_argument("--symbols", required=True)
    stage.add_argument("--sessions", required=True)
    stage.add_argument("--output", default="artifacts/bronze/market")
    stage.add_argument("--remote-root", default="/root/trading-server/artifacts/live/ticks")
    stage.add_argument("--dry-run", action="store_true")

    backfill = sub.add_parser("backfill-historical-sip")
    backfill.add_argument("--symbols", required=True)
    backfill.add_argument("--sessions", required=True)
    backfill.add_argument("--output", default="artifacts/research/raw-data")
    backfill.add_argument("--feed", default="sip")
    backfill.add_argument("--request-sleep-seconds", type=float, default=0.0)
    backfill.add_argument("--max-pages-per-session", type=int)
    backfill.add_argument("--min-free-gb", type=float, default=20.0)
    backfill.add_argument("--max-disk-used-pct", type=float, default=90.0)
    backfill.add_argument("--env-file", default=".env")
    backfill.add_argument("--credential-profile")
    backfill.add_argument("--no-reuse-existing", action="store_true")

    coverage = sub.add_parser("coverage")
    coverage.add_argument("--symbols", required=True)
    coverage.add_argument("--sessions", required=True)
    coverage.add_argument("--source-root", default="artifacts/research/raw-data/raw/alpaca/sip")
    coverage.add_argument("--output", required=True)
    coverage.add_argument("--max-gap-seconds", type=int, default=900)
    coverage.add_argument("--boundary-tolerance-seconds", type=int, default=120)

    overlap = sub.add_parser("overlap")
    overlap.add_argument("--symbols", required=True)
    overlap.add_argument("--sessions", required=True)
    overlap.add_argument("--live-root", required=True)
    overlap.add_argument("--historical-root", default="artifacts/research/raw-data/raw/alpaca/sip")
    overlap.add_argument("--output", required=True)

    shadow_capture = sub.add_parser("shadow-capture")
    shadow_capture.add_argument("--symbols", required=True)
    shadow_capture.add_argument("--session", required=True)
    shadow_capture.add_argument("--output", default="artifacts/research/shadow-capture")
    shadow_capture.add_argument("--run-id")
    shadow_capture.add_argument("--feed", default="sip")
    shadow_capture.add_argument("--env-file", default=".env")
    shadow_capture.add_argument("--credential-profile")
    shadow_capture.add_argument("--stream-url")
    shadow_capture.add_argument("--min-free-gb", type=float, default=20.0)
    shadow_capture.add_argument("--max-disk-used-pct", type=float, default=90.0)
    shadow_capture.add_argument("--duration-seconds", type=float)
    shadow_capture.add_argument("--stop-after-session-end-seconds", type=float, default=60.0)

    shadow_status = sub.add_parser("shadow-capture-status")
    shadow_status.add_argument("--capture-dir", required=True)

    mean_reversion = sub.add_parser("run-mean-reversion-research")
    mean_reversion.add_argument("--config", required=True)
    mean_reversion.add_argument("--trader", required=True)
    mean_reversion.add_argument("--sessions", required=True)
    mean_reversion.add_argument("--source-root", required=True)
    mean_reversion.add_argument("--output", default="artifacts/research/mean_reversion")
    mean_reversion.add_argument("--prepared-output", default="artifacts/research/prepared_market")
    mean_reversion.add_argument("--research-id", required=True)
    mean_reversion.add_argument("--initial-capital", default="10000")
    mean_reversion.add_argument("--fixed-notional")
    mean_reversion.add_argument("--no-grid", action="store_true")
    mean_reversion.add_argument("--no-current", action="store_true")

    reversal_volume = sub.add_parser("run-reversal-volume-research")
    reversal_volume.add_argument("--config", required=True)
    reversal_volume.add_argument("--research-id", required=True)
    reversal_volume.add_argument("--sessions")
    reversal_volume.add_argument("--symbols", default="")
    reversal_volume.add_argument("--baseline-sessions", default="")
    reversal_volume.add_argument("--source-root", default="artifacts/research/raw-data/raw/alpaca/sip")
    reversal_volume.add_argument("--output", default="artifacts/research/reversal_volume_zscore")
    reversal_volume.add_argument("--prepared-output", default="artifacts/research/prepared_market")
    reversal_volume.add_argument("--initial-capital", default="10000")
    reversal_volume.add_argument("--position-sizing", default="PRODUCTION_CONFIG_QUANTITY_V1")
    reversal_volume.add_argument("--brick-start", default="0.10")
    reversal_volume.add_argument("--brick-end", default="2.00")
    reversal_volume.add_argument("--brick-step", default="0.02")
    reversal_volume.add_argument("--candidate-pcts")
    reversal_volume.add_argument("--volume-windows", default="5,15,30,60")
    reversal_volume.add_argument("--volume-thresholds", default="NO_FILTER,0.00,0.25,0.50,0.75,1.00,1.25,1.50,1.75,2.00,2.50,3.00")
    reversal_volume.add_argument("--min-volume-baseline-sessions", type=int, default=5)
    reversal_volume.add_argument("--minimum-closed-trades", type=int, default=20)
    reversal_volume.add_argument("--skip-coverage-validation", action="store_true")

    prepare = sub.add_parser("prepare")
    prepare.add_argument("--symbol", required=True)
    prepare.add_argument("--trader-id", required=True)
    prepare.add_argument("--sessions", required=True)
    prepare.add_argument("--source-root", default="artifacts/bronze/market")
    prepare.add_argument("--output", default="artifacts/prepared_market")
    prepare.add_argument("--run-id", required=True)

    run = sub.add_parser("run")
    run.add_argument("--config", required=True)
    run.add_argument("--sessions", required=True)
    run.add_argument("--traders", default="all-enabled-renko")
    run.add_argument("--source-root", default="artifacts/bronze/market")
    run.add_argument("--initial-capital", default="10000")
    run.add_argument("--execution", default="NEXT_CANONICAL_OBSERVATION_V1")
    run.add_argument("--brick-start", default="0.05")
    run.add_argument("--brick-end", default="3.00")
    run.add_argument("--brick-step", default="0.01")
    run.add_argument("--position-sizing", default="PRODUCTION_CONFIG_QUANTITY_V1")
    run.add_argument("--parameter-grid", default="BRICK_ONLY_V1")
    run.add_argument("--entry-confirmations")
    run.add_argument("--take-profit-bricks")
    run.add_argument("--stop-loss-bricks")
    run.add_argument("--no-current-production", action="store_true")
    run.add_argument("--processes", default="1")
    run.add_argument("--candidate-batch-size", type=int, default=1)
    run.add_argument("--max-memory-gb")
    run.add_argument("--nice-level", type=int, default=0)
    run.add_argument("--host-role", choices=("local-research", "research-server", "live-trading"), default="local-research")
    run.add_argument("--checkpoint-every-candidates", type=int, default=25)
    run.add_argument("--progress-every-seconds", type=int, default=30)
    run.add_argument("--resume", action="store_true")
    run.add_argument("--retry-failed", action="store_true")
    run.add_argument("--output", default="artifacts/optimizations")
    run.add_argument("--prepared-output", default="artifacts/prepared_market")
    run.add_argument("--output-level", default="SUMMARY")
    run.add_argument("--optimization-id", required=True)
    run.add_argument("--preflight-only", action="store_true")
    run.add_argument("--candidate-pcts")
    run.add_argument("--minimum-closed-trades", type=int, default=10)
    run.add_argument("--minimum-average-trade-pnl-per-share", default="0.10")
    run.add_argument("--include-disabled-research-traders", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "stage-data":
        result = stage_sources(
            StageRequest(
                host=args.host,
                symbols=tuple(item.strip().upper() for item in args.symbols.split(",") if item.strip()),
                sessions=_sessions(args.sessions),
                output_root=args.output,
                remote_root=args.remote_root,
                dry_run=args.dry_run,
            )
        )
        print(json.dumps({"copied": result.copied, "reused": result.reused, "skipped": result.skipped, "commands": result.commands}, sort_keys=True))
        return 0
    if args.command == "backfill-historical-sip":
        result = backfill_historical_sip(
            HistoricalSipBackfillRequest(
                symbols=_symbols(args.symbols),
                sessions=_sessions(args.sessions),
                output_root=args.output,
                feed=args.feed,
                request_sleep_seconds=args.request_sleep_seconds,
                max_pages_per_session=args.max_pages_per_session,
                min_free_gb=args.min_free_gb,
                max_disk_used_pct=args.max_disk_used_pct,
                env_file=args.env_file,
                credential_profile=args.credential_profile,
                reuse_existing=not args.no_reuse_existing,
            )
        )
        print(json.dumps({"files": [item.__dict__ for item in result.files], "disk_preflight": result.disk_preflight}, sort_keys=True, default=str))
        return 0
    if args.command == "coverage":
        reports = []
        source_root = Path(args.source_root)
        for symbol in _symbols(args.symbols):
            for session in _sessions(args.sessions):
                path = source_root / symbol / session.isoformat() / "sip_trades.jsonl"
                if not path.exists():
                    compressed = path.with_suffix(path.suffix + ".gz")
                    path = compressed if compressed.exists() else path
                reports.append(
                    validate_coverage(
                        symbol=symbol,
                        session_date=session,
                        source_path=path,
                        max_gap_seconds=args.max_gap_seconds,
                        boundary_tolerance_seconds=args.boundary_tolerance_seconds,
                    )
                )
        write_coverage_reports(args.output, reports)
        print(json.dumps({"output": args.output, "pass": all(item.status == "PASS" for item in reports), "reports": [item.__dict__ for item in reports]}, sort_keys=True, default=str))
        return 0
    if args.command == "overlap":
        reports = []
        for symbol in _symbols(args.symbols):
            for session in _sessions(args.sessions):
                reports.append(
                    compare_overlap(
                        symbol=symbol,
                        session_date=session,
                        live_path=Path(args.live_root) / symbol / session.isoformat() / "sip_trades.jsonl",
                        historical_path=research_raw_path(args.historical_root, symbol, session)
                        if "raw/alpaca/sip" not in str(args.historical_root)
                        else Path(args.historical_root) / symbol / session.isoformat() / "sip_trades.jsonl.gz",
                    )
                )
        write_overlap_reports(args.output, reports)
        print(json.dumps({"output": args.output, "pass": all(item.status == "PASS" for item in reports), "reports": [item.__dict__ for item in reports]}, sort_keys=True, default=str))
        return 0
    if args.command == "shadow-capture":
        result = run_shadow_capture(
            ShadowCaptureRequest(
                symbols=_symbols(args.symbols),
                session_date=date.fromisoformat(args.session),
                output_root=args.output,
                run_id=args.run_id,
                feed=args.feed,
                env_file=args.env_file,
                credential_profile=args.credential_profile,
                stream_url=args.stream_url,
                min_free_gb=args.min_free_gb,
                max_disk_used_pct=args.max_disk_used_pct,
                duration_seconds=args.duration_seconds,
                stop_after_session_end_seconds=args.stop_after_session_end_seconds,
            )
        )
        print(json.dumps(result.__dict__, sort_keys=True, default=str))
        return 0
    if args.command == "shadow-capture-status":
        manifest = load_shadow_capture_manifest(args.capture_dir)
        print(
            json.dumps(
                {
                    "capture_dir": args.capture_dir,
                    "status": manifest.get("status"),
                    "completeness": manifest.get("completeness"),
                    "requested_session": manifest.get("requested_session"),
                    "started_at": manifest.get("started_at"),
                    "completed_at": manifest.get("completed_at"),
                    "symbols": manifest.get("symbols"),
                    "raw_counts": manifest.get("raw_counts"),
                    "canonical_counts": manifest.get("canonical_counts"),
                    "market_stream_fingerprints": manifest.get("market_stream_fingerprints"),
                    "paths": manifest.get("paths"),
                },
                sort_keys=True,
                default=str,
            )
        )
        return 0
    if args.command == "run-mean-reversion-research":
        result = run_mean_reversion_research(
            MeanReversionResearchConfig(
                research_id=args.research_id,
                config_path=args.config,
                trader_id=args.trader,
                session_dates=_sessions(args.sessions),
                source_root=args.source_root,
                output_directory=args.output,
                prepared_output_root=args.prepared_output,
                initial_capital=args.initial_capital,
                fixed_notional=args.fixed_notional,
                include_grid=not args.no_grid,
                include_current=not args.no_current,
            )
        )
        print(
            json.dumps(
                {
                    "research_id": args.research_id,
                    "output_directory": result.output_directory,
                    "candidate_count": len(result.rows),
                    "best_raw_candidate": result.best_raw_candidate,
                    "incumbent": result.incumbent,
                },
                sort_keys=True,
                default=str,
            )
        )
        return 0
    if args.command == "run-reversal-volume-research":
        result = run_reversal_volume_research(
            ReversalVolumeResearchConfig(
                research_id=args.research_id,
                config_path=args.config,
                session_dates=_sessions(args.sessions) if args.sessions else tuple(),
                symbols=_symbols(args.symbols) if args.symbols else tuple(),
                baseline_session_dates=_sessions(args.baseline_sessions) if args.baseline_sessions else tuple(),
                source_root=args.source_root,
                output_directory=args.output,
                prepared_output_root=args.prepared_output,
                initial_capital=args.initial_capital,
                position_sizing_mode=args.position_sizing,
                brick_start_pct=args.brick_start,
                brick_end_pct=args.brick_end,
                brick_step_pct_points=args.brick_step,
                candidate_pcts=_candidate_pcts(args.candidate_pcts),
                volume_windows_seconds=_csv_ints(args.volume_windows) or tuple(),
                volume_thresholds=_volume_thresholds(args.volume_thresholds),
                minimum_volume_baseline_sessions=args.min_volume_baseline_sessions,
                minimum_closed_trades=args.minimum_closed_trades,
                validate_market_data_coverage=not args.skip_coverage_validation,
            )
        )
        print(
            json.dumps(
                {
                    "research_id": args.research_id,
                    "output_directory": result.output_directory,
                    "symbols": result.symbols,
                    "session_dates": [item.isoformat() for item in result.session_dates],
                    "candidate_count": len(result.rows),
                    "opportunity_count": result.opportunity_count,
                    "research_only": True,
                    "production_live_behavior_modified": False,
                },
                sort_keys=True,
                default=str,
            )
        )
        return 0
    if args.command == "prepare":
        result = prepare_source(
            symbol=args.symbol,
            trader_id=args.trader_id,
            session_dates=_sessions(args.sessions),
            source_root=args.source_root,
            output_root=args.output,
            run_id=args.run_id,
        )
        print(json.dumps({"preparation_id": result.preparation_id, "observation_count": result.observation_count, "reused": result.reused}, sort_keys=True))
        return 0
    if args.command == "run":
        config = OptimizationConfig(
            optimization_id=args.optimization_id,
            production_config_path=args.config,
            session_dates=_sessions(args.sessions),
            source_root=args.source_root,
            selected_trader_ids=_traders(args.traders),
            initial_capital=args.initial_capital,
            execution_model=args.execution,
            brick_start_pct=args.brick_start,
            brick_end_pct=args.brick_end,
            brick_step_pct_points=args.brick_step,
            position_sizing_mode=args.position_sizing,
            parameter_grid_mode=args.parameter_grid,
            selected_entry_confirmations=_csv_strings(args.entry_confirmations),
            selected_take_profit_bricks=_csv_ints(args.take_profit_bricks),
            selected_stop_loss_bricks=_csv_ints(args.stop_loss_bricks),
            include_current_production=not args.no_current_production,
            output_directory=args.output,
            prepared_output_root=args.prepared_output,
            process_count=_processes(args.processes),
            candidate_batch_size=args.candidate_batch_size,
            max_memory_gb=args.max_memory_gb,
            nice_level=args.nice_level,
            host_role=args.host_role,
            checkpoint_every_candidates=args.checkpoint_every_candidates,
            progress_every_seconds=args.progress_every_seconds,
            resume=args.resume,
            retry_failed=args.retry_failed,
            output_level=args.output_level,
            preflight_only=args.preflight_only,
            candidate_pcts=_candidate_pcts(args.candidate_pcts),
            minimum_closed_trades=args.minimum_closed_trades,
            minimum_average_trade_pnl_per_share=args.minimum_average_trade_pnl_per_share,
            include_disabled_research_traders=args.include_disabled_research_traders,
        )
        payload = preflight(config) if args.preflight_only else None
        result = run_optimization(config)
        print(
            json.dumps(
                {
                    "optimization_id": config.optimization_id,
                    "output_directory": result.output_directory,
                    "candidate_count": len(result.candidate_rows),
                    "preflight": payload or result.preflight,
                },
                sort_keys=True,
                default=str,
            )
        )
        return 0
    raise ValueError(f"Unsupported command: {args.command}")


def _sessions(value: str) -> tuple[date, ...]:
    return tuple(date.fromisoformat(item.strip()) for item in value.split(",") if item.strip())


def _symbols(value: str) -> tuple[str, ...]:
    return tuple(item.strip().upper() for item in value.split(",") if item.strip())


def _traders(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(",") if item.strip())


def _candidate_pcts(value: str | None):
    if not value:
        return None
    return tuple(Decimal(item.strip()) for item in value.split(",") if item.strip())


def _csv_strings(value: str | None):
    if not value:
        return None
    return tuple(item.strip().upper() for item in value.split(",") if item.strip())


def _csv_ints(value: str | None):
    if not value:
        return None
    return tuple(int(item.strip()) for item in value.split(",") if item.strip())


def _volume_thresholds(value: str | None):
    if not value:
        return (None,)
    thresholds = []
    for item in value.split(","):
        cleaned = item.strip()
        if not cleaned:
            continue
        if cleaned.upper() in {"NO_FILTER", "NONE"}:
            thresholds.append(None)
        else:
            thresholds.append(Decimal(cleaned))
    return tuple(thresholds)


def _processes(value: str) -> int | str:
    cleaned = str(value).strip().lower()
    if cleaned == "auto":
        return "auto"
    return int(cleaned)


if __name__ == "__main__":
    raise SystemExit(main())
