from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

from replay.execution import NextObservationExecution
from replay.metrics import build_metrics
from shared.config.trader_config import build_strategy_config, find_trader
from silver.accounting import AccountingLedger
from silver.accounting.models import EquityPoint
from silver.market import CanonicalObservation, market_stream_contract
from strategy_engine import MeanReversionStrategyConfig, MeanReversionStrategyEngine
from trading_events import MarketTick

from .models import PreparedSource
from .source_preparation import load_prepared_observations, prepare_source


MEAN_REVERSION_RESEARCH_VERSION = "research.mean_reversion_optimizer.v1"


@dataclass(frozen=True)
class MeanReversionResearchConfig:
    research_id: str
    config_path: str
    trader_id: str
    session_dates: tuple[date, ...]
    source_root: str
    output_directory: str = "artifacts/research/mean_reversion"
    prepared_output_root: str = "artifacts/research/prepared_market"
    initial_capital: Decimal | int | float | str = Decimal("10000")
    fixed_notional: Decimal | int | float | str | None = None
    include_grid: bool = True
    include_current: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "initial_capital", Decimal(str(self.initial_capital)))
        if self.fixed_notional is not None:
            object.__setattr__(self, "fixed_notional", Decimal(str(self.fixed_notional)))

    @property
    def output_path(self) -> Path:
        root = Path(self.output_directory)
        return root if root.name == self.research_id else root / self.research_id


@dataclass(frozen=True)
class MeanReversionResearchResult:
    output_directory: str
    trader_id: str
    symbol: str
    prepared_source: PreparedSource
    rows: tuple[dict[str, Any], ...]
    best_raw_candidate: dict[str, Any] | None
    incumbent: dict[str, Any] | None


def run_mean_reversion_research(config: MeanReversionResearchConfig) -> MeanReversionResearchResult:
    data = json.loads(Path(config.config_path).read_text(encoding="utf-8"))
    trader = find_trader(data, config.trader_id)
    base_strategy = build_strategy_config(trader)
    if not isinstance(base_strategy, MeanReversionStrategyConfig):
        raise ValueError(f"{config.trader_id} is not a chop_mean_reversion trader.")
    prepared = prepare_source(
        symbol=base_strategy.symbol,
        trader_id=config.trader_id,
        session_dates=config.session_dates,
        source_root=config.source_root,
        output_root=config.prepared_output_root,
        run_id=f"{config.research_id}:{base_strategy.symbol}:prep",
    )
    observations = load_prepared_observations(prepared.observations_path)
    variants = _candidate_configs(config=config, base=base_strategy)
    rows = [
        _run_candidate(config=config, prepared=prepared, observations=observations, strategy_config=strategy_config, candidate_type=candidate_type)
        for candidate_type, strategy_config in variants
    ]
    rows = sorted(rows, key=lambda row: Decimal(str(row.get("net_pnl_dollars") or 0)), reverse=True)
    incumbent = next((row for row in rows if row.get("candidate_type") == "CURRENT_PRODUCTION"), None)
    grid_rows = [row for row in rows if row.get("candidate_type") == "APPROVED_QQQ_CHOP_GRID_V1"]
    best = grid_rows[0] if grid_rows else None
    _write_outputs(config=config, prepared=prepared, rows=rows, incumbent=incumbent, best=best)
    return MeanReversionResearchResult(
        output_directory=str(config.output_path),
        trader_id=config.trader_id,
        symbol=base_strategy.symbol,
        prepared_source=prepared,
        rows=tuple(rows),
        best_raw_candidate=best,
        incumbent=incumbent,
    )


def _candidate_configs(
    *,
    config: MeanReversionResearchConfig,
    base: MeanReversionStrategyConfig,
) -> list[tuple[str, MeanReversionStrategyConfig]]:
    result: list[tuple[str, MeanReversionStrategyConfig]] = []
    if config.include_current:
        result.append(("CURRENT_PRODUCTION", base))
    if config.include_grid:
        from scripts.qqq_chop_mean_reversion_backtest import build_variants

        for variant in build_variants():
            if bool(getattr(variant, "lunch_block", False)):
                continue
            result.append(
                (
                    "APPROVED_QQQ_CHOP_GRID_V1",
                    MeanReversionStrategyConfig(
                        symbol=base.symbol,
                        window_seconds=int(variant.window_seconds),
                        entry_z=float(variant.entry_z),
                        exit_z=float(variant.exit_z),
                        stop_z=float(variant.stop_z),
                        efficiency_max=float(variant.efficiency_max),
                        trend_exit_efficiency=float(variant.trend_exit_efficiency),
                        max_hold_seconds=int(variant.max_hold_seconds),
                        trader_id=base.trader_id,
                        allow_short=base.allow_short,
                        strategy_name=f"{base.strategy_name}:{variant.name}",
                        account_namespace=base.account_namespace,
                        qty=base.qty,
                        notional=float(config.fixed_notional) if config.fixed_notional is not None else base.notional,
                        order_type=base.order_type,
                        timezone=base.timezone,
                        entry_start=str(variant.entry_start),
                        entry_end=str(variant.entry_end),
                        force_flat=base.force_flat,
                        active_start_date=base.active_start_date,
                        active_end_date=base.active_end_date,
                        allow_overnight=base.allow_overnight,
                    ),
                )
            )
    return result


def _run_candidate(
    *,
    config: MeanReversionResearchConfig,
    prepared: PreparedSource,
    observations: tuple[CanonicalObservation, ...],
    strategy_config: MeanReversionStrategyConfig,
    candidate_type: str,
) -> dict[str, Any]:
    if not observations:
        raise ValueError("No canonical observations for mean-reversion candidate.")
    strategy = MeanReversionStrategyEngine(strategy_config)
    accounting = AccountingLedger(symbol=strategy_config.symbol, starting_capital=config.initial_capital)
    execution = NextObservationExecution(run_id=f"{config.research_id}:{_candidate_id(strategy_config, candidate_type)}", trader_id=config.trader_id)
    fills = []
    orders = []
    signals = []
    equity_curve: list[EquityPoint] = []
    position_samples: list[dict[str, Any]] = []
    order_latencies: list[Decimal] = []
    for observation in observations:
        for bundle in execution.fill_pending(observation=observation, position=accounting.position):
            order_latencies.append(bundle.latency_seconds)
            for fill in bundle.fills:
                fills.append(fill)
                accounting.process_fill(fill)
        snapshot = accounting.snapshot(mark_price=observation.close_price)
        position_samples.append(
            {
                "timestamp": observation.second_bucket,
                "side": accounting.position.side or "flat",
                "qty": accounting.position.qty,
                "mark_price": observation.close_price,
                "equity": snapshot.equity,
            }
        )
        equity_curve.append(
            EquityPoint(
                timestamp=observation.second_bucket,
                equity=snapshot.equity,
                realized_pnl=snapshot.realized_pnl,
                unrealized_pnl=snapshot.unrealized_pnl,
                net_pnl=snapshot.net_pnl,
                fill_id=f"mark:{len(equity_curve)+1}",
            )
        )
        tick = MarketTick(
            timestamp=pd.Timestamp(observation.second_bucket),
            symbol=observation.symbol,
            price=float(observation.close_price),
            size=1.0,
            exchange=observation.source_exchange,
            trade_id=observation.source_alpaca_trade_id,
            tape=observation.source_tape,
            conditions=(),
        )
        _, actions = strategy.on_tick(tick)
        for signal, intent in actions:
            signals.append(signal)
            order = execution.queue(signal=signal, intent=intent, observation=observation, position=accounting.position)
            if order is not None:
                orders.append(order)
    final_snapshot = accounting.snapshot(mark_price=observations[-1].close_price)
    metrics = build_metrics(
        snapshot=final_snapshot,
        initial_capital=config.initial_capital,
        observations=list(observations),
        order_latencies=order_latencies,
        equity_curve=equity_curve,
        position_samples=position_samples,
        fills=fills,
    )
    market_contract = market_stream_contract(symbol=strategy_config.symbol, session_timezone=strategy_config.timezone)
    candidate_id = _candidate_id(strategy_config, candidate_type)
    return {
        "research_id": config.research_id,
        "candidate_id": candidate_id,
        "candidate_type": candidate_type,
        "trader_id": config.trader_id,
        "symbol": strategy_config.symbol,
        "strategy_type": strategy_config.strategy_type,
        "strategy_name": strategy_config.strategy_name,
        "window_seconds": strategy_config.window_seconds,
        "entry_z": strategy_config.entry_z,
        "exit_z": strategy_config.exit_z,
        "stop_z": strategy_config.stop_z,
        "efficiency_max": strategy_config.efficiency_max,
        "trend_exit_efficiency": strategy_config.trend_exit_efficiency,
        "max_hold_seconds": strategy_config.max_hold_seconds,
        "entry_start": strategy_config.entry_start,
        "entry_end": strategy_config.entry_end,
        "approved_grid_source": "scripts.qqq_chop_mean_reversion_backtest.build_variants"
        if candidate_type == "APPROVED_QQQ_CHOP_GRID_V1"
        else "checked_in_current_config",
        "lunch_block_variants_excluded": True,
        "source_preparation_id": prepared.preparation_id,
        "source_combined_hash": prepared.source_combined_hash,
        "market_stream_fingerprint": market_contract.fingerprint,
        "canonical_observations": len(observations),
        "strategy_signals": len(signals),
        "queued_orders": len(orders),
        "simulated_fills": len(fills),
        "closed_trades": metrics["closed_trades"],
        "net_pnl_dollars": metrics["net_pnl_dollars"],
        "realized_pnl_dollars": metrics["realized_pnl_dollars"],
        "unrealized_pnl_dollars": metrics["unrealized_pnl_dollars"],
        "strategy_return_pct": metrics["strategy_return_pct"],
        "profit_factor": metrics["profit_factor"],
        "win_rate_pct": metrics["win_rate_pct"],
        "max_drawdown_pct": metrics["max_drawdown_pct"],
        "buy_hold_return_pct": metrics["buy_hold_return_pct"],
        "result_quality": "FULL_SESSION_RESEARCH",
        "candidate_status": "COMPLETED",
    }


def _write_outputs(
    *,
    config: MeanReversionResearchConfig,
    prepared: PreparedSource,
    rows: list[dict[str, Any]],
    incumbent: dict[str, Any] | None,
    best: dict[str, Any] | None,
) -> None:
    out = config.output_path
    out.mkdir(parents=True, exist_ok=True)
    manifest = {
        "research_id": config.research_id,
        "version": MEAN_REVERSION_RESEARCH_VERSION,
        "config_path": config.config_path,
        "trader_id": config.trader_id,
        "session_dates": [item.isoformat() for item in config.session_dates],
        "prepared_source": asdict(prepared),
        "candidate_count": len(rows),
        "approved_grid_source": "scripts.qqq_chop_mean_reversion_backtest.build_variants",
        "lunch_block_variants_excluded": True,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    (out / "recommendations.json").write_text(
        json.dumps({"incumbent": incumbent, "best_raw_candidate": best, "recommendation": "RESEARCH_ONLY_NOT_PROMOTED"}, indent=2, sort_keys=True, default=str)
        + "\n",
        encoding="utf-8",
    )
    _write_csv(out / "candidates.csv", rows)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    keys = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def _candidate_id(config: MeanReversionStrategyConfig, candidate_type: str) -> str:
    payload = {
        "candidate_type": candidate_type,
        "config": asdict(config),
        "version": MEAN_REVERSION_RESEARCH_VERSION,
    }
    return "mr_" + sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:24]
