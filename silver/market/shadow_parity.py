from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from trading_events import MarketTick

from .models import CanonicalObservation, RawSipTrade
from .one_second_sampler import CanonicalOneSecondSipSampler
from .observation_stream import CanonicalMarketObservationAdapter


@dataclass(frozen=True)
class ShadowParityResult:
    matches: bool
    production: tuple[tuple[str, str, float, str], ...]
    canonical: tuple[tuple[str, str, float, str], ...]
    differences: tuple[str, ...]


def _market_tick(raw_trade: RawSipTrade) -> MarketTick:
    return MarketTick(
        timestamp=pd.Timestamp(raw_trade.sip_timestamp),
        symbol=raw_trade.symbol,
        price=float(raw_trade.price),
        size=float(raw_trade.quantity),
        exchange=raw_trade.exchange,
        trade_id=raw_trade.trade_id,
        tape=raw_trade.tape,
        conditions=raw_trade.conditions,
    )


def _production_tuple(tick: MarketTick) -> tuple[str, str, float, str]:
    return (tick.symbol, pd.Timestamp(tick.timestamp).isoformat(), float(tick.price), tick.trade_id)


def _canonical_tuple(observation: CanonicalObservation) -> tuple[str, str, float, str]:
    return (
        observation.symbol,
        pd.Timestamp(observation.second_bucket).isoformat(),
        float(observation.close_price),
        observation.source_alpaca_trade_id,
    )


def compare_with_production_sampler(
    trades: list[RawSipTrade],
    *,
    run_id: str,
    trader_id: str,
    symbol: str,
) -> ShadowParityResult:
    production_adapter = CanonicalMarketObservationAdapter(run_id=run_id)
    sampler = CanonicalOneSecondSipSampler(run_id=run_id)
    production_rows: list[tuple[str, str, float, str]] = []
    canonical_rows: list[tuple[str, str, float, str]] = []

    for raw in trades:
        tick = _market_tick(raw)
        production_result = production_adapter.process_tick(tick, trader_id=trader_id)
        production_rows.extend(_production_tuple(row) for row in production_adapter.observation_ticks(production_result))
        result = sampler.process_trade(raw)
        canonical_rows.extend(_canonical_tuple(row) for row in result.observations)

    production = tuple(production_rows)
    canonical = tuple(canonical_rows)
    differences = tuple() if production == canonical else (f"production={production!r} canonical={canonical!r}",)
    return ShadowParityResult(matches=production == canonical, production=production, canonical=canonical, differences=differences)
