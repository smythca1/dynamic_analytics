from __future__ import annotations

from trading_events import MarketTick, is_price_forming_trade

from .models import RawSipTrade, TradeEligibilityDecision


TRADE_FILTER_VERSION = "silver.market.trade_filter.price_forming_conditions.v1"


def evaluate_market_tick_eligibility(tick: MarketTick) -> bool:
    return is_price_forming_trade(tick)


def evaluate_trade_eligibility(raw_trade: RawSipTrade) -> TradeEligibilityDecision:
    tick = MarketTick(
        timestamp=raw_trade.sip_timestamp,
        symbol=raw_trade.symbol,
        price=float(raw_trade.price),
        size=float(raw_trade.quantity),
        exchange=raw_trade.exchange,
        trade_id=raw_trade.trade_id,
        tape=raw_trade.tape,
        conditions=raw_trade.conditions,
    )
    eligible = evaluate_market_tick_eligibility(tick)
    return TradeEligibilityDecision(
        raw_trade=raw_trade,
        is_eligible=eligible,
        reason="price_forming" if eligible else "non_price_forming_conditions",
    )
