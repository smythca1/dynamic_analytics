from __future__ import annotations

from dataclasses import dataclass, asdict
from decimal import Decimal
from typing import Any, Literal


ALPACA_STOP_ELECTION_SOURCE_VERSION = "alpaca_stop_election_model.v1.2026-08-23"
ALPACA_STOP_ELECTION_SOURCE_DISCLOSURE = {
    "orders_rule_source": "https://docs.alpaca.markets/us/docs/orders-at-alpaca",
    "orders_rule": "Stop orders and trailing stops are elected on the consolidated print; sell stops elect at or below stop and buy stops at or above stop, provided the electing trade is not outside NBBO.",
    "condition_source": "https://docs.alpaca.markets/us/docs/market-data-faq",
    "condition_rule": "Trades with excluded conditions do not update price; when multiple conditions exist, the strictest rule applies.",
    "model_version": ALPACA_STOP_ELECTION_SOURCE_VERSION,
}

EXCLUDED_STOP_ELECTION_CONDITIONS = {
    "B": "average_price_trade_cts",
    "C": "cash_sale",
    "G": "bunched_sold_trade",
    "H": "price_variation_trade",
    "I": "odd_lot_trade",
    "M": "market_center_official_close",
    "N": "next_day",
    "P": "prior_reference_price",
    "Q": "market_center_official_open",
    "R": "seller",
    "T": "form_t_or_extended_hours_trade",
    "U": "extended_trading_hours_sold_out_of_sequence",
    "V": "contingent_trade",
    "W": "average_price_trade_utdf",
    "Z": "sold_out_of_sequence",
    "4": "derivatively_priced",
    "7": "qualified_contingent_trade",
    "9": "corrected_consolidated_close",
}
CONSOLIDATED_TAPES = {"A", "B", "C"}


@dataclass(frozen=True)
class StopElectionDecision:
    symbol: str
    timestamp: str
    price: str
    exchange: str
    tape: str
    trade_id: str
    conditions: tuple[str, ...]
    stop_price: str
    position_side: Literal["long", "short"]
    consolidated_print_eligible: bool
    excluded_condition_reason: str | None
    nbbo: dict[str, str] | None
    inside_nbbo: bool | None
    final_election_decision: Literal["ELECTED", "EXCLUDED_CONDITION", "OUTSIDE_NBBO", "MISSING_NBBO", "NOT_CONSOLIDATED", "NO_PRICE_CROSS"]
    source_version: str = ALPACA_STOP_ELECTION_SOURCE_VERSION

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def evaluate_stop_election(
    *,
    symbol: str,
    timestamp: str,
    price: Decimal | str | float,
    exchange: str,
    tape: str,
    trade_id: str,
    conditions: tuple[str, ...] | list[str],
    stop_price: Decimal | str | float,
    position_side: Literal["long", "short"],
    nbbo: dict[str, Decimal | str | float] | None = None,
) -> StopElectionDecision:
    price_dec = Decimal(str(price))
    stop_dec = Decimal(str(stop_price))
    condition_tuple = tuple(str(condition) for condition in conditions if condition is not None)
    consolidated = str(tape).upper().strip() in CONSOLIDATED_TAPES
    crossed = price_dec <= stop_dec if position_side == "long" else price_dec >= stop_dec
    excluded = excluded_condition_reason(condition_tuple)
    inside = inside_nbbo(price_dec, nbbo) if nbbo is not None else None
    if not crossed:
        final = "NO_PRICE_CROSS"
    elif not consolidated:
        final = "NOT_CONSOLIDATED"
    elif excluded is not None:
        final = "EXCLUDED_CONDITION"
    elif nbbo is None:
        final = "MISSING_NBBO"
    elif not inside:
        final = "OUTSIDE_NBBO"
    else:
        final = "ELECTED"
    return StopElectionDecision(
        symbol=symbol.upper(),
        timestamp=str(timestamp),
        price=str(price_dec.normalize()),
        exchange=str(exchange),
        tape=str(tape).upper().strip(),
        trade_id=str(trade_id),
        conditions=condition_tuple,
        stop_price=str(stop_dec.normalize()),
        position_side=position_side,
        consolidated_print_eligible=consolidated,
        excluded_condition_reason=excluded,
        nbbo=_nbbo_dict(nbbo),
        inside_nbbo=inside,
        final_election_decision=final,  # type: ignore[arg-type]
    )


def excluded_condition_reason(conditions: tuple[str, ...] | list[str]) -> str | None:
    reasons = [
        f"{condition}:{EXCLUDED_STOP_ELECTION_CONDITIONS[condition]}"
        for condition in (str(item).upper().strip() for item in conditions)
        if condition in EXCLUDED_STOP_ELECTION_CONDITIONS
    ]
    return ",".join(reasons) if reasons else None


def inside_nbbo(price: Decimal, nbbo: dict[str, Decimal | str | float] | None) -> bool | None:
    if nbbo is None:
        return None
    bid = Decimal(str(nbbo.get("bid_price")))
    ask = Decimal(str(nbbo.get("ask_price")))
    return bid <= price <= ask


def _nbbo_dict(nbbo: dict[str, Decimal | str | float] | None) -> dict[str, str] | None:
    if nbbo is None:
        return None
    return {
        "bid_price": str(Decimal(str(nbbo["bid_price"])).normalize()),
        "ask_price": str(Decimal(str(nbbo["ask_price"])).normalize()),
        "timestamp": str(nbbo.get("timestamp") or ""),
    }

