from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from decimal import Decimal, ROUND_HALF_UP
from hashlib import sha256
from typing import Any, Literal


EntryReferenceType = Literal["signal_price", "actual_fill_price", "renko_reference_price"]


@dataclass(frozen=True)
class PriceRounding:
    tick_size: str
    rounding: str


@dataclass(frozen=True)
class ExitPlan:
    trader_id: str
    strategy_id: str
    symbol: str
    intended_entry_side: Literal["buy", "sell"]
    closing_side: Literal["buy", "sell"]
    quantity: str
    entry_reference_type: EntryReferenceType
    entry_reference_price: str
    take_profit_bricks: int
    stop_loss_bricks: int
    brick_size: str
    take_profit_limit_price: str
    stop_loss_trigger_price: str
    time_in_force: str
    order_class: Literal["bracket", "oco"]
    candidate_config_fingerprint: str | None
    market_contract_fingerprint: str | None
    lifecycle_id: str
    client_order_id: str
    take_profit_client_order_id: str
    stop_loss_client_order_id: str
    price_rounding: PriceRounding

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def fingerprint(self) -> str:
        return stable_exit_plan_fingerprint(self.to_dict())


def build_renko_tp_sl_exit_plan(
    *,
    trader_id: str,
    strategy_id: str,
    symbol: str,
    entry_side: str,
    quantity: float | int | Decimal | str,
    entry_reference_type: EntryReferenceType,
    entry_reference_price: float | Decimal | str,
    brick_size: float | Decimal | str,
    take_profit_bricks: int,
    stop_loss_bricks: int,
    time_in_force: str,
    order_class: Literal["bracket", "oco"],
    client_order_id: str,
    candidate_config_fingerprint: str | None = None,
    market_contract_fingerprint: str | None = None,
) -> ExitPlan:
    symbol_text = str(symbol).upper().strip()
    entry_side_text = _entry_side(entry_side)
    closing_side = "sell" if entry_side_text == "buy" else "buy"
    qty = Decimal(str(quantity))
    base = Decimal(str(entry_reference_price))
    brick = Decimal(str(brick_size))
    tp_bricks = int(take_profit_bricks)
    sl_bricks = int(stop_loss_bricks)
    if entry_side_text == "buy":
        tp = base + (brick * Decimal(tp_bricks))
        sl = base - (brick * Decimal(sl_bricks))
    else:
        tp = base - (brick * Decimal(tp_bricks))
        sl = base + (brick * Decimal(sl_bricks))
    rounding = PriceRounding(tick_size=str(_tick_size(max(base, tp, sl))), rounding="ROUND_HALF_UP")
    rounded_tp = _round_price(tp, rounding.tick_size)
    rounded_sl = _round_price(sl, rounding.tick_size)
    lifecycle_id = stable_exit_plan_fingerprint(
        {
            "trader_id": trader_id,
            "strategy_id": strategy_id,
            "symbol": symbol_text,
            "client_order_id": client_order_id,
            "entry_reference_type": entry_reference_type,
            "entry_reference_price": str(base),
            "take_profit_bricks": tp_bricks,
            "stop_loss_bricks": sl_bricks,
            "brick_size": str(brick),
        }
    )[:24]
    plan = ExitPlan(
        trader_id=str(trader_id),
        strategy_id=str(strategy_id),
        symbol=symbol_text,
        intended_entry_side=entry_side_text,  # type: ignore[arg-type]
        closing_side=closing_side,  # type: ignore[arg-type]
        quantity=str(qty),
        entry_reference_type=entry_reference_type,
        entry_reference_price=str(base),
        take_profit_bricks=tp_bricks,
        stop_loss_bricks=sl_bricks,
        brick_size=str(brick),
        take_profit_limit_price=str(rounded_tp),
        stop_loss_trigger_price=str(rounded_sl),
        time_in_force=str(time_in_force or "day").lower().strip(),
        order_class=order_class,
        candidate_config_fingerprint=candidate_config_fingerprint,
        market_contract_fingerprint=market_contract_fingerprint,
        lifecycle_id=lifecycle_id,
        client_order_id=str(client_order_id),
        take_profit_client_order_id=_child_client_order_id(client_order_id, "tp"),
        stop_loss_client_order_id=_child_client_order_id(client_order_id, "sl"),
        price_rounding=rounding,
    )
    validate_exit_plan(plan)
    return plan


def validate_exit_plan(plan: ExitPlan) -> None:
    symbol = plan.symbol.upper().strip()
    if not symbol:
        raise ValueError("ExitPlan symbol is required.")
    qty = Decimal(plan.quantity)
    base = Decimal(plan.entry_reference_price)
    tp = Decimal(plan.take_profit_limit_price)
    sl = Decimal(plan.stop_loss_trigger_price)
    if qty <= 0:
        raise ValueError("ExitPlan quantity must be positive.")
    if base <= 0 or tp <= 0 or sl <= 0:
        raise ValueError("ExitPlan prices must be positive.")
    if plan.take_profit_bricks <= 0 or plan.stop_loss_bricks <= 0:
        raise ValueError("ExitPlan TP and SL bricks must be positive.")
    if plan.intended_entry_side == "buy":
        if not tp > base:
            raise ValueError("Long ExitPlan take profit must be above base price.")
        if not sl < base:
            raise ValueError("Long ExitPlan stop loss must be below base price.")
    elif plan.intended_entry_side == "sell":
        if not tp < base:
            raise ValueError("Short ExitPlan take profit must be below base price.")
        if not sl > base:
            raise ValueError("Short ExitPlan stop loss must be above base price.")
    else:
        raise ValueError("ExitPlan intended_entry_side must be buy or sell.")
    expected_close = "sell" if plan.intended_entry_side == "buy" else "buy"
    if plan.closing_side != expected_close:
        raise ValueError("ExitPlan closing side would not close the intended position.")
    tick = Decimal(plan.price_rounding.tick_size)
    for field_name, price in (("take_profit_limit_price", tp), ("stop_loss_trigger_price", sl)):
        if (price / tick) != (price / tick).to_integral_value():
            raise ValueError(f"ExitPlan {field_name} violates tick-size rules.")
    if abs(base - sl) < tick:
        raise ValueError("ExitPlan stop price is too close to the base price.")
    if not plan.client_order_id or not plan.take_profit_client_order_id or not plan.stop_loss_client_order_id:
        raise ValueError("ExitPlan client-order-ID lineage is required.")
    if not plan.candidate_config_fingerprint:
        raise ValueError("ExitPlan candidate/config fingerprint is required.")
    if not plan.market_contract_fingerprint:
        raise ValueError("ExitPlan market-contract fingerprint is required.")


def stable_exit_plan_fingerprint(payload: dict[str, Any]) -> str:
    return sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _entry_side(value: str) -> str:
    text = str(value).lower().strip()
    if text not in {"buy", "sell"}:
        raise ValueError("entry_side must be buy or sell.")
    return text


def _tick_size(price: Decimal) -> Decimal:
    return Decimal("0.0001") if price < Decimal("1") else Decimal("0.01")


def _round_price(price: Decimal, tick_size: str) -> Decimal:
    tick = Decimal(tick_size)
    return (price / tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * tick


def _child_client_order_id(parent: str, suffix: str) -> str:
    max_len = 48
    suffix_text = f"-{suffix}"
    parent_text = str(parent)
    return f"{parent_text[: max_len - len(suffix_text)]}{suffix_text}"[:max_len]
