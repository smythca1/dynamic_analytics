from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from hashlib import sha256
import json
from typing import Any


ACTIVE_STOP_STATUSES = {
    "accepted",
    "new",
    "partially_filled",
    "pending_new",
    "pending_replace",
    "held",
}
TERMINAL_STOP_STATUSES = {"canceled", "cancelled", "expired", "filled", "rejected", "suspended", "stopped"}
PROTECTION_TYPE = "reversal_catastrophic_stop"


@dataclass(frozen=True)
class ReversalStopIssue:
    code: str
    message: str
    severity: str = "critical"
    payload: dict[str, Any] | None = None


@dataclass(frozen=True)
class ReversalStopVerification:
    symbol: str
    protected: bool
    state: str
    stop_order_id: str | None = None
    stop_client_order_id: str | None = None
    protected_quantity: str | None = None
    stop_price: str | None = None
    returned_limit_price: str | None = None
    issues: tuple[ReversalStopIssue, ...] = ()

    @property
    def critical_issues(self) -> tuple[ReversalStopIssue, ...]:
        return tuple(issue for issue in self.issues if issue.severity == "critical")


def build_reversal_catastrophic_stop_plan(
    *,
    trader_id: str,
    strategy_id: str,
    symbol: str,
    entry_side: str,
    quantity: float | int | Decimal | str,
    entry_reference_price: float | Decimal | str,
    brick_size: float | Decimal | str,
    catastrophic_stop_bricks: int,
    client_order_id: str,
    candidate_config_fingerprint: str | None,
    market_contract_fingerprint: str | None,
    time_in_force: str = "gtc",
) -> dict[str, Any]:
    symbol_text = str(symbol).upper().strip()
    intended_entry_side = _entry_side(entry_side)
    position_side = "long" if intended_entry_side == "buy" else "short"
    closing_side = "sell" if position_side == "long" else "buy"
    qty = Decimal(str(quantity))
    base = Decimal(str(entry_reference_price))
    brick = Decimal(str(brick_size))
    stop_bricks = int(catastrophic_stop_bricks)
    if qty <= 0:
        raise ValueError("Reversal catastrophic stop quantity must be positive.")
    if base <= 0 or brick <= 0:
        raise ValueError("Reversal catastrophic stop base price and brick size must be positive.")
    if stop_bricks <= 0:
        raise ValueError("catastrophic_stop_bricks must be positive.")
    if not candidate_config_fingerprint:
        raise ValueError("Reversal catastrophic stop requires candidate/config fingerprint.")
    if not market_contract_fingerprint:
        raise ValueError("Reversal catastrophic stop requires market-contract fingerprint.")

    distance = brick * Decimal(stop_bricks)
    raw_stop = base - distance if position_side == "long" else base + distance
    tick = _tick_size(raw_stop)
    rounded_stop = _round_away_from_entry(raw_stop, tick, position_side)
    lineage = _child_client_order_id(client_order_id, "cstop")
    lifecycle_id = _fingerprint(
        {
            "trader_id": trader_id,
            "strategy_id": strategy_id,
            "symbol": symbol_text,
            "client_order_id": client_order_id,
            "entry_reference_price": str(base),
            "brick_size": str(brick),
            "catastrophic_stop_bricks": stop_bricks,
            "protection_type": PROTECTION_TYPE,
        }
    )[:24]
    return {
        "trader_id": str(trader_id),
        "strategy_id": str(strategy_id),
        "symbol": symbol_text,
        "protection_type": PROTECTION_TYPE,
        "intended_entry_side": intended_entry_side,
        "position_side": position_side,
        "closing_side": closing_side,
        "quantity": str(qty),
        "entry_reference_type": "actual_broker_average_entry_price",
        "entry_reference_price": str(base),
        "brick_size": str(brick),
        "catastrophic_stop_bricks": stop_bricks,
        "stop_distance": str(distance),
        "stop_price": _format_price(rounded_stop, tick),
        "time_in_force": str(time_in_force or "gtc").lower().strip(),
        "client_order_id": str(client_order_id),
        "stop_client_order_id": lineage,
        "candidate_config_fingerprint": candidate_config_fingerprint,
        "market_contract_fingerprint": market_contract_fingerprint,
        "lifecycle_id": lifecycle_id,
        "price_rounding": {
            "tick_size": str(tick),
            "rounding": "ROUND_FLOOR" if position_side == "long" else "ROUND_CEILING",
            "policy": "round_away_from_entry",
        },
    }


def catastrophic_stop_order_payload(plan: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": str(plan["symbol"]).upper(),
        "side": str(plan["closing_side"]).lower(),
        "type": "stop",
        "time_in_force": "gtc",
        "qty": _format_qty(Decimal(str(plan["quantity"]))),
        "stop_price": str(plan["stop_price"]),
        "client_order_id": str(plan["stop_client_order_id"])[:48],
        "extended_hours": False,
    }


def verify_reversal_catastrophic_stop(
    *,
    symbol: str,
    broker_position: dict[str, Any] | None,
    open_orders: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    stop_plan: dict[str, Any] | None,
    expected_config_fingerprint: str | None = None,
    expected_market_contract_fingerprint: str | None = None,
) -> ReversalStopVerification:
    symbol_text = str(symbol).upper().strip()
    issues: list[ReversalStopIssue] = []
    plan = stop_plan if isinstance(stop_plan, dict) else {}
    position = _position_snapshot(broker_position, symbol_text)
    candidates = _active_stop_orders(symbol_text, open_orders)

    if position is None:
        if candidates:
            issues.append(_issue("orphan_catastrophic_stop", "Catastrophic stop exists while broker position is flat."))
        return ReversalStopVerification(symbol=symbol_text, protected=False, state="FLAT", issues=tuple(issues))

    if not plan:
        issues.append(_issue("missing_stop_plan", "Catastrophic stop plan is required."))
    else:
        _validate_plan_metadata(
            plan=plan,
            symbol=symbol_text,
            position=position,
            expected_config_fingerprint=expected_config_fingerprint,
            expected_market_contract_fingerprint=expected_market_contract_fingerprint,
            issues=issues,
        )

    if not candidates:
        issues.append(_issue("missing_catastrophic_stop", "Broker position has no active catastrophic stop."))
        return _verification(symbol_text, position, None, plan, issues)

    matching = [order for order in candidates if _order_matches_plan(order, plan)]
    if len(candidates) > 1:
        issues.append(
            _issue(
                "duplicate_or_conflicting_catastrophic_stop",
                "Multiple active closing stop orders overlap the same position.",
                {"order_ids": [_text(order.get("id")) for order in candidates]},
            )
        )
    order = matching[0] if matching else candidates[0]
    if not matching and plan:
        issues.append(_issue("catastrophic_stop_lineage_mismatch", "No active stop matches expected client-order-ID lineage."))

    expected_closing_side = "sell" if position["side"] == "long" else "buy"
    if str(order.get("side") or "").lower().strip() != expected_closing_side:
        issues.append(_issue("invalid_catastrophic_stop_side", "Catastrophic stop side does not close broker position."))
    status = _status(order)
    if status not in ACTIVE_STOP_STATUSES:
        issues.append(_issue("invalid_catastrophic_stop_status", "Catastrophic stop status is not active/open.", {"status": status}))
    order_type = str(order.get("type") or "").lower().strip()
    if order_type not in {"stop", "stop_limit"}:
        issues.append(_issue("invalid_catastrophic_stop_order_type", "Catastrophic protection must be stop or approved stop_limit."))
    limit_price = _decimal(order.get("limit_price"))
    stop_price = _decimal(order.get("stop_price"))
    if order_type == "stop_limit":
        if expected_closing_side != "buy":
            issues.append(_issue("unexpected_sell_stop_limit", "Sell catastrophic stop must not be stop_limit."))
        elif limit_price < stop_price:
            issues.append(
                _issue(
                    "invalid_short_buy_stop_limit_price",
                    "Short-position buy stop-limit limit price must be at or above stop price.",
                    {"limit_price": str(limit_price), "stop_price": str(stop_price)},
                )
            )
    if stop_price <= 0:
        issues.append(_issue("missing_catastrophic_stop_price", "Catastrophic stop requires positive stop_price."))
    expected_stop = _decimal(plan.get("stop_price"))
    if expected_stop > 0 and stop_price > 0 and stop_price != expected_stop:
        issues.append(_issue("catastrophic_stop_price_mismatch", "Broker stop price does not match the plan."))

    remaining = _remaining_qty(order)
    if remaining > position["qty"]:
        issues.append(_issue("catastrophic_stop_quantity_exceeds_position", "Stop remaining quantity exceeds broker position."))
    if remaining < position["qty"]:
        issues.append(_issue("unprotected_reversal_partial_quantity", "Broker position has quantity without full catastrophic-stop protection."))
    if remaining != position["qty"]:
        issues.append(
            _issue(
                "catastrophic_stop_quantity_mismatch",
                "Catastrophic stop remaining quantity must equal broker position quantity.",
                {"stop_remaining": str(remaining), "position_qty": str(position["qty"])},
            )
        )

    return _verification(symbol_text, position, order, plan, issues)


def _validate_plan_metadata(
    *,
    plan: dict[str, Any],
    symbol: str,
    position: dict[str, Any],
    expected_config_fingerprint: str | None,
    expected_market_contract_fingerprint: str | None,
    issues: list[ReversalStopIssue],
) -> None:
    if str(plan.get("protection_type") or "") != PROTECTION_TYPE:
        issues.append(_issue("invalid_reversal_protection_type", "Stop plan must be reversal_catastrophic_stop."))
    if str(plan.get("symbol") or "").upper().strip() != symbol:
        issues.append(_issue("stop_plan_symbol_mismatch", "Stop plan symbol does not match broker position."))
    if str(plan.get("position_side") or "").lower().strip() != position["side"]:
        issues.append(_issue("stop_plan_side_mismatch", "Stop plan side does not match broker position."))
    plan_qty = _decimal(plan.get("quantity"))
    if plan_qty != position["qty"]:
        issues.append(_issue("stop_plan_quantity_mismatch", "Stop plan quantity does not equal broker position."))
    config_fp = _text(plan.get("candidate_config_fingerprint"))
    market_fp = _text(plan.get("market_contract_fingerprint"))
    if not config_fp:
        issues.append(_issue("missing_config_fingerprint", "Stop plan configuration fingerprint is required."))
    if not market_fp:
        issues.append(_issue("missing_market_contract_fingerprint", "Stop plan market-contract fingerprint is required."))
    if expected_config_fingerprint and config_fp != expected_config_fingerprint:
        issues.append(_issue("config_fingerprint_mismatch", "Stop plan configuration fingerprint mismatch."))
    if expected_market_contract_fingerprint and market_fp != expected_market_contract_fingerprint:
        issues.append(_issue("market_contract_fingerprint_mismatch", "Stop plan market-contract fingerprint mismatch."))


def _verification(
    symbol: str,
    position: dict[str, Any],
    order: dict[str, Any] | None,
    plan: dict[str, Any],
    issues: list[ReversalStopIssue],
) -> ReversalStopVerification:
    protected = not any(issue.severity == "critical" for issue in issues)
    state = "PROTECTED" if protected else "PROTECTION_FAILED"
    return ReversalStopVerification(
        symbol=symbol,
        protected=protected,
        state=state,
        stop_order_id=_text((order or {}).get("id")),
        stop_client_order_id=_text((order or {}).get("client_order_id")),
        protected_quantity=str(position["qty"]) if protected else None,
        stop_price=_text((order or {}).get("stop_price")),
        returned_limit_price=_text((order or {}).get("limit_price")),
        issues=tuple(issues),
    )


def _active_stop_orders(symbol: str, orders: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> list[dict[str, Any]]:
    candidates = []
    for order in orders:
        if not isinstance(order, dict):
            continue
        if str(order.get("symbol") or "").upper().strip() != symbol:
            continue
        if str(order.get("order_class") or "").lower().strip() in {"bracket", "oco"}:
            continue
        if order.get("stop_price") is None:
            continue
        if _status(order) in TERMINAL_STOP_STATUSES:
            continue
        candidates.append(order)
    return candidates


def _order_matches_plan(order: dict[str, Any], plan: dict[str, Any]) -> bool:
    if not plan:
        return True
    expected = _text(plan.get("stop_client_order_id"))
    if not expected:
        return True
    return _text(order.get("client_order_id")) == expected


def _position_snapshot(position: dict[str, Any] | None, symbol: str) -> dict[str, Any] | None:
    if not isinstance(position, dict):
        return None
    if str(position.get("symbol") or symbol).upper().strip() != symbol:
        return None
    qty = abs(_decimal(position.get("qty") or position.get("qty_available")))
    if qty <= 0:
        return None
    side = str(position.get("side") or "").lower().strip()
    if side not in {"long", "short"}:
        side = "long" if _decimal(position.get("qty")) > 0 else "short"
    return {
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "avg_entry_price": _decimal(position.get("avg_entry_price") or position.get("current_price")),
    }


def _remaining_qty(order: dict[str, Any]) -> Decimal:
    qty = abs(_decimal(order.get("qty")))
    filled = abs(_decimal(order.get("filled_qty")))
    remaining = qty - filled
    return remaining if remaining > 0 else Decimal("0")


def _entry_side(value: str) -> str:
    text = str(value).lower().strip()
    if text not in {"buy", "sell"}:
        raise ValueError("entry_side must be buy or sell.")
    return text


def _tick_size(price: Decimal) -> Decimal:
    return Decimal("0.0001") if price.copy_abs() < Decimal("1") else Decimal("0.01")


def _round_away_from_entry(price: Decimal, tick: Decimal, position_side: str) -> Decimal:
    rounding = ROUND_FLOOR if position_side == "long" else ROUND_CEILING
    return (price / tick).to_integral_value(rounding=rounding) * tick


def _format_price(price: Decimal, tick: Decimal) -> str:
    decimals = max(0, -tick.as_tuple().exponent)
    return f"{price:.{decimals}f}"


def _format_qty(value: Decimal) -> str:
    return format(value, "f").rstrip("0").rstrip(".")


def _decimal(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    return Decimal(str(value))


def _status(order: dict[str, Any] | None) -> str:
    if not isinstance(order, dict):
        return ""
    return str(order.get("status") or "").lower().strip()


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _issue(code: str, message: str, payload: dict[str, Any] | None = None) -> ReversalStopIssue:
    return ReversalStopIssue(code=code, message=message, payload=payload)


def _fingerprint(payload: dict[str, Any]) -> str:
    return sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _child_client_order_id(parent: str, suffix: str) -> str:
    child_suffix = f"-{suffix.strip('-')}"
    parent_text = str(parent)
    if len(parent_text) + len(child_suffix) <= 48:
        return f"{parent_text}{child_suffix}"
    unique_tail = parent_text.rsplit("-", 1)[-1] if "-" in parent_text else parent_text[-12:]
    unique_tail = unique_tail[-12:] or parent_text[-12:]
    tail_segment = f"-{unique_tail}"
    prefix_length = 48 - len(tail_segment) - len(child_suffix)
    prefix = parent_text[: max(0, prefix_length)].rstrip("-_")
    return f"{prefix}{tail_segment}{child_suffix}"[:48]
