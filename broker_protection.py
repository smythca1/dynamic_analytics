from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any


ACTIVE_PARENT_STATUSES = {
    "accepted",
    "new",
    "partially_filled",
    "filled",
    "pending_new",
    "pending_replace",
    "held",
}
ACTIVE_LEG_STATUSES = {
    "accepted",
    "new",
    "partially_filled",
    "pending_new",
    "pending_replace",
    "held",
}
BAD_LEG_STATUSES = {"canceled", "cancelled", "expired", "rejected", "suspended", "stopped"}


@dataclass(frozen=True)
class ProtectionIssue:
    code: str
    message: str
    severity: str = "critical"
    payload: dict[str, Any] | None = None


@dataclass(frozen=True)
class ProtectionVerification:
    symbol: str
    protected: bool
    state: str
    parent_order_id: str | None = None
    take_profit_order_id: str | None = None
    stop_loss_order_id: str | None = None
    protected_quantity: str | None = None
    remaining_position_quantity: str | None = None
    issues: tuple[ProtectionIssue, ...] = ()

    @property
    def critical_issues(self) -> tuple[ProtectionIssue, ...]:
        return tuple(issue for issue in self.issues if issue.severity == "critical")


def verify_broker_native_protection(
    *,
    symbol: str,
    broker_position: dict[str, Any] | None,
    nested_orders: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    exit_plan: dict[str, Any] | None,
    protection_state: dict[str, Any] | None = None,
    expected_config_fingerprint: str | None = None,
    expected_market_contract_fingerprint: str | None = None,
) -> ProtectionVerification:
    symbol_text = str(symbol).upper().strip()
    issues: list[ProtectionIssue] = []
    plan = exit_plan if isinstance(exit_plan, dict) else {}
    position = _position_snapshot(broker_position, symbol_text)
    groups = _protection_groups(symbol_text, nested_orders)

    if position is None:
        if groups:
            issues.append(_issue("orphan_protection_without_position", "Broker protection exists while broker position is flat."))
        return ProtectionVerification(symbol=symbol_text, protected=False, state="FLAT", issues=tuple(issues))

    if not plan:
        issues.append(_issue("missing_exit_plan", "ExitPlan is required to validate broker-native protection."))
    else:
        _validate_exit_plan_metadata(
            plan=plan,
            symbol=symbol_text,
            position=position,
            expected_config_fingerprint=expected_config_fingerprint,
            expected_market_contract_fingerprint=expected_market_contract_fingerprint,
            issues=issues,
        )

    if not groups:
        issues.append(_issue("missing_protection_group", "Broker position has no bracket/OCO protection group."))
        return _verification(symbol_text, position, None, None, None, issues)

    state = protection_state if isinstance(protection_state, dict) else {}
    matching_groups = [_group for _group in groups if _group_matches_plan(_group, plan, state)]
    if len(groups) > 1:
        issues.append(
            _issue(
                "duplicate_protection_group",
                "Multiple broker-native protection groups overlap the same position.",
                {"order_ids": [str(item.get("id")) for item in groups]},
            )
        )
    group = matching_groups[0] if matching_groups else groups[0]
    if matching_groups == [] and plan:
        issues.append(_issue("protection_lineage_mismatch", "No broker protection group matches ExitPlan or broker order-ID lineage."))

    parent_id = _text(group.get("id"))
    order_class = str(group.get("order_class") or "").lower().strip()
    if order_class not in {"bracket", "oco"}:
        issues.append(_issue("invalid_order_class", "Protection group must be bracket or OCO.", {"order_class": order_class}))
    if str(group.get("status") or "").lower().strip() not in ACTIVE_PARENT_STATUSES:
        issues.append(_issue("invalid_parent_status", "Protection parent status is not active.", {"status": group.get("status")}))

    legs = [leg for leg in group.get("legs") or [] if isinstance(leg, dict)]
    tp_leg = _take_profit_leg(legs)
    if tp_leg is None and order_class == "oco" and group.get("limit_price") is not None:
        tp_leg = group
    sl_leg = _stop_loss_leg(legs)
    if tp_leg is None:
        issues.append(_issue("missing_take_profit_leg", "Missing take-profit child leg."))
    if sl_leg is None:
        issues.append(_issue("missing_stop_loss_leg", "Missing stop-loss child leg."))

    expected_closing_side = "sell" if position["side"] == "long" else "buy"
    if order_class == "bracket":
        expected_parent_side = "buy" if position["side"] == "long" else "sell"
        if str(group.get("side") or "").lower().strip() != expected_parent_side:
            issues.append(_issue("invalid_parent_side", "Bracket parent side does not match broker position side."))
    elif order_class == "oco" and str(group.get("side") or "").lower().strip() != expected_closing_side:
        issues.append(_issue("invalid_parent_side", "OCO parent side must close the broker position."))

    for leg_name, leg in (("take_profit", tp_leg), ("stop_loss", sl_leg)):
        if leg is None:
            continue
        _validate_leg(
            leg_name=leg_name,
            leg=leg,
            parent_id=parent_id,
            expected_closing_side=expected_closing_side,
            position_qty=position["qty"],
            issues=issues,
        )

    _validate_prices(position=position, plan=plan, tp_leg=tp_leg, sl_leg=sl_leg, issues=issues)

    if tp_leg is not None and sl_leg is not None:
        tp_remaining = _remaining_qty(tp_leg)
        sl_remaining = _remaining_qty(sl_leg)
        if tp_remaining != sl_remaining:
            issues.append(
                _issue(
                    "protective_quantity_mismatch",
                    "TP and stop remaining quantities do not match.",
                    {"take_profit_remaining": str(tp_remaining), "stop_loss_remaining": str(sl_remaining)},
                )
            )
        if tp_remaining > position["qty"] or sl_remaining > position["qty"]:
            issues.append(_issue("protective_quantity_exceeds_position", "Protective quantity exceeds broker position."))
        if tp_remaining < position["qty"] or sl_remaining < position["qty"]:
            issues.append(_issue("unprotected_partial_quantity", "Broker position has quantity without full TP/SL protection."))
        if _status(tp_leg) == "filled" and _status(sl_leg) == "filled":
            issues.append(_issue("both_protective_legs_filled", "Both protective legs filled; possible reversed position."))

    return _verification(symbol_text, position, group, tp_leg, sl_leg, issues)


def _validate_exit_plan_metadata(
    *,
    plan: dict[str, Any],
    symbol: str,
    position: dict[str, Any],
    expected_config_fingerprint: str | None,
    expected_market_contract_fingerprint: str | None,
    issues: list[ProtectionIssue],
) -> None:
    if str(plan.get("symbol") or "").upper().strip() != symbol:
        issues.append(_issue("exit_plan_symbol_mismatch", "ExitPlan symbol does not match broker position."))
    expected_entry = "buy" if position["side"] == "long" else "sell"
    if str(plan.get("intended_entry_side") or "").lower().strip() != expected_entry:
        issues.append(_issue("exit_plan_side_mismatch", "ExitPlan entry side does not match broker position."))
    plan_qty = _decimal(plan.get("quantity"))
    if plan_qty < position["qty"]:
        issues.append(_issue("exit_plan_quantity_mismatch", "ExitPlan quantity is smaller than broker position."))
    config_fp = _text(plan.get("candidate_config_fingerprint"))
    market_fp = _text(plan.get("market_contract_fingerprint"))
    if not config_fp:
        issues.append(_issue("missing_config_fingerprint", "ExitPlan configuration fingerprint is required."))
    if not market_fp:
        issues.append(_issue("missing_market_contract_fingerprint", "ExitPlan market-contract fingerprint is required."))
    if expected_config_fingerprint and config_fp != expected_config_fingerprint:
        issues.append(_issue("config_fingerprint_mismatch", "ExitPlan configuration fingerprint mismatch."))
    if expected_market_contract_fingerprint and market_fp != expected_market_contract_fingerprint:
        issues.append(_issue("market_contract_fingerprint_mismatch", "ExitPlan market-contract fingerprint mismatch."))


def _validate_leg(
    *,
    leg_name: str,
    leg: dict[str, Any],
    parent_id: str | None,
    expected_closing_side: str,
    position_qty: Decimal,
    issues: list[ProtectionIssue],
) -> None:
    if not _text(leg.get("id")):
        issues.append(_issue(f"missing_{leg_name}_id", f"{leg_name} leg missing broker order ID."))
    if str(leg.get("side") or "").lower().strip() != expected_closing_side:
        issues.append(_issue(f"invalid_{leg_name}_side", f"{leg_name} leg side must close the broker position."))
    status = _status(leg)
    if status in BAD_LEG_STATUSES or status not in ACTIVE_LEG_STATUSES | {"filled"}:
        issues.append(_issue(f"invalid_{leg_name}_status", f"{leg_name} leg status is not acceptable.", {"status": status}))
    if _remaining_qty(leg) > position_qty:
        issues.append(_issue(f"oversized_{leg_name}_quantity", f"{leg_name} leg quantity exceeds broker position."))
    leg_parent_id = _text(leg.get("parent_order_id") or leg.get("parent_id"))
    if leg_parent_id and parent_id and leg_parent_id != parent_id:
        issues.append(_issue(f"invalid_{leg_name}_parent", f"{leg_name} leg parent does not match bracket/OCO parent."))


def _validate_prices(
    *,
    position: dict[str, Any],
    plan: dict[str, Any],
    tp_leg: dict[str, Any] | None,
    sl_leg: dict[str, Any] | None,
    issues: list[ProtectionIssue],
) -> None:
    base = position["avg_entry_price"]
    tp = _decimal((tp_leg or {}).get("limit_price"))
    sl = _decimal((sl_leg or {}).get("stop_price"))
    if tp <= 0:
        issues.append(_issue("invalid_take_profit_price", "Take-profit leg requires a positive limit price."))
    if sl <= 0:
        issues.append(_issue("invalid_stop_loss_price", "Stop-loss leg requires a positive stop price."))
    if position["side"] == "long":
        if tp <= base:
            issues.append(_issue("invalid_long_take_profit_price", "Long take profit must be above entry basis."))
        if sl >= base:
            issues.append(_issue("invalid_long_stop_loss_price", "Long stop loss must be below entry basis."))
    else:
        if tp >= base:
            issues.append(_issue("invalid_short_take_profit_price", "Short take profit must be below entry basis."))
        if sl <= base:
            issues.append(_issue("invalid_short_stop_loss_price", "Short stop loss must be above entry basis."))
    expected_tp = _decimal(plan.get("take_profit_limit_price"))
    expected_sl = _decimal(plan.get("stop_loss_trigger_price"))
    if expected_tp > 0 and tp > 0 and tp != expected_tp:
        issues.append(_issue("take_profit_price_mismatch", "Broker TP price does not match ExitPlan."))
    if expected_sl > 0 and sl > 0 and sl != expected_sl:
        issues.append(_issue("stop_loss_price_mismatch", "Broker stop price does not match ExitPlan."))


def _verification(
    symbol: str,
    position: dict[str, Any],
    group: dict[str, Any] | None,
    tp_leg: dict[str, Any] | None,
    sl_leg: dict[str, Any] | None,
    issues: list[ProtectionIssue],
) -> ProtectionVerification:
    protected = not any(issue.severity == "critical" for issue in issues)
    return ProtectionVerification(
        symbol=symbol,
        protected=protected,
        state="PROTECTED" if protected else "PROTECTION_FAILED",
        parent_order_id=_text((group or {}).get("id")),
        take_profit_order_id=_text((tp_leg or {}).get("id")),
        stop_loss_order_id=_text((sl_leg or {}).get("id")),
        protected_quantity=str(position["qty"]) if protected else None,
        remaining_position_quantity=str(position["qty"]),
        issues=tuple(issues),
    )


def _protection_groups(symbol: str, orders: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> list[dict[str, Any]]:
    child_order_ids = {
        _text(leg.get("id"))
        for order in orders
        if isinstance(order, dict)
        for leg in (order.get("legs") or [])
        if isinstance(leg, dict)
    }
    child_order_ids.discard(None)

    groups_by_parent_id: dict[str, dict[str, Any]] = {}
    groups_without_parent_id: list[dict[str, Any]] = []
    for order in orders:
        if not isinstance(order, dict):
            continue
        if str(order.get("symbol") or "").upper().strip() != symbol:
            continue
        order_id = _text(order.get("id"))
        if order_id in child_order_ids and not order.get("legs"):
            continue
        order_class = str(order.get("order_class") or "").lower().strip()
        if order_class not in {"bracket", "oco"} and not order.get("legs"):
            continue
        if order_id:
            existing = groups_by_parent_id.get(order_id)
            if existing is None or (order.get("legs") and not existing.get("legs")):
                groups_by_parent_id[order_id] = order
        else:
            groups_without_parent_id.append(order)
    return [*groups_by_parent_id.values(), *groups_without_parent_id]


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


def _take_profit_leg(legs: list[dict[str, Any]]) -> dict[str, Any] | None:
    return next((leg for leg in legs if leg.get("limit_price") is not None), None)


def _stop_loss_leg(legs: list[dict[str, Any]]) -> dict[str, Any] | None:
    return next((leg for leg in legs if leg.get("stop_price") is not None), None)


def _group_matches_plan(group: dict[str, Any], plan: dict[str, Any], protection_state: dict[str, Any] | None = None) -> bool:
    state = protection_state if isinstance(protection_state, dict) else {}
    order_class = str(group.get("order_class") or "").lower().strip()
    parent_order_id = _text(state.get("parent_order_id"))
    if parent_order_id and _text(group.get("id")) == parent_order_id:
        return True
    expected_leg_order_ids = {
        _text(state.get("take_profit_order_id")),
        _text(state.get("stop_loss_order_id")),
    }
    expected_leg_order_ids.discard(None)
    if expected_leg_order_ids:
        leg_order_ids = {_text(item.get("id")) for item in group.get("legs") or [] if isinstance(item, dict)}
        leg_order_ids.discard(None)
        if expected_leg_order_ids <= leg_order_ids:
            return True
    client_order_id = _text(plan.get("client_order_id"))
    group_client_order_id = _text(group.get("client_order_id"))
    if (
        order_class == "oco"
        and client_order_id
        and group_client_order_id
        and _is_recovery_client_order_id(group_client_order_id, client_order_id)
    ):
        return True
    if parent_order_id or expected_leg_order_ids:
        return False
    if not plan:
        return True
    if not client_order_id:
        return True
    if _text(group.get("client_order_id")) == client_order_id:
        return True
    if order_class == "oco" and group_client_order_id and _is_recovery_client_order_id(group_client_order_id, client_order_id):
        return True
    leg_ids = {_text(item.get("client_order_id")) for item in group.get("legs") or [] if isinstance(item, dict)}
    return bool({_text(plan.get("take_profit_client_order_id")), _text(plan.get("stop_loss_client_order_id"))} & leg_ids)


def _is_recovery_client_order_id(candidate: str, parent_client_order_id: str) -> bool:
    full_prefix = f"{parent_client_order_id}-r"
    truncated_prefix = f"{parent_client_order_id[:31]}-r"
    return candidate.startswith(full_prefix) or candidate.startswith(truncated_prefix)


def _remaining_qty(order: dict[str, Any]) -> Decimal:
    qty = abs(_decimal(order.get("qty")))
    filled = abs(_decimal(order.get("filled_qty")))
    remaining = qty - filled
    return remaining if remaining > 0 else Decimal("0")


def _status(order: dict[str, Any]) -> str:
    return str(order.get("status") or "").lower().strip()


def _decimal(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    return Decimal(str(value))


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _issue(code: str, message: str, payload: dict[str, Any] | None = None) -> ProtectionIssue:
    return ProtectionIssue(code=code, message=message, payload=payload)
