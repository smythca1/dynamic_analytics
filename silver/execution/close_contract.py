from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any, Literal

from trading_events import BrokerSide, OrderIntent


CloseResolutionStatus = Literal["resolved", "already_flat", "side_mismatch", "invalid_position"]
PositionSide = Literal["long", "short"]


@dataclass(frozen=True)
class AuthoritativePosition:
    symbol: str
    side: PositionSide | None
    qty: Decimal
    source: str
    raw: Any = None

    @property
    def is_flat(self) -> bool:
        return self.side is None or self.qty <= 0


@dataclass(frozen=True)
class CloseResolution:
    status: CloseResolutionStatus
    requested_intent: OrderIntent
    executable_intent: OrderIntent | None
    authoritative_position: AuthoritativePosition
    reason: str

    @property
    def is_executable(self) -> bool:
        return self.status == "resolved" and self.executable_intent is not None


def authoritative_position_from_accounting(position: Any, *, symbol: str) -> AuthoritativePosition:
    side = getattr(position, "side", None)
    qty = _decimal(getattr(position, "qty", 0))
    if side is None or qty <= 0:
        return AuthoritativePosition(symbol=symbol.upper(), side=None, qty=Decimal("0"), source="accounting", raw=position)
    if side not in {"long", "short"}:
        return AuthoritativePosition(symbol=symbol.upper(), side=None, qty=qty, source="accounting", raw=position)
    return AuthoritativePosition(symbol=symbol.upper(), side=side, qty=abs(qty), source="accounting", raw=position)


def authoritative_position_from_alpaca(position: dict[str, Any] | None, *, symbol: str) -> AuthoritativePosition:
    if position is None:
        return AuthoritativePosition(symbol=symbol.upper(), side=None, qty=Decimal("0"), source="alpaca", raw=None)
    side = str(position.get("side", "")).lower().strip()
    qty = _decimal(position.get("qty") or 0)
    if side not in {"long", "short"}:
        return AuthoritativePosition(symbol=symbol.upper(), side=None, qty=abs(qty), source="alpaca", raw=position)
    if abs(qty) <= 0:
        return AuthoritativePosition(symbol=symbol.upper(), side=None, qty=Decimal("0"), source="alpaca", raw=position)
    return AuthoritativePosition(symbol=symbol.upper(), side=side, qty=abs(qty), source="alpaca", raw=position)


def resolve_close_intent(intent: OrderIntent, position: AuthoritativePosition) -> CloseResolution:
    if not intent.close_position:
        raise ValueError("resolve_close_intent requires close_position=True.")

    semantic_intent = replace(intent, qty=None, target_qty=0, notional=None)
    if position.symbol.upper() != intent.symbol.upper():
        return CloseResolution(
            status="invalid_position",
            requested_intent=semantic_intent,
            executable_intent=None,
            authoritative_position=position,
            reason="authoritative_position_symbol_mismatch",
        )
    if position.is_flat:
        return CloseResolution(
            status="already_flat",
            requested_intent=semantic_intent,
            executable_intent=None,
            authoritative_position=position,
            reason="authoritative_position_already_flat",
        )
    if position.side not in {"long", "short"}:
        return CloseResolution(
            status="invalid_position",
            requested_intent=semantic_intent,
            executable_intent=None,
            authoritative_position=position,
            reason="authoritative_position_side_invalid",
        )

    required_side: BrokerSide = "sell" if position.side == "long" else "buy"
    if intent.side != required_side:
        return CloseResolution(
            status="side_mismatch",
            requested_intent=semantic_intent,
            executable_intent=None,
            authoritative_position=position,
            reason=f"close_side_{intent.side}_does_not_flatten_{position.side}",
        )

    executable = replace(
        semantic_intent,
        side=required_side,
        qty=float(position.qty),
        target_qty=0,
        notional=None,
    )
    return CloseResolution(
        status="resolved",
        requested_intent=semantic_intent,
        executable_intent=executable,
        authoritative_position=position,
        reason="resolved_from_authoritative_position",
    )


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")
