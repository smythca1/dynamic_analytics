from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from hashlib import sha256

from silver.execution import authoritative_position_from_accounting, resolve_close_intent
from silver.accounting import NormalizedFill, PositionState
from silver.market import CanonicalObservation
from reversal_protection import build_reversal_catastrophic_stop_plan
from trading_events import MarketTick
from trading_events import OrderIntent, Signal


@dataclass(frozen=True)
class SimulatedOrder:
    order_id: str
    run_id: str
    sequence: int
    signal: Signal
    intent: OrderIntent
    source_observation_id: str
    queued_at: object
    symbol: str
    trader_id: str
    qty: Decimal


@dataclass(frozen=True)
class SimulatedFillBundle:
    order: SimulatedOrder
    fills: tuple[NormalizedFill, ...]
    latency_seconds: Decimal


@dataclass(frozen=True)
class SimulatedProtection:
    parent_order_id: str
    lifecycle_id: str
    symbol: str
    position_side: str
    qty: Decimal
    take_profit_price: Decimal | None
    stop_loss_price: Decimal
    take_profit_order_id: str | None
    stop_loss_order_id: str
    state: str
    protection_type: str = "bracket"


class NextObservationExecution:
    version = "NEXT_CANONICAL_OBSERVATION_V1"

    def __init__(self, *, run_id: str, trader_id: str, auto_fill_protections: bool = True) -> None:
        self.run_id = run_id
        self.trader_id = trader_id
        self.auto_fill_protections = auto_fill_protections
        self._order_sequence = 0
        self._fill_sequence = 0
        self._pending: list[SimulatedOrder] = []
        self._protections: list[SimulatedProtection] = []
        self.protection_events: list[dict] = []

    @property
    def pending_orders(self) -> tuple[SimulatedOrder, ...]:
        return tuple(self._pending)

    @property
    def active_protections(self) -> tuple[SimulatedProtection, ...]:
        return tuple(self._protections)

    def queue(
        self,
        *,
        signal: Signal,
        intent: OrderIntent,
        observation: CanonicalObservation,
        position: PositionState | None = None,
    ) -> SimulatedOrder | None:
        if intent.close_position:
            resolution = resolve_close_intent(
                intent,
                authoritative_position_from_accounting(position, symbol=intent.symbol),
            )
            if resolution.status == "already_flat":
                return None
            if not resolution.is_executable:
                raise ValueError(f"Cannot resolve close intent: {resolution.reason}")
            intent = resolution.executable_intent
            if intent is None:
                raise ValueError(f"Cannot resolve close intent: {resolution.reason}")

        self._order_sequence += 1
        qty = Decimal(str(intent.qty or intent.target_qty or 0))
        order_id = _stable_id(
            self.run_id,
            "order",
            self._order_sequence,
            observation.observation_id,
            intent.timestamp,
            intent.symbol,
            intent.side,
            intent.signal_type,
            intent.reason,
            qty,
        )
        order = SimulatedOrder(
            order_id=order_id,
            run_id=self.run_id,
            sequence=self._order_sequence,
            signal=signal,
            intent=intent,
            source_observation_id=observation.observation_id,
            queued_at=signal.timestamp,
            symbol=intent.symbol.upper(),
            trader_id=self.trader_id,
            qty=qty,
        )
        self._pending.append(order)
        if intent.exit_plan is not None or intent.protection_type == "reversal_catastrophic_stop":
            self.protection_events.append(
                {
                    "state": "BRACKET_SUBMITTED" if intent.exit_plan is not None else "CATASTROPHIC_STOP_SUBMITTED",
                    "order_id": order.order_id,
                    "lifecycle_id": intent.exit_plan.get("lifecycle_id") if intent.exit_plan is not None else None,
                    "exit_plan": intent.exit_plan,
                    "protection_type": intent.protection_type,
                }
            )
        return order

    def fill_pending(self, *, observation: CanonicalObservation, position: PositionState) -> tuple[SimulatedFillBundle, ...]:
        ready = [order for order in self._pending if order.source_observation_id != observation.observation_id]
        self._pending = [order for order in self._pending if order.source_observation_id == observation.observation_id]
        for order in ready:
            if _order_would_close_position(order, position):
                self._cancel_reversal_catastrophic_stop_for_normal_exit(order=order, fill=None)
        protection_bundles = (
            self._fill_active_protections(observation=observation, position=position)
            if self.auto_fill_protections
            else tuple()
        )
        bundles: list[SimulatedFillBundle] = []
        working_position = position
        for bundle in protection_bundles:
            for fill in bundle.fills:
                working_position = _apply_position_shadow(working_position, fill)
            bundles.append(bundle)
        for order in ready:
            fills = self._build_fills(order, observation, working_position)
            for fill in fills:
                if fill.leg == "close":
                    self._cancel_reversal_catastrophic_stop_for_normal_exit(order=order, fill=fill)
                working_position = _apply_position_shadow(working_position, fill)
                if fill.leg == "open" and (order.intent.exit_plan is not None or order.intent.protection_type == "reversal_catastrophic_stop"):
                    self._activate_protection(order=order, fill=fill)
            latency = Decimal(str((observation.second_bucket - order.signal.timestamp.to_pydatetime()).total_seconds()))
            bundles.append(SimulatedFillBundle(order=order, fills=fills, latency_seconds=latency))
        return tuple(bundles)

    def _build_fills(
        self,
        order: SimulatedOrder,
        observation: CanonicalObservation,
        position: PositionState,
    ) -> tuple[NormalizedFill, ...]:
        side = str(order.intent.side).lower()
        total_qty = Decimal(str(order.intent.qty or order.intent.target_qty or 0))
        if total_qty <= 0:
            return tuple()
        fills: list[NormalizedFill] = []
        if order.intent.close_position:
            if position.side == "long" and side == "sell":
                return (self._fill(order, observation, side="sell", qty=min(position.qty, total_qty), leg="close"),)
            if position.side == "short" and side == "buy":
                return (self._fill(order, observation, side="buy", qty=min(position.qty, total_qty), leg="close"),)
            return tuple()
        remaining = total_qty
        if position.side == "long" and side == "sell":
            close_qty = min(position.qty, remaining)
            if close_qty > 0:
                fills.append(self._fill(order, observation, side="sell", qty=close_qty, leg="close"))
                remaining -= close_qty
        elif position.side == "short" and side == "buy":
            close_qty = min(position.qty, remaining)
            if close_qty > 0:
                fills.append(self._fill(order, observation, side="buy", qty=close_qty, leg="close"))
                remaining -= close_qty
        if remaining > 0:
            open_side = "sell_short" if side == "sell" else "buy"
            fills.append(self._fill(order, observation, side=open_side, qty=remaining, leg="open"))
        return tuple(fills)

    def _fill(self, order: SimulatedOrder, observation: CanonicalObservation, *, side: str, qty: Decimal, leg: str) -> NormalizedFill:
        self._fill_sequence += 1
        fill_id = _stable_id(self.run_id, "fill", self._fill_sequence, order.order_id, observation.observation_id, side, leg)
        return NormalizedFill(
            timestamp=observation.second_bucket,
            symbol=observation.symbol,
            trader_id=self.trader_id,
            order_id=order.order_id,
            fill_id=fill_id,
            side=side,
            qty=qty,
            price=observation.close_price,
            leg=leg,
            reason=order.intent.reason,
            source=self.version,
            source_sequence=self._fill_sequence,
        )

    def _activate_protection(self, *, order: SimulatedOrder, fill: NormalizedFill) -> None:
        plan = order.intent.exit_plan or {}
        protection_type = "bracket"
        take_profit_price = Decimal(str(plan["take_profit_limit_price"])) if plan else None
        stop_loss_price = Decimal(str(plan["stop_loss_trigger_price"])) if plan else Decimal("0")
        take_profit_order_id = str(plan.get("take_profit_client_order_id") or _stable_id(order.order_id, "tp")) if plan else None
        stop_loss_order_id = str(plan.get("stop_loss_client_order_id") or _stable_id(order.order_id, "sl"))
        lifecycle_id = str(plan.get("lifecycle_id"))
        if order.intent.protection_type == "reversal_catastrophic_stop":
            protection_type = "reversal_catastrophic_stop"
            if order.intent.brick_size is None or order.intent.catastrophic_stop_bricks is None:
                raise ValueError("reversal catastrophic stop replay requires brick_size and catastrophic_stop_bricks")
            stop_plan = build_reversal_catastrophic_stop_plan(
                trader_id=order.intent.trader_id or order.trader_id,
                strategy_id=order.intent.strategy,
                symbol=order.symbol,
                entry_side=order.intent.side,
                quantity=fill.qty,
                entry_reference_price=fill.price,
                brick_size=order.intent.brick_size,
                catastrophic_stop_bricks=order.intent.catastrophic_stop_bricks,
                client_order_id=order.intent.client_order_id,
                candidate_config_fingerprint=order.intent.candidate_config_fingerprint,
                market_contract_fingerprint=order.intent.market_contract_fingerprint,
            )
            stop_loss_price = Decimal(str(stop_plan["stop_price"]))
            stop_loss_order_id = str(stop_plan["stop_client_order_id"])
            lifecycle_id = str(stop_plan["lifecycle_id"])
        protection = SimulatedProtection(
            parent_order_id=order.order_id,
            lifecycle_id=lifecycle_id,
            symbol=order.symbol,
            position_side="long" if fill.side == "buy" else "short",
            qty=fill.qty,
            take_profit_price=take_profit_price,
            stop_loss_price=stop_loss_price,
            take_profit_order_id=take_profit_order_id,
            stop_loss_order_id=stop_loss_order_id,
            state="PROTECTED",
            protection_type=protection_type,
        )
        self._protections.append(protection)
        self.protection_events.append(
            {
                "state": "PROTECTED",
                "parent_order_id": protection.parent_order_id,
                "take_profit_order_id": protection.take_profit_order_id,
                "stop_loss_order_id": protection.stop_loss_order_id,
                "lifecycle_id": protection.lifecycle_id,
                "qty": str(protection.qty),
                "stop_price": str(protection.stop_loss_price),
                "protection_type": protection.protection_type,
            }
        )

    def _cancel_reversal_catastrophic_stop_for_normal_exit(
        self,
        *,
        order: SimulatedOrder,
        fill: NormalizedFill | None,
    ) -> None:
        cancelled: list[SimulatedProtection] = []
        remaining: list[SimulatedProtection] = []
        symbol = fill.symbol if fill is not None else order.symbol
        for protection in self._protections:
            if protection.symbol == symbol and protection.protection_type == "reversal_catastrophic_stop":
                cancelled.append(protection)
            else:
                remaining.append(protection)
        if not cancelled:
            return
        self._protections = remaining
        for protection in cancelled:
            self.protection_events.append(
                {
                    "state": "CANCELED_FOR_REVERSAL_EXIT",
                    "parent_order_id": protection.parent_order_id,
                    "stop_loss_order_id": protection.stop_loss_order_id,
                    "lifecycle_id": protection.lifecycle_id,
                    "normal_exit_order_id": order.order_id,
                    "normal_exit_fill_id": fill.fill_id if fill is not None else None,
                    "qty": str(fill.qty) if fill is not None else str(order.qty),
                    "protection_type": protection.protection_type,
                }
            )

    def fill_active_protections_on_raw_trade(
        self,
        *,
        tick: MarketTick,
        position: PositionState,
    ) -> tuple[SimulatedFillBundle, ...]:
        observation = CanonicalObservation(
            observation_id=_stable_id(self.run_id, "raw-stop", tick.timestamp, tick.symbol, tick.price, tick.trade_id),
            run_id=self.run_id,
            symbol=tick.symbol.upper(),
            second_bucket=tick.timestamp,
            emitted_sequence=0,
            close_price=Decimal(str(tick.price)),
            source_eligible_trade_id=tick.trade_id,
            source_sip_timestamp=tick.timestamp,
            source_record_id=tick.trade_id,
            source_alpaca_trade_id=tick.trade_id,
            source_exchange=tick.exchange,
            source_tape=tick.tape,
        )
        return self._fill_active_protections(observation=observation, position=position)

    def _fill_active_protections(
        self,
        *,
        observation: CanonicalObservation,
        position: PositionState,
    ) -> tuple[SimulatedFillBundle, ...]:
        if position.side is None:
            return tuple()
        bundles: list[SimulatedFillBundle] = []
        remaining_protections: list[SimulatedProtection] = []
        for protection in self._protections:
            if protection.symbol != observation.symbol:
                remaining_protections.append(protection)
                continue
            reason = None
            close_side = None
            child_order_id = None
            if protection.position_side == "long":
                if protection.take_profit_price is not None and observation.close_price >= protection.take_profit_price:
                    reason = "take_profit_broker_native"
                    close_side = "sell"
                    child_order_id = protection.take_profit_order_id
                elif observation.close_price <= protection.stop_loss_price:
                    reason = "reversal_catastrophic_stop_broker_native" if protection.protection_type == "reversal_catastrophic_stop" else "stop_loss_broker_native"
                    close_side = "sell"
                    child_order_id = protection.stop_loss_order_id
            else:
                if protection.take_profit_price is not None and observation.close_price <= protection.take_profit_price:
                    reason = "take_profit_broker_native"
                    close_side = "buy"
                    child_order_id = protection.take_profit_order_id
                elif observation.close_price >= protection.stop_loss_price:
                    reason = "reversal_catastrophic_stop_broker_native" if protection.protection_type == "reversal_catastrophic_stop" else "stop_loss_broker_native"
                    close_side = "buy"
                    child_order_id = protection.stop_loss_order_id
            if reason is None or close_side is None or child_order_id is None:
                remaining_protections.append(protection)
                continue
            intent = OrderIntent(
                client_order_id=child_order_id,
                timestamp=observation.second_bucket,
                symbol=protection.symbol,
                side=close_side,  # type: ignore[arg-type]
                signal_type="exit",
                order_type="market",
                strategy="broker_native_exit",
                reason=reason,
                qty=min(position.qty, protection.qty),
                target_qty=0,
                close_position=True,
            )
            order = SimulatedOrder(
                order_id=child_order_id,
                run_id=self.run_id,
                sequence=0,
                signal=Signal(
                    signal_id=child_order_id,
                    signal_type="exit",
                    timestamp=observation.second_bucket,
                    symbol=protection.symbol,
                    side=protection.position_side,  # type: ignore[arg-type]
                    broker_side=close_side,  # type: ignore[arg-type]
                    price=float(observation.close_price),
                    reason=reason,
                    strategy="broker_native_exit",
                    trader_id=self.trader_id,
                ),
                intent=intent,
                source_observation_id=observation.observation_id,
                queued_at=observation.second_bucket,
                symbol=protection.symbol,
                trader_id=self.trader_id,
                qty=min(position.qty, protection.qty),
            )
            fill = self._fill(order, observation, side=close_side, qty=min(position.qty, protection.qty), leg="close")
            bundles.append(SimulatedFillBundle(order=order, fills=(fill,), latency_seconds=Decimal("0")))
            self.protection_events.append(
                {
                    "state": "FLAT",
                    "parent_order_id": protection.parent_order_id,
                    "filled_child_order_id": child_order_id,
                    "sibling_cancelled": True,
                    "reason": reason,
                    "qty": str(fill.qty),
                }
            )
        self._protections = remaining_protections
        return tuple(bundles)


def _stable_id(*parts: object) -> str:
    return sha256("|".join(str(part) for part in parts).encode("utf-8")).hexdigest()


def _order_would_close_position(order: SimulatedOrder, position: PositionState) -> bool:
    side = str(order.intent.side).lower()
    return (position.side == "long" and side == "sell") or (position.side == "short" and side == "buy")


def _apply_position_shadow(position: PositionState, fill: NormalizedFill) -> PositionState:
    if fill.leg == "close":
        remaining = position.qty - fill.qty
        if remaining <= 0:
            return PositionState.flat(fill.symbol)
        return PositionState(
            symbol=position.symbol,
            side=position.side,
            qty=remaining,
            avg_entry_price=position.avg_entry_price,
            basis_known=position.basis_known,
            entry_timestamp=position.entry_timestamp,
            entry_fill_id=position.entry_fill_id,
        )
    side = "long" if fill.side == "buy" else "short"
    return PositionState(symbol=fill.symbol, side=side, qty=fill.qty, avg_entry_price=fill.price, basis_known=True)
