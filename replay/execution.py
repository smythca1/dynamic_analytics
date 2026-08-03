from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from hashlib import sha256

from silver.accounting import NormalizedFill, PositionState
from silver.market import CanonicalObservation
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


class NextObservationExecution:
    version = "NEXT_CANONICAL_OBSERVATION_V1"

    def __init__(self, *, run_id: str, trader_id: str) -> None:
        self.run_id = run_id
        self.trader_id = trader_id
        self._order_sequence = 0
        self._fill_sequence = 0
        self._pending: list[SimulatedOrder] = []

    @property
    def pending_orders(self) -> tuple[SimulatedOrder, ...]:
        return tuple(self._pending)

    def queue(self, *, signal: Signal, intent: OrderIntent, observation: CanonicalObservation) -> SimulatedOrder:
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
        return order

    def fill_pending(self, *, observation: CanonicalObservation, position: PositionState) -> tuple[SimulatedFillBundle, ...]:
        ready = [order for order in self._pending if order.source_observation_id != observation.observation_id]
        self._pending = [order for order in self._pending if order.source_observation_id == observation.observation_id]
        bundles: list[SimulatedFillBundle] = []
        working_position = position
        for order in ready:
            fills = self._build_fills(order, observation, working_position)
            for fill in fills:
                working_position = _apply_position_shadow(working_position, fill)
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


def _stable_id(*parts: object) -> str:
    return sha256("|".join(str(part) for part in parts).encode("utf-8")).hexdigest()


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
