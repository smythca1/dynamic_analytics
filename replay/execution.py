from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import date
from decimal import Decimal, ROUND_CEILING, getcontext, localcontext
from hashlib import sha256
from itertools import groupby
from zoneinfo import ZoneInfo

import pandas as pd

from silver.execution import authoritative_position_from_accounting, resolve_close_intent
from silver.accounting import AccountingSnapshot, NormalizedFill, PositionState
from silver.market import CanonicalObservation
from reversal_protection import build_reversal_catastrophic_stop_plan
from trading_events import MarketTick
from trading_events import OrderIntent, Signal
from exit_plan import stable_exit_plan_fingerprint
from broker_protection import verify_broker_native_protection
from .stop_election import CONSOLIDATED_TAPES, excluded_condition_reason


class InvalidProtectionGeometry(ValueError):
    """A verified directional violation, never a broker/infrastructure failure."""

    INVARIANTS = {
        "invalid_long_take_profit_price": "TP > actual entry fill",
        "invalid_long_stop_loss_price": "SL < actual entry fill",
        "invalid_short_take_profit_price": "TP < actual entry fill",
        "invalid_short_stop_loss_price": "SL > actual entry fill",
    }

    def __init__(self, evidence):
        super().__init__("INVALID_PROTECTION_GEOMETRY")
        self.evidence = evidence


@dataclass(frozen=True)
class RawExecutionAssumptions:
    protection_ack_delay_ms: int = 0
    force_flat_delay_ms: int = 0

    def __post_init__(self):
        for name in ("protection_ack_delay_ms", "force_flat_delay_ms"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a nonnegative integer")


@dataclass(frozen=True)
class BacktestExecutionProfile:
    """Research execution policy; no live broker behavior is configured here."""

    profile_id: str = "STRICT_ACTIVITY_SIDE_V1"
    regulatory_order_side_policy: str = "ACTIVITY_SIDE"

    def __post_init__(self):
        if self.regulatory_order_side_policy not in {"ACTIVITY_SIDE", "SIDE_FAMILY"}:
            raise ValueError("Unsupported regulatory order-side policy")
        if not self.profile_id:
            raise ValueError("Backtest execution profile requires a profile_id")

    def regulatory_order_side(self, fill: NormalizedFill) -> str:
        if self.regulatory_order_side_policy == "ACTIVITY_SIDE":
            return fill.side
        if fill.side in {"sell", "sell_short"}:
            return "sell"
        return fill.side


STRICT_ACTIVITY_SIDE_PROFILE = BacktestExecutionProfile()
SIDE_FAMILY_BACKTEST_PROFILE = BacktestExecutionProfile(
    profile_id="SIDE_FAMILY_REGULATORY_FEES_V1",
    regulatory_order_side_policy="SIDE_FAMILY",
)

ALLOWED_EQUITY_REGULATORY_SYMBOLS = frozenset({"TSLA", "SPXC"})
EQUITY_TAF_ZERO_EFFECTIVE_DATE = date(2026, 10, 1)


def raw_trade_groups(trades):
    """Equal timestamps have no proven economic ordering, regardless of row index."""
    previous = None
    for timestamp, group in groupby(trades, key=lambda trade: trade.sip_timestamp):
        if previous is not None and timestamp <= previous:
            raise ValueError("Raw execution requires chronologically ordered evidence")
        previous = timestamp
        yield tuple(group)


def raw_execution_interval(trades):
    """Certified equal-timestamp eligibility, adverse bounds, and ordered lineage."""
    eligible = [item for item in trades if item.tape in CONSOLIDATED_TAPES
                and excluded_condition_reason(item.conditions) is None]
    if not eligible:
        return None
    return (min(item.price for item in eligible), max(item.price for item in eligible),
            _stable_id(*(item.source_identity for item in eligible)))


def raw_candidate_events(trades, observations):
    pending = iter(observations)
    observation = next(pending, None)
    for group in raw_trade_groups(trades):
        timestamp = group[0].sip_timestamp
        yield group, None
        while observation is not None and observation.triggering_sip_timestamp is not None and observation.triggering_sip_timestamp <= timestamp:
            if observation.triggering_sip_timestamp != timestamp:
                raise ValueError("Canonical trigger missing from raw execution evidence")
            yield None, observation
            observation = next(pending, None)
    if observation is not None:
        raise ValueError("Canonical observation unavailable in raw execution evidence")


@dataclass(frozen=True)
class EquityRegulatoryFees:
    """Approved TSLA-only published-schedule fallback, not account verification."""

    reg_rate: Decimal = Decimal("0.0000206")
    taf_per_share: Decimal = Decimal("0.000195")
    taf_order_cap: Decimal = Decimal("9.79")
    cat_per_share: Decimal = Decimal("0.000003")
    valid_from: str = "2026-08-07"
    valid_through: str = "2026-09-25"
    version: str = "ALPACA_TSLA_DAILY_REGULATORY_V1"
    commission_status: str = "UNVERIFIED_ACCOUNT; APPROVED_DIRECT_RETAIL_ZERO_COMMISSION_ASSUMPTION"
    source: str = "https://files.alpaca.markets/disclosures/library/BrokFeeSched.pdf"
    aggregation: str = "America/New_York; separate fee types; TSLA daily totals; ROUND_CEILING cents"

    def __post_init__(self):
        for name in ("reg_rate", "taf_per_share", "taf_order_cap", "cat_per_share"):
            value = Decimal(str(getattr(self, name)))
            if not value.is_finite() or value < 0:
                raise ValueError("Regulatory fee rates/caps must be finite and nonnegative")
            object.__setattr__(self, name, value)
        if date.fromisoformat(self.valid_from) > date.fromisoformat(self.valid_through):
            raise ValueError("Invalid fee schedule validity window")
        if self.version != "ALPACA_TSLA_DAILY_REGULATORY_V1" or self.aggregation != "America/New_York; separate fee types; TSLA daily totals; ROUND_CEILING cents":
            raise ValueError("Unsupported regulatory fee semantics")

    def components(self, fill, previous_order_qty=Decimal(0)):
        session = fill.timestamp.astimezone(ZoneInfo("America/New_York")).date().isoformat()
        if fill.symbol not in ALLOWED_EQUITY_REGULATORY_SYMBOLS or not self.valid_from <= session <= self.valid_through:
            raise ValueError("Fill outside approved equity fee schedule window")
        if not fill.qty.is_finite() or not fill.price.is_finite() or fill.qty <= 0 or fill.price <= 0:
            raise ValueError("Fee quantity and principal must be finite and positive")
        if not fill.order_id:
            raise ValueError("Regulatory fees require an order ID for partial-fill caps")
        with localcontext() as context:
            context.prec = 40
            selling = fill.side in {"sell", "sell_short"}
            taf_per_share = (
                Decimal("0")
                if date.fromisoformat(session) >= EQUITY_TAF_ZERO_EFFECTIVE_DATE
                else self.taf_per_share
            )
            return {
                "REG": fill.qty * fill.price * self.reg_rate if selling else Decimal(0),
                "TAF": (min((previous_order_qty + fill.qty) * taf_per_share, self.taf_order_cap)
                        - min(previous_order_qty * taf_per_share, self.taf_order_cap)) if selling else Decimal(0),
                "CAT": fill.qty * self.cat_per_share,
            }


@dataclass(frozen=True)
class ExecutionFriction:
    """Research assumptions, never live broker settings. All charges are adverse."""

    slippage_per_share: Decimal = Decimal("0")
    slippage_bps: Decimal = Decimal("0")
    half_spread: Decimal = Decimal("0")
    fee_per_share: Decimal = Decimal("0")
    fee_per_fill: Decimal = Decimal("0")
    calibration: str = "UNCALIBRATED research assumption"
    application: str = "PRICE_ADJUSTMENT"
    base_slippage_per_share: Decimal | None = None
    stress_slippage_per_share: Decimal | None = None
    calibration_evidence_hash: str = ""
    calibration_sessions: tuple[str, ...] = ()
    fee_assumption: str = "UNVERIFIED; configured research fees only"
    regulatory_fees: EquityRegulatoryFees | None = None

    def __post_init__(self) -> None:
        if isinstance(self.regulatory_fees, dict):
            object.__setattr__(self, "regulatory_fees", EquityRegulatoryFees(**self.regulatory_fees))
        if self.regulatory_fees is not None and not isinstance(self.regulatory_fees, EquityRegulatoryFees):
            raise ValueError("Invalid regulatory fee configuration")
        for name in ("slippage_per_share", "slippage_bps", "half_spread", "fee_per_share", "fee_per_fill"):
            value = Decimal(str(getattr(self, name)))
            if not value.is_finite() or value < 0:
                raise ValueError(f"{name} must be finite and nonnegative")
            object.__setattr__(self, name, value)
        if self.application not in {"PRICE_ADJUSTMENT", "PER_FILL_CHARGE_V1"}:
            raise ValueError("Unsupported friction application")
        if self.regulatory_fees is not None and (
            (self.application != "PER_FILL_CHARGE_V1"
             and self.calibration != "TSLA PRICE-NORMALIZED EXECUTION PROXY")
            or self.fee_per_share
            or self.fee_per_fill
        ):
            raise ValueError("Regulatory fallback requires separate approved friction and no overlapping flat fees")
        object.__setattr__(self, "calibration_sessions", tuple(self.calibration_sessions))
        if self.application == "PER_FILL_CHARGE_V1":
            if self.slippage_bps or self.half_spread:
                raise ValueError("Fixed research charge cannot mix bps or spread adjustments")
            base = Decimal(str(self.base_slippage_per_share if self.base_slippage_per_share is not None else self.slippage_per_share))
            stress = Decimal(str(self.stress_slippage_per_share if self.stress_slippage_per_share is not None else base * 2))
            if not base.is_finite() or base <= 0 or stress != base * 2:
                raise ValueError("Research charge requires positive BASE and exactly 2x STRESS")
            if self.slippage_per_share not in {base, stress}:
                raise ValueError("Active friction must equal frozen BASE or STRESS")
            object.__setattr__(self, "base_slippage_per_share", base)
            object.__setattr__(self, "stress_slippage_per_share", stress)

    def price(self, price: Decimal, side: str) -> Decimal:
        if side not in {"buy", "sell", "sell_short"}:
            raise ValueError("Unsupported execution side")
        if self.application == "PER_FILL_CHARGE_V1":
            if not price.is_finite() or price <= 0:
                raise ValueError("Execution reference price must be finite and positive")
            return price
        impact = self.slippage_per_share + self.half_spread + price * self.slippage_bps / Decimal("10000")
        result = price + impact if side == "buy" else price - impact
        if not result.is_finite() or result <= 0:
            raise ValueError("Execution friction produces a nonpositive price")
        return result

    def fee(self, qty: Decimal) -> Decimal:
        return qty * self.fee_per_share + self.fee_per_fill

    def stress(self) -> "ExecutionFriction | None":
        if self.application == "PER_FILL_CHARGE_V1":
            return replace(self, slippage_per_share=self.stress_slippage_per_share)
        names = ("slippage_per_share", "slippage_bps", "half_spread", "fee_per_share", "fee_per_fill")
        if not any(getattr(self, name) for name in names):
            return None
        return replace(self, **{name: getattr(self, name) * 2 for name in names})

    def charge(self, qty: Decimal) -> Decimal:
        return qty * self.slippage_per_share if self.application == "PER_FILL_CHARGE_V1" else Decimal("0")


class ResearchFeeLedger:
    """Fee overlay on Silver results; Silver remains the sole position/PnL engine."""

    def __init__(self, friction: ExecutionFriction, *, execution_profile: BacktestExecutionProfile | None = None) -> None:
        self.friction = friction
        self.execution_profile = execution_profile or STRICT_ACTIVITY_SIDE_PROFILE
        self.total = Decimal("0")
        self.open_fees = Decimal("0")
        self.trade_fees: dict[str, Decimal] = {}
        self.cumulative_fees: dict[str, Decimal] = {}
        self.friction_total = Decimal("0")
        self.open_friction = Decimal("0")
        self.trade_friction: dict[str, Decimal] = {}
        self.cumulative_friction: dict[str, Decimal] = {}
        self.daily_components: dict[tuple[str, str], dict[str, Decimal]] = {}
        self.order_quantities: dict[tuple[str, str], Decimal] = {}
        self.order_sides: dict[tuple[str, str], str] = {}
        self.recorded_fills: dict[tuple[str, str], NormalizedFill] = {}
        self.regulatory_totals = {name: Decimal(0) for name in ("REG", "TAF", "CAT")}

    def _regulatory_fee(self, fill):
        account = fill.account_id or "standalone_tsla"
        session = fill.timestamp.astimezone(ZoneInfo("America/New_York")).date().isoformat()
        order_key = (account, fill.order_id)
        order_side = self.execution_profile.regulatory_order_side(fill)
        if order_key in self.order_sides and self.order_sides[order_key] != order_side:
            raise ValueError("Order side changed inside regulatory fee evidence")
        previous_qty = self.order_quantities.get(order_key, Decimal(0))
        components = self.friction.regulatory_fees.components(fill, previous_qty)
        accrued = self.daily_components.setdefault((account, session), {name: Decimal(0) for name in components})
        with localcontext() as context:
            context.prec = 40
            total = Decimal(0)
            for name, amount in components.items():
                previous = accrued[name].quantize(Decimal(".01"), rounding=ROUND_CEILING)
                accrued[name] += amount
                delta = accrued[name].quantize(Decimal(".01"), rounding=ROUND_CEILING) - previous
                self.regulatory_totals[name] += delta
                total += delta
            self.order_quantities[order_key] = previous_qty + fill.qty
        self.order_sides[order_key] = order_side
        return total

    def fee_evidence(self):
        return {
            "schedule": asdict(self.friction.regulatory_fees) if self.friction.regulatory_fees else None,
            "execution_profile": asdict(self.execution_profile),
            "regulatory_fees": {name: str(value) for name, value in self.regulatory_totals.items()},
            "commission_dollars": "0" if self.friction.regulatory_fees else None,
            "daily": [{"account": account, "session": session,
                       "unrounded": {name: str(value) for name, value in values.items()},
                       "rounded": {name: str(value.quantize(Decimal(".01"), rounding=ROUND_CEILING)) for name, value in values.items()}}
                      for (account, session), values in sorted(self.daily_components.items())],
            "allocation": "Causal increases in rounded daily totals; not claimed broker per-order charges",
        }

    def record(self, fill: NormalizedFill, before: PositionState) -> None:
        key = (fill.account_id or "standalone_tsla", fill.stable_fill_id)
        if self.friction.regulatory_fees is not None and key in self.recorded_fills:
            if self.recorded_fills[key] != fill:
                raise ValueError("Conflicting duplicate fee fill")
            return
        if fill.leg not in {"open", "close"} or (fill.leg == "close" and (before.qty <= 0 or fill.qty > before.qty)):
            raise ValueError("Research fees require valid explicit open/close legs")
        fee = self.friction.fee(fill.qty)
        if self.friction.regulatory_fees is not None:
            fee += self._regulatory_fee(fill)
            self.recorded_fills[key] = fill
        charge = self.friction.charge(fill.qty)
        self.friction_total += charge
        self.cumulative_friction[fill.stable_fill_id] = self.friction_total
        self.total += fee
        self.cumulative_fees[fill.stable_fill_id] = self.total
        if fill.leg == "open":
            self.open_fees += fee
            self.open_friction += charge
        elif fill.leg == "close":
            allocated = self.open_fees * fill.qty / before.qty
            self.open_fees -= allocated
            self.trade_fees[fill.stable_fill_id] = fee + allocated
            allocated_charge = self.open_friction * fill.qty / before.qty
            self.open_friction -= allocated_charge
            self.trade_friction[fill.stable_fill_id] = charge + allocated_charge
        else:
            raise ValueError("Research fees require explicit open/close fill legs")

    def snapshot(self, snapshot):
        from silver.accounting.metrics import pct, trade_metrics
        from silver.accounting.models import EquityPoint

        trades = tuple(
            replace(
                trade,
                pnl=trade.pnl - self.trade_fees.get(trade.exit_fill_id, Decimal("0")) - self.trade_friction.get(trade.exit_fill_id, Decimal("0")),
                pnl_pct=pct(trade.pnl - self.trade_fees.get(trade.exit_fill_id, Decimal("0")) - self.trade_friction.get(trade.exit_fill_id, Decimal("0")), trade.entry_price * trade.qty),
            ) if trade.pnl is not None and trade.entry_price is not None else trade
            for trade in snapshot.closed_trades
        )
        curve = snapshot.equity_curve
        if (self.total or self.friction_total) and curve:
            capital = snapshot.equity - snapshot.net_pnl
            curve = (EquityPoint(curve[0].timestamp, capital, Decimal("0"), Decimal("0"), Decimal("0"), "initial_capital"),) + tuple(
                replace(point, equity=point.equity - self.cumulative_fees.get(point.fill_id, Decimal("0")) - self.cumulative_friction.get(point.fill_id, Decimal("0")),
                        net_pnl=point.net_pnl - self.cumulative_fees.get(point.fill_id, Decimal("0")) - self.cumulative_friction.get(point.fill_id, Decimal("0")))
                for point in curve
            )
        dollars, percent = research_drawdown(curve)
        return replace(
            snapshot, net_pnl=snapshot.net_pnl - self.total - self.friction_total,
            equity=snapshot.equity - self.total - self.friction_total, closed_trades=trades,
            equity_curve=curve, max_drawdown_dollars=dollars, max_drawdown_percent=percent,
            **trade_metrics(list(trades)),
        )

    def mark_snapshot(self, accounting, mark_price):
        unrealized = accounting._unrealized_pnl(mark_price)
        gross_net = accounting.realized_pnl + unrealized
        gross_equity = accounting.starting_capital + gross_net
        context = getcontext()
        # The reference's recovered capital can vary with mark-price rounding.
        key = (id(accounting), len(accounting.equity_curve), self.total, self.friction_total,
               gross_equity - gross_net, context.prec, context.rounding,
               context.Emin, context.Emax, context.clamp)
        if getattr(self, "_mark_cache_key", None) != key:
            self._mark_cache = self.snapshot(accounting.snapshot(mark_price=mark_price))
            self._mark_cache_key = key
        net = gross_net - self.total - self.friction_total
        return replace(self._mark_cache, position=accounting.position,
                       unrealized_pnl=unrealized, net_pnl=net,
                       equity=gross_equity - self.total - self.friction_total)

    def mark_snapshot_bounded(self, accounting, mark_price):
        """Return only the live mark fields needed by bounded replay.

        Full snapshots copy the complete equity curve and recompute trade
        metrics.  Bounded replay only needs current equity/PnL/position for
        its incremental summaries; the authoritative full snapshot remains
        computed once at candidate completion.
        """
        unrealized = accounting._unrealized_pnl(mark_price)
        gross_net = accounting.realized_pnl + unrealized
        net = gross_net - self.total - self.friction_total
        return AccountingSnapshot(
            position=accounting.position,
            realized_pnl=accounting.realized_pnl,
            unrealized_pnl=unrealized,
            net_pnl=net,
            equity=accounting.starting_capital + net,
        )


def research_drawdown(equity_curve):
    """Dollar and percent maxima can occur at different points in the curve."""
    from silver.accounting.metrics import drawdown_metrics
    from .metrics import EquitySummary

    if isinstance(equity_curve, EquitySummary):
        return equity_curve.dollars, equity_curve.percent

    dollars, _ = drawdown_metrics(equity_curve)
    peak = None
    percent = Decimal("0")
    for point in equity_curve:
        if peak is None or point.equity > peak.equity:
            peak = point
        if peak.equity > 0:
            _, loss_pct = drawdown_metrics([peak, point])
            percent = max(percent, loss_pct)
    return dollars, percent


def deterministic_action(signal: Signal, intent: OrderIntent, signal_id: str, client_order_id: str):
    plan = dict(intent.exit_plan) if intent.exit_plan is not None else None
    if plan is not None:
        plan.update(client_order_id=client_order_id,
                    take_profit_client_order_id=f"{client_order_id[:45]}-tp",
                    stop_loss_client_order_id=f"{client_order_id[:45]}-sl")
        plan["lifecycle_id"] = stable_exit_plan_fingerprint({key: plan[key] for key in (
            "trader_id", "strategy_id", "symbol", "client_order_id", "entry_reference_type",
            "entry_reference_price", "take_profit_bricks", "stop_loss_bricks", "brick_size",
        )})[:24]
    return (replace(signal, signal_id=signal_id),
            replace(intent, signal_id=signal_id, client_order_id=client_order_id, exit_plan=plan))


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

    def __init__(self, *, run_id: str, trader_id: str, auto_fill_protections: bool = True,
                 friction: ExecutionFriction | None = None, conservative_protections: bool = False,
                 execution_profile: BacktestExecutionProfile | None = None) -> None:
        self.run_id = run_id
        self.trader_id = trader_id
        self.auto_fill_protections = auto_fill_protections
        self.friction = friction or ExecutionFriction()
        self.execution_profile = execution_profile or STRICT_ACTIVITY_SIDE_PROFILE
        self.conservative_protections = conservative_protections
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
        if self.conservative_protections and (intent.order_type != "market" or intent.stop_loss_order_type == "stop_limit"
                                              or intent.exit_management == "oco"):
            raise ValueError("Conservative replay does not certify limit entries, stop-limit exits, or OCO recovery")
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
        ready = [order for order in self._pending if order.source_observation_id != observation.observation_id
                 and observation.second_bucket > order.queued_at]
        self._pending = [order for order in self._pending if order not in ready]
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
                if self.conservative_protections and fill.leg == "close" and working_position.side is None:
                    self._protections = [item for item in self._protections if item.symbol != fill.symbol]
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
        price = self.friction.price(observation.close_price, side)
        if order.intent.reason == "take_profit_broker_native" and order.intent.limit_price is not None:
            limit = Decimal(str(order.intent.limit_price))
            price = min(price, limit) if side == "buy" else max(price, limit)
        return NormalizedFill(
            timestamp=observation.second_bucket,
            symbol=observation.symbol,
            trader_id=self.trader_id,
            order_id=order.order_id,
            fill_id=fill_id,
            side=side,
            qty=qty,
            price=price,
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
                order_type="limit" if reason == "take_profit_broker_native" else "market",
                limit_price=protection.take_profit_price if reason == "take_profit_broker_native" else None,
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
            fill_observation = observation
            if self.conservative_protections and reason == "take_profit_broker_native":
                fill_observation = replace(observation, close_price=protection.take_profit_price)
            fill = self._fill(order, fill_observation, side=close_side, qty=min(position.qty, protection.qty), leg="close")
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


class RawSipExecution(NextObservationExecution):
    """Opt-in economic approximation, not a broker simulator or broker-fill evidence."""

    version = "RAW_SIP_NATIVE_V1"

    def __init__(self, *, strategy, assumptions=None, **kwargs):
        kwargs.update(conservative_protections=True, auto_fill_protections=False)
        super().__init__(**kwargs)
        self.strategy = strategy
        self.assumptions = assumptions or RawExecutionAssumptions()
        config = strategy.config
        if config.strategy_type != "renko_tp_sl" or config.exit_management != "bracket":
            raise ValueError("Raw native replay requires TP/SL with bracket protection")
        if config.allow_overnight or not config.force_flat:
            raise ValueError("Raw native replay requires a configured intraday force-flat")
        self._awaiting_ack = []
        self._last_clock = None
        self._force_dates = set()
        self._force_race = None
        self.fill_evidence = {}

    def queue(self, *, signal, intent, observation, position=None):
        available = observation.triggering_sip_timestamp
        if available is None:
            raise ValueError("Raw execution requires canonical availability timestamps")
        available = pd.Timestamp(available)
        if not intent.close_position:
            if not self.strategy._entry_allowed(available):
                raise ValueError("Entry became available outside the production entry window")
            if (position is not None and position.side is not None) or self._pending:
                raise ValueError("Overlapping raw bracket entries are unsupported")
            if intent.exit_plan is None or intent.exit_management != "bracket":
                raise ValueError("Raw native entry requires the production ExitPlan")
        else:
            self._protections.clear()
            self._awaiting_ack.clear()
            self.protection_events.append({"state": "CANCELED_FOR_CLOSE", "timestamp": available})
        return super().queue(signal=replace(signal, timestamp=available),
                             intent=replace(intent, timestamp=available),
                             observation=observation, position=position)

    def fill_pending(self, *, observation, position):
        # Canonical observations drive strategy only; they cannot execute orders.
        return ()

    def _activate_protection(self, *, order, fill):
        if self._protections or self._awaiting_ack:
            raise ValueError("Overlapping protective groups are unsupported")
        super()._activate_protection(order=order, fill=fill)
        protection = self._protections.pop()
        self.protection_events.pop()
        plan = order.intent.exit_plan
        closing = "sell" if fill.side == "buy" else "buy"
        parent = {"id": order.order_id, "client_order_id": plan["client_order_id"],
                  "symbol": fill.symbol, "side": order.intent.side, "status": "filled",
                  "order_class": "bracket", "legs": [
                      {"id": protection.take_profit_order_id, "side": closing, "status": "new",
                       "qty": str(fill.qty), "limit_price": str(protection.take_profit_price)},
                      {"id": protection.stop_loss_order_id, "side": closing, "status": "new",
                       "qty": str(fill.qty), "stop_price": str(protection.stop_loss_price)}]}
        verification = verify_broker_native_protection(
            symbol=fill.symbol, broker_position={"symbol": fill.symbol, "qty": str(fill.qty),
                "side": protection.position_side, "avg_entry_price": str(fill.price)},
            nested_orders=[parent], exit_plan=plan,
            expected_config_fingerprint=self.strategy.config.candidate_config_fingerprint,
            expected_market_contract_fingerprint=self.strategy.config.market_contract_fingerprint,
        )
        if not verification.protected:
            self.protection_events.append({"state": "PROTECTION_FAILED", "timestamp": fill.timestamp,
                                           "issues": [issue.code for issue in verification.issues]})
            codes = [issue.code for issue in verification.issues]
            if codes and set(codes) <= InvalidProtectionGeometry.INVARIANTS.keys():
                raise InvalidProtectionGeometry({
                    "reason": "INVALID_PROTECTION_GEOMETRY",
                    "session": pd.Timestamp(fill.timestamp).tz_convert("America/New_York").date().isoformat(),
                    "timestamp": pd.Timestamp(fill.timestamp).isoformat(),
                    "direction": protection.position_side,
                    "signal_reference_price": str(plan["entry_reference_price"]),
                    "modeled_entry_fill": str(fill.price),
                    "intended_tp": str(protection.take_profit_price),
                    "intended_sl": str(protection.stop_loss_price),
                    "execution_friction": asdict(self.friction),
                    "issue_codes": codes,
                    "violated_invariants": [InvalidProtectionGeometry.INVARIANTS[code] for code in codes],
                })
            raise ValueError(f"Protection verification failed: {verification.issues}")
        due = pd.Timestamp(fill.timestamp) + pd.Timedelta(milliseconds=self.assumptions.protection_ack_delay_ms)
        timeout = pd.Timestamp(fill.timestamp) + pd.Timedelta(
            seconds=self.strategy.config.protection_acknowledgement_timeout_seconds)
        self._awaiting_ack.append((due, timeout, protection))
        self.protection_events.append({"state": "PROTECTION_PENDING", "timestamp": fill.timestamp,
                                       "active_at": due, "parent_order_id": order.order_id,
                                       "evidence": "SIMULATED_ACK_ASSUMPTION"})
        self._acknowledge(pd.Timestamp(fill.timestamp))

    def _acknowledge(self, timestamp):
        remaining = []
        for due, timeout, protection in self._awaiting_ack:
            if due > timeout and timestamp >= timeout:
                self.protection_events.append({"state": "PROTECTION_FAILED", "timestamp": timeout,
                                               "reason": "acknowledgement_timeout"})
                raise ValueError("Protection acknowledgement timeout; recovery cannot be inferred from SIP")
            if timestamp >= due:
                self._protections.append(protection)
                self.protection_events.append({"state": "PROTECTED", "timestamp": due,
                                               "parent_order_id": protection.parent_order_id,
                                               "evidence": "SIMULATED_VERIFIED_BRACKET"})
            else:
                remaining.append((due, timeout, protection))
        self._awaiting_ack = remaining

    def _deadline(self, timestamp):
        local = pd.Timestamp(timestamp).tz_convert(self.strategy.timezone)
        day = local.date().isoformat()
        if ((self.strategy.active_start_date and day < self.strategy.active_start_date)
                or (self.strategy.active_end_date and day > self.strategy.active_end_date)):
            return None
        return pd.Timestamp(f"{day} {self.strategy.config.force_flat}", tz=self.strategy.timezone).tz_convert("UTC") + pd.Timedelta(
            milliseconds=self.assumptions.force_flat_delay_ms)

    def advance_clock(self, timestamp, position):
        timestamp = pd.Timestamp(timestamp)
        if self._last_clock is not None and timestamp < self._last_clock:
            raise ValueError("Raw execution clock cannot move backwards")
        # Exposure cannot jump across a missing session-end interval into tomorrow.
        if position.side is not None and position.entry_timestamp is not None:
            entered = pd.Timestamp(position.entry_timestamp).tz_convert(self.strategy.timezone)
            if entered.date() < timestamp.tz_convert(self.strategy.timezone).date():
                raise ValueError("Unresolved force-flat exposure crossed a session boundary")
        self._last_clock = timestamp
        deadline = self._deadline(timestamp)
        self._acknowledge(min(timestamp, deadline) if deadline is not None else timestamp)
        if deadline is not None and timestamp >= deadline and deadline not in self._force_dates:
            self._force_dates.add(deadline)
            self.protection_events.append({"state": "FORCE_FLAT_SUBMITTED", "timestamp": deadline,
                                           "cancelled_pending_orders": len(self._pending),
                                           "cancelled_protections": len(self._protections) + len(self._awaiting_ack)})
            self._pending.clear()
            self._force_race = (deadline, tuple(self._protections))
            self._protections.clear()
            self._awaiting_ack.clear()
            if position.side is not None:
                side = "sell" if position.side == "long" else "buy"
                identity = _stable_id(self.run_id, "force_flat", deadline)
                signal = Signal(identity, "exit", deadline, position.symbol, position.side, side,
                                float(position.avg_entry_price), "session_force_flat", "research_timer", self.trader_id)
                intent = OrderIntent(identity, deadline, position.symbol, side, "exit", "market",
                                     "research_timer", "session_force_flat", qty=position.qty,
                                     close_position=True)
                super().queue(signal=signal, intent=intent,
                              observation=self._observation(deadline, position.avg_entry_price, identity),
                              position=position)
        self._acknowledge(timestamp)

    def process_trade_group(self, trades, position):
        if not trades or len({(item.symbol, item.sip_timestamp) for item in trades}) != 1:
            raise ValueError("Execution interval must contain one symbol and timestamp")
        if trades[0].symbol != self.strategy.config.symbol:
            raise ValueError("Raw execution symbol differs from strategy")
        timestamp = pd.Timestamp(trades[0].sip_timestamp)
        self.advance_clock(timestamp, position)
        interval = raw_execution_interval(trades)
        if interval is None:
            return (), None
        return self.process_interval(timestamp=timestamp, low=interval[0], high=interval[1],
                                     position=position, identity=interval[2])

    def process_interval(self, *, timestamp, low, high, position, identity="interval"):
        """Unordered interval: stop first, adverse endpoint, no TP price improvement."""
        timestamp = pd.Timestamp(timestamp)
        low, high = Decimal(str(low)), Decimal(str(high))
        if not low.is_finite() or not high.is_finite() or low <= 0 or high < low:
            raise ValueError("Invalid execution interval")
        self.advance_clock(timestamp, position)
        if position.side is None and (self._protections or self._awaiting_ack):
            raise ValueError("Orphan protection without an accounting position")
        if len(self._protections) > 1:
            raise ValueError("Overlapping protection cannot be resolved from SIP")
        bundles = []
        working = position
        # At an unresolved timer/TP tie, a market exit above the limit (long) or
        # below it (short) would be favorable. Retain the worse native TP outcome.
        if self._force_race is not None:
            deadline, protections = self._force_race
            self._force_race = None
            if timestamp == deadline and protections and position.side is not None:
                protection = protections[0]
                closing = "sell" if position.side == "long" else "buy"
                market_price = self.friction.price(low if position.side == "long" else high, closing)
                tp_is_worse = (market_price > protection.take_profit_price if position.side == "long"
                               else market_price < protection.take_profit_price)
                if tp_is_worse:
                    self._protections = list(protections)
        if self._protections:
            protection = self._protections[0]
            if protection.qty != position.qty or protection.position_side != position.side:
                raise ValueError("Protective quantity or side mismatch")
            adverse = low if position.side == "long" else high
            favorable = high if position.side == "long" else low
            stop_hit = adverse <= protection.stop_loss_price if position.side == "long" else adverse >= protection.stop_loss_price
            closing = "sell" if position.side == "long" else "buy"
            executable = self.friction.price(favorable, closing)
            # A mere limit touch is not evidence that our resting order filled.
            tp_hit = executable > protection.take_profit_price if position.side == "long" else executable < protection.take_profit_price
            if stop_hit or tp_hit:
                mark = adverse if stop_hit else favorable
                bundles.extend(self._fill_active_protections(
                    observation=self._observation(timestamp, mark, identity), position=working))
                for bundle in bundles:
                    for fill in bundle.fills:
                        working = _apply_position_shadow(working, fill)
        if self._awaiting_ack and position.side is not None:
            self.protection_events.append({"state": "EXPOSED_MARK", "timestamp": timestamp,
                                           "low": str(low), "high": str(high), "qty": str(position.qty)})
        ready = [order for order in self._pending if timestamp > order.queued_at or
                 (timestamp == order.queued_at and order.intent.reason == "session_force_flat")]
        self._pending = [order for order in self._pending if order not in ready]
        for order in ready:
            if pd.Timestamp(order.queued_at).tz_convert(self.strategy.timezone).date() != timestamp.tz_convert(self.strategy.timezone).date():
                raise ValueError("Pending order crossed a session boundary without force-flat evidence")
            mark = high if order.intent.side == "buy" else low
            observation = self._observation(timestamp, mark, identity)
            fills = self._build_fills(order, observation, working)
            for fill in fills:
                working = _apply_position_shadow(working, fill)
                if fill.leg == "open":
                    self._activate_protection(order=order, fill=fill)
                elif working.side is None:
                    self._protections.clear()
                    self._awaiting_ack.clear()
                    self.protection_events.append({"state": "FLAT", "timestamp": timestamp,
                                                   "reason": fill.reason, "sibling_cancelled": True})
            bundles.append(SimulatedFillBundle(order, fills, Decimal(str((timestamp - order.queued_at).total_seconds()))))
            if any(fill.leg == "open" for fill in fills) and self._protections:
                # With unknown intratimestamp ordering and immediate activation,
                # entry then stop is possible. Do not silently discard that loss.
                adverse = low if working.side == "long" else high
                protection = self._protections[0]
                crossed = (adverse <= protection.stop_loss_price if working.side == "long"
                           else adverse >= protection.stop_loss_price)
                if crossed:
                    exits = self._fill_active_protections(
                        observation=self._observation(timestamp, adverse, identity), position=working)
                    bundles.extend(exits)
                    for bundle in exits:
                        for fill in bundle.fills:
                            working = _apply_position_shadow(working, fill)
        mark = low if working.side == "long" else high if working.side == "short" else low
        return tuple(bundles), self._observation(timestamp, mark, identity)

    def finish(self, timestamp, position):
        self.advance_clock(timestamp, position)
        if position.side is not None or self._pending or self._awaiting_ack or self._protections:
            raise ValueError("Unresolved exposure/orders at replay end; no synthetic force-flat price is allowed")

    def _observation(self, timestamp, price, identity):
        return CanonicalObservation(_stable_id(self.run_id, "raw", identity, timestamp), self.run_id,
                                    self.strategy.config.symbol, timestamp, 0, price, identity, timestamp, identity)

    def _fill(self, order, observation, *, side, qty, leg):
        fill = super()._fill(order, observation, side=side, qty=qty, leg=leg)
        self.fill_evidence[fill.stable_fill_id] = {
            "reference_price": str(observation.close_price),
            "slippage_dollars": str(abs(fill.price - observation.close_price) * fill.qty),
            "tp_price": (order.intent.exit_plan or {}).get("take_profit_limit_price"),
            "sl_price": (order.intent.exit_plan or {}).get("stop_loss_trigger_price"),
        }
        return fill

    def trade_evidence(self, accounting, fees):
        rows = []
        for trade in accounting.closed_trades:
            entry = self.fill_evidence[trade.entry_fill_id]
            exit_fill = self.fill_evidence[trade.exit_fill_id]
            fee = fees.trade_fees[trade.exit_fill_id]
            charge = fees.trade_friction.get(trade.exit_fill_id, Decimal("0"))
            reason = {"take_profit_broker_native": "TP", "stop_loss_broker_native": "SL",
                      "session_force_flat": "FORCE_FLAT"}.get(trade.reason, trade.reason)
            rows.append({"entry_timestamp": trade.entry_timestamp, "entry_price": str(trade.entry_price),
                         "direction": trade.side, "quantity": str(trade.qty),
                         "tp_price": entry["tp_price"], "sl_price": entry["sl_price"],
                         "exit_timestamp": trade.exit_timestamp, "exit_price": str(trade.exit_price),
                         "exit_reason": reason, "gross_pnl": str(trade.pnl), "fees": str(fee),
                         "pre_friction_gross_pnl": str(trade.pnl) if self.friction.application == "PER_FILL_CHARGE_V1" else None,
                         "execution_friction": str(charge),
                         "net_pnl": str(trade.pnl - fee - charge),
                         "hold_seconds": (trade.exit_timestamp - trade.entry_timestamp).total_seconds(),
                         "slippage_dollars": str(charge if self.friction.application == "PER_FILL_CHARGE_V1" else Decimal(entry["slippage_dollars"]) + Decimal(exit_fill["slippage_dollars"]))})
        return rows


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
