from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any

from strategy_engine import MeanReversionStrategyConfig, RenkoStrategyConfig


class TraderConfigError(ValueError):
    """Raised when a trader JSON record cannot be mapped safely."""


@dataclass(frozen=True)
class BrickConfigAudit:
    sizing_mode: str
    authoritative_field: str
    effective_brick_dollars: Decimal
    configured_brick_pct: Decimal | None
    reference_price_policy: str
    reference_price: Decimal | None
    tolerance_pct_points: Decimal
    values_agree: bool | None
    name_explanation: str
    raw_fields: dict[str, Any]


@dataclass(frozen=True)
class LoadedProductionConfig:
    config_path: str
    trader: dict[str, Any]
    strategy_config: RenkoStrategyConfig
    config_hash: str
    metadata: dict[str, Any]
    brick_audit: BrickConfigAudit


def build_strategy_config(trader: dict[str, Any]) -> RenkoStrategyConfig | MeanReversionStrategyConfig:
    strategy = trader.get("strategy") if isinstance(trader.get("strategy"), dict) else {}
    session = trader.get("session") if isinstance(trader.get("session"), dict) else {}
    sizing = trader.get("sizing") if isinstance(trader.get("sizing"), dict) else {}

    strategy_type = str(strategy.get("type") or "renko_tp_sl")
    if strategy_type not in {"renko_tp_sl", "renko_psar", "renko_reversal", "chop_mean_reversion"}:
        raise TraderConfigError(f"{trader.get('id')}: unsupported strategy type {strategy_type}.")

    qty = as_float(sizing.get("qty"), default=0.0) or None
    notional = as_float(sizing.get("notional"), default=0.0) or None
    exit_management = str(strategy.get("exit_management") or "strategy").lower().strip()
    if exit_management == "oco":
        if qty is None:
            raise TraderConfigError(f"{trader.get('id')}: oco exit_management requires whole-share qty sizing.")
        if abs(qty - round(qty)) > 1e-9:
            raise TraderConfigError(f"{trader.get('id')}: Alpaca OCO does not support fractional qty sizing ({qty:g}).")

    if strategy_type == "chop_mean_reversion":
        return MeanReversionStrategyConfig(
            trader_id=str(trader["id"]),
            symbol=str(trader["symbol"]).upper(),
            window_seconds=int(strategy["window_seconds"]),
            entry_z=float(strategy["entry_z"]),
            exit_z=float(strategy.get("exit_z", 0.10)),
            stop_z=float(strategy["stop_z"]),
            efficiency_max=float(strategy["efficiency_max"]),
            trend_exit_efficiency=float(strategy["trend_exit_efficiency"]),
            max_hold_seconds=int(strategy["max_hold_seconds"]),
            allow_short=bool(strategy.get("allow_short", True)),
            strategy_name=str(trader.get("strategy_name") or trader["id"]),
            order_type=str(strategy.get("order_type") or "market").lower().strip(),
            qty=qty,
            notional=notional,
            timezone=str(session.get("timezone") or "America/New_York"),
            entry_start=session.get("entry_start"),
            entry_end=session.get("entry_end"),
            force_flat=session.get("force_flat"),
            active_start_date=session.get("active_start_date"),
            active_end_date=session.get("active_end_date"),
            allow_overnight=bool(session.get("allow_overnight", False)),
        )

    brick_size = effective_renko_brick_size(trader)
    return RenkoStrategyConfig(
        trader_id=str(trader["id"]),
        symbol=str(trader["symbol"]).upper(),
        strategy_type=strategy_type,  # type: ignore[arg-type]
        brick_size=float(brick_size),
        entry_bricks=int(strategy["entry_bricks"]),
        take_profit_bricks=int(strategy["take_profit_bricks"]) if strategy.get("take_profit_bricks") else None,
        stop_loss_bricks=int(strategy["stop_loss_bricks"]) if strategy.get("stop_loss_bricks") else None,
        psar_initial_stop_bricks=int(strategy["psar_initial_stop_bricks"]) if strategy.get("psar_initial_stop_bricks") else None,
        psar_af_step=float(strategy["psar_af_step"]) if strategy.get("psar_af_step") else None,
        psar_af_max=float(strategy["psar_af_max"]) if strategy.get("psar_af_max") else None,
        allow_short=bool(strategy.get("allow_short", True)),
        strategy_name=str(trader.get("strategy_name") or trader["id"]),
        order_type=str(strategy.get("order_type") or "market").lower().strip(),
        exit_management=exit_management,
        stop_loss_order_type=str(strategy.get("stop_loss_order_type") or "stop_market").lower().strip(),
        stop_limit_offset_pct=float(strategy.get("stop_limit_offset_pct") or 0.0),
        qty=qty,
        notional=notional,
        renko_anchor_mode=str(strategy.get("renko_anchor_mode") or strategy.get("anchor_mode") or "first_price"),
        renko_anchor_price=float(strategy["renko_anchor_price"])
        if strategy.get("renko_anchor_price") is not None
        else float(strategy["anchor_price"])
        if strategy.get("anchor_price") is not None
        else None,
        timezone=str(session.get("timezone") or "America/New_York"),
        entry_start=session.get("entry_start"),
        entry_end=session.get("entry_end"),
        force_flat=session.get("force_flat"),
        active_start_date=session.get("active_start_date"),
        active_end_date=session.get("active_end_date"),
        allow_overnight=bool(session.get("allow_overnight", False)),
    )


def load_production_renko_config(config_path: str | Path, trader_id: str) -> LoadedProductionConfig:
    path = Path(config_path)
    data = json.loads(path.read_text(encoding="utf-8"))
    trader = find_trader(data, trader_id)
    strategy_config = build_strategy_config(trader)
    if not isinstance(strategy_config, RenkoStrategyConfig):
        raise TraderConfigError(f"{trader_id}: PRODUCTION_CONFIG replay V1 supports RenkoStrategyConfig only.")
    brick_audit = audit_renko_brick_config(trader, strategy_config=strategy_config)
    metadata = production_metadata(path, trader_id, trader, strategy_config, brick_audit)
    compact = {
        "trader_id": trader_id,
        "strategy_config": asdict(strategy_config),
        "metadata": metadata,
        "brick_audit": asdict(brick_audit),
    }
    config_hash = sha256(json.dumps(compact, sort_keys=True, default=str).encode("utf-8")).hexdigest()
    return LoadedProductionConfig(str(path), trader, strategy_config, config_hash, metadata, brick_audit)


def audit_renko_brick_config(trader: dict[str, Any], *, strategy_config: RenkoStrategyConfig) -> BrickConfigAudit:
    strategy = trader.get("strategy") if isinstance(trader.get("strategy"), dict) else {}
    profile = trader.get("profile") if isinstance(trader.get("profile"), dict) else {}
    brick_size = _decimal_or_none(strategy.get("brick_size"))
    brick_pct = _decimal_or_none(strategy.get("brick_pct") or strategy.get("brick_size_ltp_percent") or profile.get("renko_brick_pct"))
    reference_price = _decimal_or_none(strategy.get("reference_price") or profile.get("ltp_reference"))
    sizing_mode = str(strategy.get("brick_sizing_mode") or profile.get("brick_sizing_mode") or "fixed_dollars").upper()
    if sizing_mode in {"FIXED", "FIXED_DOLLARS", "DOLLARS", "BRICK_DOLLARS"}:
        sizing_mode = "BRICK_DOLLARS"
    elif sizing_mode in {"PERCENT", "PCT", "BRICK_PERCENT"}:
        sizing_mode = "BRICK_PERCENT"
    else:
        raise TraderConfigError(f"{trader.get('id')}: unsupported brick sizing mode {sizing_mode}.")
    if sizing_mode == "BRICK_PERCENT":
        if brick_pct is None or reference_price in {None, Decimal("0")}:
            raise TraderConfigError(f"{trader.get('id')}: percentage-derived brick sizing requires brick percent and reference price.")
        effective = reference_price * brick_pct / Decimal("100")
        if brick_size is not None and abs(effective - brick_size) > Decimal("0.000001"):
            raise TraderConfigError(f"{trader.get('id')}: percentage-derived effective brick size disagrees with strategy.brick_size.")
        return BrickConfigAudit(
            sizing_mode=sizing_mode,
            authoritative_field="brick_pct + reference_price",
            effective_brick_dollars=effective,
            configured_brick_pct=brick_pct,
            reference_price_policy="explicit percentage-derived production contract",
            reference_price=reference_price,
            tolerance_pct_points=Decimal("0"),
            values_agree=True,
            name_explanation="Trader IDs/names are labels only; structured brick sizing fields are authoritative.",
            raw_fields={
                "strategy.brick_size": strategy.get("brick_size"),
                "strategy.brick_pct": strategy.get("brick_pct"),
                "strategy.brick_size_ltp_percent": strategy.get("brick_size_ltp_percent"),
                "profile.renko_brick_pct": profile.get("renko_brick_pct"),
                "profile.ltp_reference": profile.get("ltp_reference"),
                "strategy.brick_sizing_mode": strategy.get("brick_sizing_mode"),
                "profile.brick_sizing_mode": profile.get("brick_sizing_mode"),
                "trader_id": trader.get("id"),
                "strategy_name": trader.get("strategy_name"),
            },
        )
    if brick_size is None:
        raise TraderConfigError(f"{trader.get('id')}: fixed-dollar Renko strategy requires strategy.brick_size.")

    values_agree = None
    tolerance = Decimal("0.05")
    if brick_pct is not None and reference_price not in {None, Decimal("0")}:
        implied_pct = brick_size / reference_price * Decimal("100")
        values_agree = abs(implied_pct - brick_pct) <= tolerance
        if not values_agree:
            raise TraderConfigError(
                f"{trader.get('id')}: strategy.brick_size and profile renko_brick_pct disagree "
                f"({implied_pct} vs {brick_pct}, tolerance {tolerance} percentage points)."
            )

    return BrickConfigAudit(
        sizing_mode=sizing_mode,
        authoritative_field="strategy.brick_size",
        effective_brick_dollars=brick_size,
        configured_brick_pct=brick_pct,
        reference_price_policy="profile.ltp_reference is informational for fixed-dollar configs",
        reference_price=reference_price,
        tolerance_pct_points=tolerance,
        values_agree=values_agree,
        name_explanation="Trader IDs/names are labels only; structured strategy.brick_size is authoritative.",
        raw_fields={
            "strategy.brick_size": strategy.get("brick_size"),
            "profile.renko_brick_pct": profile.get("renko_brick_pct"),
            "profile.ltp_reference": profile.get("ltp_reference"),
            "strategy.brick_sizing_mode": strategy.get("brick_sizing_mode"),
            "profile.brick_sizing_mode": profile.get("brick_sizing_mode"),
            "trader_id": trader.get("id"),
            "strategy_name": trader.get("strategy_name"),
        },
    )


def effective_renko_brick_size(trader: dict[str, Any]) -> Decimal:
    strategy = trader.get("strategy") if isinstance(trader.get("strategy"), dict) else {}
    profile = trader.get("profile") if isinstance(trader.get("profile"), dict) else {}
    mode = str(strategy.get("brick_sizing_mode") or profile.get("brick_sizing_mode") or "fixed_dollars").upper()
    if mode in {"FIXED", "FIXED_DOLLARS", "DOLLARS", "BRICK_DOLLARS"}:
        brick_size = _decimal_or_none(strategy.get("brick_size"))
        if brick_size is None:
            raise TraderConfigError(f"{trader.get('id')}: fixed-dollar Renko strategy requires strategy.brick_size.")
        return brick_size
    if mode in {"PERCENT", "PCT", "BRICK_PERCENT"}:
        brick_pct = _decimal_or_none(strategy.get("brick_pct") or strategy.get("brick_size_ltp_percent") or profile.get("renko_brick_pct"))
        reference_price = _decimal_or_none(strategy.get("reference_price") or profile.get("ltp_reference"))
        if brick_pct is None or reference_price in {None, Decimal("0")}:
            raise TraderConfigError(f"{trader.get('id')}: percentage-derived brick sizing requires brick percent and reference price.")
        return reference_price * brick_pct / Decimal("100")
    raise TraderConfigError(f"{trader.get('id')}: unsupported brick sizing mode {mode}.")


def production_metadata(
    path: Path,
    trader_id: str,
    trader: dict[str, Any],
    strategy_config: RenkoStrategyConfig,
    brick_audit: BrickConfigAudit,
) -> dict[str, Any]:
    session = trader.get("session") if isinstance(trader.get("session"), dict) else {}
    data_block = trader.get("data") if isinstance(trader.get("data"), dict) else {}
    sizing = trader.get("sizing") if isinstance(trader.get("sizing"), dict) else {}
    return {
        "source_config_path": str(path),
        "selected_trader_id": trader_id,
        "strategy_type": strategy_config.strategy_type,
        "strategy_name": strategy_config.strategy_name,
        "configured_renko_source": normalize_renko_source(data_block.get("renko_source")),
        "brick_sizing_mode": brick_audit.sizing_mode,
        "authoritative_brick_field": brick_audit.authoritative_field,
        "configured_brick_size_dollars": float(brick_audit.effective_brick_dollars),
        "configured_brick_pct": float(brick_audit.configured_brick_pct) if brick_audit.configured_brick_pct is not None else None,
        "profile_ltp_reference": float(brick_audit.reference_price) if brick_audit.reference_price is not None else None,
        "reference_price_policy": brick_audit.reference_price_policy,
        "entry_bricks": strategy_config.entry_bricks,
        "reversal_behavior": "enabled for renko_reversal" if strategy_config.strategy_type == "renko_reversal" else "strategy-specific",
        "quantity": strategy_config.qty,
        "anchor_mode": strategy_config.renko_anchor_mode,
        "anchor_price": strategy_config.renko_anchor_price,
        "entry_start": strategy_config.entry_start,
        "entry_end": strategy_config.entry_end,
        "force_flat": strategy_config.force_flat,
        "allow_overnight": strategy_config.allow_overnight,
        "session_timezone": strategy_config.timezone,
        "session": session,
        "sizing": sizing,
        "brick_audit": asdict(brick_audit),
    }


def find_trader(data: dict[str, Any], trader_id: str) -> dict[str, Any]:
    for trader in data.get("traders", []):
        if isinstance(trader, dict) and trader.get("id") == trader_id:
            return trader
    raise TraderConfigError(f"Trader {trader_id} not found in production config.")


def normalize_renko_source(value: Any) -> str:
    raw = str(value or "ticks").lower().strip().replace("-", "_")
    if raw in {"tick", "ticks", "raw_ticks"}:
        return "ticks"
    if raw in {"1s", "1s_close", "one_second_close", "second_close", "one_second_close"}:
        return "1s_close"
    raise TraderConfigError(f"Unsupported renko_source: {value}")


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    return Decimal(str(value))
