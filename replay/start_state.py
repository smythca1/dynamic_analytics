from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from silver.accounting import PositionState
from silver.events.models import utc_datetime

from .models import ReplayStartState


def load_replay_start_state(path: str | Path) -> ReplayStartState:
    source = Path(path)
    payload = json.loads(source.read_text(encoding="utf-8"))
    engine = payload.get("engine") if isinstance(payload.get("engine"), dict) else payload
    renko = engine.get("renko") if isinstance(engine.get("renko"), dict) else None
    trade = engine.get("trade") if isinstance(engine.get("trade"), dict) else {}
    position_payload = trade.get("position") if isinstance(trade.get("position"), dict) else None
    accounting_position = _accounting_position(position_payload)
    snapshot_timestamp = payload.get("saved_at") or payload.get("snapshot_timestamp")
    completeness = "complete" if renko and "last_brick_close" in renko and trade is not None else "incomplete"
    return ReplayStartState(
        snapshot_timestamp=utc_datetime(snapshot_timestamp) if snapshot_timestamp else None,
        trader_id=str(engine.get("trader_id") or payload.get("trader_id") or ""),
        symbol=str(engine.get("symbol") or payload.get("symbol") or ""),
        renko_state=renko,
        strategy_state=engine if isinstance(engine, dict) else None,
        broker_position_quantity=accounting_position.qty if accounting_position is not None else None,
        accounting_position=accounting_position,
        accounting_basis_status="known" if accounting_position is not None and accounting_position.basis_known else "UNKNOWN",
        pending_orders=tuple(payload.get("pending_orders") or ()),
        force_flat_state={"force_flat_dates": trade.get("force_flat_dates", [])} if isinstance(trade, dict) else None,
        source_references=(str(source),),
        completeness=completeness,
    )


def _accounting_position(position: dict[str, Any] | None) -> PositionState | None:
    if not position:
        return None
    side = position.get("side")
    qty = position.get("qty")
    entry_price = position.get("entry_price")
    if side not in {"long", "short"} or qty in {None, 0, "0"}:
        return PositionState.flat(str(position.get("symbol") or ""))
    return PositionState(
        symbol=str(position.get("symbol") or ""),
        side=side,
        qty=qty,
        avg_entry_price=entry_price,
        basis_known=entry_price is not None,
        entry_timestamp=position.get("entry_time"),
        entry_fill_id="production_start_state_snapshot",
    )
