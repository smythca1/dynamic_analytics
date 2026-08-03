from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ReplayLedgers:
    raw_source_trades: list[Any] = field(default_factory=list)
    eligibility_decisions: list[Any] = field(default_factory=list)
    eligible_trades: list[Any] = field(default_factory=list)
    canonical_observations: list[Any] = field(default_factory=list)
    renko_bricks: list[Any] = field(default_factory=list)
    strategy_signals: list[Any] = field(default_factory=list)
    simulated_orders: list[Any] = field(default_factory=list)
    simulated_fills: list[Any] = field(default_factory=list)
    position_transitions: list[Any] = field(default_factory=list)
    closed_trades: list[Any] = field(default_factory=list)
    accounting_snapshots: list[Any] = field(default_factory=list)
    equity_curve: list[Any] = field(default_factory=list)
    events: list[Any] = field(default_factory=list)
    diagnostics: list[Any] = field(default_factory=list)
