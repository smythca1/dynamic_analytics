"""Shared execution contracts for replay and live trading."""

from .close_contract import (
    AuthoritativePosition,
    CloseResolution,
    authoritative_position_from_alpaca,
    authoritative_position_from_accounting,
    resolve_close_intent,
)

__all__ = [
    "AuthoritativePosition",
    "CloseResolution",
    "authoritative_position_from_alpaca",
    "authoritative_position_from_accounting",
    "resolve_close_intent",
]
