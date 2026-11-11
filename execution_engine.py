from __future__ import annotations

import json
from pathlib import Path
from typing import Protocol

import pandas as pd

from alpaca_trader import AlpacaTrader, child_client_order_id, load_alpaca_credentials
from trading_events import ExecutionEvent, OrderIntent, dataclass_to_jsonable


class ExecutionEngine(Protocol):
    def execute(self, intent: OrderIntent) -> ExecutionEvent:
        ...


class JsonlEventLog:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, event_type: str, payload: dict) -> None:
        record = {
            "event_type": event_type,
            "logged_at": pd.Timestamp.now(tz="UTC").isoformat(),
            "payload": payload,
        }
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")


class ShadowExecutionEngine:
    def __init__(self, log: JsonlEventLog | None = None) -> None:
        self.log = log

    def execute(self, intent: OrderIntent) -> ExecutionEvent:
        event = ExecutionEvent(
            timestamp=pd.Timestamp.now(tz="UTC"),
            mode="shadow",
            broker="none",
            status="shadow_recorded",
            intent=intent,
            response=None,
        )
        if self.log:
            self.log.write("execution", dataclass_to_jsonable(event))
        return event


class AlpacaExecutionEngine:
    def __init__(
        self,
        mode: str | None = None,
        credential_profile: str | None = None,
        account_namespace: str | None = None,
        expected_account_id: str | None = None,
        max_notional: float = 250000,
        log: JsonlEventLog | None = None,
    ) -> None:
        self.trader = AlpacaTrader(
            credentials=load_alpaca_credentials(mode, profile=credential_profile),
            max_notional=max_notional,
        )
        self.credential_profile = self.trader.credentials.profile
        self.account_namespace = _normalize_namespace(account_namespace or credential_profile)
        self.expected_account_id = str(expected_account_id) if expected_account_id else None
        self._verified_account_id: str | None = None
        self.log = log

    def execute(self, intent: OrderIntent) -> ExecutionEvent:
        self._verify_execution_account(intent)
        action = "close" if intent.close_position else intent.side
        payload: dict[str, object] = {
            "symbol": intent.symbol,
            "side": intent.side,
            "order_type": intent.order_type,
            "reason": intent.reason,
            "client_order_id": intent.client_order_id,
            "timestamp": intent.timestamp,
            "signal_type": intent.signal_type,
            "close_position": intent.close_position,
            "strategy": intent.strategy,
            "strategy_type": intent.strategy_type,
        }
        if intent.trader_id:
            payload["trader_id"] = intent.trader_id
        if intent.account_namespace:
            payload["account_namespace"] = intent.account_namespace
        if intent.signal_id:
            payload["signal_id"] = intent.signal_id
        if intent.lifecycle_state:
            payload["lifecycle_state"] = intent.lifecycle_state
        if intent.qty is not None:
            payload["qty"] = intent.qty
        if intent.target_qty is not None:
            payload["target_qty"] = intent.target_qty
        if intent.notional is not None:
            payload["notional"] = intent.notional
        if intent.limit_price is not None:
            payload["limit_price"] = intent.limit_price
        if intent.stop_price is not None:
            payload["stop_price"] = intent.stop_price
        if intent.stop_limit_price is not None:
            payload["stop_limit_price"] = intent.stop_limit_price
        if intent.exit_management:
            payload["exit_management"] = intent.exit_management
        if intent.stop_loss_order_type:
            payload["stop_loss_order_type"] = intent.stop_loss_order_type
        if intent.take_profit_limit_price is not None:
            payload["take_profit_limit_price"] = intent.take_profit_limit_price
        if intent.stop_loss_stop_price is not None:
            payload["stop_loss_stop_price"] = intent.stop_loss_stop_price
        if intent.stop_loss_limit_price is not None:
            payload["stop_loss_limit_price"] = intent.stop_loss_limit_price
        if intent.take_profit_offset is not None:
            payload["take_profit_offset"] = intent.take_profit_offset
        if intent.stop_loss_offset is not None:
            payload["stop_loss_offset"] = intent.stop_loss_offset
        if intent.exit_plan is not None:
            payload["exit_plan"] = intent.exit_plan
        if intent.brick_size is not None:
            payload["brick_size"] = intent.brick_size
        if intent.catastrophic_stop_bricks is not None:
            payload["catastrophic_stop_bricks"] = intent.catastrophic_stop_bricks
        if intent.protection_type:
            payload["protection_type"] = intent.protection_type
        if intent.candidate_config_fingerprint:
            payload["candidate_config_fingerprint"] = intent.candidate_config_fingerprint
        if intent.market_contract_fingerprint:
            payload["market_contract_fingerprint"] = intent.market_contract_fingerprint

        if getattr(getattr(self.trader, "credentials", None), "mode", None) == "live":
            # Promotion gates may hold the deployed adapter in sys.modules while
            # inspecting a candidate release. Resolve execution-only validation
            # when executing an intent, not while importing candidate code.
            from alpaca_trader import validate_live_tp_sl_entry
            validate_live_tp_sl_entry(payload)
        response = self.trader.submit_order(payload, action=action, side=intent.side)
        if (
            intent.signal_type == "entry"
            and intent.exit_management == "oco"
            and intent.take_profit_limit_price is not None
            and intent.stop_loss_stop_price is not None
        ):
            response = dict(response)
            response["oco_exit_response"] = self.trader.submit_oco_exit_order(
                symbol=intent.symbol,
                entry_side=intent.side,
                take_profit_limit_price=float(intent.take_profit_limit_price),
                stop_loss_stop_price=float(intent.stop_loss_stop_price),
                stop_loss_limit_price=float(intent.stop_loss_limit_price)
                if intent.stop_loss_limit_price is not None
                else None,
                take_profit_offset=float(intent.take_profit_offset)
                if intent.take_profit_offset is not None
                else None,
                stop_loss_offset=float(intent.stop_loss_offset)
                if intent.stop_loss_offset is not None
                else None,
                client_order_id=child_client_order_id(intent.client_order_id, "oco"),
                entry_order_id=str(response.get("response", {}).get("id") or "") or None,
            )
        if (
            intent.signal_type == "entry"
            and intent.exit_management == "catastrophic_stop"
            and intent.catastrophic_stop_bricks is not None
        ):
            response = dict(response)
            if intent.brick_size is None:
                raise RuntimeError("catastrophic_stop entry requires intent.brick_size")
            entry_order_id = (
                str(response.get("response", {}).get("id") or "")
                or str(response.get("open_response", {}).get("id") or "")
                or None
            )
            response["catastrophic_stop_response"] = self.trader.submit_reversal_catastrophic_stop(
                symbol=intent.symbol,
                entry_side=intent.side,
                brick_size=float(intent.brick_size),
                catastrophic_stop_bricks=int(intent.catastrophic_stop_bricks),
                client_order_id=intent.client_order_id,
                entry_order_id=entry_order_id,
                trader_id=intent.trader_id,
                strategy_id=intent.strategy,
                candidate_config_fingerprint=intent.candidate_config_fingerprint,
                market_contract_fingerprint=intent.market_contract_fingerprint,
            )
        event = ExecutionEvent(
            timestamp=pd.Timestamp.now(tz="UTC"),
            mode="real",
            broker="alpaca",
            status=str(response.get("status", "submitted")),
            intent=intent,
            response=response,
        )
        if self.log:
            self.log.write("execution", dataclass_to_jsonable(event))
        return event

    def _verify_execution_account(self, intent: OrderIntent) -> None:
        intent_namespace = _normalize_namespace(intent.account_namespace)
        engine_namespace = getattr(self, "account_namespace", None)
        if intent_namespace and engine_namespace and intent_namespace != engine_namespace:
            raise RuntimeError(
                f"Execution account namespace mismatch: intent={intent_namespace} engine={engine_namespace}"
            )
        expected_account_id = getattr(self, "expected_account_id", None)
        if not expected_account_id:
            return
        if getattr(self, "_verified_account_id", None) == expected_account_id:
            return
        account = self.trader.get_account()
        account_id = str(account.get("id") or "")
        if account_id != expected_account_id:
            raise RuntimeError(
                f"Execution account identity mismatch for namespace {engine_namespace}: "
                f"expected {expected_account_id}, got {account_id or 'UNKNOWN'}"
            )
        self._verified_account_id = account_id


def _normalize_namespace(value: object) -> str | None:
    raw = str(value or "").strip().lower()
    return raw or None
