from dataclasses import dataclass
import os
import re
import time

import requests
from fastapi import HTTPException

DEFAULT_ALPACA_LIVE_BASE_URL = "https://api.alpaca.markets"
DEFAULT_ALPACA_PAPER_BASE_URL = "https://paper-api.alpaca.markets"
MAX_CLIENT_ORDER_ID_LENGTH = 48


def child_client_order_id(parent_client_order_id: object, suffix: str) -> str:
    child_suffix = f"-{str(suffix).strip('-')}"
    parent = str(parent_client_order_id)
    if len(parent) + len(child_suffix) <= MAX_CLIENT_ORDER_ID_LENGTH:
        return f"{parent}{child_suffix}"

    unique_tail = parent.rsplit("-", 1)[-1] if "-" in parent else parent[-12:]
    unique_tail = unique_tail[-12:] or parent[-12:]
    tail_segment = f"-{unique_tail}"
    prefix_length = MAX_CLIENT_ORDER_ID_LENGTH - len(tail_segment) - len(child_suffix)
    prefix = parent[:max(0, prefix_length)].rstrip("-_")
    return f"{prefix}{tail_segment}{child_suffix}"[:MAX_CLIENT_ORDER_ID_LENGTH]


def _first_env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def _normalize_mode(mode: str | None) -> str:
    normalized = (mode or os.getenv("ALPACA_TRADING_MODE") or "paper").strip().lower()
    if normalized not in {"paper", "live"}:
        raise RuntimeError("ALPACA_TRADING_MODE must be 'paper' or 'live'")
    return normalized


def _normalize_profile(profile: str | None) -> str | None:
    if profile in {None, ""}:
        return None
    normalized = re.sub(r"[^A-Z0-9]+", "_", str(profile).strip().upper()).strip("_")
    if not normalized:
        raise RuntimeError("Alpaca credential profile must contain at least one alphanumeric character")
    return normalized


def _normalize_base_url(base_url: str) -> str:
    normalized = base_url.rstrip("/")
    if normalized.endswith("/v2"):
        normalized = normalized[:-3]
    return normalized


def _is_paper_base_url(base_url: str | None) -> bool:
    return bool(base_url) and "paper-api.alpaca.markets" in base_url.lower()


@dataclass(frozen=True)
class AlpacaCredentials:
    mode: str
    key_id: str
    secret_key: str
    base_url: str
    profile: str | None = None


def load_alpaca_credentials(mode: str | None = None, profile: str | None = None) -> AlpacaCredentials:
    resolved_mode = _normalize_mode(mode)
    resolved_profile = _normalize_profile(profile)

    if resolved_profile:
        key_id = _first_env(
            f"ALPACA_PROFILE_{resolved_profile}_API_KEY_ID",
            f"ALPACA_PROFILE_{resolved_profile}_API_KEY",
            f"APCA_PROFILE_{resolved_profile}_API_KEY_ID",
        )
        secret_key = _first_env(
            f"ALPACA_PROFILE_{resolved_profile}_API_SECRET_KEY",
            f"ALPACA_PROFILE_{resolved_profile}_SECRET_KEY",
            f"APCA_PROFILE_{resolved_profile}_API_SECRET_KEY",
        )
        base_url = (
            _first_env(f"ALPACA_PROFILE_{resolved_profile}_BASE_URL", f"APCA_PROFILE_{resolved_profile}_BASE_URL")
            or (DEFAULT_ALPACA_PAPER_BASE_URL if resolved_mode == "paper" else DEFAULT_ALPACA_LIVE_BASE_URL)
        )
        missing = []
        if not key_id:
            missing.append(f"ALPACA_PROFILE_{resolved_profile}_API_KEY_ID")
        if not secret_key:
            missing.append(f"ALPACA_PROFILE_{resolved_profile}_API_SECRET_KEY")
        if missing:
            raise RuntimeError(
                f"Missing Alpaca credentials for profile '{profile}'; set " + " and ".join(missing)
            )
        return AlpacaCredentials(
            mode=resolved_mode,
            key_id=key_id,
            secret_key=secret_key,
            base_url=_normalize_base_url(base_url),
            profile=resolved_profile,
        )

    if resolved_mode == "paper":
        key_id = _first_env(
            "APCA_PAPER_API_KEY_ID",
            "ALPACA_PAPER_API_KEY_ID",
            "ALPACA_PAPER_API_KEY",
            "APCA_API_KEY_ID",
            "ALPACA_API_KEY_ID",
            "ALPACA_API_KEY",
        )
        secret_key = _first_env(
            "APCA_PAPER_API_SECRET_KEY",
            "ALPACA_PAPER_API_SECRET_KEY",
            "ALPACA_PAPER_SECRET_KEY",
            "APCA_API_SECRET_KEY",
            "ALPACA_API_SECRET_KEY",
            "ALPACA_SECRET_KEY",
        )
        base_url = _first_env("ALPACA_PAPER_BASE_URL", "ALPACA_BASE_URL") or DEFAULT_ALPACA_PAPER_BASE_URL
    else:
        key_id = _first_env("APCA_API_KEY_ID", "ALPACA_API_KEY_ID")
        secret_key = _first_env("APCA_API_SECRET_KEY", "ALPACA_API_SECRET_KEY")
        live_base_url = _first_env("ALPACA_LIVE_BASE_URL")
        shared_base_url = _first_env("ALPACA_BASE_URL")
        if live_base_url:
            base_url = live_base_url
        elif shared_base_url and not _is_paper_base_url(shared_base_url):
            base_url = shared_base_url
        else:
            base_url = DEFAULT_ALPACA_LIVE_BASE_URL

    if resolved_mode == "live":
        missing = []
        if not key_id:
            missing.append("APCA_API_KEY_ID or ALPACA_API_KEY_ID")
        if not secret_key:
            missing.append("APCA_API_SECRET_KEY or ALPACA_API_SECRET_KEY")
        if missing:
            raise RuntimeError(
                "Missing explicit live Alpaca credentials for live trading; set "
                + " and ".join(missing)
            )
    else:
        missing = []
        if not key_id:
            missing.append("API key")
        if not secret_key:
            missing.append("API secret")
        if missing:
            raise RuntimeError(f"Missing Alpaca {' and '.join(missing)} for {resolved_mode} trading")

    return AlpacaCredentials(
        mode=resolved_mode,
        key_id=key_id,
        secret_key=secret_key,
        base_url=_normalize_base_url(base_url),
        profile=None,
    )


class AlpacaTrader:
    def __init__(self, credentials: AlpacaCredentials, max_notional: float):
        self.credentials = credentials
        self.max_notional = float(max_notional)

    def _headers(self) -> dict[str, str]:
        return {
            "APCA-API-KEY-ID": self.credentials.key_id,
            "APCA-API-SECRET-KEY": self.credentials.secret_key,
            "Content-Type": "application/json",
        }

    @staticmethod
    def _as_float(value, field_name: str, required: bool = False):
        if value is None:
            if required:
                raise HTTPException(status_code=400, detail=f"Missing {field_name}")
            return None
        try:
            return float(value)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"{field_name} must be numeric") from exc

    @staticmethod
    def _as_bool(value, default: bool = False) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}

    @staticmethod
    def _format_qty(value: float) -> str:
        return format(value, ".9f").rstrip("0").rstrip(".")

    @staticmethod
    def _format_price(value: float) -> str:
        return f"{float(value):.2f}"

    @staticmethod
    def _is_reversal_intent(action: object, reason: object, data: dict) -> bool:
        if AlpacaTrader._as_bool(data.get("reversal")):
            return True
        if str(action or "").lower().strip() == "reverse":
            return True
        reason_text = str(reason or "").lower()
        return "reverse" in reason_text or "reversal" in reason_text

    def _request(self, method: str, path: str, *, json: dict | None = None, params: dict | None = None):
        response = requests.request(
            method=method,
            url=f"{self.credentials.base_url}{path}",
            json=json,
            params=params,
            headers=self._headers(),
            timeout=20,
        )
        return response

    def _get_position(self, symbol: str) -> dict | None:
        response = self._request("GET", f"/v2/positions/{symbol}")
        if response.status_code == 404:
            return None
        if response.status_code >= 300:
            raise HTTPException(status_code=response.status_code, detail=response.text)
        return response.json()

    def get_account(self) -> dict:
        response = self._request("GET", "/v2/account")
        if response.status_code >= 300:
            raise HTTPException(status_code=response.status_code, detail=response.text)
        return response.json()

    def get_positions(self) -> list[dict]:
        response = self._request("GET", "/v2/positions")
        if response.status_code >= 300:
            raise HTTPException(status_code=response.status_code, detail=response.text)
        return response.json()

    def get_orders(self, status: str = "open", limit: int = 100) -> list[dict]:
        response = self._request(
            "GET",
            "/v2/orders",
            params={"status": status, "limit": int(limit), "direction": "desc", "nested": "false"},
        )
        if response.status_code >= 300:
            raise HTTPException(status_code=response.status_code, detail=response.text)
        return response.json()

    def get_order(self, order_id: str) -> dict:
        response = self._request("GET", f"/v2/orders/{order_id}")
        if response.status_code >= 300:
            raise HTTPException(status_code=response.status_code, detail=response.text)
        return response.json()

    def _cancel_open_orders(self, symbol: str, exclude_order_ids: set[str] | None = None) -> list[str]:
        excluded = exclude_order_ids or set()
        response = self._request(
            "GET",
            "/v2/orders",
            params={"status": "open", "limit": 500},
        )
        if response.status_code >= 300:
            raise HTTPException(status_code=response.status_code, detail=response.text)

        open_orders = response.json()
        cancelled_order_ids: list[str] = []
        for order in open_orders:
            if str(order.get("symbol", "")).upper() != symbol.upper():
                continue
            order_id = order.get("id")
            if not order_id:
                continue
            if str(order_id) in excluded:
                continue
            cancel_response = self._request("DELETE", f"/v2/orders/{order_id}")
            if cancel_response.status_code not in {200, 204, 404}:
                if cancel_response.status_code == 422 and "filled" in cancel_response.text.lower():
                    continue
                raise HTTPException(status_code=cancel_response.status_code, detail=cancel_response.text)
            cancelled_order_ids.append(order_id)

        return cancelled_order_ids

    def _submit_normalized_order(self, order: dict, action: str, reason: object, close_position: bool) -> dict:
        response = self._request("POST", "/v2/orders", json=order)

        print(
            "ALPACA NORMALIZED ORDER:",
            {
                "mode": self.credentials.mode,
                "base_url": self.credentials.base_url,
                "action": action,
                "reason": reason,
                "close_position": close_position,
                "order": order,
            },
        )
        print("ALPACA RESPONSE:", response.status_code, response.text)

        if response.status_code >= 300:
            raise HTTPException(status_code=response.status_code, detail=response.text)
        return response.json()

    def _wait_until_not_opposite_position(self, symbol: str, entry_side: str, timeout_seconds: float = 8.0) -> None:
        opposite_side = "short" if entry_side == "buy" else "long"
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            position = self._get_position(symbol)
            if position is None:
                return
            position_side = str(position.get("side", "")).lower().strip()
            if position_side != opposite_side:
                return
            position_qty = abs(self._as_float(position.get("qty"), "position qty", required=False) or 0.0)
            if position_qty <= 0:
                return
            time.sleep(0.15)

        raise HTTPException(
            status_code=409,
            detail=f"Timed out waiting for opposite {symbol} position to close before opening reversal leg.",
        )

    def _wait_until_entry_position(
        self,
        *,
        symbol: str,
        entry_side: str,
        order_id: str | None,
        timeout_seconds: float = 10.0,
    ) -> dict:
        expected_side = "long" if entry_side == "buy" else "short"
        deadline = time.monotonic() + timeout_seconds
        last_order: dict | None = None
        last_position: dict | None = None

        while time.monotonic() < deadline:
            if order_id:
                last_order = self.get_order(order_id)
                status = str(last_order.get("status", "")).lower().strip()
                if status in {"canceled", "expired", "rejected", "suspended"}:
                    raise HTTPException(status_code=409, detail=f"Entry order ended with status {status}: {last_order}")

            position = self._get_position(symbol)
            if position is not None:
                last_position = position
                position_side = str(position.get("side", "")).lower().strip()
                position_qty = abs(self._as_float(position.get("qty"), "position qty", required=False) or 0.0)
                if position_side == expected_side and position_qty > 0:
                    return position
            time.sleep(0.15)

        raise HTTPException(
            status_code=409,
            detail={
                "error": "Timed out waiting for entry position before submitting OCO exits.",
                "symbol": symbol,
                "entry_side": entry_side,
                "last_order": last_order,
                "last_position": last_position,
            },
        )

    def submit_oco_exit_order(
        self,
        *,
        symbol: str,
        entry_side: str,
        take_profit_limit_price: float,
        stop_loss_stop_price: float,
        stop_loss_limit_price: float | None = None,
        take_profit_offset: float | None = None,
        stop_loss_offset: float | None = None,
        client_order_id: str | None = None,
        entry_order_id: str | None = None,
    ) -> dict:
        position = self._wait_until_entry_position(
            symbol=symbol,
            entry_side=entry_side,
            order_id=entry_order_id,
        )
        position_side = str(position.get("side", "")).lower().strip()
        qty = abs(self._as_float(position.get("qty_available") or position.get("qty"), "position qty", required=True) or 0.0)
        if qty <= 0:
            raise HTTPException(status_code=409, detail=f"No available {symbol} position quantity for OCO exits.")

        exit_side = "sell" if position_side == "long" else "buy"
        avg_entry_price = self._as_float(position.get("avg_entry_price"), "avg_entry_price", required=False)
        bracket_base_price = avg_entry_price if avg_entry_price and avg_entry_price > 0 else None
        resolved_take_profit_limit_price = take_profit_limit_price
        resolved_stop_loss_stop_price = stop_loss_stop_price
        resolved_stop_loss_limit_price = stop_loss_limit_price
        if bracket_base_price is not None and take_profit_offset is not None and stop_loss_offset is not None:
            stop_limit_gap = 0.0
            if stop_loss_limit_price is not None:
                if position_side == "long":
                    stop_limit_gap = max(0.0, stop_loss_stop_price - stop_loss_limit_price)
                else:
                    stop_limit_gap = max(0.0, stop_loss_limit_price - stop_loss_stop_price)
            if position_side == "long":
                resolved_take_profit_limit_price = bracket_base_price + abs(float(take_profit_offset))
                resolved_stop_loss_stop_price = bracket_base_price - abs(float(stop_loss_offset))
                if stop_loss_limit_price is not None:
                    resolved_stop_loss_limit_price = resolved_stop_loss_stop_price - stop_limit_gap
            else:
                resolved_take_profit_limit_price = bracket_base_price - abs(float(take_profit_offset))
                resolved_stop_loss_stop_price = bracket_base_price + abs(float(stop_loss_offset))
                if stop_loss_limit_price is not None:
                    resolved_stop_loss_limit_price = resolved_stop_loss_stop_price + stop_limit_gap

        excluded_order_ids = {str(entry_order_id)} if entry_order_id else None
        cancelled_order_ids = self._cancel_open_orders(symbol, exclude_order_ids=excluded_order_ids)
        order = {
            "symbol": symbol,
            "side": exit_side,
            "type": "limit",
            "time_in_force": "day",
            "qty": self._format_qty(qty),
            "order_class": "oco",
            "take_profit": {
                "limit_price": self._format_price(resolved_take_profit_limit_price),
            },
            "stop_loss": {
                "stop_price": self._format_price(resolved_stop_loss_stop_price),
            },
        }
        if resolved_stop_loss_limit_price is not None:
            order["stop_loss"]["limit_price"] = self._format_price(resolved_stop_loss_limit_price)
        if client_order_id:
            order["client_order_id"] = str(client_order_id)[:48]

        response = self._submit_normalized_order(
            order,
            action="oco_exit",
            reason="broker_managed_tp_sl",
            close_position=True,
        )
        return {
            "status": "oco exit sent",
            "broker": "alpaca",
            "mode": self.credentials.mode,
            "symbol": symbol,
            "position_side": position_side,
            "avg_entry_price": bracket_base_price,
            "qty": self._format_qty(qty),
            "cancelled_order_ids": cancelled_order_ids,
            "order": order,
            "response": response,
        }

    def _split_reversal_order_if_needed(
        self,
        *,
        symbol: str,
        side: str,
        qty: float | None,
        target_qty: float | None,
        order_type: str,
        limit_price: float | None,
        action: str,
        reason: object,
        client_order_id: object,
        reversal_intent: bool,
    ) -> dict | None:
        if qty is None and target_qty is None:
            return None

        position = self._get_position(symbol)
        if position is None:
            return None

        position_side = str(position.get("side", "")).lower().strip()
        opposite_position = (side == "buy" and position_side == "short") or (
            side == "sell" and position_side == "long"
        )
        if not opposite_position:
            return None

        available_qty = abs(
            self._as_float(
                position.get("qty_available") or position.get("qty"),
                "position qty",
                required=True,
            )
            or 0.0
        )
        if available_qty <= 0:
            return None

        if target_qty is not None:
            opening_qty = target_qty
        elif qty is not None and qty > available_qty:
            opening_qty = qty - available_qty
        elif reversal_intent and qty is not None:
            opening_qty = qty
        else:
            return None

        if opening_qty <= 0:
            return None

        cancelled_order_ids = self._cancel_open_orders(symbol)
        close_order = {
            "symbol": symbol,
            "side": side,
            "time_in_force": "day",
            "type": "market",
            "qty": self._format_qty(available_qty),
        }
        open_order = {
            "symbol": symbol,
            "side": side,
            "time_in_force": "day",
            "qty": self._format_qty(opening_qty),
        }

        alpaca_order_type = "limit" if order_type == "limit" and limit_price is not None else "market"
        if alpaca_order_type == "limit":
            open_order["type"] = "limit"
            open_order["limit_price"] = str(limit_price)
        else:
            open_order["type"] = "market"

            if client_order_id:
                close_order["client_order_id"] = child_client_order_id(client_order_id, "cls")
                open_order["client_order_id"] = child_client_order_id(client_order_id, "opn")

        close_response = self._submit_normalized_order(
            close_order,
            action=action,
            reason=f"{reason}_reversal_close",
            close_position=True,
        )
        self._wait_until_not_opposite_position(symbol, side)
        open_response = self._submit_normalized_order(
            open_order,
            action=action,
            reason=f"{reason}_reversal_open",
            close_position=False,
        )
        return {
            "status": "split reversal sent",
            "broker": "alpaca",
            "mode": self.credentials.mode,
            "action": action,
            "reason": reason,
            "symbol": symbol,
            "broker_position_qty_closed": self._format_qty(available_qty),
            "target_opening_qty": self._format_qty(opening_qty),
            "cancelled_order_ids": cancelled_order_ids,
            "close_order": close_order,
            "open_order": open_order,
            "close_response": close_response,
            "open_response": open_response,
        }

    def submit_order(self, data: dict, action: str, side: str) -> dict:
        symbol = data.get("symbol")
        if not symbol:
            raise HTTPException(status_code=400, detail="Missing symbol")

        reason = data.get("reason")
        client_order_id = data.get("client_order_id")
        limit_price = self._as_float(data.get("limit_price"), "limit_price", required=False)
        order_type = str(data.get("order_type", "market")).lower().strip()
        close_position = self._as_bool(data.get("close_position"))
        is_exit_action = action in {"tp", "sl", "close"} or (action == "sell" and close_position)

        if is_exit_action:
            cancelled_order_ids = self._cancel_open_orders(symbol)
            position = self._get_position(symbol)
            if position is None:
                return {
                    "status": "no open position",
                    "broker": "alpaca",
                    "mode": self.credentials.mode,
                    "action": action,
                    "reason": reason,
                    "symbol": symbol,
                    "cancelled_order_ids": cancelled_order_ids,
                }

            position_qty = self._as_float(position.get("qty"), "position qty", required=True)
            if position_qty is None or abs(position_qty) <= 0:
                return {
                    "status": "no open position",
                    "broker": "alpaca",
                    "mode": self.credentials.mode,
                    "action": action,
                    "reason": reason,
                    "symbol": symbol,
                    "cancelled_order_ids": cancelled_order_ids,
                }

            position_side = str(position.get("side", "")).lower().strip()
            exit_side = "buy" if position_side == "short" else "sell"
            alpaca_order_type = "limit" if order_type == "limit" and limit_price is not None else "market"

            order = {
                "symbol": symbol,
                "side": exit_side,
                "time_in_force": "day",
                "qty": self._format_qty(abs(position_qty)),
            }

            if alpaca_order_type == "limit":
                order["type"] = "limit"
                order["limit_price"] = str(limit_price)
            else:
                order["type"] = "market"

            if client_order_id:
                order["client_order_id"] = str(client_order_id)
        else:
            notional = self._as_float(data.get("notional"), "notional", required=False)
            qty = self._as_float(data.get("qty"), "qty", required=False)
            target_qty = self._as_float(
                data.get("target_qty") or data.get("final_qty") or data.get("desired_qty"),
                "target_qty",
                required=False,
            )
            reversal_intent = self._is_reversal_intent(action, reason, data)

            if qty is None and notional is None and target_qty is None:
                raise HTTPException(status_code=400, detail="Either qty, target_qty, or notional is required")

            if qty is not None and qty <= 0:
                raise HTTPException(status_code=400, detail="qty must be greater than 0")

            if target_qty is not None and target_qty <= 0:
                raise HTTPException(status_code=400, detail="target_qty must be greater than 0")

            if notional is not None and (notional <= 0 or notional > self.max_notional):
                raise HTTPException(
                    status_code=400,
                    detail=f"notional must be between 0 and {self.max_notional}",
                )

            alpaca_order_type = "market"
            if order_type == "limit" and limit_price is not None:
                alpaca_order_type = "limit"

            split_response = self._split_reversal_order_if_needed(
                symbol=str(symbol),
                side=side,
                qty=qty,
                target_qty=target_qty,
                order_type=order_type,
                limit_price=limit_price,
                action=action,
                reason=reason,
                client_order_id=client_order_id,
                reversal_intent=reversal_intent,
            )
            if split_response is not None:
                return split_response

            order = {
                "symbol": symbol,
                "side": side,
                "time_in_force": "day",
            }

            if alpaca_order_type == "limit":
                order["type"] = "limit"
                order["limit_price"] = str(limit_price)
            else:
                order["type"] = "market"

            order_qty = target_qty if reversal_intent and target_qty is not None else qty
            if order_qty is None and target_qty is not None:
                order_qty = target_qty

            if order_qty is not None:
                order["qty"] = self._format_qty(order_qty)
            else:
                order["notional"] = str(notional)

            if client_order_id:
                order["client_order_id"] = str(client_order_id)

        order_response = self._submit_normalized_order(
            order,
            action=action,
            reason=reason,
            close_position=close_position,
        )

        return {
            "status": "order sent",
            "broker": "alpaca",
            "mode": self.credentials.mode,
            "action": action,
            "reason": reason,
            "order": order,
            "response": order_response,
        }
