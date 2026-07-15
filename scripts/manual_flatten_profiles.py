import json
import time

from alpaca_trader import AlpacaTrader, load_alpaca_credentials


PROFILES = [
    ("default", None),
    ("megatron", "megatron"),
    ("collatron", "collatron"),
]


def main() -> None:
    out = {}
    for label, profile in PROFILES:
        credentials = load_alpaca_credentials("live", profile=profile)
        trader = AlpacaTrader(credentials, max_notional=1_000_000)
        before_positions = trader.get_positions()
        before_orders = trader.get_orders("open", 100)
        actions = []

        for order in before_orders:
            order_id = order.get("id")
            if not order_id:
                continue
            response = trader._request("DELETE", f"/v2/orders/{order_id}")
            actions.append(
                {
                    "cancel_order": order_id,
                    "symbol": order.get("symbol"),
                    "status_code": response.status_code,
                    "response": response.text,
                }
            )

        time.sleep(0.5)

        for position in before_positions:
            symbol = position.get("symbol")
            if not symbol:
                continue
            position_side = str(position.get("side", "")).lower()
            exit_side = "sell" if position_side == "long" else "buy"
            response = trader.submit_order(
                {
                    "symbol": symbol,
                    "close_position": True,
                    "client_order_id": f"manual_flat_{label}_{symbol}_{int(time.time())}",
                    "reason": "manual_flat_start_fresh",
                },
                "close",
                exit_side,
            )
            actions.append({"close_position": symbol, "response": response})

        time.sleep(1.0)
        out[label] = {
            "before_positions": before_positions,
            "before_open_orders": before_orders,
            "actions": actions,
            "after_positions": trader.get_positions(),
            "after_open_orders": trader.get_orders("open", 100),
        }

    print(json.dumps(out, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
