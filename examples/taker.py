import asyncio
import json
import logging
import os
import signal
from typing import Dict, Optional

import thalex as th
import keys  # Rename _keys.py to keys.py and add your keys

# This example is a simple taker for a single instrument.
# We define an instrument and an acceptable pnl. If the top of book is in cross
# with the mark by the desired pnl (or is away from the mark by accepted pnl in case it's negative),
# we insert an immediate or cancel order and wait for its result.
# We keep doing this until our position in the given instrument is as desired.


# If the top of book is in cross with the mark by at least this much, we insert an ioc order
# Can be negative
PNL = -100
INSTRUMENT = "ETH-PERPETUAL"  # Name of the instrument we'll try to take
# To identify the orders and trades of this taker.
# If you run multiple in bots parallel, you should give them different labels.
LABEL = "TKR"
DESIRED_POSITION = 10.5  # We'll keep taking until we get this position.
NETWORK = th.Network.TEST

# We'll use these to match responses from thalex to the corresponding request.
# The numbers are arbitrary, but they need to be unique per CALL_ID.
CALL_ID_SET_COD = 1
CALL_ID_SUBSCRIBE = 2
CALL_ID_LOGIN = 3
CALL_ID_ORDER = 4


# Partial representation of an instrument ticker
class Ticker:
    def __init__(self, data: Dict):
        self.mark_price: float = data["mark_price"]
        self.best_bid: float = data.get("best_bid_price")
        self.best_ask: float = data.get("best_ask_price")
        self.mark_ts: float = data["mark_timestamp"]


class Taker:
    def __init__(self, network: th.Network):
        self.network: th.Network = network
        self.thalex: th.Thalex = th.Thalex(self.network)
        # >0 means we want to buy, <0 means we want to sell
        self.remaining_amount: Optional[float] = None
        # After inserting an ioc order, we wait for its result to avoid taking twice
        self.order_in_flight: bool = False

    async def insert_order(self, direction: th.Direction, price: float):
        self.order_in_flight = True
        await self.thalex.insert(
            direction=direction,
            instrument_name=INSTRUMENT,
            amount=abs(self.remaining_amount),
            price=price,
            label=LABEL,
            time_in_force=th.TimeInForce.IOC,  # immediate or cancel
            id=CALL_ID_ORDER,
        )

    async def handle_ticker(self, ticker: Ticker):
        if self.order_in_flight:
            # we already inserted an order and we don't know its result yet
            return
        if self.remaining_amount is None:
            # we have to wait for the portfolio subscription to know how much we have to take
            return
        if self.remaining_amount > 0:
            direction = th.Direction.BUY
            price = ticker.best_ask
            if price is not None and price < ticker.mark_price - PNL:
                logging.info(f"Buying {self.remaining_amount:.2f}@{price} mark: {ticker.mark_price:.1f}")
                await self.insert_order(direction, price)
        elif self.remaining_amount < 0:
            direction = th.Direction.SELL
            price = ticker.best_bid
            if price is not None and price > ticker.mark_price + PNL:
                logging.info(f"Selling {self.remaining_amount:.2f}@{price} mark: {ticker.mark_price:.1f}")
                await self.insert_order(direction, price)
        else:
            logging.info("Portfolio position is already as desired")
            return

    async def take(self):
        await self.thalex.connect()
        await self.thalex.login(
            keys.key_ids[self.network],
            keys.private_keys[self.network],
            id=CALL_ID_LOGIN,
        )
        await self.thalex.set_cancel_on_disconnect(6, id=CALL_ID_SET_COD)
        await self.thalex.public_subscribe([f"ticker.{INSTRUMENT}.raw"], id=CALL_ID_SUBSCRIBE)
        # We will also monitor the portfolio channel, in case our position changes as a result
        # of trades not originating from this taker.
        await self.thalex.private_subscribe([f"account.portfolio"], id=CALL_ID_SUBSCRIBE)

        while True:
            msg = await self.thalex.receive()
            msg = json.loads(msg)
            channel = msg.get("channel_name")
            if channel is not None:
                if channel.startswith("ticker."):
                    await self.handle_ticker(Ticker(msg["notification"]))
                elif channel == "account.portfolio":
                    for position in msg["notification"]:
                        if position["instrument_name"] == INSTRUMENT:
                            self.remaining_amount = DESIRED_POSITION - position["position"]
                            if self.remaining_amount == 0:
                                logging.info("Portfolio position is as desired")
                                await self.thalex.disconnect()
                                return
                    if self.remaining_amount is None:
                        self.remaining_amount = DESIRED_POSITION
            elif "result" in msg:
                id = msg["id"]
                if id is None:
                    logging.info(f"result to unknown request: {msg['result']}")
                elif id == CALL_ID_LOGIN:
                    logging.info(f"login result: {msg['result']}")
                elif id == CALL_ID_SUBSCRIBE:
                    logging.info(f"subscribed to: {msg['result']}")
                elif id == CALL_ID_SET_COD:
                    logging.info(f"set cancel on disconnect result: {msg['result']}")
                elif id == CALL_ID_ORDER:
                    result = msg["result"]
                    logging.info(f"order result: {result}")
                    self.order_in_flight = False
                    fill = result.get("filled_amount", 0)
                    if self.remaining_amount < 0:
                        self.remaining_amount += fill
                    elif self.remaining_amount > 0:
                        self.remaining_amount -= fill
                    status = result.get("status", "")
                    if status == "filled":
                        logging.info("take order fully filled")
                        await self.thalex.disconnect()
                        return
                else:
                    logging.info(f"result with unknown id({id}): {msg['result']}")
            else:
                logging.error(msg)


# This is what we want to happen when we get a signal from Ctrl+C or kill (cancel the task and shut down gracefully)
def handle_signal(evt_loop, task):
    logging.info("Signal received, stopping...")
    evt_loop.remove_signal_handler(signal.SIGTERM)
    evt_loop.remove_signal_handler(signal.SIGINT)
    task.cancel()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    )
    taker = Taker(NETWORK)
    loop = asyncio.get_event_loop()
    main_task = loop.create_task(taker.take())

    if os.name != "nt":  # Non-Windows platforms
        loop.add_signal_handler(signal.SIGTERM, handle_signal, loop, main_task)
        loop.add_signal_handler(signal.SIGINT, handle_signal, loop, main_task)
    try:
        loop.run_until_complete(main_task)
    finally:
        loop.close()


if __name__ == "__main__":
    main()
