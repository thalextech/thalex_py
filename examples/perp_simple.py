import asyncio
import json
import logging
import socket
import time
from typing import Optional
import websockets

import thalex
import keys  # Rename _keys.py to keys.py and add your keys. There are instructions how to create keys in that file.

NETWORK = thalex.Network.TEST
UNDERLYING = "BTCUSD"
PERP = "BTC-PERPETUAL"
TICK = 1  # USD
SPREAD = 25  # USD
AMEND_THRESHOLD = 5  # USD
SIZE = 0.1  # Number of contracts to quote

BID_ID = 1001
ASK_ID = 1002


def round_to_tick(value):
    return TICK * round(value / TICK)


def is_open(order: Optional[dict]) -> bool:
    return order is not None and (order.get("status") or "") in [
        "open",
        "partially_filled",
    ]


class PerpQuoter:
    def __init__(self, tlx: thalex.Thalex):
        self.tlx = tlx
        self.index: Optional[float] = None
        self.bid: Optional[dict] = None
        self.ask: Optional[dict] = None

    async def update_quotes(self):
        if self.index is None:
            return

        bid_price = round_to_tick(self.index - SPREAD)
        if is_open(self.bid):
            assert self.bid is not None
            if abs(self.bid["price"] - bid_price) > AMEND_THRESHOLD:
                logging.info(f"Amending bid to {bid_price}")
                await self.tlx.amend(
                    amount=SIZE, price=bid_price, client_order_id=BID_ID, id=BID_ID
                )
        else:
            logging.info(f"Inserting bid at {bid_price}")
            await self.tlx.insert(
                amount=SIZE,
                price=bid_price,
                direction=thalex.Direction.BUY,
                instrument_name=PERP,
                client_order_id=BID_ID,
                id=BID_ID,
            )
            self.bid = {"status": "open", "price": bid_price}

        ask_price = round_to_tick(self.index + SPREAD)
        if is_open(self.ask):
            assert self.ask is not None
            if abs(self.ask["price"] - ask_price) > AMEND_THRESHOLD:
                logging.info(f"Amending ask to {ask_price}")
                await self.tlx.amend(
                    amount=SIZE, price=ask_price, client_order_id=ASK_ID, id=ASK_ID
                )
        else:
            logging.info(f"Inserting ask at {ask_price}")
            await self.tlx.insert(
                amount=SIZE,
                price=ask_price,
                direction=thalex.Direction.SELL,
                instrument_name=PERP,
                client_order_id=ASK_ID,
                id=ASK_ID,
            )
            self.ask = {"status": "open", "price": ask_price}

    async def handle_notification(self, channel: str, notification):
        logging.debug(f"notificaiton in channel {channel} {notification}")
        if channel == "session.orders":
            for order in notification:
                if order["direction"] == "buy":
                    self.bid = order
                else:
                    self.ask = order
        elif channel.startswith("price_index"):
            self.index = notification["price"]
            await self.update_quotes()

    async def quote(self):
        await self.tlx.connect()
        await self.tlx.login(keys.key_ids[NETWORK], keys.private_keys[NETWORK])
        await self.tlx.set_cancel_on_disconnect(6)
        await self.tlx.public_subscribe([f"price_index.{UNDERLYING}"])
        await self.tlx.private_subscribe(["session.orders"])
        while True:
            msg = await self.tlx.receive()
            msg = json.loads(msg)
            if "channel_name" in msg:
                await self.handle_notification(msg["channel_name"], msg["notification"])
            elif "result" in msg:
                logging.debug(msg)
            else:
                logging.error(msg)
                await self.tlx.cancel_session()
                self.bid = None
                self.ask = None


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    )
    run = True  # We set this to false when we want to stop
    while run:
        tlx = thalex.Thalex(network=NETWORK)
        quoter = PerpQuoter(tlx)
        task = asyncio.create_task(quoter.quote())
        try:
            await task
        except (websockets.ConnectionClosed, socket.gaierror) as e:
            logging.error(f"Lost connection ({e}). Reconnecting...")
            time.sleep(0.1)
        except asyncio.CancelledError:
            logging.info("Cancelled")
            run = False
        except:
            logging.exception("There was an unexpected error:")
        if tlx.connected():
            await tlx.cancel_session()
            await tlx.disconnect()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


asyncio.run(main())
