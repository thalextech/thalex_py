import asyncio
import json
import logging
import socket
import sys
import time
from typing import Optional
import websockets

import thalex
from thalex.thalex import Direction
import keys  # Rename _keys.py to keys.py and add your keys. There are instructions how to create keys in that file.


def fr_to_price(fr, index):
    if fr > 0:
        return (1.00025 + fr) * index
    if fr == 0:
        return index
    return (0.99975 + fr) * index


# Static cfg
NETWORK = thalex.Network.TEST
ORDER_LABEL = "rebalancer"
INSTRUMENT = "BTC-PERPETUAL"
INDEX = "BTCUSD"
PRICE_TICK = 1
SIZE_TICK = 0.001
QUOTE_ID = {Direction.BUY: 1001, Direction.SELL: 1002}
EXIT_QUOTE_ID = 1003

# Quoting cfg
MAX_POS = 0.02
MIN_POS = -0.02
TGT_POS = 0
SIZE = 0.005  # Number of contracts to quote
AMEND_THRESHOLD = 5  # USD
THEO_FR = 0.1 / (365.25 * 3)  # 10% annualized
ASK_OFFSET = 50  # $
BID_OFFSET = 50  # $
EXIT_OFFSET = 0  # $

CID_LOGIN = 1
CID_CANCEL = 2
CID_SET_COD = 3
CID_PUBSUB = 4
CID_PRIVSUB = 5


def round_to_tick(value):
    return PRICE_TICK * round(value / PRICE_TICK)


def round_size(size):
    return SIZE_TICK * round(size / SIZE_TICK)


class Quotes:
    def __init__(self, position, mark, index):
        self.theo = fr_to_price(THEO_FR, index)
        self.bid_price = round_to_tick(self.theo - BID_OFFSET)
        if self.bid_price < mark:
            self.bid_amt = round_size(max(min(SIZE, MAX_POS - position), 0))
        else:
            self.bid_amt = 0
        self.ask_price = round_to_tick(self.theo + ASK_OFFSET)
        if self.ask_price > mark:
            self.ask_amt = round_size(max(min(SIZE, -MIN_POS + position), 0))
        else:
            self.ask_amt = 0

        self.r_amt = round_size(TGT_POS - position)
        if self.r_amt > 0:
            self.r_side = Direction.BUY
            self.r_price = round_to_tick(self.theo - EXIT_OFFSET)
        else:
            self.r_side = Direction.SELL
            self.r_price = round_to_tick(self.theo + EXIT_OFFSET)


class Quoter:
    def __init__(self, tlx: thalex.Thalex):
        self.tlx = tlx
        self.index: Optional[float] = None
        self.quotes: dict[thalex.Direction, Optional[dict]] = {
            Direction.BUY: {},
            Direction.SELL: {},
        }
        self.exit_quote: dict = {}
        self.position: Optional[float] = None
        self.tob: list[Optional[float]] = [None, None]
        self.fr: Optional[float] = None
        self.mark: Optional[float] = None
        self.subs = []

    async def adjust_order(
        self, side: Direction, price: float, amount: float, confirmed, quote_id: int
    ):
        assert confirmed is not None
        is_open = (confirmed.get("status") or "") in [
            "open",
            "partially_filled",
        ]
        if is_open:
            if Direction(confirmed["direction"]) != side:
                await self.tlx.cancel(client_order_id=quote_id, id=quote_id)
            elif amount == 0 or abs(confirmed["price"] - price) > AMEND_THRESHOLD:
                logging.info(f"Amending {side} to {amount} @ {price}")
                await self.tlx.amend(
                    amount=amount,
                    price=price,
                    client_order_id=quote_id,
                    id=quote_id,
                )
        elif amount > 0:
            logging.info(f"Inserting {side}: {amount} @ {price}")
            await self.tlx.insert(
                amount=amount,
                price=price,
                direction=side,
                instrument_name=INSTRUMENT,
                client_order_id=quote_id,
                id=quote_id,
                label=ORDER_LABEL,
            )
            confirmed["status"] = "open"
            confirmed["price"] = price
            confirmed["direction"] = side

    async def update_quotes(self, new_index):
        up = self.index is None or new_index > self.index
        self.index = new_index
        if self.position is None or self.mark is None:
            return

        qs = Quotes(self.position, self.mark, self.index)
        assert qs.bid_price < qs.r_price < qs.ask_price
        adjustments = [self.adjust_order(
                side=Direction.SELL,
                price=qs.ask_price,
                amount=qs.ask_amt,
                confirmed=self.quotes[Direction.SELL],
                quote_id=QUOTE_ID[Direction.SELL],
            ),
            self.adjust_order(
                side=qs.r_side,
                price=qs.r_price,
                amount=abs(qs.r_amt),
                confirmed=self.exit_quote,
                quote_id=EXIT_QUOTE_ID,
            ),
            self.adjust_order(
                side=Direction.BUY,
                price=qs.bid_price,
                amount=qs.bid_amt,
                confirmed=self.quotes[Direction.BUY],
                quote_id=QUOTE_ID[Direction.BUY],
            )]
        if not up:
            adjustments.reverse()
        for a in adjustments:
            await a

    async def handle_notification(self, channel: str, notification):
        logging.debug(f"notification in channel {channel} {notification}")
        if channel == "session.orders":
            for order in notification:
                if order["client_order_id"] == EXIT_QUOTE_ID:
                    self.exit_quote = order
                else:
                    self.quotes[Direction(order["direction"])] = order
        elif channel.startswith("price_index"):
            await self.update_quotes(new_index=notification["price"])
        elif channel == "account.portfolio":
            await self.tlx.cancel_session(id=CID_CANCEL)  # Cancel all orders in this session
            self.quotes = {Direction.BUY: {}, Direction.SELL: {}}
            try:
                self.position = next(
                    p for p in notification if p["instrument_name"] == INSTRUMENT
                )["position"]
            except StopIteration:
                self.position = self.position or 0
            logging.info(f"Portfolio updated - {INSTRUMENT} position: {self.position}")
        elif channel.startswith("ticker"):
            self.tob = [notification.get("best_bid_price"), notification.get("best_ask_price")]
            self.mark = notification.get("mark_price")
            self.fr = notification.get("funding_rate")

    async def quote(self):
        await self.tlx.connect()
        await self.tlx.public_subscribe([f"price_index.{INDEX}", f"ticker.{INSTRUMENT}.500ms"], id=CID_PUBSUB)
        await self.tlx.login(keys.key_ids[NETWORK], keys.private_keys[NETWORK], id=CID_LOGIN)

        while True:
            msg = await self.tlx.receive()
            msg = json.loads(msg)
            if "channel_name" in msg:
                await self.handle_notification(msg["channel_name"], msg["notification"])
            elif "result" in msg:
                cid = msg.get("id", -1)
                if cid == CID_LOGIN:
                    await self.tlx.set_cancel_on_disconnect(timeout_secs=6, id=CID_SET_COD)
                    await self.tlx.private_subscribe(["session.orders", "account.portfolio"], id=CID_PRIVSUB)
                else:
                    logging.debug(msg)
            else:
                logging.error(msg)
                await self.tlx.cancel_session(id=CID_CANCEL)
                self.quotes = {Direction.BUY: {}, Direction.SELL: {}}


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("rebalancer-log.txt", mode="a"),
        ],
    )
    run = True  # We set this to false when we want to stop
    while run:
        tlx = thalex.Thalex(network=NETWORK)
        quoter = Quoter(tlx)
        tasks = [asyncio.create_task(quoter.quote())]
        try:
            await asyncio.gather(*tasks)
        except (websockets.ConnectionClosed, socket.gaierror, websockets.InvalidStatus) as e:
            logging.warning(f"Lost connection ({e}). Reconnecting...")
            time.sleep(0.5)
        except asyncio.CancelledError:
            logging.info("Quoting cancelled")
            run = False
        except:
            logging.exception("There was an unexpected error:")
            run = False
        if tlx.connected():
            await tlx.cancel_session()
            await tlx.disconnect()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
