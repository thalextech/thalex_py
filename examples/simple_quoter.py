import asyncio
import json
import logging
import socket
import sys
import time
from typing import Optional
import websockets
from dataclasses import dataclass

import thalex
from thalex.thalex import Direction
import keys  # Rename _keys.py to keys.py and add your keys. There are instructions how to create keys in that file.

NETWORK = thalex.Network.TEST
ORDER_LABEL = "simple_quoter"
INSTRUMENT = "BTC-PERPETUAL"  # When changing INSTRUMENT, make sure to update PRICE_TICK and SIZE_TICK based on Thalex Contract Specifications
PRICE_TICK = 1  # Price Tick Size as defined in Thalex Contract Specifications - https://www.thalex.com/trading-information/contract-specifications/perpetuals
SIZE_TICK = 0.001  # Volume Tick Size as defined in Thalex Contract Specifications - https://www.thalex.com/trading-information/contract-specifications/perpetuals
HALF_SPREAD = 0.75  # BPS
AMEND_THRESHOLD = 5  # USD
SIZE = 0.1  # Number of contracts to quote
# If the size of our position is greater than this either side, we don't quote that side.
# Because we don't actively cancel orders already inserted and due to race conditions in
# notification channels, in some cases we might overshoot.
MAX_POSITION = 0.3

from enum import Enum
class OrderType(Enum):
    STOP = "stop"
    TAKE_PROFIT = "take_profit"

QUOTE_ID = {
    Direction.BUY: 1001, 
    Direction.SELL: 1002,
    OrderType.STOP: 2001,
    OrderType.TAKE_PROFIT: 2002
}

REFRESH_PORTFOLIO_EVERY_N = 100  # Refresh portfolio every N notifications to determine if to take profit or stop loss
TAKE_PROFIT_BPS = 10 # Take profit at 10 bps
STOP_LOSS_BPS = 20 # Stop loss at 20 bps


def round_to_tick(value):
    return PRICE_TICK * round(value / PRICE_TICK)


def round_size(size):
    return SIZE_TICK * round(size / SIZE_TICK)

@dataclass
class Position:
    """Simple class to represent a position"""
    instrument_name: str
    position: float
    entry_value: float
    perpetual_funding_entry_value: float
    index: float
    iv: Optional[float]
    mark_price: float
    start_price: float
    average_price: float
    unrealised_pnl: float
    unrealised_perpetual_funding: float
    realised_pnl: float
    realised_position_pnl: float
    realised_perpetual_funding: float
    session_fees: float




class Quoter:
    def __init__(self, tlx: thalex.Thalex):
        self.tlx: thalex.Thalex = tlx
        self.mark: Optional[float] = None
        self.quotes: dict[thalex.Direction, Optional[dict]] = {
            Direction.BUY: {},
            Direction.SELL: {},
        }
        self.position: Optional[Position] = None
        self.tp_order_id: Optional[int] = None
        self.sl_order_id: Optional[int] = None

    async def adjust_order(self, side, price, amount):
        confirmed = self.quotes[side]
        assert confirmed is not None
        is_open = (confirmed.get("status") or "") in [
            "open",
            "partially_filled",
        ]
        if is_open:
            if amount == 0 or abs(confirmed["price"] - price) > AMEND_THRESHOLD:
                logging.info(f"Amending {side} to {amount} @ {price}")
                await self.tlx.amend(
                    amount=amount,
                    price=price,
                    client_order_id=QUOTE_ID[side],
                    id=QUOTE_ID[side],
                )
        elif amount > 0:
            logging.info(f"Inserting {side}: {amount} @ {price}")
            await self.tlx.insert(
                amount=amount,
                price=price,
                direction=side,
                instrument_name=INSTRUMENT,
                client_order_id=QUOTE_ID[side],
                id=QUOTE_ID[side],
                label=ORDER_LABEL,
                post_only=True
            )
            self.quotes[side] = {"status": "open", "price": price}

    async def update_tp_and_sl(self, ):
        """Place a take profit and stop loss order based on current position"""
        if self.position is None or self.position.position == 0:
            # No position, nothing to do
            for order_id in [self.tp_order_id, self.sl_order_id]:
                if order_id is not None:
                    logging.info(f"Cancelling order {order_id} as we have no position")
                    await self.tlx.cancel(order_id=order_id)
            self.tp_order_id = None
            self.sl_order_id = None
            return
        entry_price = self.position.average_price
        if self.position.position > 0:
            # Long position - place take profit above and stop loss below
            tp_price = round_to_tick(entry_price * (1 + TAKE_PROFIT_BPS / 10_000))
            sl_price = round_to_tick(entry_price * (1 - STOP_LOSS_BPS / 10_000))
            sl_side, tp_side = Direction.SELL, Direction.SELL
        else:
            # Short position - place take profit below and stop loss above
            tp_price = round_to_tick(entry_price * (1 - TAKE_PROFIT_BPS / 10_000))
            sl_price = round_to_tick(entry_price * (1 + STOP_LOSS_BPS / 10_000))
            sl_side, tp_side = Direction.BUY, Direction.BUY
        tp_size, sl_size = [abs(self.position.position)] * 2

        if self.tp_order_id is not None:
            logging.info(f"Amending TP order {self.tp_order_id} to {tp_size} @ {tp_price}")
            await self.tlx.amend(
                amount=tp_size,
                price=tp_price,
                id=self.tp_order_id,
            )
        else:
            logging.info(f"Inserting TP order {tp_size} @ {tp_price}")
            await self.tlx.create_conditional_order(
                direction=tp_side,
                instrument_name=INSTRUMENT,
                amount=tp_size,
                stop_price=tp_price,
                label=f"{ORDER_LABEL}_TP",
                reduce_only=True,
                id=QUOTE_ID[OrderType.TAKE_PROFIT]
            )
            self.tp_order_id = QUOTE_ID[OrderType.TAKE_PROFIT]
            logging.info(f"TP order created with id {self.tp_order_id}")
        
        if self.sl_order_id is not None:
            logging.info(f"Amending SL order {self.sl_order_id} to {sl_size} @ {sl_price}")
            await self.tlx.amend(
                amount=sl_size,
                price=sl_price,
                id=self.sl_order_id,
            )
        else:
            logging.info(f"Inserting SL order {sl_size} @ {sl_price}")
            await self.tlx.create_conditional_order(
                direction=sl_side,
                instrument_name=INSTRUMENT,
                amount=sl_size,
                stop_price=sl_price,
                label=f"{ORDER_LABEL}_SL",
                reduce_only=True,
                id=QUOTE_ID[OrderType.STOP]
            )
            self.sl_order_id = QUOTE_ID[OrderType.STOP]
            logging.info(f"SL order created with id {self.sl_order_id}")
        

    async def update_quotes(self, new_mark):
        up = self.mark is None or new_mark > self.mark
        self.mark = new_mark
        if self.position is None:
            return

        bid_price = round_to_tick(new_mark - (HALF_SPREAD / 10_000 * new_mark))
        bid_size = round_size(max(min(SIZE, MAX_POSITION - self.position.position), 0))
        ask_price = round_to_tick(new_mark + (HALF_SPREAD / 10_000 * new_mark))
        ask_size = round_size(max(min(SIZE, MAX_POSITION + self.position.position), 0))

        if up:
            # Insert Sell before Buy to avoid self-trades if existing ask price is lower than new bid
            await self.adjust_order(Direction.SELL, price=ask_price, amount=ask_size)
            await self.adjust_order(Direction.BUY, price=bid_price, amount=bid_size)
        else:
            # Insert Buy before Sell to avoid self-trades if existing bid price is higher than new ask
            await self.adjust_order(Direction.BUY, price=bid_price, amount=bid_size)
            await self.adjust_order(Direction.SELL, price=ask_price, amount=ask_size)

    async def handle_notification(self, channel: str, notification):
        logging.debug(f"notification in channel {channel} {notification}")
        if channel == "session.orders":
            for order in notification:
                self.quotes[Direction(order["direction"])] = order
        elif channel.startswith("lwt"):
            await self.update_quotes(new_mark=notification["m"])
        elif channel == "account.portfolio":
            await self.tlx.cancel_session()  # Cancel all orders in this session
            self.quotes = {Direction.BUY: {}, Direction.SELL: {}}
            try:
                position = next(
                    p for p in notification if p["instrument_name"] == INSTRUMENT
                )
                self.position = Position(**position)
                await self.update_tp_and_sl()

            except StopIteration:
                self.position = self.position or None
            logging.info(f"Portfolio updated - {INSTRUMENT} position: {self.position.position}")
        else:
            logging.warning(f"Unhandled notification in channel {channel} {notification}")

    async def quote(self):
        await self.tlx.connect()
        await self.tlx.login(keys.key_ids[NETWORK], keys.private_keys[NETWORK])
        await self.tlx.set_cancel_on_disconnect(timeout_secs=6)
        await self.tlx.public_subscribe([f"lwt.{INSTRUMENT}.1000ms"])
        await self.tlx.private_subscribe(["session.orders", "account.portfolio"])
        iterations = 0
        while True:
            if iterations % REFRESH_PORTFOLIO_EVERY_N == 0:
                logging.info("Refreshing portfolio")
                await self.tlx.account_breakdown()
            iterations += 1
            msg = await self.tlx.receive()
            msg = json.loads(msg)
            if "channel_name" in msg:
                await self.handle_notification(msg["channel_name"], msg["notification"])
            elif "result" in msg:
                logging.debug(msg)
            else:
                logging.error(msg)
                await self.tlx.cancel_session()
                self.quotes = {Direction.BUY: {}, Direction.SELL: {}}


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("log.txt", mode="a"),
        ],
    )
    run = True  # We set this to false when we want to stop
    while run:
        tlx = thalex.Thalex(network=NETWORK)
        quoter = Quoter(tlx)
        task = asyncio.create_task(quoter.quote())
        try:
            await task
        except (websockets.ConnectionClosed, socket.gaierror) as e:
            logging.error(f"Lost connection ({e}). Reconnecting...")
            time.sleep(0.1)
        except asyncio.CancelledError:
            logging.info("Quoting cancelled")
            run = False
        except:
            logging.exception("There was an unexpected error:")
            run = False
        if tlx.connected():
            await tlx.cancel_session()
            await tlx.disconnect()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
