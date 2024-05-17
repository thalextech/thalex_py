import argparse
import asyncio
import json
import logging
import os
import signal
import socket
import sys
import time
from typing import Union, Dict, Optional, List
import enum
import websockets

import thalex_py as th
import keys  # Rename _keys.py to keys.py and add your keys. There are instructions how to create keys in that file.

# The main idea behind this example is to demonstrate how thalex_py can be used to quote the perpetual on Thalex.
# It's not financial advice, however you can use it to test the library and write your own strategy.
#
# The behaviour of the example quoter can be tuned with a number of constants defined below.
# The way this is designed, it connects to thalex, gets the name of the perpetual from thalex,
# and start quoting based on the ticker and index price subscriptions, and the portfolio.
#
# The actual operation is done in the PerpQuoter class, the quotes are created in the make_quotes function.

# These are used to configure how the quoter behaves.
UNDERLYING = "BTCUSD"
LABEL = "P"
AMEND_THRESHOLD = 10  # ticks
BUY_SKEW = 0.2        # Distance for buy quotes from Index
SELL_SKEW = 0.6       # Distance for sell quotes from Index
bidamount1 = 4.2      # ordersize for your first level bid
askamount1 = 4.2      # ordersize for your first level ask
bidamount2 = 6        # ordersize for second level
askamount2 = 6

# We'll use these to match responses from thalex to the corresponding request.
# The numbers are arbitrary, but they need to be unique per CALL_ID.
# We'll use 100+ for identifying trade requests for orders.
CALL_ID_INSTRUMENTS = 0
CALL_ID_INSTRUMENT = 1
CALL_ID_SUBSCRIBE = 2
CALL_ID_LOGIN = 3
CALL_ID_CANCEL_SESSION = 4
CALL_ID_SET_COD = 5


# We log the results to different requests at a different level to improve log readability
async def result_callback(result, cid=None):
    if cid == CALL_ID_INSTRUMENT:
        logging.debug(f"Instrument result: {result}")
    elif cid == CALL_ID_SUBSCRIBE:
        logging.info(f"Sub successful: {result}")
    elif cid == CALL_ID_LOGIN:
        logging.info(f"Login result: {result}")
    elif cid == CALL_ID_SET_COD:
        logging.info(f"Set cancel on disconnect result: {result}")
    elif cid > 99:
        logging.debug(f"Trade request result: {result}")
    else:
        logging.info(f"{cid=}: {result=}")


# These are error codes used by Thalex. We need them to identify errors we want to treat differently.
ERROR_CODE_THROTTLE = 4
ERROR_CODE_ORDER_NOT_FOUND = 1


# Represents the different statuses an order can have on thalex
class OrderStatus(enum.Enum):
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    CANCELLED = "cancelled"
    CANCELLED_PARTIALLY_FILLED = "cancelled_partially_filled"
    FILLED = "filled"


# This is the data that we keep about our orders
class Order:
    def __init__(
        self,
        oid: int,  # needs to be unique per order within the session
        price: float,
        amount: float,
        status: Optional[OrderStatus] = None,
    ):
        self.id: int = oid
        self.price: float = price
        self.amount: float = amount
        self.status: Optional[OrderStatus] = status

    def is_open(self):
        return self.status in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]

    def __repr__(self):
        st = "None" if self.status is None else self.status.value
        return f"Order(id: {self.id}, price: {self.price}, status: {st})"


# This converts the json representation of an order returned by thalex into our Order data structure
def order_from_data(data: Dict) -> Order:
    return Order(
        oid=data["client_order_id"],
        price=data["price"],
        amount=data["remaining_amount"],
        status=OrderStatus(data["status"]),
    )


# Partial representation of an instrument ticker
class Ticker:
    def __init__(self, data: Dict):
        self.mark_price: float = data["mark_price"]
        self.best_bid: Optional[float] = data.get("best_bid_price")
        self.best_ask: Optional[float] = data.get("best_ask_price")
        self.index: float = data["index"]
        self.mark_ts: float = data["mark_timestamp"]
        self.funding_rate: float = data["funding_rate"]


# We use lists like [bid, ask] in some places. This helps indexing into those.
def side_idx(side: th.Direction):
    return 0 if side == th.Direction.BUY else 1


# This is the class that does the actual quoting in two async tasks that we'll run in parallel:
# the quote_task() and the listen_task().
# The quote are created in make_quotes().
class PerpQuoter:
    def __init__(self, thalex: th.Thalex, network: th.Network):
        self.thalex = thalex
        self.network = network
        self.ticker: Optional[Ticker] = None
        self.index: Optional[float] = None
        self.quote_cv = asyncio.Condition()  # We'll use this to activate the quote_task.
        self.portfolio: Dict[str, float] = {}  # Instrument name to position
        self.orders: List[List[Order]] = [[], []]  # bids, asks
        self.client_order_id: int = 100
        self.tick: Optional[float] = None
        self.perp_name: Optional[str] = None
        self.start = time.time()

    def round_to_tick(self, value):
        return self.tick * round(value / self.tick)

    # This creates the quotes we want to have in the book in multiple levels.
    # We'll adjust our quotes in the book to reflect the result of this function.
    def make_quotes(self) -> List[List[th.SideQuote]]:  # [[bids..], [asks...]]
        # Just to demonstrate how we could create the quotes,
        # we'll create two quotes on each side based on the mark price and the index.
        # We could also use eg the funding rate (it's in the ticker) and the portfolio here.
        bid1 = th.SideQuote(
            price=self.round_to_tick(self.index - BUY_SKEW), amount=bidamount1
        )
        bid2 = th.SideQuote(
            price=self.round_to_tick(self.index - BUY_SKEW - self.index * 0.0005),
            amount=bidamount2,
        )
        ask1 = th.SideQuote(
            price=self.round_to_tick(self.index + SELL_SKEW), amount=askamount1
        )
        ask2 = th.SideQuote(
            price=self.round_to_tick(self.index + SELL_SKEW + self.index * 0.0005),
            amount=askamount2,
        )
        return [[bid1, bid2], [ask1, ask2]]
    # This is the task that creates the quotes and pushes them to thalex.
    async def quote_task(self):
        while True:
            async with self.quote_cv:
                # We wait for market conditions that we base our quotes on to change.
                await self.quote_cv.wait()
            if not (self.ticker is None or self.index is None):
                await self.adjust_quotes(self.make_quotes())

    # This function will adjust our quotes in the book to reflect the result of make_quotes.
    async def adjust_quotes(self, desired: List[List[th.SideQuote]]):
        for side_i, side in enumerate([th.Direction.BUY, th.Direction.SELL]):
            orders = self.orders[side_i]
            quotes = desired[side_i]
            for i in range(len(quotes), len(orders)):
                # We have more open orders than we want. Let's cancel the excess that's open.
                if orders[i].is_open():
                    logging.info(f"Cancelling {side.value}-{i} {orders[i].id}")
                    await self.thalex.cancel(
                        client_order_id=orders[i].id, id=orders[i].id
                    )
            for q_lvl, q in enumerate(quotes):
                if len(orders) <= q_lvl or (
                    orders[q_lvl].status is not None and not orders[q_lvl].is_open()
                ):
                    # We don't have this many levels, or the order of this level is not open.
                    # Let's insert a new one.
                    client_order_id = self.client_order_id
                    self.client_order_id = self.client_order_id + 1
                    if len(orders) <= q_lvl:
                        orders.append(Order(client_order_id, q.p, q.a))
                    else:
                        orders[q_lvl] = Order(client_order_id, q.p, q.a)
                    logging.info(
                        f"Inserting {client_order_id} {side.value}-{q_lvl} {q.a}@{q.p}"
                    )
                    await self.thalex.insert(
                        direction=side,
                        instrument_name=self.perp_name,
                        amount=q.a,
                        price=q.p,
                        post_only=True,
                        label=LABEL,
                        client_order_id=client_order_id,
                        id=client_order_id,
                    )
                elif abs(orders[q_lvl].price - q.p) > AMEND_THRESHOLD * self.tick:
                    # We have on open order on this level. If it's different enough, let's amend.
                    logging.info(
                        f"Amending {orders[q_lvl].id} {side.value}-{q_lvl} {orders[q_lvl].price} -> {q.p}"
                    )
                    await self.thalex.amend(
                        amount=q.a,
                        price=q.p,
                        client_order_id=orders[q_lvl].id,
                        id=orders[q_lvl].id,
                    )

    # Requests the instruments from thalex, then blocks until the response is received.
    # We need the name and tick size of the perpetual
    async def await_instruments(self):
        await self.thalex.instruments(CALL_ID_INSTRUMENTS)
        msg = await self.thalex.receive()
        msg = json.loads(msg)
        assert msg["id"] == CALL_ID_INSTRUMENTS
        for i in msg["result"]:
            if i["type"] == "perpetual" and i["underlying"] == UNDERLYING:
                self.tick = i["tick_size"]
                self.perp_name = i["instrument_name"]
                return
        assert False  # Perp not found

    # This task connects to thalex, logs in, sets up basic subscriptions,
    # and then listens to the responses from thalex and reacts to them.
    async def listen_task(self):
        await self.thalex.connect()
        await self.await_instruments()
        await self.thalex.login(
            keys.key_ids[self.network],
            keys.private_keys[self.network],
            id=CALL_ID_LOGIN,
        )
        await self.thalex.set_cancel_on_disconnect(6, id=CALL_ID_SET_COD)
        await self.thalex.private_subscribe(
            ["session.orders", "account.portfolio", "account.trade_history"],
            id=CALL_ID_SUBSCRIBE,
        )
        await self.thalex.public_subscribe(
            [f"ticker.{self.perp_name}.raw", f"price_index.{UNDERLYING}"],
            id=CALL_ID_SUBSCRIBE,
        )
        while True:
            msg = await self.thalex.receive()
            msg = json.loads(msg)
            if "channel_name" in msg:
                await self.notification(msg["channel_name"], msg["notification"])
            elif "result" in msg:
                await result_callback(msg["result"], msg["id"])
            else:
                await self.error_callback(msg["error"], msg["id"])

    # This is where we handle notifications in the channels we have subscribed to.
    async def notification(self, channel: str, notification: Union[Dict, List[Dict]]):
        if channel.startswith("ticker."):
            await self.ticker_callback(notification)
        elif channel.startswith("price_index."):
            await self.index_callback(notification["price"])
        elif channel == "session.orders":
            self.orders_callback(notification)
        elif channel == "account.portfolio":
            self.portfolio_callback(notification)
        elif channel == "account.trade_history":
            self.trades_callback(notification)
        else:
            logging.error(f"Notification for unknown channel: {channel}")

    # https://www.thalex.com/docs/#tag/subs_market_data/Ticker
    # This is where we handle ticker notification sent by thalex
    async def ticker_callback(self, ticker):
        self.ticker = Ticker(ticker)
        async with self.quote_cv:
            self.quote_cv.notify()

    # https://www.thalex.com/docs/#tag/subs_market_data/Index-price
    # Thalex sends us the underlying index price in this channel.
    async def index_callback(self, index: float):
        self.index = index
        async with self.quote_cv:
            self.quote_cv.notify()

    # https://www.thalex.com/docs/#tag/subs_accounting/Account-trade-history
    # Thalex sends us a notification any time we trade.
    # We log the trades that have the label we use in this quoter.
    def trades_callback(self, trades):
        for t in trades:
            if t["label"] == LABEL:
                logging.info(f"Trade: {t['direction']} {t['amount']} @ {t['price']}")


    async def order_error(self, error, oid):
        logging.error(f"Error with order({oid}): {error}")
        for side in [0, 1]:
            for i, o in enumerate(self.orders[side]):
                if o.id == oid:
                    if o.is_open():
                        side = "buy" if i == 0 else "sell"
                        logging.info(f"Cancelling {side}-{i} {oid}")
                        await self.thalex.cancel(client_order_id=oid, id=oid)
                    return

    async def error_callback(self, error, cid=None):
        if cid > 99:
            await self.order_error(error, cid)
        else:
            logging.error(f"{cid=}: error: {error}")

    def update_order(self, order):
        for side in [0, 1]:
            for i, have in enumerate(self.orders[side]):
                if have.id == order.id:
                    self.orders[side][i] = order
                    return True
        return False

    # Thalex sends us this notification every time when an order of ours changes for any reason.
    # This is where we keep track of their actual statuses.
    def orders_callback(self, orders: List[Dict]):
        for o in orders:
            o["fills"]
            if not self.update_order(order_from_data(o)):
                logging.error(f"Didn't find order {o}")

    # https://www.thalex.com/docs/#tag/subs_accounting/Account-portfolio
    # Thalex sends us this notification every time our portfolio changes.
    def portfolio_callback(self, portfolio: List[Dict]):
        for position in portfolio:
            self.portfolio[position["instrument_name"]] = position["position"]


async def reconnect_and_quote_forever(network: th.Network):
    keep_going = True  # We set this to false when we want to stop
    while keep_going:
        thalex = th.Thalex(network=network)
        perp_quoter = PerpQuoter(thalex, network)
        try:
            logging.info(f"STARTING on {network.value} {UNDERLYING=}")
            await asyncio.gather(perp_quoter.listen_task(), perp_quoter.quote_task())
        except (websockets.ConnectionClosed, socket.gaierror) as e:
            logging.error(f"Lost connection ({e}). Reconnecting...")
            time.sleep(0.1)
            continue
        except asyncio.CancelledError:
            keep_going = False
            if thalex.connected():
                await thalex.cancel_session(id=CALL_ID_CANCEL_SESSION)
                while True:
                    r = await thalex.receive()
                    r = json.loads(r)
                    if r.get("id", -1) == CALL_ID_CANCEL_SESSION:
                        logging.info(f"Cancelled session orders")
                        break
                await thalex.disconnect()


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
    parser = argparse.ArgumentParser(
        description="thalex example perpetual quoter",
    )

    parser.add_argument("--network", metavar="CSTR")
    parser.add_argument("--log", default="info", metavar="CSTR")
    args = parser.parse_args(sys.argv[1:])

    if args.network == "prod":
        arg_network = th.Network.PROD
    elif args.network == "test":
        arg_network = th.Network.TEST
    else:
        logging.error("--network invalid or missing")
        assert False  # --network invalid or missing

    if args.log == "debug":
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)

    loop = asyncio.get_event_loop()
    main_task = loop.create_task(reconnect_and_quote_forever(arg_network))

    if os.name != "nt":  # Non-Windows platforms
        loop.add_signal_handler(signal.SIGTERM, handle_signal, loop, main_task)
        loop.add_signal_handler(signal.SIGINT, handle_signal, loop, main_task)
    try:
        loop.run_until_complete(main_task)
    finally:
        loop.close()


if __name__ == "__main__":
    main()
