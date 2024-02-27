import copy
import argparse
import asyncio
import json
import logging
import socket
import sys
import signal
import os
import time
from datetime import datetime, timezone
from typing import Union, Dict, Optional, List
import enum
import websockets

import thalex_py
import black_scholes
import keys  # Rename _keys.py to keys.py and add your keys


# The main idea behind this example is to demonstrate how thalex_py can be used to quote options on Thalex.
# It's not meant to be an out-of-the-box money printer, however you can use it to test the library and
# write your own strategy.
#
# The behaviour of the example quoter can be tuned with a number of constants defined below.
# The way this is designed, it connects to thalex, gets all instruments and then starts quoting options in
# the nearest NUMBER_OF_EXPIRIES_TO_QUOTE expiries with absolute deltas within DELTA_RANGE_TO_QUOTE.
#
# The actual operation is done in the OptionQuoter class

# These are used to configure how the quoter behaves.
UNDERLYING = "BTCUSD"  # We'll only quote options of this underlying
NUMBER_OF_EXPIRIES_TO_QUOTE = 5  # More expiries means more quotes, but more throttling
SUBSCRIBE_INTERVAL = "200ms"  # Also how frequently we adjust quotes
UNQUOTED_SUBSCRIBE_INTERVAL = "5000ms"  # For tickers of not quoted instruments, only to track greeks
DELTA_RANGE_TO_QUOTE = (0.1, 0.8)  # Wider range means more quotes, but more throttling
SPREAD = 30  # base spread to create a spread around the mark price
SPREAD_FACTOR = 1.05  # extra spread per cumulative absolute open options deltas
LOTS = 0.5  # Default options quote size
AMEND_THRESHOLD = 3  # In ticks. We don't amend smaller than this, to avoid throttling.
INDEX_RECALC_THRESHOLD = 10  # If index changes this many dollars, we recalculate quotes
RETREAT = 0.01  # Skew prices for open position size
MAX_MARGIN = 300000  # If the required margin goes above this, we only reduce positions.
# If the deltas go outside (-MAX_DELTAS, MAX_DELTAS),
# we'll only insert quotes that reduce their absolute value.
MAX_DELTAS = 1.0
MAX_OPEN_OPTION_DELTAS = 10.0  # As in cumulative absolute deltas across all option positions.
DELTA_SKEW = 300  # To skew quote prices for portfolio delta
VEGA_SKEW = 20  # To skew quote prices for portfolio vega
GAMMA_SKEW = 4000000  # To skew quote prices for portfolio gamma
MAX_VEGA = 60  # If we breach (-MAX_VEGA, MAX_VEGA) we stop selling/buying options
MAX_GAMMA = 0.02  # If we breach (-MAX_GAMMA, MAX_GAMMA) we stop selling/buying options
# To identify the orders and trades of this quoter. If you run multiple in parallel (eg for different underlyings),
# you should give them different labels.
LABEL = "X"
TRADE_LOG = "trades_options.log"  # File path for logging trades
GREEKS_CALC_PERIOD = 30  # We'll recalculate portfolio greeks and pricing skews every GREEKS_PRINT_PERIOD seconds.
GREEKS_PRINT_PERIOD = 600  # We'll print portfolio greeks and pricing skews every GREEKS_PRINT_PERIOD seconds.


# We'll use these to match responses from thalex to the corresponding request.
# The numbers are arbitrary, but they need to be unique per CALL_ID.
# We'll use 100+ for identifying trade requests for orders.
CALL_ID_SUBSCRIBE = 0
CALL_ID_LOGIN = 1
CALL_ID_CANCEL_SESSION = 2
CALL_ID_SET_COD = 3


# These are error codes used by Thalex. We need them to identify errors we want to treat differently.
ERROR_CODE_THROTTLE = 4
ERROR_CODE_ORDER_NOT_FOUND = 1


# Represents the different instrument types offered by thalex
class InstrumentType(enum.Enum):
    OPTION = "option"
    FUTURE = "future"
    COMBO = "combination"
    PERP = "perpetual"


# Represents the different statuses an order can have on thalex
class OrderStatus(enum.Enum):
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    CANCELLED = "cancelled"
    CANCELLED_PARTIALLY_FILLED = "cancelled_partially_filled"
    FILLED = "filled"


# Partial representation of the details of an instrument on thalex
class Instrument:
    def __init__(self, data: Dict):
        self.name: str = data["instrument_name"]
        self.underlying = data["underlying"]
        self.tick_size: float = data["tick_size"]
        self.type: InstrumentType = InstrumentType(data["type"])
        self.option_type: Optional[str] = data.get("option_type")
        self.expiry_ts: Optional[int] = data.get("expiration_timestamp")
        self.strike_price: Optional[int] = data.get("strike_price")

    def __repr__(self):
        return self.name


# This is the data that we keep about our orders
class Order:
    def __init__(
        self,
        oid: int,  # needs to be unique per order within the session
        price: float,
        side: thalex_py.Direction,
        status: Optional[OrderStatus] = None,
    ):
        self.id: int = oid
        self.price: float = price
        self.side: thalex_py.Direction = side
        self.status: Optional[OrderStatus] = status

    def is_open(self):
        return self.status in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]

    def __repr__(self):
        st = "None" if self.status is None else self.status.value
        return f"Order(id: {self.id}, price: {self.price}, side: {self.side.value}, status: {st})"


# This converts the json representation of an order returned by thalex into our Order data structure
def order_from_data(data: Dict) -> Order:
    side: thalex_py.Direction = thalex_py.Direction(data["direction"])
    price: float = data.get("price", 0 if side == thalex_py.Direction.SELL else float("inf"))
    return Order(
        oid=data["client_order_id"],
        price=price,
        side=side,
        status=OrderStatus(data["status"]),
    )


# This is a partial representation of what thalex tells us about our trades. We use this for analyisis of trades.
class Trade:
    def __init__(self, data: Dict):
        self.instrument: str = data["instrument_name"]
        self.direction: thalex_py.Direction = thalex_py.Direction(data["direction"])
        self.amount: float = data["amount"]
        self.price: float = data["price"]
        self.pos_after: float = data.get("position_after", 0.0)
        self.type = data.get("trade_type", "unknown")
        self.oid = data.get("client_order_id", data.get("order_id", "-"))
        self.index = data.get("index", "-")
        self.maker = data.get("maker_taker", "?")
        self.time = data.get("time", "-")
        self.label = data.get("label", "-")

    def __repr__(self):
        return (
            f"{self.direction.value} {self.amount} @ {self.price} {self.time} {self.maker} {self.instrument} type: "
            f"{self.type}, oid: {self.oid}, label: {self.label}, pos after: {self.pos_after}, "
            f"index: {int(self.index)}"
        )


# Partial representation of an instrument ticker
class Ticker:
    def __init__(self, data: Dict):
        self.mark_price: float = data["mark_price"]
        self.delta: float = data["delta"]
        self.best_bid: Optional[float] = data.get("best_bid_price")
        self.best_ask: Optional[float] = data.get("best_ask_price")
        self.index: float = data["index"]
        self.mark_ts: float = data["mark_timestamp"]
        self.iv: Optional[float] = data.get("iv", 0.0)
        self.fwd: float = data["forward"]


# The timestamps returned by thalex are in utc timezone. Let's use the same.
def utcnow() -> float:
    return datetime.now(timezone.utc).timestamp()


# We log the results to different requests at a different level to improve log readability
def result_callback(result, cid=None):
    if cid == CALL_ID_SUBSCRIBE:
        logging.debug(f"subscribe result: {result}")
    elif cid is not None and cid > 99:
        logging.debug(f"Adjust order result: {result}")
    elif cid == CALL_ID_LOGIN:
        logging.info(f"login result: {result}")
    elif cid == CALL_ID_SET_COD:
        logging.debug(f"Set cancel on disconnect result: {result}")
    else:
        logging.info(result)


# Thalex sends us a snapshot in the "instruments" channel of all the active instruments when we subscribe to it,
# and also a notification of the delta every time an active instrument is added or removed.
# If the notification is not a snapshot, meaning the set of active instruments changed,
# we raise this exception in order to restart the quoter to clean its state, like when there's a connection loss.
# This is much easier than adjusting the existing state.
class InstrumentsChanged(Exception):
    def __init__(self):
        self.message = f"the set of active instruments changed"
        super().__init__(self.message)


# We track everything we need to know about an instruments we quote in this
class InstrumentData:
    def __init__(self, instrument: Instrument):
        self.instrument: Instrument = instrument
        self.ticker: Optional[Ticker] = None
        self.greeks = black_scholes.Greeks()
        self.last_quote_index = 0.0
        self.prices_sent: List[Optional[float]] = [None, None]  # [bid, ask]
        self.orders: List[Optional[Order]] = [None, None]  # [bid, ask]
        self.position: float = 0.0
        self.retreat = 0


# We track everything we need to know about an instruments we do not quote in this
class UnquotedInstrument:
    def __init__(self, instrument: Instrument):
        self.instrument: Instrument = instrument
        self.ticker: Optional[Ticker] = None
        self.subbed = False
        self.position: float = 0.0


# We use lists like [bid, ask] in some places. This helps indexing into those.
def side_idx(side: thalex_py.Direction):
    return 0 if side == thalex_py.Direction.BUY else 1


# To align the price with instrument tick size.
def round_to_tick(value, tick):
    return tick * round(value / tick)


# This is the class that does the actual quoting in two async tasks that we'll run in parallel:
# the greeks_task() and the listen_task().
# The quote prices are calculated in pricing().
# We'll adjust our quotes if there's a notification in a ticker channel or if we trade.
class OptionQuoter:
    def __init__(self, thalex: thalex_py.Thalex, network: thalex_py.Network):
        self.thalex: thalex_py.Thalex = thalex
        self.network: thalex_py.Network = network
        self.options: Dict[str, InstrumentData] = {}
        self.unquoted_instruments: Dict[str, UnquotedInstrument] = {}
        self.client_order_id: int = 100
        self.close_only = False
        self.open_opt_deltas: float = 0.0
        self.greeks: black_scholes.Greeks = black_scholes.Greeks()
        self.can_trade = False
        self.spread: float = 0.0
        self.d_skew: float = 0.0
        self.g_skew: float = 0.0
        self.v_skew: float = 0.0

    # Sets up basic subscriptions and then periodically calls recalc_greeks
    async def greeks_task(self):
        await self.thalex.connect()
        await self.thalex.login(
            keys.key_ids[self.network],
            keys.private_keys[self.network],
            id=CALL_ID_LOGIN,
        )
        await self.thalex.set_cancel_on_disconnect(6, id=CALL_ID_SET_COD)
        await self.thalex.public_subscribe(
            ["instruments", f"price_index.{UNDERLYING}"],
            CALL_ID_SUBSCRIBE,
        )
        await self.thalex.private_subscribe(
            [
                "session.orders",
                "account.trade_history",
                "account.portfolio",
                "account.summary",
            ],
            CALL_ID_SUBSCRIBE,
        )
        await asyncio.sleep(5)  # Wait for notifications
        self.recalc_greeks()
        logging.info(
            f"Greeks: {self.greeks}, open option deltas: {self.open_opt_deltas:.2f}, spread: {self.spread:.0f}, "
            f"d_skew: {self.d_skew:.0f}, v_skew: {self.v_skew:.0f}, g_skew: {self.g_skew:.0f}"
        )
        self.can_trade = True
        last_print = 0
        while True:
            await asyncio.sleep(GREEKS_CALC_PERIOD)
            self.recalc_greeks()
            now = time.time()
            if last_print < now - GREEKS_PRINT_PERIOD:
                last_print = now
                logging.info(
                    f"Greeks: {self.greeks}, open option deltas: {self.open_opt_deltas:.2f}, "
                    f"spread: {self.spread:.0f}, d_skew: {self.d_skew:.0f}, v_skew: {self.v_skew:.0f},"
                    f"g_skew: {self.g_skew:.0f}"
                )

    # Recalculates the greeks of the portfolio and the skews that are used in pricing
    def recalc_greeks(self):
        # We reset our metrics to 0, iterate over every instrument that we quote or have a position in,
        # calculate the greeks for the instrument and then we add the instrument metrics multiplied by our position
        # to our portfolio metrics.
        self.greeks = black_scholes.Greeks()
        self.open_opt_deltas = 0
        for i in [
            *self.options.values(),
            *[i for i in self.unquoted_instruments.values() if i.position != 0],
        ]:
            if i.ticker is None:
                logging.warning(f"Not ticker for {i.instrument.name}")
                continue
            if i.instrument.type == InstrumentType.OPTION:
                # Thalex doesn't give us all the greeks, only delta, so we'll use the Black and Scholes model
                # to calculate the rest of the greeks for this instrument ourselves.
                maturity = (i.instrument.expiry_ts - i.ticker.mark_ts) / (365.25 * 24 * 3600)
                i.greeks = black_scholes.all_greeks(
                    i.ticker.fwd,
                    i.instrument.strike_price,
                    i.ticker.iv,
                    maturity,
                    i.instrument.option_type == "put",
                    i.ticker.delta,
                )
                if i.position != 0:
                    m_greeks = copy.copy(i.greeks)
                    m_greeks.mult(i.position)
                    self.greeks.add(m_greeks)
                    self.open_opt_deltas += abs(i.position * i.ticker.delta)
            else:
                # If it's not an option it's assumed to be d1 (it has to be a perp or a future)
                self.greeks.delta += i.position
        # These are the skews that we use in pricing()
        self.d_skew = self.greeks.delta * DELTA_SKEW * LOTS
        self.v_skew = (self.greeks.vega / MAX_VEGA) * VEGA_SKEW * LOTS
        self.g_skew = (self.greeks.gamma / MAX_GAMMA) * GAMMA_SKEW * LOTS
        self.spread = max(1.0, self.open_opt_deltas * SPREAD_FACTOR) * SPREAD

    # The quote prices are calculated by creating a spread around the mark_price coming from the
    # instrument ticker based on some skews that are calculated in recalc_greeks() and portfolio positions.
    def pricing(self, i: InstrumentData) -> List[Optional[float]]:  # [bid, ask]
        if i.ticker is None:
            # We don't have a ticker for this instrument yet, there's not much we can do
            return [None, None]
        within_delta_range = (
            i.instrument.type == InstrumentType.OPTION
            and DELTA_RANGE_TO_QUOTE[0] < abs(i.ticker.delta) < DELTA_RANGE_TO_QUOTE[1]
        )
        want_to_quote = (within_delta_range and not self.close_only) or i.position != 0
        if not want_to_quote or not self.can_trade:
            # We don't want to quote this at all, because it's either outside of our desired delta range, or the
            # pricing skews have not yet been calculated.
            # Note that if we already have a position in this instrument we want to quote it even if we're in close
            # only mode exactly to reduce the position.
            return [None, None]

        d_skew = self.d_skew * i.greeks.delta  # Skew both sides for portfolio delta
        v_skew = self.v_skew * i.greeks.vega  # Skew both sides for portfolio vega
        g_skew = self.g_skew * i.greeks.gamma  # Skew both sides for portfolio gamma
        bid = i.ticker.mark_price - self.spread - d_skew - v_skew - g_skew
        ask = i.ticker.mark_price + self.spread - d_skew - v_skew - g_skew

        i.retreat = (
            RETREAT * i.ticker.mark_price * i.instrument.tick_size * i.position / LOTS
        )  # Skew for already open position we have in this instrument. Only applied to one side.
        if i.position > 0:
            bid -= i.retreat
        if i.position < 0:
            ask -= i.retreat

        # Let's cap the price at the mark price of the instrument if all the skewing above happens to cross it.
        bid = min(bid, i.ticker.mark_price - i.instrument.tick_size)
        ask = max(ask, i.ticker.mark_price + i.instrument.tick_size)

        # Align the prices with the instrument tick size
        bid = round_to_tick(bid, i.instrument.tick_size)
        ask = round_to_tick(ask, i.instrument.tick_size)

        # There are certain scenarios in which we don't want to quote one side of an instrument.
        # We'll check for those here and remove one side if necessary.
        if (
            bid < i.instrument.tick_size
            or (self.close_only and i.position >= 0)
            or MAX_VEGA < self.greeks.vega
            or MAX_GAMMA < self.greeks.gamma
            or (MAX_OPEN_OPTION_DELTAS < self.open_opt_deltas and i.position >= 0)
            or (MAX_DELTAS < self.greeks.delta and i.ticker.delta > 0)
            or (-MAX_DELTAS > self.greeks.delta and i.ticker.delta < 0)
        ):
            bid = None
        if (
            ask < i.instrument.tick_size
            or (self.close_only and i.position <= 0)
            or -MAX_VEGA > self.greeks.vega
            or -MAX_GAMMA > self.greeks.gamma
            or (MAX_OPEN_OPTION_DELTAS < self.open_opt_deltas and i.position <= 0)
            or (MAX_DELTAS < self.greeks.delta and i.ticker.delta < 0)
            or (-MAX_DELTAS > self.greeks.delta and i.ticker.delta > 0)
        ):
            ask = None
        return [bid, ask]

    # Insert/Amend/Cancel our bid & ask for one instrument based on the quotes pricing() comes up with,
    # and what we know about the status of our already open orders for the instrument (if any).
    async def adjust_quotes(self, i: InstrumentData):
        quote_prices = self.pricing(i)  # The [bid, ask] we ideally want to have as open orders
        for idx, side in [(0, thalex_py.Direction.BUY), (1, thalex_py.Direction.SELL)]:
            price = quote_prices[idx]
            sent = i.prices_sent[idx]
            if price is None:
                # We don't want to quote this side of this instrument
                if sent is not None or (i.orders[idx] is not None and i.orders[idx].is_open()):
                    # We have an open order that we want to cancel.
                    # If the order is open because the previous cancel was not yet processed, we might be trying to
                    # delete it twice, but that should be rare and even then it's better to be safe than sorry.
                    i.prices_sent[idx] = None
                    client_order_id = i.orders[idx].id
                    logging.debug(f"Cancel {client_order_id} {side.value} {i.instrument}")
                    await self.thalex.cancel(
                        client_order_id=client_order_id,
                        id=client_order_id,
                    )
            elif sent is None:
                # This is a new quote. Let's insert it.
                # We might have one already open, for which a cancel is in flight, leave that to its fate.
                i.prices_sent[idx] = price
                client_order_id = self.client_order_id
                self.client_order_id = self.client_order_id + 1
                i.orders[idx] = Order(client_order_id, price, side)
                logging.debug(f"Insert {client_order_id} {i.instrument.name} {side.value} {price}")
                await self.thalex.insert(
                    direction=side,
                    instrument_name=i.instrument.name,
                    amount=LOTS,
                    price=price,
                    post_only=True,
                    label=LABEL,
                    client_order_id=client_order_id,
                    id=client_order_id,
                )
            elif abs(price - sent) >= AMEND_THRESHOLD * i.instrument.tick_size:
                # Neither price nor sent are none, and they differ enough for us to want to amend the order.
                # We only amend if the difference is large enough, to avoid throttling.
                i.prices_sent[idx] = price
                client_order_id = i.orders[idx].id
                logging.debug(f"Amend {client_order_id} {i.instrument.name} {side.value} {sent} -> {price}")
                await self.thalex.amend(
                    amount=LOTS,
                    price=price,
                    client_order_id=client_order_id,
                    id=client_order_id,
                )

    # If there's something wrong with what we sent, this will revert our idea of the order to that of thalex,
    # and take some action if necessary
    async def order_error(self, error, oid):
        # First we find the order based on its id.
        # We could store an order id to order map to avoid this linear search, but we shouldn't need to do this often.
        for idata in self.options.values():
            for order in idata.orders:
                if order is not None and order.id == oid:
                    # We found the order, we'll figure out what went wrong and take action accordingly.
                    side = side_idx(order.side)
                    sent = idata.prices_sent[side]
                    if sent is None:
                        if error["code"] == ERROR_CODE_ORDER_NOT_FOUND:
                            # We tried to cancel something that's not there. It might have been canceled
                            # or filled before our cancel request was processed.
                            # We don't need to do anything.
                            logging.info(f"{order} already filled/cancelled for {idata.instrument}")
                        else:
                            # We failed to pull an order for some other reason. That's bad.
                            # Best we can do is try again immediately
                            logging.error(f"Failed to cancel {order} for {idata.instrument}: {error}")
                            await self.thalex.cancel(
                                client_order_id=oid,
                                id=oid,
                            )
                    elif not order.is_open():
                        # We don't have an open order. We'll leave it at that for now and insert one later.
                        idata.prices_sent[side] = None
                        if error["code"] == ERROR_CODE_THROTTLE:
                            logging.info(f"Insert {order} @{sent} {idata.instrument.name} throttled")
                        else:
                            # If it's not just throttling, let's show some elaborate logs
                            # to be able to analyze what was wrong with the order.
                            logging.warning(
                                f"Failed to insert {order} for {idata.instrument}: {error}\n"
                                f"mark={idata.ticker.mark_price:.4f} {self.spread=:.4f} {self.d_skew=:.4f} "
                                f"{self.g_skew=:.4f} {self.v_skew=:.4f} {idata.retreat=:.4f} {idata.greeks=}"
                            )
                    else:
                        # We did send some price and there's an open order
                        if error["code"] == ERROR_CODE_THROTTLE:
                            # If you see this a lot, you should decrease NUMBER_OF_EXPIRIES_TO_QUOTE or
                            # DELTA_RANGE_TO_QUOTE or increase AMEND_THRESHOLD in order to amend less frequently.
                            logging.info(f"Amend {order} for {idata.instrument} -> {sent} throttled")
                        else:
                            logging.warning(
                                f"Failed to amend {order} for {idata.instrument} -> {sent}: {error}\n"
                                f"mark={idata.ticker.mark_price:.4f} {self.spread=:.4f} {self.d_skew=:.4f}"
                                f"{self.g_skew=:.4f} {self.v_skew=:.4f} {idata.retreat=:.4f} {idata.greeks=}"
                            )
                        if abs(order.price - sent) >= 2 * AMEND_THRESHOLD * idata.instrument.tick_size:
                            # The order is open at the wrong price, and we're getting throttled,
                            # so we won't try to amand again, but cancel it for now and reinsert it later.
                            idata.prices_sent[side] = None
                            await self.thalex.cancel(client_order_id=oid, id=oid)
                    return  # The order was found so let's not iterate over the rest
        # The order wasn't found above
        logging.error(f"Error with unknown order({oid}): {error}")

    # This task listens to the responses from thalex and reacts to them.
    async def listen_task(self):
        while not self.thalex.connected():
            await asyncio.sleep(0.5)
        while True:
            msg = await self.thalex.receive()
            msg = json.loads(msg)
            channel = msg.get("channel_name")
            if channel is not None:
                await self.notification(channel, msg["notification"], msg.get("snapshot", False))
            elif "result" in msg:
                result_callback(msg["result"], msg["id"])
            else:
                await self.error_callback(msg["error"], msg["id"])

    # This is where we handle notifications in the channels we have subscribed to.
    async def notification(self, channel: str, notification: Union[Dict, List], snapshot: bool):
        if channel.startswith("ticker."):
            # https://www.thalex.com/docs/#tag/subs_market_data/Ticker
            iname = channel.split(".")[1]
            ticker = Ticker(notification)
            i = self.options.get(iname)
            if i is not None:
                i.ticker = ticker
                i.last_quote_index = ticker.index
                await self.adjust_quotes(i)
            else:
                self.unquoted_instruments[iname].ticker = ticker
        elif channel == f"price_index.{UNDERLYING}":
            # https://www.thalex.com/docs/#tag/subs_market_data/Index-price
            index: float = notification["price"]
            for i in self.options.values():
                if abs(i.last_quote_index - index) > INDEX_RECALC_THRESHOLD:
                    i.last_quote_index = index
                    await self.adjust_quotes(i)
        elif channel == "session.orders":
            self.orders_callback(notification)
        elif channel == "account.trade_history":
            self.trades_callback(notification)
        elif channel == "account.portfolio":
            await self.portfolio_callback(notification)
        elif channel == "account.summary":
            self.account_summary(notification)
        elif channel == "instruments":
            if not snapshot:
                # See the comment at the definition of InstrumentsChanged for an explanation
                raise InstrumentsChanged()
            await self.instruments_callback(notification)
        else:
            logging.error(f"Notification for unknown {channel=}")

    # https://www.thalex.com/docs/#tag/subs_accounting/Account-portfolio
    # Thalex sends us this notification every time our portfolio changes.
    # We use this to track our positions in instruments for risk metrics calculations.
    async def portfolio_callback(self, portfolio: List[Dict]):
        for position in portfolio:
            iname = position["instrument_name"]
            i = self.options.get(iname)
            if i is not None:
                i.position = position["position"]
            else:
                unq = self.unquoted_instruments.get(iname)
                if unq is not None:
                    unq.position = position["position"]
                    if not unq.subbed:
                        # If we're not yet subscribed to this instrument's ticker let's subscribe now,
                        # so that we get the necessary data for greeks calculations.
                        await self.thalex.public_subscribe(
                            [f"ticker.{unq.instrument.name}.{UNQUOTED_SUBSCRIBE_INTERVAL}"],
                            id=CALL_ID_SUBSCRIBE,
                        )
                        unq.subbed = True

    # https://www.thalex.com/docs/#tag/subs_accounting/Account-summary
    # Thalex sends us this notification periodically.
    # We use this to track our required margin in order to avoid liquidation.
    def account_summary(self, acc_sum: Dict):
        required_margin = acc_sum["required_margin"]
        if required_margin > MAX_MARGIN and not self.close_only:
            logging.warning(f"Required margin: {required_margin}. Going into close only mode.")
            self.close_only = True
        # We need some factor here, because there's some margin required for open orders, so if we start quoting
        # immediately after we go below our decided margin limits, we'll just flicker the close_only back and forth.
        # If you see this flickering happen, reduce this factor.
        elif required_margin < MAX_MARGIN * 0.4 and self.close_only:
            logging.info(f"Required margin: {required_margin}. Quoting everything again.")
            self.close_only = False

    # https://www.thalex.com/docs/#tag/subs_accounting/Account-trade-history
    # Thalex sends us this notification every time we get a fill.
    # We use trades notifications to log the trades done by this quoter for analysis
    def trades_callback(self, trades: List[Dict]):
        lines = []
        for t in trades:
            t = Trade(t)
            if t.label != LABEL:
                continue
            i = self.options.get(t.instrument)
            if i is not None:
                mark = i.ticker.mark_price
                lines.append(
                    f"{mark=:.1f} {t} {self.spread=:.4f} {self.d_skew=:.4f} {self.g_skew=:.4f} {self.v_skew=:.4f} "
                    f"{i.retreat=:.4f} {i.greeks.delta=:.2f} {i.greeks.vega=:.2f} {i.greeks.gamma=:.4f}"
                )
            else:
                lines.append(f"{t}")
        # Our portfolio changed, so we need to recalculate our risk metrics
        self.recalc_greeks()
        # lines being empty means none of the trades were done by this quoter, so we won't log them.
        if len(lines) > 0:
            now = datetime.utcfromtimestamp(utcnow()).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            with open(TRADE_LOG, "a") as file:
                for line in lines:
                    logging.info(f"Traded {line}")
                    file.write(f"{now} " + line + "\n")

    # https://www.thalex.com/docs/#tag/subs_accounting/Session-orders
    # Thalex sends us this notification every time when an order of ours changes for any reason.
    # This is where we keep track of their actual statuses.
    def orders_callback(self, orders: List[Dict]):
        for o in orders:
            i = self.options.get(o["instrument_name"])
            if i is None or o.get("label", "-") != LABEL:
                continue
            side_ind = side_idx(thalex_py.Direction(o["direction"]))
            o = order_from_data(o)
            if o.id is not None:
                i.orders[side_ind] = o
                if not i.orders[side_ind].is_open():
                    i.prices_sent[side_ind] = None

    # This is called if thalex returns an error to one of our requests.
    async def error_callback(self, error, cid=None):
        if cid is not None and cid > 99:
            await self.order_error(error, cid)
        else:
            logging.error(f"{cid=}: error: {error}")

    # https://www.thalex.com/docs/#tag/subs_market_data/Instruments
    # This function should only be called with a snapshot in the instruments channel.
    # If it's not a snapshot, we raise an InstrumentsChanged exception instead of calling this function.
    # See the definition of InstrumentsChanged for an explanation.
    # We iterate over the added instruments and select the options we want to quote.
    async def instruments_callback(self, instruments):
        for i in instruments:
            if "removed" in i:
                logging.error("instruments_callback was called with a removed instrument")
                raise InstrumentsChanged()
        # At this point all the instruments must be "added"
        instruments = [Instrument(i["added"]) for i in instruments]

        # Filter for instruments with our desired underlying
        instruments = [i for i in instruments if i.underlying == UNDERLYING]

        # Filter the options in the expiries that we want to quote
        expiries = list(set([i.expiry_ts for i in instruments if i.type == InstrumentType.OPTION]))
        expiries.sort()
        expiries = expiries[:NUMBER_OF_EXPIRIES_TO_QUOTE]

        # We keep track of all instruments. We'll put the options we quote in self.options,
        # and all the rest in self.unquoted_instruments.
        for i in instruments:
            if i.expiry_ts in expiries and i.type == InstrumentType.OPTION:
                self.options[i.name] = InstrumentData(i)
            else:
                self.unquoted_instruments[i.name] = UnquotedInstrument(i)

        # Subscribe to the tickers of the options we want to quote.
        # We'll subscribe to the tickers of the not quoted instruments later, only if we have a position in those.
        subs = [f"ticker.{opt.instrument.name}.{SUBSCRIBE_INTERVAL}" for opt in self.options.values()]
        await self.thalex.public_subscribe(subs, CALL_ID_SUBSCRIBE)


# This is our main task, where we keep (re)initializing the thalex connector and the option quoter,
# and run the necessary tasks. If anything goes wrong, we'll just reinitialize everything,
# reconnect to thalex and start quoting again.
async def reconnect_and_quote_forever(network):
    keep_going = True  # We set this to false when we want to stop
    while keep_going:
        thalex = thalex_py.Thalex(network=network)
        trader = OptionQuoter(thalex, network)
        try:
            logging.info(f"STARTING on {network.value} {UNDERLYING=}, {NUMBER_OF_EXPIRIES_TO_QUOTE=}")
            await asyncio.gather(trader.listen_task(), trader.greeks_task())
        except (websockets.ConnectionClosed, socket.gaierror) as e:
            # It can (and does) happen that we lose connection for whatever reason. If it happens We wait a little,
            # so that if the connection loss is persistent, we don't just keep trying in a hot loop, then reconnect.
            # An exponential backoff would be nicer.
            logging.error(f"Lost connection ({e}). Reconnecting...")
            time.sleep(0.1)
            continue
        except InstrumentsChanged as e:
            # See the comment at the definition of InstrumentsChanged for an explanation of what's happening here.
            logging.warning(e.message)
            time.sleep(7)
            continue
        except asyncio.CancelledError:
            # This means we are stopping the program from the outside (eg Ctrl+C on OSX/Linux)
            # Set the keep going flag to False, and if we're still connected disconnect and actively cancel our orders
            # in this session instead of letting cancel_on_disconnect kick in.
            keep_going = False
            if thalex.connected():
                # We want to call cancel_session instead of cancel_all here,
                # so that the orders created by other bots or manually on the ui are left alone.
                await thalex.cancel_session(id=CALL_ID_CANCEL_SESSION)
                while True:
                    r = await thalex.receive()
                    r = json.loads(r)
                    if r.get("id", -1) == CALL_ID_CANCEL_SESSION:
                        logging.info(f"Cancelled all {r.get('result', 0)} orders")
                        break
                await thalex.disconnect()


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
    parser = argparse.ArgumentParser(
        description="thalex example trader",
    )
    parser.add_argument("--network", metavar="CSTR")
    parser.add_argument("--log", default="info", metavar="CSTR")
    args = parser.parse_args(sys.argv[1:])

    if args.network == "prod":
        arg_network = thalex_py.Network.PROD
    elif args.network == "test":
        arg_network = thalex_py.Network.TEST
    elif args.network == "dev":
        arg_network = thalex_py.Network.DEV
    else:
        logging.error("network invalid or missing")
        assert False  # network invalid or missing

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
