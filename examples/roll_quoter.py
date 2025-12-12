import asyncio
import csv
import json
import logging
import os
import socket
import time
from datetime import datetime, timezone
from typing import Union, Dict, Optional, List
import enum
import websockets

import thalex as th
import keys  # Rename _keys.py to keys.py and add your keys. There are instructions how to create keys in that file.


# The main idea behind this example is to demonstrate how thalex_py can be used to quote rolls on Thalex.
# It's not financial advice, however you can use it to test the library and write your own strategy.
#
# The behaviour of the example quoter can be tuned with a number of constants defined below.
# The way this is designed, it connects to thalex, gets all instruments,
# and then starts quoting future rolls that have a perpetual leg, and expire within MAX_DTE days.
#
# The actual operation is done in the OptionQuoter class

# These are used to configure how the roll quoter behaves.
UNDERLYING = "BTCUSD"  # We'll only quote rolls of this underlying eg BTCUSD or ETHUSD
MAX_DTE = 2  # Maximum days to expiry to quote. Mqp rewards are within 35 days.
# How often to receive tickers. Choices are 100ms, 200ms, 500ms, 1000ms, 5000ms, 60000ms or raw
SUBSCRIBE_INTERVAL = "1000ms"
FND_MA_1DTE_WINDOW = (
    5  # Length of moving window to average funding rate in seconds for same day expiries
)
FND_MA_3DTE_WINDOW = 60  # Length of moving window to average funding rate in seconds for expiries within 3 days
FND_LONGTERM = -0.0001  # Hard coded funding rate for far away expiries
AMEND_THRESHOLD = 3  # In ticks. We don't amend smaller than this, to avoid throttling.
FND_CAP = 0.00005  # Absolute cap for the fnd ma
# If fnd ma changes by this much, we recalculate quotes.
FND_RECALC_THRESHOLD = 0.0001
# If index changes this many dollars, we recalculate quotes
INDEX_RECALC_THRESHOLD = 2
SPREAD_1DTE = 10  # price spread around fair value for rolls that expire within 1 day
SPREAD_3DTE = 20  # price spread around fair value for rolls that expire within 3 days
SPREAD_LONGTERM = (
    30  # price spread around fair value for rolls that expire in more than 3 days
)
# Store funding rate for reconnection and quick restart data persistence
FND_CSV_PATH = "fnd.csv"
# In seconds. If the data we restore is older than this, we start from scratch
MAX_GAP = 10
SIZE = 0.1
MAX_POSITION = 1.0  # Maximum absolute position of any instrument
# To identify the orders and trades of this quoter. If you run multiple in parallel (eg for different expiries),
# you should give them different labels.
LABEL = "R"
TRADE_LOG = "trades_rolls.log"  # File path for logging trades
NETWORK = th.Network.TEST


# We'll use these to match responses from thalex to the corresponding request.
# The numbers are arbitrary, but they need to be unique per CALL_ID.
# We'll use 100+ for identifying trade requests for orders.
CALL_ID_INSTRUMENTS = 0
CALL_ID_INSTRUMENT = 1
CALL_ID_SUBSCRIBE = 2
CALL_ID_LOGIN = 3
CALL_ID_CANCEL_SESSION = 4
CALL_ID_SET_COD = 5


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


# Partial representation of the leg of a combo instrument on thalex
class ComboLeg:
    def __init__(self, data: Dict):
        self.iname = data["instrument_name"]
        self.quantity = data["quantity"]


# Partial representation of the details of an instrument on thalex
class Instrument:
    def __init__(self, data: Dict):
        self.name: str = data["instrument_name"]
        self.underlying = data["underlying"]
        self.tick_size: float = data["tick_size"]
        self.type: InstrumentType = InstrumentType(data["type"])
        self.option_type: Optional[str] = data.get("option_type")
        self.expiry_ts: Optional[int] = data.get("expiration_timestamp")
        # self.volume_tick: Optional[float] = data.get("volume_tick_size")
        self.legs: List[ComboLeg] = [ComboLeg(d) for d in data.get("legs", [])]

    def __repr__(self):
        return f"Instrument: {self.name}"


# This is the data that we keep about our orders
class Order:
    def __init__(
        self,
        oid: int,  # needs to be unique per order within the session
        price: float,
        side: th.Direction,
        status: Optional[OrderStatus] = None,
    ):
        self.id: int = oid
        self.price: float = price
        self.side: th.Direction = side
        self.status: Optional[OrderStatus] = status

    def is_open(self):
        return self.status in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]

    def __repr__(self):
        st = "None" if self.status is None else self.status.value
        return f"Order({self.id=}, {self.price=}, side={self.side.value}, status={st})"


# This converts the json representation of an order returned by thalex into our Order data structure
def order_from_data(data: Dict) -> Order:
    side = th.Direction(data["direction"])
    price = data.get("price", 0 if side == th.Direction.SELL else float("inf"))
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
        self.direction = th.Direction(data["direction"])
        self.amount: float = data["amount"]
        self.price: float = data["price"]
        self.pos_after: float = data.get("position_after", 0.0)
        self.type = data.get("trade_type", "unknown")
        self.oid = data.get("client_order_id", data.get("order_id", "-"))
        self.index = data.get("index", "-")
        self.maker = data.get("maker_taker", "?")
        self.time = data.get("time", "-")
        self.label = data.get("label", "-")
        self.perp_pnl = data.get("perpetual_funding_pnl", "-")
        self.fnd_mark = data.get("funding_mark", "-")

    def __repr__(self):
        return (
            f"{self.time} {self.maker} {self.direction.value} {self.instrument} {self.amount} @ {self.price} type: "
            f"{self.type}, oid: {self.oid}, label: {self.label}, pos after: {self.pos_after}, "
            f"index: {int(self.index)}, perp_pnl: {self.perp_pnl}, fnd_mark: {self.fnd_mark}"
        )


# Partial representation of an instrument ticker
class Ticker:
    def __init__(self, data: Dict):
        self.mark_price: float = data["mark_price"]
        self.delta: float = data["delta"]
        self.best_bid: float = data.get("best_bid_price")
        self.best_ask: float = data.get("best_ask_price")
        self.mark_ts: float = data["mark_timestamp"]
        self.iv: float = data.get("iv", 0.0)
        self.fwd: float = data["forward"]


# To align the price with instrument tick size.
def round_to_tick(value, tick):
    return tick * round(value / tick)


# We use lists like [bid, ask] in some places. This helps indexing into those.
def side_idx(side: th.Direction):
    return 0 if side == th.Direction.BUY else 1


# We log the results to different requests at a different level to improve log readability
async def result_callback(result, cid=None):
    if cid == CALL_ID_INSTRUMENT:
        pass
    elif cid == CALL_ID_SUBSCRIBE:
        logging.debug(f"Sub successful: {result}")
    elif cid == CALL_ID_LOGIN:
        logging.info(f"Login result: {result}")
    elif cid == CALL_ID_SET_COD:
        logging.info(f"Set cancel on disconnect result: {result}")
    elif cid > 99:
        logging.debug(f"Trade request result: {result}")
    else:
        logging.info(f"{cid=}: {result=}")


#  Cap a number at [-cap, cap]
def apply_cap(value, cap):
    return max(-cap, min(value, cap))


# The timestamps returned by thalex are in utc timezone. Let's use the same.
def utcnow() -> float:
    return datetime.now(timezone.utc).timestamp()


# This is the data that we want to track about roll instruments
class RollData:
    def __init__(self, instrument: Instrument):
        self.instrument: Instrument = instrument
        self.prices_sent: List[Optional[float]] = [None, None]  # [bid, ask]
        self.orders: List[Optional[Order]] = [None, None]  # [bid, ask]
        for leg in instrument.legs:
            # We only quote rolls that have a perp leg, so it should be safe to assume that the other leg is a future
            if "PERPETUAL" not in leg.iname:
                self.future_leg: str = leg.iname
                break


# This is the data that we want to track about future instruments
class FutureData:
    def __init__(self, instrument: Instrument):
        self.instrument: Instrument = instrument
        self.prices_sent: List[Optional[float]] = [None, None]  # [bid, ask]
        self.orders: List[Optional[Order]] = [None, None]  # [bid, ask]
        self.ticker: Optional[Ticker] = None
        self.position: float = 0.0


# This is the data that we want to track about the perpetual
class PerpData:
    def __init__(self, instrument: Instrument):
        self.instrument: Instrument = instrument
        self.ticker_sub: str = f"ticker.{instrument.name}.{SUBSCRIBE_INTERVAL}"
        self.mark: Optional[float] = None
        self.position: float = 0.0
        # If there's some data we stored in write_fnd_to_disk() we try to restore it
        if os.path.exists(FND_CSV_PATH):
            with open(FND_CSV_PATH, "r") as file:
                csv_reader = csv.reader(file)
                fnd = [tuple(map(float, row)) for row in csv_reader]
                now = utcnow()
                if len(fnd) > 0:
                    logging.info(
                        f"Found {len(fnd)} data points from {now - fnd[0][0]:.0f} - "
                        f"{now - fnd[len(fnd) - 1][0]:.0f} seconds ago"
                    )
                if fnd[len(fnd) - 1][0] < now - MAX_GAP:
                    # We were out for too long. We have to throw this away.
                    fnd = []
        else:
            fnd = []
        self.ticker_ma: List[(float, float)] = fnd  # (timestamp, funding rate)
        self.trim()
        logging.info(f"Restored {len(self.ticker_ma)} data points")

    # If we lose connection to the exchange or we restart the quoter, we store the data,
    # so that we don't have to start to rebuild our moving average from scratch
    def write_fnd_to_disk(self):
        self.trim()
        logging.info(f"Storing {len(self.ticker_ma)} data points")
        with open(FND_CSV_PATH, "w", newline="") as file:
            csv_writer = csv.writer(file)
            csv_writer.writerows(self.ticker_ma)

    # This throws data away that's already too old
    def trim(self):
        start = utcnow() - FND_MA_3DTE_WINDOW
        i_start = 0
        for i, (ticker_ts, ticker_fnd) in enumerate(self.ticker_ma):
            if ticker_ts >= start:
                i_start = i
                break
        self.ticker_ma = self.ticker_ma[i_start:]


# This is the class that does the actual quoting by reacting to notifications in the listen_task()
# The quote prices are calculated in pricing().
# We'll adjust our quotes if there's a notification in the perpetual ticker or index channels or if we trade.
class RollQuoter:
    def __init__(self, thalex: th.Thalex):
        self.thalex = thalex
        self.futures: Dict[str, FutureData] = {}
        self.rolls: Dict[str, RollData] = {}
        self.perp: Optional[PerpData] = None
        self.index: Optional[float] = None
        # Funding rate moving average for pricing same day expiries
        self.fnd_ma_1dte: float = 0.0
        # Funding rate moving average for pricing expiries within 3 days
        self.fnd_ma_3dte: float = 0.0
        self.last_quote_fnd_ma = float("inf")  # The last value we adjusted quotes at
        self.last_quote_index = 0.0  # The last value we adjusted quotes at
        self.client_order_id: int = 100

    # We iterate self.perp.ticker_ma, and we calculate an average of the funding rate data points that are
    # not older than FND_MA_1DTE_WINDOW, and an other that's not older than FND_MA_3DTE_WINDOW.
    def recalc_fnd_ma(self, now):
        # The pairs are [1dte, 3dte]
        dte1_idx = 0
        dte3_idx = 1
        ma_start = [now - FND_MA_1DTE_WINDOW, now - FND_MA_3DTE_WINDOW]
        i_start = [None, None]
        sums = [0, 0]

        # The data in self.perp.ticker_ma is in chronological order as we received it.
        # We find the indices at which the data that's not older than
        for i, (ticker_ts, ticker_fnd) in enumerate(self.perp.ticker_ma):
            for dte_idx in [dte1_idx, dte3_idx]:
                if ticker_ts >= ma_start[dte_idx]:
                    sums[dte_idx] += ticker_fnd
                    if i_start[dte_idx] is None:
                        i_start[dte_idx] = i

        if i_start[dte1_idx] is None:
            # Meaning we don't have any data that's not older than FND_MA_1DTE_WINDOW
            return

        # Calculate the moving averages
        len_1dte = len(self.perp.ticker_ma) - i_start[0]
        len_3dte = len(self.perp.ticker_ma) - i_start[1]
        self.fnd_ma_1dte = apply_cap(sums[0] / len_1dte, FND_CAP)
        self.fnd_ma_3dte = apply_cap(sums[0] / len_3dte, FND_CAP)

        self.perp.ticker_ma = self.perp.ticker_ma[i_start[1] :]  # Throw old data away

    # This is where we create our quote prices, based on live data and the constants at the top of this file.
    # First we calculate a fair value per roll according to:
    #     fair_value = days_to_expiry * funding_rate * index_price
    # Then we create a spread around this fair value.
    # We have 3 different categories of rolls: the one that expiries within a day, tho ones that expire withn 3 days,
    # and the ones that expire in more than 3 days.
    # For the ones that expire within 3 days we keep a moving average in two different windows,
    # and for the ones that expire outside of that, we have a manually set funding rate.
    def pricing(self, roll: RollData, now: float) -> List[Optional[float]]:  # [bid, ask]
        # We decide which funding rate and spread to use based on days to expiry
        dte = (roll.instrument.expiry_ts - now) / (24 * 3600)
        if dte > MAX_DTE or len(self.perp.ticker_ma) == 0:
            return [None, None]  # too far in the future or we don't have fnd data
        elif dte <= 1:
            if now - self.perp.ticker_ma[0][0] < FND_MA_1DTE_WINDOW:
                # The funding rate we collected is not enough yet to quote this
                return [None, None]
            fnd = self.fnd_ma_1dte
            spread = SPREAD_1DTE
        elif dte <= 3:
            if now - self.perp.ticker_ma[0][0] < FND_MA_3DTE_WINDOW:
                # The funding rate we collected is not enough yet to quote this
                return [None, None]
            fnd = self.fnd_ma_3dte
            spread = SPREAD_3DTE
        else:
            fnd = FND_LONGTERM
            spread = SPREAD_LONGTERM

        # Calculate tha actual fair value and create a spread around it
        fair_value = dte * fnd * self.index
        bid = round_to_tick(fair_value - spread, roll.instrument.tick_size)
        ask = round_to_tick(fair_value + spread, roll.instrument.tick_size)

        # If our absolute position in one of the legs of this roll have already reached MAX_POSITION
        # we don't want to increase it further
        if self.perp.position > MAX_POSITION:
            ask = None
        elif self.perp.position < -MAX_POSITION:
            bid = None
        fut_leg_pos = self.futures[roll.future_leg].position
        if fut_leg_pos > MAX_POSITION:
            bid = None
        elif fut_leg_pos < -MAX_POSITION:
            ask = None

        return [bid, ask]

    # Insert/Amend/Cancel our bid & ask for one instrument based on the quotes pricing() comes up with,
    # and what we know about the status of our already open orders for the instrument (if any).
    async def adjust_quotes(self, now: float):
        if self.index is None:
            logging.error("Don't have an index to adjust quotes to")
            return
        for roll in self.rolls.values():
            quote = self.pricing(roll, now)  # [bid, ask]
            logging.debug(f"Quotes for {roll.instrument}: {quote}")
            for idx, side in [
                (0, th.Direction.BUY),
                (1, th.Direction.SELL),
            ]:
                price = quote[idx]
                sent = roll.prices_sent[idx]
                if price is None:
                    if sent is not None or (
                        roll.orders[idx] is not None and roll.orders[idx].is_open()
                    ):
                        # We have an open order that we want to cancel.
                        # If the order is open because the previous cancel was not yet processed, we might be trying to
                        # delete it twice, but that should be rare and even then it's better to be safe than sorry.
                        client_order_id = roll.orders[idx].id
                        logging.debug(
                            f"Cancel {client_order_id} {side.value} {roll.instrument}"
                        )
                        await self.thalex.cancel(
                            client_order_id=client_order_id,
                            id=client_order_id,
                        )
                elif sent is None:
                    # This is a new quote. Let's insert it.
                    # We might have one already open, for which a cancel is in flight, leave that to its fate.
                    roll.prices_sent[idx] = price
                    client_order_id = self.client_order_id
                    self.client_order_id = self.client_order_id + 1
                    roll.orders[idx] = Order(client_order_id, price, side)
                    logging.debug(
                        f"Insert {client_order_id} {roll.instrument.name} {side.value} {price}"
                    )
                    await self.thalex.insert(
                        direction=side,
                        instrument_name=roll.instrument.name,
                        amount=SIZE,
                        price=price,
                        post_only=True,
                        label=LABEL,
                        client_order_id=client_order_id,
                        id=client_order_id,
                    )
                elif abs(price - sent) >= AMEND_THRESHOLD * roll.instrument.tick_size:
                    # Neither price nor sent are none, and they differ enough for us to want to amend the order.
                    # We only amend if the difference is large enough, to avoid throttling.
                    roll.prices_sent[idx] = price
                    client_order_id = roll.orders[idx].id
                    logging.debug(
                        f"Amend {client_order_id} {roll.instrument.name} {side.value} {sent} -> {price}"
                    )
                    await self.thalex.amend(
                        amount=SIZE,
                        price=price,
                        client_order_id=client_order_id,
                        id=client_order_id,
                    )
        self.last_quote_fnd_ma = self.fnd_ma_1dte
        self.last_quote_index = self.index

    # If there's something wrong with what we sent, this will revert our idea of the order to that of thalex,
    # and take some action if necessary
    async def order_error(self, error, oid):
        # First we find the order based on its id.
        # We could store an order id to order map to avoid this linear search, but we shouldn't need to do this often.
        for idata in self.rolls.values():
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
                            logging.info(
                                f"Order({oid}) already filled/cancelled: {order.side.value} "
                                f"{idata.instrument.name}"
                            )
                        else:
                            # We failed to pull an order for some other reason. That's bad.
                            # Best we can do is try again immediately
                            logging.error(
                                f"Failed to cancel {order} for {idata.instrument}: {error}"
                            )
                            await self.thalex.cancel(client_order_id=oid, id=oid)
                    elif not order.is_open():
                        # We don't have an open order. We'll leave it at that for now and insert one later.
                        idata.prices_sent[side] = None
                        if error["code"] == ERROR_CODE_THROTTLE:
                            logging.info(
                                f"Insert {order} @{sent} {idata.instrument.name} throttled"
                            )
                        else:
                            # If it's not just throttling, let's show some elaborate logs
                            # to be able to analyze what was wrong with the order.
                            logging.warning(
                                f"Failed to insert {order} for {idata.instrument}: {error}\n"
                                f"{self.fnd_ma_1dte=:.5f} {self.fnd_ma_1dte=:.5f} "
                            )
                    else:
                        # We did send some price and there's an open order
                        if error["code"] == ERROR_CODE_THROTTLE:
                            # If you see this a lot, you should decrease RECALC_THRESHOLD, MAX_DTE or
                            # increase AMEND_THRESHOLD in order to amend less frequently
                            logging.info(
                                f"Amend {order} for {idata.instrument} -> {sent} throttled"
                            )
                        else:
                            logging.warning(
                                f"Failed to amend order({oid}) {order.side.value} {idata.instrument.name} "
                                f"{order.price} -> {sent}: {error}\n"
                            )
                        if (
                            abs(order.price - sent)
                            >= 2 * AMEND_THRESHOLD * idata.instrument.tick_size
                        ):
                            # The order is open at the wrong price, and we're getting throttled,
                            # so we won't try to amand again, but cancel it for now and reinsert it later.
                            idata.prices_sent[side] = None
                            await self.thalex.cancel(client_order_id=oid, id=oid)
                    return  # The order was found so let's not iterate over the rest
        # The order wasn't found above
        logging.error(f"Error with unknown order({oid}): {error}")

    # This is called if thalex returns an error to one of our requests.
    async def error_callback(self, error, cid=None):
        if cid > 99:
            await self.order_error(error, cid)
        else:
            logging.error(f"{cid=}: error: {error}")

    # Thalex sends us this notification every time when an order of ours changes for any reason.
    # This is where we keep track of their actual statuses.
    def orders_callback(self, orders: List[Dict]):
        for o in orders:
            i = self.rolls.get(o["instrument_name"])
            if i is None or o.get("label", "-") != LABEL:
                continue
            side_ind = side_idx(th.Direction(o["direction"]))
            o = order_from_data(o)
            i.orders[side_ind] = o
            if not i.orders[side_ind].is_open():
                i.prices_sent[side_ind] = None

    # https://www.thalex.com/docs/#tag/subs_market_data/Ticker
    # This is where we handle ticker notification sent by thalex
    async def ticker_callback(self, channel, ticker):
        now = utcnow()
        if channel == self.perp.ticker_sub:
            self.perp.mark = ticker["mark_price"]
            self.perp.ticker_ma.append((ticker["mark_timestamp"], ticker["funding_rate"]))
            self.index = ticker["index"]
            self.recalc_fnd_ma(now)
            if abs(self.fnd_ma_1dte - self.last_quote_fnd_ma) > FND_RECALC_THRESHOLD:
                await self.adjust_quotes(now)
        else:
            iname = channel.split(".")[1]
            i = self.futures.get(iname)
            if i is not None:
                i.ticker = Ticker(ticker)

    # https://www.thalex.com/docs/#tag/subs_market_data/Index-price
    # Thalex sends us the underlying index price in this channel. If it changed enough, we adjust quotes.
    async def index_callback(self, index: float):
        self.index = index
        if abs(index - self.last_quote_index) > INDEX_RECALC_THRESHOLD:
            await self.adjust_quotes(utcnow())

    # https://www.thalex.com/docs/#tag/subs_market_data/Instruments
    # We iterate over the instruments, pop the removed ones from our dicts, and start tracking the new ones we want.
    async def instruments_callback(self, instruments):
        subs = []  # So that we call subscribe only once, with all the new channels
        for one_i in instruments:
            added = one_i.get("added")
            if added is not None:
                i = Instrument(added)
                if i.underlying == UNDERLYING:
                    if i.type == InstrumentType.PERP:
                        # If the perpetual was "added" it means that we're starting up,
                        # and this notification is a snapshot with all active instruments.
                        self.perp = PerpData(i)
                        subs.append(self.perp.ticker_sub)
                    elif i.type == InstrumentType.COMBO:
                        self.rolls[i.name] = RollData(i)
                    elif i.type == InstrumentType.FUTURE:
                        self.futures[i.name] = FutureData(i)
            removed = one_i.get("removed")
            if removed is not None:
                i = Instrument(removed)
                # -1 because we don't care if i wasn't in there
                self.rolls.pop(i.name, -1)
                self.futures.pop(i.name, -1)

        # Filter the rolls without a perp leg out
        to_pop = []
        for roll in self.rolls.values():
            if self.perp.instrument.name not in [
                leg.iname for leg in roll.instrument.legs
            ]:
                to_pop.append(roll.instrument.name)
        for tp in to_pop:
            self.rolls.pop(tp)

        for fut in self.futures.keys():
            subs.append(f"ticker.{fut}.{SUBSCRIBE_INTERVAL}")
        await self.thalex.public_subscribe(subs, id=CALL_ID_SUBSCRIBE)

    # This task connects to thalex, logs in, sets up basic subscriptions,
    # and then listens to the responses from thalex and reacts to them.
    async def listen_task(self):
        await self.thalex.connect()
        await self.thalex.login(
            keys.key_ids[NETWORK],
            keys.private_keys[NETWORK],
            id=CALL_ID_LOGIN,
        )
        await self.thalex.set_cancel_on_disconnect(6, id=CALL_ID_SET_COD)
        await self.thalex.public_subscribe(
            ["instruments", f"price_index.{UNDERLYING}"], id=CALL_ID_SUBSCRIBE
        )
        await self.thalex.private_subscribe(
            ["session.orders", "account.trade_history", "account.portfolio"],
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
            await self.ticker_callback(channel, notification)
        elif channel == f"price_index.{UNDERLYING}":
            await self.index_callback(notification["price"])
        elif channel == "session.orders":
            self.orders_callback(notification)
        elif channel == "account.trade_history":
            await self.trades_callback(notification)
        elif channel == "instruments":
            await self.instruments_callback(notification)
        elif channel == "account.portfolio":
            self.portfolio_callback(notification)
        else:
            logging.error(f"Notification for unknown channel: {channel}")

    # https://www.thalex.com/docs/#tag/subs_accounting/Account-portfolio
    # Thalex sends us this notification every time our portfolio changes.
    # We use this to track our positions in the legs of the rolls.
    def portfolio_callback(self, portfolio: List[Dict]):
        for position in portfolio:
            iname = position["instrument_name"]
            if iname == self.perp.instrument.name:
                self.perp.position = position["position"]
            else:
                i = self.futures.get(iname)
                if i is not None:
                    i.position = position["position"]

    # https://www.thalex.com/docs/#tag/subs_accounting/Account-trade-history
    # Thalex sends us this notification every time we get a fill.
    # We use trades notifications to log the trades done by this quoter for analysis
    async def trades_callback(self, trades: List[Dict]):
        lines = []
        for t in trades:
            t = Trade(t)
            if t.label != LABEL:
                continue
            i = self.futures.get(t.instrument)
            if i is not None and i.ticker is not None:
                mark = i.ticker.mark_price
                lines.append(f"{mark=:.1f} {t}")
            elif t.instrument == self.perp.instrument.name:
                mark = self.perp.mark
                lines.append(f"{mark=:.1f} {t}")
            else:
                lines.append(f"{t}")
        # lines being empty means none of the trades were done by this quoter, so we won't log them.
        if len(lines) == 0:
            return
        lines.append(f"{self.fnd_ma_1dte=:.5f} {self.fnd_ma_3dte=:.5f}")
        now = utcnow()
        now_str = datetime.utcfromtimestamp(now).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        with open(TRADE_LOG, "a") as file:
            for line in lines:
                logging.info(f"Traded {line}")
                file.write(f"{now_str} " + line + "\n")
        await self.adjust_quotes(now)


# This is our main task, where we keep (re)initializing the thalex connector and the roll quoter,
# and run the necessary tasks. If anything goes wrong, we'll just reinitialize everything,
# reconnect to thalex and start quoting again.
async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    )
    keep_going = True  # We set this to false when we want to stop
    while keep_going:
        thalex = th.Thalex(network=NETWORK)
        roll_quoter = RollQuoter(thalex)
        try:
            logging.info(f"STARTING on {NETWORK} {UNDERLYING=}, {MAX_DTE=}")
            await asyncio.gather(roll_quoter.listen_task())
        except (websockets.ConnectionClosed, socket.gaierror) as e:
            # It can (and does) happen that we lose connection for whatever reason. If it happens We wait a little,
            # so that if the connection loss is persistent, we don't just keep trying in a hot loop, then reconnect.
            # An exponential backoff would be nicer.
            logging.error(f"Lost connection ({e}). Reconnecting...")
            time.sleep(0.1)
            # Before reinitializing the quoter let's save the data we gathered,
            # so that we don't have to start from scratch
            if roll_quoter.perp is not None:
                roll_quoter.perp.write_fnd_to_disk()
            continue
        except asyncio.CancelledError:
            # This means we are stopping the program from the outside (eg Ctrl+C on OSX/Linux)
            # Set the keep going flag to False, and if we're still connected disconnect and actively cancel our orders
            # in this session instead of letting cancel_on_disconnect kick in.
            keep_going = False
            if roll_quoter.perp is not None:
                roll_quoter.perp.write_fnd_to_disk()
            if thalex.connected():
                await thalex.cancel_session(id=CALL_ID_CANCEL_SESSION)
                while True:
                    r = await thalex.receive()
                    r = json.loads(r)
                    if r.get("id", -1) == CALL_ID_CANCEL_SESSION:
                        logging.info("Cancelled session orders")
                        break
                await thalex.disconnect()


asyncio.run(main())
