import copy
import enum
import argparse
import asyncio
import json
import logging
import math
import socket
import sys
import signal
import os
import time
from typing import Dict, Optional, List

import websockets

import black_scholes
import keys  # Rename _keys.py to keys.py and add your keys

import process_msg
import thalex_py
from black_scholes import Greeks

# The main idea behind this example is to demonstrate how thalex_py can be used to trade on Thalex.
# It's not meant to be a profitable strategy, however you can use it to test the library and
# write your own.
#
# The way this is designed, it connects to thalex, gets all instruments and then starts quoting options in
# the nearest NUMBER_OF_EXPIRIES_TO_QUOTE expiries.
#
# Technically this is done in a number of async tasks running in ~parallel:
#  - greeks_task: Simply recalculates the greeks for the portfolio periodically
#  - hedge_task: Sets up basic subscriptions and makes the call to thalex.instruments.
#                The response for that is going to be processed in process_instruments,
#                subscribing to all the relevant tickers which trigger quote adjustments.
#                After the initial setup hedge_task periodically checks if the portfolio breached
#                2*MAX_DELTAS and hedges them if necessary.
#  - listen_task: This task listens to the responses from the exchange and reacts to them. It uses process_msg
#                 as a helper, that'll call the appropriate callback functions when thalex sends a subscription
#                 notification or any other message.
#                 The ticker_callback is called when there's a ticker notification, this is going to result in
#                 quotes being adjusted.

# We'll use these to match responses from thalex to the corresponding request.
CALL_ID_INSTRUMENTS = 0
CALL_ID_INSTRUMENT = 1
CALL_ID_SUBSCRIBE = 2
CALL_ID_LOGIN = 3
CALL_ID_CANCEL_ALL = 4
CALL_ID_HEDGE = 5

# These are used to configure how the trader behaves.
UNDERLYING = "BTCUSD"  # We'll only quote options of this underlying
NUMBER_OF_EXPIRIES_TO_QUOTE = 2  # More expiries means more quotes, but more throttling
SUBSCRIBE_INTERVAL = "1000ms"  # Also how frequently we adjust quotes
DELTA_RANGE_TO_QUOTE = (0.2, 0.7)  # Wider range means more quotes, but more throttling
EDGE = 40  # base edge to create a spread around the mark price
EDGE_FACTOR = 1.5  # extra edge per open delta
LOTS = 0.1  # Default options quote size
AMEND_THRESHOLD = 3  # In ticks. We don't amend smaller than this, to avoid throttling.
RETREAT = 0.002  # Skew prices for open position size
MAX_MARGIN = 10000  # If the required margin goes above this, we only reduce positions.
# If the deltas go outside (-MAX_DELTAS, MAX_DELTAS), we'll only insert quotes that reduces their absolute value.
# If the absolute deltas breach 2*MAX_DELTAS, we'll hedge half of them with HEDGE_INSTRUMENT
MAX_DELTAS = 0.8
MAX_OPEN_OPTION_DELTAS = 5.0  # We won't increase open option positions above this.
HEDGE_INSTRUMENT = "BTC-PERPETUAL"  # It's assumed to be d1
DELTA_SKEW = 500  # To skew quote prices for portfolio delta
VEGA_SKEW = 20  # To skew quote prices for portfolio vega
GAMMA_SKEW = 2000000  # To skew quote prices for portfolio gamma
MAX_VEGA = 20  # If we breach this, we only reduce positions
MAX_GAMMA = 0.01  # If we breach this, we only reduce positions


class InstrumentType(enum.Enum):
    OPTION = "option"
    FUTURE = "future"
    COMBO = "combination"
    PERP = "perpetual"


class OrderStatus(enum.Enum):
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    CANCELLED = "cancelled"
    CANCELLED_PARTIALLY_FILLED = "cancelled_partially_filled"
    FILLED = "filled"


def is_open(status: OrderStatus):
    return status in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]


class Instrument:
    def __init__(self, data: Dict):
        self.name: str = data["instrument_name"]
        self.underlying = data["underlying"]
        self.tick_size: float = data["tick_size"]
        self.type: InstrumentType = InstrumentType(data["type"])
        self.option_type = data.get("option_type")
        self.expiry_ts: int = data.get("expiration_timestamp")
        self.strike_price: int = data.get("strike_price")
        self.volume_tick: float = data.get("volume_tick_size")


def is_option(instrument_name: str):
    return instrument_name.count("-") == 3


class Order:
    def __init__(self, id: int, price: float, side: thalex_py.Direction):
        self.id: int = id
        self.price: float = price
        self.side: thalex_py.Direction = side
        self.status: Optional[OrderStatus] = None

    def update(self, data: Dict):
        self.status = OrderStatus(data["status"])
        self.price = data.get(
            "price", 0 if self.side == thalex_py.Direction.SELL else float("inf")
        )

    def __repr__(self):
        st = "None" if self.status is None else self.status.value
        return f"Order({self.id=}, {self.price=}, side={self.side.value}, status={st})"


class Trade:
    def __init__(self, data: Dict):
        self.instrument: str = data["instrument_name"]
        self.direction = thalex_py.Direction(data["direction"])
        self.amount: float = data["amount"]
        self.price: float = data["price"]


class Ticker:
    def __init__(self, data: Dict):
        self.mark_price: float = data["mark_price"]
        self.delta: float = data["delta"]
        self.best_bid: float = data.get("best_bid_price")
        self.best_ask: float = data.get("best_ask_price")
        self.mark_ts: float = data["mark_timestamp"]
        self.iv: float = data.get("iv", 0.0)
        self.fwd: float = data["forward"]


class InstrumentData:
    def __init__(self, instrument: Instrument):
        self.instrument: Instrument = instrument
        self.ticker: Optional[Ticker] = None
        self.greeks = Greeks()
        self.quote_prices: List[Optional[float]] = [None, None]  # [bid, ask]
        self.prices_sent: List[Optional[float]] = [None, None]  # [bid, ask]
        self.orders: List[Optional[Order]] = [None, None]  # [bid, ask]
        self.position: float = 0.0
        self.edge = 0
        self.retreat = 0
        self.d_skew = 0
        self.g_skew = 0
        self.v_skew = 0


def side_idx(side: thalex_py.Direction):
    return 0 if side == thalex_py.Direction.BUY else 1


def round_to_tick(value, tick):
    return tick * round(value / tick)


class Trader:
    def __init__(self, thalex: thalex_py.Thalex, network: thalex_py.Network):
        self.thalex = thalex
        self.network = network
        self.options: Dict[str, InstrumentData] = {}
        self.processor = process_msg.Processor()
        self.eligible_instruments = set()
        self.client_order_id: int = 100
        self.hedge_instrument: Optional[InstrumentData] = None
        self.close_only = False
        self.open_opt_deltas = 0.0
        self.greeks = Greeks()
        self.can_trade = False

    async def hedge_task(self):
        await self.thalex.connect()
        token = thalex_py.make_auth_token(
            keys.key_ids[self.network], keys.private_keys[self.network]
        )
        await self.thalex.login(token, id=CALL_ID_LOGIN)
        await self.thalex.set_cancel_on_disconnect(6)
        self.processor.add_callback(process_msg.Channel.LWT, self.lwt_callback)
        self.processor.add_callback(process_msg.Channel.TICKER, self.ticker_callback)
        self.processor.add_callback(process_msg.Channel.BOOK, self.book_callback)
        self.processor.add_callback(process_msg.Channel.ORDERS, self.orders_callback)
        self.processor.add_callback(
            process_msg.Channel.TRADE_HISTORY, self.trades_callback
        )
        self.processor.add_callback(
            process_msg.Channel.PORTFOLIO, self.portfolio_callback
        )
        self.processor.add_callback(
            process_msg.Channel.ACCOUNT_SUMMARY, self.account_summary_callback
        )
        self.processor.add_result_callback(self.result_callback)
        self.processor.add_error_callback(self.error_callback)
        await self.thalex.instruments(CALL_ID_INSTRUMENTS)
        await self.thalex.private_subscribe(
            [
                "account.orders",
                "account.trade_history",
                "account.portfolio",
                "account.summary",
            ],
            CALL_ID_SUBSCRIBE,
        )
        while True:
            await self.hedge_deltas()
            await asyncio.sleep(300)

    async def hedge_deltas(self):
        deltas: float = (
            0 if self.hedge_instrument is None else self.hedge_instrument.position
        )
        self.open_opt_deltas = 0
        for i in self.options.values():
            d = i.position * i.ticker.delta
            deltas += d
            self.open_opt_deltas += abs(d)
        if abs(deltas) > MAX_DELTAS * 2 and self.hedge_instrument is not None:
            side = thalex_py.Direction.SELL if deltas > 0 else thalex_py.Direction.BUY
            # Assuming we're hedging with D1 instrument
            amount = round_to_tick(
                abs(deltas / 2.0), self.hedge_instrument.instrument.volume_tick
            )
            logging.info(f"Hedging {math.copysign(amount, deltas):2} deltas")
            await self.thalex.insert(
                direction=side,
                order_type=thalex_py.OrderType.MARKET,
                instrument_name=HEDGE_INSTRUMENT,
                amount=amount,
                id=CALL_ID_HEDGE,
            )

    async def greeks_task(self):
        await asyncio.sleep(3)
        self.recalc_greeks()
        self.can_trade = True
        while True:
            await asyncio.sleep(30)
            self.recalc_greeks()

    def recalc_greeks(self):
        self.greeks = Greeks(self.hedge_instrument.position)
        self.open_opt_deltas = 0
        for i in self.options.values():
            maturity = (i.instrument.expiry_ts - i.ticker.mark_ts) / (
                365.25 * 24 * 3600
            )
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

    def pricing(self, i: InstrumentData) -> [float, float]:  # [bid, ask]
        within_range = (
            i.instrument.type == InstrumentType.OPTION
            and DELTA_RANGE_TO_QUOTE[0] < abs(i.ticker.delta) < DELTA_RANGE_TO_QUOTE[1]
        )
        want_to_quote = (within_range and not self.close_only) or i.position != 0
        if not want_to_quote or not self.can_trade:
            return [None, None]

        i.retreat = (
            RETREAT * i.ticker.mark_price * i.instrument.tick_size * i.position / LOTS
        )
        i.d_skew = self.greeks.delta * i.greeks.delta * DELTA_SKEW * LOTS
        i.v_skew = (self.greeks.vega / MAX_VEGA) * i.greeks.vega * VEGA_SKEW * LOTS
        i.g_skew = (self.greeks.gamma / MAX_GAMMA) * i.greeks.gamma * GAMMA_SKEW * LOTS
        i.edge = max(1.0, self.open_opt_deltas * EDGE_FACTOR) * EDGE
        bid = i.ticker.mark_price - i.edge - i.retreat - i.d_skew - i.v_skew - i.g_skew
        ask = i.ticker.mark_price + i.edge - i.retreat - i.d_skew - i.v_skew - i.g_skew
        bid = round_to_tick(bid, i.instrument.tick_size)
        if bid < i.instrument.tick_size:
            bid = None
        ask = round_to_tick(ask, i.instrument.tick_size)
        if (
            (self.close_only and i.position >= 0)
            or (MAX_VEGA < self.greeks.vega and i.position >= 0)
            or (MAX_GAMMA < self.greeks.gamma and i.position >= 0)
            or (MAX_OPEN_OPTION_DELTAS < self.open_opt_deltas and i.position >= 0)
            or (MAX_DELTAS < self.greeks.delta and i.ticker.delta > 0)
            or (-MAX_DELTAS > self.greeks.delta and i.ticker.delta < 0)
        ):
            bid = None
        if (
            (self.close_only and i.position <= 0)
            or (MAX_VEGA < self.greeks.vega and i.position <= 0)
            or (MAX_GAMMA < self.greeks.gamma and i.position <= 0)
            or (MAX_OPEN_OPTION_DELTAS < self.open_opt_deltas and i.position <= 0)
            or (MAX_DELTAS < self.greeks.delta and i.ticker.delta < 0)
            or (-MAX_DELTAS > self.greeks.delta and i.ticker.delta > 0)
        ):
            ask = None
        return [bid, ask]

    async def adjust_quotes(self, i: InstrumentData):
        i.quote_prices = self.pricing(i)
        for idx, side in [(0, thalex_py.Direction.BUY), (1, thalex_py.Direction.SELL)]:
            price = i.quote_prices[idx]
            sent = i.prices_sent[idx]
            if price is None:
                if sent is not None and is_open(i.orders[idx].status):
                    # We have an open order that we want to cancel.
                    i.prices_sent[idx] = None
                    client_order_id = i.orders[idx].id
                    logging.debug(
                        f"Cancel {client_order_id} {i.instrument.name} {side.value}"
                    )
                    await self.thalex.cancel(
                        client_order_id=client_order_id,
                        id=client_order_id,
                    )
            elif sent is None:
                # We didn't send any quote to the exchange, this is a new one. Let's insert it.
                i.prices_sent[idx] = price
                if i.orders[idx] is None:
                    client_order_id = self.client_order_id
                    self.client_order_id = self.client_order_id + 1
                    i.orders[idx] = Order(client_order_id, price, side)
                else:
                    client_order_id = i.orders[idx].id
                logging.debug(
                    f"Insert {client_order_id} {i.instrument.name} {side.value} {price}"
                )
                await self.thalex.insert(
                    direction=side,
                    instrument_name=i.instrument.name,
                    amount=LOTS,
                    price=price,
                    post_only=True,
                    client_order_id=client_order_id,
                    id=client_order_id,
                )
            elif abs(price - sent) >= AMEND_THRESHOLD * i.instrument.tick_size:
                # We only amend if the difference is large enough, to avoid throttling.
                i.prices_sent[idx] = price
                client_order_id = i.orders[idx].id
                logging.debug(
                    f"Amend {client_order_id} {i.instrument.name} {side.value} {sent} -> {price}"
                )
                await self.thalex.amend(
                    amount=LOTS,
                    price=price,
                    client_order_id=client_order_id,
                    id=client_order_id,
                )

    async def handle_throttled(self, oid):
        # If you see this a lot, you should decrease NUMBER_OF_EXPIRIES_TO_QUOTE or DELTA_RANGE_TO_QUOTE.
        logging.warning(f"Throttled request for order: {oid}")
        await asyncio.sleep(0.2)
        for idata in self.options.values():
            for order in idata.orders:
                if order is not None and order.id == oid:
                    if order.status is None:
                        # order not yet confirmed on the exchange,
                        # so it was an insert that got throttled
                        idata.prices_sent[side_idx(order.side)] = None
                    await self.adjust_quotes(idata)
                    return

    async def listen_task(self):
        while not self.thalex.connected():
            await asyncio.sleep(0.5)
        while True:
            msg = await self.thalex.receive()
            await self.processor.process_msg(msg)

    async def lwt_callback(self, channel, lwt):
        logging.info(f"{channel}: {lwt}")

    async def ticker_callback(self, channel, ticker):
        iname = channel.split(".")[1]
        if iname == HEDGE_INSTRUMENT:
            self.hedge_instrument.ticker = Ticker(ticker)
        elif is_option(iname):
            i = self.options[iname]
            i.ticker = Ticker(ticker)
            await self.adjust_quotes(i)

    async def book_callback(self, channel, book):
        logging.info(f"{channel}: {book}")

    async def portfolio_callback(self, _, portfolio):
        for position in portfolio:
            iname = position["instrument_name"]
            if iname == HEDGE_INSTRUMENT:
                self.hedge_instrument.position = position["position"]
            else:
                i = self.options.get(iname)
                if i is not None:
                    i.position = position["position"]

    async def account_summary_callback(self, _, acc_sum):
        required_margin = acc_sum["required_margin"]
        if required_margin > MAX_MARGIN and not self.close_only:
            logging.warning(
                f"Required margin {required_margin} going into margin breach mode"
            )
            self.close_only = True
        elif required_margin < MAX_MARGIN * 0.4 and self.close_only:
            logging.info(f"Required margin {required_margin} quoting everything")
            self.close_only = False

    async def trades_callback(self, _, trades):
        for t in trades:
            t = Trade(t)
            mark = None
            if t.instrument == HEDGE_INSTRUMENT and self.hedge_instrument is not None:
                mark = self.hedge_instrument.ticker.mark_price
            else:
                i = self.options.get(t.instrument)
                if i is not None:
                    mark = i.ticker.mark_price
            if mark is None:
                logging.info(
                    f"Traded {t.instrument}: {t.direction.value} {t.amount}@{t.price}"
                )
            else:
                logging.info(
                    f"Traded {t.instrument}: {t.direction.value} {t.amount}@{t.price} {mark=:.1f}"
                )
        self.recalc_greeks()

    async def orders_callback(self, _, orders):
        for o in orders:
            i = self.options.get(o["instrument_name"])
            if i is None:
                continue
            side_ind = side_idx(thalex_py.Direction(o["direction"]))
            order = i.orders[side_ind]
            order.update(o)
            if not is_open(order.status):
                i.prices_sent[side_ind] = None

    async def result_callback(self, result, id=None):
        if id == CALL_ID_INSTRUMENTS:
            await self.process_instruments(result)
        elif id == CALL_ID_SUBSCRIBE:
            logging.debug(f"subscribe result: {result}")
        elif id is not None and id > 99:
            logging.debug(f"Adjust order result: {result}")
        elif id == CALL_ID_LOGIN:
            logging.info(f"login result: {result}")
        elif id == CALL_ID_HEDGE:
            logging.debug(f"Hedge result: {result}")
        else:
            logging.info(result)

    async def error_callback(self, error, id=None):
        if id is not None and id > 99:
            logging.error(f"Problem with order {id}: {error}")
            if error["code"] == 4:
                await self.handle_throttled(id)
        else:
            logging.error(f"{id=}: error: {error}")

    async def process_instruments(self, instruments):
        instruments = [Instrument(i) for i in instruments]
        instruments = [i for i in instruments if i.underlying == UNDERLYING]
        expiries = list(
            set([i.expiry_ts for i in instruments if i.type == InstrumentType.OPTION])
        )
        expiries.sort()
        expiries = expiries[:NUMBER_OF_EXPIRIES_TO_QUOTE]
        for i in instruments:
            if i.name == HEDGE_INSTRUMENT:
                self.hedge_instrument = InstrumentData(i)
            elif i.expiry_ts in expiries:
                if i.type == InstrumentType.OPTION:
                    self.options[i.name] = InstrumentData(i)
        subs = [
            f"ticker.{opt.instrument.name}.{SUBSCRIBE_INTERVAL}"
            for opt in self.options.values()
        ]
        subs.append(f"ticker.{HEDGE_INSTRUMENT}.{SUBSCRIBE_INTERVAL}")
        await self.thalex.public_subscribe(subs, CALL_ID_SUBSCRIBE)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
)

parser = argparse.ArgumentParser(
    description="thalex example trader",
)

parser.add_argument("--network", default="test", metavar="CSTR")
parser.add_argument("--log", default="info", metavar="CSTR")
args = parser.parse_args(sys.argv[1:])
if args.network == "prod":
    network = thalex_py.Network.PROD
elif args.network == "test":
    network = thalex_py.Network.TEST


if args.log == "debug":
    logging.getLogger().setLevel(logging.DEBUG)
else:
    logging.getLogger().setLevel(logging.INFO)


async def main():
    keep_going = True
    while keep_going:
        thalex = thalex_py.Thalex(network=network)
        trader = Trader(thalex, network)
        try:
            await asyncio.gather(
                trader.listen_task(), trader.hedge_task(), trader.greeks_task()
            )
        except (websockets.ConnectionClosed, socket.gaierror) as e:
            logging.error(f"Lost connection ({e}). Reconnecting...")
            time.sleep(0.1)
            continue
        except asyncio.CancelledError:
            keep_going = False
            if thalex.connected():
                await thalex.cancel_all(id=CALL_ID_CANCEL_ALL)
                while True:
                    r = await thalex.receive()
                    r = json.loads(r)
                    if r.get("id", -1) == CALL_ID_CANCEL_ALL:
                        logging.info(f"Cancelled all {r.get('result', 0)} orders")
                        break
                await thalex.disconnect()


def handle_signal(loop, task):
    logging.info("Signal received, stopping...")
    loop.remove_signal_handler(signal.SIGTERM)
    loop.remove_signal_handler(signal.SIGINT)
    task.cancel()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    main_task = loop.create_task(main())

    if os.name != "nt":  # Non-Windows platforms
        loop.add_signal_handler(signal.SIGTERM, handle_signal, loop, main_task)
        loop.add_signal_handler(signal.SIGINT, handle_signal, loop, main_task)
    try:
        loop.run_until_complete(main_task)
    finally:
        loop.close()
