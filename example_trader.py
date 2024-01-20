import asyncio
import enum
import logging
from typing import Dict, Optional, List

import keys
import process_msg
import thalex_py


CALL_ID_INSTRUMENTS = 0
CALL_ID_INSTRUMENT = 1
CALL_ID_SUBSCRIBE = 2
CALL_ID_INSERT_AMEND = 3

WIDTH_FACTOR = 20
LOTS = 0.1


private_key = """-----BEGIN RSA PRIVATE KEY-----
...
-----END RSA PRIVATE KEY-----
"""


key_id = "K123456798"


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

class Instrument:
    def __init__(self, data: Dict):
        self.name = data["instrument_name"]
        self.product = data["product"]
        self.tick_size = data["tick_size"]
        self.type = InstrumentType(data["type"])
        self.option_type = data.get("option_type")
        self.expiry_ts = data.get("expiration_timestamp")
        self.strike_price = data.get("strike_price")

class Order:
    def __init__(self, data: Dict):
        self.id: str = data["order_id"]
        self.instrument = data["instrument_name"]
        self.direction = thalex_py.Direction[data["direction"]]
        self.status = OrderStatus[data["status"]]

class Ticker:
    def __init__(self, data: Dict):
        self.mark_price = data["mark_price"]
        self.delta = data["delta"]
        self.bid = data.get("best_bid_price")
        self.ask = data.get("best_ask_price")


class Trader:
    def __init__(self, thalex: thalex_py.Thalex, network: thalex_py.Network):
        self.thalex = thalex
        self.network = network
        self.instruments: Dict[str, Instrument] = {}
        self.tickers: Dict[str, Ticker] = {}
        self.processor = process_msg.Processor()
        self.active_options: [Instrument] = []
        self.hedge_instrument: Optional[Instrument] = None
        self.orders: Dict[str, List[Optional[Order]]] = {}  # [bid, ask]

    async def start_trading(self):
        token = thalex_py.make_auth_token(key_id, private_key)
        await self.thalex.login(token)
        self.processor.add_callback(process_msg.Channel.LWT, self.lwt_callback)
        self.processor.add_callback(process_msg.Channel.TICKER, self.ticker_callback)
        self.processor.add_callback(process_msg.Channel.BOOK, self.book_callback)
        self.processor.add_callback(process_msg.Channel.ORDERS, self.orders_callback)
        self.processor.add_result_callback(self.result_callback)
        await self.thalex.instruments(CALL_ID_INSTRUMENTS)
        while True:
            await self.manage_portfolio()
            await asyncio.sleep(1)

    async def manage_portfolio(self):
        for i, t in self.tickers.items():
            bid = t.mark_price - WIDTH_FACTOR * self.instruments[i].tick_size
            ask = t.mark_price + WIDTH_FACTOR * self.instruments[i].tick_size
            if bid > 0:
                await self.insert_or_amend(i, bid, thalex_py.Direction.BUY)
            else:
                orders = self.orders.get(i)
                if orders is not None:
                    if orders[0] is not None:
                        await self.thalex.cancel(orders[0].id)
            await self.insert_or_amend(i, ask, thalex_py.Direction.SELL)

    async def insert_or_amend(self, instrument, price, side):
        logging.debug(f"Quoting {instrument}: {side.value}: {price}")
        side_idx = 0 if side == thalex_py.Direction.BUY else 1
        orders = self.orders.get(instrument)
        if orders is not None:
            if orders[side_idx] is not None:
                await self.thalex.amend(amount=LOTS, price=price, order_id=orders[side_idx].id)
                return
        await self.thalex.insert(direction=side, instrument_name=instrument, amount=LOTS, price=price, id=CALL_ID_INSERT_AMEND)

    async def listen_task(self):
        while True:
            msg = await self.thalex.receive()
            await self.processor.process_msg(msg)

    async def lwt_callback(self, channel, lwt):
        logging.info(lwt)

    async def ticker_callback(self, channel, ticker):
        i = channel.split(".")[1]
        self.tickers[i] = Ticker(ticker)

    async def book_callback(self, channel, book):
        logging.info(f"{channel}: {book}")

    async def orders_callback(self, _, orders):
        logging.info(f"{orders=}")
        for o in orders:
            o = Order(o)
            if self.orders[o.instrument] is None:
                self.orders[o.instrument] = [None, None]
            side_idx = 0 if o.direction == thalex_py.Direction.BUY else 1
            self.orders[o.instrument][side_idx] = o

    async def result_callback(self, result, id=None):
        if id == CALL_ID_INSTRUMENTS:
            await self.process_instruments(result)
        elif id == CALL_ID_SUBSCRIBE:
            logging.info(f"subscribe result: {result}")
        elif id == CALL_ID_INSERT_AMEND:
            logging.info(f"insert or amend result: {result}")
        else:
            logging.info(result)

    async def process_instruments(self, instruments):
        instruments = [i for i in instruments if i["underlying"] == "BTCUSD"]
        for i in instruments:
            i = Instrument(i)
            if i.type != InstrumentType.COMBO:
                self.instruments[i.name] = i
        min_ts = 9999999999
        for i in [
            opt
            for opt in self.instruments.values()
            if opt.type == InstrumentType.OPTION
        ]:
            if min_ts > i.expiry_ts:
                min_ts = i.expiry_ts
        for i in [
            opt
            for opt in self.instruments.values()
            if opt.type == InstrumentType.OPTION
        ]:
            if i.expiry_ts == min_ts:
                self.active_options.append(i)
        for i in [
            fut
            for fut in self.instruments.values()
            if fut.type == InstrumentType.FUTURE
        ]:
            if i.expiry_ts == min_ts:
                self.hedge_instrument = i

        subs = [f"ticker.{opt.name}.1000ms" for opt in self.active_options]
        await self.thalex.public_subscribe(
            [*subs, f"ticker.{self.hedge_instrument.name}.1000ms"], CALL_ID_SUBSCRIBE
        )
