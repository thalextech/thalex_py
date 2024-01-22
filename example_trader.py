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
CALL_ID_LOGIN = 4
CALL_ID_CANCEL_ALL = 5

NUMBER_OF_EXPIRIES = 1
SUBSCRIBE_INTERVAL = "1000ms"
DELTA_RANGE_TO_QUOTE = (0.4, 0.6)
BASE_SPREAD = 50
SCALE_FACTOR = 0.5
LOTS = 0.1


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
        self.direction = thalex_py.Direction(data["direction"])
        self.status = OrderStatus(data["status"])


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
        self.eligible_instruments = set()

    async def start_trading(self):
        token = thalex_py.make_auth_token(
            keys.key_ids[self.network], keys.private_keys[self.network]
        )
        await self.thalex.login(token, id=CALL_ID_LOGIN)
        self.processor.add_callback(process_msg.Channel.LWT, self.lwt_callback)
        self.processor.add_callback(process_msg.Channel.TICKER, self.ticker_callback)
        self.processor.add_callback(process_msg.Channel.BOOK, self.book_callback)
        self.processor.add_callback(process_msg.Channel.ORDERS, self.orders_callback)
        self.processor.add_result_callback(self.result_callback)
        await self.thalex.instruments(CALL_ID_INSTRUMENTS)
        await self.thalex.private_subscribe(["account.orders"])
        while True:
            await self.manage_portfolio()
            await asyncio.sleep(1)

    async def manage_portfolio(self):
        for instrument_name, ticker in self.tickers.items():
            instrument = self.instruments.get(instrument_name)
            if (
                instrument.type == InstrumentType.OPTION
                and DELTA_RANGE_TO_QUOTE[0]
                <= abs(ticker.delta)
                <= DELTA_RANGE_TO_QUOTE[1]
            ):
                dynamic_spread = max(BASE_SPREAD, ticker.mark_price * SCALE_FACTOR)

                bid = ticker.mark_price - dynamic_spread
                ask = ticker.mark_price + dynamic_spread

                if "BTC" in instrument_name:
                    bid = self.round_to_nearest(bid, 5)
                    ask = self.round_to_nearest(ask, 5)
                if bid > 0:
                    await self.insert_or_amend(
                        instrument_name, bid, thalex_py.Direction.BUY
                    )
                await self.insert_or_amend(
                    instrument_name, ask, thalex_py.Direction.SELL
                )

    async def cancel_orders(self, instrument_name):
        orders = self.orders.get(instrument_name)
        if orders:
            for order in orders:
                if order is not None:
                    await self.thalex.cancel(order.id)
                    logging.debug(f"Canceled order for {instrument_name}")

    def round_to_nearest(self, value, base):
        return base * round(value / base)

    async def insert_or_amend(self, instrument, price, side):
        try:
            side_idx = 0 if side == thalex_py.Direction.BUY else 1
            current_order = self.orders.get(instrument, [None, None])[side_idx]
            if current_order and current_order.status == OrderStatus.OPEN:
                await self.thalex.amend(
                    amount=LOTS,
                    price=price,
                    order_id=current_order.id,
                    id=CALL_ID_INSERT_AMEND,
                )
            else:
                await self.thalex.insert(
                    direction=side,
                    instrument_name=instrument,
                    amount=LOTS,
                    price=price,
                    post_only=True,
                    id=CALL_ID_INSERT_AMEND,
                )
        except Exception:
            logging.exception(
                f"Error placing/amending post-only order for {instrument}"
            )

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
        for o in orders:
            order = Order(o)
            instrument = order.instrument
            side_idx = 0 if order.direction == thalex_py.Direction.BUY else 1
            if instrument not in self.orders:
                self.orders[instrument] = [None, None]
            self.orders[instrument][side_idx] = order

    async def result_callback(self, result, id=None):
        if id == CALL_ID_INSTRUMENTS:
            await self.process_instruments(result)
        elif id == CALL_ID_SUBSCRIBE:
            logging.info(f"subscribe result: {result}")
        elif id == CALL_ID_INSERT_AMEND:
            logging.debug(f"insert or amend result: {result}")
        elif id == CALL_ID_LOGIN:
            logging.info(f"login result: {result}")
        else:
            logging.info(result)

    async def process_instruments(self, instruments):
        instruments = [
            Instrument(i) for i in instruments if i["underlying"] == "BTCUSD"
        ]
        expiries = [i.expiry_ts for i in instruments if i.type == InstrumentType.OPTION]
        expiries.sort()
        expiries = expiries[:NUMBER_OF_EXPIRIES]
        for i in instruments:
            if i.expiry_ts in expiries:
                self.instruments[i.name] = i
        subs = [
            f"ticker.{opt.name}.{SUBSCRIBE_INTERVAL}"
            for opt in self.instruments.values()
        ]
        await self.thalex.public_subscribe(subs, CALL_ID_SUBSCRIBE)
