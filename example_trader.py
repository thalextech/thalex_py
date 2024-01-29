import enum
import argparse
import asyncio
import json
import logging
import math
import sys
import signal
import os
from typing import Dict, Optional, List

import keys

# private_keys = {
#     thalex_py.Network.TEST: """-----BEGIN RSA PRIVATE KEY-----
# ...
# -----END RSA PRIVATE KEY-----
# """,
#     thalex_py.Network.PROD: """-----BEGIN RSA PRIVATE KEY-----
# ...
# -----END RSA PRIVATE KEY-----
# """
# }
#
# key_ids = {
#     thalex_py.Network.TEST: "K123456789",
#     thalex_py.Network.PROD: "K123456789"
# }

import process_msg
import thalex_py


CALL_ID_INSTRUMENTS = 0
CALL_ID_INSTRUMENT = 1
CALL_ID_SUBSCRIBE = 2
CALL_ID_LOGIN = 3
CALL_ID_CANCEL_ALL = 4
CALL_ID_HEDGE = 5

NUMBER_OF_EXPIRIES_TO_QUOTE = 1
SUBSCRIBE_INTERVAL = "1000ms"
DELTA_RANGE_TO_QUOTE = (0.02, 0.8)
BASE_SPREAD = 5.0
SCALE_FACTOR = 0.1
BTC_LOTS = 0.1
ETH_LOTS = 1
AMEND_THRESHOLD = 3  # in ticks
RETREAT = 0.5  # tick size * position / lots
MAX_MARGIN = 5000
DELTA_HEDGE_THRESHOLD = 0.5
HEDGE_INSTRUMENT = "BTC-PERPETUAL"  # It's assumed to be d1


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
        # self.option_type = data.get("option_type")
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


class InstrumentData:
    def __init__(self, instrument: Instrument):
        self.instrument: Instrument = instrument
        self.ticker: Optional[Ticker] = None
        self.quote_prices: List[Optional[float]] = [None, None]  # [bid, ask]
        self.prices_sent: List[Optional[float]] = [None, None]  # [bid, ask]
        self.orders: List[Optional[Order]] = [None, None]  # [bid, ask]
        self.position: float = 0.0


def pricing(i: InstrumentData, reduce_only: bool) -> [float, float]:  # [bid, ask]
    if (
        i.instrument.type != InstrumentType.OPTION
        or abs(i.ticker.delta) < DELTA_RANGE_TO_QUOTE[0]
        or DELTA_RANGE_TO_QUOTE[1] < abs(i.ticker.delta)
    ):
        return [None, None]
    if reduce_only and i.position == 0:
        return [None, None]

    lots = BTC_LOTS if i.instrument.underlying == "BTCUSD" else ETH_LOTS
    retreat = RETREAT * i.instrument.tick_size * i.position / lots
    spread = max(BASE_SPREAD, i.ticker.mark_price * SCALE_FACTOR)
    bid = i.ticker.mark_price - spread - retreat
    ask = i.ticker.mark_price + spread - retreat
    bid = round_to_tick(bid, i.instrument.tick_size)
    if bid < i.instrument.tick_size:
        bid = None
    ask = round_to_tick(ask, i.instrument.tick_size)
    if reduce_only and i.position > 0:
        bid = None
    if reduce_only and i.position < 0:
        ask = None
    return [bid, ask]


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
        self.client_order_id: int = 0
        self.hedge_instrument: Optional[InstrumentData] = None
        self.margin_breach = False  # As in max desired margin not actual margin

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
            await asyncio.sleep(1)

    async def hedge_deltas(self):
        if self.hedge_instrument is None:
            return
        deltas: float = self.hedge_instrument.position
        for i in self.options.values():
            deltas += i.position * i.ticker.delta
        if abs(deltas) > DELTA_HEDGE_THRESHOLD:
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

    async def adjust_quotes(self, i: InstrumentData):
        i.quote_prices = pricing(i, self.margin_breach)
        amount = BTC_LOTS if i.instrument.underlying == "BTCUSD" else ETH_LOTS
        for idx, side in [(0, thalex_py.Direction.BUY), (1, thalex_py.Direction.SELL)]:
            price = i.quote_prices[idx]
            sent = i.prices_sent[idx]
            if price is None:
                if sent is not None and is_open(i.orders[idx].status):
                    i.prices_sent[idx] = None
                    client_order_id = i.orders[idx].id
                    logging.debug(
                        f"Cancel {client_order_id} {i.instrument.name} {side.value}"
                    )
                    await self.thalex.cancel(
                        client_order_id=client_order_id,
                        id=100 + client_order_id,
                    )
            elif sent is None:
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
                    amount=amount,
                    price=price,
                    post_only=True,
                    client_order_id=client_order_id,
                    id=100 + client_order_id,
                )
            elif abs(price - sent) >= AMEND_THRESHOLD * i.instrument.tick_size:
                i.prices_sent[idx] = price
                client_order_id = i.orders[idx].id
                logging.debug(
                    f"Amend {client_order_id} {i.instrument.name} {side.value} {sent} -> {price}"
                )
                await self.thalex.amend(
                    amount=amount,
                    price=price,
                    client_order_id=client_order_id,
                    id=100 + client_order_id,
                )

    async def handle_throttled(self, oid):
        logging.warning(f"Throttled request for order: {oid}")
        await asyncio.sleep(0.1)
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
        logging.info(lwt)

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
                self.options[iname].position = position["position"]

    async def account_summary_callback(self, _, acc_sum):
        required_margin = acc_sum["required_margin"]
        if required_margin > MAX_MARGIN and not self.margin_breach:
            logging.warning(
                f"Required margin {required_margin} going into margin breach mode"
            )
            self.margin_breach = True
        elif required_margin < MAX_MARGIN * 0.4 and self.margin_breach:
            logging.info(f"Required margin {required_margin} quoting everything")
            self.margin_breach = False

    async def trades_callback(self, _, trades):
        for t in trades:
            t = Trade(t)
            logging.info(
                f"Traded {t.instrument}: {t.direction.value} {t.amount}@{t.price}"
            )

    async def orders_callback(self, _, orders):
        for o in orders:
            if o["instrument_name"] != HEDGE_INSTRUMENT:
                i = self.options[o["instrument_name"]]
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
            logging.error(
                f"Problem with order {id - 100}: {error}"
            )
            if error["code"] == 4:
                await self.handle_throttled(id - 100)
        else:
            logging.error(f"{id=}: error: {error}")

    async def process_instruments(self, instruments):
        instruments = [Instrument(i) for i in instruments]
        expiries = list(
            set([i.expiry_ts for i in instruments if i.type == InstrumentType.OPTION])
        )
        expiries.sort()
        expiries = expiries[:NUMBER_OF_EXPIRIES_TO_QUOTE]
        for i in instruments:
            if i.expiry_ts in expiries:
                if i.type == InstrumentType.OPTION:
                    self.options[i.name] = InstrumentData(i)
            elif i.name == HEDGE_INSTRUMENT:
                self.hedge_instrument = InstrumentData(i)
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
    thalex = thalex_py.Thalex(network=network)
    trader = Trader(thalex, network)
    try:
        await asyncio.gather(trader.listen_task(), trader.hedge_task())
    except asyncio.CancelledError:
        pass
    finally:
        await asyncio.sleep(0.2)
        await thalex.cancel_all(id=CALL_ID_CANCEL_ALL)
        while True:
            r = await thalex.receive()
            r = json.loads(r)
            if r.get("id", -1) == CALL_ID_CANCEL_ALL:
                logging.info(f"Cancelled all {r.get('result', 0)} orders")
                break
        await thalex.disconnect()
        logging.info("disconnected")


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
