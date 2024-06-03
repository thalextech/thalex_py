import asyncio
import enum
import json
import logging
from typing import Optional, Dict

import thalex_py as th
import keys

# In this example we're looking for arbitrage opportunities, by trying to find a synthetic future ask/bid,
# that's in cross with the real future bid/ask.
# We're using 3 individual limit ioc orders to enter a statically hedged position if an opportunity presents,
# so there's a risk that some of them may not go through, and we'll end up with a position that we'll need to hedge.
# We could opt for market orders instead and thus run a pnl risk.
# Looking for hedging a synthetic with an other synthetic of the same expiry is not in scope of the example.


NETWORK = th.Network.TEST
EXPIRY = "2024-06-04"
UNDERLYING = "BTCUSD"
LABEL = "SA"
MIN_PNL = 100

CID_LOGIN = 1
CID_INSTRUMENTS = 2
CID_SUBSCRIBE = 3
CID_TRADE = 4


class InstrumentType(enum.Enum):
    FUTURE = "future"
    CALL = "call"
    PUT = "put"


class TOB:
    def __init__(self, iname):
        self.iname = iname
        self.strike_price: Optional[int] = None
        self.type: Optional[InstrumentType] = None
        self.bid: Optional[th.SideQuote] = None
        self.ask: Optional[th.SideQuote] = None

    def __repr__(self):
        bid = None if self.bid is None else self.bid.p
        ask = None if self.ask is None else self.ask.p
        return f"{self.iname} {bid=} {ask=}"


class StrikeTOB:
    def __init__(self):
        self.call: Optional[TOB] = None
        self.put: Optional[TOB] = None

    def __repr__(self):
        return f"{self.call.strike_price} call: {self.call} put: {self.put}"


class SynthArbitrage:
    def __init__(self, thalex):
        self.thalex = thalex
        self.ch_to_tob: Dict[str, TOB] = {}
        self.book_queue = asyncio.Queue()
        self.future_tob: Optional[TOB] = None
        self.strike_tobs: Dict[int, StrikeTOB] = {}

    # Here we create a TOB instance for the future and all options of the chosen expiry,
    # subscribe to their lwt channels and create some mappings so that we can find the tob easily later.
    async def load_instruments(self):
        await self.thalex.instruments(id=CID_INSTRUMENTS)
        instruments = json.loads(await self.thalex.receive())
        assert instruments["id"] == CID_INSTRUMENTS
        tickers = []
        for i in instruments["result"]:
            t = i["type"]
            if (
                t in ["future", "option"]
                and i["expiry_date"] == EXPIRY
                and i["underlying"] == UNDERLYING
            ):
                iname = i["instrument_name"]
                channel = f"lwt.{iname}.raw"
                tickers.append(channel)
                tob = TOB(iname)
                self.ch_to_tob[channel] = tob
                if t == "future":
                    self.future_tob = tob
                    tob.type = InstrumentType.FUTURE
                elif t == "option":
                    strike = i["strike_price"]
                    tob.strike_price = strike
                    if strike not in self.strike_tobs:
                        self.strike_tobs[strike] = StrikeTOB()
                    if i["option_type"] == "call":
                        tob.type = InstrumentType.CALL
                        self.strike_tobs[strike].call = tob
                    else:
                        tob.type = InstrumentType.PUT
                        self.strike_tobs[strike].put = tob
        assert self.future_tob is not None
        for s in self.strike_tobs.values():
            assert s is not None
        await self.thalex.public_subscribe(tickers, CID_SUBSCRIBE)

    async def listen(self):
        await self.load_instruments()
        await self.thalex.login(
            keys.key_ids[NETWORK], keys.private_keys[NETWORK], id=CID_LOGIN
        )
        while True:
            msg = json.loads(await self.thalex.receive())
            channel: str = msg.get("channel_name")
            if channel is not None:
                # We're parsing an lwt notification here.
                # See: https://www.thalex.com/docs/#tag/subs_market_data/Ticker-(lightweight-payload)
                notification = msg["notification"]
                bid = notification.get("b")
                if bid is not None:
                    self.ch_to_tob[channel].bid = th.SideQuote(bid[0], bid[1])
                ask = notification.get("a")
                if ask is not None:
                    self.ch_to_tob[channel].ask = th.SideQuote(ask[0], ask[1])
                await self.book_queue.put(channel)
            else:
                logging.info(msg)

    async def execute(self):
        while True:
            next_ch = await self.book_queue.get()
            next_tob = self.ch_to_tob[next_ch]
            if next_tob.type == InstrumentType.FUTURE:
                # do all
                pass
            else:
                strike = self.strike_tobs[next_tob.strike_price]
                await self.trade_arbitrage(strike)

    async def trade_arbitrage(self, strike: StrikeTOB):
        # We buy a synth and hedge it with a future
        if (
            strike.call.ask is None
            or strike.put.bid is None
            or self.future_tob.bid is None
        ):
            amount_buy = 0
            pnl_buy = 0
        else:
            amount_buy = min(strike.call.ask.a, strike.put.bid.a, self.future_tob.bid.a)
            pnl_buy = (
                -strike.call.ask.p
                + strike.put.bid.p
                - strike.put.strike_price
                + self.future_tob.bid.p
            )
        # We sell a synth and hedge it with a future
        if (
            strike.call.bid is None
            or strike.put.ask is None
            or self.future_tob.ask is None
        ):
            amount_sell = 0
            pnl_sell = 0
        else:
            amount_sell = min(strike.call.bid.a, strike.put.ask.a, self.future_tob.ask.a)
            pnl_sell = (
                strike.call.bid.p
                - strike.put.ask.p
                + strike.put.strike_price
                - self.future_tob.ask.p
            )
        if pnl_buy * amount_buy > MIN_PNL:
            logging.info(
                f"future bid: {self.future_tob.bid.p} - strike: {strike.put.strike_price} + "
                f"put bid: {strike.put.bid.p} - call ask: {strike.call.ask.p} -> {pnl_buy=} "
                f"Buying {strike.call.iname}, Selling {strike.put.iname}, {self.future_tob.iname} "
                f"{amount_buy=}"
            )
            trade_tasks = [
                self.thalex.buy(
                    instrument_name=strike.call.iname,
                    amount=amount_buy,
                    price=strike.call.ask.p,
                    label=LABEL,
                    order_type=th.OrderType.LIMIT,
                    time_in_force=th.TimeInForce.IOC,
                    post_only=False,
                    id=CID_TRADE,
                ),
                self.thalex.sell(
                    instrument_name=strike.put.iname,
                    amount=amount_buy,
                    price=strike.put.bid.p,
                    label=LABEL,
                    order_type=th.OrderType.LIMIT,
                    time_in_force=th.TimeInForce.IOC,
                    post_only=False,
                    id=CID_TRADE,
                ),
                self.thalex.sell(
                    instrument_name=self.future_tob.iname,
                    amount=amount_buy,
                    price=self.future_tob.bid.p,
                    label=LABEL,
                    order_type=th.OrderType.LIMIT,
                    time_in_force=th.TimeInForce.IOC,
                    post_only=False,
                    id=CID_TRADE,
                ),
            ]
        elif pnl_sell * amount_sell > MIN_PNL:
            logging.info(
                f"strike: {strike.put.strike_price} - future ask: {self.future_tob.ask.p} + "
                f"call bid: {strike.call.bid.p} - put bid: {strike.put.bid.p} -> {pnl_sell=} "
                f"Selling {strike.call.iname}, Buying {strike.put.iname}, {self.future_tob.iname} "
                f"{amount_buy=}"
            )
            trade_tasks = [
                self.thalex.sell(
                    instrument_name=strike.call.iname,
                    amount=amount_sell,
                    price=strike.call.bid.p,
                    label=LABEL,
                    order_type=th.OrderType.LIMIT,
                    time_in_force=th.TimeInForce.IOC,
                    post_only=False,
                    id=CID_TRADE,
                ),
                self.thalex.buy(
                    instrument_name=strike.put.iname,
                    amount=amount_sell,
                    price=strike.put.ask.p,
                    label=LABEL,
                    order_type=th.OrderType.LIMIT,
                    time_in_force=th.TimeInForce.IOC,
                    post_only=False,
                    id=CID_TRADE,
                ),
                self.thalex.buy(
                    instrument_name=self.future_tob.iname,
                    amount=amount_sell,
                    price=self.future_tob.ask.p,
                    label=LABEL,
                    order_type=th.OrderType.LIMIT,
                    time_in_force=th.TimeInForce.IOC,
                    post_only=False,
                    id=CID_TRADE,
                ),
            ]
        else:
            trade_tasks = None
        if trade_tasks is not None:
            await asyncio.gather(*trade_tasks)


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    )
    thalex = th.Thalex(NETWORK)
    await thalex.connect()
    sa = SynthArbitrage(thalex)
    tasks = [sa.listen(), sa.execute()]
    try:
        await asyncio.gather(*tasks)
    except:
        logging.exception("There was an oupsie:")
    await thalex.disconnect()


asyncio.run(main())
