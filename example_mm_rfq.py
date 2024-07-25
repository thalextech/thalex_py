import asyncio
import json
import logging
from typing import Optional, Dict

import thalex_py as th
import keys

NETWORK = th.Network.TEST

CID_LOGIN = 1
CID_INSTRUMENTS = 2
CID_SUBSCRIBE = 3
CID_TRADE = 4
CALL_ID_SET_COD = 5


def round_to_cent(value):
    return 0.01 * round(value * 100)


class Ticker:
    def __init__(self, data: Dict[str, any]):
        self.best_bid: Optional[float] = data.get("best_bid_price")
        self.best_ask: Optional[float] = data.get("best_ask_price")
        self.mark: float = data["mark_price"]


class Rfq:
    def __init__(self, data: Dict[str, any]):
        self.rfq_id = data["rfq_id"]
        self.amount = data["amount"]
        self.create_time = data["create_time"]
        self.legs = []
        for leg in data["legs"]:
            self.legs.append(th.RfqLeg(leg["quantity"], leg["instrument_name"]))

    def __repr__(self):
        repr = f"id: {self.rfq_id} ("
        for leg in self.legs:
            repr += leg.instrument_name + ": " + str(leg.amount)
        repr += ")"
        return repr


class RfqQuoter:
    def __init__(self, thalex):
        self.thalex = thalex
        self.tickers: Dict[str, Ticker] = {}

    async def load_instruments(self):
        await self.thalex.instruments(id=CID_INSTRUMENTS)
        instruments = json.loads(await self.thalex.receive())
        assert instruments["id"] == CID_INSTRUMENTS
        tickers = []
        for i in instruments["result"]:
            iname = i["instrument_name"]
            tickers.append(f"ticker.{iname}.1000ms")
        await self.thalex.public_subscribe(tickers, id=CID_SUBSCRIBE)

    async def quote_rfq(self, rfq: Rfq):
        mark = 0
        gross_val = 0
        for leg in rfq.legs:
            leg_tick = self.tickers.get(leg.instrument_name)
            if leg_tick is None:
                return
            mark += leg.amount * leg_tick.mark
            gross_val += abs(leg.amount * leg_tick.mark)
        bid = round_to_cent(mark - gross_val * 0.05)
        ask = round_to_cent(mark + gross_val * 0.05)
        logging.info(f"Quoting rfq {rfq} {bid=} {ask=}")
        await self.thalex.mm_rfq_insert_quote(
            direction=th.Direction.BUY,
            amount=rfq.amount,
            price=bid,
            rfq_id=rfq.rfq_id,
            id=CID_TRADE,
        )
        await self.thalex.mm_rfq_insert_quote(
            direction=th.Direction.SELL,
            amount=rfq.amount,
            price=ask,
            rfq_id=rfq.rfq_id,
            id=CID_TRADE,
        )

    async def quote(self):
        await self.load_instruments()
        await self.thalex.login(
            keys.key_ids[NETWORK], keys.private_keys[NETWORK], id=CID_LOGIN
        )
        await self.thalex.set_cancel_on_disconnect(6, id=CALL_ID_SET_COD)
        await self.thalex.private_subscribe(["mm.rfqs"], id=CID_SUBSCRIBE)
        while True:
            msg = json.loads(await self.thalex.receive())
            channel: str = msg.get("channel_name")
            if channel is not None:
                notification = msg["notification"]
                if channel == "mm.rfqs":
                    for rfq in notification:
                        logging.info(f"Got rfq {rfq}")
                        await self.quote_rfq(Rfq(rfq))
                else:  # It must be a ticker
                    iname = channel.split(".")[1]
                    ticker = Ticker(notification)
                    self.tickers[iname] = ticker
            else:
                logging.info(f"Thalex says: {msg}")


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    )
    thalex = th.Thalex(NETWORK)
    await thalex.connect()
    quoter = RfqQuoter(thalex)
    tasks = [quoter.quote()]
    try:
        await asyncio.gather(*tasks)
    except:
        logging.exception("There was an oupsie:")
    await thalex.disconnect()


asyncio.run(main())
