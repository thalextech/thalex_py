import asyncio
import json
import logging

import keys
import thalex as th

NETWORK = th.Network.TEST
KEY_ID = keys.key_ids[NETWORK]
PRIV_KEY = keys.private_keys[NETWORK]

INSTRUMENT_NAME = "BTC-PERPETUAL"
TARGET_SIZE = 0.1
STOP_BUY = 96990
STOP_SELL = 96950


class BinaryReplicator:
    def __init__(self, subscribe_id=1, buy_order_id=101, sell_order_id=102):
        self.thalex = th.Thalex(NETWORK)
        self.orders = []
        self.position = 0
        self.subscribe_id = subscribe_id
        self.buy_order_id = buy_order_id
        self.sell_order_id = sell_order_id

    async def run(self):
        await self.thalex.connect()
        await self.thalex.login(KEY_ID, PRIV_KEY)
        await self.thalex.private_subscribe(
            ["account.portfolio", "account.conditional_orders"],
            id=self.subscribe_id,
        )
        logging.info("Started binary replicator")
        while True:
            data = await self.thalex.receive()
            msg = json.loads(data)

            if msg.get("channel_name") == "account.conditional_orders":
                self.orders = [
                    o
                    for o in msg.get("notification")
                    if o.get("instrument_name") == INSTRUMENT_NAME
                    and o.get("stop_price") is not None
                    and o.get("status") == "active"
                ]
                logging.info(f"Active orders: {self.orders}")

            if msg.get("channel_name") == "account.portfolio":
                self.position = next(
                    (pos["position"] for pos in msg.get("notification") if pos["instrument_name"] == INSTRUMENT_NAME),
                    0,
                )

                logging.info(f"New position in {INSTRUMENT_NAME}: {round(self.position, 3)}")

                if self.position == 0:
                    await self.thalex.create_conditional_order(
                        direction=th.Direction.BUY,
                        instrument_name=INSTRUMENT_NAME,
                        amount=TARGET_SIZE,
                        stop_price=STOP_BUY,
                        reduce_only=False,
                        id=self.buy_order_id,
                    )

                elif self.position > 0:
                    await self.thalex.create_conditional_order(
                        direction=th.Direction.SELL,
                        instrument_name=INSTRUMENT_NAME,
                        amount=self.position,
                        stop_price=STOP_SELL,
                        reduce_only=True,
                        id=self.sell_order_id,
                    )
                else:
                    logging.info("Something went wrong.")


async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    await BinaryReplicator().run()


if __name__ == "__main__":
    asyncio.run(main())
