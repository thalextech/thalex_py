import asyncio
import json
import logging

import keys
import thalex as th

NETWORK = th.Network.TEST


# This is a very basic example, only to demonstrate how you can create and trade an rfq on thalex.
async def create_and_buy_rfq(thalex: th.Thalex):
    await thalex.create_rfq(
        legs=[
            th.RfqLeg(0.1, "BTC-28JUN24-70000-C"),
            th.RfqLeg(-0.1, "BTC-28JUN24-70000-P"),
            th.RfqLeg(-0.1, "BTC-27SEP24"),
        ],
        id=2,
    )
    resp = json.loads(await thalex.receive())
    result = resp.get("result")
    if result is None:
        logging.error(f"create RFQ error: {resp}")
        return
    rfq_id = result["rfq_id"]

    await thalex.open_rfqs(id=3)
    resp = json.loads(await thalex.receive())
    result = resp.get("result")
    if result is None:
        logging.error(f"get RFQ error: {resp}")
        return
    rfq = next(r for r in result if r["rfq_id"] == rfq_id)
    logging.info(f"bid: {rfq.get('quoted_bid')}, ask: {rfq.get('quoted_ask')}")

    # We should wait for actual quotes here

    await thalex.trade_rfq(rfq_id, th.Direction.BUY, 5000, id=4)
    resp = json.loads(await thalex.receive())
    result = resp.get("result")
    if result is None:
        logging.error(f"trade RFQ error: {resp}")
        return
    logging.info(result)


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    )
    thalex = th.Thalex(NETWORK)
    await thalex.connect()
    await thalex.login(keys.key_ids[NETWORK], keys.private_keys[NETWORK], id=1)
    logging.info(await thalex.receive())
    await create_and_buy_rfq(thalex)
    await thalex.disconnect()


asyncio.run(main())
