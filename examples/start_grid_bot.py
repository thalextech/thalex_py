import asyncio
import json
import logging
import time

import keys  # Rename _keys.py to keys.py and add your keys. There are instructions how to create keys in that file.
import thalex


NETWORK = thalex.Network.TEST
CID = 111


async def start_grid_bot():
    tlx = thalex.Thalex(network=NETWORK)
    await tlx.connect()
    await tlx.login(keys.key_ids[NETWORK], keys.private_keys[NETWORK])
    await tlx.create_grid_bot(
        end_time=time.time() + 24 * 3600,
        instrument_name="BTC-PERPETUAL",
        grid=[90_000, 90_500, 91_000, 91_500, 92_000, 92_500],
        step_size=0.001,
        label="gridbot",
        id=CID,
    )
    while True:
        msg = json.loads(await tlx.receive())
        err = msg.get("error")
        if err is not None:
            logging.error(f"There was an error: {err['message']}")
            exit(1)
        if msg.get("id", -1) == CID:
            logging.info(f"Create bot result: {msg['result']}")
            exit(0)


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    )
    await start_grid_bot()


asyncio.run(main())
