import argparse
import asyncio
import json
import logging
import sys
import signal
import os

import thalex_py
import example_trader


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
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    )
    logging.getLogger().setLevel(logging.DEBUG)
else:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    )
    logging.getLogger().setLevel(logging.INFO)


async def main():
    thalex = thalex_py.Thalex(network=network)
    trader = example_trader.Trader(thalex, network)
    try:
        await thalex.connect()
        await asyncio.gather(trader.listen_task(), trader.start_trading())
    except asyncio.CancelledError:
        pass
    finally:
        await thalex.cancel_all(id=example_trader.CALL_ID_CANCEL_ALL)
        while True:
            r = await thalex.receive()
            r = json.loads(r)
            if r.get("id", -1) == example_trader.CALL_ID_CANCEL_ALL:
                logging.info(f"Cancelled all {r['result']} orders")
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
