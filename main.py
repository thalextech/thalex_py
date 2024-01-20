import argparse
import asyncio
import logging
import sys
from signal import SIGINT, SIGTERM

import thalex_py
import example_trader


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
)

parser = argparse.ArgumentParser(
    description="replicator",
)

parser.add_argument("--network", default="test", metavar="CSTR")
parser.add_argument("--log", default="info", metavar="CSTR")
args = parser.parse_args(sys.argv[1:])
if args.network == "prod":
    network = thalex_py.Network.PROD
if args.network == "test":
    network = thalex_py.Network.TEST


if args.log == "debug":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    )
else:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    )
logging.debug("yo")
logging.info("yo")

async def main():
    try:
        thalex = thalex_py.Thalex(network=thalex_py.Network.TEST)
        await thalex.connect()
        trader = example_trader.Trader(thalex, network)
        await asyncio.gather(trader.listen_task(), trader.start_trading())
    finally:
        await thalex.cancel_all()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    main_task = asyncio.ensure_future(main())
    for signal in [SIGINT, SIGTERM]:
        loop.add_signal_handler(signal, main_task.cancel)
    try:
        loop.run_until_complete(main_task)
    finally:
        loop.close()
