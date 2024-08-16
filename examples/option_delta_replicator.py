import asyncio
import csv
import json
import logging
import socket
import time
from typing import Optional, List

import websockets

import keys
import thalex_py as th

# This is the option that we want to track the deltas of, by buying/selling perpetual with ioc market orders.
# We aim to have a perpetual position that's equal to the deltas of this option. At maturity the options should have
# a delta of either 1 or 0, so we will have either a full perpetual contract (similarly to if an option with physical
# delivery expired in the money), or no perpetual position at all (in case the option expires out of the money).
# Ultimately this strategy should have the same payout as entering a position of one contract long of this option.
OPTION = "BTC-16AUG24-60000-C"
# The ticker subscription channel of the option we're tracking. 1s delay is good enough for the sake of this exercise.
OPTION_TICKER = f"ticker.{OPTION}.1000ms"
# The name of the perpetual contract of the underlying of the option.
PERPETUAL = "BTC-PERPETUAL"
# The ticker subscription channel of the perpetual. 1s delay is good enough for the sake of this exercise.
PERP_TICKER = f"ticker.{PERPETUAL}.1000ms"
# This is how closely we will be tracking the deltas. If our perpetual position is off by more than this amount,
# we make a market order to match the deltas of the option perfectly.
HEDGING_BAND = 0.01
# Minimum seconds between trading requests to avoid throttling.
# Not an ideal way to avoid throttling, but will be fine for the sake of this exercise.
ORDER_DELAY = 0.1
# We will be trading on testnet.
NETWORK = th.Network.TEST
KEY_ID = keys.key_ids[NETWORK]
PRIV_KEY = keys.private_keys[NETWORK]

# We'll use these to match responses from thalex to the corresponding request.
# The numbers are arbitrary, but they need to be unique per CALL_ID.
CID_PORTFOLIO = 1001
CID_INSERT = 1002


class OptionDeltaReplicator:
    def __init__(self, thalex):
        # delta_actual is our perpetual position. We aim to have this within HEDGING_BAND of the deltas of
        # the chosen option. We will be making ioc market orders to achieve that.
        self.delta_actual: Optional[float] = None
        # delta_target is the deltas of the chosen option contract. Notice that at expiry this is either 0 or 1,
        # so if we keep doing this strategy until the option expires,  we should end up with a perp position
        # of either within HEDING_BAND range of 1 or 0.
        self.delta_target: Optional[float] = None
        # Mark price of the option, only for comparing pnl of delta replicating to buying the option.
        self.mark_option: Optional[float] = None
        # Mark price of the perpetual contract for pnl tracking.
        self.mark_perp: Optional[float] = None
        self.thalex: th.Thalex = thalex
        # The timestamp of the last initiation of a trading request to avoid throttling.
        # Not ideal, but it's simple and will work fine for this usecase.
        self.last_order_ts: float = 0

    def flush_data(self, trades, error = None):
        # This is the format of the data we want for parsing later to calculate total pnl (not in this script).
        data = {
            "trades": trades,
            "mark_perpetual": self.mark_perp,
            "mark_option": self.mark_option,
            "error": error,
        }
        with open("replicator.csv", "a", newline="") as f:
            w = csv.writer(f)
            if f.tell() == 0:
                w.writerow(data.keys())
            w.writerow(data.values())

    def notification(self, channel, notification):
        if channel == OPTION_TICKER:
            # We track the deltas of the option with the ticker, this is what we want to replicate.
            # We only need the mark of the option to compare the pnl of this strategy,
            # to that of just simply buying the option.
            self.delta_target = notification["delta"]
            self.mark_option = notification["mark_price"]
        elif channel == PERP_TICKER:
            # We only need the mark price of the perpetual for pnl tracking.
            self.mark_perp = notification["mark_price"]

    def result(self, msg_id, result):
        if msg_id is None:
            return  # If we didn't specify a call id, we don't care about the result.
        if msg_id == CID_INSERT:
            # This is the result of an insert. We log the fills we got for pnl tracking.
            fills = result["fills"]
            direction = result["direction"]
            side_sign = 1 if direction == "buy" else -1
            trades = [{"direction": direction, "price": f['price'], "amount": f['amount']} for f in fills]
            logging.info(f"Traded {trades}")
            for fill in fills:
                self.delta_actual += side_sign * fill["amount"]
            self.flush_data(trades=trades)
        elif msg_id == CID_PORTFOLIO:
            # We make one portfolio call on startup, to find out our initial perpetual position.
            try:
                self.delta_actual = next(
                    pos["position"]
                    for pos in result
                    if pos["instrument_name"] == PERPETUAL
                )
            except StopIteration:
                self.delta_actual = 0

    async def replicate(self):
        while True:
            msg = json.loads(await self.thalex.receive())
            # If there's an error, just log it and carry on.
            error = msg.get("error")
            if error is not None:
                logging.error(msg)
                self.flush_data(error=error["message"], trades=[])
                continue
            channel = msg.get("channel_name")
            # If the message is not an error, then it's either a notification for a subscription
            # or a result of an api call.
            if channel is not None:
                self.notification(channel, msg["notification"])
            else:
                self.result(msg.get("id"), msg["result"])
            await self.maybe_insert()

    async def maybe_insert(self):
        # Very simple way to avoid throttling. Not ideal, but will do for the sake of this exercise.
        if self.last_order_ts + ORDER_DELAY > time.time():
            return
        # We need to know the deltas of the option we're tracking and our perpetual position.
        if self.delta_actual is not None and self.delta_target is not None:
            offset = round(self.delta_target - self.delta_actual, 3)
            # Tracking delta exposure over time would be better, but this is simpler.
            if abs(offset) > HEDGING_BAND:
                logging.info(
                    f"Sending perpetual order for {self.delta_target:.3f} - {self.delta_actual:.3f} = {offset}"
                )
                await self.thalex.insert(
                    id=CID_INSERT,
                    direction=(th.Direction.BUY if offset > 0 else th.Direction.SELL),
                    instrument_name=PERPETUAL,
                    order_type=th.OrderType.MARKET,
                    amount=abs(offset),
                )
                self.last_order_ts = time.time()


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    )
    while True:
        thalex = th.Thalex(NETWORK)
        try:
            await thalex.connect()
            await thalex.login(KEY_ID, PRIV_KEY)
            # Setting cancel on disconnect doesn't matter, because we will be using ioc market orders,
            # but it gives us an elevated message rate.
            await thalex.set_cancel_on_disconnect(5)
            # We need the option ticker to track the deltas of the option that we want to replicate.
            # We need the perpetual subscription only for logging the mark price of the perpetual.
            await thalex.public_subscribe([OPTION_TICKER, PERP_TICKER])
            # We call portfolio once, to start tracking our perpetual position.
            await thalex.portfolio(CID_PORTFOLIO)
            replicator = OptionDeltaReplicator(thalex)
            await replicator.replicate()
        except (websockets.ConnectionClosed, socket.gaierror):
            # It can (and does) happen that we lose connection for whatever reason. If it happens We wait a little,
            # so that if the connection loss is persistent, we don't just keep trying in a hot loop, then reconnect.
            # An exponential backoff would be nicer.
            logging.exception(f"Lost connection. Reconnecting...")
            time.sleep(0.1)
        except asyncio.CancelledError:
            # This means we are stopping the program from the outside (eg Ctrl+C on OSX/Linux)
            logging.info("Signal received. Stopping...")
            if thalex.connected():
                await thalex.disconnect()
            return


asyncio.run(main())
