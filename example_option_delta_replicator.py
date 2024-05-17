import asyncio
import csv
import dataclasses
import json
from typing import Optional

import keys
import thalex_py as th

network = th.Network.TEST
KEY_ID = keys.key_ids[network]
PRIV_KEY = keys.private_keys[network]

INSTRUMENT_NAME = "BTC-31MAY24-70000-C"
HEDGING_BAND = 0.01

PORTFOLIO_REQUEST_ID = 1001
TICKER_REQUEST_ID = 1002
ORDER_REQUEST_ID = 1003


@dataclasses.dataclass
class RunResult:
    delta: Optional[float]
    position: Optional[float]
    ordered: Optional[float]
    filled: Optional[float]
    error: Optional[str]


async def run_bot():
    delta: Optional[float] = None
    position: Optional[float] = None
    ordered: Optional[float] = None

    thalex = th.Thalex(network=network)
    await thalex.connect()
    await thalex.login(KEY_ID, PRIV_KEY)
    await thalex.portfolio(PORTFOLIO_REQUEST_ID)
    await thalex.ticker(INSTRUMENT_NAME, TICKER_REQUEST_ID)

    while True:
        msg = json.loads(await thalex.receive())
        if "error" in msg:
            return RunResult(delta, position, ordered, None, msg["error"]["message"])

        msg_id = msg.get("id")
        if msg_id == PORTFOLIO_REQUEST_ID:
            position = 0
            for update in msg["result"]:
                if update["instrument_name"] == "BTC-PERPETUAL":
                    position = update["position"]
            print(f"Perpetual position: {position}")
        elif msg_id == TICKER_REQUEST_ID:
            delta = round(msg["result"]["delta"], ndigits=3)
            print(f"Ticker delta: {delta}")
        elif msg_id == ORDER_REQUEST_ID:
            filled = round(msg["result"]["filled_amount"], ndigits=3)
            if msg["result"]["direction"] == th.Direction.SELL.value:
                filled *= -1
            print(f"Order was executed, filled {filled} of {ordered}")
            return RunResult(delta, position, ordered, filled, None)

        if ordered is None and delta is not None and position is not None:
            position_offset = round(delta - position, 3)
            if abs(position_offset) > HEDGING_BAND:
                print(f"Sending perpetual order for {position_offset}")
                await thalex.insert(
                    id=ORDER_REQUEST_ID,
                    direction=th.Direction.BUY if position_offset > 0 else th.Direction.SELL,
                    instrument_name="BTC-PERPETUAL",
                    order_type=th.OrderType.MARKET,
                    amount=abs(position_offset)
                )
                ordered = position_offset
            else:
                print(f"Option delta of {delta} - {position} = {position_offset} does not exceed hedging band ({HEDGING_BAND}), no need to do anything")
                return RunResult(delta, position, 0, None, None)


def append_result_to_csv(run_result: RunResult, filename: str):
    with open(filename, 'a') as file:
        w = csv.writer(file)
        if file.tell() == 0:
            w.writerow(["delta", "position", "ordered", "filled", "error"])
        w.writerow([
            run_result.delta,
            run_result.position,
            run_result.ordered,
            run_result.filled,
            run_result.error
        ])


def run_bot_and_handle_exceptions():
    try:
        loop = asyncio.get_event_loop()
        task = loop.create_task(run_bot())
        return loop.run_until_complete(task)
    except Exception as e:
        return RunResult(None, None, None, None, str(e))


if __name__ == "__main__":
    run_result = run_bot_and_handle_exceptions()
    append_result_to_csv(run_result, "results.csv")
