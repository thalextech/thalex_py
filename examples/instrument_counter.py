import asyncio
import json

import thalex as th


async def main():
    ppt = 0
    fut = 0
    opt = 0
    roll = 0
    thalex = th.Thalex(network=th.Network.PROD)
    await thalex.connect()
    await thalex.instruments(id=1)
    while True:
        r = await thalex.receive()
        msg = json.loads(r)
        if msg["id"] == 1:
            for i in msg["result"]:
                it = i["type"]
                if it == "perpetual":
                    ppt += 1
                elif it == "option":
                    opt += 1
                elif it == "future":
                    fut += 1
                elif it == "combination":
                    roll += 1
                else:
                    print("Found unknown instrument")
                    assert False
            await thalex.disconnect()
            break
    print(
        f"""
        Perpetuals: {ppt}
        Futures: {fut}
        Options: {opt}
        Rolls: {roll}
        """
    )


asyncio.run(main())
