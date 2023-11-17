import asyncio
import thalex_py


def ignore(p):
    pass


async def sub(th):
    # await th.subscribe(["ticker.BTC-01DEC23-36000-C.100ms"])
    # await th.instruments()
    # await th.instrument("ETH-PERPETUAL", 7)
    # await th.login(token="asd", account="acc")
    leg1 = thalex_py.RfqLeg(instrument_name="asd", amount=8)
    leg2 = thalex_py.RfqLeg(instrument_name="bbq", amount=6)
    legs = [leg1, leg2]
    await th.create_rfq(legs, label="eminem")


async def listen(th):
    while True:
        r = await th.receive()
        print(r)


async def main2():
    th = thalex_py.Thalex(network=thalex_py.Network.PROD)
    await th.connect()
    # await th.subscribe(["ticker.ETH-01DEC23-2100-C.100ms"])
    listen_task = asyncio.create_task(listen(th))
    sub_task = asyncio.create_task(sub(th))
    await asyncio.wait([listen_task, sub_task])


if __name__ == "__main__":
    asyncio.run(main2())
