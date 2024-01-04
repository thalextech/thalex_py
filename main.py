import asyncio
import thalex_py


private_key = """-----BEGIN RSA PRIVATE KEY-----
...
-----END RSA PRIVATE KEY-----
"""


kid = "K123456798"

async def sub(th):
    token = thalex_py.make_auth_token(kid, private_key)
    await th.login(token)
    await th.private_subscribe(["account.orders"])
    # await th.public_subscribe(["book.BTC-PERPETUAL.none.all.100ms"])
    # await th.instruments()
    # await th.instrument("ETH-PERPETUAL", 7)
    # await th.login(token="asd", account="acc")
    # leg1 = thalex_py.RfqLeg(instrument_name="asd", amount=8)
    # leg2 = thalex_py.RfqLeg(instrument_name="bbq", amount=6)
    # legs = [leg1, leg2]
    # await th.create_rfq(legs, label="eminem")


async def listen(th):
    while True:
        r = await th.receive()
        print(r)


async def main():
    th = thalex_py.Thalex(network=thalex_py.Network.TEST)
    await th.connect()
    listen_task = asyncio.create_task(listen(th))
    trade_task = asyncio.create_task(sub(th))
    await asyncio.wait([listen_task, trade_task])


if __name__ == "__main__":
    asyncio.run(main())
