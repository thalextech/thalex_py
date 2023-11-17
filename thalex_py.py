import enum
import json
from typing import Optional, List

import websockets


class Network(enum.Enum):
    TEST = "wss://testnet.thalex.com/ws/api/v2"
    PROD = "wss://thalex.com/ws/api/v2"


class Direction(enum.Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(enum.Enum):
    LIMIT = "limit"
    MARKET = "market"


class TimeInForce(enum.Enum):
    GTC = "good_till_cancelled"
    IOC = "immediate_or_cancel"


class Collar(enum.Enum):
    IGNORE = "ignore"
    REJECT = "reject"
    CLAMP = "clamp"


class Target(enum.Enum):
    LAST = "last"
    MARK = "mark"
    INDEX = "index"


class RfqLeg:
    def __init__(self, amount: float, instrument_name: str):
        self.instrument_name = instrument_name
        self.amount = amount

    def dumps(self):
        return {"amount": self.amount, "instrument_name": self.instrument_name}


class SideQuote:
    def __init__(
        self,
        price: float,
        amount: float,
    ):
        self.p = price
        self.a = amount

    def dumps(self):
        return {"a": self.a, "p": self.p}


class Quote:
    def __init__(
        self, instrument_name: str, bid: Optional[SideQuote], ask: Optional[SideQuote]
    ):
        self.i = instrument_name
        self.b = bid
        self.a = ask

    def dumps(self):
        d = {"i": self.i}
        if self.d is not None:
            d["b"] = self.b.dumps()
        if self.a is not None:
            d["a"] = self.a.dumps()


class Asset:
    def __init__(
        self,
        asset_name: float,
        amount: float,
    ):
        self.asset_name = asset_name
        self.amount = amount

    def dumps(self):
        return {"asset_name": self.asset_name, "amount": self.amount}


class Position:
    def __init__(
        self,
        instrument_name: float,
        amount: float,
    ):
        self.instrument_name = instrument_name
        self.amount = amount

    def dumps(self):
        return {"instrument_name": self.instrument_name, "amount": self.amount}


class Thalex:
    def __init__(self, network: Network):
        self.net: Network = network
        self.ws: websockets.client = None

    async def receive(self):
        return await self.ws.recv()

    async def connect(self):
        self.ws = await websockets.connect(self.net.value, ping_interval=200)

    async def _send(self, method: str, id: Optional[int], **kwargs):
        request = {"method": method, "params": {}}
        if id is not None:
            request["id"] = id
        for key, value in kwargs.items():
            if value is not None:
                request["params"][key] = value
        request = json.dumps(request)
        await self.ws.send(request)

    async def login(
        self, token: str, account: Optional[str] = None, id: Optional[int] = None
    ):
        await self._send(
            "public/login",
            id,
            token=token,
            account=account,
        )

    async def set_cancel_on_disconnect(
        self, timeout_secs: Optional[int] = None, id: Optional[int] = None
    ):
        await self._send(
            "private/set_cancel_on_disconnect",
            id,
            timeout_secs=timeout_secs,
        )

    async def instruments(self, id: Optional[int] = None):
        await self._send("public/instruments", id)

    async def all_instruments(self, id: Optional[int] = None):
        await self._send("public/all_instruments", id)

    async def instrument(self, instrument_name: str, id: Optional[int] = None):
        await self._send("public/instrument", id, instrument_name=instrument_name)

    async def ticker(self, instrument_name: str, id: Optional[int] = None):
        await self._send("public/ticker", id, instrument_name=instrument_name)

    async def index(self, underlying: str, id: Optional[int] = None):
        await self._send("public/index", id, underlying=underlying)

    async def book(self, instrument_name: str, id: Optional[int] = None):
        await self._send("public/book", id, instrument_name=instrument_name)

    async def insert(
        self,
        direction: Direction,
        instrument_name: str,
        amount: float,
        client_order_id: Optional[int] = None,
        price: Optional[float] = None,
        label: Optional[str] = None,
        order_type: Optional[OrderType] = None,
        time_in_force: Optional[TimeInForce] = None,
        post_only: Optional[bool] = None,
        reject_post_only: Optional[bool] = None,
        reduce_only: Optional[bool] = None,
        collar: Optional[Collar] = None,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/insert",
            id,
            direction=direction.value,
            instrument_name=instrument_name,
            amount=amount,
            client_order_id=client_order_id,
            price=price,
            label=label,
            order_type=order_type.value,
            time_in_force=time_in_force.value,
            post_only=post_only,
            reject_post_only=reject_post_only,
            reduce_only=reduce_only,
            collar=collar,
        )

    async def buy(
        self,
        instrument_name: str,
        amount: float,
        client_order_id: Optional[int] = None,
        price: Optional[float] = None,
        label: Optional[str] = None,
        order_type: Optional[OrderType] = None,
        time_in_force: Optional[TimeInForce] = None,
        post_only: Optional[bool] = None,
        reject_post_only: Optional[bool] = None,
        reduce_only: Optional[bool] = None,
        collar: Optional[Collar] = None,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/buy",
            id,
            instrument_name=instrument_name,
            amount=amount,
            client_order_id=client_order_id,
            price=price,
            label=label,
            order_type=order_type.value,
            time_in_force=time_in_force.value,
            post_only=post_only,
            reject_post_only=reject_post_only,
            reduce_only=reduce_only,
            collar=collar,
        )

    async def sell(
        self,
        instrument_name: str,
        amount: float,
        client_order_id: Optional[int] = None,
        price: Optional[float] = None,
        label: Optional[str] = None,
        order_type: Optional[OrderType] = None,
        time_in_force: Optional[TimeInForce] = None,
        post_only: Optional[bool] = None,
        reject_post_only: Optional[bool] = None,
        reduce_only: Optional[bool] = None,
        collar: Optional[Collar] = None,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/sell",
            id,
            instrument_name=instrument_name,
            amount=amount,
            client_order_id=client_order_id,
            price=price,
            label=label,
            order_type=order_type.value,
            time_in_force=time_in_force.value,
            post_only=post_only,
            reject_post_only=reject_post_only,
            reduce_only=reduce_only,
            collar=collar,
        )

    async def amend(
        self,
        amount: float,
        price: float,
        order_id: Optional[int] = None,
        client_order_id: Optional[int] = None,
        collar: Optional[Collar] = None,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/amend",
            id,
            amount=amount,
            price=price,
            order_id=order_id,
            client_order_id=client_order_id,
            collar=collar,
        )

    async def cancel(
        self,
        order_id: Optional[int] = None,
        client_order_id: Optional[int] = None,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/cancel",
            id,
            order_id=order_id,
            client_order_id=client_order_id,
        )

    async def cancel_all(
        self,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/cancel_all",
            id,
        )

    async def cancel_session(
        self,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/cancel_session",
            id,
        )

    async def create_rfq(
        self,
        legs: List[RfqLeg],
        label: Optional[str] = None,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/create_rfq", id, legs=[leg.dumps() for leg in legs], label=label
        )

    async def cancel_rfq(
        self,
        rfq_id,
        id: Optional[int] = None,
    ):
        await self._send("private/cancel_rfq", id, rfq_id=rfq_id)

    async def trade_rfq(
        self,
        rfq_id: str,
        direction: Direction,
        limit_price: float,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/trade_rfq",
            id,
            rfq_id=rfq_id,
            direction=direction.value,
            limit_price=limit_price,
        )

    async def mm_rfqs(
        self,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/mm_rfqs",
            id,
        )

    async def mm_rfq_insert_quote(
        self,
        direction: Direction,
        amount: float,
        price: float,
        rfq_id: str,
        client_order_id: Optional[int] = None,
        label: Optional[str] = None,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/mm_rfq_insert_quote",
            id,
            rfq_id=rfq_id,
            direction=direction.value,
            amount=amount,
            price=price,
            client_order_id=client_order_id,
            label=label,
        )

    async def mm_rfq_amend_quote(
        self,
        amount: float,
        price: float,
        order_id: Optional[int] = None,
        client_order_id: Optional[int] = None,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/mm_rfq_amend_quote",
            id,
            amount=amount,
            price=price,
            order_id=order_id,
            client_order_id=client_order_id,
        )

    async def mm_rfq_delete_quote(
        self,
        order_id: Optional[int] = None,
        client_order_id: Optional[int] = None,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/mm_rfq_delete_quote",
            id,
            order_id=order_id,
            client_order_id=client_order_id,
        )

    async def mm_rfq_quotes(
        self,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/mm_rfq_quotes",
            id,
        )

    async def open_orders(
        self,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/open_orders",
            id,
        )

    async def order_history(
        self,
        limit: Optional[int] = None,
        time_low: Optional[int] = None,
        time_high: Optional[int] = None,
        bookmark: Optional[str] = None,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/order_history",
            id,
            limit=limit,
            time_low=time_low,
            time_high=time_high,
            bookmark=bookmark,
        )

    async def trade_history(
        self,
        limit: Optional[int] = None,
        time_low: Optional[int] = None,
        time_high: Optional[int] = None,
        bookmark: Optional[str] = None,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/trade_history",
            id,
            limit=limit,
            time_low=time_low,
            time_high=time_high,
            bookmark=bookmark,
        )

    async def transaction_history(
        self,
        limit: Optional[int] = None,
        time_low: Optional[int] = None,
        time_high: Optional[int] = None,
        bookmark: Optional[str] = None,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/transaction_history",
            id,
            limit=limit,
            time_low=time_low,
            time_high=time_high,
            bookmark=bookmark,
        )

    async def rfq_history(
        self,
        limit: Optional[int] = None,
        time_low: Optional[int] = None,
        time_high: Optional[int] = None,
        bookmark: Optional[str] = None,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/rfq_history",
            id,
            limit=limit,
            time_low=time_low,
            time_high=time_high,
            bookmark=bookmark,
        )

    async def account_breakdown(
        self,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/account_breakdown",
            id,
        )

    async def account_summary(
        self,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/account_summary",
            id,
        )

    async def required_margin_breakdown(
        self,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/required_margin_breakdown",
            id,
        )

    async def required_margin_for_order(
        self,
        instrument_name: str,
        price: float,
        amount: float,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/required_margin_for_order",
            id,
            instrument_name=instrument_name,
            amount=amount,
            price=price,
        )

    async def private_subscribe(self, channels: [str], id: Optional[int] = None):
        await self._send("private/subscribe", id, channels=channels)

    async def public_subscribe(self, channels: [str], id: Optional[int] = None):
        await self._send("public/subscribe", id, channels=channels)

    async def unsubscribe(self, channels: [str], id: Optional[int] = None):
        await self._send("unsubscribe", id, channels=channels)

    async def conditional_orders(
        self,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/conditional_orders",
            id,
        )

    async def create_conditional_order(
        self,
        direction: Direction,
        instrument_name: str,
        amount: float,
        stop_price: float,
        limit_price: Optional[float] = None,
        bracket_price: Optional[float] = None,
        trailing_stop_callback_rate: Optional[float] = None,
        label: Optional[str] = None,
        reduce_only: Optional[bool] = None,
        target: Optional[Target] = None,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/create_conditional_order",
            id,
            direction=direction.value,
            instrument_name=instrument_name,
            amount=amount,
            label=label,
            reduce_only=reduce_only,
            stop_price=stop_price,
            limit_price=limit_price,
            bracket_price=bracket_price,
            trailing_stop_callback_rate=trailing_stop_callback_rate,
            target=target,
        )

    async def cancel_conditional_order(
        self,
        order_id: Optional[int] = None,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/cancel_conditional_order",
            id,
            order_id=order_id,
        )

    async def cancel_all_conditional_orders(
        self,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/cancel_all_conditional_orders",
            id,
        )

    async def notifications_inbox(
        self,
        limit: Optional[int] = None,
        id: Optional[int] = None,
    ):
        await self._send("private/notifications_inbox", id, limit=limit)

    async def mark_inbox_notification_as_read(
        self,
        notification_id: str,
        read: Optional[bool] = None,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/mark_inbox_notification_as_read",
            id,
            notification_id=notification_id,
            read=read,
        )

    async def mass_quote(
        self,
        quotes: List[Quote],
        label: Optional[str] = None,
        post_only: Optional[bool] = None,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/mass_quote", id, quotes=quotes, label=label, post_only=post_only
        )

    async def set_mm_protection(
        self,
        product: str,
        amount: float,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/set_mm_protection", id, product=product, amount=amount
        )

    async def verify_withdrawal(
        self,
        asset_name: str,
        amount: float,
        target_address: str,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/verify_withdrawal",
            id,
            asset_name=asset_name,
            amount=amount,
            target_address=target_address,
        )

    async def withdraw(
        self,
        asset_name: str,
        amount: float,
        target_address: str,
        label: Optional[str] = None,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/withdraw",
            id,
            asset_name=asset_name,
            amount=amount,
            target_address=target_address,
            label=label,
        )

    async def crypto_withdrawals(
        self,
        id: Optional[int] = None,
    ):
        await self._send(
            "public/crypto_withdrawals",
            id,
        )

    async def crypto_deposits(
        self,
        id: Optional[int] = None,
    ):
        await self._send(
            "public/crypto_deposits",
            id,
        )

    async def btc_deposit_address(
        self,
        id: Optional[int] = None,
    ):
        await self._send(
            "public/btc_deposit_address",
            id,
        )

    async def eth_deposit_address(
        self,
        id: Optional[int] = None,
    ):
        await self._send(
            "public/eth_deposit_address",
            id,
        )

    async def verify_internal_transfer(
        self,
        destination_account_number: str,
        assets: Optional[List[Asset]] = None,
        positions: Optional[List[Position]] = None,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/verify_internal_transfer",
            id,
            destination_account_number=destination_account_number,
            assets=assets,
            positions=positions,
        )

    async def internal_transfer(
        self,
        destination_account_number: str,
        assets: Optional[List[Asset]] = None,
        positions: Optional[List[Position]] = None,
        label: Optional[str] = None,
        id: Optional[int] = None,
    ):
        await self._send(
            "private/internal_transfer",
            id,
            destination_account_number=destination_account_number,
            assets=assets,
            positions=positions,
            label=label,
        )

    async def system_info(
        self,
        id: Optional[int] = None,
    ):
        await self._send(
            "public/system_info",
            id,
        )
