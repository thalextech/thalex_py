import enum
import json
import logging

import jwt
import time
from typing import Optional, List, Union

import websockets
from websockets.protocol import State as WsState


def _make_auth_token(kid, private_key):
    return jwt.encode(
        {"iat": time.time()},
        private_key,
        algorithm="RS512",
        headers={"kid": kid},
    )


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


class Product(enum.Enum):
    BTC_FUTURES = "FBTCUSD"
    BTC_OPTIONS = "OBTCUSD"
    ETH_FUTURES = "FETHUSD"
    ETH_OPTIONS = "OETHUSD"


class Resolution(enum.Enum):
    M1 = "1m"
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    D1 = "1d"
    W1 = "1w"


class Sort(enum.Enum):
    ASC = "ascending"
    DESC = "descending"


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

    def __repr__(self):
        return f"{self.a}@{self.p}"


class Quote:
    def __init__(
        self, instrument_name: str, bid: Optional[SideQuote], ask: Optional[SideQuote]
    ):
        self.i = instrument_name
        self.b = bid
        self.a = ask

    def dumps(self):
        d = {"i": self.i}
        if self.b is not None:
            d["b"] = self.b.dumps()
        if self.a is not None:
            d["a"] = self.a.dumps()
        return d

    def __repr__(self):
        return f"(b: {self.b}, a: {self.a})"


class Asset:
    def __init__(
        self,
        asset_name: str,
        amount: float,
    ):
        self.asset_name = asset_name
        self.amount = amount

    def dumps(self):
        return {"asset_name": self.asset_name, "amount": self.amount}


class Position:
    def __init__(
        self,
        instrument_name: str,
        amount: float,
    ):
        self.instrument_name = instrument_name
        self.amount = amount

    def dumps(self):
        return {"instrument_name": self.instrument_name, "amount": self.amount}


class Combo:
    def __init__(
        self,
        instrument_name: float,
        quantity: int,
    ):
        self.instrument_name = instrument_name
        self.quantity = quantity

    def dumps(self):
        return {"instrument_name": self.instrument_name, "quantity": self.quantity}


class Thalex:
    def __init__(self, network: Network, user_agent: str = "ThalexPythonBot/1.1"):
        self.net: Network = network
        self.ws: websockets.client = None
        self.user_agent = user_agent

    async def receive(self):
        return await self.ws.recv()

    def connected(self):
        return self.ws is not None and self.ws.state in [WsState.CONNECTING, WsState.OPEN]

    async def connect(self):
        headers = {"User-Agent": self.user_agent}
        self.ws = await websockets.connect(
            self.net.value, ping_interval=5, additional_headers=headers
        )

    async def disconnect(self):
        await self.ws.close()

    async def _send(self, method: str, id: Optional[int], **kwargs):
        request = {"method": method, "params": {}}
        if id is not None:
            request["id"] = id
        for key, value in kwargs.items():
            if value is not None:
                request["params"][key] = value
        request = json.dumps(request)
        logging.debug(f"Sending {request=}")
        await self.ws.send(request)

    async def login(
        self,
        key_id: str,
        private_key: str,
        account: Optional[str] = None,
        id: Optional[int] = None,
    ):
        """Login

        :key_id:  The key id to use
        :private_key:  Private key to use
        :account:  Number of an account to select for use in this session. Optional, if not specified,
            default account for the API key is selected.
        """
        await self._send(
            "public/login",
            id,
            token=_make_auth_token(key_id, private_key),
            account=account,
        )

    async def set_cancel_on_disconnect(self, timeout_secs: int, id: Optional[int] = None):
        """Set cancel on disconnect

        :timeout_secs:  Heartbeat interval
        """
        await self._send(
            "private/set_cancel_on_disconnect",
            id,
            timeout_secs=timeout_secs,
        )

    async def instruments(self, id: Optional[int] = None):
        """Active instruments"""
        await self._send("public/instruments", id)

    async def all_instruments(self, id: Optional[int] = None):
        """All instruments"""
        await self._send("public/all_instruments", id)

    async def instrument(self, instrument_name: str, id: Optional[int] = None):
        """Single instrument

        :instrument_name:  Name of the instrument to query.
        """
        await self._send("public/instrument", id, instrument_name=instrument_name)

    async def ticker(self, instrument_name: str, id: Optional[int] = None):
        """Single ticker value

        :instrument_name:  Name of the instrument to query.
        """
        await self._send("public/ticker", id, instrument_name=instrument_name)

    async def index(self, underlying: str, id: Optional[int] = None):
        """Single index value

        :underlying:  The underlying (e.g. `BTCUSD`).
        """
        await self._send("public/index", id, underlying=underlying)

    async def book(self, instrument_name: str, id: Optional[int] = None):
        """Single order book

        :instrument_name:  Name of the instrument to query.
        """
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
        """Insert order

        :direction:  Direction
        :client_order_id:  Session-local identifier for this order. Only valid for websocket sessions. If set,
            must be an integer between 0 and 2^64-1, inclusive. When using numbers larger than 2^32,
            please beware of implicit floating point conversions in some JSON libraries.
        :instrument_name:  Instrument name
        :price:  Limit price; required for limit orders.
        :amount:  Amount of currency to trade (e.g. BTC for futures).
        :label: {'type': 'string'},
        :order_type:  OrderType, default': 'limit'
        :time_in_force:  Note that for limit orders, the default `time_in_force` is `good_till_cancelled`,
            while for market orders, the default is `immediate_or_cancel`.
            It is illegal to send a GTC market order, or an IOC post order.
        :post_only:  If the order price is in cross with the current best price on the opposite side in the
            order book, then the price is adjusted to one tick away from that price, ensuring that
            the order will never trade on insert. If the adjusted price of a buy order falls at or
            below zero where not allowed, then the order is cancelled with delete reason 'immediate_cancel'.
        :reject_post_only:  This flag is only effective in combination with post_only.
            If set, then instead of adjusting the order price, the order will be cancelled with delete reason 'immediate_cancel'.
            The combination of post_only and reject_post_only is effectively a book-or-cancel order.
        :reduce_only:  An order marked `reduce_only` will have its amount reduced to the open position.
            If there is no open position, or if the order direction would cause an increase of the open position,
            the order is rejected. If the order is placed in the book, it will be subsequently monitored,
            and reduced to the open position if the position changes through other means (best effort).
            Multiple reduce-only orders will all be reduced individually.
        :collar:  If the instrument has a safety price collar set, and the limit price of the order
            (infinite for market orders) is in cross with (more aggressive than) this collar, how to handle.
            If set to `ignore`, the order will proceed as requested. If `reject`,\nthe order fails early.
            If `clamp`, the price is adjusted to the collar.
            The default is `clamp` for market orders and `reject` for everything else.
            Collar `ignore` is forbidden for market orders.
        """
        await self._send(
            "private/insert",
            id,
            direction=direction.value,
            instrument_name=instrument_name,
            amount=amount,
            client_order_id=client_order_id,
            price=price,
            label=label,
            order_type=order_type and order_type.value,
            time_in_force=time_in_force and time_in_force.value,
            post_only=post_only,
            reject_post_only=reject_post_only,
            reduce_only=reduce_only,
            collar=collar,
        )

    async def insert_combo(
        self,
        direction: Direction,
        legs: List[Combo],
        amount: float,
        client_order_id: Optional[int] = None,
        price: Optional[float] = None,
        label: Optional[str] = None,
        order_type: Optional[OrderType] = None,
        time_in_force: Optional[TimeInForce] = TimeInForce.IOC,
        collar: Optional[Collar] = None,
        id: Optional[int] = None,
    ):
        """Insert order

        :direction:  Direction
        :client_order_id:  Session-local identifier for this order. Only valid for websocket sessions. If set,
            must be an integer between 0 and 2^64-1, inclusive. When using numbers larger than 2^32,
            please beware of implicit floating point conversions in some JSON libraries.
        :legs:  List of legs for a combination order.
            There must be at least two and at most four legs specified. All leg instruments must be distinct.
            Other constraints apply, please check trading information page on combination orders.
        :price:  Limit price; required for limit orders. Specifies limit price per unit of the combination.
        :amount:  Specifies the amount of units of the combination to trade.
        :label: {'type': 'string'},
        :order_type:  OrderType, default': 'limit'
        :time_in_force:  Must always be set to IOC (`immediate_or_cancel`).
            It is illegal to send a GTC market order, or an IOC post order.
        :collar:  If the instrument has a safety price collar set, and the limit price of the order
            (infinite for market orders) is in cross with (more aggressive than) this collar, how to handle.
            If set to `ignore`, the order will proceed as requested. If `reject`,\nthe order fails early.
            If `clamp`, the price is adjusted to the collar.
            The default is `clamp` for market orders and `reject` for everything else.
            Collar `ignore` is forbidden for market orders.
            Price collar is a linear combination of the leg collars with their corresponding quantities as coefficients.
        """
        await self._send(
            "private/insert",
            id,
            direction=direction.value,
            legs=legs,
            amount=amount,
            client_order_id=client_order_id,
            price=price,
            label=label,
            order_type=order_type and order_type.value,
            time_in_force=time_in_force and time_in_force.value,
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
        """Insert buy order

        :client_order_id:  Session-local identifier for this order. Only valid for websocket sessions. If set,
            must be an integer between 0 and 2^64-1, inclusive. When using numbers larger than 2^32,
            please beware of implicit floating point conversions in some JSON libraries.
        :instrument_name:  Instrument name
        :price:  Limit price; required for limit orders.
        :amount:  Amount of currency to trade (e.g. BTC for futures).
        :label: {'type': 'string'},
        :order_type:  OrderType, default': 'limit'
        :time_in_force:  Note that for limit orders, the default `time_in_force` is `good_till_cancelled`,
            while for market orders, the default is `immediate_or_cancel`.
            It is illegal to send a GTC market order, or an IOC post order.
        :post_only:  If the order price is in cross with the current best price on the opposite side in the
            order book, then the price is adjusted to one tick away from that price, ensuring that
            the order will never trade on insert. If the adjusted price of a buy order falls at or
            below zero where not allowed, then the order is cancelled with delete reason 'immediate_cancel'.
        :reject_post_only:  This flag is only effective in combination with post_only.
            If set, then instead of adjusting the order price, the order will be cancelled with delete reason 'immediate_cancel'.
            The combination of post_only and reject_post_only is effectively a book-or-cancel order.
        :reduce_only:  An order marked `reduce_only` will have its amount reduced to the open position.
            If there is no open position, or if the order direction would cause an increase of the open position,
            the order is rejected. If the order is placed in the book, it will be subsequently monitored,
            and reduced to the open position if the position changes through other means (best effort).
            Multiple reduce-only orders will all be reduced individually.
        :collar:  If the instrument has a safety price collar set, and the limit price of the order
            (infinite for market orders) is in cross with (more aggressive than) this collar, how to handle.
            If set to `ignore`, the order will proceed as requested. If `reject`,\nthe order fails early.
            If `clamp`, the price is adjusted to the collar.
            The default is `clamp` for market orders and `reject` for everything else.
            Collar `ignore` is forbidden for market orders.
        """
        await self._send(
            "private/buy",
            id,
            instrument_name=instrument_name,
            amount=amount,
            client_order_id=client_order_id,
            price=price,
            label=label,
            order_type=order_type and order_type.value,
            time_in_force=time_in_force and time_in_force.value,
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
        """Insert sell order

        :client_order_id:  Session-local identifier for this order. Only valid for websocket sessions. If set,
            must be an integer between 0 and 2^64-1, inclusive. When using numbers larger than 2^32,
            please beware of implicit floating point conversions in some JSON libraries.
        :instrument_name:  Instrument name
        :price:  Limit price; required for limit orders.
        :amount:  Amount of currency to trade (e.g. BTC for futures).
        :label: {'type': 'string'},
        :order_type:  OrderType, default': 'limit'
        :time_in_force:  Note that for limit orders, the default `time_in_force` is `good_till_cancelled`,
            while for market orders, the default is `immediate_or_cancel`.
            It is illegal to send a GTC market order, or an IOC post order.
        :post_only:  If the order price is in cross with the current best price on the opposite side in the
            order book, then the price is adjusted to one tick away from that price, ensuring that
            the order will never trade on insert. If the adjusted price of a buy order falls at or
            below zero where not allowed, then the order is cancelled with delete reason 'immediate_cancel'.
        :reject_post_only:  This flag is only effective in combination with post_only.
            If set, then instead of adjusting the order price, the order will be cancelled with delete reason 'immediate_cancel'.
            The combination of post_only and reject_post_only is effectively a book-or-cancel order.
        :reduce_only:  An order marked `reduce_only` will have its amount reduced to the open position.
            If there is no open position, or if the order direction would cause an increase of the open position,
            the order is rejected. If the order is placed in the book, it will be subsequently monitored,
            and reduced to the open position if the position changes through other means (best effort).
            Multiple reduce-only orders will all be reduced individually.
        :collar:  If the instrument has a safety price collar set, and the limit price of the order
            (infinite for market orders) is in cross with (more aggressive than) this collar, how to handle.
            If set to `ignore`, the order will proceed as requested. If `reject`,\nthe order fails early.
            If `clamp`, the price is adjusted to the collar.
            The default is `clamp` for market orders and `reject` for everything else.
            Collar `ignore` is forbidden for market orders.
        """
        await self._send(
            "private/sell",
            id,
            instrument_name=instrument_name,
            amount=amount,
            client_order_id=client_order_id,
            price=price,
            label=label,
            order_type=order_type and order_type.value,
            time_in_force=time_in_force and time_in_force.value,
            post_only=post_only,
            reject_post_only=reject_post_only,
            reduce_only=reduce_only,
            collar=collar,
        )

    async def amend(
        self,
        amount: float,
        price: float,
        order_id: Optional[str] = None,
        client_order_id: Optional[int] = None,
        collar: Optional[Collar] = None,
        id: Optional[int] = None,
    ):
        """Amend order

        :client_order_id:  Exactly one of `client_order_id` or `order_id` must be specified.
        :order_id:  Exactly one of `client_order_id` or `order_id` must be specified.
        :price: number
        :amount: number
        :collar:  If the instrument has a safety price collar set, and the new limit price
            is in cross with (more aggressive than) this collar,
            how to handle. If set to `ignore`, the amend will proceed as requested. If `reject`,
            the request fails early. If `clamp`, the price is adjusted to the collar.
            The default is `reject`.
        """
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
        order_id: Optional[str] = None,
        client_order_id: Optional[int] = None,
        id: Optional[int] = None,
    ):
        """Cancel order

        :client_order_id:  Exactly one of `client_order_id` or `order_id` must be specified.
        :order_id:  Exactly one of `client_order_id` or `order_id` must be specified.
        """
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
        """Bulk cancel all orders"""
        await self._send(
            "private/cancel_all",
            id,
        )

    async def cancel_session(
        self,
        id: Optional[int] = None,
    ):
        """Bulk cancel all orders in session"""
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
        """Create a request for quote

        :legs:  Specify any number of legs that you'd like to trade in a single package. Leg amounts
            may be positive (long) or negative (short), and must adhere to the regular volume tick size for the
            respective instrument. At least one leg must be long.
        :label:  User label for this RFQ, which will be reflected in eventual trades.
        """
        await self._send(
            "private/create_rfq", id, legs=[leg.dumps() for leg in legs], label=label
        )

    async def cancel_rfq(
        self,
        rfq_id,
        id: Optional[int] = None,
    ):
        """Cancel an RFQ

        :rfq_id:  The ID of the RFQ to be cancelled
        """
        await self._send("private/cancel_rfq", id, rfq_id=rfq_id)

    async def trade_rfq(
        self,
        rfq_id: str,
        direction: Direction,
        limit_price: float,
        id: Optional[int] = None,
    ):
        """Trade an RFQ

        :rfq_id:  The ID of the RFQ
        :direction:  Whether to buy or sell. *Important*: this relates to the combination as created by the system,
            *not* the package as originally requested (although they should be equal).
        :limit_price:  The maximum (for buy) or minimum (for sell) price to trade at.
            This is the price for one combination, not for the entire package.
        """
        await self._send(
            "private/trade_rfq",
            id,
            rfq_id=rfq_id,
            direction=direction.value,
            limit_price=limit_price,
        )

    async def open_rfqs(self, id: Optional[int] = None):
        """Retrieves a list of open RFQs created by this account."""
        await self._send("private/open_rfqs", id)

    async def mm_rfqs(
        self,
        id: Optional[int] = None,
    ):
        """Retrieves a list of open RFQs that this account has access to."""
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
        """Quote on an RFQ

        :rfq_id:  The ID of the RFQ this quote is for.
        :client_order_id:  Session-local identifier for this order. Only valid for websocket sessions. If set, must be a
            number between 0 and 2^64-1, inclusive. When using numbers larger than 2^32, please beware of implicit
            floating point conversions in some JSON libraries.
        :direction:  The side of the quote.
        :price:  Limit price for the quote (for one combination).
        :amount:  Number of combinations to quote. Anything over the requested amount will not be visible to the requester.
        :label:  A label to attach to eventual trades.
        """
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
        """Amend quote

        :client_order_id:  Exactly one of `client_order_id` or `order_id` must be specified.
        :order_id:  Exactly one of `client_order_id` or `order_id` must be specified.
        :price:  Limit price for the quote (for one combination).
        :amount:  Number of combinations to quote. Anything over the requested amount will not be visible to the requester.
        """
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
        """Delete quote

        :client_order_id:  Exactly one of `client_order_id` or `order_id` must be specified.
        :order_id:  Exactly one of `client_order_id` or `order_id` must be specified.
        """
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
        """List of active quotes"""
        await self._send(
            "private/mm_rfq_quotes",
            id,
        )

    async def portfolio(
        self,
        id: Optional[int] = None,
    ):
        """Portfolio"""
        await self._send(
            "private/portfolio",
            id,
        )

    async def open_orders(
        self,
        id: Optional[int] = None,
    ):
        """Open orders"""
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
        """Order history

        :limit:  Max results to return.
        :time_low:  Start time (UNIX timestamp) defaults to zero.
        :time_high:  End time (UNIX timestamp) defaults to now.
        :bookmark:  Set to bookmark from previous call to get next page.
        """
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
        sort: Optional[Sort] = None,
        instrument_names: Optional[str] = None,
        bot_ids: Optional[str] = None,
        id: Optional[int] = None,
    ):
        """Trade history

        :limit:  Max results to return.
        :time_low:  Start time (UNIX timestamp) defaults to zero.
        :time_high:  End time (UNIX timestamp) defaults to now.
        :bookmark:  Set to bookmark from previous call to get next page.
        """
        await self._send(
            "private/trade_history",
            id,
            limit=limit,
            time_low=time_low,
            time_high=time_high,
            bookmark=bookmark,
            sort=sort and sort.value,
            instrument_names=instrument_names,
            bot_ids=bot_ids,
        )

    async def daily_mark_history(
        self,
        limit: Optional[int] = None,
        time_low: Optional[int] = None,
        time_high: Optional[int] = None,
        bookmark: Optional[str] = None,
        id: Optional[int] = None,
    ):
        """For instruments that are subject to futures-style settlement we perform daily settlement at the mark price.
            The settlement procedure realizes the positional and perpetual funding profits/losses
            accumulated during the session, and resets the start price of the position to the mark price.
            This API endpoint returns a historical log of settled profits/losses (daily marks).
        :limit:  Max results to return.
        :time_low:  Start time (UNIX timestamp) defaults to zero.
        :time_high:  End time (UNIX timestamp) defaults to now.
        :bookmark:  Set to bookmark from previous call to get next page.
        """
        await self._send(
            "private/daily_mark_history",
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
        """Transaction history

        :limit:  Max results to return.
        :time_low:  Start time (UNIX timestamp) defaults to zero.
        :time_high:  End time (UNIX timestamp) defaults to now.
        :bookmark:  Set to bookmark from previous call to get next page.
        """
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
        """RFQ history

        :limit:  Max results to return.
        :time_low:  Start time (UNIX timestamp) defaults to zero.
        :time_high:  End time (UNIX timestamp) defaults to now.
        :bookmark:  Set to bookmark from previous call to get next page.
        """
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
        """Account breakdown"""
        await self._send(
            "private/account_breakdown",
            id,
        )

    async def account_summary(
        self,
        id: Optional[int] = None,
    ):
        """Account summary"""
        await self._send(
            "private/account_summary",
            id,
        )

    async def required_margin_breakdown(
        self,
        id: Optional[int] = None,
    ):
        """Margin breakdown"""
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
        """Margin breakdown with order

        :instrument_name:  The name of the instrument of this hypothetical order with which the margin is to be broken down with.
        :price:  The price of the hypothetical order.
        :amount:  The amount that would be traded.
        """
        await self._send(
            "private/required_margin_for_order",
            id,
            instrument_name=instrument_name,
            amount=amount,
            price=price,
        )

    async def required_margin_for_combo_order(
        self,
        legs: List[Combo],
        price: float,
        amount: float,
        id: Optional[int] = None,
    ):
        """Margin breakdown with combination order

        :legs:  List of legs for a combination order.
            There must be at least two and at most four legs specified. All leg instruments must be distinct.
            Other constraints apply, please check trading information page on combination orders.
        :price:  The price of the hypothetical order.
        :amount:  The amount that would be traded.
        """
        await self._send(
            "private/required_margin_for_order",
            id,
            legs=legs,
            amount=amount,
            price=price,
        )

    async def private_subscribe(self, channels: [str], id: Optional[int] = None):
        """Subscribe to private channels

        :channels:  List of channels to subscribe to.
        """
        await self._send("private/subscribe", id, channels=channels)

    async def public_subscribe(self, channels: [str], id: Optional[int] = None):
        """Subscribe to public channels

        :channels:  List of channels to subscribe to.
        """
        await self._send("public/subscribe", id, channels=channels)

    async def unsubscribe(self, channels: [str], id: Optional[int] = None):
        """Unsubscribe

        :channels:  List of channels to unsubscribe from. Public and private channels may be mixed.
        """
        await self._send("unsubscribe", id, channels=channels)

    async def conditional_orders(
        self,
        id: Optional[int] = None,
    ):
        """Conditional orders"""
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
        """Create conditional order

        :direction: enum
        :instrument_name: string
        :amount: number
        :limit_price:  If set, creates a stop limit order
        :target:  The trigger target that `stop_price` and `bracket_price` refer to.
        :stop_price:  Trigger price
        :bracket_price:  If set, creates a bracket order
        :trailing_stop_callback_rate:  If set, creates a trailing stop order
        :label:  Label will be set on the activated order
        :reduce_only:  Activated order will be reduce-only
        """
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
        """Cancel conditional order

        :order_id: string
        """
        await self._send(
            "private/cancel_conditional_order",
            id,
            order_id=order_id,
        )

    async def cancel_all_conditional_orders(
        self,
        id: Optional[int] = None,
    ):
        """Bulk cancel conditional orders"""
        await self._send(
            "private/cancel_all_conditional_orders",
            id,
        )

    async def notifications_inbox(
        self,
        limit: Optional[int] = None,
        id: Optional[int] = None,
    ):
        """Notifications inbox

        :limit:  Max results to return.
        """
        await self._send("private/notifications_inbox", id, limit=limit)

    async def mark_inbox_notification_as_read(
        self,
        notification_id: str,
        read: Optional[bool] = None,
        id: Optional[int] = None,
    ):
        """Marking notification as read

        :notification_id:  ID of the notification to mark.
        :read:  Set to `true` to mark as read, `false` to mark as not read.
        """
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
        reject_post_only: Optional[bool] = None,
        id: Optional[int] = None,
    ):
        """Send a mass quote

        :quotes:  List of quotes (maximum 100).

            Each item is a double sided quote on a single instrument. A quote atomically replace a previous quote. Both
            bid and ask price may be specified. If either bid or ask is not specified, that side is *not* replaced or
            removed. If a double-sided quote for an instrument that was specified in an earlier call is omitted from the next
            call, that quote is *not* removed or replaced. To remove a quote, set the amount to zero.

            To replace only some of the quotes you have, send only the quotes (sides) you need to replace.

            Sending a quote with the exact same price and amount as in the previous call *will* replace the quote, which
            will result in the quote losing priority. It is thus advised to avoid sending duplicate quotes.

            Note that mass quoting only allows for a one level quote on each side on the instrument. I.e. if you specify
            two or more double sided quotes on the same instrument then the quotes occurring earlier in the list will be
            replaced by the quotes occurring later in the list, as if all the double sided quotes for the same instrument
            were sent in separate API calls.

            Note that market maker protection must have been configured for the instrument's product group, and both bid
            and ask amount must not exceed the most recent protection configuration amount.

        :label:  Optional user label to apply to every quote side.
        :post_only:  If set, price may be widened so it will not cross an existing order in the book.
            If the adjusted price for any bid falls at or below zero where not allowed, then
            that side will be removed with delete reason 'immediate_cancel'.
        :reject_post_only: This flag is only effective in combination with post_only.
            If set, then instead of adjusting the order price,
            the order will be cancelled with delete reason 'immediate_cancel'.
            The combination of post_only and reject_post_only is effectively a book-or-cancel order.
        """
        await self._send(
            "private/mass_quote",
            id,
            quotes=[q.dumps() for q in quotes],
            label=label,
            post_only=post_only,
            reject_post_only=reject_post_only,
        )

    async def cancel_mass_quote(
        self,
        product: Optional[Union[Product, str]] = None,
        id: Optional[int] = None,
    ):
        """Cancel mass quotes across all sessions.
        If a product is set, only the quotes on that product will be cancelled.
        Otherwise all quotes are cancelled.

        Note that market maker protection groups are reset and must be re-initialised.

        :product: If set, only the mass quotes on this product will be cancelled.
        """
        if isinstance(product, Product):
            product = product.value
        await self._send("private/cancel_mass_quote", id, product=product)

    async def set_mm_protection(
        self,
        product: Union[Product, str],
        trade_amount: float,
        quote_amount: float,
        amount: Optional[float] = None,
        id: Optional[int] = None,
    ):
        """Set the maximum trading amount for mass quote orders on a particular product in this session.
        After this amount is executed, the remaining mass quotes on the product in this session are cancelled.

        These settings affect only a particular protection group (protection set for a product group within a session).

        Note that the amount can be overshot under certain conditions.

        See Mass quoting and market maker protection section for more information.

        :product:  Product group ('F' + index or 'O' + index)
        :trade_amount: Total amount of mass quote orders (number of contracts) on this protection group
            that is allowed to be executed before the remaining mass quotes are canceled.
            The value must be lower or equal to the quote_amount.
        :quote_amount: Maximum amount of a single quote on this protection group.
            Any orders larger than this will be rejected. Mass quote margin requirements are calculated for this amount.
        :amount: Deprecated. Overwrites 'trade_amount' and 'quote_amount' with this value.
        """
        if isinstance(product, Product):
            product = product.value
        await self._send(
            "private/set_mm_protection",
            id,
            product=product,
            trade_amount=trade_amount,
            quote_amount=quote_amount,
            amount=amount,
        )

    async def verify_withdrawal(
        self,
        asset_name: str,
        amount: float,
        target_address: str,
        id: Optional[int] = None,
    ):
        """Verify if withdrawal is possible

        :asset_name:  Asset name.
        :amount:  Amount to withdraw.
        :target_address:  Target address.
        """
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
        """Withdraw assets

        :asset_name:  Asset name.
        :amount:  Amount to withdraw.
        :target_address:  Target address.
        :label:  Optional label to attach to the withdrawal request.
        """
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
        """Withdrawals"""
        await self._send(
            "public/crypto_withdrawals",
            id,
        )

    async def crypto_deposits(
        self,
        id: Optional[int] = None,
    ):
        """Deposits"""
        await self._send(
            "public/crypto_deposits",
            id,
        )

    async def btc_deposit_address(
        self,
        id: Optional[int] = None,
    ):
        """Bitcoin deposit address"""
        await self._send(
            "public/btc_deposit_address",
            id,
        )

    async def eth_deposit_address(
        self,
        id: Optional[int] = None,
    ):
        """Ethereum deposit address"""
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
        """Verify internal transfer

        :destination_account_number:  Destination account number.
        :assets: array
        :positions: array
        """
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
        """Internal transfer

        :destination_account_number:  Destination account number.
        :assets: array
        :positions: array
        :label:  Optional label attached to the transfer.
        """
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
        """System info"""
        await self._send(
            "public/system_info",
            id,
        )

    async def mark_price_historical_data(
        self,
        instrument_name: str,
        ts_from: float,
        ts_to: float,
        resolution: Union[Resolution, str],
        id: Optional[int] = None,
    ):
        """Returns mark price historical data in the specified interval and resolution in OHLC format.

        :instrument_name:  Feedcode of the instrument (e.g. BTC-PERPETUAL).
        :ts_from:  Start time (Unix timestamp).
        :ts_to:  End time (Unix timestamp) (exclusive).
        :resolution:  Enum: "1m" "5m" "15m" "30m" "1h" "1d" "1w"
            Each data point will be aggregated using OHLC according to the specified resolution.
        """
        if isinstance(resolution, Resolution):
            resolution = resolution.value
        request = {
            "method": "public/mark_price_historical_data",
            "params": {
                "instrument_name": instrument_name,
                "from": ts_from,
                "to": ts_to,
                "resolution": resolution,
            },
            "id": id,
        }
        request = json.dumps(request)
        logging.debug(f"Sending {request=}")
        await self.ws.send(request)

    async def index_price_historical_data(
        self,
        index_name: str,
        ts_from: float,
        ts_to: float,
        resolution: Union[Resolution, str],
        id: Optional[int] = None,
    ):
        """Returns index price historical data in the specified interval and resolution in OHLC format.

        :index_name:  Index name (e.g. BTCUSD, ETHUSD).
        :ts_from:  Start time (Unix timestamp).
        :ts_to:  End time (Unix timestamp) (exclusive).
        :resolution:  Enum: "1m" "5m" "15m" "30m" "1h" "1d" "1w"
            Each data point will be aggregated using OHLC according to the specified resolution.
        """
        if isinstance(resolution, Resolution):
            resolution = resolution.value
        request = {
            "method": "public/index_price_historical_data",
            "params": {
                "index_name": index_name,
                "from": ts_from,
                "to": ts_to,
                "resolution": resolution,
            },
            "id": id,
        }
        request = json.dumps(request)
        logging.debug(f"Sending {request=}")
        await self.ws.send(request)

    async def create_sgsl_bot(
        self,
        end_time: float,
        instrument_name: str,
        signal: Target,
        entry_price: float,
        target_position: float,
        exit_price: float,
        exit_position: float,
        max_slippage: Optional[float] = None,
        label: Optional[str] = None,
        id: Optional[int] = None,
    ):
        """Instantiate a bot that keeps continually trading in your name according to the sgsl strategy.
         See https://www.thalex.com/docs/#tag/bot_strategies/Start-Gain-Stop-Loss for more info on the strategy.
         For risk fencing reasons and because of the complex ways manual trades can interact with bot strategies,
         you might want to consider running bots on a separate/dedicated sub account.
         Also be aware that you can set up bots with different strategies in a way that they would end up trading with each other.

        :instrument_name:  Name of the instrument to run SGSL on.
        :signal: The target that entry_price and exit_price will be compared to.
        :entry_price: Price to compare signal price to, to determine necessary adjustments to the portfolio.
        :target_position: The target position to maintain in the subaccount if signal price is above entry_price.
        :exit_price: Price to compare signal price to, to determine necessary adjustments to the portfolio.
        :exit_position: The target position to maintain in the subaccount if signal price is below exit_price.
        :max_slippage: Maximum slippage per trade, expressed as % of the traded instruments mark price.
        :end_time: Timestamp when the bot should stop executing.
            When end_time is reached, the bot will leave all positions intact, it will not open/close any of them.
        :label:  A label that the bot will add to all orders for easy identification.
        """
        await self._send(
            "private/create_bot",
            id,
            strategy="sgsl",
            instrument_name=instrument_name,
            signal=signal and signal.value,
            entry_price=entry_price,
            target_position=target_position,
            exit_price=exit_price,
            exit_position=exit_position,
            max_slippage=max_slippage,
            end_time=end_time,
            label=label,
        )

    async def create_ocq_bot(
        self,
        end_time: float,
        instrument_name: str,
        signal: Target,
        bid_offset: float,
        ask_offset: float,
        quote_size: float,
        min_position: float,
        max_position: float,
        exit_offset: Optional[float] = None,
        target_position: Optional[float] = None,
        label: Optional[str] = None,
        id: Optional[int] = None,
    ):
        """Instantiate a bot that keeps continually trading in your name according to the ocq strategy.
         See https://www.thalex.com/docs/#tag/bot_strategies/One-Click-Quoter for more info on the strategy.
         For risk fencing reasons and because of the complex ways manual trades can interact with bot strategies,
         you might want to consider running bots on a separate/dedicated sub account.
         Also be aware that you can set up bots with different strategies in a way that they would end up trading with each other.

        :instrument_name:  Name of the instrument to run OCQ on.
        :signal: The target signal to offset the quotes from.
        :bid_offset: The offset of the price of the bid quote from the signal price. Must be smaller than ask_offset.
        :ask_offset: The offset of the price of the ask quote from the signal price. Must be greater than bid_offset.
        :exit_offset: Optional offset of the price of the exit quote from the signal price. Must be between bid_offset and ask_offset.
        :quote_size: The default size of both the bid and ask quote.
        :min_position: The minimum portfolio position to maintain in the subaccount. Must be smaller than max_position.
        :max_position: The maximum portfolio position to maintain in the subaccount. Must be greater than min_position.
        :target_position: Optional portfolio position to maintain in the subaccount. Must be between min_position and max_position.
        :end_time: Timestamp when the bot should stop executing.
            When end_time is reached, the bot will leave all positions intact, it will not open/close any of them.
        :label:  A label that the bot will add to all orders for easy identification.
        """
        await self._send(
            "private/create_bot",
            id,
            strategy="ocq",
            instrument_name=instrument_name,
            signal=signal and signal.value,
            bid_offset=bid_offset,
            ask_offset=ask_offset,
            quote_size=quote_size,
            min_position=min_position,
            max_position=max_position,
            exit_offset=exit_offset,
            target_position=target_position,
            end_time=end_time,
            label=label,
        )

    async def create_levels_bot(
        self,
        end_time: float,
        instrument_name: str,
        bids: [float],
        asks: [float],
        step_size: float,
        base_position: Optional[float] = None,
        target_mean_price: Optional[float] = None,
        upside_exit_price: Optional[float] = None,
        downside_exit_price: Optional[float] = None,
        max_slippage: Optional[float] = None,
        label: Optional[str] = None,
        id: Optional[int] = None,
    ):
        """Instantiate a bot that keeps continually trading in your name according to the ocq strategy.
         See https://www.thalex.com/docs/#tag/bot_strategies/Levels for more info on the strategy.
         For risk fencing reasons and because of the complex ways manual trades can interact with bot strategies,
         you might want to consider running bots on a separate/dedicated sub account.
         Also be aware that you can set up bots with different strategies in a way that they would end up trading with each other.

        :instrument_name:  Name of the instrument to run Levels bot on.
        :bids: The default price levels the bot will be bidding at.
        :asks: The default price levels the bot will be quoting on the ask side.
        :step_size: The default size to quote on one level.
        :base_position: Will be used as a reference to compare the subaccount's portfolio position in the instrument that this bot is trading to.
            Defaults to the portfolio position of the subaccount in instrument_name at the time of sending the request.
        :target_mean_price: The price level to exit positions at.
        :upside_exit_price: If the mark price of the instrument this bot is trading goes above `upside_exit_price`,
                the bot cancels the maker orders, aggressively trades into `base_position`, and then stops executing.
        :downside_exit_price: If the mark price of the instrument this bot is trading goes below `downside_exit_price`,
                the bot cancels the maker orders, aggressively trades into `base_position`, and then stops executing.
        :max_slippage: Maximum slippage per trade when exiting any position, expressed as % of the traded instruments mark price.
        :step_size: The default size of both the bid and ask quote.
        :end_time: Timestamp when the bot should stop executing.
            When end_time is reached, the bot will leave all positions intact, it will not open/close any of them.
        :label:  A label that the bot will add to all orders for easy identification.
        """
        await self._send(
            "private/create_bot",
            id,
            strategy="levels",
            instrument_name=instrument_name,
            bids=bids,
            asks=asks,
            step_size=step_size,
            base_position=base_position,
            target_mean_price=target_mean_price,
            upside_exit_price=upside_exit_price,
            downside_exit_price=downside_exit_price,
            max_slippage=max_slippage,
            end_time=end_time,
            label=label,
        )

    async def create_grid_bot(
        self,
        end_time: float,
        instrument_name: str,
        grid: [float],
        step_size: float,
        base_position: Optional[float] = None,
        target_mean_price: Optional[float] = None,
        upside_exit_price: Optional[float] = None,
        downside_exit_price: Optional[float] = None,
        max_slippage: Optional[float] = None,
        label: Optional[str] = None,
        id: Optional[int] = None,
    ):
        """Instantiate a bot that keeps continually trading in your name according to the ocq strategy.
         See https://www.thalex.com/docs/#tag/bot_strategies/Grid for more info on the strategy.
         For risk fencing reasons and because of the complex ways manual trades can interact with bot strategies,
         you might want to consider running bots on a separate/dedicated sub account.
         Also be aware that you can set up bots with different strategies in a way that they would end up trading with each other.

        :instrument_name:  Name of the instrument to run Grid bot on.
        :grid: The default price levels the bot will be quoting at.
        :step_size: The default size to quote on one level.
        :base_position: Will be used as a reference to compare the subaccount's portfolio position in the instrument that this bot is trading to.
            Defaults to the portfolio position of the subaccount in instrument_name at the time of sending the request.
        :target_mean_price: As long as the account's portfolio position in the instrument that the bot is trading is equal to base_position,
                the bot will insert step_size asks on the grid above target_mean_price (one per level),
                and step_size bids below target_mean_price.
        :upside_exit_price: If the mark price of the instrument this bot is trading goes above `upside_exit_price`,
                the bot cancels the maker orders, aggressively trades into `base_position`, and then stops executing.
        :downside_exit_price: If the mark price of the instrument this bot is trading goes below `downside_exit_price`,
                the bot cancels the maker orders, aggressively trades into `base_position`, and then stops executing.
        :max_slippage: Maximum slippage per trade when exiting any position, expressed as % of the traded instruments mark price.
        :end_time: Timestamp when the bot should stop executing.
            When end_time is reached, the bot will leave all positions intact, it will not open/close any of them.
        :label:  A label that the bot will add to all orders for easy identification.
        """
        await self._send(
            "private/create_bot",
            id,
            strategy="grid",
            instrument_name=instrument_name,
            grid=grid,
            step_size=step_size,
            base_position=base_position,
            target_mean_price=target_mean_price,
            upside_exit_price=upside_exit_price,
            downside_exit_price=downside_exit_price,
            max_slippage=max_slippage,
            end_time=end_time,
            label=label,
        )

    async def create_delta_hedger_bot(
        self,
        end_time: float,
        instrument_name: str,
        period: float,
        position: Optional[str] = None,
        target_delta: Optional[float] = None,
        threshold: Optional[float] = None,
        tolerance: Optional[float] = None,
        max_slippage: Optional[float] = None,
        label: Optional[str] = None,
        id: Optional[int] = None,
    ):
        """Instantiate a bot that keeps continually trading in your name according to the ocq strategy.
         See https://www.thalex.com/docs/#tag/bot_strategies/Delta-Hedger for more info on the strategy.
         For risk fencing reasons and because of the complex ways manual trades can interact with bot strategies,
         you might want to consider running bots on a separate/dedicated sub account.
         Also be aware that you can set up bots with different strategies in a way that they would end up trading with each other.

        :instrument_name:  Name of the instrument the bot will trade.
            Must be an outright instrument with at least 0.25 delta.
        :position: Name of an outright instrument. If specified, the bot will only hedge the position in this instrument, not the entire underlying.
        :target_delta: Delta to target. Defaults to 0.
        :threshold: Hedging threshold. Defaults to 0.
        :tolerance: Maximum deviation allowed from target deltas at any time.
        :period: Number of seconds to let the deltas stay outside of [target-threshold, target+threshold], before hedging them. Must be between 1 and 3600.
        :max_slippage: Maximum slippage per trade when exiting any position, expressed as % of the traded instruments mark price.
        :end_time: Timestamp when the bot should stop executing.
            When end_time is reached, the bot will leave all positions intact, it will not open/close any of them.
        :label:  A label that the bot will add to all orders for easy identification.
        """
        await self._send(
            "private/create_bot",
            id,
            strategy="dhedge",
            instrument_name=instrument_name,
            period=period,
            position=position,
            target_delta=target_delta,
            threshold=threshold,
            tolerance=tolerance,
            max_slippage=max_slippage,
            end_time=end_time,
            label=label,
        )

    async def create_delta_follower_bot(
        self,
        end_time: float,
        instrument_name: str,
        target_instrument: str,
        target_amount: float,
        period: float,
        threshold: Optional[float] = None,
        tolerance: Optional[float] = None,
        max_slippage: Optional[float] = None,
        label: Optional[str] = None,
        id: Optional[int] = None,
    ):
        """Instantiate a bot that keeps continually trading in your name according to the ocq strategy.
         See https://www.thalex.com/docs/#tag/bot_strategies/Delta-Follower for more info on the strategy.
         For risk fencing reasons and because of the complex ways manual trades can interact with bot strategies,
         you might want to consider running bots on a separate/dedicated sub account.
         Also be aware that you can set up bots with different strategies in a way that they would end up trading with each other.

        :instrument_name:  Name of the instrument the bot will trade.
            Must be an outright instrument with at least 0.25 delta.
        :target_instrument: Name of the outright option to follow the deltas of.
        :target_amount: Amount of target_instrument contracts to follow the deltas of. Must be between 0.1 and 1000.
        :threshold: Hedging threshold. Defaults to 0.
        :tolerance: Maximum deviation allowed from target deltas at any time.
        :period: Number of seconds to let the deltas stay outside of [target-threshold, target+threshold], before hedging them. Must be between 1 and 3600.
        :max_slippage: Maximum slippage per trade when exiting any position, expressed as % of the traded instruments mark price.
        :end_time: Timestamp when the bot should stop executing.
            When end_time is reached, the bot will leave all positions intact, it will not open/close any of them.
        :label:  A label that the bot will add to all orders for easy identification.
        """
        await self._send(
            "private/create_bot",
            id,
            strategy="dfollow",
            instrument_name=instrument_name,
            period=period,
            target_instrument=target_instrument,
            target_amount=target_amount,
            threshold=threshold,
            tolerance=tolerance,
            max_slippage=max_slippage,
            end_time=end_time,
            label=label,
        )

    async def cancel_bot(self, bot_id: str, id: Optional[int] = None):
        """Cancel a specific bot instance.

        :bot_id: The bot_id returned when creating the bot, or calling private/bots.
        """
        await self._send("private/cancel_bot", id, bot_id=bot_id)

    async def cancel_all_bots(self, id: Optional[int] = None):
        """Cancel all bots of this subaccount"""
        await self._send("private/cancel_all_bots", id)

    async def bots(self, id: Optional[int] = None):
        """Get the bots of this subaccount."""
        await self._send("private/bots", id)
