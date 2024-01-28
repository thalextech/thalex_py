import json
import enum
import logging
from typing import Dict, List, Optional


class Channel(enum.Enum):
    LWT = "lwt."
    TICKER = "ticker."
    BOOK = "book."
    RECENT_TRADES = "recent_trades."
    ORDERS = "account.orders"
    TRADE_HISTORY = "account.trade_history"
    INSTRUMENTS = "instruments"
    PORTFOLIO = "account.portfolio"
    ACCOUNT_SUMMARY = "account.summary"


def _get_channel(full_channel: str) -> Optional[Channel]:
    for c in Channel.__members__.values():
        if full_channel.startswith(c.value):
            return c
    return None


def _is_better_price(is_bid, lhs, rhs):
    if is_bid:
        return lhs > rhs + 1e-6
    else:
        return lhs < rhs - 1e-6


def _merge_changes(is_bid, orders, changes):
    for price, amount, _ in changes:
        i = 0
        while i < len(orders) and _is_better_price(is_bid, orders[i][0], price):
            i += 1
        if amount == 0.0:
            if i < len(orders):
                del orders[i]
        else:
            if i < len(orders) and not _is_better_price(is_bid, price, orders[i][0]):
                orders[i] = (price, amount)
            else:
                orders.insert(i, (price, amount))


def _process_book(book: List, is_snapshot: bool, notification: Dict):
    book[5] = notification["time"]
    bids, asks, trades, _, _, _ = book
    if is_snapshot:
        del bids[:]
        del asks[:]
    trades.extend(notification.get("trades", []))
    _merge_changes(True, bids, notification.get("bid_changes", []))
    _merge_changes(False, asks, notification.get("ask_changes", []))


class Processor:
    def __init__(self):
        self.result_callback: callable = None
        self.error_callback: callable = None
        self.callbacks: Dict[str, callable] = {}
        self.books: Dict[str, List] = {}

    def add_callback(self, channel: Channel, callback: callable):
        self.callbacks[channel.value] = callback

    def add_result_callback(self, callback: callable):
        self.result_callback = callback

    def add_error_callback(self, callback: callable):
        self.error_callback = callback

    async def process_msg(self, msg: str):
        msg = json.loads(msg)
        if "channel_name" in msg:
            await self.process_sub(msg)
        elif "result" in msg:
            if self.result_callback is not None:
                await self.result_callback(msg["result"], msg["id"])
            else:
                logging.info("No callback for result")
        else:
            if self.error_callback is not None:
                await self.error_callback(msg["error"], msg["id"])
            else:
                logging.error(msg)

    async def process_sub(self, msg: Dict):
        try:
            channel = msg["channel_name"]
            is_snapshot = msg.get("snapshot", False)
            notification = msg["notification"]

            if channel.startswith(Channel.BOOK.value):
                try:
                    book = self.books[channel]
                except KeyError:
                    book = [[], [], [], 0, 0, 0]
                    self.books[channel] = book
                _process_book(book, is_snapshot, notification)
                await self._call_callback(channel, book)
            else:
                await self._call_callback(channel, notification)
        except Exception as e:
            logging.exception(f"couldn't process subscription notification: {msg}")
            raise e

    async def _call_callback(self, channel: str, notification):
        ch: Channel = _get_channel(channel)
        cb = self.callbacks.get(ch.value)
        if cb is not None:
            await cb(channel, notification)
        else:
            logging.warning(f"No callback for {ch}")
