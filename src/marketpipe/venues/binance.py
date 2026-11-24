"""Binance spot: trade stream and incremental order book (diff depth) stream.

* Trades: `<symbol>@trade`, trade id `t` contiguous per symbol, trade time `T` and event
  time `E`. With `timeUnit=MICROSECOND` both are in microseconds.
* Order book: `<symbol>@depth@100ms`, update ids `[U, u]` that chain from one message to
  the next. A missing update cannot be recovered; the book has to be rebuilt from a REST
  snapshot (see book.py).
* Gap recovery: GET /api/v3/historicalTrades?fromId=..., public, 1000 trades per call.
* A connection lives at most 24 hours; we rotate it after 23 hours.
"""

from __future__ import annotations

from typing import Any

import httpx
import orjson

from marketpipe.models import BUY, SELL, DepthUpdate, Event, Source, Trade
from marketpipe.venues.base import BookSnapshot, VenueAdapter


def _levels(raw: list[list[str]]) -> list[tuple[float, float]]:
    return [(float(p), float(q)) for p, q in raw]


class BinanceAdapter(VenueAdapter):
    name = "binance"
    rest_base = "https://api.binance.com"
    ws_base = "wss://stream.binance.com:443"  # also served on 9443, often filtered on corporate networks
    min_rest_interval_s = 0.5  # historicalTrades weighs 25 of the 6000 per minute allowed
    stale_after_s = 30.0
    max_session_s = 23 * 3600.0

    def __init__(self, symbols: list[str], order_book: bool = False) -> None:
        super().__init__(symbols, order_book)
        # "BTC-USDT" <-> "BTCUSDT"
        self._native = {s: s.replace("-", "") for s in self.symbols}
        self._normalised = {v: k for k, v in self._native.items()}

    def native(self, symbol: str) -> str:
        return self._native[symbol]

    def ws_url(self) -> str:
        streams = [f"{self._native[s].lower()}@trade" for s in self.symbols]
        if self.order_book:
            streams += [f"{self._native[s].lower()}@depth@100ms" for s in self.symbols]
        return f"{self.ws_base}/stream?streams={'/'.join(streams)}&timeUnit=MICROSECOND"

    def parse(self, raw: str | bytes, recv_ts_ns: int, recv_mono_ns: int) -> list[Event]:
        msg = orjson.loads(raw)
        data: dict[str, Any] | None = msg.get("data")
        if data is None:
            return []
        kind = data.get("e")
        if kind == "trade":
            return [
                Trade(
                    venue=self.name,
                    symbol=self._normalised[data["s"]],
                    trade_id=int(data["t"]),
                    price=float(data["p"]),
                    qty=float(data["q"]),
                    side=SELL if data["m"] else BUY,  # m: buyer is the maker -> the taker sold
                    exchange_ts_ns=int(data["T"]) * 1_000,
                    recv_ts_ns=recv_ts_ns,
                    recv_mono_ns=recv_mono_ns,
                    event_ts_ns=int(data["E"]) * 1_000,
                )
            ]
        if kind == "depthUpdate":
            return [
                DepthUpdate(
                    venue=self.name,
                    symbol=self._normalised[data["s"]],
                    first_id=int(data["U"]),
                    last_id=int(data["u"]),
                    bids=_levels(data["b"]),
                    asks=_levels(data["a"]),
                    exchange_ts_ns=int(data["E"]) * 1_000,
                    recv_ts_ns=recv_ts_ns,
                    recv_mono_ns=recv_mono_ns,
                )
            ]
        return []

    async def fetch_trades(
        self, client: httpx.AsyncClient, symbol: str, first_id: int, last_id: int, hint_ts_ns: int
    ) -> list[Trade]:
        trades: list[Trade] = []
        next_id = first_id
        while next_id <= last_id:
            limit = min(1000, last_id - next_id + 1)
            response = await self._get(
                client,
                "/api/v3/historicalTrades",
                params={"symbol": self._native[symbol], "fromId": next_id, "limit": limit},
                headers={"X-MBX-TIME-UNIT": "MICROSECOND"},
            )
            rows = response.json()
            if not rows:
                break
            for row in rows:
                tid = int(row["id"])
                if tid > last_id:
                    break
                trades.append(
                    Trade(
                        venue=self.name,
                        symbol=symbol,
                        trade_id=tid,
                        price=float(row["price"]),
                        qty=float(row["qty"]),
                        side=SELL if row["isBuyerMaker"] else BUY,
                        exchange_ts_ns=int(row["time"]) * 1_000,
                        recv_ts_ns=0,
                        source=Source.REST,
                    )
                )
            next_id = int(rows[-1]["id"]) + 1
        return trades

    async def fetch_book_snapshot(self, client: httpx.AsyncClient, symbol: str) -> BookSnapshot:
        response = await self._get(
            client, "/api/v3/depth", params={"symbol": self._native[symbol], "limit": 1000}
        )
        body = response.json()
        return BookSnapshot(symbol, int(body["lastUpdateId"]), _levels(body["bids"]), _levels(body["asks"]))
