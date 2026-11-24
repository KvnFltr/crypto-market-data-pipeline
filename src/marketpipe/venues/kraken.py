"""Kraken spot, websocket API v2: `trade` channel.

* `trade_id` is contiguous per pair; `side` is the taker side.
* A heartbeat message arrives every second on an active subscription (no trade id).
* Gap recovery: GET /0/public/Trades?pair=...&since=<ns> is indexed by time, not by id, so
  the search starts at the exchange time of the last trade received before the gap.
"""

from __future__ import annotations

from typing import Any

import httpx
import orjson

from marketpipe.models import BUY, SELL, Event, Heartbeat, Source, Trade
from marketpipe.venues.base import VenueAdapter, iso_to_ns

_REST_PAIRS = {"BTC": "XBT"}  # Kraken's legacy asset codes used by the REST API


class KrakenAdapter(VenueAdapter):
    name = "kraken"
    rest_base = "https://api.kraken.com"
    min_rest_interval_s = 1.1  # public endpoints: about one call per second
    stale_after_s = 15.0

    def __init__(self, symbols: list[str], order_book: bool = False) -> None:
        super().__init__(symbols, order_book)
        self._ws_symbol = {s: s.replace("-", "/") for s in self.symbols}  # "BTC-USD" -> "BTC/USD"
        self._normalised = {v: k for k, v in self._ws_symbol.items()}

    def ws_url(self) -> str:
        return "wss://ws.kraken.com/v2"

    def subscribe_messages(self) -> list[str]:
        params = {"channel": "trade", "symbol": list(self._ws_symbol.values()), "snapshot": False}
        return [orjson.dumps({"method": "subscribe", "params": params}).decode()]

    def parse(self, raw: str | bytes, recv_ts_ns: int, recv_mono_ns: int) -> list[Event]:
        msg: dict[str, Any] = orjson.loads(raw)
        channel = msg.get("channel")
        if channel == "trade":
            return [
                Trade(
                    venue=self.name,
                    symbol=self._normalised[row["symbol"]],
                    trade_id=int(row["trade_id"]),
                    price=float(row["price"]),
                    qty=float(row["qty"]),
                    side=BUY if row["side"] == "buy" else SELL,
                    exchange_ts_ns=iso_to_ns(row["timestamp"]),
                    recv_ts_ns=recv_ts_ns,
                    recv_mono_ns=recv_mono_ns,
                )
                for row in msg["data"]
            ]
        if channel == "heartbeat":
            return [Heartbeat(self.name, None, None, recv_ts_ns)]
        if msg.get("method") == "subscribe" and not msg.get("success", True):
            raise RuntimeError(f"kraken subscription failed: {msg.get('error')}")
        return []

    def rest_pair(self, symbol: str) -> str:
        base, quote = symbol.split("-")
        return _REST_PAIRS.get(base, base) + quote

    async def fetch_trades(
        self, client: httpx.AsyncClient, symbol: str, first_id: int, last_id: int, hint_ts_ns: int
    ) -> list[Trade]:
        found: dict[int, Trade] = {}
        since = max(hint_ts_ns - 1_000_000_000, 0)  # start one second before the last trade seen
        for _ in range(100):  # bounded number of pages
            response = await self._get(
                client,
                "/0/public/Trades",
                params={"pair": self.rest_pair(symbol), "since": str(since), "count": 1000},
            )
            body = response.json()
            if body.get("error"):
                raise RuntimeError(f"kraken REST error: {body['error']}")
            result = body["result"]
            rows = next(v for k, v in result.items() if k != "last")
            for price, volume, ts, side, _type, _misc, tid in rows:
                tid = int(tid)
                if first_id <= tid <= last_id:
                    found[tid] = Trade(
                        venue=self.name,
                        symbol=symbol,
                        trade_id=tid,
                        price=float(price),
                        qty=float(volume),
                        side=BUY if side == "b" else SELL,
                        exchange_ts_ns=round(float(ts) * 1e6) * 1_000,
                        recv_ts_ns=0,
                        source=Source.REST,
                    )
            if not rows or int(rows[-1][6]) >= last_id or int(result["last"]) <= since:
                break
            since = int(result["last"])
        return [found[k] for k in sorted(found)]
