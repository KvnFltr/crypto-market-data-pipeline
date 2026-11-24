"""Coinbase Exchange: `matches` and `heartbeat` channels.

* `trade_id` is contiguous per product.
* `side` is the side of the *maker* order, so the aggressor is the opposite side.
* The heartbeat channel publishes, every second and per product, the id of the last trade.
  Comparing it with the last id received reveals missing trades even on a quiet market.
* Gap recovery: GET /products/{id}/trades?after=N returns trades with id < N, newest first.
"""

from __future__ import annotations

import httpx
import orjson

from marketpipe.models import BUY, SELL, Event, Heartbeat, Source, Trade
from marketpipe.venues.base import VenueAdapter, iso_to_ns


class CoinbaseAdapter(VenueAdapter):
    name = "coinbase"
    rest_base = "https://api.exchange.coinbase.com"
    min_rest_interval_s = 0.2
    stale_after_s = 15.0  # heartbeats arrive every second

    def ws_url(self) -> str:
        return "wss://ws-feed.exchange.coinbase.com"

    def subscribe_messages(self) -> list[str]:
        sub = {"type": "subscribe", "product_ids": self.symbols, "channels": ["matches", "heartbeat"]}
        return [orjson.dumps(sub).decode()]

    def parse(self, raw: str | bytes, recv_ts_ns: int, recv_mono_ns: int) -> list[Event]:
        msg = orjson.loads(raw)
        kind = msg.get("type")
        if kind in ("match", "last_match"):
            return [
                Trade(
                    venue=self.name,
                    symbol=msg["product_id"],
                    trade_id=int(msg["trade_id"]),
                    price=float(msg["price"]),
                    qty=float(msg["size"]),
                    side=BUY if msg["side"] == "sell" else SELL,  # maker side -> aggressor side
                    exchange_ts_ns=iso_to_ns(msg["time"]),
                    recv_ts_ns=recv_ts_ns,
                    recv_mono_ns=recv_mono_ns,
                )
            ]
        if kind == "heartbeat":
            last = msg.get("last_trade_id")
            return [Heartbeat(self.name, msg.get("product_id"), int(last) if last else None, recv_ts_ns)]
        if kind == "error":
            raise RuntimeError(f"coinbase error: {msg.get('message')} {msg.get('reason')}")
        return []

    async def fetch_trades(
        self, client: httpx.AsyncClient, symbol: str, first_id: int, last_id: int, hint_ts_ns: int
    ) -> list[Trade]:
        trades: list[Trade] = []
        cursor = last_id + 1  # 'after' returns ids strictly below the cursor
        while cursor > first_id:
            response = await self._get(
                client, f"/products/{symbol}/trades", params={"after": cursor, "limit": 1000}
            )
            rows = response.json()
            if not rows:
                break
            for row in rows:
                tid = int(row["trade_id"])
                if first_id <= tid <= last_id:
                    trades.append(
                        Trade(
                            venue=self.name,
                            symbol=symbol,
                            trade_id=tid,
                            price=float(row["price"]),
                            qty=float(row["size"]),
                            side=BUY if row["side"] == "sell" else SELL,
                            exchange_ts_ns=iso_to_ns(row["time"]),
                            recv_ts_ns=0,
                            source=Source.REST,
                        )
                    )
            cursor = min(int(row["trade_id"]) for row in rows)
        trades.sort(key=lambda t: t.trade_id)
        return trades
