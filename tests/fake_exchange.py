"""A fake exchange for end-to-end tests.

It produces a trade tape at a fixed rate whether or not anyone is connected, publishes it on
a local websocket, and keeps the full history for "REST" recovery. Faults can be injected:
messages silently dropped, connections cut, reconnections refused for a while.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import dataclass, field

import httpx
import orjson
from websockets.asyncio.server import Server, ServerConnection, serve

from marketpipe.models import BUY, SELL, Event, Source, Trade
from marketpipe.venues.base import VenueAdapter


@dataclass
class FakeExchange:
    symbol: str = "FAKE-USD"
    interval_s: float = 0.002  # one trade every 2 ms
    drop_ids: set[int] = field(default_factory=set)  # produced but never published
    cut_after_ids: set[int] = field(default_factory=set)  # close the connection after these ids
    refuse_s: float = 0.3  # refuse reconnections for this long after a cut
    history: dict[int, Trade] = field(default_factory=dict)
    port: int = 0
    connections: int = 0
    _clients: set[ServerConnection] = field(default_factory=set)
    _refuse_until: float = 0.0
    _server: Server | None = None
    _first_connection: asyncio.Event = field(default_factory=asyncio.Event)
    _producer: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._server = await serve(self._handler, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        self._producer = asyncio.create_task(self._produce())

    async def stop(self) -> None:
        if self._producer:
            self._producer.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._producer
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    @property
    def last_id(self) -> int:
        return max(self.history, default=0)

    async def _handler(self, ws: ServerConnection) -> None:
        if time.monotonic() < self._refuse_until:
            await ws.close(code=1013, reason="try again later")
            return
        self.connections += 1
        self._first_connection.set()
        self._clients.add(ws)
        try:
            await ws.wait_closed()
        finally:
            self._clients.discard(ws)

    async def _produce(self) -> None:
        # The tape starts with the first connection, so that trade ids are deterministic
        # relative to the test, and follows wall time: on Windows asyncio sleeps last at least
        # one timer tick (~15.6 ms), so trades are produced in bursts to keep the nominal rate.
        await self._first_connection.wait()
        start = time.monotonic()
        trade_id = 0
        while True:
            await asyncio.sleep(self.interval_s)
            due = int((time.monotonic() - start) / self.interval_s)
            for _ in range(max(0, due - trade_id)):
                trade_id += 1
                await self._publish(trade_id)

    async def _publish(self, trade_id: int) -> None:
        now = time.time_ns()
        side = BUY if trade_id % 3 else SELL
        trade = Trade(
            "fake", self.symbol, trade_id, 100.0 + trade_id % 7, 0.5, side, now, 0, source=Source.REST
        )
        self.history[trade_id] = trade
        if trade_id in self.drop_ids:
            return
        message = orjson.dumps(
            {"i": trade_id, "p": trade.price, "q": trade.qty, "s": side, "t": now}
        ).decode()
        for ws in list(self._clients):
            with contextlib.suppress(Exception):
                await ws.send(message)
        if trade_id in self.cut_after_ids:
            self._refuse_until = time.monotonic() + self.refuse_s
            for ws in list(self._clients):
                await ws.close(code=1001, reason="fault injection")


class FakeAdapter(VenueAdapter):
    name = "fake"
    min_rest_interval_s = 0.0
    stale_after_s = 2.0
    exchange: FakeExchange  # set by the test

    def ws_url(self) -> str:
        return f"ws://127.0.0.1:{self.exchange.port}"

    def parse(self, raw: str | bytes, recv_ts_ns: int, recv_mono_ns: int) -> list[Event]:
        m = orjson.loads(raw)
        return [
            Trade("fake", self.symbols[0], m["i"], m["p"], m["q"], m["s"], m["t"], recv_ts_ns, recv_mono_ns)
        ]

    async def fetch_trades(
        self, client: httpx.AsyncClient, symbol: str, first_id: int, last_id: int, hint_ts_ns: int
    ) -> list[Trade]:
        return [self.exchange.history[i] for i in range(first_id, last_id + 1) if i in self.exchange.history]
