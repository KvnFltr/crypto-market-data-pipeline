"""Interface implemented by every venue adapter."""

from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import httpx

from marketpipe.models import Event, Trade

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MICROSECOND = timedelta(microseconds=1)


def iso_to_ns(text: str) -> int:
    """RFC 3339 timestamp with up to microsecond precision -> integer ns since epoch (exact)."""
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return ((dt - _EPOCH) // _MICROSECOND) * 1_000


@dataclass(slots=True, frozen=True)
class BookSnapshot:
    symbol: str
    last_update_id: int
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]


class RateLimiter:
    """Minimum spacing between REST requests to one venue (public endpoints are rate limited)."""

    def __init__(self, min_interval_s: float) -> None:
        self._interval = min_interval_s
        self._next = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self._lock:
            now = time.monotonic()
            if now < self._next:
                await asyncio.sleep(self._next - now)
            self._next = max(now, self._next) + self._interval


class VenueAdapter(ABC):
    """Translates one venue's websocket and REST APIs into normalised events."""

    name: str = ""
    rest_base: str = ""
    min_rest_interval_s: float = 0.2
    stale_after_s: float = 30.0  # reconnect if nothing at all arrives for this long
    max_session_s: float | None = None  # proactive reconnection before a venue-imposed limit

    def __init__(self, symbols: list[str], order_book: bool = False) -> None:
        if not symbols:
            raise ValueError(f"{self.name}: at least one symbol is required")
        self.symbols = list(symbols)
        self.order_book = order_book
        self.rest_limiter = RateLimiter(self.min_rest_interval_s)

    @abstractmethod
    def ws_url(self) -> str: ...

    def subscribe_messages(self) -> list[str]:
        return []

    @abstractmethod
    def parse(self, raw: str | bytes, recv_ts_ns: int, recv_mono_ns: int) -> list[Event]:
        """Decode one websocket message. Unknown or control messages yield no event."""

    @abstractmethod
    async def fetch_trades(
        self, client: httpx.AsyncClient, symbol: str, first_id: int, last_id: int, hint_ts_ns: int
    ) -> list[Trade]:
        """Trades with ids in [first_id, last_id] from the REST history, for gap recovery.

        `hint_ts_ns` is the exchange time of the last trade received before the gap; venues
        whose history is indexed by time (Kraken) start searching from there.
        """

    async def fetch_book_snapshot(self, client: httpx.AsyncClient, symbol: str) -> BookSnapshot:
        raise NotImplementedError(f"{self.name} does not support order book snapshots")

    async def _get(self, client: httpx.AsyncClient, path: str, **kwargs: object) -> httpx.Response:
        await self.rest_limiter.wait()
        response = await client.get(self.rest_base + path, **kwargs)  # type: ignore[arg-type]
        response.raise_for_status()
        return response
