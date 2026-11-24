"""Local order book maintained from a REST snapshot and incremental depth updates.

Synchronisation follows the procedure documented by Binance:

1. buffer the depth updates received on the stream;
2. fetch a snapshot, with id L = lastUpdateId;
3. drop buffered updates with u <= L; the first remaining one must satisfy U <= L + 1 <= u
   (otherwise the snapshot is older than the stream and must be fetched again);
4. apply the updates in order; each must chain, U == previous u + 1. Any break is a gap:
   the missing updates cannot be replayed, so the book is rebuilt from a new snapshot.

A quantity of zero removes a price level. The best bid and offer are published as a Quote
whenever they change.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from enum import Enum

from marketpipe.clock import wall_ns
from marketpipe.models import DepthUpdate, Gap, Quote
from marketpipe.venues.base import BookSnapshot

log = logging.getLogger(__name__)

MAX_BUFFER = 20_000  # updates kept while waiting for a snapshot
SNAPSHOT_WAIT_S = 5.0  # how long a snapshot may wait for the stream to reach it


class OrderBook:
    __slots__ = ("asks", "bids", "last_update_id")

    def __init__(self) -> None:
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.last_update_id = 0

    def load(self, snapshot: BookSnapshot) -> None:
        self.bids = {p: q for p, q in snapshot.bids if q > 0}
        self.asks = {p: q for p, q in snapshot.asks if q > 0}
        self.last_update_id = snapshot.last_update_id

    def apply(self, update: DepthUpdate) -> None:
        for side, levels in ((self.bids, update.bids), (self.asks, update.asks)):
            for price, qty in levels:
                if qty == 0.0:
                    side.pop(price, None)
                else:
                    side[price] = qty
        self.last_update_id = update.last_id

    def best_bid(self) -> tuple[float, float] | None:
        if not self.bids:
            return None
        price = max(self.bids)
        return price, self.bids[price]

    def best_ask(self) -> tuple[float, float] | None:
        if not self.asks:
            return None
        price = min(self.asks)
        return price, self.asks[price]

    def is_crossed(self) -> bool:
        bid, ask = self.best_bid(), self.best_ask()
        return bid is not None and ask is not None and bid[0] >= ask[0]


class SyncState(Enum):
    WAITING_SNAPSHOT = "waiting_snapshot"
    LIVE = "live"


class BookSynchronizer:
    """Keeps one instrument's book in sync and reports quotes, gaps and resyncs."""

    def __init__(
        self,
        venue: str,
        symbol: str,
        fetch_snapshot: Callable[[], Awaitable[BookSnapshot]],
        on_quote: Callable[[Quote], None],
        on_gap: Callable[[Gap], None],
    ) -> None:
        self.venue = venue
        self.symbol = symbol
        self._fetch_snapshot = fetch_snapshot
        self._on_quote = on_quote
        self._on_gap = on_gap
        self.book = OrderBook()
        self.state = SyncState.WAITING_SNAPSHOT
        self._buffer: list[DepthUpdate] = []
        self._task: asyncio.Task[None] | None = None
        self._last_bbo: tuple[float, float, float, float] | None = None
        self.resyncs = 0
        self.snapshots = 0
        self.crossed = 0
        self.after_reconnect = False

    def on_reconnect(self) -> None:
        """The next break in the update chain is due to the disconnection."""
        self.after_reconnect = True

    def on_update(self, update: DepthUpdate) -> None:
        if self.state is SyncState.LIVE:
            expected = self.book.last_update_id + 1
            if update.last_id < expected:
                return  # already applied
            if update.first_id > expected:
                self._on_gap(
                    Gap(
                        self.venue,
                        self.symbol,
                        "book",
                        expected,
                        update.first_id - 1,
                        wall_ns(),
                        self.after_reconnect,
                        status="resynced",
                    )
                )
                self._start_resync()
            else:
                self.after_reconnect = False
                self._apply(update)
                return
        self._buffer.append(update)
        if len(self._buffer) > MAX_BUFFER:
            self._buffer = self._buffer[-MAX_BUFFER // 2 :]
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._synchronise(), name=f"book-sync-{self.symbol}")

    def _start_resync(self) -> None:
        self.state = SyncState.WAITING_SNAPSHOT
        self.resyncs += 1
        self._buffer = []

    def _apply(self, update: DepthUpdate) -> None:
        self.book.apply(update)
        if self.book.is_crossed():
            self.crossed += 1
        bid, ask = self.book.best_bid(), self.book.best_ask()
        if bid is None or ask is None:
            return
        bbo = (bid[0], bid[1], ask[0], ask[1])
        if bbo != self._last_bbo:
            self._last_bbo = bbo
            self._on_quote(
                Quote(
                    self.venue,
                    self.symbol,
                    update.last_id,
                    bid[0],
                    bid[1],
                    ask[0],
                    ask[1],
                    update.exchange_ts_ns,
                    update.recv_ts_ns,
                    update.recv_mono_ns,
                )
            )

    async def _synchronise(self) -> None:
        delay = 0.5
        while self.state is not SyncState.LIVE:
            try:
                snapshot = await self._fetch_snapshot()
            except Exception as exc:  # network errors, rate limits: retry with backoff
                log.warning("%s %s: snapshot failed (%s), retrying", self.venue, self.symbol, exc)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)
                continue
            self.snapshots += 1
            # The REST snapshot is often *ahead* of the stream: wait for the stream to catch up
            # instead of discarding a perfectly good snapshot.
            for _ in range(int(SNAPSHOT_WAIT_S / 0.05)):
                if self._buffer and self._buffer[-1].last_id > snapshot.last_update_id:
                    break
                await asyncio.sleep(0.05)
            if self._try_complete(snapshot):
                return
            await asyncio.sleep(delay)  # snapshot older than the buffered stream: fetch a newer one

    def _try_complete(self, snapshot: BookSnapshot) -> bool:
        last = snapshot.last_update_id
        pending = [u for u in self._buffer if u.last_id > last]
        if not pending or pending[0].first_id > last + 1:
            return False
        self.book.load(snapshot)
        self.state = SyncState.LIVE
        self._buffer = []
        self._last_bbo = None
        for i, update in enumerate(pending):  # re-enter on_update so that chaining is still checked
            self.on_update(update)
            if self.state is not SyncState.LIVE:  # a break inside the buffer: start over
                self._buffer.extend(pending[i + 1 :])
                return False
        log.info("%s %s: order book live at update %d", self.venue, self.symbol, self.book.last_update_id)
        return True
