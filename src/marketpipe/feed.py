"""One websocket feed: connection management, sequencing and routing of events.

Failure handling:

* connection errors and closures -> reconnection with exponential backoff and full jitter;
* a silent connection (no message, not even a heartbeat, for `stale_after_s`) -> reconnection,
  since a half-open TCP connection can otherwise stay "up" forever;
* venue-imposed session limits -> proactive rotation;
* every trade id is checked; any gap is recorded and handed to the backfill worker, so that
  trades lost during a disconnection are recovered from the REST history.

Nothing is dropped on the floor: when the writer falls behind, `queue.put` blocks the
reader, the venue eventually disconnects us, and the resulting gap is repaired like any other.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import random
import zlib
from collections.abc import Awaitable, Callable

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidURI

from marketpipe.book import BookSynchronizer
from marketpipe.clock import ClockModel, mono_ns, wall_ns
from marketpipe.config import ChaosConfig
from marketpipe.metrics import Metrics
from marketpipe.models import ConnectionEvent, DepthUpdate, Event, Gap, Heartbeat, Quote, Trade
from marketpipe.sequencing import SeqStatus, SequenceTracker
from marketpipe.venues.base import BookSnapshot, VenueAdapter

log = logging.getLogger(__name__)

HEARTBEAT_GRACE_NS = 2_000_000_000  # a heartbeat may overtake the trades it announces


class Reconnect(Exception):
    """Raised inside a session to close it and reconnect."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def backoff_delay(
    attempt: int, base: float = 0.5, cap: float = 30.0, rng: random.Random | None = None
) -> float:
    """Exponential backoff with full jitter: uniform in [0, min(cap, base * 2^attempt)]."""
    upper = min(cap, base * (2.0**attempt))
    return (rng or random).uniform(0.0, upper)


GapSink = Callable[[Gap, int], None]  # (gap, exchange time of the last trade before it)


class FeedHandler:
    def __init__(
        self,
        adapter: VenueAdapter,
        out: asyncio.Queue[Trade | Quote],
        metrics: Metrics,
        clock: ClockModel,
        on_gap: GapSink,
        on_connection: Callable[[ConnectionEvent], None],
        fetch_snapshot: Callable[[str], Awaitable[BookSnapshot]] | None = None,
        chaos: ChaosConfig | None = None,
        ping_interval_s: float = 10.0,
    ) -> None:
        self.adapter = adapter
        self.venue = adapter.name
        self.out = out
        self.metrics = metrics
        self.clock = clock
        self.on_gap = on_gap
        self.on_connection = on_connection
        self.chaos = chaos or ChaosConfig()
        self.ping_interval_s = ping_interval_s
        self._rng = random.Random(self.chaos.seed * 1_000_003 + zlib.crc32(self.venue.encode()))
        self.trackers = {s: SequenceTracker() for s in adapter.symbols}
        self._last_trade_ts = dict.fromkeys(adapter.symbols, 0)
        self._fresh = dict.fromkeys(adapter.symbols, True)  # first trade since (re)connection
        self._cause = dict.fromkeys(adapter.symbols, "reconnect")
        self._pending_hb: dict[str, tuple[int, int]] = {}  # symbol -> (announced id, deadline)
        self._derived: list[Trade | Quote] = []
        self.books: dict[str, BookSynchronizer] = {}
        if adapter.order_book:
            if fetch_snapshot is None:
                raise ValueError("order book requested without a snapshot source")
            for s in adapter.symbols:
                self.books[s] = BookSynchronizer(
                    self.venue, s, functools.partial(fetch_snapshot, s), self._derived.append, self._book_gap
                )
        self.last_message_mono = mono_ns()
        self.sessions = 0

    def seed(self, high_water: dict[str, tuple[int, int]]) -> None:
        """Resume from the last stored trade ids, so that a restart is treated like a gap."""
        for symbol, (trade_id, ts_ns) in high_water.items():
            if symbol in self.trackers:
                self.trackers[symbol].last = trade_id
                self._last_trade_ts[symbol] = ts_ns
                self._cause[symbol] = "restart"

    # ------------------------------------------------------------------ connection loop
    async def run(self) -> None:
        attempt = 0
        while True:
            reason = "error"
            started = mono_ns()
            downtime = 0.0
            try:
                async with connect(
                    self.adapter.ws_url(),
                    open_timeout=10,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=2,
                    max_size=2**23,
                    max_queue=4096,
                ) as ws:
                    self.sessions += 1
                    started = mono_ns()
                    self.metrics.connected.labels(self.venue).set(1)
                    self.on_connection(ConnectionEvent(self.venue, "connected", "", wall_ns()))
                    for message in self.adapter.subscribe_messages():
                        await ws.send(message)
                    for book in self.books.values():
                        book.on_reconnect()
                    for s in self._fresh:
                        self._fresh[s] = True
                    attempt = 0
                    log.info("%s: connected (session %d)", self.venue, self.sessions)
                    pinger = asyncio.create_task(self._ping_loop(ws))
                    try:
                        await self._session(ws)
                    finally:
                        pinger.cancel()
            except Reconnect as exc:
                reason = exc.reason
                if reason == "chaos":
                    downtime = self._rng.uniform(self.chaos.min_downtime_s, self.chaos.max_downtime_s)
            except ConnectionClosed as exc:
                reason = "closed"
                log.warning("%s: connection closed (%s)", self.venue, exc)
            except (OSError, TimeoutError, InvalidHandshake, InvalidURI) as exc:
                reason = "connect_error"
                log.warning("%s: connection failed (%s: %s)", self.venue, type(exc).__name__, exc)
            except Exception:  # a malformed message or a venue error must not kill the feed
                reason = "error"
                log.exception("%s: unexpected error, reconnecting", self.venue)
            self.metrics.connected.labels(self.venue).set(0)
            self.metrics.reconnects.labels(self.venue, reason).inc()
            session_s = (mono_ns() - started) / 1e9
            self.on_connection(ConnectionEvent(self.venue, "disconnected", reason, wall_ns(), session_s))
            for s in self._cause:
                if not (self._cause[s] == "restart" and self._fresh[s]):  # no trade received since start
                    self._cause[s] = "reconnect"
            if reason in ("chaos", "rotation", "stale"):
                delay = downtime
            else:
                delay = backoff_delay(attempt, rng=self._rng)
                attempt += 1
            log.info("%s: reconnecting in %.2fs (%s)", self.venue, delay, reason)
            await asyncio.sleep(delay)

    async def _session(self, ws: ClientConnection) -> None:
        stale_after = self.adapter.stale_after_s
        connected_at = mono_ns()
        max_session_ns = int(self.adapter.max_session_s * 1e9) if self.adapter.max_session_s else None
        chaos_at = (
            connected_at + int(self._rng.expovariate(1.0 / self.chaos.mean_interval_s) * 1e9)
            if self.chaos.enabled
            else None
        )
        messages = self.metrics.messages.labels(self.venue)
        nbytes = self.metrics.bytes.labels(self.venue)
        while True:
            try:
                async with asyncio.timeout(stale_after):
                    raw = await ws.recv()
            except TimeoutError:
                raise Reconnect("stale") from None
            recv_ts, recv_mono = wall_ns(), mono_ns()
            self.last_message_mono = recv_mono
            messages.inc()
            nbytes.inc(len(raw))
            events = self.adapter.parse(raw, recv_ts, recv_mono)
            self.metrics.parse_seconds.labels(self.venue).observe((mono_ns() - recv_mono) / 1e9)
            await self.handle(events)
            if chaos_at is not None and recv_mono >= chaos_at:
                raise Reconnect("chaos")
            if max_session_ns is not None and recv_mono - connected_at >= max_session_ns:
                raise Reconnect("rotation")

    async def _ping_loop(self, ws: ClientConnection) -> None:
        rtt = self.metrics.ws_rtt.labels(self.venue)
        while True:
            await asyncio.sleep(self.ping_interval_s)
            try:
                waiter = await ws.ping()
                latency = await asyncio.wait_for(waiter, timeout=10)
                rtt.observe(float(latency))
            except (ConnectionClosed, TimeoutError):
                return

    # ------------------------------------------------------------------ event routing
    async def handle(self, events: list[Event]) -> None:
        for ev in events:
            if isinstance(ev, Trade):
                if self._accept_trade(ev):
                    await self.out.put(ev)
            elif isinstance(ev, DepthUpdate):
                book = self.books.get(ev.symbol)
                if book is not None:
                    book.on_update(ev)
            elif isinstance(ev, Heartbeat):
                self._on_heartbeat(ev)
        if self._derived:
            # the order books append to this list through a bound method: clear it in place
            derived = self._derived.copy()
            self._derived.clear()
            for item in derived:
                if isinstance(item, Quote):
                    self.metrics.events.labels(self.venue, "quote").inc()
                await self.out.put(item)
        self._check_pending_heartbeats()

    def _accept_trade(self, t: Trade) -> bool:
        tracker = self.trackers.get(t.symbol)
        if tracker is None:
            return False
        result = tracker.observe(t.trade_id)
        if result.status is SeqStatus.DUPLICATE:
            self.metrics.duplicates.labels(self.venue).inc()
            return False
        if result.status is SeqStatus.GAP:
            fresh = self._fresh[t.symbol]
            cause = self._cause[t.symbol] if fresh else "in_session"
            self._report_trade_gap(t.symbol, result.first_missing, result.last_missing, fresh, cause)
        self._fresh[t.symbol] = False
        self._last_trade_ts[t.symbol] = t.exchange_ts_ns
        pending = self._pending_hb.get(t.symbol)
        if pending is not None and tracker.last is not None and tracker.last >= pending[0]:
            del self._pending_hb[t.symbol]
        self.metrics.events.labels(self.venue, "trade").inc()
        self.metrics.exchange_latency.labels(self.venue).observe(
            self.clock.corrected_latency_ns(t.recv_ts_ns, t.exchange_ts_ns) / 1e9
        )
        return True

    def _on_heartbeat(self, hb: Heartbeat) -> None:
        if hb.symbol is None or hb.last_trade_id is None or hb.symbol not in self.trackers:
            return
        tracker = self.trackers[hb.symbol]
        if tracker.last is not None and hb.last_trade_id > tracker.last and hb.symbol not in self._pending_hb:
            self._pending_hb[hb.symbol] = (hb.last_trade_id, mono_ns() + HEARTBEAT_GRACE_NS)

    def _check_pending_heartbeats(self) -> None:
        if not self._pending_hb:
            return
        now = mono_ns()
        for symbol, (announced, deadline) in list(self._pending_hb.items()):
            tracker = self.trackers[symbol]
            if tracker.last is not None and tracker.last >= announced:
                del self._pending_hb[symbol]
            elif now >= deadline:
                del self._pending_hb[symbol]
                result = tracker.observe_heartbeat(announced)
                if result.status is SeqStatus.GAP:
                    self._report_trade_gap(
                        symbol, result.first_missing, result.last_missing, False, "heartbeat"
                    )

    def _report_trade_gap(
        self, symbol: str, first: int, last: int, after_reconnect: bool, cause: str
    ) -> None:
        size = last - first + 1
        self.metrics.gaps.labels(self.venue, "trades", cause).inc()
        self.metrics.missing.labels(self.venue, "trades", cause).inc(size)
        log.warning("%s %s: %d trades missing [%d, %d] (%s)", self.venue, symbol, size, first, last, cause)
        gap = Gap(self.venue, symbol, "trades", first, last, wall_ns(), after_reconnect, cause=cause)
        self.on_gap(gap, self._last_trade_ts[symbol])

    def _book_gap(self, gap: Gap) -> None:
        cause = "reconnect" if gap.after_reconnect else "in_session"
        gap.cause = cause
        self.metrics.gaps.labels(self.venue, "book", cause).inc()
        self.metrics.missing.labels(self.venue, "book", cause).inc(gap.size)
        self.metrics.book_resyncs.labels(self.venue, gap.symbol).inc()
        self.on_gap(gap, 0)
