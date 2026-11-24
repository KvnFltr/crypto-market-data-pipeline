"""FeedHandler against the fake exchange: reconnection, gap detection, staleness, chaos."""

import asyncio
import contextlib
import random

from prometheus_client import CollectorRegistry

from marketpipe.clock import ClockModel
from marketpipe.config import ChaosConfig
from marketpipe.feed import FeedHandler, backoff_delay
from marketpipe.metrics import Metrics
from marketpipe.models import ConnectionEvent, Gap, Quote, Trade
from tests.fake_exchange import FakeAdapter, FakeExchange


def test_backoff_is_bounded_and_jittered() -> None:
    rng = random.Random(1)
    delays = [backoff_delay(a, base=0.5, cap=30.0, rng=rng) for a in range(12)]
    assert all(0.0 <= d <= min(30.0, 0.5 * 2**a) for a, d in enumerate(delays))
    assert len(set(delays)) == len(delays)  # jitter: no two clients retry in lockstep


class Collector:
    def __init__(self) -> None:
        self.gaps: list[tuple[Gap, int]] = []
        self.connections: list[ConnectionEvent] = []

    def on_gap(self, gap: Gap, hint: int) -> None:
        self.gaps.append((gap, hint))

    def on_connection(self, ev: ConnectionEvent) -> None:
        self.connections.append(ev)


async def run_feed(exchange: FakeExchange, seconds: float, chaos: ChaosConfig | None = None):  # type: ignore[no-untyped-def]
    await exchange.start()
    FakeAdapter.exchange = exchange
    adapter = FakeAdapter([exchange.symbol])
    queue: asyncio.Queue[Trade | Quote] = asyncio.Queue()
    sink = Collector()
    feed = FeedHandler(
        adapter,
        queue,
        Metrics(CollectorRegistry()),
        ClockModel(),
        sink.on_gap,
        sink.on_connection,
        chaos=chaos,
        ping_interval_s=0.2,
    )
    task = asyncio.create_task(feed.run())
    await asyncio.sleep(seconds)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    await exchange.stop()
    received = []
    while not queue.empty():
        item = queue.get_nowait()
        assert isinstance(item, Trade)
        received.append(item.trade_id)
    return feed, sink, received


def covered(received: list[int], gaps: list[tuple[Gap, int]]) -> set[int]:
    ids = set(received)
    for gap, _ in gaps:
        ids |= set(range(gap.first_missing, gap.last_missing + 1))
    return ids


async def test_dropped_messages_are_detected_in_session() -> None:
    exchange = FakeExchange(drop_ids={40, 41, 42, 100})
    _feed, sink, received = await run_feed(exchange, 0.6)
    in_session = [g for g, _ in sink.gaps if not g.after_reconnect]
    assert [(g.first_missing, g.last_missing) for g in in_session] == [(40, 42), (100, 100)]
    assert received == sorted(received) and len(set(received)) == len(received)
    first = received[0]
    assert covered(received, sink.gaps) >= set(range(first, received[-1] + 1))  # nothing unaccounted for


async def test_disconnection_gap_is_detected_after_reconnect() -> None:
    exchange = FakeExchange(cut_after_ids={60}, refuse_s=0.25)
    feed, sink, received = await run_feed(exchange, 1.5)
    assert exchange.connections >= 2 and feed.sessions >= 2
    after = [g for g, _ in sink.gaps if g.after_reconnect]
    assert len(after) == 1
    gap, hint = sink.gaps[0]
    assert gap.first_missing == 61 and gap.size >= 50  # 0.25 s refused at one trade per 2 ms
    assert hint == exchange.history[60].exchange_ts_ns  # REST search starts at the last trade seen
    reasons = [e.reason for e in sink.connections if e.event == "disconnected"]
    assert reasons and reasons[0] in ("closed", "connect_error")
    first = received[0]
    assert covered(received, sink.gaps) >= set(range(first, received[-1] + 1))


async def test_silent_connection_is_detected_as_stale() -> None:
    exchange = FakeExchange(interval_s=10.0)  # no message at all within the test
    FakeAdapter.stale_after_s = 0.3
    try:
        _feed, sink, _ = await run_feed(exchange, 1.2)
    finally:
        FakeAdapter.stale_after_s = 2.0
    assert "stale" in [e.reason for e in sink.connections]


async def test_chaos_mode_forces_disconnections() -> None:
    chaos = ChaosConfig(enabled=True, mean_interval_s=0.15, min_downtime_s=0.05, max_downtime_s=0.1)
    exchange = FakeExchange()
    _feed, sink, received = await run_feed(exchange, 1.5, chaos)
    assert "chaos" in [e.reason for e in sink.connections]
    assert any(g.after_reconnect for g, _ in sink.gaps)
    first = received[0]
    assert covered(received, sink.gaps) >= set(range(first, received[-1] + 1))
