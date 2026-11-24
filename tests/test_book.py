import asyncio

from marketpipe.book import BookSynchronizer, OrderBook, SyncState
from marketpipe.models import DepthUpdate, Gap, Quote
from marketpipe.venues.base import BookSnapshot

SNAPSHOT = BookSnapshot("BTC-USDT", 100, bids=[(99.0, 1.0), (98.0, 2.0)], asks=[(101.0, 1.0), (102.0, 3.0)])


def upd(first: int, last: int, bids=(), asks=()) -> DepthUpdate:  # type: ignore[no-untyped-def]
    return DepthUpdate("binance", "BTC-USDT", first, last, list(bids), list(asks), first * 1_000, 0)


def test_order_book_applies_and_removes_levels() -> None:
    book = OrderBook()
    book.load(SNAPSHOT)
    assert book.best_bid() == (99.0, 1.0) and book.best_ask() == (101.0, 1.0)
    book.apply(upd(101, 101, bids=[(100.0, 0.5), (99.0, 0.0)], asks=[(101.0, 0.0)]))
    assert book.best_bid() == (100.0, 0.5)  # new level
    assert 99.0 not in book.bids  # removed by a zero quantity
    assert book.best_ask() == (102.0, 3.0)
    assert book.last_update_id == 101 and not book.is_crossed()


class Harness:
    def __init__(self, snapshots: list[BookSnapshot]) -> None:
        self.snapshots = list(snapshots)
        self.quotes: list[Quote] = []
        self.gaps: list[Gap] = []
        self.sync = BookSynchronizer("binance", "BTC-USDT", self.fetch, self.quotes.append, self.gaps.append)

    async def fetch(self) -> BookSnapshot:
        await asyncio.sleep(0)
        return self.snapshots.pop(0)


async def settle(h: Harness) -> None:
    for _ in range(100):
        await asyncio.sleep(0.01)
        if h.sync.state is SyncState.LIVE:
            return


async def test_sync_follows_the_documented_procedure() -> None:
    h = Harness([SNAPSHOT])
    h.sync.on_update(upd(95, 99))  # older than the snapshot: discarded
    h.sync.on_update(upd(100, 102, bids=[(99.5, 1.0)]))  # straddles lastUpdateId + 1 = 101
    h.sync.on_update(upd(103, 104, asks=[(100.5, 2.0)]))
    await settle(h)
    assert h.sync.state is SyncState.LIVE
    assert h.sync.book.last_update_id == 104
    assert h.sync.book.best_bid() == (99.5, 1.0) and h.sync.book.best_ask() == (100.5, 2.0)
    assert [q.update_id for q in h.quotes] == [102, 104]  # one quote per BBO change
    assert h.gaps == []


async def test_snapshot_ahead_of_stream_waits_for_the_stream() -> None:
    h = Harness([SNAPSHOT])
    h.sync.on_update(upd(90, 95))  # stream still behind the snapshot (id 100)
    task = asyncio.create_task(settle(h))
    await asyncio.sleep(0.1)
    assert h.sync.state is SyncState.WAITING_SNAPSHOT
    h.sync.on_update(upd(96, 101))  # the stream catches up
    await task
    assert h.sync.state is SyncState.LIVE and h.sync.snapshots == 1


async def test_broken_chain_triggers_resync_and_records_the_gap() -> None:
    newer = BookSnapshot("BTC-USDT", 200, bids=[(97.0, 1.0)], asks=[(103.0, 1.0)])
    h = Harness([SNAPSHOT, newer])
    h.sync.on_update(upd(101, 101))
    await settle(h)
    h.sync.on_update(upd(102, 110))
    h.sync.on_update(upd(150, 160))  # ids 111..149 never arrived
    assert h.sync.state is SyncState.WAITING_SNAPSHOT
    assert len(h.gaps) == 1
    g = h.gaps[0]
    assert (g.stream, g.first_missing, g.last_missing, g.status) == ("book", 111, 149, "resynced")
    h.sync.on_update(upd(161, 205))
    await settle(h)
    assert h.sync.state is SyncState.LIVE and h.sync.book.last_update_id == 205
    assert h.sync.resyncs == 1 and h.sync.crossed == 0


async def test_gap_after_reconnect_is_attributed_to_the_reconnection() -> None:
    h = Harness([SNAPSHOT, BookSnapshot("BTC-USDT", 300, [(1.0, 1.0)], [(2.0, 1.0)])])
    h.sync.on_update(upd(101, 101))
    await settle(h)
    h.sync.on_reconnect()
    h.sync.on_update(upd(250, 260))
    assert h.gaps[0].after_reconnect is True
    if h.sync._task is not None:  # stop the resync that is now waiting for the stream
        h.sync._task.cancel()
