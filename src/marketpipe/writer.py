"""Batching writer: drains the event queue into TimescaleDB.

A batch is flushed when it reaches `batch_size` rows or when `flush_interval_s` has elapsed
since its first row, which bounds the added latency while keeping inserts efficient. If the
database is unavailable the batch is retried with backoff and kept in memory; meanwhile the
bounded queue fills up and applies backpressure to the feeds.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import TypeVar

import asyncpg

from marketpipe.clock import mono_ns
from marketpipe.metrics import Metrics
from marketpipe.models import Quote, Trade
from marketpipe.storage import Storage

log = logging.getLogger(__name__)

Row = TypeVar("Row", Trade, Quote)


class Writer:
    def __init__(
        self,
        queue: asyncio.Queue[Trade | Quote],
        storage: Storage,
        metrics: Metrics,
        batch_size: int = 2_000,
        flush_interval_s: float = 0.2,
    ) -> None:
        self.queue = queue
        self.storage = storage
        self.metrics = metrics
        self.batch_size = batch_size
        self.flush_interval_s = flush_interval_s
        self.trades_written = 0
        self.quotes_written = 0

    async def run(self) -> None:
        try:
            while True:
                batch = await self._collect()
                await self._flush(batch)
        except asyncio.CancelledError:
            await self.drain()
            raise

    async def drain(self) -> None:
        """Writes whatever is still queued (used at shutdown)."""
        batch: list[Trade | Quote] = []
        while not self.queue.empty():
            batch.append(self.queue.get_nowait())
            if len(batch) >= self.batch_size:
                await self._flush(batch)
                batch = []
        if batch:
            await self._flush(batch)

    async def _collect(self) -> list[Trade | Quote]:
        batch: list[Trade | Quote] = [await self.queue.get()]
        deadline = asyncio.get_running_loop().time() + self.flush_interval_s
        while len(batch) < self.batch_size:
            try:
                batch.append(self.queue.get_nowait())
                continue
            except asyncio.QueueEmpty:
                pass
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                break
            try:
                batch.append(await asyncio.wait_for(self.queue.get(), remaining))
            except TimeoutError:
                break
        self.metrics.queue_depth.set(self.queue.qsize())
        return batch

    async def _flush(self, batch: list[Trade | Quote]) -> None:
        trades = [e for e in batch if isinstance(e, Trade)]
        quotes = [e for e in batch if isinstance(e, Quote)]
        if trades:
            inserted = await self._with_retry("trades", self.storage.write_trades, trades)
            self.trades_written += inserted
            self.metrics.rows_written.labels("trades").inc(inserted)
            self.metrics.rows_deduplicated.labels("trades").inc(len(trades) - inserted)
        if quotes:
            inserted = await self._with_retry("quotes", self.storage.write_quotes, quotes)
            self.quotes_written += inserted
            self.metrics.rows_written.labels("quotes").inc(inserted)
        now = mono_ns()
        for kind, rows in (("trade", trades), ("quote", quotes)):
            hist = self.metrics.ingest_latency.labels(kind)
            for row in rows:
                if row.recv_mono_ns:
                    hist.observe((now - row.recv_mono_ns) / 1e9)

    async def _with_retry(
        self, table: str, write: Callable[[Sequence[Row]], Awaitable[int]], rows: Sequence[Row]
    ) -> int:
        delay = 0.5
        while True:
            start = mono_ns()
            try:
                inserted: int = await write(rows)
            except (asyncpg.PostgresError, OSError, asyncpg.InterfaceError) as exc:
                self.metrics.db_errors.labels(table).inc()
                log.error("writing %d %s failed (%s); retrying in %.1fs", len(rows), table, exc, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)
                continue
            self.metrics.db_batch_size.labels(table).observe(len(rows))
            self.metrics.db_write_seconds.labels(table).observe((mono_ns() - start) / 1e9)
            return inserted
