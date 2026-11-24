"""Gap recovery: fetch missing trades from the venue's REST history and store them.

One worker per venue processes gaps in detection order, which respects the venue's rate
limits. Each gap is persisted first (status 'open') and updated once resolved, so that the
`gaps` table is a complete audit trail of every message lost and of what became of it.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

import httpx

from marketpipe.metrics import Metrics
from marketpipe.models import Gap
from marketpipe.storage import Storage
from marketpipe.venues.base import VenueAdapter

log = logging.getLogger(__name__)


@dataclass(slots=True)
class BackfillJob:
    gap: Gap
    hint_ts_ns: int


class Backfiller:
    def __init__(
        self,
        adapter: VenueAdapter,
        storage: Storage,
        metrics: Metrics,
        client: httpx.AsyncClient,
        max_trades: int = 500_000,
        delay_s: float = 1.0,
        attempts: int = 6,
    ) -> None:
        self.adapter = adapter
        self.storage = storage
        self.metrics = metrics
        self.client = client
        self.max_trades = max_trades
        self.delay_s = delay_s  # let the REST history catch up with the stream
        self.attempts = attempts
        self.queue: asyncio.Queue[BackfillJob] = asyncio.Queue()
        self.recovered_total = 0
        self.unrecovered_total = 0

    def submit(self, gap: Gap, hint_ts_ns: int) -> None:
        self.queue.put_nowait(BackfillJob(gap, hint_ts_ns))

    async def run(self) -> None:
        while True:
            job = await self.queue.get()
            try:
                await self.repair(job)
            except Exception:  # never let one bad gap stop the worker
                log.exception("%s: gap repair failed", self.adapter.name)
            finally:
                self.queue.task_done()

    async def repair(self, job: BackfillJob) -> Gap:
        gap = job.gap
        venue = self.adapter.name
        if gap.gap_id is None:
            await self.storage.insert_gap(gap)
        if gap.size > self.max_trades:
            log.error("%s %s: gap of %d trades exceeds the backfill limit", venue, gap.symbol, gap.size)
            return await self._resolve(gap, 0)

        await asyncio.sleep(self.delay_s)
        trades = None
        for attempt in range(self.attempts):
            try:
                trades = await self.adapter.fetch_trades(
                    self.client, gap.symbol, gap.first_missing, gap.last_missing, job.hint_ts_ns
                )
                break
            except (httpx.HTTPError, RuntimeError, ValueError) as exc:
                wait = min(60.0, 2.0**attempt)
                log.warning(
                    "%s %s: backfill attempt %d failed (%s), retry in %.0fs",
                    venue,
                    gap.symbol,
                    attempt + 1,
                    exc,
                    wait,
                )
                await asyncio.sleep(wait)
        if trades is None:
            return await self._resolve(gap, 0)

        await self.storage.write_trades(trades)
        recovered = len({t.trade_id for t in trades})
        log.info("%s %s: recovered %d/%d missing trades", venue, gap.symbol, recovered, gap.size)
        return await self._resolve(gap, recovered)

    async def _resolve(self, gap: Gap, recovered: int) -> Gap:
        gap.recovered = recovered
        if recovered == gap.size:
            gap.status = "recovered"
        elif recovered > 0:
            gap.status = "partial"
        else:
            gap.status = "unrecoverable"
        venue = self.adapter.name
        self.metrics.recovered.labels(venue).inc(recovered)
        self.metrics.unrecovered.labels(venue).inc(gap.size - recovered)
        self.recovered_total += recovered
        self.unrecovered_total += gap.size - recovered
        await self.storage.resolve_gap(gap)
        return gap
