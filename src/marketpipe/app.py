"""Wires the components together and runs the pipeline."""

from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
from datetime import timedelta

import httpx
from prometheus_client import start_http_server

from marketpipe.backfill import Backfiller
from marketpipe.clock import ClockModel
from marketpipe.config import Config
from marketpipe.feed import FeedHandler
from marketpipe.metrics import Metrics
from marketpipe.models import ConnectionEvent, Gap, Quote, Trade
from marketpipe.monitors import clock_monitor, loop_lag_monitor, staleness_monitor
from marketpipe.storage import Storage
from marketpipe.venues import make_adapter
from marketpipe.writer import Writer

log = logging.getLogger(__name__)

USER_AGENT = "marketpipe/1.0 (market data research pipeline)"


class Pipeline:
    def __init__(self, config: Config, label: str = "live", metrics: Metrics | None = None) -> None:
        self.config = config
        self.label = label
        self.metrics = metrics or Metrics()
        self.storage = Storage(config.dsn)
        self.clock = ClockModel()
        self.queue: asyncio.Queue[Trade | Quote] = asyncio.Queue(maxsize=config.queue_size)
        self.feeds: list[FeedHandler] = []
        self.backfillers: dict[str, Backfiller] = {}
        self._background: set[asyncio.Task[None]] = set()

    def _spawn(self, coro: object, name: str) -> asyncio.Task[None]:
        task: asyncio.Task[None] = asyncio.create_task(coro, name=name)  # type: ignore[arg-type]
        self._background.add(task)
        task.add_done_callback(self._background.discard)
        return task

    async def run(self, duration_s: float | None = None) -> None:
        cfg = self.config
        await self.storage.connect()
        run_id = await self.storage.start_run(self.label, cfg.to_dict())
        log.info("run %d (%s) started", run_id, self.label)

        async with httpx.AsyncClient(timeout=15.0, headers={"User-Agent": USER_AGENT}) as client:
            for fc in cfg.feeds:
                adapter = make_adapter(fc.venue, fc.symbols, fc.order_book)
                backfiller = Backfiller(adapter, self.storage, self.metrics, client, cfg.max_backfill_trades)
                self.backfillers[adapter.name] = backfiller

                def on_gap(gap: Gap, hint_ts_ns: int, bf: Backfiller = backfiller) -> None:
                    if gap.stream == "trades":
                        bf.submit(gap, hint_ts_ns)
                    else:  # order book gaps cannot be backfilled: record the resync
                        self._spawn(self.storage.insert_gap(gap), "record-book-gap")

                def on_connection(ev: ConnectionEvent) -> None:
                    self._spawn(self.storage.insert_connection_event(ev), "record-connection")

                feed = FeedHandler(
                    adapter,
                    self.queue,
                    self.metrics,
                    self.clock,
                    on_gap,
                    on_connection,
                    fetch_snapshot=functools.partial(adapter.fetch_book_snapshot, client),
                    chaos=cfg.chaos,
                    ping_interval_s=cfg.ping_interval_s,
                )
                if cfg.resume:
                    marks = await self.storage.high_water_marks(
                        adapter.name, adapter.symbols, timedelta(seconds=cfg.resume_max_age_s)
                    )
                    feed.seed(marks)
                    if marks:
                        log.info(
                            "%s: resuming after stored trade ids %s",
                            adapter.name,
                            {s: m[0] for s, m in marks.items()},
                        )
                self.feeds.append(feed)

            writer = Writer(self.queue, self.storage, self.metrics, cfg.batch_size, cfg.flush_interval_s)
            tasks = [asyncio.create_task(writer.run(), name="writer")]
            tasks += [
                asyncio.create_task(bf.run(), name=f"backfill-{v}") for v, bf in self.backfillers.items()
            ]
            tasks += [asyncio.create_task(f.run(), name=f"feed-{f.venue}") for f in self.feeds]
            tasks.append(
                asyncio.create_task(
                    clock_monitor(
                        self.clock,
                        cfg.ntp_servers,
                        self.metrics,
                        self.storage.insert_clock_sample,
                        client if cfg.clock_rest_check else None,
                        cfg.clock_interval_s,
                    ),
                    name="clock",
                )
            )
            tasks.append(asyncio.create_task(loop_lag_monitor(self.metrics), name="loop-lag"))
            tasks.append(asyncio.create_task(staleness_monitor(self.feeds, self.metrics), name="staleness"))

            try:
                if duration_s is None:
                    await asyncio.gather(*tasks)
                else:
                    done, _ = await asyncio.wait(
                        tasks, timeout=duration_s, return_when=asyncio.FIRST_EXCEPTION
                    )
                    for t in done:
                        t.result()  # surface a crashed component
            finally:
                await self._shutdown(tasks)

    async def _shutdown(self, tasks: list[asyncio.Task[None]]) -> None:
        log.info("shutting down")
        feed_tasks = [t for t in tasks if t.get_name().startswith("feed-")]
        for t in feed_tasks:
            t.cancel()
        await asyncio.gather(*feed_tasks, return_exceptions=True)
        # let pending gap repairs finish (bounded), then stop the writer, which drains its queue
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(
                asyncio.gather(*(bf.queue.join() for bf in self.backfillers.values())), timeout=120
            )
        for t in tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if self._background:
            await asyncio.gather(*self._background, return_exceptions=True)
        await self.storage.end_run()
        await self.storage.close()
        log.info("stopped")


def serve_metrics(port: int) -> None:
    if port > 0:
        start_http_server(port)
        log.info("metrics on http://localhost:%d/metrics", port)
