"""Background monitors: clock offset, event-loop lag, feed staleness."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence

import httpx

from marketpipe.clock import ClockModel, OffsetSample, cristian_sample, mono_ns, sntp_query, wall_ns
from marketpipe.feed import FeedHandler
from marketpipe.metrics import Metrics

log = logging.getLogger(__name__)


async def binance_time_sample(client: httpx.AsyncClient) -> OffsetSample:
    """Cristian's algorithm on Binance's server time (microsecond resolution)."""
    t0 = wall_ns()
    response = await client.get(
        "https://api.binance.com/api/v3/time", headers={"X-MBX-TIME-UNIT": "MICROSECOND"}
    )
    t3 = wall_ns()
    response.raise_for_status()
    return cristian_sample("binance-rest", t0, t3, int(response.json()["serverTime"]) * 1_000)


async def clock_monitor(
    model: ClockModel,
    servers: Sequence[str],
    metrics: Metrics,
    record: Callable[[OffsetSample], Awaitable[None]],
    client: httpx.AsyncClient | None,
    interval_s: float = 60.0,
) -> None:
    """Samples NTP servers (used for the correction) and the Binance clock (for comparison)."""
    while True:
        for server in servers:
            try:
                sample = await sntp_query(server)
            except (OSError, TimeoutError, ValueError) as exc:
                log.warning("NTP query to %s failed: %s", server, exc)
                continue
            model.add(sample)
            metrics.clock_offset.labels(server).set(sample.offset_ns / 1e9)
            metrics.clock_delay.labels(server).set(sample.delay_ns / 1e9)
            await record(sample)
        if client is not None:
            try:
                sample = await binance_time_sample(client)
                metrics.clock_offset.labels(sample.reference).set(sample.offset_ns / 1e9)
                metrics.clock_delay.labels(sample.reference).set(sample.delay_ns / 1e9)
                await record(sample)
            except (httpx.HTTPError, ValueError, KeyError) as exc:
                log.warning("Binance time query failed: %s", exc)
        best = model.best
        if best is not None:
            metrics.clock_offset.labels("model").set(best.offset_ns / 1e9)
        await asyncio.sleep(interval_s)


async def loop_lag_monitor(metrics: Metrics, period_s: float = 0.1) -> None:
    """Measures how late the event loop wakes up a sleeping task.

    Every receive timestamp is taken when the event loop hands us a message, so a busy
    loop delays it; this lag bounds the measurement error of the latencies we report.
    """
    while True:
        start = mono_ns()
        await asyncio.sleep(period_s)
        lag = (mono_ns() - start) / 1e9 - period_s
        metrics.loop_lag.observe(max(lag, 0.0))


async def staleness_monitor(feeds: Sequence[FeedHandler], metrics: Metrics, period_s: float = 1.0) -> None:
    while True:
        now = mono_ns()
        for feed in feeds:
            metrics.last_message_age.labels(feed.venue).set((now - feed.last_message_mono) / 1e9)
        await asyncio.sleep(period_s)
