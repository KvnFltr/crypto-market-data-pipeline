"""Independent latency probe, used as a control for the pipeline's latency measurements.

A minimal process that only receives the Binance trade stream and records, per second, the
distribution of (local receipt time - Binance event time E). It does nothing else: no
parsing beyond the timestamp, no database, no other feed. If it sees the same latency spikes
as the pipeline, they originate upstream (exchange or network), not in the pipeline.

    python tools/latency_probe.py --symbol btcusdt --out results/probe_btcusdt.csv
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import statistics
import time

import orjson
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake


async def probe(symbol: str, out: str, duration: float | None) -> None:
    url = f"wss://stream.binance.com:443/ws/{symbol}@trade?timeUnit=MICROSECOND"
    started = time.monotonic()
    with open(out, "a", newline="") as fh:
        writer = csv.writer(fh)
        if fh.tell() == 0:
            writer.writerow(["second", "count", "min_ms", "p50_ms", "p99_ms", "max_ms"])
        while duration is None or time.monotonic() - started < duration:
            try:
                async with connect(url, max_size=2**22) as ws:
                    second, samples = int(time.time()), []
                    async for raw in ws:
                        now_ns = time.time_ns()
                        event_us = orjson.loads(raw)["E"]
                        samples.append((now_ns - event_us * 1_000) / 1e6)
                        if now_ns // 1_000_000_000 != second:
                            if samples:
                                samples.sort()
                                writer.writerow(
                                    [
                                        second,
                                        len(samples),
                                        f"{samples[0]:.3f}",
                                        f"{statistics.median(samples):.3f}",
                                        f"{samples[min(len(samples) - 1, int(0.99 * len(samples)))]:.3f}",
                                        f"{samples[-1]:.3f}",
                                    ]
                                )
                                fh.flush()
                            second, samples = now_ns // 1_000_000_000, []
                        if duration is not None and time.monotonic() - started >= duration:
                            break
            except (
                OSError,
                TimeoutError,
                ConnectionClosed,
                InvalidHandshake,
            ) as exc:  # reconnect, keep recording
                print(f"probe reconnecting after {exc!r}")
                await asyncio.sleep(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--symbol", default="btcusdt")
    parser.add_argument("--out", default="results/probe_btcusdt.csv")
    parser.add_argument("--duration", type=float, default=None)
    args = parser.parse_args()
    asyncio.run(probe(args.symbol, args.out, args.duration))


if __name__ == "__main__":
    main()
