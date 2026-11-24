"""Write throughput of the storage layer, on a scratch database.

Compares the pipeline's path (COPY into a temporary table, then one deduplicating
INSERT ... SELECT) with the textbook alternative (executemany of an INSERT ... ON CONFLICT
DO NOTHING), for several batch sizes, on fresh rows and on a full replay of rows already
stored (the cost of idempotency).

    python analysis/bench_storage.py --dsn postgresql://.../marketpipe_test
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import time
from pathlib import Path

from marketpipe.models import BUY, Trade
from marketpipe.storage import Storage, trade_record

OUT = Path(__file__).resolve().parents[1] / "results" / "storage_benchmark.csv"
T0 = 1_795_483_153_000_000_000

EXECUTEMANY = """
INSERT INTO trades (venue, symbol, trade_id, price, qty, side, exchange_ts, event_ts, recv_ts, source)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT DO NOTHING
"""


def tape(n: int, venue: str) -> list[Trade]:
    return [
        Trade(
            venue, "BTC-USDT", i, 60_000.0 + i % 100, 0.01, BUY, T0 + i * 10_000_000, T0 + i * 10_000_000 + 1
        )
        for i in range(1, n + 1)
    ]


async def timed_copy(storage: Storage, trades: list[Trade], batch: int) -> float:
    start = time.perf_counter()
    for k in range(0, len(trades), batch):
        await storage.write_trades(trades[k : k + batch])
    return time.perf_counter() - start


async def timed_executemany(storage: Storage, trades: list[Trade], batch: int) -> float:
    start = time.perf_counter()
    async with storage.pool.acquire() as conn:
        for k in range(0, len(trades), batch):
            async with conn.transaction():
                await conn.executemany(EXECUTEMANY, [trade_record(t) for t in trades[k : k + batch]])
    return time.perf_counter() - start


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--out", type=Path, default=OUT)
    args = parser.parse_args()

    storage = Storage(args.dsn)
    await storage.connect()
    results = []
    try:
        for batch in (100, 1_000, 10_000):
            for method, fn in (("copy_dedup", timed_copy), ("executemany", timed_executemany)):
                venue = f"bench-{method}-{batch}"
                await storage.pool.execute("DELETE FROM trades WHERE venue LIKE 'bench-%'")
                trades = tape(args.rows, venue)
                fresh = await fn(storage, trades, batch)
                replay = await fn(storage, trades, batch)  # every row is a duplicate
                stored = await storage.pool.fetchval("SELECT count(*) FROM trades WHERE venue = $1", venue)
                assert stored == args.rows
                results.append(
                    {
                        "method": method,
                        "batch": batch,
                        "rows": args.rows,
                        "fresh_rows_per_s": round(args.rows / fresh),
                        "replay_rows_per_s": round(args.rows / replay),
                    }
                )
                print(results[-1])
        await storage.pool.execute("DELETE FROM trades WHERE venue LIKE 'bench-%'")
    finally:
        await storage.close()
    with args.out.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(results[0]))
        writer.writeheader()
        writer.writerows(results)


if __name__ == "__main__":
    asyncio.run(main())
