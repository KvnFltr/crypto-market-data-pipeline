"""TimescaleDB storage.

Trades are written in batches: COPY into a per-connection temporary table, then a single
INSERT ... SELECT into the hypertable that skips trades already stored. Writes are therefore
idempotent: a trade received twice (live, then again from the REST history, or after a
restart) is stored once. Combined with gap recovery this gives effectively-once storage on
top of an at-least-once delivery.

A unique constraint on a hypertable must include the partitioning column, so the primary key
is (venue, symbol, trade_id, exchange_ts). Two sources may timestamp the same trade a few
microseconds apart, so deduplication also checks trade ids within a +/- 1 minute window,
which keeps the lookup on the index and on recent chunks only.
"""

from __future__ import annotations

import json
import socket
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime, timedelta
from importlib import resources
from typing import Any

import asyncpg

from marketpipe.clock import OffsetSample
from marketpipe.models import ConnectionEvent, Gap, Quote, Trade

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

TRADE_COLUMNS = (
    "venue",
    "symbol",
    "trade_id",
    "price",
    "qty",
    "side",
    "exchange_ts",
    "event_ts",
    "recv_ts",
    "source",
)
QUOTE_COLUMNS = (
    "venue",
    "symbol",
    "update_id",
    "bid_px",
    "bid_qty",
    "ask_px",
    "ask_qty",
    "exchange_ts",
    "recv_ts",
)

_INSERT_TRADES = """
INSERT INTO trades (venue, symbol, trade_id, price, qty, side, exchange_ts, event_ts, recv_ts, source)
SELECT DISTINCT ON (s.venue, s.symbol, s.trade_id)
       s.venue, s.symbol, s.trade_id, s.price, s.qty, s.side, s.exchange_ts, s.event_ts, s.recv_ts, s.source
FROM trades_stage s
WHERE NOT EXISTS (
    SELECT 1 FROM trades t
    WHERE t.venue = s.venue AND t.symbol = s.symbol AND t.trade_id = s.trade_id
      AND t.exchange_ts BETWEEN s.exchange_ts - INTERVAL '1 minute' AND s.exchange_ts + INTERVAL '1 minute'
)
ORDER BY s.venue, s.symbol, s.trade_id, s.source DESC  -- prefer the live copy ('ws' > 'rest')
ON CONFLICT DO NOTHING
"""


def ns_to_datetime(ns: int | None) -> datetime | None:
    """Exact conversion to microsecond precision (PostgreSQL's resolution)."""
    if not ns:
        return None
    return _EPOCH + timedelta(microseconds=ns // 1_000)


def datetime_to_ns(dt: datetime) -> int:
    return ((dt - _EPOCH) // timedelta(microseconds=1)) * 1_000


def trade_record(t: Trade) -> tuple[Any, ...]:
    return (
        t.venue,
        t.symbol,
        t.trade_id,
        t.price,
        t.qty,
        t.side,
        ns_to_datetime(t.exchange_ts_ns),
        ns_to_datetime(t.event_ts_ns),
        ns_to_datetime(t.recv_ts_ns),
        t.source.value,
    )


def quote_record(q: Quote) -> tuple[Any, ...]:
    return (
        q.venue,
        q.symbol,
        q.update_id,
        q.bid_px,
        q.bid_qty,
        q.ask_px,
        q.ask_qty,
        ns_to_datetime(q.exchange_ts_ns),
        ns_to_datetime(q.recv_ts_ns),
    )


def schema_sql() -> str:
    return resources.files("marketpipe").joinpath("schema.sql").read_text(encoding="utf-8")


async def _init_connection(conn: asyncpg.Connection) -> None:
    await conn.execute(
        "CREATE TEMP TABLE IF NOT EXISTS trades_stage (LIKE trades INCLUDING DEFAULTS) ON COMMIT DELETE ROWS"
    )


class Storage:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self._pool: asyncpg.Pool | None = None
        self.run_id: int | None = None

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("storage is not connected")
        return self._pool

    async def connect(self, migrate: bool = True) -> None:
        if migrate:
            conn = await asyncpg.connect(self.dsn)
            try:
                await conn.execute(schema_sql())
            finally:
                await conn.close()
        self._pool = await asyncpg.create_pool(self.dsn, min_size=1, max_size=4, init=_init_connection)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    # ------------------------------------------------------------------ market data
    async def write_trades(self, trades: Sequence[Trade]) -> int:
        """Stores a batch; returns the number of new rows (duplicates are skipped)."""
        if not trades:
            return 0
        async with self.pool.acquire() as conn, conn.transaction():
            await conn.copy_records_to_table(
                "trades_stage", records=[trade_record(t) for t in trades], columns=TRADE_COLUMNS
            )
            status = await conn.execute(_INSERT_TRADES)
        return int(status.split()[-1])

    async def write_quotes(self, quotes: Sequence[Quote]) -> int:
        if not quotes:
            return 0
        async with self.pool.acquire() as conn:
            await conn.copy_records_to_table(
                "quotes", records=[quote_record(q) for q in quotes], columns=QUOTE_COLUMNS
            )
        return len(quotes)

    async def high_water_marks(
        self, venue: str, symbols: Iterable[str], max_age: timedelta
    ) -> dict[str, tuple[int, int]]:
        """Last stored (trade_id, exchange_ts_ns) per symbol, to resume after a restart."""
        rows = await self.pool.fetch(
            """
            SELECT DISTINCT ON (symbol) symbol, trade_id, exchange_ts
            FROM trades
            WHERE venue = $1 AND symbol = ANY($2::text[]) AND exchange_ts > now() - $3::interval
            ORDER BY symbol, exchange_ts DESC, trade_id DESC
            """,
            venue,
            list(symbols),
            max_age,
        )
        return {r["symbol"]: (r["trade_id"], datetime_to_ns(r["exchange_ts"])) for r in rows}

    async def stored_trade_ids(self, venue: str, symbol: str, first_id: int, last_id: int) -> list[int]:
        rows = await self.pool.fetch(
            "SELECT trade_id FROM trades WHERE venue = $1 AND symbol = $2 AND trade_id BETWEEN $3 AND $4 "
            "ORDER BY trade_id",
            venue,
            symbol,
            first_id,
            last_id,
        )
        return [r["trade_id"] for r in rows]

    async def unresolved_gaps(self) -> list[Gap]:
        """Trade gaps that are still open or were only partly recovered."""
        rows = await self.pool.fetch(
            """
            SELECT gap_id, venue, symbol, stream, first_missing, last_missing, detected_at, after_reconnect,
                   status, recovered, coalesce(cause, '') AS cause
            FROM gaps WHERE stream = 'trades' AND status IN ('open', 'partial', 'unrecoverable')
            ORDER BY gap_id
            """
        )
        return [
            Gap(
                r["venue"],
                r["symbol"],
                r["stream"],
                r["first_missing"],
                r["last_missing"],
                datetime_to_ns(r["detected_at"]),
                r["after_reconnect"],
                r["recovered"],
                r["status"],
                r["gap_id"],
                r["cause"],
            )
            for r in rows
        ]

    async def trade_time_before(self, venue: str, symbol: str, trade_id: int) -> int:
        """Exchange time of the last stored trade preceding `trade_id` (0 if none)."""
        ts = await self.pool.fetchval(
            "SELECT exchange_ts FROM trades WHERE venue = $1 AND symbol = $2 AND trade_id < $3 "
            "ORDER BY trade_id DESC LIMIT 1",
            venue,
            symbol,
            trade_id,
        )
        return datetime_to_ns(ts) if ts is not None else 0

    # ------------------------------------------------------------------ bookkeeping
    async def start_run(self, label: str, config: dict[str, Any]) -> int:
        self.run_id = await self.pool.fetchval(
            "INSERT INTO runs (label, host, config) VALUES ($1, $2, $3::jsonb) RETURNING run_id",
            label,
            socket.gethostname(),
            json.dumps(config),
        )
        return int(self.run_id)

    async def end_run(self) -> None:
        if self.run_id is not None:
            await self.pool.execute("UPDATE runs SET ended_at = now() WHERE run_id = $1", self.run_id)

    async def insert_gap(self, gap: Gap) -> int:
        gap_id = await self.pool.fetchval(
            """
            INSERT INTO gaps (run_id, venue, symbol, stream, first_missing, last_missing, detected_at,
                              after_reconnect, status, recovered, cause)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, nullif($11, '')) RETURNING gap_id
            """,
            self.run_id,
            gap.venue,
            gap.symbol,
            gap.stream,
            gap.first_missing,
            gap.last_missing,
            ns_to_datetime(gap.detected_ts_ns),
            gap.after_reconnect,
            gap.status,
            gap.recovered,
            gap.cause,
        )
        gap.gap_id = int(gap_id)
        return gap.gap_id

    async def resolve_gap(self, gap: Gap) -> None:
        await self.pool.execute(
            "UPDATE gaps SET status = $2, recovered = $3, resolved_at = now() WHERE gap_id = $1",
            gap.gap_id,
            gap.status,
            gap.recovered,
        )

    async def insert_connection_event(self, ev: ConnectionEvent) -> None:
        await self.pool.execute(
            "INSERT INTO connection_events (ts, run_id, venue, event, reason, session_s) "
            "VALUES ($1, $2, $3, $4, $5, $6)",
            ns_to_datetime(ev.ts_ns),
            self.run_id,
            ev.venue,
            ev.event,
            ev.reason,
            ev.session_s,
        )

    async def insert_clock_sample(self, s: OffsetSample) -> None:
        await self.pool.execute(
            "INSERT INTO clock_samples (ts, run_id, reference, offset_s, delay_s) "
            "VALUES ($1, $2, $3, $4, $5)",
            ns_to_datetime(s.ts_ns),
            self.run_id,
            s.reference,
            s.offset_ns / 1e9,
            s.delay_ns / 1e9,
        )
