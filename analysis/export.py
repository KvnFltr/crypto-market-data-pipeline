"""Exports the experiment from TimescaleDB and Prometheus into compact CSV files.

The raw trades (about a million rows) stay in the database; this script keeps what the
analysis needs: latency quantiles and histograms per analysis window (analysis/windows.toml),
per-minute and per-second series, gaps, connection events, clock samples, completeness audits
and Prometheus summaries. The CSV files are versioned so that the figures and the report can
be rebuilt without the database.

    python analysis/export.py [--dsn ...] [--prometheus http://localhost:9090] [--audit] [--compress]
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import tomllib
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import asyncpg
import httpx

from marketpipe.app import USER_AGENT
from marketpipe.audit import audit_trades
from marketpipe.config import DEFAULT_DSN
from marketpipe.storage import Storage
from marketpipe.venues import make_adapter

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "results"
LATENCY_BINS_MS = [round(10 ** (i / 20), 4) for i in range(0, 81)]  # 1 ms .. 10 s, 20 bins per decade
# Latency on the reference time scale: raw (local clock) plus the clock offset of that minute.
LATENCY_MS = "(extract(epoch FROM t.recv_ts - t.exchange_ts) * 1000 + c.offset_ms)"
CLOCK_JOIN = "JOIN clock_minute c ON c.minute = date_trunc('minute', t.recv_ts)"


def write_csv(name: str, rows: list[dict[str, Any]]) -> None:
    path = OUT / name
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        for row in rows:
            writer.writerow({k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in row.items()})
    print(f"  {name}: {len(rows)} rows")


def resolve_windows(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Windows of analysis/windows.toml as lists of [start, end] segments (exclusions removed)."""
    spec = tomllib.loads((ROOT / "analysis" / "windows.toml").read_text(encoding="utf-8"))
    by_label = {r["label"]: r for r in runs}
    windows = []
    for w in spec["window"]:
        run = by_label.get(w["run"])
        if run is None:
            continue
        start = (
            max(run["started_at"], datetime.fromisoformat(w["start"])) if "start" in w else run["started_at"]
        )
        end = min(run["ended_at"], datetime.fromisoformat(w["end"])) if "end" in w else run["ended_at"]
        segments = [(start, end)]
        for ex_start, ex_end in w.get("exclude", []):
            xs, xe = datetime.fromisoformat(ex_start), datetime.fromisoformat(ex_end)
            cut = []
            for s0, e0 in segments:
                if xe <= s0 or xs >= e0:
                    cut.append((s0, e0))
                    continue
                if s0 < xs:
                    cut.append((s0, xs))
                if xe < e0:
                    cut.append((xe, e0))
            segments = cut
        segments = [(a, b) for a, b in segments if b > a]
        if segments:
            windows.append(
                {
                    "window": w["name"],
                    "run_id": run["run_id"],
                    "start": segments[0][0],
                    "end": segments[-1][1],
                    "segments": segments,
                    "minutes": round(sum((b - a).total_seconds() for a, b in segments) / 60, 1),
                }
            )
    return windows


def histogram_quantile(q: float, counts: dict[float, float]) -> float:
    """Quantile of a cumulative histogram {upper bound: count}, with Prometheus's interpolation."""
    bounds = sorted(counts)
    total = counts[bounds[-1]] if bounds else 0.0
    if total <= 0:
        return math.nan
    rank = q * total
    lower, below = 0.0, 0.0
    for upper in bounds:
        if counts[upper] >= rank:
            if math.isinf(upper):
                return lower  # in the overflow bucket: the largest finite bound
            return lower + (upper - lower) * (rank - below) / max(counts[upper] - below, 1e-12)
        lower, below = upper, counts[upper]
    return lower


def in_window_sql(column: str) -> str:
    """SQL predicate: `column` falls in one of the window's segments ($1 = starts, $2 = ends)."""
    return (
        f"EXISTS (SELECT 1 FROM unnest($1::timestamptz[], $2::timestamptz[]) AS w(s, e) "
        f"WHERE {column} BETWEEN w.s AND w.e)"
    )


def segment_args(w: dict[str, Any]) -> tuple[list[datetime], list[datetime]]:
    return [a for a, _ in w["segments"]], [b for _, b in w["segments"]]


async def build_clock_minutes(conn: asyncpg.Connection, runs: list[dict[str, Any]]) -> None:
    """Offset of the local clock for every minute of the experiment, as a temporary table.

    For each minute, the NTP sample with the smallest round-trip delay within +/- 5 minutes is
    used, as in NTP's clock filter; its error bound is half that delay. A per-minute series
    follows any step of the local clock (e.g. a resynchronisation by the operating system).
    """
    samples = await conn.fetch(
        "SELECT ts, offset_s, delay_s FROM clock_samples WHERE reference <> 'binance-rest' ORDER BY ts"
    )
    start = runs[0]["started_at"].replace(second=0, microsecond=0)
    end = runs[-1]["ended_at"]
    rows = []
    minute = start
    while minute <= end:
        near = [x for x in samples if abs((x["ts"] - minute).total_seconds()) <= 300]
        if near:
            best = min(near, key=lambda x: x["delay_s"])
            rows.append((minute, best["offset_s"] * 1e3, best["delay_s"] * 1e3 / 2))
        minute += timedelta(minutes=1)
    await conn.execute(
        "CREATE TEMP TABLE clock_minute (minute timestamptz PRIMARY KEY, offset_ms float8, unc_ms float8)"
    )
    await conn.copy_records_to_table("clock_minute", records=rows)
    write_csv("clock_minutes.csv", [{"minute": m, "offset_ms": o, "unc_ms": u} for m, o, u in rows])


async def export_db(
    conn: asyncpg.Connection, include_open: bool
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    runs = [
        dict(r)
        for r in await conn.fetch(
            "SELECT run_id, label, started_at, coalesce(ended_at, now()) AS ended_at FROM runs "
            "WHERE (ended_at IS NOT NULL OR $1) AND label IN ('baseline', 'chaos') ORDER BY run_id",
            include_open,
        )
    ]
    write_csv("runs.csv", runs)
    windows = resolve_windows(runs)
    write_csv(
        "windows.csv",
        [
            {
                **{k: v for k, v in w.items() if k != "segments"},
                "segments": ";".join(f"{a.isoformat()}/{b.isoformat()}" for a, b in w["segments"]),
            }
            for w in windows
        ],
    )

    write_csv(
        "clock_samples.csv",
        [
            dict(r)
            for r in await conn.fetch(
                "SELECT ts, run_id, reference, offset_s, delay_s FROM clock_samples ORDER BY ts"
            )
        ],
    )
    write_csv(
        "connection_events.csv",
        [
            dict(r)
            for r in await conn.fetch(
                "SELECT ts, run_id, venue, event, reason, session_s FROM connection_events ORDER BY ts"
            )
        ],
    )
    write_csv(
        "gaps.csv",
        [
            dict(r)
            for r in await conn.fetch(
                "SELECT gap_id, run_id, venue, symbol, stream, first_missing, last_missing, size, detected_at, "
                "after_reconnect, status, recovered, resolved_at, cause FROM gaps ORDER BY gap_id"
            )
        ],
    )

    await build_clock_minutes(conn, runs)

    # Clock-corrected latency (venue time -> local receipt), per venue and minute.
    write_csv(
        "latency_minutes.csv",
        [
            dict(r)
            for r in await conn.fetch(
                f"""
        SELECT t.venue, time_bucket('1 minute', t.recv_ts) AS minute, count(*) AS n,
               percentile_cont(ARRAY[0.01, 0.5, 0.9, 0.99, 0.999]) WITHIN GROUP (ORDER BY {LATENCY_MS}) AS q,
               max({LATENCY_MS}) AS max_ms
        FROM trades t {CLOCK_JOIN} WHERE t.source = 'ws' AND t.recv_ts >= $1
        GROUP BY 1, 2 ORDER BY 1, 2
        """,
                runs[0]["started_at"],
            )
        ],
    )

    summary, histogram = [], []
    for w in windows:
        rows = await conn.fetch(
            f"""
            SELECT t.venue, count(*) AS n,
                   percentile_cont(ARRAY[0.001, 0.01, 0.1, 0.5, 0.9, 0.99, 0.999]) WITHIN GROUP
                       (ORDER BY {LATENCY_MS}) AS q,
                   min({LATENCY_MS}) AS min_ms, max({LATENCY_MS}) AS max_ms
            FROM trades t {CLOCK_JOIN} WHERE t.source = 'ws' AND {in_window_sql("t.recv_ts")}
            GROUP BY 1 ORDER BY 1
            """,
            *segment_args(w),
        )
        summary += [{"window": w["window"], **dict(r)} for r in rows]
        hist = await conn.fetch(
            f"""
            SELECT t.venue, width_bucket({LATENCY_MS}, $3::float8[]) AS bin, count(*) AS n
            FROM trades t {CLOCK_JOIN} WHERE t.source = 'ws' AND {in_window_sql("t.recv_ts")}
            GROUP BY 1, 2 ORDER BY 1, 2
            """,
            *segment_args(w),
            LATENCY_BINS_MS,
        )
        histogram += [
            {
                "window": w["window"],
                "venue": h["venue"],
                "bin": h["bin"],
                "lower_ms": LATENCY_BINS_MS[h["bin"] - 1] if h["bin"] >= 1 else 0.0,
                "upper_ms": LATENCY_BINS_MS[h["bin"]] if h["bin"] < len(LATENCY_BINS_MS) else math.inf,
                "n": h["n"],
            }
            for h in hist
        ]
    write_csv("latency_summary.csv", summary)
    write_csv("latency_histogram.csv", histogram)

    # Binance BTC-USDT per second: matching -> publication (E - T) and publication -> receipt (recv - E),
    # to be compared with the independent probe.
    write_csv(
        "binance_seconds.csv",
        [
            dict(r)
            for r in await conn.fetch(
                """
        SELECT date_trunc('second', recv_ts) AS second, count(*) AS n,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM recv_ts - event_ts) * 1000)
                   AS recv_minus_e_p50_ms,
               max(extract(epoch FROM recv_ts - event_ts) * 1000) AS recv_minus_e_max_ms,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM event_ts - exchange_ts) * 1000)
                   AS e_minus_t_p50_ms,
               max(extract(epoch FROM event_ts - exchange_ts) * 1000) AS e_minus_t_max_ms,
               max(c.offset_ms) AS clock_offset_ms  -- raw values above; add this to correct them
        FROM trades t JOIN clock_minute c ON c.minute = date_trunc('minute', t.recv_ts)
        WHERE venue = 'binance' AND symbol = 'BTC-USDT' AND source = 'ws' AND recv_ts >= $1
        GROUP BY 1 ORDER BY 1
        """,
                runs[0]["started_at"],
            )
        ],
    )

    write_csv(
        "throughput_minutes.csv",
        [
            dict(r)
            for r in await conn.fetch(
                "SELECT venue, symbol, bucket AS minute, trades, volume, close FROM ohlcv_1m "
                "WHERE bucket >= $1 ORDER BY 3, 1, 2",
                runs[0]["started_at"] - timedelta(minutes=1),
            )
        ],
    )
    write_csv(
        "message_minutes.csv",
        [
            dict(r)
            for r in await conn.fetch(
                """
        SELECT venue, time_bucket('1 minute', recv_ts) AS minute, count(*) AS live_trades
        FROM trades WHERE source = 'ws' AND recv_ts >= $1 GROUP BY 1, 2 ORDER BY 2, 1
        """,
                runs[0]["started_at"],
            )
        ],
    )
    write_csv(
        "trade_counts.csv",
        [
            dict(r)
            for r in await conn.fetch(
                """
        SELECT r.label AS run, t.venue, t.symbol, count(*) AS trades,
               count(*) FILTER (WHERE t.source = 'ws') AS live, count(*) FILTER (WHERE t.source = 'rest') AS recovered
        FROM trades t JOIN runs r ON t.exchange_ts BETWEEN r.started_at AND r.ended_at
        WHERE r.label IN ('baseline', 'chaos')
        GROUP BY 1, 2, 3 ORDER BY 1, 2, 3
        """
            )
        ],
    )
    write_csv(
        "quote_counts.csv",
        [
            dict(r)
            for r in await conn.fetch(
                "SELECT venue, symbol, count(*) AS quotes, count(*) FILTER (WHERE bid_px >= ask_px) AS crossed "
                "FROM quotes GROUP BY 1, 2 ORDER BY 1, 2"
            )
        ],
    )
    return runs, windows


async def export_storage(conn: asyncpg.Connection) -> None:
    """Table sizes before and after columnar compression of the experiment's chunks."""
    sizes: list[dict[str, Any]] = []
    for table in ("trades", "quotes"):
        before = await conn.fetchval("SELECT hypertable_size($1)", table)
        rows = await conn.fetchval(f"SELECT count(*) FROM {table}")
        await conn.execute(
            f"SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('{table}') c"
        )
        after = await conn.fetchval("SELECT hypertable_size($1)", table)
        sizes.append({"table": table, "rows": rows, "bytes_uncompressed": before, "bytes_compressed": after})
    write_csv("storage.csv", sizes)


async def export_prometheus(base: str, windows: list[dict[str, Any]]) -> None:
    """Quantiles of the histograms only Prometheus has, over each analysis window."""
    queries = {
        "ingest_trade": ("marketpipe_ingest_latency_seconds_bucket", 'kind="trade"', ""),
        "ingest_quote": ("marketpipe_ingest_latency_seconds_bucket", 'kind="quote"', ""),
        "db_write_trades": ("marketpipe_db_write_seconds_bucket", 'table="trades"', ""),
        "batch_rows_trades": ("marketpipe_db_batch_rows_bucket", 'table="trades"', ""),
        "event_loop_lag": ("marketpipe_event_loop_lag_seconds_bucket", "", ""),
        "parse": ("marketpipe_parse_seconds_bucket", "", "venue"),
        "ws_rtt": ("marketpipe_ws_ping_rtt_seconds_bucket", "", "venue"),
    }
    rows: list[dict[str, Any]] = []
    async with httpx.AsyncClient(base_url=base, timeout=30) as client:
        for w in windows:
            for name, (metric, selector, by) in queries.items():
                # bucket increases summed over the window's segments, then interpolated like Prometheus
                buckets: dict[str, dict[float, float]] = defaultdict(lambda: defaultdict(float))
                for seg_start, seg_end in w["segments"]:
                    span = f"{int((seg_end - seg_start).total_seconds())}s"
                    group = f"le{',' + by if by else ''}"
                    expr = f"sum by ({group}) (increase({metric}{{{selector}}}[{span}]))"
                    response = await client.get(
                        "/api/v1/query", params={"query": expr, "time": seg_end.timestamp()}
                    )
                    for result in response.json()["data"]["result"]:
                        label = result["metric"].get(by, "")
                        buckets[label][float(result["metric"]["le"])] += float(result["value"][1])
                for label, counts in buckets.items():
                    for q in (0.5, 0.9, 0.99, 0.999):
                        rows.append(
                            {
                                "window": w["window"],
                                "metric": name,
                                "label": label,
                                "quantile": q,
                                "value": histogram_quantile(q, counts),
                            }
                        )
    write_csv("prometheus_quantiles.csv", rows)


async def run_audits(dsn: str, runs: list[dict[str, Any]]) -> None:
    """Completeness audit of every run against each venue's REST history.

    The window excludes the first and last minute of each run, whose boundary trades may
    legitimately belong to the neighbouring period.
    """
    pairs = [
        ("binance", "BTC-USDT"),
        ("binance", "ETH-USDT"),
        ("coinbase", "BTC-USD"),
        ("coinbase", "ETH-USD"),
        ("kraken", "BTC-USD"),
        ("kraken", "ETH-USD"),
    ]
    storage = Storage(dsn)
    await storage.connect(migrate=False)
    reports = []
    try:
        async with httpx.AsyncClient(timeout=30, headers={"User-Agent": USER_AGENT}) as client:
            for run in runs:
                start = run["started_at"] + timedelta(minutes=1)
                end = run["ended_at"] - timedelta(minutes=1)
                for venue, symbol in pairs:
                    report = await audit_trades(
                        storage, make_adapter(venue, [symbol]), client, symbol, start, end
                    )
                    d = report.to_dict()
                    d["run"] = run["label"]
                    reports.append(d)
                    print(
                        f"  audit {run['label']} {venue} {symbol}: {report.reference} reference, "
                        f"{report.missing} missing, {report.unexpected} unexpected, {report.mismatched} mismatched"
                    )
    finally:
        await storage.close()
    (OUT / "audit.json").write_text(json.dumps(reports, indent=2), encoding="utf-8")


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--dsn", default=DEFAULT_DSN)
    parser.add_argument("--prometheus", default="http://localhost:9090")
    parser.add_argument("--audit", action="store_true", help="also run the REST completeness audits (slow)")
    parser.add_argument("--compress", action="store_true", help="compress the chunks and record table sizes")
    parser.add_argument("--include-open", action="store_true", help="include runs still in progress")
    args = parser.parse_args()
    OUT.mkdir(exist_ok=True)

    conn = await asyncpg.connect(args.dsn)
    try:
        runs, windows = await export_db(conn, args.include_open)
        if args.compress:
            await export_storage(conn)
    finally:
        await conn.close()
    await export_prometheus(args.prometheus, windows)
    if args.audit:
        await run_audits(args.dsn, runs)


if __name__ == "__main__":
    asyncio.run(main())
