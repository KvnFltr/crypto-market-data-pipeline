"""Command-line interface.

marketpipe run    --config config/pipeline.toml [--label NAME] [--duration SECONDS] [--chaos]
marketpipe audit  --config config/pipeline.toml --venue binance --symbol BTC-USDT --start ... --end ...
marketpipe repair --config config/pipeline.toml [--max-trades N]   # retry unresolved gaps
marketpipe schema                                  # print the SQL schema
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import sys
from datetime import UTC, datetime

import httpx

from marketpipe.app import USER_AGENT, Pipeline, serve_metrics
from marketpipe.audit import audit_trades
from marketpipe.config import load_config
from marketpipe.storage import Storage, schema_sql
from marketpipe.venues import make_adapter


def _parse_time(text: str) -> datetime:
    dt = datetime.fromisoformat(text)
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _event_loop_policy() -> None:
    if sys.platform != "win32":
        try:
            import uvloop

            uvloop.install()
        except ImportError:
            pass


async def _run(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    if args.chaos:
        config.chaos.enabled = True
    if args.chaos_interval is not None:
        config.chaos.mean_interval_s = args.chaos_interval
    if args.metrics_port is not None:
        config.metrics_port = args.metrics_port
    serve_metrics(config.metrics_port)
    await Pipeline(config, args.label).run(args.duration)


async def _audit(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    adapter = make_adapter(args.venue, [args.symbol])
    storage = Storage(config.dsn)
    await storage.connect(migrate=False)
    try:
        async with httpx.AsyncClient(timeout=30.0, headers={"User-Agent": USER_AGENT}) as client:
            report = await audit_trades(
                storage, adapter, client, args.symbol, _parse_time(args.start), _parse_time(args.end)
            )
    finally:
        await storage.close()
    print(json.dumps(report.to_dict(), indent=2))


async def _repair(args: argparse.Namespace) -> None:
    from prometheus_client import CollectorRegistry

    from marketpipe.backfill import Backfiller, BackfillJob
    from marketpipe.metrics import Metrics

    config = load_config(args.config)
    storage = Storage(config.dsn)
    await storage.connect()
    try:
        gaps = await storage.unresolved_gaps()
        print(f"{len(gaps)} unresolved gap(s)")
        metrics = Metrics(CollectorRegistry())
        async with httpx.AsyncClient(timeout=30.0, headers={"User-Agent": USER_AGENT}) as client:
            for gap in gaps:
                adapter = make_adapter(gap.venue, [gap.symbol])
                backfiller = Backfiller(adapter, storage, metrics, client, args.max_trades, delay_s=0.0)
                hint = await storage.trade_time_before(gap.venue, gap.symbol, gap.first_missing)
                await backfiller.repair(BackfillJob(gap, hint))
                print(
                    f"gap {gap.gap_id} {gap.venue} {gap.symbol} [{gap.first_missing}, {gap.last_missing}]: "
                    f"{gap.status}, {gap.recovered}/{gap.size} recovered"
                )
    finally:
        await storage.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="marketpipe", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="ingest market data")
    run.add_argument("--config", default="config/pipeline.toml")
    run.add_argument("--label", default="live", help="name of the run, stored in the runs table")
    run.add_argument("--duration", type=float, default=None, help="stop after this many seconds")
    run.add_argument("--chaos", action="store_true", help="inject random disconnections")
    run.add_argument("--chaos-interval", type=float, default=None, help="mean seconds between faults")
    run.add_argument("--metrics-port", type=int, default=None)

    audit = sub.add_parser("audit", help="compare stored trades with the venue's REST history")
    audit.add_argument("--config", default="config/pipeline.toml")
    audit.add_argument("--venue", required=True)
    audit.add_argument("--symbol", required=True)
    audit.add_argument("--start", required=True, help="ISO 8601, UTC if no offset")
    audit.add_argument("--end", required=True)

    repair = sub.add_parser("repair", help="retry the recovery of open or partially recovered gaps")
    repair.add_argument("--config", default="config/pipeline.toml")
    repair.add_argument("--max-trades", type=int, default=5_000_000, help="largest gap to attempt")

    sub.add_parser("schema", help="print the database schema")

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)  # one line per REST call is too verbose
    if args.command == "schema":
        print(schema_sql())
        return
    _event_loop_policy()
    with contextlib.suppress(KeyboardInterrupt):
        commands = {"run": _run, "audit": _audit, "repair": _repair}
        asyncio.run(commands[args.command](args))


if __name__ == "__main__":
    main()
