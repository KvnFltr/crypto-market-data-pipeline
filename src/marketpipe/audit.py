"""Completeness audit: compares the stored trades with the venue's own REST history.

Sequence checks only see what the pipeline knows about. The audit is an independent check:
for every stored trade id range it downloads the venue's record of the same trades and
reports trades that are missing, unexpected, or stored with different values.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

import httpx

from marketpipe.storage import Storage, datetime_to_ns
from marketpipe.venues.base import VenueAdapter


@dataclass(slots=True)
class AuditReport:
    venue: str
    symbol: str
    start: str
    end: str
    first_id: int = 0
    last_id: int = 0
    reference: int = 0  # trades in the venue history for the id range
    stored: int = 0
    stored_live: int = 0
    stored_recovered: int = 0
    missing: int = 0
    unexpected: int = 0
    mismatched: int = 0
    missing_ids: list[int] = field(default_factory=list)

    @property
    def completeness(self) -> float:
        return 1.0 - self.missing / self.reference if self.reference else math.nan

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["completeness"] = self.completeness
        d["missing_ids"] = self.missing_ids[:50]
        return d


async def audit_trades(
    storage: Storage,
    adapter: VenueAdapter,
    client: httpx.AsyncClient,
    symbol: str,
    start: datetime,
    end: datetime,
) -> AuditReport:
    report = AuditReport(adapter.name, symbol, start.isoformat(), end.isoformat())
    rows = await storage.pool.fetch(
        """
        SELECT trade_id, price, qty, side, exchange_ts, source
        FROM trades
        WHERE venue = $1 AND symbol = $2 AND exchange_ts >= $3 AND exchange_ts < $4
        ORDER BY trade_id
        """,
        adapter.name,
        symbol,
        start,
        end,
    )
    if not rows:
        return report
    stored = {r["trade_id"]: r for r in rows}
    report.first_id, report.last_id = rows[0]["trade_id"], rows[-1]["trade_id"]
    report.stored = len(stored)
    report.stored_live = sum(1 for r in rows if r["source"] == "ws")
    report.stored_recovered = report.stored - report.stored_live

    hint = datetime_to_ns(rows[0]["exchange_ts"])
    reference = await adapter.fetch_trades(client, symbol, report.first_id, report.last_id, hint)
    ref = {t.trade_id: t for t in reference}
    report.reference = len(ref)
    missing = sorted(set(ref) - set(stored))
    report.missing = len(missing)
    report.missing_ids = missing
    report.unexpected = len(set(stored) - set(ref))
    for tid, t in ref.items():
        r = stored.get(tid)
        if r is None:
            continue
        same_time = abs(datetime_to_ns(r["exchange_ts"]) - t.exchange_ts_ns) <= 1_000  # 1 us rounding
        if r["price"] != t.price or r["qty"] != t.qty or r["side"] != t.side or not same_time:
            report.mismatched += 1
    return report
