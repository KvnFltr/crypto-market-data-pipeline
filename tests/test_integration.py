"""Integration tests against a real TimescaleDB (set MARKETPIPE_TEST_DSN).

The end-to-end tests run the complete pipeline against the fake exchange with messages
dropped, connections cut, random forced disconnections and a restart, then check that the
database holds every trade of the tape exactly once.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from prometheus_client import CollectorRegistry

from marketpipe.app import Pipeline
from marketpipe.audit import audit_trades
from marketpipe.backfill import Backfiller, BackfillJob
from marketpipe.config import ChaosConfig, Config, FeedConfig
from marketpipe.metrics import Metrics
from marketpipe.models import BUY, Gap, Quote, Source, Trade
from marketpipe.storage import Storage
from marketpipe.venues import ADAPTERS
from tests.conftest import TEST_DSN
from tests.fake_exchange import FakeAdapter, FakeExchange

pytestmark = pytest.mark.integration

T0 = 1_795_483_153_000_000_000


def trade(i: int, ts_offset_ns: int = 0, source: Source = Source.WEBSOCKET, price: float = 100.0) -> Trade:
    return Trade(
        "binance",
        "BTC-USDT",
        i,
        price,
        0.1,
        BUY,
        T0 + i * 1_000_000 + ts_offset_ns,
        T0 + i * 1_000_000 + 50_000_000,
        7,
        source,
    )


async def ids(storage: Storage, venue: str = "binance") -> list[int]:
    rows = await storage.pool.fetch("SELECT trade_id FROM trades WHERE venue = $1 ORDER BY trade_id", venue)
    return [r["trade_id"] for r in rows]


# ---------------------------------------------------------------------- storage
async def test_writes_are_idempotent(storage: Storage) -> None:
    batch = [trade(i) for i in range(1, 1_001)]
    assert await storage.write_trades(batch) == 1_000
    assert await storage.write_trades(batch) == 0  # replay: nothing new
    assert await storage.write_trades([trade(i) for i in range(990, 1_011)]) == 10  # overlap
    assert await ids(storage) == list(range(1, 1_011))


async def test_same_trade_from_two_sources_is_stored_once(storage: Storage) -> None:
    # the REST copy carries a timestamp rounded differently by a few microseconds
    await storage.write_trades([trade(5)])
    assert await storage.write_trades([trade(5, ts_offset_ns=3_000, source=Source.REST)]) == 0
    # duplicates inside one batch: the live copy wins
    assert await storage.write_trades([trade(6, source=Source.REST), trade(6)]) == 1
    row = await storage.pool.fetchrow("SELECT source, recv_ts FROM trades WHERE trade_id = 6")
    assert row["source"] == "ws" and row["recv_ts"] is not None


async def test_quotes_and_high_water_marks(storage: Storage) -> None:
    now_ns = int(datetime.now(UTC).timestamp() * 1e9)
    await storage.write_trades(
        [
            Trade("binance", "BTC-USDT", i, 1.0, 1.0, BUY, now_ns - (100 - i) * 1_000_000, now_ns)
            for i in range(1, 101)
        ]
    )
    await storage.write_quotes([Quote("binance", "BTC-USDT", 9, 99.0, 1.0, 101.0, 2.0, now_ns, now_ns)])
    marks = await storage.high_water_marks("binance", ["BTC-USDT", "ETH-USDT"], timedelta(hours=1))
    assert marks == {"BTC-USDT": (100, now_ns - now_ns % 1_000)}
    assert await storage.pool.fetchval("SELECT count(*) FROM quotes") == 1


async def test_gap_lifecycle(storage: Storage) -> None:
    await storage.start_run("test", {"k": 1})
    gap = Gap("binance", "BTC-USDT", "trades", 10, 19, T0, after_reconnect=True)
    await storage.insert_gap(gap)
    gap.status, gap.recovered = "recovered", 10
    await storage.resolve_gap(gap)
    row = await storage.pool.fetchrow("SELECT size, status, recovered, resolved_at, run_id FROM gaps")
    assert (row["size"], row["status"], row["recovered"], row["run_id"]) == (
        10,
        "recovered",
        10,
        storage.run_id,
    )
    assert row["resolved_at"] is not None


# ---------------------------------------------------------------------- backfill
@pytest.mark.parametrize(
    ("history_ids", "expected_status", "expected_recovered"),
    [(range(1, 101), "recovered", 50), (range(1, 81), "partial", 30), (range(1, 21), "unrecoverable", 0)],
)
async def test_backfiller_outcomes(
    storage: Storage, history_ids: range, expected_status: str, expected_recovered: int
) -> None:
    exchange = FakeExchange()
    exchange.history = {i: trade(i, source=Source.REST) for i in history_ids}
    FakeAdapter.exchange = exchange
    adapter = FakeAdapter(["BTC-USDT"])
    async with httpx.AsyncClient() as client:
        bf = Backfiller(adapter, storage, Metrics(CollectorRegistry()), client, delay_s=0)
        gap = Gap("fake", "BTC-USDT", "trades", 51, 100, T0, after_reconnect=False)
        await bf.repair(BackfillJob(gap, 0))
    assert (gap.status, gap.recovered) == (expected_status, expected_recovered)
    status = await storage.pool.fetchval("SELECT status FROM gaps WHERE gap_id = $1", gap.gap_id)
    assert status == expected_status


async def test_oversized_gap_is_not_fetched(storage: Storage) -> None:
    async with httpx.AsyncClient() as client:
        FakeAdapter.exchange = FakeExchange()
        bf = Backfiller(FakeAdapter(["X"]), storage, Metrics(CollectorRegistry()), client, max_trades=100)
        gap = await bf.repair(BackfillJob(Gap("fake", "X", "trades", 1, 1_000, T0, False), 0))
    assert gap.status == "unrecoverable" and bf.unrecovered_total == 1_000


# ---------------------------------------------------------------------- end to end
def fake_config(chaos: ChaosConfig | None = None) -> Config:
    assert TEST_DSN is not None
    return Config(
        dsn=TEST_DSN,
        feeds=[FeedConfig("fake", ["FAKE-USD"])],
        batch_size=500,
        flush_interval_s=0.05,
        metrics_port=0,
        ntp_servers=[],
        clock_rest_check=False,
        chaos=chaos or ChaosConfig(),
    )


@pytest.fixture
def fake_venue():  # type: ignore[no-untyped-def]
    ADAPTERS["fake"] = FakeAdapter
    yield
    del ADAPTERS["fake"]


async def run_pipeline(config: Config, exchange: FakeExchange, seconds: float, label: str) -> None:
    FakeAdapter.exchange = exchange
    pipeline = Pipeline(config, label, Metrics(CollectorRegistry()))
    await pipeline.run(duration_s=seconds)


async def test_pipeline_stores_every_trade_despite_faults(storage: Storage, fake_venue: None) -> None:
    exchange = FakeExchange(
        drop_ids={150, 151, 152, 400, 401, 1_200},
        cut_after_ids={700, 1_500},
        refuse_s=0.3,
    )
    await exchange.start()
    try:
        await run_pipeline(fake_config(), exchange, 5.0, "e2e-faults")
    finally:
        await exchange.stop()
    stored = await ids(storage, "fake")
    assert stored == list(range(stored[0], stored[-1] + 1)), (
        "every trade between the first and last is stored"
    )
    assert stored[0] <= 5 and stored[-1] > 1_500
    gaps = await storage.pool.fetch("SELECT * FROM gaps ORDER BY gap_id")
    assert {(g["first_missing"], g["last_missing"]) for g in gaps if not g["after_reconnect"]} >= {
        (150, 152),
        (400, 401),
        (1_200, 1_200),
    }
    assert sum(g["after_reconnect"] for g in gaps) == 2
    assert all(g["status"] == "recovered" for g in gaps)
    recovered = await storage.pool.fetchval("SELECT count(*) FROM trades WHERE source = 'rest'")
    assert recovered == sum(g["size"] for g in gaps)


async def test_pipeline_resumes_after_restart_and_chaos(storage: Storage, fake_venue: None) -> None:
    exchange = FakeExchange()
    await exchange.start()
    try:
        chaos = ChaosConfig(enabled=True, mean_interval_s=0.4, min_downtime_s=0.05, max_downtime_s=0.2)
        await run_pipeline(fake_config(chaos), exchange, 2.5, "e2e-chaos")
        await asyncio.sleep(0.5)  # the tape keeps running while the pipeline is down
        await run_pipeline(fake_config(), exchange, 1.5, "e2e-restart")
    finally:
        await exchange.stop()
    stored = await ids(storage, "fake")
    assert stored == list(range(stored[0], stored[-1] + 1))
    assert len(stored) == len(set(stored))
    runs = await storage.pool.fetch("SELECT label, ended_at FROM runs ORDER BY run_id")
    assert [r["label"] for r in runs] == ["e2e-chaos", "e2e-restart"] and all(r["ended_at"] for r in runs)
    chaos_events = await storage.pool.fetchval(
        "SELECT count(*) FROM connection_events WHERE reason = 'chaos'"
    )
    assert chaos_events >= 1
    restart_gap = await storage.pool.fetchrow(
        "SELECT * FROM gaps g JOIN runs r USING (run_id) "
        "WHERE r.label = 'e2e-restart' ORDER BY gap_id LIMIT 1"
    )
    assert restart_gap is not None and restart_gap["status"] == "recovered" and restart_gap["size"] > 100


async def test_audit_finds_missing_and_altered_trades(storage: Storage) -> None:
    now = datetime.now(UTC)
    now_ns = int(now.timestamp() * 1e9)
    tape = {
        i: Trade(
            "fake", "FAKE-USD", i, 100.0, 1.0, BUY, now_ns - (500 - i) * 1_000_000, 0, source=Source.REST
        )
        for i in range(1, 501)
    }
    await storage.write_trades(list(tape.values()))
    await storage.pool.execute("DELETE FROM trades WHERE trade_id = 250")
    await storage.pool.execute("UPDATE trades SET price = 101.0 WHERE trade_id = 300")
    exchange = FakeExchange()
    exchange.history = tape
    FakeAdapter.exchange = exchange
    async with httpx.AsyncClient() as client:
        report = await audit_trades(
            storage,
            FakeAdapter(["FAKE-USD"]),
            client,
            "FAKE-USD",
            now - timedelta(minutes=5),
            now + timedelta(minutes=1),
        )
    assert (report.reference, report.stored, report.missing, report.mismatched) == (500, 499, 1, 1)
    assert report.missing_ids == [250] and report.completeness == pytest.approx(0.998)
