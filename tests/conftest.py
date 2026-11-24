import os
from collections.abc import AsyncIterator

import pytest

from marketpipe.storage import Storage

TEST_DSN = os.environ.get("MARKETPIPE_TEST_DSN")

TABLES = ("trades", "quotes", "gaps", "connection_events", "clock_samples", "runs")


@pytest.fixture
async def storage() -> AsyncIterator[Storage]:
    """A migrated, empty database. Integration tests are skipped without MARKETPIPE_TEST_DSN."""
    if not TEST_DSN:
        pytest.skip("set MARKETPIPE_TEST_DSN to run the database integration tests")
    s = Storage(TEST_DSN)
    await s.connect()
    await s.pool.execute(f"TRUNCATE {', '.join(TABLES)} RESTART IDENTITY CASCADE")
    try:
        yield s
    finally:
        await s.close()
