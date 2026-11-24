"""Prometheus metrics exported by the pipeline (scraped by Prometheus, shown in Grafana)."""

from __future__ import annotations

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge, Histogram

# Latency buckets from 100 us to 10 s, roughly logarithmic.
_LATENCY_BUCKETS = (
    0.0001,
    0.00025,
    0.0005,
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.075,
    0.1,
    0.15,
    0.2,
    0.3,
    0.5,
    0.75,
    1.0,
    2.5,
    5.0,
    10.0,
)


class Metrics:
    """All metrics, attached to one registry (a fresh one per test, the default one in production)."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        kw = registry if registry is not None else REGISTRY
        self.messages = Counter(
            "marketpipe_messages_total", "Websocket messages received", ["venue"], registry=kw
        )
        self.bytes = Counter(
            "marketpipe_bytes_total", "Websocket payload bytes received", ["venue"], registry=kw
        )
        self.events = Counter(
            "marketpipe_events_total", "Normalised events produced", ["venue", "kind"], registry=kw
        )
        self.exchange_latency = Histogram(
            "marketpipe_exchange_latency_seconds",
            "Venue timestamp to local receipt, corrected for the local clock offset",
            ["venue"],
            buckets=_LATENCY_BUCKETS,
            registry=kw,
        )
        self.ingest_latency = Histogram(
            "marketpipe_ingest_latency_seconds",
            "Local receipt to database commit (monotonic clock)",
            ["kind"],
            buckets=_LATENCY_BUCKETS,
            registry=kw,
        )
        self.parse_seconds = Histogram(
            "marketpipe_parse_seconds",
            "Decoding time of one websocket message",
            ["venue"],
            buckets=(1e-6, 5e-6, 1e-5, 2.5e-5, 5e-5, 1e-4, 2.5e-4, 1e-3, 1e-2),
            registry=kw,
        )
        self.gaps = Counter(
            "marketpipe_gaps_total",
            "Runs of missing sequence numbers detected",
            ["venue", "stream", "cause"],
            registry=kw,
        )
        self.missing = Counter(
            "marketpipe_missing_total",
            "Sequence numbers detected as missing",
            ["venue", "stream", "cause"],
            registry=kw,
        )
        self.recovered = Counter(
            "marketpipe_recovered_total",
            "Missing trades recovered from the REST history",
            ["venue"],
            registry=kw,
        )
        self.unrecovered = Counter(
            "marketpipe_unrecovered_total",
            "Missing trades the REST history could not provide",
            ["venue"],
            registry=kw,
        )
        self.duplicates = Counter(
            "marketpipe_duplicates_total", "Trades received more than once", ["venue"], registry=kw
        )
        self.reconnects = Counter(
            "marketpipe_reconnects_total", "Websocket reconnections", ["venue", "reason"], registry=kw
        )
        self.connected = Gauge(
            "marketpipe_connected", "1 while the websocket is open", ["venue"], registry=kw
        )
        self.last_message_age = Gauge(
            "marketpipe_last_message_age_seconds", "Time since the last message", ["venue"], registry=kw
        )
        self.book_resyncs = Counter(
            "marketpipe_book_resyncs_total",
            "Order book rebuilds after a broken update chain",
            ["venue", "symbol"],
            registry=kw,
        )
        self.queue_depth = Gauge("marketpipe_queue_depth", "Events waiting for the writer", registry=kw)
        self.rows_written = Counter(
            "marketpipe_rows_written_total", "Rows inserted in the database", ["table"], registry=kw
        )
        self.rows_deduplicated = Counter(
            "marketpipe_rows_deduplicated_total",
            "Rows skipped because already stored",
            ["table"],
            registry=kw,
        )
        self.db_batch_size = Histogram(
            "marketpipe_db_batch_rows",
            "Rows per database batch",
            ["table"],
            buckets=(1, 10, 50, 100, 250, 500, 1000, 2500, 5000, 10000),
            registry=kw,
        )
        self.db_write_seconds = Histogram(
            "marketpipe_db_write_seconds",
            "Duration of one batch write",
            ["table"],
            buckets=_LATENCY_BUCKETS,
            registry=kw,
        )
        self.db_errors = Counter("marketpipe_db_errors_total", "Failed batch writes", ["table"], registry=kw)
        self.clock_offset = Gauge(
            "marketpipe_clock_offset_seconds", "Reference clock minus local clock", ["reference"], registry=kw
        )
        self.clock_delay = Gauge(
            "marketpipe_clock_delay_seconds",
            "Round-trip delay of the clock measurement",
            ["reference"],
            registry=kw,
        )
        self.loop_lag = Histogram(
            "marketpipe_event_loop_lag_seconds",
            "Scheduling delay of the asyncio event loop",
            buckets=_LATENCY_BUCKETS,
            registry=kw,
        )
        self.ws_rtt = Histogram(
            "marketpipe_ws_ping_rtt_seconds",
            "Websocket ping round-trip time",
            ["venue"],
            buckets=_LATENCY_BUCKETS,
            registry=kw,
        )
