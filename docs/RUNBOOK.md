# Runbook

What each alert means and what to do about it. Alert rules live in
[`deploy/prometheus/alerts.yml`](../deploy/prometheus/alerts.yml); the Grafana dashboard
(<http://localhost:3000>) has a panel for every metric mentioned below.

Useful queries:

```sql
-- gaps of the last hour and what became of them
SELECT detected_at, venue, symbol, stream, size, after_reconnect, status, recovered
FROM gaps WHERE detected_at > now() - interval '1 hour' ORDER BY detected_at DESC;

-- disconnections of the last hour
SELECT ts, venue, reason, round(session_s) AS session_s
FROM connection_events WHERE event = 'disconnected' AND ts > now() - interval '1 hour' ORDER BY ts DESC;
```

## TradesLost (critical)

Trades were missed and the REST history could not provide them (`marketpipe_unrecovered_total`).

1. Find the gaps with `status IN ('open', 'partial', 'unrecoverable')` in the `gaps` table.
2. Retry them: `marketpipe repair` re-runs the recovery of every such gap (add `--max-trades N` for gaps that
   exceeded `max_backfill_trades`, which the pipeline does not attempt).
3. If the REST calls failed (HTTP 429/418 in the logs), the venue rate-limited us: wait before retrying.
4. Confirm with the audit for the window (`marketpipe audit --venue ... --symbol ... --start ... --end ...`).
5. If the venue's history no longer has the trades, the loss is final: record it for the users of the data.

## FeedStale / FeedDisconnected (critical)

No message from a venue for 30 seconds, or no connection for 2 minutes.

1. Check the venue's status page and the `connection_events` reasons (`connect_error`, `closed`, `stale`).
2. `connect_error` on every attempt: DNS, network or a geo-restriction; try the websocket URL by hand.
3. Repeated `stale`: the connection opens but nothing flows; check the subscription messages against the
   venue's current API.
4. Nothing to repair afterwards: the trades missed while down are recovered automatically at reconnection.

## DatabaseWriteErrors (critical) / WriterBacklog (warning)

Batch writes fail or the writer cannot keep up (`marketpipe_queue_depth`).

1. Check that TimescaleDB is up (`docker compose ps`) and has disk space.
2. The writer retries a failed batch with backoff and keeps it in memory; when the queue is full the feeds stop
   reading, venues eventually disconnect us, and the resulting gaps are repaired once the database is back.
   No action is needed beyond restoring the database, but expect a burst of backfill afterwards.
3. A sustained backlog with a healthy database means the write path is too slow: check
   `marketpipe_db_write_seconds` and the batch size.

## GapRateHigh (warning)

More than 5 gaps in 10 minutes on a venue. Gaps are repaired, but frequent disconnections usually point to a
network problem or to a slow consumer: check `marketpipe_event_loop_lag_seconds` (a busy host makes the
pipeline read too slowly) and the disconnection reasons.

## ExchangeLatencyHigh (warning)

p99 latency above one second for five minutes.

1. Compare with the independent probe (`tools/latency_probe.py`): if it sees the same latency, the delay is
   upstream (network or venue) and there is nothing to fix locally.
2. If only the pipeline is slow, look at the event-loop lag and at other workloads on the host.

## ClockOffsetLarge (warning)

The local clock is more than 50 ms away from NTP, so raw latencies are biased by that amount. Check the
operating system's time synchronisation (`w32tm /query /status` on Windows, `chronyc tracking` on Linux).
The stored receipt times are raw; the analysis corrects them from the `clock_samples` table.
