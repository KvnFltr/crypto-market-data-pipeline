# Crypto Market Data Pipeline

[![CI](https://github.com/KvnFltr/crypto-market-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/KvnFltr/crypto-market-data-pipeline/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-informational)

A real-time market data pipeline that ingests trades and order books from **Binance, Coinbase and Kraken**
over websockets, stores them in **TimescaleDB**, and answers two questions with numbers rather than hope:

1. **Is any data missing?** Every venue numbers its trades. The pipeline checks every sequence number, records
   every gap, recovers the missing trades from the venue's REST history, and stores them idempotently. An
   independent audit compares the database with each venue's own records.
2. **How late is the data?** Latencies are corrected for the offset of the local clock (measured with NTP) and
   reported with its uncertainty, compared with the speed-of-light bound, and checked against an independent
   probe that separates the pipeline's own delay from the network's.

The report, [`report/report.pdf`](report/report.pdf), describes the design and a live 2.4-hour experiment
with a planned restart, fault injection, a planned noisy-neighbour phase and an independent completeness audit.

![Grafana dashboard](docs/img/grafana.png)

## Results of the live experiment

Measured from a laptop in Paris on a university Wi-Fi network, over 58 quiet minutes:

| | Binance (Tokyo) | Coinbase | Kraken |
|---|---:|---:|---:|
| Median latency, clock-corrected | 119 ms | 57 ms | 15 ms |
| 99th percentile | 379 ms | 221 ms | 63 ms |
| Minimum (speed-of-light bound in fibre) | 111.9 ms (48.6) | 44.7 ms (30.9) | 8.9 ms |

- **1,318,080 trades** stored. **391,457 trades missed** by the websockets (a planned restart, 12 injected
  disconnections and an *unplanned* 26-minute suspension of the host, with a single gap of 120,213 Binance
  trades) and **100% recovered** from the venues' histories. No gap inside a connection, no crossed quote.
- **Independent audit** against the venues' own records: 1,216,302 trades, **0 missing,
  0 altered**.
- **The local clock was 1.02 s ahead of UTC** (NTP, ±3 ms; confirmed by Binance's server time). Uncorrected,
  Binance would have looked ten times slower than it is.
- **Control experiment**: a minimal independent receiver sees the same latency to within 0.8 ms in the median
  and most of the same spikes, so they are upstream. A planned noisy-neighbour load did not affect the
  pipeline; its remaining tail excess is consistent with order book streams sharing the connection.
- Receipt to database commit: 125 ms median (batching); 17 MB for 1.3 M trades after compression
  (14 bytes per trade, ratio 20).

![Latency over time](docs/img/latency_time.png)
![Control experiment](docs/img/probe.png)

More figures: [throughput](docs/img/throughput.png), [latency distribution](docs/img/latency_cdf.png),
[clock offset](docs/img/clock.png), [losses vs downtime](docs/img/gaps_downtime.png).

## Architecture

```mermaid
flowchart LR
    B["Binance<br/>trades · order book"] -- websocket --> F
    C["Coinbase<br/>matches · heartbeat"] -- websocket --> F
    K["Kraken<br/>trades · heartbeat"] -- websocket --> F
    F["Feed handlers<br/>parse · sequence check<br/>order book sync"] -- bounded queue --> W["Batch writer<br/>COPY + dedup"]
    W --> DB[("TimescaleDB<br/>hypertables · 1-min bars")]
    F -. gaps .-> R["Backfill workers<br/>REST history"]
    R --> DB
    N["Clock monitor<br/>NTP · exchange time"] -. offset .-> F
    F -. metrics .-> P["Prometheus"]
    W -. metrics .-> P
    P --> G["Grafana · alerts"]
```

- **Exact loss accounting.** Trade ids are contiguous per instrument on all three venues. A gap is classified
  as in-session, after a reconnection, after a restart (the last id stored in the database seeds the tracker),
  or revealed by a Coinbase heartbeat on a quiet market. Binance order book updates must chain (`U == u' + 1`);
  a break triggers a rebuild from a REST snapshot.
- **Recovery and effectively-once storage.** Gaps are repaired from the REST history (`historicalTrades` by id
  on Binance, backwards pagination on Coinbase, time-indexed search on Kraken). Writes go through `COPY` into
  a staging table and one deduplicating `INSERT ... SELECT`, so replays are harmless.
- **Honest latency.** SNTP against two servers, offset taken from the minimum-delay samples with a `±δ/2` bound;
  Binance's event time splits venue-side and transport delay; event-loop lag is monitored; an independent probe
  provides a control.
- **Robust connections.** Exponential backoff with full jitter, staleness detection for half-open sockets,
  proactive rotation before Binance's 24-hour limit, backpressure instead of dropping.
- **Observability.** 25 Prometheus metrics, a provisioned Grafana dashboard and alert rules (stale feed, lost
  trades, latency, backlog, write errors, clock drift), with a [runbook](docs/RUNBOOK.md) for each alert.

## Quick start

Requirements: Docker and Python 3.11+.

```bash
docker compose up -d                        # TimescaleDB, Prometheus, Grafana
pip install -e ".[dev,analysis]"
marketpipe run                              # ingest with config/pipeline.toml
```

Grafana is on <http://localhost:3000> (the dashboard opens by default), Prometheus on <http://localhost:9090>,
metrics on <http://localhost:8000/metrics>. To run everything in containers instead:
`docker compose --profile pipeline up -d`.

Other commands:

```bash
marketpipe run --chaos --chaos-interval 120         # inject random disconnections
marketpipe audit --venue binance --symbol BTC-USDT --start 2026-11-24T03:00 --end 2026-11-24T04:00
marketpipe repair                                   # retry gaps left open or partially recovered
python tools/latency_probe.py                       # independent latency probe (control)
```

## Tests

```bash
pytest                                       # unit tests
MARKETPIPE_TEST_DSN=postgresql://marketpipe:marketpipe@localhost:5432/marketpipe_test pytest   # + database
```

50 tests: sequencing, SNTP arithmetic (with a local NTP server), parsers on captured live messages,
REST pagination, the order book procedure, and end-to-end runs against a **fake exchange** that drops
messages, cuts connections and refuses reconnections: the database must contain every trade of the tape
exactly once. CI runs ruff, `mypy --strict` and the tests on Python 3.11 to 3.13 against a TimescaleDB service
container, builds the Docker image and compiles the report.

## Reproducing the report

```bash
python analysis/export.py --audit --compress   # database + Prometheus -> results/*.csv
python analysis/make_figures.py                # figures, LaTeX tables and macros
latexmk -pdf -cd report/report.tex
```

The exported data of the experiment are versioned in [`results/`](results), so the figures and the report
can be rebuilt without the database.

## Repository layout

```text
src/marketpipe/      the pipeline
  venues/              Binance, Coinbase, Kraken adapters (parsing, REST recovery)
  feed.py              connection management, sequencing, fault injection
  book.py              order book synchronisation
  backfill.py          gap recovery
  writer.py storage.py batching and idempotent TimescaleDB writes, schema.sql
  clock.py monitors.py SNTP, clock model, event-loop lag, staleness
  metrics.py audit.py  Prometheus metrics, completeness audit
tests/               unit, fake-exchange and database integration tests
deploy/              Prometheus config and alerts, Grafana provisioning and dashboard
tools/               independent latency probe, dashboard generator
analysis/            export, storage benchmark, figures
report/              LaTeX report
```

## License

MIT, see [LICENSE](LICENSE).
