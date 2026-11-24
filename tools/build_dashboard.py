"""Generates the Grafana dashboard deploy/grafana/dashboards/marketpipe.json.

python tools/build_dashboard.py
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

PROM = {"type": "prometheus", "uid": "prometheus"}
TSDB = {"type": "grafana-postgresql-datasource", "uid": "timescaledb"}
OUT = Path(__file__).resolve().parents[1] / "deploy" / "grafana" / "dashboards" / "marketpipe.json"

_next_id = 0


def _id() -> int:
    global _next_id
    _next_id += 1
    return _next_id


def prom(expr: str, legend: str = "", ref: str = "A") -> dict[str, Any]:
    return {"datasource": PROM, "expr": expr, "legendFormat": legend, "refId": ref}


def sql(query: str, fmt: str = "time_series", ref: str = "A") -> dict[str, Any]:
    return {
        "datasource": TSDB,
        "rawSql": query,
        "format": fmt,
        "rawQuery": True,
        "editorMode": "code",
        "refId": ref,
    }


def row(title: str, y: int) -> dict[str, Any]:
    return {
        "type": "row",
        "title": title,
        "id": _id(),
        "collapsed": False,
        "gridPos": {"h": 1, "w": 24, "x": 0, "y": y},
        "panels": [],
    }


def panel(
    kind: str,
    title: str,
    targets: list[dict[str, Any]],
    x: int,
    y: int,
    w: int,
    h: int,
    unit: str = "short",
    description: str = "",
    **options: Any,
) -> dict[str, Any]:
    p: dict[str, Any] = {
        "type": kind,
        "title": title,
        "id": _id(),
        "description": description,
        "datasource": targets[0]["datasource"],
        "targets": targets,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "fieldConfig": {"defaults": {"unit": unit, "custom": {}}, "overrides": []},
        "options": {
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
            "tooltip": {"mode": "multi", "sort": "desc"},
        },
    }
    if kind == "timeseries":
        p["fieldConfig"]["defaults"]["custom"] = {"lineWidth": 2, "fillOpacity": 0, "showPoints": "never"}
    if kind == "stat":
        p["options"] = {
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "colorMode": "value",
            "graphMode": "area",
            "textMode": "auto",
        }
    p["options"].update(options)
    return p


def thresholds(p: dict[str, Any], steps: list[tuple[float | None, str]]) -> dict[str, Any]:
    p["fieldConfig"]["defaults"]["thresholds"] = {
        "mode": "absolute",
        "steps": [{"value": v, "color": c} for v, c in steps],
    }
    p["fieldConfig"]["defaults"]["color"] = {"mode": "thresholds"}
    return p


def quantile(q: float, metric: str, by: str = "venue", window: str = "1m") -> str:
    return f"histogram_quantile({q}, sum by ({by}, le) (rate({metric}_bucket[{window}])))"


def build() -> dict[str, Any]:
    panels: list[dict[str, Any]] = []
    y = 0
    panels.append(row("Health", y))
    y += 1
    panels += [
        thresholds(
            panel("stat", "Feeds connected", [prom("sum(marketpipe_connected)")], 0, y, 4, 4),
            [(None, "red"), (2, "orange"), (3, "green")],
        ),
        panel(
            "stat",
            "Trades / s",
            [prom("sum(rate(marketpipe_events_total{kind='trade'}[1m]))")],
            4,
            y,
            4,
            4,
            unit="ops",
        ),
        panel(
            "stat",
            "Trades missed (time range)",
            [
                sql(
                    "SELECT coalesce(sum(size), 0) AS missed FROM gaps "
                    "WHERE stream = 'trades' AND $__timeFilter(detected_at)",
                    fmt="table",
                )
            ],
            8,
            y,
            4,
            4,
            description="Trade ids detected as missing, before recovery (gaps table, exact across restarts)",
        ),
        thresholds(
            panel(
                "stat",
                "Trades not recovered (time range)",
                [
                    sql(
                        "SELECT coalesce(sum(size - recovered), 0) AS lost FROM gaps "
                        "WHERE stream = 'trades' AND status <> 'open' AND $__timeFilter(detected_at)",
                        fmt="table",
                    )
                ],
                12,
                y,
                4,
                4,
                description="Missing trades that the REST history could not provide",
            ),
            [(None, "green"), (1, "red")],
        ),
        panel(
            "stat",
            "p99 exchange latency",
            [prom(quantile(0.99, "marketpipe_exchange_latency_seconds", "le", "5m"))],
            16,
            y,
            4,
            4,
            unit="s",
        ),
        thresholds(
            panel(
                "stat",
                "Clock offset vs NTP",
                [prom("marketpipe_clock_offset_seconds{reference='model'}")],
                20,
                y,
                4,
                4,
                unit="s",
                description="Reference minus local clock, used to correct latencies",
            ),
            [(None, "red"), (-0.05, "green"), (0.05, "red")],
        ),
    ]
    y += 4
    panels.append(row("Throughput and latency", y))
    y += 1
    panels += [
        panel(
            "timeseries",
            "Messages per second",
            [prom("sum by (venue) (rate(marketpipe_messages_total[1m]))", "{{venue}}")],
            0,
            y,
            8,
            8,
            unit="ops",
        ),
        panel(
            "timeseries",
            "Exchange -> receipt latency (p50, p99)",
            [
                prom(quantile(0.5, "marketpipe_exchange_latency_seconds"), "p50 {{venue}}", "A"),
                prom(quantile(0.99, "marketpipe_exchange_latency_seconds"), "p99 {{venue}}", "B"),
            ],
            8,
            y,
            8,
            8,
            unit="s",
            description="Corrected for the local clock offset measured by NTP",
        ),
        panel(
            "timeseries",
            "Receipt -> database commit (p50, p99)",
            [
                prom(quantile(0.5, "marketpipe_ingest_latency_seconds", "kind"), "p50 {{kind}}", "A"),
                prom(quantile(0.99, "marketpipe_ingest_latency_seconds", "kind"), "p99 {{kind}}", "B"),
            ],
            16,
            y,
            8,
            8,
            unit="s",
            description="Monotonic clock: batching delay plus write time",
        ),
    ]
    y += 8
    panels.append(row("Losses and recovery", y))
    y += 1
    panels += [
        panel(
            "timeseries",
            "Missing sequence numbers per minute",
            [
                prom(
                    "sum by (venue, stream, cause) (increase(marketpipe_missing_total[1m]))",
                    "{{venue}} {{stream}} {{cause}}",
                )
            ],
            0,
            y,
            8,
            8,
            drawStyle="bars",
        ),
        panel(
            "timeseries",
            "Recovered vs lost trades (cumulative)",
            [
                prom("sum by (venue) (marketpipe_recovered_total)", "recovered {{venue}}", "A"),
                prom("sum by (venue) (marketpipe_unrecovered_total)", "lost {{venue}}", "B"),
            ],
            8,
            y,
            8,
            8,
        ),
        panel(
            "timeseries",
            "Reconnections per 5 minutes",
            [
                prom(
                    "sum by (venue, reason) (increase(marketpipe_reconnects_total[5m]))",
                    "{{venue}} {{reason}}",
                )
            ],
            16,
            y,
            8,
            8,
        ),
    ]
    y += 8
    panels.append(row("Pipeline internals", y))
    y += 1
    panels += [
        panel("timeseries", "Writer queue depth", [prom("marketpipe_queue_depth", "events")], 0, y, 6, 7),
        panel(
            "timeseries",
            "Database batch write time (p99)",
            [prom(quantile(0.99, "marketpipe_db_write_seconds", "table"), "{{table}}")],
            6,
            y,
            6,
            7,
            unit="s",
        ),
        panel(
            "timeseries",
            "Event loop lag (p99)",
            [
                prom(
                    "histogram_quantile(0.99, sum by (le) "
                    "(rate(marketpipe_event_loop_lag_seconds_bucket[1m])))",
                    "lag",
                )
            ],
            12,
            y,
            6,
            7,
            unit="s",
            description="Bounds the error of every receipt timestamp",
        ),
        panel(
            "timeseries",
            "Time since last message",
            [prom("marketpipe_last_message_age_seconds", "{{venue}}")],
            18,
            y,
            6,
            7,
            unit="s",
        ),
    ]
    y += 7
    panels.append(row("Market data (TimescaleDB)", y))
    y += 1
    panels += [
        panel(
            "timeseries",
            "BTC close, one-minute bars",
            [
                sql(
                    "SELECT bucket AS time, venue AS metric, close FROM ohlcv_1m "
                    "WHERE symbol LIKE 'BTC-%' AND $__timeFilter(bucket) ORDER BY 1"
                )
            ],
            0,
            y,
            12,
            8,
            unit="currencyUSD",
        ),
        panel(
            "timeseries",
            "Trades per minute",
            [
                sql(
                    "SELECT bucket AS time, venue || ' ' || symbol AS metric, trades FROM ohlcv_1m "
                    "WHERE $__timeFilter(bucket) ORDER BY 1"
                )
            ],
            12,
            y,
            12,
            8,
        ),
    ]
    y += 8
    panels.append(
        panel(
            "table",
            "Latest gaps",
            [
                sql(
                    "SELECT detected_at, venue, symbol, stream, size, after_reconnect, status, recovered "
                    "FROM gaps "
                    "ORDER BY detected_at DESC LIMIT 50",
                    fmt="table",
                )
            ],
            0,
            y,
            24,
            8,
        )
    )

    return {
        "uid": "marketpipe",
        "title": "marketpipe: market data pipeline",
        "tags": ["marketpipe"],
        "timezone": "utc",
        "schemaVersion": 39,
        "version": 1,
        "editable": True,
        "refresh": "10s",
        "time": {"from": "now-3h", "to": "now"},
        "panels": panels,
        "annotations": {
            "list": [
                {
                    "name": "Disconnections",
                    "datasource": TSDB,
                    "enable": True,
                    "iconColor": "rgba(255, 96, 96, 1)",
                    "rawQuery": "SELECT ts AS time, venue || ': ' || reason AS text FROM connection_events "
                    "WHERE event = 'disconnected' AND $__timeFilter(ts)",
                }
            ]
        },
    }


def main() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(build(), indent=2) + "\n", encoding="utf-8")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
