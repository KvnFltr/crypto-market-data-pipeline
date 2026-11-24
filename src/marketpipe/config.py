"""Configuration, loaded from a TOML file with environment overrides."""

from __future__ import annotations

import os
import tomllib
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

DEFAULT_DSN = "postgresql://marketpipe:marketpipe@localhost:5432/marketpipe"


@dataclass(slots=True)
class FeedConfig:
    venue: str
    symbols: list[str]
    order_book: bool = False


@dataclass(slots=True)
class ChaosConfig:
    """Fault injection: forced disconnections at exponentially distributed intervals."""

    enabled: bool = False
    mean_interval_s: float = 300.0
    min_downtime_s: float = 1.0
    max_downtime_s: float = 20.0
    seed: int = 7


@dataclass(slots=True)
class Config:
    dsn: str = DEFAULT_DSN
    feeds: list[FeedConfig] = field(default_factory=list)
    queue_size: int = 100_000
    batch_size: int = 2_000
    flush_interval_s: float = 0.1
    metrics_port: int = 8000
    resume: bool = True  # recover the trades missed while the pipeline was stopped
    resume_max_age_s: float = 6 * 3600.0
    max_backfill_trades: int = 500_000  # larger gaps are recorded as unrecoverable
    ntp_servers: list[str] = field(default_factory=lambda: ["time.cloudflare.com", "pool.ntp.org"])
    clock_interval_s: float = 60.0
    clock_rest_check: bool = True  # also compare with Binance's server time (Cristian)
    ping_interval_s: float = 10.0
    chaos: ChaosConfig = field(default_factory=ChaosConfig)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["dsn"] = _redact(self.dsn)
        return d


def _redact(dsn: str) -> str:
    if "@" not in dsn or "://" not in dsn:
        return dsn
    scheme, rest = dsn.split("://", 1)
    creds, host = rest.rsplit("@", 1)
    user = creds.split(":", 1)[0]
    return f"{scheme}://{user}:***@{host}"


def load_config(path: str | Path | None = None) -> Config:
    raw: dict[str, Any] = {}
    if path is not None:
        with open(path, "rb") as fh:
            raw = tomllib.load(fh)

    pipeline = raw.get("pipeline", {})
    clock = raw.get("clock", {})
    cfg = Config(
        dsn=raw.get("database", {}).get("dsn", DEFAULT_DSN),
        feeds=[
            FeedConfig(f["venue"], list(f["symbols"]), bool(f.get("order_book", False)))
            for f in raw.get("feeds", [])
        ],
        chaos=ChaosConfig(**raw.get("chaos", {})),
    )
    for key in (
        "queue_size",
        "batch_size",
        "flush_interval_s",
        "metrics_port",
        "resume",
        "resume_max_age_s",
        "max_backfill_trades",
        "ping_interval_s",
    ):
        if key in pipeline:
            setattr(cfg, key, type(getattr(cfg, key))(pipeline[key]))
    if "ntp_servers" in clock:
        cfg.ntp_servers = list(clock["ntp_servers"])
    if "interval_s" in clock:
        cfg.clock_interval_s = float(clock["interval_s"])
    if "rest_check" in clock:
        cfg.clock_rest_check = bool(clock["rest_check"])

    cfg.dsn = os.environ.get("MARKETPIPE_DSN", cfg.dsn)
    if not cfg.feeds:
        raise ValueError("configuration defines no [[feeds]]")
    return cfg
