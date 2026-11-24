"""Normalised market data events shared by every venue adapter."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

# Aggressor side of a trade: +1 when the taker bought, -1 when the taker sold.
BUY = 1
SELL = -1


class Source(StrEnum):
    WEBSOCKET = "ws"
    REST = "rest"  # recovered by the backfill worker


@dataclass(slots=True, frozen=True)
class Trade:
    venue: str
    symbol: str  # normalised, e.g. "BTC-USDT"
    trade_id: int  # venue sequence number, contiguous per (venue, symbol)
    price: float
    qty: float
    side: int  # BUY or SELL (aggressor)
    exchange_ts_ns: int  # trade time stamped by the venue, ns since epoch
    recv_ts_ns: int  # local wall clock when the message was handed to us
    recv_mono_ns: int = 0  # local monotonic clock, for internal latency only (not stored)
    source: Source = Source.WEBSOCKET
    event_ts_ns: int | None = None  # time the venue published the message, when provided


@dataclass(slots=True, frozen=True)
class Quote:
    """Best bid and offer derived from the local order book."""

    venue: str
    symbol: str
    update_id: int
    bid_px: float
    bid_qty: float
    ask_px: float
    ask_qty: float
    exchange_ts_ns: int
    recv_ts_ns: int
    recv_mono_ns: int = 0


@dataclass(slots=True, frozen=True)
class DepthUpdate:
    """Incremental order book update covering the venue update ids [first_id, last_id]."""

    venue: str
    symbol: str
    first_id: int
    last_id: int
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]
    exchange_ts_ns: int
    recv_ts_ns: int
    recv_mono_ns: int = 0


@dataclass(slots=True, frozen=True)
class Heartbeat:
    """Venue heartbeat. `last_trade_id`, when present, lets us detect gaps even without trades."""

    venue: str
    symbol: str | None
    last_trade_id: int | None
    recv_ts_ns: int


@dataclass(slots=True)
class Gap:
    """A run of missing sequence numbers detected on one stream."""

    venue: str
    symbol: str
    stream: str  # "trades" or "book"
    first_missing: int
    last_missing: int
    detected_ts_ns: int
    after_reconnect: bool
    recovered: int = 0
    status: str = "open"  # open | recovered | partial | unrecoverable | resynced
    gap_id: int | None = field(default=None)
    cause: str = ""  # in_session | reconnect | restart | heartbeat

    @property
    def size(self) -> int:
        return self.last_missing - self.first_missing + 1


@dataclass(slots=True, frozen=True)
class ConnectionEvent:
    venue: str
    event: str  # connected | disconnected
    reason: str
    ts_ns: int
    session_s: float = 0.0  # duration of the session that just ended


Event = Trade | Quote | DepthUpdate | Heartbeat
