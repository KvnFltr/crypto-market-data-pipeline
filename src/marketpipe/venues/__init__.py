"""Venue adapters."""

from marketpipe.venues.base import BookSnapshot, VenueAdapter
from marketpipe.venues.binance import BinanceAdapter
from marketpipe.venues.coinbase import CoinbaseAdapter
from marketpipe.venues.kraken import KrakenAdapter

ADAPTERS: dict[str, type[VenueAdapter]] = {
    BinanceAdapter.name: BinanceAdapter,
    CoinbaseAdapter.name: CoinbaseAdapter,
    KrakenAdapter.name: KrakenAdapter,
}


def make_adapter(venue: str, symbols: list[str], order_book: bool = False) -> VenueAdapter:
    try:
        cls = ADAPTERS[venue]
    except KeyError:
        raise ValueError(f"unknown venue {venue!r}; available: {sorted(ADAPTERS)}") from None
    return cls(symbols, order_book)


__all__ = [
    "ADAPTERS",
    "BinanceAdapter",
    "BookSnapshot",
    "CoinbaseAdapter",
    "KrakenAdapter",
    "VenueAdapter",
    "make_adapter",
]
