"""Parsers and REST recovery of each venue, on messages captured from the live feeds."""

import httpx
import pytest

from marketpipe.models import BUY, SELL, DepthUpdate, Heartbeat, Source, Trade
from marketpipe.venues import BinanceAdapter, CoinbaseAdapter, KrakenAdapter, make_adapter
from marketpipe.venues.base import iso_to_ns

RECV, MONO = 1_795_483_153_200_000_000, 42

BINANCE_TRADE = (
    '{"stream":"btcusdt@trade","data":{"e":"trade","E":1795483153135399,"s":"BTCUSDT","t":6903678988,'
    '"p":"61495.44000000","q":"0.00009000","T":1795483153134938,"m":true,"M":true}}'
)
BINANCE_DEPTH = (
    '{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1795483152714000,"s":"BTCUSDT",'
    '"U":103562226781,"u":103562226837,"b":[["61495.44000000","0.88875000"],["61493.12000000","0.00000000"]],'
    '"a":[["61495.45000000","1.20000000"]]}}'
)
COINBASE_MATCH = (
    '{"type":"match","trade_id":1144825103,"maker_order_id":"3e3b","taker_order_id":"d835","side":"buy",'
    '"size":"0.00128473","price":"61404.28","product_id":"BTC-USD","sequence":140134567376,'
    '"time":"2026-11-24T01:19:12.559094Z"}'
)
COINBASE_HEARTBEAT = (
    '{"type":"heartbeat","last_trade_id":1144825110,"product_id":"BTC-USD","sequence":140134567999,'
    '"time":"2026-11-24T01:19:13.000000Z"}'
)
KRAKEN_TRADES = (
    '{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"buy","price":61403.8,'
    '"qty":0.00151932,"ord_type":"limit","trade_id":117641209,"timestamp":"2026-11-24T01:19:12.104509Z"},'
    '{"symbol":"BTC/USD","side":"sell","price":61403.7,"qty":0.01,"ord_type":"market","trade_id":117641210,'
    '"timestamp":"2026-11-24T01:19:12.104600Z"}]}'
)


def test_iso_to_ns_is_exact() -> None:
    assert iso_to_ns("1970-01-01T00:00:01.000001Z") == 1_000_001_000
    assert iso_to_ns("2026-11-24T01:19:12.559094Z") % 1_000 == 0
    assert iso_to_ns("2026-11-24T01:19:12.559094+00:00") == iso_to_ns("2026-11-24T01:19:12.559094Z")


def test_binance_trade() -> None:
    a = BinanceAdapter(["BTC-USDT"])
    [t] = a.parse(BINANCE_TRADE, RECV, MONO)
    assert isinstance(t, Trade)
    assert (t.venue, t.symbol, t.trade_id) == ("binance", "BTC-USDT", 6903678988)
    assert (t.price, t.qty) == (61495.44, 0.00009)
    assert t.side == SELL  # buyer is the maker: the aggressor sold
    assert t.exchange_ts_ns == 1795483153134938_000 and t.event_ts_ns == 1795483153135399_000
    assert (t.recv_ts_ns, t.recv_mono_ns, t.source) == (RECV, MONO, Source.WEBSOCKET)


def test_binance_depth_update_and_url() -> None:
    a = BinanceAdapter(["BTC-USDT", "ETH-USDT"], order_book=True)
    [d] = a.parse(BINANCE_DEPTH, RECV, MONO)
    assert isinstance(d, DepthUpdate)
    assert (d.first_id, d.last_id) == (103562226781, 103562226837)
    assert d.bids == [(61495.44, 0.88875), (61493.12, 0.0)] and d.asks == [(61495.45, 1.2)]
    url = a.ws_url()
    assert "btcusdt@trade" in url and "ethusdt@depth@100ms" in url and url.endswith("timeUnit=MICROSECOND")


def test_binance_ignores_control_messages() -> None:
    assert BinanceAdapter(["BTC-USDT"]).parse('{"result":null,"id":1}', RECV, MONO) == []


def test_coinbase_match_uses_maker_side() -> None:
    a = CoinbaseAdapter(["BTC-USD"])
    [t] = a.parse(COINBASE_MATCH, RECV, MONO)
    assert isinstance(t, Trade)
    assert (t.trade_id, t.price, t.qty) == (1144825103, 61404.28, 0.00128473)
    assert t.side == SELL  # the maker was buying: the taker sold
    assert t.exchange_ts_ns == iso_to_ns("2026-11-24T01:19:12.559094Z")


def test_coinbase_heartbeat_and_error() -> None:
    a = CoinbaseAdapter(["BTC-USD"])
    [hb] = a.parse(COINBASE_HEARTBEAT, RECV, MONO)
    assert hb == Heartbeat("coinbase", "BTC-USD", 1144825110, RECV)
    with pytest.raises(RuntimeError, match="coinbase error"):
        a.parse('{"type":"error","message":"Failed to subscribe","reason":"bad product"}', RECV, MONO)
    assert '"matches"' in a.subscribe_messages()[0] and '"heartbeat"' in a.subscribe_messages()[0]


def test_kraken_batch_of_trades() -> None:
    a = KrakenAdapter(["BTC-USD", "ETH-USD"])
    trades = a.parse(KRAKEN_TRADES, RECV, MONO)
    assert [t.trade_id for t in trades if isinstance(t, Trade)] == [117641209, 117641210]
    assert [t.side for t in trades if isinstance(t, Trade)] == [BUY, SELL]  # Kraken reports the taker side
    assert all(isinstance(t, Trade) and t.symbol == "BTC-USD" for t in trades)
    assert a.rest_pair("BTC-USD") == "XBTUSD" and a.rest_pair("ETH-USD") == "ETHUSD"
    assert isinstance(a.parse('{"channel":"heartbeat"}', RECV, MONO)[0], Heartbeat)


def test_make_adapter() -> None:
    assert isinstance(make_adapter("kraken", ["BTC-USD"]), KrakenAdapter)
    with pytest.raises(ValueError, match="unknown venue"):
        make_adapter("nasdaq", ["AAPL"])
    with pytest.raises(ValueError, match="at least one symbol"):
        make_adapter("binance", [])


# ---------------------------------------------------------------------- REST recovery
def _client(handler) -> httpx.AsyncClient:  # type: ignore[no-untyped-def]
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def test_binance_backfill_pages_by_trade_id() -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        start, limit = int(request.url.params["fromId"]), int(request.url.params["limit"])
        calls.append((start, limit))
        assert request.headers["X-MBX-TIME-UNIT"] == "MICROSECOND"
        rows = [
            {"id": i, "price": "100.5", "qty": "0.1", "time": 1_000_000 + i, "isBuyerMaker": i % 2 == 0}
            for i in range(start, start + min(limit, 1000))
        ]
        return httpx.Response(200, json=rows)

    a = BinanceAdapter(["BTC-USDT"])
    a.rest_limiter._interval = 0  # no rate limiting in tests
    async with _client(handler) as client:
        trades = await a.fetch_trades(client, "BTC-USDT", 10, 2_509, 0)
    assert [t.trade_id for t in trades] == list(range(10, 2_510))
    assert calls == [(10, 1000), (1010, 1000), (2010, 500)]
    assert all(t.source is Source.REST and t.recv_ts_ns == 0 for t in trades)
    assert trades[0].side == SELL and trades[1].side == BUY  # id 10 even: buyer is maker, taker sold


async def test_coinbase_backfill_pages_backwards() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        after = int(request.url.params["after"])
        ids = range(after - 1, max(after - 1001, 0), -1)  # newest first, ids < after
        rows = [
            {"trade_id": i, "side": "sell", "size": "1", "price": "10", "time": "2026-11-24T01:00:00Z"}
            for i in ids
        ]
        return httpx.Response(200, json=rows)

    a = CoinbaseAdapter(["BTC-USD"])
    a.rest_limiter._interval = 0
    async with _client(handler) as client:
        trades = await a.fetch_trades(client, "BTC-USD", 5_000, 7_500, 0)
    assert [t.trade_id for t in trades] == list(range(5_000, 7_501))
    assert all(t.side == BUY for t in trades)


async def test_kraken_backfill_searches_by_time() -> None:
    history = [(i, 1_795_000_000.0 + i * 0.01) for i in range(1, 3_001)]

    def handler(request: httpx.Request) -> httpx.Response:
        since = int(request.url.params["since"])
        rows = [
            [f"{100 + i}", "0.5", ts, "b" if i % 2 else "s", "l", "", i]
            for i, ts in history
            if ts * 1e9 > since
        ][:1000]
        last = str(int(rows[-1][2] * 1e9)) if rows else str(since)
        return httpx.Response(200, json={"error": [], "result": {"XXBTZUSD": rows, "last": last}})

    a = KrakenAdapter(["BTC-USD"])
    a.rest_limiter._interval = 0
    async with _client(handler) as client:
        hint = int((1_795_000_000.0 + 1_200 * 0.01) * 1e9)  # last trade seen before the gap: id 1200
        trades = await a.fetch_trades(client, "BTC-USD", 1_201, 2_600, hint)
    assert [t.trade_id for t in trades] == list(range(1_201, 2_601))
