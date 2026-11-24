import asyncio
import struct

import pytest

from marketpipe.clock import (
    ClockModel,
    OffsetSample,
    build_sntp_request,
    cristian_sample,
    ns_to_ntp,
    ntp_to_ns,
    parse_sntp_response,
    sntp_query,
)

T0 = 1_795_483_153_000_000_000  # an arbitrary instant in 2026, ns since epoch


def server_reply(t0: int, t1: int, t2: int, stratum: int = 2, mode: int = 4) -> bytes:
    words = [0, 0, 0, 0, 0, *ns_to_ntp(t0), *ns_to_ntp(t1), *ns_to_ntp(t2)]
    return struct.pack("!B B B b 11I", (4 << 3) | mode, stratum, 0, -20, *words)


def test_ntp_timestamp_round_trip_is_sub_microsecond() -> None:
    for ns in (0, T0, T0 + 123_456_789):
        assert abs(ntp_to_ns(*ns_to_ntp(ns)) - ns) <= 1


def test_request_layout() -> None:
    packet = build_sntp_request(T0)
    assert len(packet) == 48
    assert packet[0] == 0x23  # LI 0, version 4, mode 3 (client)


def test_offset_and_delay_with_symmetric_paths() -> None:
    # local clock 30 ms behind the server, 10 ms each way, 1 ms in the server
    offset, one_way = 30_000_000, 10_000_000
    t0 = T0
    t1 = t0 + one_way + offset
    t2 = t1 + 1_000_000
    t3 = t2 - offset + one_way
    s = parse_sntp_response(server_reply(t0, t1, t2), t0, t3, "test")
    assert abs(s.offset_ns - offset) <= 2
    assert abs(s.delay_ns - 2 * one_way) <= 2
    assert s.uncertainty_ns == s.delay_ns // 2


def test_asymmetric_paths_bias_offset_within_half_delay() -> None:
    t0 = T0
    t1 = t0 + 30_000_000  # 30 ms out, true offset 0
    t2 = t1
    t3 = t2 + 2_000_000  # 2 ms back
    s = parse_sntp_response(server_reply(t0, t1, t2), t0, t3, "test")
    assert abs(s.offset_ns) <= s.uncertainty_ns  # the error bound holds


@pytest.mark.parametrize(
    ("reply", "error"),
    [
        (server_reply(T0, T0, T0, stratum=0), "unusable"),
        (server_reply(T0, T0, T0, mode=3), "unusable"),
        (server_reply(T0 + 10_000_000, T0, T0), "originate"),
        (b"\x00" * 20, "short"),
    ],
)
def test_bad_replies_are_rejected(reply: bytes, error: str) -> None:
    with pytest.raises(ValueError, match=error):
        parse_sntp_response(reply, T0, T0 + 1, "test")


def test_cristian_sample() -> None:
    s = cristian_sample("rest", t0_ns=T0, t3_ns=T0 + 100_000_000, server_ns=T0 + 70_000_000)
    assert s.offset_ns == 20_000_000 and s.delay_ns == 100_000_000


def test_clock_model_trusts_the_tightest_sample() -> None:
    m = ClockModel(window=3)
    assert m.offset_ns == 0 and m.best is None
    m.add(OffsetSample("a", 0, 5_000_000, 40_000_000))
    m.add(OffsetSample("b", 0, 2_000_000, 8_000_000))
    m.add(OffsetSample("c", 0, 9_000_000, 30_000_000))
    assert m.offset_ns == 2_000_000
    m.add(OffsetSample("d", 0, 7_000_000, 50_000_000))  # evicts "a"
    m.add(OffsetSample("e", 0, 7_000_000, 50_000_000))  # evicts "b": the best is now "c"
    assert m.offset_ns == 9_000_000
    assert m.corrected_latency_ns(recv_ts_ns=1_000_000_000, exchange_ts_ns=900_000_000) == 109_000_000


async def test_sntp_query_against_local_server() -> None:
    """A tiny NTP server on localhost whose clock is 50 ms ahead of ours."""

    class Server(asyncio.DatagramProtocol):
        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            self.transport = transport

        def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
            import time

            fields = struct.unpack("!B B B b 11I", data)
            t0 = ntp_to_ns(fields[13], fields[14])
            now = time.time_ns() + 50_000_000
            self.transport.sendto(server_reply(t0, now, now), addr)  # type: ignore[attr-defined]

    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(Server, local_addr=("127.0.0.1", 0))
    port = transport.get_extra_info("sockname")[1]
    try:
        sample = await sntp_query("127.0.0.1", port=port)
    finally:
        transport.close()
    assert abs(sample.offset_ns - 50_000_000) < 5_000_000
    assert sample.delay_ns < 50_000_000
