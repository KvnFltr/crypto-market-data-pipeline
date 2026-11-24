"""Clocks and clock-offset estimation.

One-way latency, exchange timestamp -> local receipt, compares two different clocks. If the
local clock is 20 ms fast, every latency is overstated by 20 ms. We therefore estimate the
offset of the local clock against reference clocks and report latencies with that
correction, together with the uncertainty of the estimate.

* SNTP (RFC 4330) against public NTP servers. With t0 = client send, t1 = server receive,
  t2 = server transmit, t3 = client receive, the server-minus-client offset is
  theta = ((t1 - t0) + (t2 - t3)) / 2 and the round-trip delay delta = (t3 - t0) - (t2 - t1).
  If the true one-way delays are unknown, theta is exact up to +/- delta / 2.
* Cristian's algorithm against an exchange time endpoint: theta = T_server - (t0 + t3) / 2,
  again exact up to +/- (t3 - t0) / 2.

Internal latencies (receipt -> database commit) use the monotonic clock only and do not
suffer from this problem.
"""

from __future__ import annotations

import asyncio
import socket
import struct
import time
from collections import deque
from dataclasses import dataclass

NTP_EPOCH_OFFSET = 2_208_988_800  # seconds between 1900-01-01 and 1970-01-01
_NTP_PACKET = struct.Struct("!B B B b 11I")


def wall_ns() -> int:
    return time.time_ns()


def mono_ns() -> int:
    return time.perf_counter_ns()


@dataclass(slots=True, frozen=True)
class OffsetSample:
    reference: str
    ts_ns: int  # local wall time of the measurement
    offset_ns: int  # reference clock minus local clock
    delay_ns: int  # round-trip delay; |error of offset_ns| <= delay_ns / 2

    @property
    def uncertainty_ns(self) -> int:
        return self.delay_ns // 2


def ns_to_ntp(ns: int) -> tuple[int, int]:
    seconds, rem = divmod(ns, 1_000_000_000)
    return seconds + NTP_EPOCH_OFFSET, (rem << 32) // 1_000_000_000


def ntp_to_ns(seconds: int, fraction: int) -> int:
    return (seconds - NTP_EPOCH_OFFSET) * 1_000_000_000 + ((fraction * 1_000_000_000) >> 32)


def build_sntp_request(t0_ns: int) -> bytes:
    sec, frac = ns_to_ntp(t0_ns)
    # LI = 0, version = 4, mode = 3 (client); transmit timestamp = t0
    return _NTP_PACKET.pack(0x23, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, sec, frac)


def parse_sntp_response(data: bytes, t0_ns: int, t3_ns: int, reference: str) -> OffsetSample:
    if len(data) < 48:
        raise ValueError("short NTP packet")
    fields = _NTP_PACKET.unpack(data[:48])
    mode = fields[0] & 0x7
    stratum = fields[1]
    if mode != 4 or stratum == 0:  # must be a server reply; stratum 0 is a kiss-of-death
        raise ValueError(f"unusable NTP reply (mode={mode}, stratum={stratum})")
    words = fields[4:]
    originate = ntp_to_ns(words[5], words[6])
    if abs(originate - t0_ns) > 1_000:  # reply does not match our request
        raise ValueError("NTP originate timestamp mismatch")
    t1 = ntp_to_ns(words[7], words[8])  # server receive
    t2 = ntp_to_ns(words[9], words[10])  # server transmit
    offset = ((t1 - t0_ns) + (t2 - t3_ns)) // 2
    delay = (t3_ns - t0_ns) - (t2 - t1)
    return OffsetSample(reference, t3_ns, offset, max(delay, 0))


def cristian_sample(reference: str, t0_ns: int, t3_ns: int, server_ns: int) -> OffsetSample:
    """Offset from a single server timestamp taken somewhere between t0 and t3."""
    return OffsetSample(reference, t3_ns, server_ns - (t0_ns + t3_ns) // 2, t3_ns - t0_ns)


class _SntpProtocol(asyncio.DatagramProtocol):
    def __init__(self) -> None:
        self.reply: asyncio.Future[tuple[bytes, int]] = asyncio.get_running_loop().create_future()

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        if not self.reply.done():
            self.reply.set_result((data, wall_ns()))

    def error_received(self, exc: Exception) -> None:
        if not self.reply.done():
            self.reply.set_exception(exc)


async def sntp_query(host: str, timeout_s: float = 2.0, port: int = 123) -> OffsetSample:
    loop = asyncio.get_running_loop()
    infos = await loop.getaddrinfo(host, port, family=socket.AF_INET, type=socket.SOCK_DGRAM)
    addr = (str(infos[0][4][0]), int(infos[0][4][1]))
    transport, protocol = await loop.create_datagram_endpoint(_SntpProtocol, remote_addr=addr)
    try:
        t0 = wall_ns()
        transport.sendto(build_sntp_request(t0))
        data, t3 = await asyncio.wait_for(protocol.reply, timeout_s)
        return parse_sntp_response(data, t0, t3, host)
    finally:
        transport.close()


class ClockModel:
    """Current estimate of (reference - local) offset.

    Like the NTP clock filter, it keeps the last samples and trusts the one with the
    smallest round-trip delay, since its error bound is the tightest.
    """

    def __init__(self, window: int = 8) -> None:
        self._samples: deque[OffsetSample] = deque(maxlen=window)

    def add(self, sample: OffsetSample) -> None:
        self._samples.append(sample)

    @property
    def best(self) -> OffsetSample | None:
        return min(self._samples, key=lambda s: s.delay_ns, default=None)

    @property
    def offset_ns(self) -> int:
        best = self.best
        return best.offset_ns if best else 0

    def corrected_latency_ns(self, recv_ts_ns: int, exchange_ts_ns: int) -> int:
        """Latency with the local timestamp expressed on the reference time scale."""
        return recv_ts_ns + self.offset_ns - exchange_ts_ns
