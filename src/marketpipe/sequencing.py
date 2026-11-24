"""Sequence tracking: turns venue sequence numbers into an exact count of lost messages.

Every venue used here numbers its trades with an integer id that increases by exactly one
per trade and per instrument. A websocket runs over TCP, so within one connection nothing
can be reordered or silently dropped by the network; ids can still go missing when the
venue drops messages for a slow consumer, and they always go missing while we are
disconnected. Comparing each id with the previous one tells us exactly which ids we have
not seen, which is what makes "messages lost" a measured quantity rather than a guess.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class SeqStatus(Enum):
    FIRST = "first"  # no reference yet
    IN_ORDER = "in_order"  # exactly last + 1
    GAP = "gap"  # ids were skipped
    DUPLICATE = "duplicate"  # already seen (e.g. replay after a reconnect)


@dataclass(slots=True, frozen=True)
class SeqResult:
    status: SeqStatus
    first_missing: int = 0
    last_missing: int = 0

    @property
    def missing(self) -> int:
        return self.last_missing - self.first_missing + 1 if self.status is SeqStatus.GAP else 0


class SequenceTracker:
    """Tracks one strictly increasing, unit-step sequence (trade ids of one instrument)."""

    __slots__ = ("duplicates", "gaps", "last", "missing", "seen")

    def __init__(self, last: int | None = None) -> None:
        self.last = last  # highest id seen so far (may be seeded from the database)
        self.seen = 0
        self.duplicates = 0
        self.gaps = 0
        self.missing = 0

    def observe(self, seq: int) -> SeqResult:
        self.seen += 1
        last = self.last
        if last is None:
            self.last = seq
            return SeqResult(SeqStatus.FIRST)
        if seq == last + 1:
            self.last = seq
            return SeqResult(SeqStatus.IN_ORDER)
        if seq <= last:
            self.duplicates += 1
            return SeqResult(SeqStatus.DUPLICATE)
        self.last = seq
        self.gaps += 1
        self.missing += seq - last - 1
        return SeqResult(SeqStatus.GAP, last + 1, seq - 1)

    def observe_heartbeat(self, last_id: int) -> SeqResult:
        """A heartbeat announcing the venue's latest id reveals trades we never received."""
        if self.last is None or last_id <= self.last:
            return SeqResult(SeqStatus.IN_ORDER)
        first = self.last + 1
        self.last = last_id
        self.gaps += 1
        self.missing += last_id - first + 1
        return SeqResult(SeqStatus.GAP, first, last_id)


class RangeTracker:
    """Tracks batched updates covering id ranges [first, last], e.g. order book diffs.

    Consecutive updates must chain: next.first == previous.last + 1.
    """

    __slots__ = ("gaps", "last", "missing")

    def __init__(self, last: int | None = None) -> None:
        self.last = last
        self.gaps = 0
        self.missing = 0

    def observe(self, first: int, last: int) -> SeqResult:
        if first > last:
            raise ValueError(f"invalid range [{first}, {last}]")
        prev = self.last
        if prev is None:
            self.last = last
            return SeqResult(SeqStatus.FIRST)
        if last <= prev:
            return SeqResult(SeqStatus.DUPLICATE)
        self.last = last
        if first <= prev + 1:
            return SeqResult(SeqStatus.IN_ORDER)
        self.gaps += 1
        self.missing += first - prev - 1
        return SeqResult(SeqStatus.GAP, prev + 1, first - 1)
