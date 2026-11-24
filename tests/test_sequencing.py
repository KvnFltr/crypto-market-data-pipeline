import pytest

from marketpipe.sequencing import RangeTracker, SeqStatus, SequenceTracker


def test_contiguous_sequence_has_no_gap() -> None:
    t = SequenceTracker()
    assert t.observe(100).status is SeqStatus.FIRST
    for i in range(101, 200):
        assert t.observe(i).status is SeqStatus.IN_ORDER
    assert (t.gaps, t.missing, t.duplicates, t.seen) == (0, 0, 0, 100)


def test_gap_reports_exact_missing_range() -> None:
    t = SequenceTracker()
    t.observe(10)
    r = t.observe(15)
    assert r.status is SeqStatus.GAP
    assert (r.first_missing, r.last_missing, r.missing) == (11, 14, 4)
    assert t.observe(16).status is SeqStatus.IN_ORDER
    assert (t.gaps, t.missing) == (1, 4)


def test_duplicates_and_late_messages_are_flagged_not_counted_as_gaps() -> None:
    t = SequenceTracker()
    for i in (1, 2, 3):
        t.observe(i)
    assert t.observe(3).status is SeqStatus.DUPLICATE
    assert t.observe(1).status is SeqStatus.DUPLICATE
    assert t.observe(4).status is SeqStatus.IN_ORDER
    assert (t.duplicates, t.gaps, t.last) == (2, 0, 4)


def test_seeded_tracker_detects_gap_across_restart() -> None:
    t = SequenceTracker(last=1_000)  # high-water mark loaded from the database
    r = t.observe(1_250)
    assert r.status is SeqStatus.GAP
    assert (r.first_missing, r.last_missing) == (1_001, 1_249)


def test_heartbeat_reveals_trades_never_received() -> None:
    t = SequenceTracker()
    t.observe(50)
    r = t.observe_heartbeat(53)
    assert r.status is SeqStatus.GAP and (r.first_missing, r.last_missing) == (51, 53)
    # the trades announced by the heartbeat are now behind us
    assert t.observe(52).status is SeqStatus.DUPLICATE
    assert t.observe(54).status is SeqStatus.IN_ORDER
    assert t.observe_heartbeat(54).status is SeqStatus.IN_ORDER


def test_range_tracker_chains_batched_updates() -> None:
    r = RangeTracker()
    assert r.observe(100, 110).status is SeqStatus.FIRST
    assert r.observe(111, 120).status is SeqStatus.IN_ORDER
    assert r.observe(115, 125).status is SeqStatus.IN_ORDER  # overlap is fine
    assert r.observe(100, 120).status is SeqStatus.DUPLICATE
    gap = r.observe(130, 140)
    assert gap.status is SeqStatus.GAP and (gap.first_missing, gap.last_missing) == (126, 129)
    assert (r.gaps, r.missing, r.last) == (1, 4, 140)


def test_range_tracker_rejects_inverted_range() -> None:
    with pytest.raises(ValueError):
        RangeTracker().observe(5, 4)
