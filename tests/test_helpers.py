"""Tests for tick(), merge() and the directional channel views."""

import pytest

from pyroutine import (
    Chan,
    ChanClosed,
    RecvChan,
    SendChan,
    go,
    merge,
    recv_case,
    select,
    send_case,
    tick,
)


# --------------------------------------------------------------------- #
# tick
# --------------------------------------------------------------------- #


def test_tick_delivers_repeatedly_then_stops_on_close():
    beat = tick(0.03)
    t0 = beat.recv(timeout=5)
    t1 = beat.recv(timeout=5)
    t2 = beat.recv(timeout=5)
    assert t0 <= t1 <= t2
    beat.close()
    with pytest.raises(ChanClosed):
        # at most one tick is buffered, so the second recv must raise
        beat.recv(timeout=1)
        beat.recv(timeout=1)


def test_tick_rejects_nonpositive_interval():
    with pytest.raises(ValueError):
        tick(0)


# --------------------------------------------------------------------- #
# merge
# --------------------------------------------------------------------- #


def test_merge_carries_everything_then_closes():
    a, b, c = Chan(), Chan(4), Chan()
    out = merge(a, b, c)

    def feed(ch, lo, hi):
        for n in range(lo, hi):
            ch.send(n)
        ch.close()

    go(feed, a, 0, 10)
    go(feed, b, 10, 20)
    go(feed, c, 20, 30)

    got = sorted(out)  # iterates until out closes
    assert got == list(range(30))
    assert out.closed


def test_merge_accepts_recv_only_views():
    a, b = Chan(2), Chan(2)
    out = merge(a.recv_only(), b.recv_only())
    a.send(1)
    b.send(2)
    a.close()
    b.close()
    assert sorted(out) == [1, 2]


def test_merge_needs_at_least_one_channel():
    with pytest.raises(ValueError):
        merge()


# --------------------------------------------------------------------- #
# directional views
# --------------------------------------------------------------------- #


def test_recv_only_view():
    ch = Chan(2)
    r = ch.recv_only()
    assert isinstance(r, RecvChan)
    ch.send(1)
    assert r.recv(timeout=5) == 1
    assert r.try_recv() == (None, False)
    assert not hasattr(r, "send")
    assert not hasattr(r, "close")
    ch.send(2)
    ch.close()
    assert list(r) == [2]
    assert r.closed
    assert r.cap == 2
    assert len(r) == 0


def test_send_only_view():
    ch = Chan(2)
    s = ch.send_only()
    assert isinstance(s, SendChan)
    s.send(1)
    assert s.try_send(2)
    assert not s.try_send(3)  # buffer full
    assert not hasattr(s, "recv")
    assert len(s) == 2
    assert s.cap == 2
    s.close()
    assert ch.closed and s.closed
    with pytest.raises(ChanClosed):
        s.send(4)


def test_views_work_in_select():
    ch = Chan(1)
    s, r = ch.send_only(), ch.recv_only()
    idx, val = select(send_case(s, 42))
    assert (idx, val) == (0, None)
    idx, val = select(recv_case(r))
    assert (idx, val) == (0, 42)


def test_wrong_direction_rejected_by_select_cases():
    ch = Chan()
    with pytest.raises(TypeError):
        recv_case(ch.send_only())
    with pytest.raises(TypeError):
        send_case(ch.recv_only(), 1)
