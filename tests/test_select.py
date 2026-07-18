import threading
import time

import pytest

from pyroutine import Chan, ChanClosed, after, go, recv_case, select, send_case


def test_select_picks_the_ready_case():
    a, b = Chan(1), Chan(1)
    b.send("from b")
    idx, val = select(recv_case(a), recv_case(b))
    assert (idx, val) == (1, "from b")


def test_select_default_when_nothing_ready():
    a, b = Chan(), Chan()
    idx, val = select(recv_case(a), recv_case(b), default=True)
    assert (idx, val) == (-1, None)


def test_select_blocks_then_wakes_on_send():
    a, b = Chan(), Chan()
    out = Chan(1)

    def selector():
        out.send(select(recv_case(a), recv_case(b)))

    go(selector)
    time.sleep(0.1)  # let it park properly, no polling should be happening
    b.send(99)
    assert out.recv(timeout=2.0) == (1, 99)


def test_select_send_case_fires_when_receiver_arrives():
    ch = Chan()
    out = Chan(1)

    def selector():
        out.send(select(send_case(ch, "payload")))

    go(selector)
    time.sleep(0.1)
    assert ch.recv(timeout=2.0) == "payload"
    assert out.recv(timeout=2.0) == (0, None)


def test_select_timeout():
    ch = Chan()
    t0 = time.monotonic()
    with pytest.raises(TimeoutError):
        select(recv_case(ch), timeout=0.15)
    assert time.monotonic() - t0 < 2.0
    # channel must be left clean, a later pairing still works
    go(ch.send, 7)
    assert ch.recv(timeout=2.0) == 7


def test_select_raises_chan_closed_with_index():
    a = Chan()
    b = Chan()
    b.close()
    with pytest.raises(ChanClosed) as ei:
        select(recv_case(a), recv_case(b))
    assert ei.value.index == 1


def test_select_wakes_on_close():
    ch = Chan()
    out = Chan(1)

    def selector():
        try:
            select(recv_case(ch))
        except ChanClosed as e:
            out.send(("closed", e.index))

    go(selector)
    time.sleep(0.1)
    ch.close()
    assert out.recv(timeout=2.0) == ("closed", 0)


def test_only_one_select_wins_a_single_value():
    ch = Chan()
    hits = Chan(8)

    def selector():
        try:
            idx, val = select(recv_case(ch), timeout=1.0)
            hits.send(("got", val))
        except TimeoutError:
            hits.send(("timeout", None))

    for _ in range(4):
        go(selector)
    time.sleep(0.1)
    ch.send("only-one")

    results = [hits.recv(timeout=3.0) for _ in range(4)]
    assert results.count(("got", "only-one")) == 1
    assert results.count(("timeout", None)) == 3


def test_select_same_channel_twice_is_fine():
    ch = Chan(1)
    ch.send(5)
    idx, val = select(recv_case(ch), recv_case(ch))
    assert val == 5
    assert idx in (0, 1)


def test_after_fires_roughly_on_time():
    t0 = time.monotonic()
    idx, val = select(recv_case(after(0.2)))
    elapsed = time.monotonic() - t0
    assert idx == 0
    assert 0.15 < elapsed < 2.0


def test_after_as_a_timeout_arm():
    work = Chan()
    idx, _ = select(recv_case(work), recv_case(after(0.2)))
    assert idx == 1


def test_select_needs_cases():
    with pytest.raises(ValueError):
        select()
    with pytest.raises(TypeError):
        select(("bogus", None, None))
