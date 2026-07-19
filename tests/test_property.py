"""Property based tests (hypothesis) for the channel core.

Three angles the example based tests cannot sweep:

1. A buffered channel's non blocking API must match a trivial deque
   model over any operation sequence.
2. Conservation: with arbitrary producer/consumer/buffer shapes, every
   sent value is received exactly once, nothing lost, nothing doubled.
3. select over several channels delivers exactly the union of what the
   producers sent, for any distribution of values over channels.
"""

import contextlib
from collections import Counter, deque

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from pyroutine import Chan, ChanClosed, WaitGroup, go, merge

# --------------------------------------------------------------------- #
# 1. model check of the non blocking API
# --------------------------------------------------------------------- #

_ops = st.lists(
    st.one_of(
        st.tuples(st.just("send"), st.integers(0, 999)),
        st.tuples(st.just("recv"), st.none()),
        st.tuples(st.just("close"), st.none()),
    ),
    max_size=60,
)


@settings(max_examples=60, deadline=None)
@given(cap=st.integers(0, 5), ops=_ops)
def test_chan_matches_deque_model(cap, ops):
    ch = Chan(cap)
    model = deque()
    closed = False
    for op, arg in ops:
        if op == "send":
            if closed:
                with pytest.raises(ChanClosed):
                    ch.try_send(arg)
            else:
                ok = ch.try_send(arg)
                # single threaded: no receiver is parked, so a try_send
                # succeeds exactly when a buffer slot is free
                assert ok == (cap > 0 and len(model) < cap)
                if ok:
                    model.append(arg)
        elif op == "recv":
            if model:
                value, ok = ch.try_recv()
                assert ok and value == model.popleft()
            elif closed:
                with pytest.raises(ChanClosed):
                    ch.try_recv()
            else:
                assert ch.try_recv() == (None, False)
        else:
            ch.close()
            closed = True
        assert len(ch) == len(model)
        assert ch.closed == closed
        assert ch.cap == cap


# --------------------------------------------------------------------- #
# 2. conservation across arbitrary pipeline shapes
# --------------------------------------------------------------------- #


@settings(max_examples=15, deadline=None)
@given(
    producers=st.integers(1, 3),
    consumers=st.integers(1, 3),
    per_producer=st.integers(0, 30),
    cap=st.integers(0, 4),
)
def test_every_value_received_exactly_once(producers, consumers, per_producer, cap):
    ch = Chan(cap)
    out = Chan(64)
    wg = WaitGroup()
    consumers_wg = WaitGroup()

    def producer(pid):
        for i in range(per_producer):
            ch.send((pid, i))

    def consumer():
        for value in ch:
            out.send(value)

    for pid in range(producers):
        wg.go(producer, pid)
    for _ in range(consumers):
        consumers_wg.go(consumer)
    go(lambda: (wg.wait(), ch.close()))
    go(lambda: (consumers_wg.wait(), out.close()))

    got = Counter(out)
    expected = Counter(
        (pid, i) for pid in range(producers) for i in range(per_producer)
    )
    assert got == expected


# --------------------------------------------------------------------- #
# 3. select drains exactly the union of all channels
# --------------------------------------------------------------------- #


@settings(max_examples=15, deadline=None)
@given(
    routing=st.lists(st.integers(0, 3), max_size=40),
)
def test_select_receives_exactly_the_union(routing):
    n_chans = 4
    chans = [Chan(2) for _ in range(n_chans)]

    def producer(idx):
        for pos, target in enumerate(routing):
            if target == idx:
                chans[idx].send((idx, pos))
        chans[idx].close()

    for idx in range(n_chans):
        go(producer, idx)

    # merge is select in a loop with the drain-and-drop pattern
    got = sorted(merge(*chans))
    expected = sorted((target, pos) for pos, target in enumerate(routing))
    assert got == expected


def test_property_suite_cleanup():
    """Give lingering helper routines a beat to finish so later tests
    see a quiet scheduler. Not a property, just hygiene."""
    with contextlib.suppress(Exception):
        go(lambda: None).result(timeout=5)
