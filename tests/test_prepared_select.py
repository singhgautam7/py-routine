"""Tests for the prepared Select object."""

import pytest

from pyroutine import Chan, ChanClosed, Select, go, recv_case, send_case


def test_select_object_reuse_across_many_waits():
    a, b = Chan(4), Chan(4)

    def feed():
        for i in range(50):
            (a if i % 2 else b).send(i)
        a.close()
        b.close()

    go(feed)
    got = []
    live = [a, b]
    sel = Select(*[recv_case(c) for c in live])
    while live:
        try:
            _, val = sel.wait()
            got.append(val)
        except ChanClosed as e:
            # a closed case keeps raising on every wait, so drop it and
            # rebuild, the same pattern a select() loop uses
            del live[e.index]
            if live:
                sel = Select(*[recv_case(c) for c in live])
    assert sorted(got) == list(range(50))


def test_select_object_default_and_timeout():
    ch = Chan()
    sel = Select(recv_case(ch))
    assert sel.wait(default=True) == (-1, None)
    with pytest.raises(TimeoutError):
        sel.wait(timeout=0.05)
    go(lambda: ch.send(9))
    assert sel.wait(timeout=5) == (0, 9)


def test_select_object_send_cases_and_closed_index():
    out = Chan(1)
    sel = Select(send_case(out, "x"), recv_case(Chan()))
    assert sel.wait() == (0, None)
    assert out.recv() == "x"
    out.close()
    with pytest.raises(ChanClosed) as ei:
        sel.wait()
    assert ei.value.index == 0


def test_select_object_validates_cases():
    with pytest.raises(ValueError):
        Select()
    with pytest.raises(TypeError):
        Select(("bogus",))


def test_select_object_repr():
    ch = Chan()
    sel = Select(recv_case(ch), send_case(ch, 1))
    assert "2 cases" in repr(sel)
