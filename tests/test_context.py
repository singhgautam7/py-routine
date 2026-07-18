"""Tests for the context package: cancellation, deadlines, propagation."""

import time

import pytest

from pyroutine import (
    Canceled,
    Chan,
    ChanClosed,
    DeadlineExceeded,
    background,
    go,
    recv_case,
    select,
    with_cancel,
    with_deadline,
    with_timeout,
)


def test_background_never_cancelled():
    ctx = background()
    assert ctx.err() is None
    assert ctx.deadline() is None
    assert not ctx.cancelled()
    assert background() is ctx  # shared root


def test_cancel_sets_err_and_closes_done():
    ctx, cancel = with_cancel()
    assert ctx.err() is None
    cancel()
    assert isinstance(ctx.err(), Canceled)
    assert ctx.cancelled()
    with pytest.raises(ChanClosed):
        ctx.done().recv()
    cancel()  # idempotent
    assert isinstance(ctx.err(), Canceled)


def test_select_unblocks_on_cancel():
    ctx, cancel = with_cancel()
    work = Chan()

    def waiter():
        try:
            select(recv_case(work), recv_case(ctx.done()))
        except ChanClosed as e:
            return e.index

    h = go(waiter)
    time.sleep(0.05)  # let the routine park
    cancel()
    assert h.result(timeout=5) == 1


def test_parent_cancel_reaches_grandchildren():
    parent, pcancel = with_cancel()
    child, _ = with_cancel(parent)
    grand, _ = with_cancel(child)
    pcancel()
    assert isinstance(grand.err(), Canceled)
    with pytest.raises(ChanClosed):
        grand.done().recv(timeout=5)


def test_child_cancel_leaves_parent_alone():
    parent, _ = with_cancel()
    child, ccancel = with_cancel(parent)
    ccancel()
    assert child.err() is not None
    assert parent.err() is None


def test_child_of_cancelled_parent_starts_cancelled():
    parent, pcancel = with_cancel()
    pcancel()
    child, _ = with_cancel(parent)
    assert isinstance(child.err(), Canceled)


def test_timeout_fires_deadline_exceeded():
    ctx, cancel = with_timeout(0.05)
    with pytest.raises(ChanClosed):
        ctx.done().recv(timeout=5)
    err = ctx.err()
    assert isinstance(err, DeadlineExceeded)
    assert isinstance(err, Canceled)
    assert isinstance(err, TimeoutError)
    cancel()  # after the fact, still fine


def test_explicit_cancel_beats_the_timer():
    ctx, cancel = with_timeout(30)
    cancel()
    err = ctx.err()
    assert isinstance(err, Canceled)
    assert not isinstance(err, DeadlineExceeded)


def test_deadline_capped_to_parent():
    parent, _ = with_timeout(0.05)
    child, _ = with_timeout(60, parent)
    assert child.deadline() is not None
    assert child.deadline() <= parent.deadline()
    with pytest.raises(ChanClosed):
        child.done().recv(timeout=5)
    assert isinstance(child.err(), DeadlineExceeded)


def test_child_inherits_parent_deadline_view():
    parent, pcancel = with_timeout(60)
    child, _ = with_cancel(parent)
    assert child.deadline() == parent.deadline()
    pcancel()


def test_with_deadline_already_past():
    ctx, _ = with_deadline(time.monotonic() - 1)
    assert isinstance(ctx.err(), DeadlineExceeded)
