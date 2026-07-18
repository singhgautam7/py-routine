"""Tests for Mutex, RWMutex and ErrGroup."""

import time

import pytest

from pyroutine import (
    Chan,
    ChanClosed,
    ErrGroup,
    Mutex,
    RWMutex,
    WaitGroup,
    go,
)


# --------------------------------------------------------------------- #
# Mutex
# --------------------------------------------------------------------- #


def test_mutex_mutual_exclusion():
    m = Mutex()
    counter = 0
    wg = WaitGroup()

    def bump():
        nonlocal counter
        for _ in range(1000):
            with m:
                counter += 1

    for _ in range(8):
        wg.go(bump)
    assert wg.wait(timeout=30)
    assert counter == 8000


def test_mutex_lock_unlock_methods():
    m = Mutex()
    m.lock()
    m.unlock()
    with pytest.raises(RuntimeError):
        m.unlock()


# --------------------------------------------------------------------- #
# RWMutex
# --------------------------------------------------------------------- #


def test_rwmutex_readers_share():
    rw = RWMutex()
    inside = Chan(2)
    release = Chan()

    def reader():
        with rw.read():
            inside.send(True)
            try:
                release.recv()
            except ChanClosed:
                pass

    go(reader)
    go(reader)
    # both readers hold the read lock at the same time
    assert inside.recv(timeout=5)
    assert inside.recv(timeout=5)
    release.close()


def test_rwmutex_writer_excludes_readers():
    rw = RWMutex()
    wrote = []
    done = Chan(1)

    rw.rlock()

    def writer():
        with rw.write():
            wrote.append("w")
        done.send(True)

    go(writer)
    time.sleep(0.05)  # give the writer a chance to (wrongly) get in
    assert wrote == []
    rw.runlock()
    assert done.recv(timeout=5)
    assert wrote == ["w"]


def test_rwmutex_waiting_writer_blocks_new_readers():
    rw = RWMutex()
    rw.rlock()
    got_read = Chan(1)

    h = go(rw.lock)  # writer parks behind the reader
    time.sleep(0.05)

    def late_reader():
        rw.rlock()
        got_read.send(True)
        rw.runlock()

    go(late_reader)
    time.sleep(0.05)
    # the late reader must be blocked behind the waiting writer
    assert got_read.try_recv() == (None, False)

    rw.runlock()
    assert h.join(timeout=5)  # writer got the lock
    rw.unlock()
    assert got_read.recv(timeout=5)  # and only then the late reader


def test_rwmutex_unlock_errors():
    rw = RWMutex()
    with pytest.raises(RuntimeError):
        rw.unlock()
    with pytest.raises(RuntimeError):
        rw.runlock()


# --------------------------------------------------------------------- #
# ErrGroup
# --------------------------------------------------------------------- #


def test_errgroup_success_collects_nothing():
    eg = ErrGroup()
    results = []
    eg.go(results.append, 1)
    eg.go(results.append, 2)
    eg.wait(timeout=30)
    assert sorted(results) == [1, 2]
    # wait() cancels the group context on the way out, like Go
    assert eg.ctx.err() is not None


def test_errgroup_reraises_first_error_and_cancels_siblings():
    eg = ErrGroup()
    sibling_saw_cancel = Chan(1)

    def failer():
        raise ValueError("boom")

    def sibling():
        try:
            eg.ctx.done().recv(timeout=30)
        except ChanClosed:
            sibling_saw_cancel.send(True)

    eg.go(sibling)
    eg.go(failer)
    with pytest.raises(ValueError, match="boom"):
        eg.wait(timeout=30)
    assert sibling_saw_cancel.recv(timeout=5)


def test_errgroup_handle_still_usable():
    eg = ErrGroup()
    h = eg.go(lambda: 42)
    assert h.result(timeout=5) == 42
    eg.wait(timeout=30)


def test_errgroup_wait_timeout():
    eg = ErrGroup()
    release = Chan()
    eg.go(release.recv)
    with pytest.raises(TimeoutError):
        eg.wait(timeout=0.05)
    release.close()
    with pytest.raises(ChanClosed):
        eg.wait(timeout=30)  # the routine died with ChanClosed
