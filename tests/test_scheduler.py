"""Tests for the worker reusing scheduler behind go()."""

import threading
import time

import pytest

from pyroutine import Chan, ChanClosed, WaitGroup, _routines, go


def test_workers_are_reused_for_sequential_spawns():
    before = threading.active_count()
    for _ in range(100):
        go(int).result(timeout=5)
    # a fresh thread per routine would have left a much bigger footprint;
    # sequential spawns should be served by a handful of pooled workers
    after = threading.active_count()
    assert after - before < 20


def test_burst_of_blocked_routines_all_get_threads():
    """The liveness property: routines that block must never prevent new
    routines from running. The pool has to grow without bound."""
    n = 100
    gate = Chan()
    started = WaitGroup()
    for _ in range(n):
        started.add(1)

    def blocker():
        started.done()
        gate.recv()  # park until released

    handles = [go(blocker) for _ in range(n)]
    # every single one must reach its blocking point
    assert started.wait(timeout=30)
    # and a routine spawned while all of them block must still run
    assert go(lambda: "alive").result(timeout=5) == "alive"
    gate.close()
    for h in handles:
        with pytest.raises(ChanClosed):
            h.result(timeout=10)  # gate.recv() died when the gate closed


def test_exceptions_still_reraised_from_pooled_workers():
    def boom():
        raise ValueError("pooled boom")

    h = go(boom)
    with pytest.raises(ValueError, match="pooled boom"):
        h.result(timeout=5)
    # the worker survives the failure and serves the next routine
    assert go(lambda: 7).result(timeout=5) == 7


def test_thread_named_after_routine_while_running():
    def report_name():
        return threading.current_thread().name

    name = go(report_name).result(timeout=5)
    assert name.startswith("pyroutine-")
    assert "report_name" in name


def test_thousands_of_blocked_routines_with_small_stacks():
    """M:N step 2: parked routines are affordable in bulk when worker
    stacks are small. 2000 routines block simultaneously, and the
    scheduler must stay fully live for newcomers throughout."""
    import contextlib

    from pyroutine import ChanClosed, set_worker_stack_size

    try:
        set_worker_stack_size(512 * 1024)
    except ValueError:
        pytest.skip("platform rejects custom thread stack sizes")
    try:
        n = 2000
        gate = Chan()
        started = WaitGroup()
        for _ in range(n):
            started.add(1)

        def blocker():
            started.done()
            with contextlib.suppress(ChanClosed):
                gate.recv()

        handles = [go(blocker) for _ in range(n)]
        assert started.wait(timeout=60)  # every routine reached its park
        assert go(lambda: "alive").result(timeout=10) == "alive"
        gate.close()
        for h in handles:
            assert h.join(timeout=30)
    finally:
        set_worker_stack_size(0)


def test_set_worker_stack_size_validates():
    from pyroutine import set_worker_stack_size

    with pytest.raises((ValueError, TypeError)):
        set_worker_stack_size(1)  # far below any platform minimum
    set_worker_stack_size(0)  # default restore always works


def test_idle_workers_retire(monkeypatch):
    monkeypatch.setattr(_routines, "_IDLE_TIMEOUT", 0.05)
    handles = [go(int) for _ in range(10)]
    for h in handles:
        h.result(timeout=5)
    # workers should notice the short idle timeout and exit. Workers that
    # parked before the patch still wait out the original 5s, so the
    # deadline stays comfortably above that.
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        if not _routines._scheduler._idle:
            break
        time.sleep(0.05)
    assert not _routines._scheduler._idle
