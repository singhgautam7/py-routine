"""Tests for the unretrieved exception hook and deadlock detection."""

import contextlib
import gc
import time
import warnings

import pytest

from pyroutine import (
    Chan,
    ChanClosed,
    DeadlockWarning,
    Timer,
    disable_deadlock_detection,
    enable_deadlock_detection,
    go,
    set_excepthook,
)

# --------------------------------------------------------------------- #
# unretrieved exception reporting
# --------------------------------------------------------------------- #


def _boom():
    raise ValueError("lost failure")


def test_unretrieved_exception_reaches_hook():
    seen = []
    set_excepthook(lambda h, e: seen.append((h, e)))
    try:
        h = go(_boom)
        h.join(timeout=5)  # join alone does NOT collect the exception
        del h
        gc.collect()
        assert len(seen) == 1
        assert isinstance(seen[0][1], ValueError)
        assert "boom" in repr(seen[0][0])
    finally:
        set_excepthook(None)


def test_retrieved_exception_is_not_reported():
    seen = []
    set_excepthook(lambda h, e: seen.append(e))
    try:
        h = go(_boom)
        with pytest.raises(ValueError):
            h.result(timeout=5)
        del h
        gc.collect()
        assert seen == []
    finally:
        set_excepthook(None)


def test_successful_routine_is_not_reported():
    seen = []
    set_excepthook(lambda h, e: seen.append(e))
    try:
        h = go(lambda: 42)
        h.join(timeout=5)
        del h
        gc.collect()
        assert seen == []
    finally:
        set_excepthook(None)


def test_default_excepthook_prints_to_stderr(capsys):
    h = go(_boom)
    h.join(timeout=5)
    del h
    gc.collect()
    err = capsys.readouterr().err
    assert "was never retrieved" in err
    assert "ValueError: lost failure" in err


def test_errgroup_handles_are_not_double_reported():
    from pyroutine import ErrGroup

    seen = []
    set_excepthook(lambda h, e: seen.append(e))
    try:
        eg = ErrGroup()
        eg.go(_boom)
        with pytest.raises(ValueError):
            eg.wait(timeout=30)
        gc.collect()
        assert seen == []  # the group already delivered the error
    finally:
        set_excepthook(None)


def test_set_excepthook_validates():
    with pytest.raises(TypeError):
        set_excepthook(42)


# --------------------------------------------------------------------- #
# deadlock detection
# --------------------------------------------------------------------- #


def _wait_for_warning(records, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if any(issubclass(r.category, DeadlockWarning) for r in records):
            return True
        time.sleep(0.05)
    return False


def test_deadlock_between_routines_is_reported():
    a, b = Chan(), Chan()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        enable_deadlock_detection(interval=0.05)
        try:
            h1 = go(a.recv)  # each waits on a channel nobody sends to
            h2 = go(b.recv)
            assert _wait_for_warning(caught)
        finally:
            disable_deadlock_detection()
            a.close()
            b.close()
    for h in (h1, h2):
        with pytest.raises(ChanClosed):
            h.result(timeout=10)
    msg = str(
        next(w.message for w in caught if issubclass(w.category, DeadlockWarning))
    )
    assert "blocked" in msg
    assert "main thread is still runnable" in msg


def test_no_warning_while_a_timer_is_pending():
    t = Timer(30)  # a pending timer can wake things, never report
    ch = Chan()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        enable_deadlock_detection(interval=0.05)
        try:
            h = go(ch.recv)
            assert not _wait_for_warning(caught, timeout=0.5)
        finally:
            disable_deadlock_detection()
            t.stop()
            ch.close()
    with pytest.raises(ChanClosed):
        h.result(timeout=10)


def test_no_warning_for_timed_waits():
    ch = Chan()

    def patient():
        # ChanClosed arrives when the test cleans up by closing ch
        with contextlib.suppress(TimeoutError, ChanClosed):
            ch.recv(timeout=30)  # wakes itself, not a deadlock

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        enable_deadlock_detection(interval=0.05)
        try:
            h = go(patient)
            assert not _wait_for_warning(caught, timeout=0.5)
        finally:
            disable_deadlock_detection()
            ch.close()
    h.result(timeout=10)


def test_enable_disable_idempotent_and_validated():
    with pytest.raises(ValueError):
        enable_deadlock_detection(interval=0)
    enable_deadlock_detection(interval=1)
    enable_deadlock_detection(interval=1)  # second call is a no-op
    disable_deadlock_detection()
    disable_deadlock_detection()
