"""select() over multiple channel operations, plus the after() timer.

This is the part that makes channels actually pleasant to use. The
implementation follows Go's runtime rather than polling:

1. Take the locks of every involved channel in a canonical order
   (sorted by id), which makes multi lock acquisition deadlock free.
2. With everything frozen, check the cases in random order. If one can
   proceed, do it right there and return.
3. Otherwise park ONE shared waiter on every channel, release the
   locks, and sleep until some other thread completes exactly one case.
4. On wake, pull our leftover registrations off the other channels.

No busy waiting anywhere.
"""

from __future__ import annotations

import contextlib
import random
import threading
import time
from typing import Any, Optional, Tuple, TypeVar, Union

from . import _debug
from ._chan import _CLOSED, _TIMEOUT_INDEX, Chan, ChanClosed, RecvChan, SendChan, _Waiter

_RECV = "recv"
_SEND = "send"


T = TypeVar("T")


def recv_case(ch: "Union[Chan[T], RecvChan[T]]") -> tuple:
    """A select case that receives from ch (a Chan or RecvChan)."""
    if isinstance(ch, RecvChan):
        ch = ch._chan
    if not isinstance(ch, Chan):
        raise TypeError("recv_case() expects a Chan or RecvChan")
    return (_RECV, ch, None)


def send_case(ch: "Union[Chan[T], SendChan[T]]", value: T) -> tuple:
    """A select case that sends value into ch (a Chan or SendChan)."""
    if isinstance(ch, SendChan):
        ch = ch._chan
    if not isinstance(ch, Chan):
        raise TypeError("send_case() expects a Chan or SendChan")
    return (_SEND, ch, value)


def _validate_cases(cases: tuple) -> None:
    if not cases:
        raise ValueError("select() needs at least one case")
    for c in cases:
        if not (isinstance(c, tuple) and len(c) == 3 and c[0] in (_RECV, _SEND)):
            raise TypeError("cases must be built with recv_case() or send_case()")


def _ordered_chans_for(cases: tuple) -> Tuple[Chan, ...]:
    # every channel exactly once, locked in a canonical global order
    chan_by_id = {id(c[1]): c[1] for c in cases}
    return tuple(chan_by_id[k] for k in sorted(chan_by_id))


def select(
    *cases: tuple,
    default: bool = False,
    timeout: Optional[float] = None,
) -> Tuple[int, Any]:
    """Wait until one of the cases can proceed, then perform it.

    Returns (index, value). For recv cases value is the received item,
    for send cases it is None. With default=True, returns (-1, None)
    immediately when nothing is ready. With a timeout, raises
    TimeoutError if nothing fires in time.

    Raises ChanClosed (with .index set) if the winning case hit a
    closed channel.

    Selecting over the same channels in a loop? Build a Select once and
    call wait() on it, that skips this function's per call setup.
    """
    _validate_cases(cases)
    return _perform_select(
        cases, _ordered_chans_for(cases), list(range(len(cases))), default, timeout
    )


class Select:
    """A prepared select() over a fixed set of cases.

    select() validates its cases, deduplicates the channels and sorts
    them into the canonical locking order on every call. A Select does
    that work once, so loops that keep selecting over the same channels
    skip the setup:

        sel = Select(recv_case(inbox), recv_case(control))
        while True:
            idx, val = sel.wait()
            ...

    wait() has exactly select()'s semantics, including default and
    timeout. NOT thread safe: a Select belongs to one routine, exactly
    like a select statement belongs to one goroutine. Two routines may
    each build their own Select over the same channels.
    """

    __slots__ = ("_cases", "_ordered_chans", "_order")

    def __init__(self, *cases: tuple):
        _validate_cases(cases)
        self._cases = cases
        self._ordered_chans = _ordered_chans_for(cases)
        self._order = list(range(len(cases)))

    def wait(
        self, timeout: Optional[float] = None, default: bool = False
    ) -> Tuple[int, Any]:
        """Perform the select. Same return values and exceptions as
        select()."""
        return _perform_select(
            self._cases, self._ordered_chans, self._order, default, timeout
        )

    def __repr__(self) -> str:
        return f"<Select {len(self._cases)} cases over {len(self._ordered_chans)} chans>"


def _perform_select(
    cases: tuple,
    ordered_chans: "Tuple[Chan, ...]",
    order: list,
    default: bool,
    timeout: Optional[float],
) -> Tuple[int, Any]:
    """The select algorithm. `order` is shuffled in place, which is why
    a Select instance is single routine only."""
    if len(cases) == 1 and not default:
        # fast path: a one case select without default is exactly the
        # plain channel operation, minus the multi channel machinery
        kind, ch, val = cases[0]
        try:
            if kind == _RECV:
                return 0, ch.recv(timeout)
            ch.send(val, timeout)
            return 0, None
        except ChanClosed as e:
            e.index = 0
            raise

    random.shuffle(order)  # same pseudo random fairness Go gives you

    # opportunistic pass: try each case holding only its own channel
    # lock. Under load a select usually finds a ready case here, paying
    # one lock instead of len(chans). Semantically identical to
    # performing that case, and rule 1 (one channel lock at a time)
    # holds throughout this pass.
    for i in order:
        kind, ch, val = cases[i]
        try:
            if kind == _RECV:
                value, ok = ch.try_recv()
                if ok:
                    return i, value
            else:
                if ch.try_send(val):
                    return i, None
        except ChanClosed as e:
            e.index = i
            raise
    if default:
        return -1, None

    waiter: Optional[_Waiter] = None
    for ch in ordered_chans:
        ch._lock.acquire()
    try:
        # atomic re-check under every lock: something may have become
        # ready between the opportunistic pass and here, and parking is
        # only correct if nothing is ready while we hold all the locks
        for i in order:
            kind, ch, val = cases[i]
            try:
                if kind == _RECV:
                    value, ok = ch._poll_recv_locked()
                    if ok:
                        return i, value
                else:
                    if ch._poll_send_locked(val):
                        return i, None
            except ChanClosed as e:
                e.index = i
                raise
        # nothing ready, park one shared waiter everywhere
        waiter = _Waiter()
        for i in range(len(cases)):
            kind, ch, val = cases[i]
            if kind == _RECV:
                ch._recv_waiters.append((waiter, i))
            else:
                ch._send_waiters.append((waiter, i, val))
    finally:
        for ch in reversed(ordered_chans):
            ch._lock.release()

    # sleep until someone completes one of our cases
    if not waiter.wait(timeout):
        if waiter.commit(_TIMEOUT_INDEX):
            for ch in ordered_chans:
                ch._unregister(waiter)
            raise TimeoutError("select timed out")
        # lost the race right at the deadline, a result is incoming
        waiter.wait()

    # clean our leftover registrations off the losing channels
    for ch in ordered_chans:
        ch._unregister(waiter)

    idx = waiter.index
    assert idx is not None  # finish() only runs after a commit set it
    if waiter.value is _CLOSED:
        raise ChanClosed("select hit a closed channel", index=idx)
    if cases[idx][0] == _RECV:
        return idx, waiter.value
    return idx, None


class Timer:
    """A stoppable one shot timer, like Go's time.Timer.

    `Timer(seconds).chan` receives the current monotonic time once when
    the timer fires, then closes. stop() cancels a timer that has not
    fired and closes the channel, so anything parked on it wakes up
    with ChanClosed instead of waiting out a deadline nobody needs
    anymore. after(s) is shorthand for Timer(s).chan for the cases
    where you never stop it.

        t = Timer(5.0)
        try:
            idx, val = select(recv_case(work), recv_case(t.chan))
        finally:
            t.stop()
    """

    __slots__ = ("chan", "_timer", "_lock", "_fired")

    def __init__(self, seconds: float):
        if seconds < 0:
            raise ValueError("Timer needs a non negative delay")
        self.chan: Chan = Chan(1)
        self._lock = threading.Lock()
        self._fired = False
        self._timer = threading.Timer(seconds, self._fire)
        self._timer.daemon = True
        _debug.timer_started()
        self._timer.start()

    def _fire(self) -> None:
        with self._lock:
            first = not self._fired
            self._fired = True
        if not first:
            return  # stop() got there at the last instant
        _debug.timer_finished()
        with contextlib.suppress(ChanClosed):
            self.chan.try_send(time.monotonic())
        self.chan.close()

    def stop(self) -> bool:
        """Cancel the timer. True if it was stopped before firing, in
        which case the channel closes without ever carrying a value.
        Safe to call more than once, and after the timer fired."""
        self._timer.cancel()
        with self._lock:
            stopped = not self._fired
            self._fired = True
        if stopped:
            _debug.timer_finished()
            self.chan.close()
        return stopped

    def __repr__(self) -> str:
        state = "fired-or-stopped" if self._fired else "pending"
        return f"<Timer {state}>"


def after(seconds: float) -> Chan:
    """Returns a channel that receives the current monotonic time once,
    after the given delay, then closes. Handy as a select case:

        idx, _ = select(recv_case(work), recv_case(after(1.0)))

    Use Timer if you might need to cancel it before it fires.
    """
    return Timer(seconds).chan


def tick(seconds: float) -> Chan:
    """A channel that receives the current monotonic time roughly every
    `seconds`, like Go's time.Tick. The channel has a buffer of one, so
    a slow receiver simply misses ticks, they never queue up.

    Close the returned channel to stop the ticker. Until then it costs
    one pending timer, nothing more:

        beat = tick(1.0)
        for t in beat:
            heartbeat()
            if shutting_down:
                beat.close()
    """
    if seconds <= 0:
        raise ValueError("tick() needs a positive interval")
    ch: Chan = Chan(1)

    def fire() -> None:
        _debug.timer_finished()
        try:
            ch.try_send(time.monotonic())
        except ChanClosed:
            return  # the user closed the channel, stop rescheduling
        t = threading.Timer(seconds, fire)
        t.daemon = True
        _debug.timer_started()
        t.start()

    t = threading.Timer(seconds, fire)
    t.daemon = True
    _debug.timer_started()
    t.start()
    return ch
