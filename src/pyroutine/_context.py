"""Context and cancellation, in the spirit of Go's context package.

A Context carries a cancellation signal (a done channel that closes)
and an optional deadline, nothing else. Routines accept a ctx argument
and either check ctx.err() at convenient points or park on ctx.done()
inside a select:

    ctx, cancel = with_cancel()
    ...
    try:
        idx, val = select(recv_case(jobs), recv_case(ctx.done()))
    except ChanClosed as e:
        if e.index == 1:
            return  # cancelled, unwind

Cancellation flows strictly parent to child. Cancelling a child never
affects its parent. Deadlines are time.monotonic() based and a child
never outlives its parent's deadline.

Locking note: Context locks are private leaf-ish locks. _cancel() never
holds one while closing the done channel or while cancelling children,
so no path holds a context lock and a channel lock at the same time.
"""

from __future__ import annotations

import contextlib
import threading
import time
from typing import Callable, List, Optional, Tuple

from ._chan import Chan


class Canceled(Exception):
    """The context was cancelled via its cancel function, or inherited
    cancellation from a parent."""


class DeadlineExceeded(Canceled, TimeoutError):
    """The context's deadline passed. Also a Canceled (one except clause
    covers both) and a TimeoutError (reads naturally in Python)."""


class Context:
    """The root, never cancelled context. Use background() to get the
    shared instance, and with_cancel()/with_timeout()/with_deadline()
    to derive cancellable children."""

    __slots__ = ("_done",)

    def __init__(self) -> None:
        self._done: Chan = Chan()

    def done(self) -> Chan:
        """A channel that closes when the context is cancelled. Park on
        it with recv_case(ctx.done()); when it closes, the recv raises
        ChanClosed, which is the wake up signal."""
        return self._done

    def err(self) -> Optional[BaseException]:
        """None while live. After cancellation, the Canceled or
        DeadlineExceeded instance explaining why."""
        return None

    def deadline(self) -> Optional[float]:
        """The absolute time.monotonic() moment this context dies, or
        None if it has no deadline."""
        return None

    def cancelled(self) -> bool:
        """Convenience for err() is not None."""
        return self.err() is not None

    def __repr__(self) -> str:
        return "<Context background>"


_BACKGROUND = Context()

CancelFunc = Callable[[], None]


class _CancelContext(Context):
    """A cancellable node in the context tree."""

    __slots__ = ("_lock", "_err", "_children", "_parent", "_timer", "_deadline")

    def __init__(self, parent: Context, deadline: Optional[float] = None):
        Context.__init__(self)
        self._lock = threading.Lock()
        self._err: Optional[BaseException] = None
        self._children: List["_CancelContext"] = []
        self._parent = parent
        self._timer: Optional[threading.Timer] = None
        # a child never outlives its parent's deadline
        parent_deadline = parent.deadline()
        effective: Optional[float]
        if deadline is not None and parent_deadline is not None:
            effective = min(deadline, parent_deadline)
        elif deadline is not None:
            effective = deadline
        else:
            effective = parent_deadline
        self._deadline = effective

        inherited: Optional[BaseException] = None
        if isinstance(parent, _CancelContext):
            with parent._lock:
                if parent._err is None:
                    parent._children.append(self)
                else:
                    inherited = parent._err
        if inherited is not None:
            self._cancel(inherited)
            return

        # only own a timer if this context introduced a deadline itself
        if deadline is not None:
            assert effective is not None  # deadline was given, so min() kept a float
            remaining = effective - time.monotonic()
            if remaining <= 0:
                self._cancel(DeadlineExceeded("context deadline exceeded"))
                return
            t = threading.Timer(remaining, self._on_deadline)
            t.daemon = True
            self._timer = t
            t.start()

    def done(self) -> Chan:
        return self._done

    def err(self) -> Optional[BaseException]:
        with self._lock:
            return self._err

    def deadline(self) -> Optional[float]:
        return self._deadline

    def _on_deadline(self) -> None:
        self._cancel(DeadlineExceeded("context deadline exceeded"))

    def _cancel(self, exc: BaseException) -> None:
        with self._lock:
            if self._err is not None:
                return
            self._err = exc
            children = self._children
            self._children = []
            timer = self._timer
            self._timer = None
        # everything below runs without holding our lock
        if timer is not None:
            timer.cancel()
        self._done.close()
        for child in children:
            child._cancel(exc)
        if isinstance(self._parent, _CancelContext):
            self._parent._remove_child(self)

    def _remove_child(self, child: "_CancelContext") -> None:
        with self._lock, contextlib.suppress(ValueError):
            self._children.remove(child)

    def __repr__(self) -> str:
        err = self.err()
        state = f"cancelled: {err!r}" if err is not None else "live"
        return f"<Context {state}>"


def background() -> Context:
    """The empty root context. Never cancelled, no deadline."""
    return _BACKGROUND


def with_cancel(parent: Optional[Context] = None) -> Tuple[Context, CancelFunc]:
    """A child of parent (default background()) plus the function that
    cancels it. Cancelling is idempotent. Always call cancel when the
    operation completes, that releases the child from the parent."""
    ctx = _CancelContext(parent if parent is not None else _BACKGROUND)

    def cancel() -> None:
        ctx._cancel(Canceled("context cancelled"))

    return ctx, cancel


def with_deadline(
    deadline: float, parent: Optional[Context] = None
) -> Tuple[Context, CancelFunc]:
    """Like with_cancel(), but the context also self cancels with
    DeadlineExceeded at the given absolute time.monotonic() moment.
    A deadline later than the parent's is capped to the parent's."""
    ctx = _CancelContext(parent if parent is not None else _BACKGROUND, deadline=deadline)

    def cancel() -> None:
        ctx._cancel(Canceled("context cancelled"))

    return ctx, cancel


def with_timeout(
    seconds: float, parent: Optional[Context] = None
) -> Tuple[Context, CancelFunc]:
    """with_deadline() with a relative duration instead of an absolute
    moment. The spelling most code wants."""
    return with_deadline(time.monotonic() + seconds, parent=parent)
