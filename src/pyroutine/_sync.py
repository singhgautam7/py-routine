"""Small synchronization helpers modeled on Go's sync package."""

from __future__ import annotations

import threading
from typing import Any, Callable, Optional

from ._routines import Handle


class WaitGroup:
    """Go's sync.WaitGroup, plus a shortcut that most Python code will
    actually want:

        wg = WaitGroup()
        wg.go(worker, 1)    # add(1), spawn, and done() automatically
        wg.go(worker, 2)
        wg.wait()
    """

    def __init__(self) -> None:
        self._count = 0
        self._cond = threading.Condition()

    def add(self, n: int = 1) -> None:
        with self._cond:
            self._count += n
            if self._count < 0:
                raise ValueError("WaitGroup counter went negative")
            if self._count == 0:
                self._cond.notify_all()

    def done(self) -> None:
        self.add(-1)

    def wait(self, timeout: Optional[float] = None) -> bool:
        """Block until the counter hits zero. True if it did."""
        with self._cond:
            return self._cond.wait_for(lambda: self._count == 0, timeout)

    def go(self, fn: Callable, *args: Any, **kwargs: Any) -> Handle:
        """add(1), spawn fn as a routine, and call done() when it exits,
        no matter how it exits."""
        self.add(1)

        def wrapped(*a: Any, **kw: Any) -> Any:
            try:
                return fn(*a, **kw)
            finally:
                self.done()

        wrapped.__name__ = getattr(fn, "__name__", "wrapped")
        return Handle(wrapped, args, kwargs)


class Once:
    """Run a callable exactly once across all routines, like sync.Once."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._done = False

    def do(self, fn: Callable, *args: Any, **kwargs: Any) -> None:
        if self._done:
            return
        with self._lock:
            if not self._done:
                try:
                    fn(*args, **kwargs)
                finally:
                    self._done = True
