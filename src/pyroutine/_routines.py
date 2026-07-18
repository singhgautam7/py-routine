"""Spawning routines.

For now a routine is one daemon OS thread. That is deliberately boring:
it is easy to reason about, plays nicely with everything else in the
standard library, and on free threaded CPython (3.13+ with the GIL off)
it gives real parallelism without any extra work from us. An M:N
scheduler that multiplexes routines over a small thread pool is on the
roadmap, see CLAUDE.md, and it must not change this public API.
"""

from __future__ import annotations

import functools
import itertools
import threading
from typing import Any, Callable, Optional

_counter = itertools.count(1)


class Handle:
    """A running routine. Like a Future, kept deliberately small."""

    __slots__ = ("_result", "_exc", "_done", "_thread")

    def __init__(self, fn: Callable, args: tuple, kwargs: dict):
        self._result: Any = None
        self._exc: Optional[BaseException] = None
        self._done = threading.Event()
        name = f"pyroutine-{next(_counter)}-{getattr(fn, '__name__', 'fn')}"
        self._thread = threading.Thread(
            target=self._run, args=(fn, args, kwargs), name=name, daemon=True
        )
        self._thread.start()

    def _run(self, fn: Callable, args: tuple, kwargs: dict) -> None:
        try:
            self._result = fn(*args, **kwargs)
        except BaseException as e:
            # held onto and re-raised from result(), never swallowed silently
            self._exc = e
        finally:
            self._done.set()

    def join(self, timeout: Optional[float] = None) -> bool:
        """Wait for the routine to finish. True if it did."""
        return self._done.wait(timeout)

    def result(self, timeout: Optional[float] = None) -> Any:
        """Wait for the routine and return its value. Re-raises whatever
        exception it died with."""
        if not self._done.wait(timeout):
            raise TimeoutError("routine is still running")
        if self._exc is not None:
            raise self._exc
        return self._result

    @property
    def done(self) -> bool:
        return self._done.is_set()

    def __repr__(self) -> str:
        state = "done" if self.done else "running"
        return f"<Handle {self._thread.name} {state}>"


def go(fn: Callable, *args: Any, **kwargs: Any) -> Handle:
    """Spawn fn(*args, **kwargs) as a routine, like `go f(x)` in Go.
    Always spawns immediately and returns a Handle."""
    if not callable(fn):
        raise TypeError("go() needs a callable as its first argument")
    return Handle(fn, args, kwargs)


def routine(fn: Callable) -> Callable:
    """Decorator form. Calling the decorated function spawns it:

        @routine
        def fetch(url): ...

        h = fetch("https://example.com")   # runs concurrently
        page = h.result()

    Kept separate from go() on purpose. Overloading go() to also act as
    a decorator made zero argument calls ambiguous, we tried.
    """

    @functools.wraps(fn)
    def spawner(*args: Any, **kwargs: Any) -> Handle:
        return Handle(fn, args, kwargs)

    return spawner
