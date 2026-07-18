"""Spawning routines.

Routines run on a pool of reusable daemon worker threads. Spawning
pops an idle worker and hands it the task through a private one slot
mailbox; only when no worker is idle does a new OS thread get created.
Workers that sit idle past a timeout retire. This keeps the semantics
of one-thread-per-routine (a routine that blocks still occupies a
thread, and the pool grows without bound so a burst of blocked
routines can never starve a runnable one) while making spawn cost a
queue handoff instead of a thread creation in the steady state.

Full M:N parking, where a routine blocked on a channel releases its
thread, is future work and needs continuation support the language
does not give us cheaply. See CLAUDE.md.
"""

from __future__ import annotations

import functools
import itertools
import threading
from queue import Empty, SimpleQueue
from typing import Any, Callable, List, Optional, Tuple

_counter = itertools.count(1)
_worker_counter = itertools.count(1)

# how long an idle worker waits for new work before its thread exits
_IDLE_TIMEOUT = 5.0

# a task is (handle, fn, args, kwargs, name)
_Task = Tuple["Handle", Callable, tuple, dict, str]


class _Worker:
    __slots__ = ("mailbox", "thread", "idle_name")

    def __init__(self) -> None:
        self.mailbox: "SimpleQueue[_Task]" = SimpleQueue()
        self.idle_name = f"pyroutine-worker-{next(_worker_counter)}"
        self.thread = threading.Thread(
            target=_worker_loop, args=(self,), name=self.idle_name, daemon=True
        )


class _Scheduler:
    __slots__ = ("_lock", "_idle")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # LIFO: the most recently parked worker is the warmest
        self._idle: List[_Worker] = []

    def submit(self, task: _Task) -> None:
        with self._lock:
            worker = self._idle.pop() if self._idle else None
        if worker is not None:
            worker.mailbox.put(task)
        else:
            worker = _Worker()
            worker.mailbox.put(task)
            worker.thread.start()

    def _park(self, worker: _Worker) -> None:
        with self._lock:
            self._idle.append(worker)

    def _try_retire(self, worker: _Worker) -> bool:
        """True if the worker was still idle and is now retired. False
        means submit() claimed it concurrently and a task is incoming."""
        with self._lock:
            try:
                self._idle.remove(worker)
                return True
            except ValueError:
                return False


_scheduler = _Scheduler()


def _worker_loop(worker: _Worker) -> None:
    while True:
        try:
            task = worker.mailbox.get(timeout=_IDLE_TIMEOUT)
        except Empty:
            if _scheduler._try_retire(worker):
                return
            # claimed at the last moment, the task is on its way
            task = worker.mailbox.get()
        handle, fn, args, kwargs, name = task
        worker.thread.name = name
        try:
            handle._run(fn, args, kwargs)
        finally:
            worker.thread.name = worker.idle_name
            _scheduler._park(worker)


class Handle:
    """A running routine. Like a Future, kept deliberately small."""

    __slots__ = ("_result", "_exc", "_done", "_name")

    def __init__(self, fn: Callable, args: tuple, kwargs: dict):
        self._result: Any = None
        self._exc: Optional[BaseException] = None
        self._done = threading.Event()
        self._name = f"pyroutine-{next(_counter)}-{getattr(fn, '__name__', 'fn')}"
        _scheduler.submit((self, fn, args, kwargs, self._name))

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
        return f"<Handle {self._name} {state}>"


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
