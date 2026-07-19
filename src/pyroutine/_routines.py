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
import sys
import threading
import traceback
from queue import Empty, SimpleQueue
from typing import Any, Callable, List, Optional, Tuple

from . import _debug

_counter = itertools.count(1)
_worker_counter = itertools.count(1)

# routines currently executing user code, read by the deadlock detector
_active = 0
_active_lock = threading.Lock()


def active_routines() -> int:
    """How many routines are currently executing (including ones blocked
    inside a channel op). Used by the deadlock detector."""
    with _active_lock:
        return _active

# how long an idle worker waits for new work before its thread exits
_IDLE_TIMEOUT = 5.0

# stack size for newly created worker threads, 0 = platform default.
# threading.stack_size() is process global state, so setting it around
# thread creation is serialized by _stack_lock
_worker_stack_size = 0
_stack_lock = threading.Lock()


def set_worker_stack_size(nbytes: int) -> None:
    """Stack size in bytes for worker threads created from now on, 0
    restores the platform default. Existing workers are unaffected.

    A blocked routine pins its worker thread, and the thread stack is
    what bounds how many concurrently blocked routines you can afford:
    at the common 8 MiB default, 50k parked routines reserve ~400 GiB
    of address space, at 512 KiB they reserve ~25 GiB. Small stacks
    crash deep recursion, treat 512 KiB as the floor unless you know
    your call depths. Raises ValueError for sizes the platform rejects
    (generally anything under 32 KiB)."""
    global _worker_stack_size
    with _stack_lock:
        if nbytes:
            previous = threading.stack_size(nbytes)  # validates
            threading.stack_size(previous)
        _worker_stack_size = nbytes

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
            return
        size = _worker_stack_size
        if size:
            # stack_size is process global, keep the window serialized
            with _stack_lock:
                previous = threading.stack_size(size)
                try:
                    worker = _Worker()
                    worker.mailbox.put(task)
                    worker.thread.start()
                finally:
                    threading.stack_size(previous)
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
        global _active
        with _active_lock:
            _active += 1
        try:
            handle._run(fn, args, kwargs)
        finally:
            with _active_lock:
                _active -= 1
            worker.thread.name = worker.idle_name
            _scheduler._park(worker)
        # drop the task references BEFORE parking in get(): a worker
        # sitting idle must not keep the finished routine's Handle (and
        # its arguments) alive, that would delay garbage collection and
        # the unretrieved exception report
        del task, handle, fn, args, kwargs


# called when a Handle holding a never retrieved exception is garbage
# collected; replace with set_excepthook()
_excepthook: Optional[Callable[["Handle", BaseException], None]] = None


def _default_excepthook(handle: "Handle", exc: BaseException) -> None:
    stream = getattr(sys, "stderr", None)
    if stream is None:  # interpreter teardown
        return
    print(
        f"pyroutine: exception in routine {handle._name!r} was never retrieved",
        file=stream,
    )
    traceback.print_exception(type(exc), exc, exc.__traceback__, file=stream)


def set_excepthook(
    hook: Optional[Callable[["Handle", BaseException], None]],
) -> None:
    """Install a hook called as hook(handle, exc) when a routine died
    with an exception nobody ever collected via result(). Mirrors
    threading.excepthook in spirit: by default such exceptions are
    printed to stderr when the Handle is garbage collected, because a
    silently vanishing failure is the one thing Go would never allow
    (it crashes the whole program instead). Pass None to restore the
    default printer."""
    global _excepthook
    if hook is not None and not callable(hook):
        raise TypeError("excepthook must be callable or None")
    _excepthook = hook


class Handle:
    """A running routine. Like a Future, kept deliberately small.

    If the routine dies with an exception and result() is never called,
    the exception is reported through the excepthook (stderr by
    default) when the Handle is garbage collected, so failures cannot
    vanish silently."""

    __slots__ = ("_result", "_exc", "_done", "_name", "_exc_retrieved")

    def __init__(self, fn: Callable, args: tuple, kwargs: dict):
        self._result: Any = None
        self._exc: Optional[BaseException] = None
        self._done = threading.Event()
        self._exc_retrieved = False
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

    def _wait_done(self, timeout: Optional[float]) -> bool:
        if timeout is None and _debug.enabled:
            _debug.park_begin("join")
            try:
                return self._done.wait()
            finally:
                _debug.park_end()
        return self._done.wait(timeout)

    def join(self, timeout: Optional[float] = None) -> bool:
        """Wait for the routine to finish. True if it did. Does not
        collect the result or exception, that is result()'s job."""
        return self._wait_done(timeout)

    def result(self, timeout: Optional[float] = None) -> Any:
        """Wait for the routine and return its value. Re-raises whatever
        exception it died with."""
        if not self._wait_done(timeout):
            raise TimeoutError("routine is still running")
        if self._exc is not None:
            self._exc_retrieved = True
            raise self._exc
        return self._result

    @property
    def done(self) -> bool:
        return self._done.is_set()

    def __repr__(self) -> str:
        state = "done" if self.done else "running"
        return f"<Handle {self._name} {state}>"

    def __del__(self) -> None:
        if self._exc is None or self._exc_retrieved:
            return
        try:
            hook = _excepthook if _excepthook is not None else _default_excepthook
            hook(self, self._exc)
        except Exception:
            pass  # never let a reporting failure escape the collector


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
