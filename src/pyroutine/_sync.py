"""Small synchronization helpers modeled on Go's sync package."""

from __future__ import annotations

import functools
import threading
from contextlib import contextmanager
from typing import Any, Callable, Optional, Union

from ._context import Context, with_cancel
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


class Mutex:
    """sync.Mutex: threading.Lock with Go's method names, plus context
    manager support so the common case reads well:

        m = Mutex()
        with m:
            shared += 1
    """

    __slots__ = ("_lock",)

    def __init__(self) -> None:
        self._lock = threading.Lock()

    def lock(self) -> None:
        self._lock.acquire()

    def unlock(self) -> None:
        try:
            self._lock.release()
        except RuntimeError:
            raise RuntimeError("unlock of an unlocked Mutex") from None

    def __enter__(self) -> "Mutex":
        self.lock()
        return self

    def __exit__(self, *exc) -> None:
        self.unlock()


class RWMutex:
    """sync.RWMutex: any number of readers or exactly one writer.

    Writers get preference. Once a writer is waiting, new rlock() calls
    block until it has come and gone, so a steady stream of readers
    cannot starve writers. Like in Go, that makes recursive read locking
    a deadlock risk, do not nest rlock() on one mutex in one routine.

        with rw.read():
            snapshot = dict(shared)
        with rw.write():
            shared[key] = value
    """

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._readers = 0
        self._writer = False
        self._writers_waiting = 0

    def rlock(self) -> None:
        with self._cond:
            while self._writer or self._writers_waiting:
                self._cond.wait()
            self._readers += 1

    def runlock(self) -> None:
        with self._cond:
            if self._readers <= 0:
                raise RuntimeError("runlock of an unlocked RWMutex")
            self._readers -= 1
            if self._readers == 0:
                self._cond.notify_all()

    def lock(self) -> None:
        with self._cond:
            self._writers_waiting += 1
            try:
                while self._writer or self._readers:
                    self._cond.wait()
            finally:
                self._writers_waiting -= 1
            self._writer = True

    def unlock(self) -> None:
        with self._cond:
            if not self._writer:
                raise RuntimeError("unlock of an unlocked RWMutex")
            self._writer = False
            self._cond.notify_all()

    @contextmanager
    def read(self):
        self.rlock()
        try:
            yield
        finally:
            self.runlock()

    @contextmanager
    def write(self):
        self.lock()
        try:
            yield
        finally:
            self.unlock()


class ErrGroup:
    """errgroup.Group: a WaitGroup for routines that can fail.

    The first exception raised by any routine cancels the group context
    and is re-raised from wait(). Sibling routines watch eg.ctx (via
    select on eg.ctx.done(), or eg.ctx.err() checks) to stop early:

        eg = ErrGroup()
        for url in urls:
            eg.go(fetch, url, eg.ctx)
        eg.wait()   # re-raises the first fetch error, if any
    """

    def __init__(self, parent: Optional[Context] = None) -> None:
        self._ctx, self._cancel = with_cancel(parent)
        self._wg = WaitGroup()
        self._lock = threading.Lock()
        self._exc: Optional[BaseException] = None
        self._sem: Optional[threading.BoundedSemaphore] = None
        self._spawned = False

    @property
    def ctx(self) -> Context:
        """Cancelled as soon as any routine raises, and in any case once
        wait() returns, exactly like Go's errgroup.WithContext."""
        return self._ctx

    def set_limit(self, n: Optional[int]) -> None:
        """Cap the number of concurrently running routines, like Go's
        errgroup.SetLimit. While n routines are active, go() blocks the
        caller until one finishes. None removes the cap. Must be called
        before the first go()."""
        if self._spawned:
            raise RuntimeError("set_limit() must be called before go()")
        if n is not None and n < 1:
            raise ValueError("limit must be a positive integer or None")
        self._sem = threading.BoundedSemaphore(n) if n is not None else None

    def go(self, fn: Callable, *args: Any, **kwargs: Any) -> Handle:
        """Spawn fn as a routine in the group. The Handle still works
        normally if you want this routine's own result or exception.
        With set_limit() in effect this blocks while the group is at
        its concurrency cap."""
        self._spawned = True
        sem = self._sem
        if sem is not None:
            sem.acquire()
        self._wg.add(1)

        def wrapped() -> Any:
            try:
                return fn(*args, **kwargs)
            except BaseException as e:
                with self._lock:
                    if self._exc is None:
                        self._exc = e
                self._cancel()
                raise
            finally:
                self._wg.done()
                if sem is not None:
                    sem.release()

        wrapped.__name__ = getattr(fn, "__name__", "wrapped")
        return Handle(wrapped, (), {})

    def wait(self, timeout: Optional[float] = None) -> None:
        """Block until every routine has finished, then re-raise the
        first exception any of them died with. Cancels the group context
        on the way out."""
        if not self._wg.wait(timeout):
            raise TimeoutError("ErrGroup routines still running")
        self._cancel()
        with self._lock:
            if self._exc is not None:
                raise self._exc


def once(fn: Callable) -> Callable:
    """Decorator: fn runs at most once, no matter how many routines
    call it or with what arguments. Every call returns the first call's
    result; if the first call raised, every call re-raises that same
    exception. Like Go's sync.OnceValues:

        @once
        def config():
            return expensive_load()

        config()   # loads
        config()   # cached, same object back
    """
    lock = threading.Lock()
    done = False
    value: Any = None
    exc: Optional[BaseException] = None

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        nonlocal done, value, exc
        if not done:
            with lock:
                if not done:
                    try:
                        value = fn(*args, **kwargs)
                    except BaseException as e:
                        exc = e
                    finally:
                        done = True
        if exc is not None:
            raise exc
        return value

    return wrapper


def _synchronized_wrapper(fn: Callable, mutex: Mutex) -> Callable:
    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        with mutex:
            return fn(*args, **kwargs)

    return wrapper


def synchronized(fn_or_mutex: Union[Callable, Mutex]) -> Callable:
    """Decorator: the function body runs under a Mutex, so at most one
    routine is ever inside it. Bare form gives the function a private
    lock; pass a Mutex to share one lock across several functions:

        @synchronized
        def bump():             # private lock
            counter[0] += 1

        m = Mutex()

        @synchronized(m)        # deposit and withdraw exclude each other
        def deposit(x): ...

        @synchronized(m)
        def withdraw(x): ...
    """
    if isinstance(fn_or_mutex, Mutex):
        mutex = fn_or_mutex

        def decorator(fn: Callable) -> Callable:
            return _synchronized_wrapper(fn, mutex)

        return decorator
    if not callable(fn_or_mutex):
        raise TypeError("synchronized expects a function or a Mutex")
    return _synchronized_wrapper(fn_or_mutex, Mutex())
