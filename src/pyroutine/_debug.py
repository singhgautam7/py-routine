"""Opt in debugging helpers, off by default and free when off.

enable_deadlock_detection() starts a daemon watcher in the spirit of
Go's "all goroutines are asleep - deadlock!" runtime check. Every
interval it asks: is every running routine blocked forever inside a
pyroutine primitive (a channel op, select, join or WaitGroup.wait with
no timeout), with no pyroutine timer pending that could wake one? If
the same stuck picture shows up twice in a row, it issues a
DeadlockWarning naming the blocked routines.

Unlike Go's runtime we do not own every thread in the process, so this
is warn only and best effort: a non pyroutine thread or an asyncio
task using pyroutine.aio could still legally complete a channel
operation we cannot see coming. No false positive can crash you, and a
real deadlock is reported within two intervals.

Waits WITH a timeout are never counted as stuck, they wake themselves.
Mutex/RWMutex waits are not instrumented in this version.
"""

from __future__ import annotations

import threading
import warnings
from typing import Dict, Optional, Tuple

# read directly by the park sites; plain module attribute on purpose so
# the disabled cost is one attribute load and a falsy check
enabled = False

_lock = threading.Lock()
_parked: Dict[threading.Thread, str] = {}
_pending_timers = 0
_stop_event: Optional[threading.Event] = None

_Snapshot = Tuple[int, int, Tuple[Tuple[str, str], ...]]


class DeadlockWarning(RuntimeWarning):
    """All running routines are blocked in pyroutine operations and
    nothing pyroutine knows about can wake them."""


def park_begin(kind: str) -> None:
    """The current thread is entering an untimed pyroutine wait."""
    with _lock:
        _parked[threading.current_thread()] = kind


def park_end() -> None:
    with _lock:
        _parked.pop(threading.current_thread(), None)


def timer_started() -> None:
    """A pyroutine timer (Timer, tick, context deadline) is pending."""
    global _pending_timers
    with _lock:
        _pending_timers += 1


def timer_finished() -> None:
    global _pending_timers
    with _lock:
        _pending_timers -= 1


def enable_deadlock_detection(interval: float = 1.0) -> None:
    """Start the watcher. Idempotent. Enable it early, ideally before
    spawning routines, so the bookkeeping sees every park."""
    global enabled, _stop_event
    if interval <= 0:
        raise ValueError("interval must be positive")
    with _lock:
        if enabled:
            return
        enabled = True
        _stop_event = threading.Event()
        watcher = threading.Thread(
            target=_watch,
            args=(interval, _stop_event),
            name="pyroutine-deadlock-detector",
            daemon=True,
        )
    watcher.start()


def disable_deadlock_detection() -> None:
    """Stop the watcher and drop the bookkeeping. Idempotent."""
    global enabled, _stop_event
    with _lock:
        if not enabled:
            return
        enabled = False
        stop = _stop_event
        _stop_event = None
        _parked.clear()
    if stop is not None:
        stop.set()


def _watch(interval: float, stop: threading.Event) -> None:
    last: Optional[_Snapshot] = None
    warned_for: Optional[_Snapshot] = None
    while not stop.wait(interval):
        from ._routines import active_routines  # late import, avoids a cycle

        active = active_routines()
        with _lock:
            timers = _pending_timers
            parked = list(_parked.items())
        names = tuple(sorted((t.name, kind) for t, kind in parked))
        snapshot: _Snapshot = (active, timers, names)
        main = threading.main_thread()
        main_parked = any(t is main for t, _ in parked)
        workers_parked = sum(1 for t, _ in parked if t.name.startswith("pyroutine-"))
        stuck = active > 0 and timers == 0 and workers_parked >= active

        if stuck and snapshot == last and snapshot != warned_for:
            blocked = ", ".join(f"{name} [{kind}]" for name, kind in names)
            verdict = (
                "the main thread is blocked too, this is a deadlock"
                if main_parked
                else "the main thread is still runnable, this deadlocks unless it acts"
            )
            warnings.warn(
                f"pyroutine: all {active} running routine(s) are blocked in "
                f"pyroutine operations with no pending timers ({blocked}); "
                f"{verdict}",
                DeadlockWarning,
                stacklevel=2,
            )
            warned_for = snapshot
        if snapshot != last:
            warned_for = None
        last = snapshot
