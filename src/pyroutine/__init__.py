"""py-routine: Go style concurrency for Python.

Routines, channels, select and WaitGroup, with semantics that match Go
where it makes sense and stay Pythonic where it does not.

    from pyroutine import go, routine, Chan, WaitGroup, select, recv_case

Designed with free threaded CPython (3.13+, GIL disabled) in mind, but
works on every supported CPython. See the README for the full tour.
"""

import os as _os
import sys as _sys
import warnings as _warnings

from ._chan import Chan, ChanClosed, RecvChan, SendChan
from ._context import (
    Canceled,
    Context,
    DeadlineExceeded,
    background,
    with_cancel,
    with_deadline,
    with_timeout,
)
from ._debug import (
    DeadlockWarning,
    disable_deadlock_detection,
    enable_deadlock_detection,
)
from ._helpers import merge
from ._routines import Handle, go, routine, set_excepthook
from ._select import Select, Timer, after, recv_case, select, send_case, tick
from ._sync import ErrGroup, Mutex, Once, RWMutex, WaitGroup, once, synchronized

__version__ = "0.1.0"


class GILEnabledWarning(RuntimeWarning):
    """Issued once, at import, when the interpreter runs with the GIL
    enabled. Everything still works correctly, but routines interleave
    on one core instead of running in parallel."""


def free_threading() -> bool:
    """True when this interpreter runs with the GIL disabled, i.e. a
    free threaded CPython build (3.13+ built with --disable-gil, with
    the GIL not re-enabled at runtime). Only then do routines run
    Python code in parallel across cores."""
    try:
        return not _sys._is_gil_enabled()  # type: ignore[attr-defined]
    except AttributeError:
        # older interpreters have no free threading at all
        return False


if not free_threading() and not _os.environ.get("PYROUTINE_NO_GIL_WARNING"):
    _msg = (
        "pyroutine: this interpreter has the GIL enabled, so routines "
        "interleave on one core instead of running in parallel. I/O bound "
        "and coordination heavy code is fine, CPU bound routines will not "
        "speed up. For real parallelism use free threaded CPython (3.13+, "
        "GIL disabled). Set PYROUTINE_NO_GIL_WARNING=1 to silence this "
        "warning."
    )
    if getattr(_sys.stderr, "isatty", lambda: False)() and not _os.environ.get(
        "NO_COLOR"
    ):
        _msg = "\033[33m" + _msg + "\033[0m"
    _warnings.warn(_msg, GILEnabledWarning, stacklevel=2)

__all__ = [
    "Canceled",
    "Chan",
    "ChanClosed",
    "Context",
    "DeadlineExceeded",
    "DeadlockWarning",
    "ErrGroup",
    "GILEnabledWarning",
    "Handle",
    "Mutex",
    "Once",
    "RWMutex",
    "RecvChan",
    "Select",
    "SendChan",
    "Timer",
    "WaitGroup",
    "after",
    "background",
    "disable_deadlock_detection",
    "enable_deadlock_detection",
    "free_threading",
    "go",
    "merge",
    "once",
    "recv_case",
    "routine",
    "select",
    "send_case",
    "set_excepthook",
    "synchronized",
    "tick",
    "with_cancel",
    "with_deadline",
    "with_timeout",
]
