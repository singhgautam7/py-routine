"""py-routine: Go style concurrency for Python.

Routines, channels, select and WaitGroup, with semantics that match Go
where it makes sense and stay Pythonic where it does not.

    from pyroutine import go, routine, Chan, WaitGroup, select, recv_case

Designed with free threaded CPython (3.13+, GIL disabled) in mind, but
works on every supported CPython. See the README for the full tour.
"""

import sys as _sys
import warnings as _warnings

from ._chan import Chan, ChanClosed
from ._routines import Handle, go, routine
from ._select import after, recv_case, select, send_case
from ._sync import Once, WaitGroup

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
        return not _sys._is_gil_enabled()
    except AttributeError:
        # older interpreters have no free threading at all
        return False


if not free_threading():
    _warnings.warn(
        "pyroutine: this interpreter has the GIL enabled, so routines "
        "interleave on one core instead of running in parallel. I/O bound "
        "and coordination heavy code is fine, CPU bound routines will not "
        "speed up. For real parallelism use free threaded CPython (3.13+, "
        "GIL disabled). To silence, add before the first import: "
        "warnings.filterwarnings('ignore', message='pyroutine:')",
        GILEnabledWarning,
        stacklevel=2,
    )

__all__ = [
    "Chan",
    "ChanClosed",
    "GILEnabledWarning",
    "Handle",
    "Once",
    "WaitGroup",
    "after",
    "free_threading",
    "go",
    "recv_case",
    "routine",
    "select",
    "send_case",
]
