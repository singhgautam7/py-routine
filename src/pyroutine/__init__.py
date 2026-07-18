"""py-routine: Go style concurrency for Python.

Routines, channels, select and WaitGroup, with semantics that match Go
where it makes sense and stay Pythonic where it does not.

    from pyroutine import go, routine, Chan, WaitGroup, select, recv_case

Designed with free threaded CPython (3.13+, GIL disabled) in mind, but
works on every supported CPython. See the README for the full tour.
"""

from ._chan import Chan, ChanClosed
from ._routines import Handle, go, routine
from ._select import after, recv_case, select, send_case
from ._sync import Once, WaitGroup

__version__ = "0.1.0"

__all__ = [
    "Chan",
    "ChanClosed",
    "Handle",
    "Once",
    "WaitGroup",
    "after",
    "go",
    "recv_case",
    "routine",
    "select",
    "send_case",
]
