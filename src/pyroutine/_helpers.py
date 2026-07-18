"""Helpers built on top of channels, select and routines.

Nothing in here touches channel internals. These are the utilities Go
programmers keep rewriting by hand, provided once.
"""

from __future__ import annotations

from typing import TypeVar, Union

from ._chan import Chan, ChanClosed, RecvChan
from ._routines import go
from ._select import recv_case, select

T = TypeVar("T")


def merge(*chans: "Union[Chan[T], RecvChan[T]]", maxsize: int = 0) -> "Chan[T]":
    """Fan in: one channel carrying every value from all the inputs.

    The returned channel closes once every input is closed and drained.
    A single routine multiplexes the inputs with select(), so merging N
    channels costs one thread, not N. Closing the returned channel early
    stops the merge; a value already taken off an input at that instant
    is dropped, matching what closing a pipe mid stream means anywhere.

        out = merge(ch_a, ch_b, ch_c)
        for value in out:
            handle(value)
    """
    if not chans:
        raise ValueError("merge() needs at least one channel")
    out: "Chan[T]" = Chan(maxsize)

    def pump() -> None:
        remaining = list(chans)
        # rebuilt only when an input closes, not once per message
        cases = [recv_case(c) for c in remaining]
        while remaining:
            try:
                _, value = select(*cases)
            except ChanClosed as e:
                assert e.index is not None  # select always sets it
                del remaining[e.index]
                cases = [recv_case(c) for c in remaining]
                continue
            try:
                out.send(value)
            except ChanClosed:
                return  # consumer closed the output, stop pumping
        out.close()

    go(pump)
    return out
