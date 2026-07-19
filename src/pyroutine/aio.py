"""pyroutine.aio: awaitable bridges between asyncio and channels.

The core library stays synchronous. This module lets async code talk
to the same channels that routines use, so a threaded pipeline and an
asyncio front end can meet in the middle:

    from pyroutine import Chan, go
    from pyroutine import aio

    ch = Chan(16)
    go(blocking_producer, ch)          # a routine fills the channel

    async def consume():
        async for item in aio.iterate(ch):
            await handle(item)

No polling and no executor threads: an awaiting task registers the
same kind of waiter a blocked thread would, and whoever completes the
operation resolves an asyncio future via call_soon_threadsafe. An idle
bridge costs nothing.

Caveat, inherent to bridging: when an aio.send or aio.recv is
cancelled at the exact moment another thread completes it, the
operation has already happened. The cancellation still propagates, and
for recv the delivered value is dropped. If that matters, prefer
draining with iterate() and closing the channel to shut down.
"""

from __future__ import annotations

import asyncio
import random
from typing import Any, AsyncIterator, Optional, Tuple, TypeVar, Union

from ._chan import (
    _CLOSED,
    _SEND_OK,
    _TIMEOUT_INDEX,
    Chan,
    ChanClosed,
    RecvChan,
    SendChan,
    _Waiter,
)
from ._select import _RECV, _SEND

T = TypeVar("T")


class _AsyncWaiter(_Waiter):
    """A waiter whose finish() also resolves an asyncio future on the
    loop that created it. commit() stays the plain compare and set, so
    sync and async parties race for completion on equal terms."""

    __slots__ = ("_loop", "_future")

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        _Waiter.__init__(self)
        self._loop = loop
        self._future: "asyncio.Future[None]" = loop.create_future()

    def finish(self, value: Any) -> None:
        _Waiter.finish(self, value)
        self._loop.call_soon_threadsafe(self._resolve)

    def _resolve(self) -> None:
        if not self._future.done():
            self._future.set_result(None)


def _underlying_recv(ch: "Union[Chan[T], RecvChan[T]]") -> "Chan[T]":
    if isinstance(ch, RecvChan):
        return ch._chan
    if isinstance(ch, Chan):
        return ch
    raise TypeError("aio.recv() expects a Chan or RecvChan")


def _underlying_send(ch: "Union[Chan[T], SendChan[T]]") -> "Chan[T]":
    if isinstance(ch, SendChan):
        return ch._chan
    if isinstance(ch, Chan):
        return ch
    raise TypeError("aio.send() expects a Chan or SendChan")


async def _await_waiter(
    ch: Chan, w: _AsyncWaiter, timeout: Optional[float]
) -> None:
    """Wait for the parked waiter to be completed, handling timeout and
    cancellation with the same self commit protocol the sync API uses."""
    timer = None
    if timeout is not None:

        def on_timeout() -> None:
            # runs on the loop thread; both calls only take tiny leaf
            # or channel critical sections
            if w.commit(_TIMEOUT_INDEX):
                ch._unregister(w)
            w._resolve()

        timer = w._loop.call_later(timeout, on_timeout)
    try:
        await w._future
    except asyncio.CancelledError:
        if w.commit(_TIMEOUT_INDEX):
            ch._unregister(w)
        # if the commit lost, the operation completed concurrently, see
        # the module docstring caveat
        raise
    finally:
        if timer is not None:
            timer.cancel()


async def recv(
    ch: "Union[Chan[T], RecvChan[T]]", timeout: Optional[float] = None
) -> T:
    """Awaitable ch.recv(). Raises ChanClosed once the channel is closed
    and drained, TimeoutError on timeout."""
    underlying = _underlying_recv(ch)
    loop = asyncio.get_running_loop()
    with underlying._lock:
        value, ok = underlying._poll_recv_locked()
        if ok:
            return value
        w = _AsyncWaiter(loop)
        underlying._recv_waiters.append((w, 0))
    await _await_waiter(underlying, w, timeout)
    if w.index == _TIMEOUT_INDEX:
        raise TimeoutError("recv timed out")
    if w.value is _CLOSED:
        raise ChanClosed("recv on closed channel")
    return w.value


async def send(
    ch: "Union[Chan[T], SendChan[T]]", value: T, timeout: Optional[float] = None
) -> None:
    """Awaitable ch.send(value). Raises ChanClosed if the channel is or
    becomes closed, TimeoutError on timeout."""
    underlying = _underlying_send(ch)
    loop = asyncio.get_running_loop()
    with underlying._lock:
        if underlying._poll_send_locked(value):
            return
        w = _AsyncWaiter(loop)
        underlying._send_waiters.append((w, 0, value))
    await _await_waiter(underlying, w, timeout)
    if w.index == _TIMEOUT_INDEX:
        raise TimeoutError("send timed out")
    if w.value is _CLOSED:
        raise ChanClosed("channel closed while sending")
    assert w.value is _SEND_OK


async def select(
    *cases: tuple,
    default: bool = False,
    timeout: Optional[float] = None,
) -> Tuple[int, Any]:
    """Awaitable pyroutine.select() over the same recv_case()/send_case()
    cases the sync API uses, with identical semantics: random fairness,
    exactly one case fires, (index, value) back, ChanClosed with .index
    on a closed winning case, (-1, None) with default=True, TimeoutError
    on deadline. The awaiting task parks with no polling.

        idx, val = await aio.select(recv_case(events), recv_case(ctl))
    """
    if not cases:
        raise ValueError("select() needs at least one case")
    for c in cases:
        if not (isinstance(c, tuple) and len(c) == 3 and c[0] in (_RECV, _SEND)):
            raise TypeError("cases must be built with recv_case() or send_case()")

    if len(cases) == 1 and not default:
        kind, ch, val = cases[0]
        try:
            if kind == _RECV:
                return 0, await recv(ch, timeout)
            await send(ch, val, timeout)
            return 0, None
        except ChanClosed as e:
            e.index = 0
            raise

    loop = asyncio.get_running_loop()
    order = list(range(len(cases)))
    random.shuffle(order)

    # opportunistic pass, one channel lock at a time, see _select.py
    for i in order:
        kind, ch, val = cases[i]
        try:
            if kind == _RECV:
                value, ok = ch.try_recv()
                if ok:
                    return i, value
            else:
                if ch.try_send(val):
                    return i, None
        except ChanClosed as e:
            e.index = i
            raise
    if default:
        return -1, None

    chan_by_id = {id(c[1]): c[1] for c in cases}
    ordered_chans = [chan_by_id[k] for k in sorted(chan_by_id)]

    waiter: Optional[_AsyncWaiter] = None
    for ch in ordered_chans:
        ch._lock.acquire()
    try:
        # atomic re-check under every lock before parking
        for i in order:
            kind, ch, val = cases[i]
            try:
                if kind == _RECV:
                    value, ok = ch._poll_recv_locked()
                    if ok:
                        return i, value
                else:
                    if ch._poll_send_locked(val):
                        return i, None
            except ChanClosed as e:
                e.index = i
                raise
        waiter = _AsyncWaiter(loop)
        for i in range(len(cases)):
            kind, ch, val = cases[i]
            if kind == _RECV:
                ch._recv_waiters.append((waiter, i))
            else:
                ch._send_waiters.append((waiter, i, val))
    finally:
        for ch in reversed(ordered_chans):
            ch._lock.release()

    timer = None
    if timeout is not None:

        def on_timeout() -> None:
            if waiter.commit(_TIMEOUT_INDEX):
                for c in ordered_chans:
                    c._unregister(waiter)
            waiter._resolve()

        timer = loop.call_later(timeout, on_timeout)
    try:
        await waiter._future
    except asyncio.CancelledError:
        if waiter.commit(_TIMEOUT_INDEX):
            for c in ordered_chans:
                c._unregister(waiter)
        raise
    finally:
        if timer is not None:
            timer.cancel()

    for ch in ordered_chans:
        ch._unregister(waiter)

    idx = waiter.index
    assert idx is not None
    if idx == _TIMEOUT_INDEX:
        raise TimeoutError("select timed out")
    if waiter.value is _CLOSED:
        raise ChanClosed("select hit a closed channel", index=idx)
    if cases[idx][0] == _RECV:
        return idx, waiter.value
    return idx, None


async def iterate(ch: "Union[Chan[T], RecvChan[T]]") -> "AsyncIterator[T]":
    """Async iteration until the channel is closed and drained, the
    awaitable spelling of `for v in ch`:

        async for value in aio.iterate(ch):
            ...
    """
    while True:
        try:
            yield await recv(ch)
        except ChanClosed:
            return
