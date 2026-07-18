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
from typing import Any, AsyncIterator, Optional, TypeVar, Union

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
