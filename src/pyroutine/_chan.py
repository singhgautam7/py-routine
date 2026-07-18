"""Channels with waiter registration.

The design borrows heavily from how Go's runtime implements channels.
Every blocking operation parks a "waiter" on the channel instead of
polling. A waiter can be shared across several channels by select(),
so completing one is a tiny race: whoever calls commit() first wins,
everyone else skips that waiter and moves on. This is what lets a
sender wake up exactly one of N selects parked on the same channel.

Locking rules. Do not break these, they are what keeps this deadlock free:

1. Regular operations hold at most ONE channel lock at a time.
2. select() may hold several channel locks, but only if it acquires
   them in the canonical order (sorted by id()). See _select.py.
3. The waiter lock is a leaf lock. Only touch it through commit().
4. Never call anything that might block while holding a channel lock.
"""

from __future__ import annotations

import threading
from collections import deque
from typing import Any, Deque, Generic, Iterator, Optional, Tuple, TypeVar

T = TypeVar("T")

# internal markers, never exposed to users
_UNSET = object()      # waiter has no result yet
_CLOSED = object()     # the channel closed under a parked waiter
_SEND_OK = object()    # a parked sender's value was taken

# a waiter claims itself with this index when its own timeout fires
_TIMEOUT_INDEX = -2


class ChanClosed(Exception):
    """Raised on send() to a closed channel, or recv() from one that is
    closed and fully drained.

    When select() raises this, the .index attribute tells you which case
    hit the closed channel.
    """

    def __init__(self, msg: str = "channel is closed", index: Optional[int] = None):
        super().__init__(msg)
        self.index = index


class _Waiter:
    """One parked operation. select() shares a single waiter across all
    of its cases, which is why completion has to be a compare and set."""

    __slots__ = ("_lock", "_event", "index", "value")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._event = threading.Event()
        self.index: Optional[int] = None
        self.value: Any = _UNSET

    def commit(self, index: int) -> bool:
        # first caller wins, everyone else backs off
        with self._lock:
            if self.index is not None:
                return False
            self.index = index
            return True

    def finish(self, value: Any) -> None:
        # only ever called by whoever won commit()
        self.value = value
        self._event.set()

    def wait(self, timeout: Optional[float] = None) -> bool:
        return self._event.wait(timeout)


class Chan(Generic[T]):
    """A Go style channel, generic over its element type: annotate with
    Chan[int] and type checkers follow values through send/recv/select.
    The type parameter is purely static, runtime behavior is identical.

    Chan() is unbuffered: send() blocks until a receiver takes the value,
    a true rendezvous. Chan(n) is buffered: send() only blocks once n
    values are queued up.

    Iterating with `for v in ch` receives until the channel is closed
    and drained, then exits cleanly. Use the channel as a context
    manager to close it on exit.
    """

    def __init__(self, maxsize: int = 0):
        if maxsize < 0:
            raise ValueError("maxsize must be >= 0")
        self._maxsize = maxsize
        self._buf: Deque[Any] = deque()
        self._lock = threading.Lock()
        # entries: (waiter, case_index) for receivers,
        #          (waiter, case_index, value) for senders
        self._recv_waiters: Deque[Tuple[_Waiter, int]] = deque()
        self._send_waiters: Deque[Tuple[_Waiter, int, Any]] = deque()
        self._closed = False

    # ------------------------------------------------------------------ #
    # lock-held primitives, shared by the public API and by select()
    # ------------------------------------------------------------------ #

    def _poll_recv_locked(self) -> Tuple[Any, bool]:
        """Try to receive without blocking. Caller holds self._lock.
        Returns (value, True) on success, (None, False) if we would block.
        Raises ChanClosed if the channel is closed and drained."""
        if self._buf:
            value = self._buf.popleft()
            self._promote_sender_locked()
            return value, True
        # no buffer content, maybe a parked sender can hand off directly
        while self._send_waiters:
            w, idx, sval = self._send_waiters.popleft()
            if w.commit(idx):
                w.finish(_SEND_OK)
                return sval, True
            # that waiter was already claimed elsewhere, drop it and keep going
        if self._closed:
            raise ChanClosed("recv on closed channel")
        return None, False

    def _poll_send_locked(self, value: Any) -> bool:
        """Try to send without blocking. Caller holds self._lock.
        Returns True on success, False if we would block."""
        if self._closed:
            raise ChanClosed("send on closed channel")
        # a parked receiver beats the buffer, hand the value over directly
        while self._recv_waiters:
            w, idx = self._recv_waiters.popleft()
            if w.commit(idx):
                w.finish(value)
                return True
        if self._maxsize > 0 and len(self._buf) < self._maxsize:
            self._buf.append(value)
            return True
        return False

    def _promote_sender_locked(self) -> None:
        # a buffer slot just opened up, move one parked sender's value in
        while self._send_waiters and len(self._buf) < self._maxsize:
            w, idx, sval = self._send_waiters.popleft()
            if w.commit(idx):
                self._buf.append(sval)
                w.finish(_SEND_OK)
                break

    def _unregister(self, waiter: _Waiter) -> None:
        # drop every entry belonging to this waiter, identity comparison
        with self._lock:
            if any(e[0] is waiter for e in self._recv_waiters):
                self._recv_waiters = deque(
                    e for e in self._recv_waiters if e[0] is not waiter
                )
            if any(e[0] is waiter for e in self._send_waiters):
                self._send_waiters = deque(
                    e for e in self._send_waiters if e[0] is not waiter
                )

    # ------------------------------------------------------------------ #
    # public API
    # ------------------------------------------------------------------ #

    def send(self, value: T, timeout: Optional[float] = None) -> None:
        """Send a value. Blocks until delivered, or raises TimeoutError."""
        with self._lock:
            if self._poll_send_locked(value):
                return
            w = _Waiter()
            self._send_waiters.append((w, 0, value))
        if not w.wait(timeout):
            if w.commit(_TIMEOUT_INDEX):
                self._unregister(w)
                raise TimeoutError("send timed out")
            # lost the race, someone completed us at the last moment
            w.wait()
        if w.value is _CLOSED:
            raise ChanClosed("channel closed while sending")

    def recv(self, timeout: Optional[float] = None) -> T:
        """Receive a value. Blocks until one arrives, or raises TimeoutError.
        Raises ChanClosed once the channel is closed and drained."""
        with self._lock:
            value, ok = self._poll_recv_locked()
            if ok:
                return value
            w = _Waiter()
            self._recv_waiters.append((w, 0))
        if not w.wait(timeout):
            if w.commit(_TIMEOUT_INDEX):
                self._unregister(w)
                raise TimeoutError("recv timed out")
            w.wait()
        if w.value is _CLOSED:
            raise ChanClosed("recv on closed channel")
        return w.value

    def try_send(self, value: T) -> bool:
        """Non blocking send. True if delivered or queued. On an unbuffered
        channel this succeeds only when a receiver is already waiting."""
        with self._lock:
            return self._poll_send_locked(value)

    def try_recv(self) -> Tuple[Optional[T], bool]:
        """Non blocking receive. Returns (value, True) or (None, False)."""
        with self._lock:
            return self._poll_recv_locked()

    def close(self) -> None:
        """Close the channel. Buffered values can still be drained.
        Wakes everything currently parked on the channel."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            recvs = list(self._recv_waiters)
            sends = list(self._send_waiters)
            self._recv_waiters.clear()
            self._send_waiters.clear()
        # finish outside the lock, wakers will want the lock right away
        for w, idx in recvs:
            if w.commit(idx):
                w.finish(_CLOSED)
        for w, idx, _ in sends:
            if w.commit(idx):
                w.finish(_CLOSED)

    # ------------------------------------------------------------------ #
    # conveniences
    # ------------------------------------------------------------------ #

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def cap(self) -> int:
        return self._maxsize

    def __len__(self) -> int:
        # number of buffered values, always 0 for unbuffered channels
        return len(self._buf)

    def __iter__(self) -> Iterator[T]:
        while True:
            try:
                yield self.recv()
            except ChanClosed:
                return

    def __enter__(self) -> "Chan":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def __repr__(self) -> str:
        state = "closed" if self._closed else "open"
        return f"<Chan cap={self._maxsize} len={len(self._buf)} {state}>"

    def recv_only(self) -> "RecvChan[T]":
        """A receive only view of this channel, like Go's `<-chan T`."""
        return RecvChan(self)

    def send_only(self) -> "SendChan[T]":
        """A send only view of this channel, like Go's `chan<- T`."""
        return SendChan(self)


class RecvChan(Generic[T]):
    """A receive only view of a Chan, like Go's `<-chan T`.

    Hands code the ability to receive and iterate but not to send or
    close, so a consumer cannot corrupt the producer's side of the
    protocol. recv_case() accepts it. Get one from Chan.recv_only().
    """

    __slots__ = ("_chan",)

    def __init__(self, ch: "Chan[T]"):
        if not isinstance(ch, Chan):
            raise TypeError("RecvChan wraps a Chan")
        self._chan = ch

    def recv(self, timeout: Optional[float] = None) -> T:
        return self._chan.recv(timeout)

    def try_recv(self) -> Tuple[Optional[T], bool]:
        return self._chan.try_recv()

    @property
    def closed(self) -> bool:
        return self._chan.closed

    @property
    def cap(self) -> int:
        return self._chan.cap

    def __len__(self) -> int:
        return len(self._chan)

    def __iter__(self) -> Iterator[T]:
        return iter(self._chan)

    def __repr__(self) -> str:
        return f"<RecvChan of {self._chan!r}>"


class SendChan(Generic[T]):
    """A send only view of a Chan, like Go's `chan<- T`.

    Can send and close (closing is a sender's job in Go too) but not
    receive. send_case() accepts it. Get one from Chan.send_only().
    """

    __slots__ = ("_chan",)

    def __init__(self, ch: "Chan[T]"):
        if not isinstance(ch, Chan):
            raise TypeError("SendChan wraps a Chan")
        self._chan = ch

    def send(self, value: T, timeout: Optional[float] = None) -> None:
        self._chan.send(value, timeout)

    def try_send(self, value: T) -> bool:
        return self._chan.try_send(value)

    def close(self) -> None:
        self._chan.close()

    @property
    def closed(self) -> bool:
        return self._chan.closed

    @property
    def cap(self) -> int:
        return self._chan.cap

    def __len__(self) -> int:
        return len(self._chan)

    def __repr__(self) -> str:
        return f"<SendChan of {self._chan!r}>"
