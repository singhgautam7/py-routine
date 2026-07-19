"""Tests for Timer, ErrGroup.set_limit, aio.select, the one case select
fast path, and the once/synchronized decorators."""

import asyncio
import contextlib
import threading
import time

import pytest

from pyroutine import (
    Chan,
    ChanClosed,
    ErrGroup,
    Mutex,
    Timer,
    WaitGroup,
    aio,
    go,
    once,
    recv_case,
    select,
    send_case,
    synchronized,
)

# --------------------------------------------------------------------- #
# Timer
# --------------------------------------------------------------------- #


def test_timer_fires_and_delivers():
    t = Timer(0.05)
    val = t.chan.recv(timeout=5)
    assert isinstance(val, float)
    assert not t.stop()  # too late, it already fired


def test_timer_stop_before_firing_closes_channel():
    t = Timer(30)
    assert t.stop()
    with pytest.raises(ChanClosed):
        t.chan.recv(timeout=1)
    assert t.stop() is False  # idempotent, second stop reports False


def test_timer_stop_wakes_parked_select():
    t = Timer(30)
    work = Chan()

    def waiter():
        try:
            select(recv_case(work), recv_case(t.chan))
        except ChanClosed as e:
            return e.index

    h = go(waiter)
    time.sleep(0.05)  # let it park
    t.stop()
    assert h.result(timeout=5) == 1


def test_timer_rejects_negative_delay():
    with pytest.raises(ValueError):
        Timer(-1)


# --------------------------------------------------------------------- #
# select single case fast path
# --------------------------------------------------------------------- #


def test_single_case_select_recv_send_and_closed_index():
    ch = Chan(1)
    idx, val = select(send_case(ch, 5))
    assert (idx, val) == (0, None)
    idx, val = select(recv_case(ch))
    assert (idx, val) == (0, 5)
    with pytest.raises(TimeoutError):
        select(recv_case(ch), timeout=0.05)
    ch.close()
    with pytest.raises(ChanClosed) as ei:
        select(recv_case(ch))
    assert ei.value.index == 0


def test_single_case_select_with_default_still_polls():
    ch = Chan()
    assert select(recv_case(ch), default=True) == (-1, None)


# --------------------------------------------------------------------- #
# ErrGroup.set_limit
# --------------------------------------------------------------------- #


def test_errgroup_limit_caps_concurrency():
    eg = ErrGroup()
    eg.set_limit(2)
    active = [0]
    peak = [0]
    lock = threading.Lock()
    entered = Chan(8)
    release = Chan()

    def unit():
        with lock:
            active[0] += 1
            peak[0] = max(peak[0], active[0])
        entered.send(True)
        with contextlib.suppress(ChanClosed):
            release.recv()
        with lock:
            active[0] -= 1

    def spawn_all():
        for _ in range(6):
            eg.go(unit)  # blocks at the cap, so run in a routine

    spawner = go(spawn_all)
    # deterministic: wait until both permitted units are inside, then a
    # short settle so a third could wrongly slip in before we look
    assert entered.recv(timeout=10)
    assert entered.recv(timeout=10)
    time.sleep(0.15)
    with lock:
        assert active[0] == 2
        assert peak[0] == 2
    release.close()
    assert spawner.join(timeout=10)
    eg.wait(timeout=30)
    assert peak[0] == 2


def test_errgroup_limit_validation():
    eg = ErrGroup()
    with pytest.raises(ValueError):
        eg.set_limit(0)
    eg.set_limit(None)  # allowed, means unlimited
    eg.go(lambda: None)
    with pytest.raises(RuntimeError):
        eg.set_limit(3)
    eg.wait(timeout=30)


# --------------------------------------------------------------------- #
# aio.select
# --------------------------------------------------------------------- #


def test_aio_select_ready_case():
    async def main():
        a, b = Chan(1), Chan(1)
        b.send("ready")
        idx, val = await aio.select(recv_case(a), recv_case(b))
        assert (idx, val) == (1, "ready")

    asyncio.run(main())


def test_aio_select_parks_until_thread_sends():
    async def main():
        a, b = Chan(), Chan()
        go(lambda: b.send(7))
        idx, val = await aio.select(recv_case(a), recv_case(b))
        assert (idx, val) == (1, 7)

    asyncio.run(main())


def test_aio_select_default_and_timeout():
    async def main():
        ch = Chan()
        assert await aio.select(recv_case(ch), default=True) == (-1, None)
        with pytest.raises(TimeoutError):
            await aio.select(recv_case(ch), timeout=0.05)
        # the timed out registration must be gone
        go(lambda: ch.send(1))
        assert await aio.select(recv_case(ch), timeout=5) == (0, 1)

    asyncio.run(main())


def test_aio_select_send_case_and_closed_index():
    async def main():
        ch = Chan(1)
        idx, val = await aio.select(send_case(ch, 9))
        assert (idx, val) == (0, None)
        assert ch.recv() == 9
        ch.close()
        with pytest.raises(ChanClosed) as ei:
            await aio.select(recv_case(Chan()), recv_case(ch))
        assert ei.value.index == 1

    asyncio.run(main())


def test_aio_select_exactly_one_case_fires():
    """20 async selects race threads for 20 values across two channels."""

    async def main():
        a, b = Chan(), Chan()
        tasks = [
            asyncio.ensure_future(aio.select(recv_case(a), recv_case(b)))
            for _ in range(20)
        ]
        await asyncio.sleep(0.05)

        def feed():
            for i in range(10):
                a.send(("a", i))
                b.send(("b", i))

        go(feed)
        results = await asyncio.gather(*tasks)
        values = sorted(v for _, v in results)
        expected = sorted([("a", i) for i in range(10)] + [("b", i) for i in range(10)])
        assert values == expected

    asyncio.run(main())


# --------------------------------------------------------------------- #
# decorators
# --------------------------------------------------------------------- #


def test_once_runs_exactly_once_and_caches():
    calls = []

    @once
    def init():
        calls.append(1)
        return {"loaded": True}

    wg = WaitGroup()
    results = []
    lock = threading.Lock()

    def caller():
        r = init()
        with lock:
            results.append(r)

    for _ in range(8):
        wg.go(caller)
    assert wg.wait(timeout=30)
    assert len(calls) == 1
    assert all(r is results[0] for r in results)  # same object back


def test_once_reraises_first_exception():
    @once
    def broken():
        raise RuntimeError("first failure")

    with pytest.raises(RuntimeError, match="first failure"):
        broken()
    with pytest.raises(RuntimeError, match="first failure"):
        broken()  # not retried, same exception again


def test_synchronized_bare_form_excludes():
    counter = [0]

    @synchronized
    def bump():
        v = counter[0]
        time.sleep(0)  # invite a race if the lock were missing
        counter[0] = v + 1

    wg = WaitGroup()
    for _ in range(8):
        wg.go(lambda: [bump() for _ in range(200)])
    assert wg.wait(timeout=30)
    assert counter[0] == 1600


def test_synchronized_shared_mutex_form():
    m = Mutex()
    order = []

    @synchronized(m)
    def first():
        order.append("in-first")

    @synchronized(m)
    def second():
        order.append("in-second")

    m.lock()
    h1, h2 = go(first), go(second)
    time.sleep(0.05)
    assert order == []  # both excluded by the shared held mutex
    m.unlock()
    assert h1.join(timeout=5) and h2.join(timeout=5)
    assert sorted(order) == ["in-first", "in-second"]


def test_synchronized_rejects_garbage():
    with pytest.raises(TypeError):
        synchronized(42)
