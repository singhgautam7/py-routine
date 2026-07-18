"""Tests for the pyroutine.aio asyncio bridge."""

import asyncio

import pytest

from pyroutine import Chan, ChanClosed, WaitGroup, go
from pyroutine import aio


def test_aio_recv_fast_path_buffered():
    async def main():
        ch = Chan(2)
        ch.send(1)
        ch.send(2)
        assert await aio.recv(ch) == 1
        assert await aio.recv(ch) == 2

    asyncio.run(main())


def test_aio_recv_parks_until_thread_sends():
    async def main():
        ch = Chan()  # unbuffered, the task must park
        go(lambda: ch.send("from-thread"))
        assert await aio.recv(ch) == "from-thread"

    asyncio.run(main())


def test_aio_send_parks_until_thread_receives():
    async def main():
        ch = Chan()
        h = go(ch.recv)
        await aio.send(ch, "to-thread")
        assert h.result(timeout=5) == "to-thread"

    asyncio.run(main())


def test_aio_timeouts():
    async def main():
        ch = Chan()
        with pytest.raises(TimeoutError):
            await aio.recv(ch, timeout=0.05)
        with pytest.raises(TimeoutError):
            await aio.send(ch, 1, timeout=0.05)
        # the timed out waiters must be gone, a real exchange still works
        go(lambda: ch.send(42))
        assert await aio.recv(ch, timeout=5) == 42

    asyncio.run(main())


def test_aio_closed_channel():
    async def main():
        ch = Chan(1)
        ch.send(1)
        ch.close()
        assert await aio.recv(ch) == 1  # drain
        with pytest.raises(ChanClosed):
            await aio.recv(ch)
        with pytest.raises(ChanClosed):
            await aio.send(ch, 2)

    asyncio.run(main())


def test_aio_close_wakes_parked_task():
    async def main():
        ch = Chan()
        task = asyncio.ensure_future(aio.recv(ch))
        await asyncio.sleep(0.05)  # let the task park
        go(ch.close)
        with pytest.raises(ChanClosed):
            await task

    asyncio.run(main())


def test_aio_iterate_drains_pipeline():
    async def main():
        ch = Chan(8)
        wg = WaitGroup()

        def producer(lo, hi):
            for n in range(lo, hi):
                ch.send(n)

        wg.go(producer, 0, 50)
        wg.go(producer, 50, 100)
        go(lambda: (wg.wait(), ch.close()))

        got = [v async for v in aio.iterate(ch)]
        assert sorted(got) == list(range(100))

    asyncio.run(main())


def test_aio_directional_views():
    async def main():
        ch = Chan(1)
        await aio.send(ch.send_only(), 9)
        assert await aio.recv(ch.recv_only()) == 9
        with pytest.raises(TypeError):
            await aio.recv(ch.send_only())
        with pytest.raises(TypeError):
            await aio.send(ch.recv_only(), 1)

    asyncio.run(main())


def test_aio_many_tasks_one_channel():
    """One value per parked async task, exactly once each."""

    async def main():
        ch = Chan()
        tasks = [asyncio.ensure_future(aio.recv(ch)) for _ in range(20)]
        await asyncio.sleep(0.05)  # let every task park

        def feed():
            for i in range(20):
                ch.send(i)

        go(feed)
        results = await asyncio.gather(*tasks)
        assert sorted(results) == list(range(20))

    asyncio.run(main())
