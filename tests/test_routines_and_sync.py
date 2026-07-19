import time

import pytest

from pyroutine import Once, WaitGroup, go, routine


def test_go_returns_a_handle_with_result():
    h = go(lambda a, b: a + b, 2, 3)
    assert h.result(timeout=2.0) == 5
    assert h.done


def test_go_reraises_exceptions():
    def boom():
        raise ValueError("nope")

    h = go(boom)
    with pytest.raises(ValueError, match="nope"):
        h.result(timeout=2.0)


def test_routine_decorator_spawns_on_call():
    @routine
    def double(x):
        return x * 2

    handles = [double(i) for i in range(5)]
    assert [h.result(2.0) for h in handles] == [0, 2, 4, 6, 8]
    assert double.__name__ == "double"


def test_go_requires_callable():
    with pytest.raises(TypeError):
        go("not callable")


def test_waitgroup_waits_for_everyone():
    wg = WaitGroup()
    hits = []

    def worker(i):
        time.sleep(0.05)
        hits.append(i)

    for i in range(6):
        wg.go(worker, i)
    assert wg.wait(timeout=5.0)
    assert sorted(hits) == list(range(6))


def test_waitgroup_done_runs_even_on_crash():
    wg = WaitGroup()

    def crash():
        raise RuntimeError("worker died")

    h = wg.go(crash)
    assert wg.wait(timeout=2.0)
    # collect the exception so the unretrieved-exception hook stays quiet
    with pytest.raises(RuntimeError):
        h.result(timeout=5)


def test_waitgroup_negative_counter_raises():
    wg = WaitGroup()
    with pytest.raises(ValueError):
        wg.done()


def test_waitgroup_wait_timeout():
    wg = WaitGroup()
    wg.add(1)
    assert wg.wait(timeout=0.1) is False
    wg.done()
    assert wg.wait(timeout=2.0)


def test_once_runs_exactly_once():
    once = Once()
    calls = []
    wg = WaitGroup()
    for _ in range(8):
        wg.go(once.do, calls.append, 1)
    wg.wait(5.0)
    assert calls == [1]
