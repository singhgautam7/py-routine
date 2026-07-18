import threading
import time

import pytest

from pyroutine import Chan, ChanClosed, go


def test_buffered_send_recv():
    ch = Chan(3)
    ch.send(1)
    ch.send(2)
    assert len(ch) == 2
    assert ch.recv() == 1
    assert ch.recv() == 2


def test_unbuffered_is_a_rendezvous():
    ch = Chan()
    delivered = threading.Event()

    def sender():
        ch.send("hello")
        delivered.set()

    go(sender)
    # sender must still be blocked, nobody has received yet
    assert not delivered.wait(0.15)
    assert ch.recv() == "hello"
    assert delivered.wait(2.0)


def test_send_timeout_on_full_buffer():
    ch = Chan(1)
    ch.send(1)
    with pytest.raises(TimeoutError):
        ch.send(2, timeout=0.1)
    # the timed out value must not sneak into the channel later
    assert ch.recv() == 1
    _, ok = ch.try_recv()
    assert not ok


def test_recv_timeout():
    ch = Chan()
    with pytest.raises(TimeoutError):
        ch.recv(timeout=0.1)
    # a later send must still pair with a fresh receiver
    go(ch.send, 42)
    assert ch.recv(timeout=2.0) == 42


def test_close_drains_buffer_then_raises():
    ch = Chan(4)
    ch.send(1)
    ch.send(2)
    ch.close()
    assert ch.recv() == 1
    assert ch.recv() == 2
    with pytest.raises(ChanClosed):
        ch.recv()
    with pytest.raises(ChanClosed):
        ch.send(3)


def test_close_wakes_blocked_receiver():
    ch = Chan()
    result = {}

    def receiver():
        try:
            ch.recv()
        except ChanClosed:
            result["closed"] = True

    h = go(receiver)
    time.sleep(0.1)
    ch.close()
    assert h.join(2.0)
    assert result.get("closed")


def test_close_wakes_blocked_sender():
    ch = Chan()
    result = {}

    def sender():
        try:
            ch.send(1)
        except ChanClosed:
            result["closed"] = True

    h = go(sender)
    time.sleep(0.1)
    ch.close()
    assert h.join(2.0)
    assert result.get("closed")


def test_iteration_stops_on_close():
    ch = Chan(8)
    for i in range(5):
        ch.send(i)
    ch.close()
    assert list(ch) == [0, 1, 2, 3, 4]


def test_try_send_unbuffered_needs_a_waiting_receiver():
    ch = Chan()
    assert ch.try_send(1) is False

    got = {}

    def receiver():
        got["v"] = ch.recv()

    h = go(receiver)
    # give the receiver a moment to park, then try_send should succeed
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        if ch.try_send("direct"):
            break
        time.sleep(0.01)
    else:
        pytest.fail("try_send never found the parked receiver")
    assert h.join(2.0)
    assert got["v"] == "direct"


def test_try_recv():
    ch = Chan(1)
    _, ok = ch.try_recv()
    assert not ok
    ch.send("x")
    v, ok = ch.try_recv()
    assert ok and v == "x"


def test_context_manager_closes():
    with Chan(1) as ch:
        ch.send(1)
    assert ch.closed
    assert ch.recv() == 1


def test_stress_many_producers_and_consumers():
    ch = Chan(16)
    n_producers, per_producer = 8, 200
    total = Chan(1)
    lock = threading.Lock()
    acc = {"sum": 0, "count": 0}
    expected_count = n_producers * per_producer
    expected_sum = n_producers * sum(range(per_producer))

    def producer():
        for i in range(per_producer):
            ch.send(i)

    def consumer():
        for v in ch:
            with lock:
                acc["sum"] += v
                acc["count"] += 1
                if acc["count"] == expected_count:
                    total.send(acc["sum"])

    handles = [go(producer) for _ in range(n_producers)]
    for _ in range(4):
        go(consumer)
    for h in handles:
        assert h.join(10.0)
    assert total.recv(timeout=10.0) == expected_sum
    ch.close()
