"""A classic Go worker pool, in Python.

Run with: python examples/worker_pool.py
"""

try:
    import pyroutine  # noqa: F401
except ModuleNotFoundError:
    # running from a source checkout without installing the package,
    # fall back to the in-repo sources
    import os
    import sys

    sys.path.insert(
        0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
    )

from pyroutine import Chan, WaitGroup, go, recv_case, select, after

jobs = Chan(4)
results = Chan()
wg = WaitGroup()


def producer():
    for n in range(20):
        jobs.send(n)
    jobs.close()


def worker(wid):
    for n in jobs:
        results.send((wid, n, n * n))


def closer():
    wg.wait()
    results.close()


def main():
    go(producer)
    for wid in range(3):
        wg.go(worker, wid)
    go(closer)

    done_ch = after(5.0)  # safety timeout for the whole run
    while True:
        try:
            idx, val = select(recv_case(results), recv_case(done_ch))
        except Exception:
            break  # results closed, all work done
        if idx == 1:
            print("timed out")
            break
        wid, n, sq = val
        print(f"worker {wid}: {n}^2 = {sq}")


if __name__ == "__main__":
    main()
