"""A classic Go worker pool, in Python.

Run with: python examples/worker_pool.py
"""

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
