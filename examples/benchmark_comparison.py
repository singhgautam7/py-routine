"""The same pipeline three ways: threading, asyncio and pyroutine.

Every framework runs an identical two stage workload:

  Stage 1, I/O bound pipeline: N_JOBS simulated network calls (a sleep
  of IO_DELAY each) fanned out over WORKERS concurrent workers through
  a queue/channel pipeline, results collected at the end.

  Stage 2, CPU bound fan out: CPU_TASKS independent number crunching
  tasks spread over the framework's natural concurrency mechanism.

The timings are measured live on your machine and compared in a table,
nothing is hard coded. Expect the whole script to take 15 to 25 seconds.

Run with: python examples/benchmark_comparison.py
"""

import asyncio
import os
import queue
import sys
import threading
import time

# this script prints its own interpreter summary, so silence the notice
os.environ.setdefault("PYROUTINE_NO_GIL_WARNING", "1")

try:
    import pyroutine  # noqa: F401
except ModuleNotFoundError:
    # running from a source checkout without installing the package,
    # fall back to the in-repo sources
    sys.path.insert(
        0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
    )
    import pyroutine  # noqa: F401

from pyroutine import Chan, WaitGroup, free_threading, go

N_JOBS = 400           # simulated network calls in the I/O stage
WORKERS = 25           # concurrent workers in the I/O stage
IO_DELAY = 0.12        # seconds per simulated network call
CPU_TASKS = 8          # independent tasks in the CPU stage
CPU_LOOP = 15_000_000  # loop iterations per CPU task

EXPECTED = sorted(n * n for n in range(N_JOBS))

# ansi styling
RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
MAGENTA = "\033[35m"
CYAN = "\033[36m"


def simulated_fetch(job):
    time.sleep(IO_DELAY)  # a network round trip
    return job * job


def crunch(n=CPU_LOOP):
    total = 0
    for i in range(n):
        total += i * i
    return total


# --------------------------------------------------------------------- #
# traditional threading: threads + queue.Queue + sentinels + join
# --------------------------------------------------------------------- #

_STOP = object()


def threading_io():
    jobs = queue.Queue(maxsize=WORKERS)
    results = queue.Queue()

    def worker():
        while True:
            job = jobs.get()
            if job is _STOP:
                return
            results.put(simulated_fetch(job))

    threads = [threading.Thread(target=worker) for _ in range(WORKERS)]
    for t in threads:
        t.start()
    for n in range(N_JOBS):
        jobs.put(n)
    for _ in threads:
        jobs.put(_STOP)
    for t in threads:
        t.join()
    return [results.get() for _ in range(N_JOBS)]


def threading_cpu():
    out = [None] * CPU_TASKS

    def run(i):
        out[i] = crunch()

    threads = [threading.Thread(target=run, args=(i,)) for i in range(CPU_TASKS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return out


# --------------------------------------------------------------------- #
# asyncio: tasks + asyncio.Queue, to_thread for the CPU stage
# --------------------------------------------------------------------- #


async def _aio_io():
    jobs = asyncio.Queue(maxsize=WORKERS)
    results = []

    async def worker():
        while True:
            job = await jobs.get()
            if job is None:
                return
            await asyncio.sleep(IO_DELAY)  # the async spelling of the fetch
            results.append(job * job)

    workers = [asyncio.create_task(worker()) for _ in range(WORKERS)]
    for n in range(N_JOBS):
        await jobs.put(n)
    for _ in workers:
        await jobs.put(None)
    await asyncio.gather(*workers)
    return results


def asyncio_io():
    return asyncio.run(_aio_io())


async def _aio_cpu():
    return await asyncio.gather(*[asyncio.to_thread(crunch) for _ in range(CPU_TASKS)])


def asyncio_cpu():
    return asyncio.run(_aio_cpu())


# --------------------------------------------------------------------- #
# pyroutine: go() + channels + WaitGroup, reads like the Go version
# --------------------------------------------------------------------- #


def pyroutine_io():
    jobs = Chan(WORKERS)
    results = Chan()
    wg = WaitGroup()

    def producer():
        for n in range(N_JOBS):
            jobs.send(n)
        jobs.close()

    def worker():
        for job in jobs:
            results.send(simulated_fetch(job))

    go(producer)
    for _ in range(WORKERS):
        wg.go(worker)
    go(lambda: (wg.wait(), results.close()))
    return list(results)


def pyroutine_cpu():
    handles = [go(crunch) for _ in range(CPU_TASKS)]
    return [h.result() for h in handles]


# --------------------------------------------------------------------- #
# measurement and reporting
# --------------------------------------------------------------------- #


def timed(fn):
    t0 = time.perf_counter()
    out = fn()
    return time.perf_counter() - t0, out


def hr(width=66, ch="─"):
    return DIM + ch * width + RESET


def main():
    ft = free_threading()
    gil_note = (
        f"{GREEN}disabled (free threading, threads run in parallel){RESET}"
        if ft
        else f"{YELLOW}enabled (threads interleave on one core){RESET}"
    )
    print()
    print(f"{BOLD}{MAGENTA}py-routine benchmark: threading vs asyncio vs pyroutine{RESET}")
    print(hr())
    print(f"  python      : {CYAN}{sys.version.split()[0]}{RESET}")
    print(f"  GIL         : {gil_note}")
    io_floor = N_JOBS / WORKERS * IO_DELAY
    print(
        f"  I/O stage   : {CYAN}{N_JOBS}{RESET} calls of {CYAN}{IO_DELAY:.2f}s{RESET}"
        f" over {CYAN}{WORKERS}{RESET} workers"
    )
    print(
        f"  CPU stage   : {CYAN}{CPU_TASKS}{RESET} tasks"
        f" of {CYAN}{CPU_LOOP:,}{RESET} loop iterations"
    )
    sequential_estimate = N_JOBS * IO_DELAY
    print(
        f"  {DIM}the I/O stage is {io_floor:.1f}s of pure waiting ({N_JOBS}/{WORKERS}"
        f" x {IO_DELAY:.2f}s). That floor is the workload itself, no framework,"
        f"{RESET}\n  {DIM}interpreter or language can beat it (sequentially it"
        f" would be {sequential_estimate:.0f}s). The CPU{RESET}\n  {DIM}stage is"
        f" where the interpreter matters, watch that column below.{RESET}"
    )
    print(hr())

    frameworks = [
        ("threading", YELLOW, threading_io, threading_cpu),
        ("asyncio", BLUE, asyncio_io, asyncio_cpu),
        ("pyroutine", CYAN, pyroutine_io, pyroutine_cpu),
    ]

    rows = []
    for name, color, io_fn, cpu_fn in frameworks:
        print(f"\n{BOLD}{color}▶ {name}{RESET}")
        io_t, io_res = timed(io_fn)
        if sorted(io_res) != EXPECTED:
            raise AssertionError(f"{name} produced wrong I/O results")
        print(
            f"    I/O pipeline : {BOLD}{io_t:6.2f}s{RESET}"
            f"  {DIM}({N_JOBS} jobs, all results verified){RESET}"
        )
        cpu_t, _ = timed(cpu_fn)
        print(f"    CPU fan out  : {BOLD}{cpu_t:6.2f}s{RESET}  {DIM}({CPU_TASKS} tasks){RESET}")
        total = io_t + cpu_t
        print(f"    total        : {BOLD}{color}{total:6.2f}s{RESET}")
        rows.append((name, color, io_t, cpu_t, total))

    slowest = max(r[4] for r in rows)
    fastest = min(r[4] for r in rows)

    print(f"\n{BOLD}{MAGENTA}Results{RESET}")
    print(f"{DIM}┌───────────┬─────────┬─────────┬─────────┬────────────────────┐{RESET}")
    print(
        f"{DIM}│{RESET} {BOLD}framework {RESET}{DIM}│{RESET} {BOLD}    I/O {RESET}{DIM}│{RESET}"
        f" {BOLD}    CPU {RESET}{DIM}│{RESET} {BOLD}  total {RESET}{DIM}│{RESET}"
        f" {BOLD}vs slowest         {RESET}{DIM}│{RESET}"
    )
    print(f"{DIM}├───────────┼─────────┼─────────┼─────────┼────────────────────┤{RESET}")
    for name, color, io_t, cpu_t, total in rows:
        pct = (slowest - total) / slowest * 100
        if total == slowest:
            text, mark = "baseline (slowest)", RED
        elif pct < 1.0:
            text, mark = "~same as slowest", DIM
        else:
            text = f"{pct:.1f}% faster"
            mark = GREEN if total == fastest else YELLOW
        verdict = f"{mark}{text:<19}{RESET}"
        star = f"{GREEN}★{RESET}" if total == fastest else " "
        print(
            f"{DIM}│{RESET}{star}{color}{name:<10}{RESET}{DIM}│{RESET}"
            f" {io_t:6.2f}s {DIM}│{RESET} {cpu_t:6.2f}s {DIM}│{RESET}"
            f" {BOLD}{total:6.2f}s{RESET} {DIM}│{RESET} {verdict}{DIM}│{RESET}"
        )
    print(f"{DIM}└───────────┴─────────┴─────────┴─────────┴────────────────────┘{RESET}")

    print()
    if ft:
        cpu_avg = sum(r[3] for r in rows) / len(rows)
        print(
            f"{GREEN}Free threaded build: the CPU stage ran across your cores in"
            f" parallel ({cpu_avg:.2f}s here,{RESET}\n{GREEN}several times longer"
            f" on a GIL build). Of the total, {io_floor:.1f}s is the I/O waiting"
            f" floor,{RESET}\n{GREEN}which no interpreter can shrink. Run this same"
            f" file with a GIL Python to compare.{RESET}"
        )
    else:
        print(
            f"{YELLOW}Note:{RESET} with the GIL enabled, every framework is bound by the same"
            f" waits, so the\nconcurrent approaches finish close together (and roughly"
            f" {BOLD}{sequential_estimate / slowest:.0f}x{RESET} faster"
            f" than\nsequential code would)."
            f" The interesting column is ergonomics: compare pyroutine_io()\nagainst threading_io()"
            f" in this file. On a free threaded build (3.13+/3.14, GIL\ndisabled) the CPU column"
            f" also drops by up to {CPU_TASKS}x for the thread based frameworks."
        )
    print()


if __name__ == "__main__":
    main()
