"""py-routine benchmark suite (roadmap item 2 in CLAUDE.md).

Compares pyroutine against threading, asyncio and multiprocessing on
six scenarios that cover the library's claims:

  spawn        cost of starting and joining many concurrent units
  throughput   1 producer -> 1 consumer message stream
  pingpong     rendezvous round trip latency between two workers
  select8      one consumer multiplexing eight independent sources
  cpu          CPU bound fan out over 8 tasks
  words        shared memory aggregation (word counts over a corpus)

Every implementation of a scenario does the same verified work. Run it
on both interpreters and compare:

    python benchmarks/run.py
    python3.14t benchmarks/run.py

Total runtime is around a minute per interpreter.
"""

import asyncio
import json
import multiprocessing
import os
import platform
import queue
import sys
import threading
import time

os.environ.setdefault("PYROUTINE_NO_GIL_WARNING", "1")

try:
    import pyroutine  # noqa: F401
except ModuleNotFoundError:
    sys.path.insert(
        0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
    )
    import pyroutine  # noqa: F401

from pyroutine import (
    Chan,
    ChanClosed,
    Select,
    WaitGroup,
    free_threading,
    go,
    recv_case,
    select,
)

# PYROUTINE_BENCH_SCALE shrinks the workloads proportionally, used by
# the CI regression job to keep runs short; ratios stay comparable
_SCALE = float(os.environ.get("PYROUTINE_BENCH_SCALE", "1"))


def _scaled(n: int, floor: int = 50) -> int:
    return max(floor, int(n * _SCALE))


SPAWN_N = _scaled(3_000)
THROUGHPUT_N = _scaled(200_000)
PINGPONG_N = _scaled(20_000)
SELECT_SOURCES = 8
SELECT_MSGS = _scaled(5_000)
CPU_TASKS = 8
CPU_LOOP = _scaled(4_000_000)
WORD_DOCS = _scaled(300_000)

RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
MAGENTA = "\033[35m"
CYAN = "\033[36m"


def timed(fn):
    t0 = time.perf_counter()
    fn()
    return time.perf_counter() - t0


# --------------------------------------------------------------------- #
# spawn: start and join SPAWN_N concurrent units doing trivial work
# --------------------------------------------------------------------- #


def spawn_threading():
    threads = [threading.Thread(target=int) for _ in range(SPAWN_N)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def spawn_pyroutine():
    handles = [go(int) for _ in range(SPAWN_N)]
    for h in handles:
        h.join()


def spawn_asyncio():
    async def main():
        async def unit():
            return 0

        await asyncio.gather(*[unit() for _ in range(SPAWN_N)])

    asyncio.run(main())


# --------------------------------------------------------------------- #
# throughput: one producer streams THROUGHPUT_N ints to one consumer
# --------------------------------------------------------------------- #


def throughput_threading():
    q = queue.Queue(maxsize=1024)

    def producer():
        for i in range(THROUGHPUT_N):
            q.put(i)
        q.put(None)

    got = 0

    def consumer():
        nonlocal got
        while q.get() is not None:
            got += 1

    t1 = threading.Thread(target=producer)
    t2 = threading.Thread(target=consumer)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    assert got == THROUGHPUT_N


def throughput_pyroutine():
    ch = Chan(1024)

    def producer():
        for i in range(THROUGHPUT_N):
            ch.send(i)
        ch.close()

    got = 0

    def consumer():
        nonlocal got
        for _ in ch:
            got += 1

    h1 = go(producer)
    h2 = go(consumer)
    h1.join()
    h2.join()
    assert got == THROUGHPUT_N


def throughput_asyncio():
    async def main():
        q = asyncio.Queue(maxsize=1024)
        got = 0

        async def producer():
            for i in range(THROUGHPUT_N):
                await q.put(i)
            await q.put(None)

        async def consumer():
            nonlocal got
            while await q.get() is not None:
                got += 1

        await asyncio.gather(producer(), consumer())
        assert got == THROUGHPUT_N

    asyncio.run(main())


# --------------------------------------------------------------------- #
# pingpong: PINGPONG_N synchronous round trips between two workers
# --------------------------------------------------------------------- #


def pingpong_threading():
    a, b = queue.Queue(maxsize=1), queue.Queue(maxsize=1)

    def ponger():
        for _ in range(PINGPONG_N):
            b.put(a.get())

    t = threading.Thread(target=ponger)
    t.start()
    for i in range(PINGPONG_N):
        a.put(i)
        b.get()
    t.join()


def pingpong_pyroutine():
    a, b = Chan(), Chan()  # true rendezvous channels

    def ponger():
        for _ in range(PINGPONG_N):
            b.send(a.recv())

    h = go(ponger)
    for i in range(PINGPONG_N):
        a.send(i)
        b.recv()
    h.join()


def pingpong_asyncio():
    async def main():
        a, b = asyncio.Queue(maxsize=1), asyncio.Queue(maxsize=1)

        async def ponger():
            for _ in range(PINGPONG_N):
                await b.put(await a.get())

        task = asyncio.create_task(ponger())
        for i in range(PINGPONG_N):
            await a.put(i)
            await b.get()
        await task

    asyncio.run(main())


# --------------------------------------------------------------------- #
# select8: one consumer draining eight independent bursty sources.
# threading has no select, the standard idiom is a forwarder thread per
# source feeding one merged queue. pyroutine multiplexes directly.
# --------------------------------------------------------------------- #

_TOTAL_SELECT = SELECT_SOURCES * SELECT_MSGS


def select8_threading():
    sources = [queue.Queue(maxsize=64) for _ in range(SELECT_SOURCES)]
    merged = queue.Queue(maxsize=256)

    def producer(q):
        for i in range(SELECT_MSGS):
            q.put(i)
        q.put(None)

    def forwarder(q):
        while True:
            item = q.get()
            if item is None:
                merged.put(None)
                return
            merged.put(item)

    threads = [threading.Thread(target=producer, args=(q,)) for q in sources]
    threads += [threading.Thread(target=forwarder, args=(q,)) for q in sources]
    for t in threads:
        t.start()
    got, closed = 0, 0
    while closed < SELECT_SOURCES:
        if merged.get() is None:
            closed += 1
        else:
            got += 1
    for t in threads:
        t.join()
    assert got == _TOTAL_SELECT


def select8_pyroutine():
    sources = [Chan(64) for _ in range(SELECT_SOURCES)]

    def producer(ch):
        for i in range(SELECT_MSGS):
            ch.send(i)
        ch.close()

    for ch in sources:
        go(producer, ch)
    got = 0
    live = list(sources)
    while live:
        try:
            _, _ = select(*[recv_case(c) for c in live])
            got += 1
        except ChanClosed as e:
            del live[e.index]
    assert got == _TOTAL_SELECT


def select8_pyroutine_prepared():
    sources = [Chan(64) for _ in range(SELECT_SOURCES)]

    def producer(ch):
        for i in range(SELECT_MSGS):
            ch.send(i)
        ch.close()

    for ch in sources:
        go(producer, ch)
    got = 0
    live = list(sources)
    sel = Select(*[recv_case(c) for c in live])  # rebuilt only on close
    while live:
        try:
            _, _ = sel.wait()
            got += 1
        except ChanClosed as e:
            del live[e.index]
            if live:
                sel = Select(*[recv_case(c) for c in live])
    assert got == _TOTAL_SELECT


def select8_asyncio():
    async def main():
        sources = [asyncio.Queue(maxsize=64) for _ in range(SELECT_SOURCES)]

        async def producer(q):
            for i in range(SELECT_MSGS):
                await q.put(i)
            await q.put(None)

        for q in sources:
            asyncio.ensure_future(producer(q))
        got = 0
        pending = {asyncio.ensure_future(q.get()): q for q in sources}
        while pending:
            done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                q = pending.pop(task)
                if task.result() is not None:
                    got += 1
                    pending[asyncio.ensure_future(q.get())] = q
        assert got == _TOTAL_SELECT

    asyncio.run(main())


# --------------------------------------------------------------------- #
# cpu: fan CPU_TASKS crunches out over the natural mechanism
# --------------------------------------------------------------------- #


def crunch(n=CPU_LOOP):
    total = 0
    for i in range(n):
        total += i * i
    return total


def cpu_sequential():
    for _ in range(CPU_TASKS):
        crunch()


def cpu_threading():
    threads = [threading.Thread(target=crunch) for _ in range(CPU_TASKS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def cpu_pyroutine():
    wg = WaitGroup()
    for _ in range(CPU_TASKS):
        wg.go(crunch)
    assert wg.wait(timeout=120)


def cpu_asyncio():
    async def main():
        async def unit():
            crunch()  # pure asyncio has nowhere else to run it

        await asyncio.gather(*[unit() for _ in range(CPU_TASKS)])

    asyncio.run(main())


def cpu_multiprocessing():
    with multiprocessing.Pool(CPU_TASKS) as pool:
        pool.map(crunch, [CPU_LOOP] * CPU_TASKS)


# --------------------------------------------------------------------- #
# words: shared memory aggregation over an in-memory corpus
# --------------------------------------------------------------------- #


def count_words(docs):
    counts = {}
    for doc in docs:
        for word in doc.split():
            counts[word] = counts.get(word, 0) + 1
    return counts


_corpus = None


def _get_corpus():
    global _corpus
    if _corpus is None:
        _corpus = [
            f"the quick brown fox {i} jumps over the lazy dog {i % 97} " * 5
            for i in range(WORD_DOCS)
        ]
    return _corpus


def _chunks(docs, n=CPU_TASKS):
    step = (len(docs) + n - 1) // n
    return [docs[i : i + step] for i in range(0, len(docs), step)]


def words_sequential():
    count_words(_get_corpus())


def words_pyroutine():
    handles = [go(count_words, c) for c in _chunks(_get_corpus())]
    for h in handles:
        h.result()


def words_multiprocessing():
    with multiprocessing.Pool(CPU_TASKS) as pool:
        pool.map(count_words, _chunks(_get_corpus()))


# --------------------------------------------------------------------- #
# runner
# --------------------------------------------------------------------- #

SCENARIOS = [
    (
        "spawn",
        f"start + join {SPAWN_N:,} concurrent units",
        [
            ("threading", spawn_threading),
            ("asyncio", spawn_asyncio),
            ("pyroutine", spawn_pyroutine),
        ],
    ),
    (
        "throughput",
        f"stream {THROUGHPUT_N:,} messages, 1 producer -> 1 consumer",
        [
            ("threading", throughput_threading),
            ("asyncio", throughput_asyncio),
            ("pyroutine", throughput_pyroutine),
        ],
    ),
    (
        "pingpong",
        f"{PINGPONG_N:,} synchronous round trips",
        [
            ("threading", pingpong_threading),
            ("asyncio", pingpong_asyncio),
            ("pyroutine", pingpong_pyroutine),
        ],
    ),
    (
        "select8",
        f"drain {SELECT_SOURCES} sources x {SELECT_MSGS:,} messages from one consumer",
        [
            ("threading", select8_threading),
            ("asyncio", select8_asyncio),
            ("pyroutine", select8_pyroutine),
            ("pyroutine-sel", select8_pyroutine_prepared),
        ],
    ),
    (
        "cpu",
        f"{CPU_TASKS} tasks x {CPU_LOOP:,} iteration crunch",
        [
            ("sequential", cpu_sequential),
            ("threading", cpu_threading),
            ("asyncio", cpu_asyncio),
            ("multiprocessing", cpu_multiprocessing),
            ("pyroutine", cpu_pyroutine),
        ],
    ),
    (
        "words",
        f"word counts over {WORD_DOCS:,} in-memory documents",
        [
            ("sequential", words_sequential),
            ("multiprocessing", words_multiprocessing),
            ("pyroutine", words_pyroutine),
        ],
    ),
]


def main():
    ft = free_threading()
    build = "free threaded (GIL disabled)" if ft else "GIL enabled"
    print()
    print(f"{BOLD}{MAGENTA}py-routine benchmark suite{RESET}")
    print(
        f"{DIM}python {sys.version.split()[0]} [{build}] on {platform.machine()},"
        f" {os.cpu_count()} cpus{RESET}"
    )
    json_path = None
    only = set()
    for arg in sys.argv[1:]:
        if arg.startswith("--json="):
            json_path = arg.split("=", 1)[1]
        else:
            only.add(arg)
    all_results = []  # (key, desc, [(approach, seconds), ...])
    for key, desc, impls in SCENARIOS:
        if only and key not in only:
            continue
        print(f"\n{BOLD}{CYAN}▶ {key}{RESET}  {DIM}{desc}{RESET}")
        results = []
        for name, fn in impls:
            if sys.stdout.isatty():
                print(f"    {DIM}running {name}...{RESET}", end="\r", flush=True)
            elapsed = timed(fn)
            results.append((name, elapsed))
            print(f"    {name:<16} {BOLD}{elapsed:7.3f}s{RESET}          ")
        all_results.append((key, desc, results))

    # ----------------------------------------------------------------- #
    # results table
    # ----------------------------------------------------------------- #
    print(
        f"\n{BOLD}{MAGENTA}Results{RESET}"
        f"  {DIM}(python {sys.version.split()[0]}, {build}){RESET}"
    )
    print(f"{DIM}┌────────────┬──────────────────┬─────────┬─────────────┐{RESET}")
    print(
        f"{DIM}│{RESET} {BOLD}scenario   {RESET}{DIM}│{RESET}"
        f" {BOLD}approach         {RESET}{DIM}│{RESET}"
        f" {BOLD}   time {RESET}{DIM}│{RESET} {BOLD}vs fastest  {RESET}{DIM}│{RESET}"
    )
    for key, _, results in all_results:
        print(f"{DIM}├────────────┼──────────────────┼─────────┼─────────────┤{RESET}")
        best = min(t for _, t in results)
        shown_key = key
        for name, t in results:
            if t == best:
                verdict = f"{GREEN}{'fastest':<12}{RESET}"
                star = f"{GREEN}★{RESET}"
            else:
                verdict = f"{YELLOW}{f'{t / best:.1f}x slower':<12}{RESET}"
                star = " "
            color = CYAN if name == "pyroutine" else ""
            print(
                f"{DIM}│{RESET} {shown_key:<11}{DIM}│{RESET}"
                f"{star}{color}{name:<17}{RESET}{DIM}│{RESET}"
                f" {BOLD}{t:6.3f}s{RESET} {DIM}│{RESET} {verdict}{DIM}│{RESET}"
            )
            shown_key = ""
    print(f"{DIM}└────────────┴──────────────────┴─────────┴─────────────┘{RESET}")

    # ----------------------------------------------------------------- #
    # summary: where pyroutine stands, generated from the numbers
    # ----------------------------------------------------------------- #
    print(f"\n{BOLD}{MAGENTA}Summary{RESET}")
    for key, _, results in all_results:
        times = dict(results)
        if "pyroutine" not in times:
            continue
        pr = times["pyroutine"]
        best_name, best_t = min(results, key=lambda r: r[1])
        if best_name == "pyroutine":
            others = [(n, t) for n, t in results if n != "pyroutine"]
            runner_name, runner_t = min(others, key=lambda r: r[1])
            print(
                f"  {GREEN}✓ {key:<11}{RESET} pyroutine fastest,"
                f" {runner_t / pr:.1f}x ahead of {runner_name}"
            )
        elif pr <= best_t * 1.15:
            print(
                f"  {YELLOW}~ {key:<11}{RESET} pyroutine on par with"
                f" {best_name} ({pr / best_t:.2f}x)"
            )
        else:
            print(
                f"  {YELLOW}✗ {key:<11}{RESET} {best_name} fastest,"
                f" pyroutine {pr / best_t:.1f}x behind"
            )
    if ft:
        print(f"\n  {GREEN}Free threaded build: thread based approaches used all cores.{RESET}")
    else:
        print(
            f"\n  {YELLOW}GIL build: CPU scenarios cannot parallelize with threads here."
            f" Re-run on a{RESET}\n  {YELLOW}free threaded build (python3.14t) to see"
            f" the cpu and words rows change.{RESET}"
        )
    print()

    if json_path:
        payload = {
            "python": sys.version.split()[0],
            "free_threading": ft,
            "machine": platform.machine(),
            "cpus": os.cpu_count(),
            "scale": _SCALE,
            "results": {
                key: dict(results) for key, _, results in all_results
            },
        }
        with open(json_path, "w") as f:
            json.dump(payload, f, indent=2)
        print(f"{DIM}wrote {json_path}{RESET}")


if __name__ == "__main__":
    main()
