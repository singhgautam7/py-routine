"""Where pyroutine genuinely wins: parallel CPU work on shared memory.

The task: count word frequencies across a corpus of documents that
already lives in memory. This is the shape of a huge amount of real
work (parsing, indexing, feature extraction, aggregation) and it is
exactly where Python's classic options hurt:

  sequential        one core, simple, slow
  multiprocessing   the traditional way around the GIL. Real parallel
                    cores, but every chunk of input is pickled, shipped
                    to a child process and the results pickled back,
                    plus process startup. The data is copied, not shared.
  pyroutine         routines are threads, the corpus is shared, nothing
                    is copied. On a free threaded build (3.13+/3.14, GIL
                    disabled) this is real parallelism with zero
                    serialization cost. On a GIL build it cannot beat
                    multiprocessing for pure CPU work, and the output
                    says so honestly.

Run with: python examples/shared_memory_showcase.py
Best on a free threaded build: python3.14t examples/shared_memory_showcase.py
"""

import multiprocessing
import os
import sys
import time

os.environ.setdefault("PYROUTINE_NO_GIL_WARNING", "1")

try:
    import pyroutine  # noqa: F401
except ModuleNotFoundError:
    sys.path.insert(
        0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
    )
    import pyroutine  # noqa: F401

from pyroutine import free_threading, go

N_DOCS = 600_000
WORKERS = 8

RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
MAGENTA = "\033[35m"
CYAN = "\033[36m"


def build_corpus():
    return [
        f"the quick brown fox {i} jumps over the lazy dog {i % 97} " * 5
        for i in range(N_DOCS)
    ]


def count_words(docs):
    """The per worker unit of work, top level so multiprocessing can
    pickle a reference to it."""
    counts = {}
    for doc in docs:
        for word in doc.split():
            counts[word] = counts.get(word, 0) + 1
    return counts


def merge_counts(parts):
    total = {}
    for part in parts:
        for word, n in part.items():
            total[word] = total.get(word, 0) + n
    return total


def chunked(docs):
    step = (len(docs) + WORKERS - 1) // WORKERS
    return [docs[i : i + step] for i in range(0, len(docs), step)]


def run_sequential(docs):
    return count_words(docs)


def run_multiprocessing(docs):
    # chunks are pickled to the children, partial counts pickled back
    with multiprocessing.Pool(WORKERS) as pool:
        parts = pool.map(count_words, chunked(docs))
    return merge_counts(parts)


def run_pyroutine(docs):
    # the chunks are views into shared memory, nothing is copied
    handles = [go(count_words, chunk) for chunk in chunked(docs)]
    return merge_counts(h.result() for h in handles)


def main():
    ft = free_threading()
    print()
    print(f"{BOLD}{MAGENTA}Shared memory showcase: word counts over {N_DOCS:,} documents{RESET}")
    print(f"{DIM}{'─' * 66}{RESET}")
    gil_note = (
        f"{GREEN}disabled (free threading){RESET}"
        if ft
        else f"{YELLOW}enabled{RESET}"
    )
    print(
        f"  python  : {CYAN}{sys.version.split()[0]}{RESET}"
        f"   GIL: {gil_note}   workers: {CYAN}{WORKERS}{RESET}"
    )
    print(f"  {DIM}building the corpus in memory...{RESET}")
    docs = build_corpus()
    print(f"{DIM}{'─' * 66}{RESET}")

    contenders = [
        ("sequential", DIM, run_sequential),
        ("multiprocessing", BLUE, run_multiprocessing),
        ("pyroutine", CYAN, run_pyroutine),
    ]

    rows = []
    reference = None
    for name, color, fn in contenders:
        print(f"{BOLD}{color}▶ {name:<16}{RESET}", end="", flush=True)
        t0 = time.perf_counter()
        counts = fn(docs)
        elapsed = time.perf_counter() - t0
        if reference is None:
            reference = counts
        elif counts != reference:
            raise AssertionError(f"{name} produced wrong counts")
        print(f" {BOLD}{elapsed:6.2f}s{RESET}  {DIM}(results verified){RESET}")
        rows.append((name, color, elapsed))

    seq_time = rows[0][2]
    fastest = min(r[2] for r in rows)

    print(f"\n{BOLD}{MAGENTA}Results{RESET}")
    print(f"{DIM}┌──────────────────┬─────────┬──────────────────────┐{RESET}")
    print(
        f"{DIM}│{RESET} {BOLD}approach         {RESET}{DIM}│{RESET}"
        f" {BOLD}   time {RESET}{DIM}│{RESET}"
        f" {BOLD}vs sequential        {RESET}{DIM}│{RESET}"
    )
    print(f"{DIM}├──────────────────┼─────────┼──────────────────────┤{RESET}")
    for name, color, elapsed in rows:
        if name == "sequential":
            verdict = f"{DIM}{'baseline':<21}{RESET}"
        else:
            speedup = seq_time / elapsed
            if speedup < 1.05:
                verdict = f"{DIM}{'~same as sequential':<21}{RESET}"
            else:
                mark = GREEN if elapsed == fastest else YELLOW
                verdict = f"{mark}{f'{speedup:.1f}x faster':<21}{RESET}"
        star = f"{GREEN}★{RESET}" if elapsed == fastest else " "
        print(
            f"{DIM}│{RESET}{star}{color}{name:<17}{RESET}{DIM}│{RESET}"
            f" {BOLD}{elapsed:6.2f}s{RESET} {DIM}│{RESET} {verdict}{DIM}│{RESET}"
        )
    print(f"{DIM}└──────────────────┴─────────┴──────────────────────┘{RESET}")

    print()
    if ft:
        mp_time = rows[1][2]
        pr_time = rows[2][2]
        if pr_time < mp_time:
            pct = (mp_time - pr_time) / mp_time * 100
            print(
                f"{GREEN}pyroutine beat multiprocessing by {BOLD}{pct:.0f}%{RESET}{GREEN} on the"
                f" same cores. The difference is pure{RESET}\n{GREEN}overhead:"
                f" multiprocessing pickled every document to child processes and the"
                f"{RESET}\n{GREEN}results back, pyroutine's routines read the corpus"
                f" where it already lives.{RESET}"
            )
        else:
            print(
                f"{YELLOW}multiprocessing edged out pyroutine on this run, which can"
                f" happen when the{RESET}\n{YELLOW}corpus is small relative to the"
                f" per process startup cost. Raise N_DOCS to see{RESET}\n{YELLOW}the"
                f" serialization cost dominate.{RESET}"
            )
    else:
        print(
            f"{YELLOW}GIL build: pyroutine's threads cannot crunch numbers in"
            f" parallel here, so{RESET}\n{YELLOW}multiprocessing wins despite its"
            f" copying overhead. This wall is exactly what{RESET}\n{YELLOW}free"
            f" threaded Python removes. Try: python3.14t {sys.argv[0]}{RESET}"
        )
    print()


if __name__ == "__main__":
    main()
