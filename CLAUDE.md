# CLAUDE.md

Project context for Claude Code. Read this fully before touching any code.

## What this project is

py-routine (import name: `pyroutine`) brings Go's concurrency model to
Python: routines spawned with `go()`, channels (`Chan`), `select()` over
multiple channel operations, `WaitGroup` and `Once`. Pure Python, zero
dependencies, designed primarily for free threaded CPython (3.13+ with the
GIL disabled) where threads give real parallelism, but it must keep working
correctly on regular GIL builds down to Python 3.9.

Layout:

```
src/pyroutine/
  __init__.py     public API surface, keep it small and stable
  _chan.py        Chan, ChanClosed, _Waiter (the core, most delicate file)
  _select.py      select(), recv_case(), send_case(), after()
  _routines.py    Handle, go(), @routine
  _sync.py        WaitGroup, Once
tests/            pytest suite, includes race and stress tests
```

## What is currently implemented (v1 status)

- Channels use waiter registration, not polling. Every blocking op parks
  a `_Waiter` (an Event plus a compare and set `commit()`), and whoever
  completes the operation wins the waiter via `commit()` before calling
  `finish()`. Dead waiters (already claimed by another case) are skipped
  and discarded lazily by the poll loops.
- Unbuffered channels are a true rendezvous. Buffered channels promote a
  parked sender into the buffer whenever a slot frees up
  (`_promote_sender_locked`).
- `select()` follows Go's runtime design: acquire the locks of ALL
  involved channels in canonical order (sorted by `id()`), check every
  case in random shuffled order for fairness, and either complete one
  case immediately or register one shared waiter on every channel, then
  release all locks and sleep. On wake, leftover registrations are
  removed with `_unregister()`.
- Timeouts everywhere use waiter self commit with `_TIMEOUT_INDEX`. If
  the self commit loses the race at the deadline, the operation counts
  as completed and the code waits for `finish()`.
- `close()` snapshots and clears the waiter queues under the lock, then
  commits and finishes each waiter with `_CLOSED` outside the lock.
- Routines are one daemon OS thread each (`Handle` in `_routines.py`).
  Exceptions are captured and re-raised from `result()`.
- `go(fn, *args)` always spawns. The decorator is separate as
  `@routine`. Do not merge them, the zero argument case is ambiguous
  and caused a real deadlock in an earlier draft.

## Locking invariants, absolutely strict

Breaking any of these introduces deadlocks or lost wakeups that only show
up under load. Treat them as law:

1. Regular channel operations hold at most ONE channel lock at a time.
2. Only `select()` may hold multiple channel locks, and only by acquiring
   them sorted by `id()` and releasing in reverse. Never add another code
   path that takes two channel locks.
3. `_Waiter._lock` is a leaf lock. It is taken only inside `commit()`.
   Never call `commit()` while holding another waiter's lock.
4. `finish()` is called only by whoever won `commit()` on that waiter,
   and always after the commit.
5. Never call anything that can block (send, recv, wait, join, I/O)
   while holding a channel lock.
6. Methods suffixed `_locked` assume the caller holds `self._lock`.
   Keep that naming convention.

## Strict do and do not

Do:

- Keep the library pure Python. No C extension until benchmarks prove a
  specific hot path needs it, and even then behind a pure Python fallback.
- Keep every public API working identically on GIL and free threaded
  builds. Run the test suite on both before merging anything.
- Add a regression test for every concurrency bug fixed. Stress tests
  should use channels and WaitGroup for coordination, never sleeps as
  the primary synchronization (small sleeps to let a thread park are
  acceptable in tests, keep them under 0.2s and pair with generous
  wait timeouts).
- Preserve exception semantics: send/recv/select raise `ChanClosed`
  on closed channels, `TimeoutError` on deadlines. `select` sets
  `ChanClosed.index`.
- Keep `__init__.py` exports and signatures backward compatible. This
  is a library, the API is the product.

Do not:

- Do not introduce polling or busy wait loops anywhere, including in
  tests of production code paths. The whole point of v1 is that idle
  costs nothing. If you find yourself writing `while True: try_x();
  sleep(...)` inside the library, stop and use waiter registration.
- Do not add per operation allocations beyond the single `_Waiter` on
  the blocking path. The fast paths (buffer hit, direct handoff) must
  stay allocation light.
- Do not use `time.time()` for deadlines, only `time.monotonic()`.
- Do not add dependencies. Standard library only.
- Do not swallow exceptions from user callables. Capture and re-raise
  from `result()`.
- Do not make `Chan` or `select` async/await based. An asyncio bridge is
  a future separate module, the core stays synchronous.
- Do not use em dashes in code comments, docs or README. House style.

## Performance notes

- The GIL build is the compatibility target, the free threaded build is
  the performance target. Benchmark on 3.14 free threaded before and
  after any change to `_chan.py` or `_select.py`.
- Contention lives on `Chan._lock`. Keep critical sections tiny: no
  logging, no allocation heavy work, no user code under the lock.
- `deque` append/popleft are the queue primitives, keep them. The
  `_unregister` rebuild is O(n) but only runs on the slow path
  (timeouts and select cleanup), that trade is intentional.

## Future plans, in priority order

1. M:N scheduler: multiplex routines over a small thread pool so a
   parked routine does not pin an OS thread. Must not change the public
   API. The tricky part is parking/unparking without the current
   one thread per routine assumption in `Handle`.
2. Benchmark suite (`benchmarks/`) comparing threading, asyncio,
   multiprocessing and pyroutine on 3.14 GIL and free threaded builds.
   Publish numbers in the README.
3. Generic typing: `Chan[int]`, typed select cases. Runtime behavior
   unchanged, purely static.
4. Context/cancellation in the spirit of Go's context package
   (deadline propagation, done channels).
5. asyncio bridge as `pyroutine.aio`: awaitable recv/send wrappers.

## Commands

```
pip install -e ".[dev]"      # dev install
pytest -q                    # run tests
for i in 1 2 3 4 5; do pytest -q; done   # concurrency bugs are shy, repeat
python -m build              # build sdist and wheel
```

Releases: bump version in pyproject.toml, update CHANGELOG.md, tag and
publish a GitHub release. The publish.yml workflow pushes to PyPI via
trusted publishing.
