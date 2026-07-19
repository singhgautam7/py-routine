# py-routine

[![CI](https://github.com/singhgautam7/py-routine/actions/workflows/ci.yml/badge.svg)](https://github.com/singhgautam7/py-routine/actions/workflows/ci.yml)
[![Benchmarks](https://github.com/singhgautam7/py-routine/actions/workflows/bench.yml/badge.svg)](https://github.com/singhgautam7/py-routine/actions/workflows/bench.yml)

Go style concurrency for Python: routines, channels, `select`, `WaitGroup`
and `Once`.

If you have written Go, you already know this API. If you have not, the short
version is: instead of sharing state and guarding it with locks, you pass
values between concurrent routines over channels. The locks still exist, but
they live inside the library where they belong.

```python
from pyroutine import Chan, WaitGroup, go

jobs = Chan(4)        # buffered channel, like make(chan int, 4)
results = Chan()      # unbuffered, send blocks until someone receives
wg = WaitGroup()

def producer():
    for n in range(10):
        jobs.send(n)
    jobs.close()

def worker(wid):
    for n in jobs:                 # receive until the channel closes
        results.send((wid, n * n))

go(producer)
for wid in range(3):
    wg.go(worker, wid)             # add(1) + spawn + done() in one call

go(lambda: (wg.wait(), results.close()))

for wid, square in results:
    print(f"worker {wid} produced {square}")
```

## Why

Python already has `threading`, `asyncio` and `multiprocessing`. What it does
not have is Go's model: cheap concurrent routines that talk over rendezvous
points, `select` over several of them at once, and structured teardown with
`WaitGroup`. That model composes unusually well and it does not split your
codebase into sync and async halves.

The timing matters too. Free threaded CPython (3.13 experimental, officially
supported in 3.14) removes the GIL, which means threads finally run Python
code in parallel. This library is written for that world: pure Python, no C
extension, pooled worker threads, and every blocking primitive parks on
a condition instead of polling. On a free threaded build, CPU bound fan out
across routines actually uses your cores.

## Why pyroutine and not threading, asyncio or multiprocessing

The short version, every row measured on this repo's own benchmarks
(commands below, full tables in the Benchmarks section, all on Python
3.14.6 free threaded unless stated):

| you are doing | reach for | measured reason |
|---|---|---|
| parallel CPU work on shared data (3.13t/3.14t) | **pyroutine, clear winner** | 4.3x over sequential and 25 to 50% ahead of multiprocessing, because nothing is pickled to child processes. asyncio is stuck on one core, 4.6x slower |
| streaming pipelines (producer to consumer) | **pyroutine** | `Chan` is about 2x faster than `queue.Queue` and edges out `asyncio.Queue`, on every build including 3.9+ GIL |
| request/response coordination | **pyroutine** | rendezvous round trips match threading and beat asyncio 3x |
| spawning many short lived tasks | pyroutine over threading, asyncio overall | 9x faster than `threading.Thread` (pooled workers), still 4x behind asyncio tasks |
| I/O bound fan out | any of the three | all within a few percent, the wait is the workload. pyroutine just reads best |
| CPU work on a GIL build (3.12 and older) | multiprocessing, honestly | nothing thread based can beat the GIL there |

And where the speed is a tie, the API is not. Channels with `select`,
`WaitGroup`/`ErrGroup`, contexts with deadlines, timers as channels,
directional endpoints, exceptions that cannot vanish silently:
threading has none of that, and asyncio offers its equivalents only if
you rewrite your codebase async. pyroutine works with every blocking
library you already use.

Verify all of it on your machine:

```
# the six scenario suite vs threading, asyncio and multiprocessing
python benchmarks/run.py
python3.14t benchmarks/run.py               # free threaded build

# the multiprocessing pickling tax, in isolation
python3.14t examples/shared_memory_showcase.py

# one identical I/O + CPU pipeline written in all three frameworks
python examples/benchmark_comparison.py
```

## Install

```
pip install py-routine
```

No dependencies. Python 3.9+. The import name is `pyroutine`:

```python
import pyroutine
```

### Running the bundled examples from a source checkout

If you cloned this repo and `python examples/worker_pool.py` fails with
`ModuleNotFoundError: No module named 'pyroutine'`, the package is not
installed in the Python you are running. From the repo root:

```
pip install -e ".[dev]"       # editable install, code changes apply live
python examples/worker_pool.py
```

The examples also carry a small fallback that adds `src/` to `sys.path`, so
they run from a fresh clone even without installing. Installing editable is
still the recommended setup for development.

## Parallelism and the GIL

Routines are real OS threads, always. What differs by interpreter is
whether those threads can run Python bytecode simultaneously:

- On Python 3.12 and older there is no way around the GIL at all. Only
  one thread runs Python code at a time, routines interleave. That is
  exactly what you want for I/O bound and coordination heavy work, but
  CPU bound routines will not get faster, period.
- On Python 3.13 and 3.14 the GIL is *optional*, but only in the separate
  free threaded build (the `python3.13t` / `python3.14t` binaries, on
  Homebrew `brew install python-freethreading`). A normal 3.13/3.14
  install still has the GIL and behaves like 3.12.
- On a free threaded build the same routines run in parallel across all
  cores with zero code changes. In our benchmark
  (`examples/benchmark_comparison.py`) the CPU bound stage drops about
  5x moving from 3.12 to 3.14 free threaded on an 8 core machine.

Because this difference is easy to miss, `pyroutine` warns once at import
time when the GIL is enabled, in yellow when stderr is a terminal:

```
GILEnabledWarning: pyroutine: this interpreter has the GIL enabled, so
routines interleave on one core instead of running in parallel. ...
```

You can check programmatically:

```python
import pyroutine

pyroutine.free_threading()   # True only on a free threaded build with GIL off
```

If you know what you are doing (say, an I/O bound service on a regular
build), disable the warning with an environment variable, either outside:

```
PYROUTINE_NO_GIL_WARNING=1 python app.py
```

or at the top of your entry point, before the first import:

```python
import os
os.environ.setdefault("PYROUTINE_NO_GIL_WARNING", "1")
import pyroutine
```

The warning also respects the `NO_COLOR` convention (color is dropped)
and standard `warnings` filters against the `GILEnabledWarning` category
still apply if you prefer that machinery.

## Python versions: what changes where

One wheel, zero code changes, but the interpreter decides how much
parallelism you get. The dependency is not the version number alone, it
is whether the build has the GIL:

| interpreter | GIL | routines run | what pyroutine gives you there |
|---|---|---|---|
| 3.9 to 3.12 | always on | interleaved, one core | the Go API and its wins that never needed parallelism: 2x `queue.Queue` streaming, fast rendezvous, 4x faster spawning than `threading.Thread`, select/context/errgroup. For CPU parallelism, keep multiprocessing |
| 3.13, 3.14 regular build | on | interleaved, one core | exactly the same as 3.12, a regular 3.13/3.14 install changes nothing |
| 3.13t, 3.14t free threaded | off | parallel, all cores | everything above, plus CPU bound routines actually scale and shared memory parallel work overtakes multiprocessing |

How the same benchmarks move between 3.12 (GIL) and 3.14t on an 11 core
machine, from `benchmarks/run.py`:

| scenario | 3.12 GIL | 3.14t | what happened |
|---|---|---|---|
| cpu, 8 crunches | 0.99s | 0.19s | 5.2x, the cores finally count |
| words, shared corpus | 1.14s | 0.26s | 4.4x, and it overtakes multiprocessing (0.40s) |
| throughput | 0.07s | 0.10s | coordination cost, not compute; roughly flat |
| pingpong | 0.16s | 0.14s | flat, wakeups dominate |
| spawn | 0.025s | 0.025s | flat, pooled either way |

So: on 3.12 and older you adopt pyroutine for the model and the
coordination wins, and the CPU rows stay multiprocessing territory. On
3.13t/3.14t the same program also becomes your parallelism story. To get
a free threaded build: `brew install python-freethreading` on macOS, the
"free-threaded Python" checkbox in the python.org installer on Windows,
or `uv python install 3.14t` anywhere. `pyroutine.free_threading()`
tells you at runtime which world you are in.

## The full API tour

Everything the package exports, with examples:

- Routines: `go`, `routine`, `Handle`
- Channels: `Chan`, `ChanClosed`, `RecvChan`, `SendChan`, `merge`
- Multiplexing and timers: `select`, `Select`, `recv_case`, `send_case`,
  `after`, `Timer`, `tick`
- Cancellation: `Context`, `background`, `with_cancel`, `with_timeout`,
  `with_deadline`, `Canceled`, `DeadlineExceeded`
- Sync: `WaitGroup`, `ErrGroup`, `Once`, `Mutex`, `RWMutex`
- Decorators: `@routine`, `@once`, `@synchronized`
- Asyncio bridge: `pyroutine.aio` with awaitable `recv`, `send`,
  `select`, `iterate`
- Failure visibility: `set_excepthook`, `enable_deadlock_detection`,
  `DeadlockWarning`
- Interpreter introspection: `free_threading`, `GILEnabledWarning`

### Spawning routines: `go()`

`go(fn, *args, **kwargs)` spawns `fn` immediately on its own daemon thread,
like `go f(x)` in Go, and returns a `Handle`:

```python
from pyroutine import go

def fetch(url, retries=3):
    ...
    return body

h = go(fetch, "https://example.com", retries=5)
```

### `Handle`: join, results and exceptions

The `Handle` returned by `go()` is a tiny Future:

```python
h.join()                # block until the routine finishes
h.join(timeout=1.0)     # returns True if it finished, False on timeout

body = h.result()       # join AND get the return value
body = h.result(timeout=2.0)   # raises TimeoutError if still running

h.done                  # non blocking: has it finished yet?
```

Exceptions raised inside a routine are never silently dropped. They are
captured and re-raised, with their original traceback, from `result()`:

```python
def boom():
    raise ValueError("nope")

h = go(boom)
h.join()          # completes fine, the routine is simply done
h.result()        # raises ValueError: nope, right here in the caller
```

If you never call `result()`, the exception is still reported when the
handle is garbage collected, through the excepthook described in the
Failure visibility section. Nothing vanishes.

Planning to park thousands of routines at once? Shrink the worker
stacks first; the OS default (often 8 MiB of reserved address space
per thread) is sized for call depths you probably do not have:

```python
from pyroutine import set_worker_stack_size

set_worker_stack_size(512 * 1024)   # affects workers created afterwards
```

### The `@routine` decorator

Same thing, decorator form. Calling the decorated function spawns it:

```python
from pyroutine import routine

@routine
def fetch(url):
    return get(url)

h = fetch("https://example.com")   # runs concurrently, h is a Handle
page = h.result()
```

`go()` and `@routine` are deliberately separate functions. Overloading one
name to act as both a spawner and a decorator makes zero argument calls
ambiguous.

### Channels: `Chan`

```python
from pyroutine import Chan

ch = Chan()      # unbuffered: send() blocks until a recv() takes the value
ch = Chan(8)     # buffered: send() only blocks when 8 values are queued
```

An unbuffered channel is a true rendezvous, exactly like Go: the sender and
receiver meet, the value passes, both continue. That property is what makes
unbuffered channels useful as synchronization points, not just pipes.

Blocking operations:

```python
ch.send(value)               # blocks per the rules above
value = ch.recv()            # blocks until a value arrives

ch.send(value, timeout=1.0)  # raises TimeoutError if not delivered in time
value = ch.recv(timeout=1.0) # raises TimeoutError if nothing arrives
```

Non blocking variants, like Go's `select` with a `default` on one channel:

```python
ok = ch.try_send(value)      # True if delivered or buffered, never blocks
value, ok = ch.try_recv()    # (value, True) or (None, False), never blocks
```

On an unbuffered channel `try_send` succeeds only when a receiver is
already parked and waiting.

Closing:

```python
ch.close()      # idempotent, safe to call twice
```

Close follows Go semantics exactly:

- `send()` to a closed channel raises `ChanClosed`.
- Buffered values survive close and can still be received (the channel
  drains).
- `recv()` on a closed and drained channel raises `ChanClosed`.
- Everyone currently blocked on the channel wakes up immediately.

Introspection:

```python
ch.closed       # bool, has close() been called
ch.cap          # buffer capacity, like cap(ch); 0 for unbuffered
len(ch)         # values currently buffered, like len(ch)
```

Iteration receives until the channel is closed and drained, then stops
cleanly, like `for v := range ch` in Go:

```python
for value in ch:
    handle(value)
# loop exits when ch is closed and empty, no exception escapes
```

And a channel is a context manager that closes itself on exit:

```python
with Chan(16) as ch:
    go(producer, ch)
    consume_some(ch)
# ch.close() has been called, producers unblock with ChanClosed
```

### `ChanClosed`

The one exception class for closed channel conditions:

```python
from pyroutine import ChanClosed

try:
    ch.send(item)
except ChanClosed:
    ...   # channel was closed under us
```

When `select()` raises it, the `.index` attribute tells you which case hit
the closed channel (see below). For plain `send`/`recv` it is `None`.

### `select()` over multiple operations

`select` waits on several channel operations at once and performs exactly
one of them, with the same guarantees as Go's `select` statement:

```python
from pyroutine import select, recv_case, send_case

idx, val = select(
    recv_case(inbox),           # case 0: value = received item
    recv_case(control),         # case 1
    send_case(outbox, item),    # case 2: value = None on completion
)
if idx == 0:
    handle(val)
elif idx == 1:
    reconfigure(val)
else:
    pass  # item was sent
```

Rules, all matching Go:

- If several cases are ready, one is picked pseudo randomly for fairness.
- Exactly one case fires, even when many `select` calls race over the
  same channels for the same value. No value is ever delivered twice or
  lost.
- While blocked it consumes zero CPU. There is no polling loop anywhere.

Non blocking poll, like Go's `select` with a `default:` branch:

```python
idx, val = select(recv_case(inbox), default=True)
if idx == -1:
    ...   # nothing was ready, we did not block
```

With a deadline, two equivalent spellings:

```python
idx, val = select(recv_case(inbox), timeout=1.0)   # raises TimeoutError

idx, val = select(recv_case(inbox), recv_case(after(1.0)))
if idx == 1:
    ...   # the timer fired first
```

If the winning case hit a closed channel, `select` raises `ChanClosed` with
`.index` set so you know which one:

```python
try:
    idx, val = select(recv_case(a), recv_case(b))
except ChanClosed as e:
    print(f"case {e.index} is closed")
```

Selecting over the same channels in a loop? Prepare it once with
`Select`, which does select()'s per call setup (validation, channel
dedup, canonical lock ordering) a single time:

```python
from pyroutine import Select

sel = Select(recv_case(inbox), recv_case(control))
while True:
    idx, val = sel.wait()      # same semantics, timeout and default too
    ...
```

One `Select` belongs to one routine, exactly like a select statement
belongs to one goroutine; two routines each build their own over the
same channels. When a case's channel closes it keeps raising
`ChanClosed` on every later `wait()`, so drop that case and build a new
`Select` from the survivors, the same way a `select()` loop would.

### `after()` and `Timer`: timer channels

`after(seconds)` returns a channel that receives one value (the current
`time.monotonic()`) after the delay, then closes. It is Go's
`time.After`, and its natural home is a `select` case:

```python
from pyroutine import after, recv_case, select

idx, val = select(recv_case(work), recv_case(after(0.5)))
if idx == 1:
    print("no work for 500ms")
```

When the timeout might become irrelevant before it fires, use `Timer`,
which is `after()` plus a `stop()`. Stopping closes the timer's channel,
so anything parked on it wakes with `ChanClosed` instead of holding a
registration for a deadline nobody wants anymore:

```python
from pyroutine import Timer

t = Timer(5.0)
try:
    idx, val = select(recv_case(replies), recv_case(t.chan))
finally:
    t.stop()      # True if it had not fired yet
```

### `WaitGroup`

Go's `sync.WaitGroup`, for waiting on a batch of routines:

```python
from pyroutine import WaitGroup

wg = WaitGroup()

# the manual Go style
wg.add(1)
def task():
    try:
        work()
    finally:
        wg.done()
go(task)

# the shortcut you will actually use: add(1) + spawn + done() in one call,
# done() runs even if the routine raises
wg.go(worker, arg1, key=value)

wg.wait()               # block until the counter hits zero
wg.wait(timeout=30)     # returns True if it hit zero, False on timeout
```

`wg.go()` also returns the `Handle`, so you can still collect results or
re-raise errors after `wait()`.

### `Once`

Run something exactly once no matter how many routines race to do it,
like `sync.Once`:

```python
from pyroutine import Once

once = Once()

def init_pool():
    global pool
    pool = create_expensive_pool()

# every routine can call this safely, init_pool runs a single time
once.do(init_pool)
```

Later calls return immediately without calling the function, including
when the first call raised.

### `tick()`: repeating timer channels

Where `after()` fires once, `tick(seconds)` returns a channel that
receives the current `time.monotonic()` roughly every interval, like
Go's `time.Tick`. The channel is buffered at one, so a slow receiver
misses ticks instead of piling them up. Close the channel to stop the
ticker:

```python
from pyroutine import tick

beat = tick(1.0)
for t in beat:
    push_heartbeat()
    if shutting_down:
        beat.close()      # stops the ticker, the loop exits
```

Inside a select it gives you periodic work alongside real events:

```python
idx, val = select(recv_case(events), recv_case(beat))
if idx == 1:
    flush_metrics()
```

### Context and cancellation

Go programs thread a `context.Context` through everything that should
stop on request or on a deadline. Same here:

```python
from pyroutine import with_cancel, with_timeout, recv_case, select, ChanClosed

ctx, cancel = with_cancel()          # child of background() by default

def worker(ctx, jobs):
    while True:
        try:
            idx, job = select(recv_case(jobs), recv_case(ctx.done()))
        except ChanClosed as e:
            return                   # jobs closed, or ctx cancelled
        process(job)

go(worker, ctx, jobs)
...
cancel()                             # every routine watching ctx unwinds
```

The pieces:

- `ctx.done()` is a channel that closes on cancellation. Receiving from
  it (directly or in a `select`) raises `ChanClosed` at that moment,
  which is the wake up signal, the same way a closed data channel ends
  a `for` loop.
- `ctx.err()` is `None` while live, then a `Canceled` or
  `DeadlineExceeded` explaining why it died. `ctx.cancelled()` is the
  boolean shortcut. Cheap enough to check inside loops.
- `with_cancel(parent)` derives a child context. Cancelling a parent
  cancels all its children, transitively. Cancelling a child never
  touches the parent.
- `with_timeout(seconds, parent)` and `with_deadline(monotonic_time,
  parent)` self cancel with `DeadlineExceeded` when time runs out. A
  child's deadline is capped to its parent's, deadlines only ever
  shrink down the tree.
- `DeadlineExceeded` subclasses both `Canceled` (one except clause
  handles every cancellation) and `TimeoutError` (reads naturally).
- `background()` is the empty root: never cancelled, no deadline.

```python
ctx, cancel = with_timeout(5.0)
try:
    result = fetch_with_ctx(ctx, url)
finally:
    cancel()    # always call cancel, it detaches the child from the parent
```

### `ErrGroup`: WaitGroup with error handling

`WaitGroup` waits; `ErrGroup` waits and deals with failure, like
`golang.org/x/sync/errgroup`. The first routine that raises cancels the
group's context, and `wait()` re-raises that first exception:

```python
from pyroutine import ErrGroup

eg = ErrGroup()
for url in urls:
    eg.go(fetch_into_db, url, eg.ctx)   # pass eg.ctx so they can stop early

eg.wait()    # blocks for all, then re-raises the first error, if any
```

Routines that might run long watch `eg.ctx` (via `select` on
`eg.ctx.done()` or `eg.ctx.err()` checks) and bail out once a sibling
has failed. `eg.go()` still returns the routine's `Handle` when you
want individual results, and `wait()` cancels the group context on the
way out even on success, exactly like Go.

Bound the fan out with `set_limit`, like Go's `errgroup.SetLimit`:

```python
eg = ErrGroup()
eg.set_limit(8)              # at most 8 routines in flight
for url in thousands_of_urls:
    eg.go(fetch, url)        # blocks here while 8 are already running
eg.wait()
```

### Directional channels: `RecvChan` and `SendChan`

Go APIs say `chan<- T` (send only) or `<-chan T` (receive only) to make
misuse impossible. The equivalent here:

```python
ch = Chan(8)

def producer(out):          # out: SendChan, cannot recv or iterate
    for item in source:
        out.send(item)
    out.close()             # closing is the sender's job, so it is allowed

def consumer(inp):          # inp: RecvChan, cannot send or close
    for item in inp:
        handle(item)

go(producer, ch.send_only())
go(consumer, ch.recv_only())
```

The views add no overhead worth mentioning, they just delegate to the
underlying channel, and `recv_case()`/`send_case()` accept the matching
view. Passing the wrong direction to a select case is a `TypeError`.

### `merge()`: fan in

Combining several channels into one is the utility every Go codebase
reinvents. Provided here once:

```python
from pyroutine import merge

out = merge(feed_a, feed_b, feed_c)   # accepts Chan or RecvChan views
for value in out:                     # everything from all three feeds
    handle(value)
# the loop ends once ALL inputs are closed and drained
```

One background routine multiplexes all inputs with `select`, so merging
N channels costs one thread, not N. `merge(..., maxsize=32)` buffers the
output. Closing the output channel early stops the merge.

### `Mutex` and `RWMutex`

Channels first, but sometimes a plain lock is the honest answer.
`Mutex` is `threading.Lock` with Go's names and context manager
support:

```python
from pyroutine import Mutex

m = Mutex()
with m:                     # or m.lock() / m.unlock()
    shared += 1
```

`RWMutex` is `sync.RWMutex`, which the stdlib does not offer: any
number of readers or exactly one writer, with writer preference so a
steady stream of readers cannot starve writers:

```python
from pyroutine import RWMutex

rw = RWMutex()

with rw.read():             # many routines may hold this at once
    snapshot = dict(config)

with rw.write():            # exclusive
    config[key] = value
```

Method forms `rlock()/runlock()/lock()/unlock()` exist for when a
`with` block does not fit. As in Go, do not take `rlock()` recursively
in one routine, a waiting writer between the two acquisitions
deadlocks you.

### Decorators: `@once` and `@synchronized`

(`@routine`, the "spawn on call" decorator, is covered under Routines.)

`@once` makes a function run at most once, no matter how many routines
race to call it. Everyone gets the first call's return value back, and
if the first call raised, everyone gets that same exception. It is
Go's `sync.OnceValues` as a decorator, ideal for lazy singletons:

```python
from pyroutine import once

@once
def db_pool():
    return create_expensive_pool()

db_pool()    # creates the pool
db_pool()    # same pool object, instantly, from any routine
```

`@synchronized` runs the function body under a `Mutex`. The bare form
gives the function its own private lock; passing a `Mutex` shares one
lock across several functions:

```python
from pyroutine import Mutex, synchronized

@synchronized                # private lock, calls serialize
def bump_counter():
    state["n"] += 1

m = Mutex()

@synchronized(m)             # deposit and withdraw exclude each other
def deposit(x): ...

@synchronized(m)
def withdraw(x): ...
```

### Failure visibility: lost exceptions and deadlocks

Go refuses to let concurrency failures vanish: an unrecovered panic
crashes the program, and a fully blocked program dies with "all
goroutines are asleep - deadlock!". pyroutine mirrors both, the
Python way.

**Exceptions can never disappear silently.** A routine's exception is
re-raised from `result()`. If nobody ever calls `result()`, the
exception is reported to stderr when the `Handle` is garbage
collected, the same protection asyncio gives Tasks:

```
pyroutine: exception in routine 'pyroutine-7-fetch' was never retrieved
Traceback (most recent call last):
  ...
ValueError: bad response
```

Route it somewhere else (your logger, your crash reporter, or re-raise
to crash hard like Go) with a hook:

```python
from pyroutine import set_excepthook

set_excepthook(lambda handle, exc: log.error("routine died", exc_info=exc))
set_excepthook(None)     # back to the stderr default
```

`ErrGroup` handles are exempt: the group already delivers the first
error from `wait()`.

**Deadlock detection** is opt-in, made for tests and development:

```python
from pyroutine import enable_deadlock_detection

enable_deadlock_detection(interval=0.5)
```

A background watcher checks whether every running routine is blocked
forever inside a pyroutine primitive (channel op, `select`, `join`,
`WaitGroup.wait`, all without timeouts) with no pyroutine timer
pending that could wake anything. Seeing the same stuck picture twice
in a row, it issues a `DeadlockWarning` naming the blocked routines
and stating whether the main thread is stuck too. Unlike Go's runtime
we do not own every thread in the process, so this warns instead of
crashing: an outside thread or an asyncio task could still legally
complete a channel operation the watcher cannot see. Waits with a
timeout are never reported, they wake themselves.

### Typed channels: `Chan[int]`

Channels are generic. Annotate them and type checkers follow values
through `send`, `recv`, iteration and the directional views. The type
parameter is purely static, runtime behavior is identical:

```python
ch: Chan[int] = Chan(8)
ch.send(3)            # ok
ch.send("three")      # flagged by mypy/pyright, runs like before

def consume(inp: RecvChan[int]) -> int:
    return sum(inp)   # values are known to be ints
```

`select` cases carry their types too. A select over recv cases infers
the union of the channels' element types:

```python
ints: Chan[int] = Chan()
strs: Chan[str] = Chan()

pair = select(recv_case(ints), recv_case(strs))
# pair: tuple[int, int | str] to mypy and pyright
```

### The asyncio bridge: `pyroutine.aio`

The core library is synchronous on purpose, but async code can talk to
the same channels through `pyroutine.aio`. That lets a threaded
pipeline and an asyncio front end (a web handler, a websocket) meet in
the middle without rewriting either side:

```python
from pyroutine import Chan, go
from pyroutine import aio

ch = Chan(64)
go(blocking_producer, ch)          # routines fill the channel...

async def handler():
    async for item in aio.iterate(ch):    # ...async code drains it
        await push_to_client(item)

value = await aio.recv(ch, timeout=1.0)   # awaitable recv, TimeoutError on deadline
await aio.send(ch, value)                 # awaitable send, ChanClosed semantics match

idx, val = await aio.select(              # awaitable select, same cases,
    recv_case(events),                    # same fairness and exactly once
    recv_case(control),                   # guarantees as the sync one
    timeout=1.0,
)
```

The bridge registers the same waiters blocking threads use and resolves
an asyncio future via `call_soon_threadsafe` when the operation
completes, so there is no polling and no hidden executor thread. An
idle bridge costs nothing. One caveat: a task cancelled at the exact
moment a thread completes its operation cannot undo that operation
(for a cancelled `aio.recv` the delivered value is dropped), so prefer
`iterate()` plus `close()` for shutdown.

## How it compares

Against the tools Python already gives you:

| capability | threading | asyncio | multiprocessing | pyroutine |
|---|---|---|---|---|
| parallel CPU on free threaded builds | yes | no (one loop) | yes | yes |
| parallel CPU without copying/pickling data | yes | - | no | yes |
| works with blocking libraries (requests, DB drivers) | yes | needs async ports | yes | yes |
| no sync/async split in your codebase | yes | no, colored functions | yes | yes |
| block on several sources at once | polling or extra threads | only inside the loop | no | `select()` |
| rendezvous (unbuffered) channels | no | no | no | yes |
| cancellation contexts with deadlines | manual flags | `Task.cancel` | terminate | `Context` |
| first-error group teardown | manual | `TaskGroup` (3.11+) | no | `ErrGroup` |
| exceptions surface at join | no, printed to stderr | yes | partially | yes, re-raised |
| timers as first class events | no | callbacks | no | `after()`, `tick()` |
| direction restricted endpoints | no | no | no | `RecvChan`, `SendChan` |

The performance rows are backed by measurements, see the two sections
below and the `benchmarks/` suite.

## Where it shines, and where it is just threads

pyroutine is real OS threads underneath. That means it inherits
threading's strengths for free, and it cannot cheat physics where
threading cannot. Honest expectations, all measured by
`benchmarks/run.py` (numbers in the next section):

Where it is faster than the usual tool:

| workload | usual tool | measured edge | why |
|---|---|---|---|
| spawning many short lived units | `threading.Thread` | 4 to 11x | routines run on pooled, reused worker threads, so a spawn is a queue handoff, not a thread creation |
| streaming pipelines (producer to consumer) | `queue.Queue` | about 2x | `Chan`'s waiter handoff wakes exactly one parked thread, `queue.Queue` takes more lock traffic per item |
| parallel CPU over shared data, free threaded build | `multiprocessing` | 1.5 to 1.7x | routines read the data where it lives, `multiprocessing` pickles input to children and results back |
| parallel CPU, free threaded build | asyncio | about 4x | an event loop runs on one core no matter how many you have |
| multiplexing many sources | threading's forwarder idiom | 1.2 to 1.8x with a prepared `Select` | the opportunistic pass pays one channel lock per delivery, forwarders pay a queue hop and N standing threads |
| two way coordination (request/response) | asyncio | about 3x | a rendezvous handoff is cheaper than two trips through an event loop |

Where it is the same as threading, on purpose:

| workload | outcome | evidence |
|---|---|---|
| I/O bound fan out (network calls, sleeps) | identical, the wait is the workload | `examples/benchmark_comparison.py`, all frameworks within 3% |
| CPU bound on a GIL build | identical, both are stuck behind the GIL, use `multiprocessing` there | `cpu` scenario: threading 1.03s, pyroutine 0.99s, multiprocessing 0.29s |
| CPU bound on a free threaded build | identical, both use all cores | `cpu` scenario: threading 0.16s, pyroutine 0.21s |
| a blocked routine still occupies a thread | asyncio tasks stay far cheaper to *start* (about 4x) and to park in huge numbers; full M:N parking is the remaining roadmap item | `spawn` scenario |

Multiplexing many sources used to be the honest tradeoff here; it is
now a win. `select` makes an opportunistic pass that takes one channel
lock per delivery (falling back to the atomic all locks path only to
park), so a prepared `Select` beats the threading idiom (a forwarder
thread per source feeding one merged queue) on both builds: 0.06s vs
0.07s on GIL, 0.10s vs 0.18s free threaded, on the select8 benchmark.
And it does that with zero extra threads and no sentinel protocol.
Compare:

```python
# threading: 8 sources need 8 forwarder threads, a merged queue,
# and a sentinel-counting protocol
merged = queue.Queue()
def forward(q):
    while (item := q.get()) is not None:
        merged.put(item)
    merged.put(None)
for q in sources:
    threading.Thread(target=forward, args=(q,)).start()
closed = 0
while closed < len(sources):
    item = merged.get()
    ...

# pyroutine: just ask for the next ready value
while live:
    try:
        idx, item = select(*[recv_case(c) for c in live])
    except ChanClosed as e:
        del live[e.index]
```

(For pure fan in, `merge(*sources)` is one line and hides even that.)

## Benchmarks

From `benchmarks/run.py`, which runs six verified scenarios across
threading, asyncio, multiprocessing and pyroutine. Numbers below from an
11 core Apple Silicon machine (arm64), Python 3.12.2 (GIL) and 3.14.6
free threaded, seconds, lower is better, best per row and build in bold:

| scenario | approach | 3.12 GIL | 3.14 free threaded |
|---|---|---|---|
| spawn, 3k units | threading | 0.09 | 0.22 |
| | asyncio | **0.006** | **0.005** |
| | pyroutine | 0.025 | 0.020 |
| throughput, 200k messages | threading | 0.14 | 0.20 |
| | asyncio | 0.09 | 0.10 |
| | pyroutine | **0.07** | **0.10** |
| pingpong, 20k round trips | threading | 0.18 | 0.19 |
| | asyncio | 0.55 | 0.55 |
| | pyroutine | **0.16** | **0.14** |
| select8, 40k messages | threading | 0.07 | 0.18 |
| | asyncio | 0.32 | 0.33 |
| | pyroutine, select() loop | 0.14 | 0.21 |
| | pyroutine, prepared Select | **0.06** | **0.10** |
| cpu, 8 x 4M crunch | sequential | 1.03 | 0.87 |
| | threading | 1.03 | **0.16** |
| | asyncio | 1.03 | 0.86 |
| | multiprocessing | **0.29** | 0.31 |
| | pyroutine | 0.99 | 0.21 |
| words, 300k documents | sequential | 1.21 | 1.33 |
| | multiprocessing | **0.38** | 0.44 |
| | pyroutine | 1.14 | **0.27** |

Reproduce with:

```
python benchmarks/run.py            # all scenarios
python benchmarks/run.py cpu words  # a subset
python benchmarks/run.py --json=bench.json   # machine readable report
```

A nightly CI job (`bench.yml`) re-runs a scaled down version of this
suite on both a 3.12 GIL and a 3.14t free threaded interpreter and
fails on regression tripwires (`benchmarks/check.py`): channels
falling behind `queue.Queue`, spawning falling behind raw threads,
CPU scenarios losing their free threaded parallelism, and so on. The
bounds are loose by design, they catch lost fast paths, not noise.

The headline reads: spawning beats raw threads 4 to 11x thanks to the
worker pool, channels beat `queue.Queue` for streaming on every build,
rendezvous coordination beats threading and triples asyncio, a
prepared `Select` beats the threading forwarder idiom on both builds
thanks to the opportunistic single lock pass, and on free threaded
Python the shared memory scenarios flip from "multiprocessing or
nothing" to pyroutine winning outright. The one row still standing:
asyncio starts tasks cheaper, which is the full M:N parking roadmap
item.

There are also two narrative examples: `examples/benchmark_comparison.py`
(the same I/O + CPU pipeline in three frameworks) and
`examples/shared_memory_showcase.py` (the multiprocessing pickling tax,
in isolation).

## How it works

Routines run on a pool of reusable daemon worker threads: spawning hands
the task to an idle worker through a private mailbox, and only creates a
new OS thread when every worker is busy. The pool grows without bound
(so blocked routines can never starve runnable ones, same liveness as
thread-per-routine) and idle workers retire after a few seconds.

Every blocking channel operation registers a waiter on the channel and
sleeps on an event. Completion is a compare and set on the waiter, which
is what lets one `send` wake exactly one of many `select` calls parked
on the same channel. `select` freezes all involved channels by taking
their locks in a canonical order, checks readiness atomically, and only
then parks. This is the same overall design Go's runtime uses, minus
continuation style parking.

Because there is no busy waiting anywhere, an idle pipeline costs
nothing.

## Honest limitations

- On a regular (GIL) build, routines interleave rather than run in parallel,
  so this helps I/O bound and coordination heavy code, not raw number
  crunching. On free threaded builds that restriction disappears. You get
  a `GILEnabledWarning` at import so nobody finds out in production.
- A *blocked* routine still occupies an OS thread (running ones do too,
  of course). What that costs is mostly stack reservation, and it is
  tunable: `set_worker_stack_size(512 * 1024)` makes tens of thousands
  of concurrently parked routines practical (the test suite proves
  liveness with thousands blocked at once). Truly Go-scale counts need
  full M:N parking, which remains the headline roadmap item because it
  requires continuation support pure stdlib Python does not offer.
- The generic typing (`Chan[int]`, per case `select` values) is static
  only, nothing checks types at runtime. That is deliberate: runtime
  checks would tax every operation, and a type checker catches the same
  mistakes for free. One mypy quirk to know: unpacking
  `idx, val = select(...)` directly types `val` as `Any`; bind the
  tuple first (`pair = select(...)`) when you want the inferred union.

## Roadmap

- Full M:N parking: a routine blocked on a channel should release its
  worker thread back to the pool, making routine startup as cheap as an
  asyncio task. Thread reuse and tunable stacks already landed; true
  parking needs continuation support that pure stdlib Python does not
  offer, so this waits on the language rather than on this library.

Everything else from the original roadmap has shipped: context and
cancellation, the benchmark suite with nightly regression tripwires,
generic `Chan[int]` typing, per case select typing, the asyncio
bridge, the worker pool scheduler, the select fast paths, and the
opportunistic select pass that closed the multiplexing gap.

## Development

```
pip install -e ".[dev]"
pytest
```

Concurrency bugs are shy, run the suite a few times:

```
for i in 1 2 3 4 5; do pytest -q; done
```

The suite includes property based tests (hypothesis): the channel's
non blocking API is model checked against a plain deque, and random
pipeline shapes verify that every value is delivered exactly once.

Releases are built with `python -m build` and published to PyPI from CI via
trusted publishing, see `.github/workflows/publish.yml`. Bump the version in
`pyproject.toml`, update `CHANGELOG.md`, then cut a GitHub release and the
workflow does the rest.

## License

MIT
