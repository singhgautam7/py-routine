# py-routine

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
extension, one OS thread per routine, and every blocking primitive parks on
a condition instead of polling. On a free threaded build, CPU bound fan out
across routines actually uses your cores.

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

- On a standard CPython build the GIL lets only one thread run Python code
  at a time. Routines interleave, which is exactly what you want for I/O
  bound and coordination heavy work, but CPU bound routines will not get
  faster.
- On a free threaded build (CPython 3.13+ compiled with the GIL disabled,
  the `python3.13t` / `python3.14t` binaries) the same routines run in
  parallel across cores with zero code changes.

Because this difference is easy to miss, `pyroutine` warns once at import
time when the GIL is enabled:

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
build) silence the warning before the first import:

```python
import warnings
warnings.filterwarnings("ignore", message="pyroutine:")
import pyroutine
```

or run Python with `-W "ignore:pyroutine:"`.

## The full API tour

Everything the package exports, with examples. The public surface is
deliberately small: `go`, `routine`, `Handle`, `Chan`, `ChanClosed`,
`select`, `recv_case`, `send_case`, `after`, `WaitGroup`, `Once`,
`free_threading`, `GILEnabledWarning`.

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

Exceptions raised inside a routine are never printed to stderr and never
silently dropped. They are captured and re-raised, with their original
traceback, from `result()`:

```python
def boom():
    raise ValueError("nope")

h = go(boom)
h.join()          # completes fine, the routine is simply done
h.result()        # raises ValueError: nope, right here in the caller
```

If you never call `result()`, the exception is dropped when the handle is
garbage collected, same as Go's rule that a panic you do not recover from
belongs to the goroutine that raised it. Call `result()` for anything whose
failure you care about.

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

### `after()`: timer channels

`after(seconds)` returns a channel that receives one value (the current
`time.monotonic()`) after the delay, then closes. It is Go's
`time.After`, and its natural home is a `select` case:

```python
from pyroutine import after, recv_case, select

idx, val = select(recv_case(work), recv_case(after(0.5)))
if idx == 1:
    print("no work for 500ms")
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

## How it works

One routine is one daemon OS thread, and every blocking operation registers
a waiter on the channel and sleeps on an event. Completion is a compare and
set on the waiter, which is what lets one `send` wake exactly one of many
`select` calls parked on the same channel. `select` freezes all involved
channels by taking their locks in a canonical order, checks readiness
atomically, and only then parks. This is the same overall design Go's
runtime uses, minus the custom scheduler.

Because there is no busy waiting anywhere, an idle pipeline costs nothing.

## Honest limitations

- On a regular (GIL) build, routines interleave rather than run in parallel,
  so this helps I/O bound and coordination heavy code, not raw number
  crunching. On free threaded builds that restriction disappears. You get
  a `GILEnabledWarning` at import so nobody finds out in production.
- A routine is an OS thread. Hundreds are fine, hundreds of thousands are
  not. An M:N scheduler is the headline roadmap item.
- Channels are untyped at runtime. Type hints via generics are planned.

## Roadmap

- M:N scheduler so routines stop costing a full thread each
- Generic typing, `Chan[int]`
- Benchmarks against threading, asyncio and multiprocessing on 3.14
  free threaded builds
- Context/cancellation, in the spirit of Go's context package
- An asyncio bridge so channels can be awaited from async code

## Development

```
pip install -e ".[dev]"
pytest
```

Concurrency bugs are shy, run the suite a few times:

```
for i in 1 2 3 4 5; do pytest -q; done
```

Releases are built with `python -m build` and published to PyPI from CI via
trusted publishing, see `.github/workflows/publish.yml`. Bump the version in
`pyproject.toml`, update `CHANGELOG.md`, then cut a GitHub release and the
workflow does the rest.

## License

MIT
