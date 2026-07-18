# py-routine

Go style concurrency for Python: routines, channels, `select` and `WaitGroup`.

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
not have is Go's model: cheap concurrent routines that talk over typed
rendezvous points, `select` over several of them at once, and structured
teardown with `WaitGroup`. That model composes unusually well and it does not
split your codebase into sync and async halves.

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

No dependencies. Python 3.9+.

## The API in five minutes

### Routines

```python
from pyroutine import go, routine

h = go(fetch, url)          # spawn now, like `go fetch(url)`
page = h.result()           # join and get the return value
h.join(timeout=1.0)         # or just wait

@routine                    # decorator form
def fetch(url): ...
h = fetch(url)              # calling it spawns it
```

Exceptions inside a routine are captured and re-raised from `result()`,
never silently dropped.

### Channels

```python
from pyroutine import Chan, ChanClosed

ch = Chan()                 # unbuffered rendezvous
ch = Chan(8)                # buffered

ch.send(value)              # blocks per Go semantics
value = ch.recv()           # blocks until a value arrives
ch.send(value, timeout=1.0) # optional timeouts on everything
ok = ch.try_send(value)     # non blocking variants
value, ok = ch.try_recv()

ch.close()                  # wakes everyone, buffered values still drain
for value in ch: ...        # iterate until closed and drained
```

`recv()` on a closed, drained channel raises `ChanClosed`. Iteration catches
it for you and just stops.

### select

```python
from pyroutine import select, recv_case, send_case, after

idx, val = select(
    recv_case(inbox),
    recv_case(control),
    send_case(outbox, item),
)

# non blocking poll
idx, val = select(recv_case(inbox), default=True)   # (-1, None) if idle

# with a deadline, two equivalent spellings
idx, val = select(recv_case(inbox), timeout=1.0)
idx, val = select(recv_case(inbox), recv_case(after(1.0)))
```

`select` blocks on all cases simultaneously with no polling loop, picks a
ready case at random for fairness like Go does, and guarantees exactly one
case fires even when many selects race for one value.

### WaitGroup and Once

```python
from pyroutine import WaitGroup, Once

wg = WaitGroup()
wg.go(worker, arg)          # the shortcut you will actually use
wg.wait(timeout=30)

once = Once()
once.do(init_expensive_thing)
```

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
  crunching. On free threaded builds that restriction disappears.
- A routine is an OS thread. Hundreds are fine, hundreds of thousands are
  not. An M:N scheduler is the headline roadmap item.
- Channels are untyped. Type hints via generics are planned.

## Roadmap

- M:N scheduler so routines stop costing a full thread each
- Generic typing, `Chan[int]`
- Benchmarks against threading, asyncio and multiprocessing on 3.14
  free threaded builds
- An asyncio bridge so channels can be awaited from async code
- Context/cancellation, in the spirit of Go's context package

## Development

```
pip install -e ".[dev]"
pytest
```

Releases are built with `python -m build` and published to PyPI from CI via
trusted publishing, see `.github/workflows/publish.yml`. Cut a GitHub
release and the workflow does the rest.

## License

MIT
