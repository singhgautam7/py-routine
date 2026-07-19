# Changelog

## 0.1.0

First release.

- go() and the @routine decorator for spawning routines
- Chan with unbuffered rendezvous and buffered modes, timeouts,
  try_send/try_recv, close semantics and iteration
- select() over multiple recv/send cases with default and timeout,
  implemented with waiter registration, no polling
- after() timer channels
- WaitGroup (with the wg.go shortcut) and Once
- Context and cancellation in the spirit of Go's context package:
  background(), with_cancel(), with_timeout(), with_deadline(),
  Canceled and DeadlineExceeded, parent to child propagation
- tick() repeating timer channels, close to stop
- ErrGroup: WaitGroup that cancels its context on the first error and
  re-raises it from wait()
- Directional channel views RecvChan and SendChan via ch.recv_only()
  and ch.send_only(), accepted by recv_case()/send_case()
- merge() fan in helper, N channels multiplexed into one on one thread
- Mutex and RWMutex (writer preference readers writer lock)
- free_threading() to detect GIL-disabled interpreters, and a one time
  GILEnabledWarning at import when routines cannot run in parallel
  (yellow on terminals, NO_COLOR respected, disable with
  PYROUTINE_NO_GIL_WARNING=1)
- Benchmark suite (benchmarks/run.py): six scenarios against threading,
  asyncio and multiprocessing, numbers published in the README
- Worker pool scheduler: routines reuse idle daemon threads, spawning
  is 4 to 11x faster than threading.Thread while keeping unbounded
  growth (a burst of blocked routines can never starve a runnable one)
- Generic typing: Chan[int], RecvChan[T], SendChan[T], typed merge and
  select case builders, purely static
- pyroutine.aio asyncio bridge: awaitable recv/send/select and async
  iteration over channels via the same waiter registration, no polling,
  no executor threads
- Timer: a stoppable after(), stop() closes the channel and wakes
  anything parked on it
- ErrGroup.set_limit(n) to bound in flight routines, like errgroup's
  SetLimit
- once and synchronized decorators (sync.OnceValues and a Mutex guarded
  function, respectively)
- select() fast path: a one case select without default now costs the
  same as the bare channel operation
- Select: a prepared select over a fixed case set for tight loops,
  precomputes validation, channel dedup and lock ordering once
- _Waiter parks on a pre-acquired Lock instead of an Event, cutting the
  one allocation on every blocking path several-fold. pingpong ~15%
  faster, select8 ~25% faster; with a prepared Select the select8 gap
  to threading's forwarder idiom disappears on GIL builds. Free list
  pooling of waiters was evaluated and rejected: close() commits its
  waiter snapshot outside the channel lock, a recycled waiter could be
  committed by a stale close
- mypy in CI, the package type checks clean
- Unretrieved routine exceptions are reported at garbage collection
  (stderr by default, set_excepthook to customize), so failures can
  never vanish silently
- Opt-in deadlock detection (enable_deadlock_detection) issuing a
  DeadlockWarning when every running routine is blocked forever in
  pyroutine primitives with no pending timers, in the spirit of Go's
  "all goroutines are asleep"
- Worker threads drop task references while idle, finished Handles are
  collectable immediately
- Fully typed, pure Python, zero dependencies
