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
- Fully typed, pure Python, zero dependencies
