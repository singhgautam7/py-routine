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
- Fully typed, pure Python, zero dependencies
