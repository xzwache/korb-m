# Phase 2 baseline: goroutine per connection (`gopc`)

How much memory one idle WebSocket connection costs with two goroutines
(reader + writer) per connection. This is the baseline the event-loop
transport (Phase 3) is compared against.

## How to reproduce

```sh
make bench-linux                     # 10K, 50K, 100K connections in a Linux container
make bench-linux STEPS="10000"       # one step
```

Numbers come from `korbd -debug-addr` → `/debug/stats?gc=1` (forces a GC first,
so only live memory is counted) and `/proc/net/sockstat` for kernel socket memory.

## Results: Linux (Docker)

_Pending: run `make bench-linux` with Docker running._

## Results: macOS, 10K idle connections (preliminary)

Apple M-series, Go 1.25.7, loadgen and korbd on the same machine.

| | total | per connection |
|---|---|---|
| goroutines | 20,007 | 2 |
| heap in use | 95.5 MB | **~9.9 KB** |
| goroutine stacks | 62.0 MB | **~6.4 KB** |
| Go `Sys` (all memory from the OS) | 178.6 MB | ~18 KB |

Not included: kernel memory for socket buffers (measured on Linux only).

### Where the heap goes (`go tool pprof`, 10K connections)

| allocation | total | per conn | what it is |
|---|---|---|---|
| `(*conn).readLoop` | 38.7 MB | 3.9 KB | the 4 KB read buffer, allocated even when idle |
| `newConnection` | 25.1 MB | 2.5 KB | the send channel: 64 slots × 32 B, allocated up front |
| `runtime.malg` + `allocm` | 12.0 MB | 1.2 KB | goroutine and thread bookkeeping |
| `(*conn).writeLoop`, tickers, timers | ~7 MB | ~0.7 KB | ping ticker and timer per connection |

**~16 KB per connection** of Go memory, and over half of it is buffers that
sit empty on an idle connection. At 1M connections that is ~16 GB before
the kernel's socket buffers.

### Latency (2,000 connections × 1 msg/s, echo, localhost)

p50 < 128 µs, p99 < 2 ms.

## What Phase 3 attacks

1. **Read buffer per connection:** an event loop reads into one shared
   buffer per loop and only keeps bytes for connections with a partial frame.
2. **Two goroutine stacks per connection:** no goroutines per connection at all.
3. **Send channel allocated up front:** a queue that allocates only when
   there is something to send.
4. **A ticker per connection:** one timer wheel per loop.
