# Roadmap

Each phase ends with something runnable plus a short write-up in `docs/`.

| Phase | Scope | Done when |
|---|---|---|
| **0** | Repo bootstrap: module, Makefile, CI (Linux + macOS), lint, license | CI is green on `main` |
| **1** | WebSocket codec (RFC 6455): pure functions over `[]byte`, partial frames, masking, control frames, fuzz tests | Fuzzing runs clean; Autobahn-style edge cases covered |
| **2** | Transport `gopc`: goroutine per connection, ping/pong, deadlines, bounded send queue, slow-consumer eviction; `loadgen`; Prometheus metrics | 100K connections on Linux, bytes/conn recorded |
| **3** | Transport `evloop`: poller (kqueue / epoll), non-blocking fds, one loop per core with `SO_REUSEPORT`, timer wheel, buffered writes, pooled lazy buffers | Same benchmark as Phase 2; comparison table published |
| **4** | Rooms + hub, persist-then-publish via Redis Streams, resume with `lastID`, worker pool between loop and Redis | Reconnecting client receives every missed message exactly once |
| **5** | Go client SDK: jittered backoff, gap detection, `client_msg_id` idempotent retries | Chaos test (kill server mid-stream) loses nothing |
| **6** | Multi-node: per-room subscriptions, presence for DMs, graceful drain | Rolling restart without message loss |
| **7** | Distributed load test toward 1M (several load-gen IPs), kernel tuning guide | 1M connections, results + write-up published |

Design decisions are recorded in [`docs/decisions/`](decisions/).
