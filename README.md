# korb-m

A realtime messaging server in Go, built step by step toward **1 million
concurrent WebSocket connections** on a single node.

> **Status:** early development (Phase 0). Not usable yet. See the
> [roadmap](docs/ROADMAP.md).

## Why

Most "C1M" demos are benchmark toys. korb-m aims to be a real, production-ready
system (rooms, persistence, resume after reconnect, multi-node) and to
document *why* each design choice was made, with measurements.

It ships two interchangeable transports behind one interface:

| Transport | Model | Purpose |
|---|---|---|
| `gopc` | goroutine per connection | simple, idiomatic baseline |
| `evloop` | hand-written epoll (Linux) / kqueue (macOS) event loop | minimal memory per connection |

Both are benchmarked under the same load, so the trade-off is measured, not
guessed.

## Development

Requires Go 1.25+.

```sh
make help        # list targets
make test        # unit tests
make race        # tests with the race detector
make lint        # golangci-lint
make test-linux  # run tests in a Linux container (epoll paths) via Docker
make build       # binaries into ./bin
```

## License

[Apache License 2.0](LICENSE)
