# 0002: HTTP/1.1 Upgrade with our own parser

- **Date:** 2026-11-14
- **Status:** accepted

## Decision

Accept WebSocket only through the HTTP/1.1 Upgrade handshake (RFC 6455 §4),
parsed by our own minimal parser in `internal/ws`, not `net/http`.

## Why not HTTP/2 (RFC 8441) or HTTP/3 (RFC 9220)

- One client = one socket, so stream multiplexing gives us nothing, and the
  kernel/epoll stops seeing individual clients.
- It requires a full HTTP/2 stack (framing, HPACK, flow control) inside the
  event loop, plus a second framing layer on every message.
- Browsers use it only over TLS on an existing HTTP/2 connection and fall back
  to HTTP/1.1 otherwise; load balancers usually speak HTTP/1.1 to backends.

## Why not net/http

`net/http` owns the socket and runs a goroutine per connection, which does
not fit the event-loop transport. We only need to parse one kind of request,
so a ~150-line parser with the same `[]byte` + `ErrNeedMore` style as the
frame codec serves both transports.
