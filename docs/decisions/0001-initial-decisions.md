# 0001: Initial decisions

- **Date:** 2026-11-14
- **Status:** accepted

## 1. Wire protocol: WebSocket, with our own codec

Browsers and existing clients can connect without custom tooling. We write
the RFC 6455 framing ourselves (instead of using gorilla/websocket or
gobwas/ws) because the event-loop transport needs a parser that works on
byte buffers and resumes on partial frames. Libraries built around blocking
`io.Reader` don't fit that model.

## 2. Two transports behind one `Conn` interface

`gopc` (goroutine per connection) is the baseline and `evloop`
(epoll/kqueue) is the optimized transport. Rooms, persistence and the hub
must not know which one is running. Consequence: handlers must never
block, because a blocking call inside an event loop stalls every
connection on that loop. Blocking work (Redis) goes through a worker pool.

## 3. Syscalls via `golang.org/x/sys/unix`

These are still raw syscalls, but with maintained constants and struct
layouts for each OS/arch. The standard `syscall` package is frozen.

## 4. License: Apache-2.0

It's permissive and includes an explicit patent grant, which is common for
infrastructure projects.

## 5. Platforms

Development happens on macOS (kqueue). Benchmarks and 1M targets run on
Linux (epoll). CI runs both.
