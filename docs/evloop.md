# How evloop works

`internal/transport/evloop` serves many connections from a few goroutines.
This document walks through the code in the order things happen.

## 0. The problem it solves

`gopc` gives every connection two goroutines that block in `Read`/`Write`.
That is simple, but each connection costs ~16 KB (stacks + buffers). At
1M connections that is 16 GB, mostly for goroutines that are asleep.

`evloop` flips it around: **nobody waits on one socket**. A loop asks the
kernel "which of my 100K sockets can I read or write right now?", handles
only those, and asks again. A connection is just a struct (~1–2 KB).

## 1. Three kernel ideas everything rests on

**File descriptor (fd).** Every socket is an `int`. The listening socket
is one fd, and every accepted connection is another.

**Non-blocking sockets.** A normal `read` sleeps until data arrives. On a
non-blocking socket, `read` returns at once with `EAGAIN` ("nothing now").
So a loop can never get stuck on one slow client. We create sockets
non-blocking: `newSocket` / `accept` in `sys_linux.go` and `sys_darwin.go`.

**Readiness polling (epoll / kqueue).** You give the kernel a list of fds
and what you care about (readable? writable?). Then one call, `Wait`,
sleeps until at least one of them is ready and returns the list of ready
ones. `internal/poller` wraps it:

| poller call | Linux syscall | macOS syscall | meaning |
|---|---|---|---|
| `Add(fd, Read)` | `epoll_ctl(ADD)` | `kevent(EV_ADD)` | start watching fd |
| `Modify(fd, Read\|Write)` | `epoll_ctl(MOD)` | `kevent(EV_ENABLE/DISABLE)` | change what we watch |
| `Delete(fd)` / `close(fd)` | `epoll_ctl(DEL)` | `kevent(EV_DELETE)` | stop watching |
| `Wait(events, timeout)` | `epoll_wait` | `kevent` | sleep until something is ready |
| `Wake()` | write to an `eventfd` | `EVFILT_USER` trigger | make `Wait` return now |

The poller is **level-triggered**: as long as a socket has unread data, every
`Wait` reports it again. If we read only part of it, we get reminded.

### How the right poller is chosen (Linux vs macOS)

At **compile time**, by file name. Go builds `foo_linux.go` only when
`GOOS=linux` and `foo_darwin.go` only when `GOOS=darwin`. The
`//go:build` line at the top says the same explicitly:

```
internal/poller/epoll_linux.go    //go:build linux    → type Poller (epoll)
internal/poller/kqueue_darwin.go  //go:build darwin   → type Poller (kqueue)
internal/poller/unsupported.go    //go:build !linux && !darwin → returns ErrUnsupported
```

All three define the same `type Poller` with the same methods, and exactly
one of them is compiled. The rest of the code calls `poller.New()` and never
knows which one it got. There is no `if runtime.GOOS == ...` at runtime.
The same trick picks `sys_linux.go` / `sys_darwin.go` in evloop.

## 2. Why multiple loops

One loop is one goroutine, and one goroutine runs on one CPU core at a
time. With one loop, a 16-core server would use 1 core and leave 15 idle.

So `Listen` creates **N loops, N = number of cores** (`Config.Loops`,
default `GOMAXPROCS`). Each loop is fully independent: its own poller, its
own connections, its own read buffer. They share nothing, so they need
no locks between them and scale with cores.

Why not more loops than cores? Extra loops would only fight over the same
cores. Why not one goroutine per connection? That is `gopc`, the 16 KB per
connection we are trying to avoid.

How do connections get spread over the loops?

```
Linux: SO_REUSEPORT                  macOS: no balancing in the kernel
  loop0 ── listen socket :8080         loop0 ── listen socket :8080
  loop1 ── listen socket :8080           │  accept(), then round-robin:
  loop2 ── listen socket :8080           ├─► loop1 inbox (taskAccept)
  kernel hashes each new connection      └─► loop2 inbox (taskAccept)
  to one of the sockets
```

See `Server.Listen` and `Server.pick` in `server.go`.

## 3. Startup, step by step

```go
srv := evloop.New(handler, cfg)
addr, err := srv.Listen(":8080")
err = srv.Serve()   // blocks until Shutdown
```

1. `Listen` → `newLoop` ×N (`loop.go`): for each loop, `poller.New()`
   (epoll/kqueue fd), a 64 KB read buffer, an empty `conns map[int]*conn`.
2. `Listen` → `listenSocket` (`socket.go`): `socket()` → `setsockopt(SO_REUSEADDR,
   SO_REUSEPORT)` → `bind()` → `listen()`.
3. `setListener` → **`poller.Add(listenFd, Read)`**. This is the first epoll
   registration. A listening socket is "readable" when a new client is
   waiting to be accepted.
4. `Serve` starts `loop.run()` in one goroutine per loop.

## 4. The loop: `loop.run()`

```go
for {
    n := l.poller.Wait(l.events, -1)        // ① sleep until something is ready
    for _, e := range l.events[:n] {        // ② handle each ready fd
        if e.FileDescriptor == l.listenFd {
            l.accept()                      //    new clients
            continue
        }
        c := l.conns[e.FileDescriptor]      //    fd → our conn struct
        if e.Readable || e.Hangup { l.onReadable(c, e.Hangup) }
        if e.Writable              { l.flush(c) }
    }
    if l.drain() { return }                 // ③ tasks from other goroutines
    l.flushDirty()                          // ④ write what this round produced
}
```

Every iteration is: **wait → react → run queued tasks → write**. The
only place the loop sleeps is ①. Everything else is non-blocking.

## 4.5 How events work

**Per socket, the kernel keeps two buffers.**

```
            ┌────────── fd 12 ──────────┐
network ──► │ receive buffer  [h e l l o]│ ──► our read()
            │ send buffer     [        ] │ ◄── our write()
network ◄── └───────────────────────────┘
```

- readable = receive buffer has bytes (listener: a client waits in accept)
- writable = send buffer has free space
- hangup   = peer closed, or an error

**Inside epoll there are two lists.**

```
interest list (what we registered)      ready list (what is ready now)
  fd 3  (listener)  Read                  fd 12  IN
  fd 12             Read                  fd 40  OUT
  fd 40             Read|Write
```

`Add`/`Modify` edit the interest list. When a packet lands in fd 12's
receive buffer, the kernel moves fd 12 to the ready list and wakes
`epoll_wait`. `epoll_wait` copies the ready list into our array. We never
scan all sockets: cost is proportional to activity, not connection count.

`poller.Wait` translates each kernel entry into
`Event{FileDescriptor, Readable, Writable, Hangup}`. kqueue reports read
and write as separate filters, so one fd may appear twice in one `Wait`.

**Timeline of one connection:**

```
t0  Wait()                                   ready list empty, loop sleeps
t1  client connects     → fd 3 ready
    Wait → [{3 Readable}]   accept → fd 12, Add(12, Read)
t2  handshake arrives   → fd 12 ready
    Wait → [{12 Readable}]  read → ParseRequest → write "101 ..."
t3  "hello" arrives
    Wait → [{12 Readable}]  read → decoder → OnMessage
t4  we send 1 MB, client reads slowly
    write takes 200 KB, then EAGAIN → Modify(12, Read|Write)
    client reads, send buffer drains → fd 12 ready (OUT)
    Wait → [{12 Writable}]  flush rest → Modify(12, Read)
t5  client closes the tab → FIN → fd 12 ready (IN|RDHUP)
    Wait → [{12 Readable Hangup}]  read → 0 = EOF → close(12)
    (close also removes fd 12 from the interest list)
t6  another goroutine: SendOrClose → push → Wake() writes the eventfd
    Wait returns, wake event hidden by the poller → drain() runs the task
```

**Level-triggered.** After each `Wait` the kernel re-checks: fd still has
unread data → stays ready. Read half, get reminded next time. Price: a
permanent Write interest would fire on every `Wait` (send buffer is almost
always free), so we enable it only while `c.out` has stuck bytes.
Edge-triggered reports only *changes*: faster, but if you don't read
until `EAGAIN` the fd is never reported again and the connection hangs.

## 5. Life of one connection

### 5.1 Accept and register: `accept`, `register` in `loop.go`

`Wait` reports `listenFd` readable, so `accept()` runs:

```
accept(listenFd)          → new fd 12 (already non-blocking)
setsockopt(TCP_NODELAY)   → send small frames immediately
poller.Add(12, Read)      ← REGISTRATION: "tell me when fd 12 has data"
conns[12] = newConn(...)  ← fd → struct lookup used in step ② of the loop
```

It accepts up to 64 clients per event (`acceptBatch`) so a connection storm
can't starve existing connections. Thanks to level-triggering, any clients
left waiting are reported again on the next `Wait`.

### 5.2 Handshake: `onReadable`, `onData`, `handshake` in `conn.go`

The client sends `GET /ws HTTP/1.1 ... Upgrade: websocket`. `Wait` reports
fd 12 readable:

```
read(12, loop.buf)            → maybe the whole request, maybe half of it
ws.ParseRequest(data)
  ErrNeedMore → copy the bytes into c.in, wait for the next readable event
  error       → write "HTTP/1.1 400 ...", close
  ok          → append "101 Switching Protocols" to c.out
                state = stateOpen
                handler.OnOpen(c, req)
```

`loop.buf` is **shared by all connections of the loop**. That is safe because
the loop handles one fd at a time. Anything we must keep (an incomplete
head, half a frame header) is copied into `c.in`, because the next `read`
for another connection will overwrite `loop.buf`.

### 5.3 Messages: `parse`, `handle`

```
read(12, loop.buf)
for {
    msg, used, err := c.dec.Next(data)   // Phase 1 decoder
    ErrNeedMore → keep the leftover in c.in, stop
    ping        → append pong to c.out
    close       → beginClose (5.6)
    text/binary → handler.OnMessage(c, op, payload)
}
```

One `read` may hold 10 messages, or 1/10 of a message. The decoder handles
both: it keeps its own state between calls.

### 5.4 Sending: `SendOrClose`, `push`, `drain`, `flush`

The app calls `c.SendOrClose(op, payload)` from **any** goroutine, for
example while broadcasting from a connection owned by another loop. Only
the owning loop may touch `c`, so the send is **queued**:

```
any goroutine                            owning loop
SendOrClose
  pending += frame size
  too much unsent? → slow consumer, close
  loop.push(task{send, c, payload}) ───►  inbox (slice + mutex)
     inbox was empty → poller.Wake() ──►  Wait returns (③)
                                          drain():   c.out += frame; mark dirty
                                          flushDirty(): flush(c)
```

`Wake` is called only when the inbox goes from empty to non-empty: 1000
sends cost one wake. All frames queued in one round leave in a single
`write` syscall.

### 5.5 Writing and write interest: `flush`, `setWriteInterest`

```
write(12, c.out)
  all written  → c.out empty; stop watching "writable"  (Modify(12, Read))
  EAGAIN       → kernel send buffer is full (client reads slowly)
                 keep the rest in c.out
                 Modify(12, Read|Write)   ← "wake me when I can write again"
                 later: Wait reports fd 12 writable → flush again
```

We watch "writable" **only while something is stuck**. An empty send buffer
is always writable, so a level-triggered poller would otherwise wake us on
every `Wait` for nothing.

### 5.6 Closing: `beginClose`, `destroy`

```
client close frame / handler Close / protocol error
  → append our close frame to c.out, state = stateClosing
  → Modify(12, Write)        stop reading, only finish writing
  → flush; when c.out is empty → destroy
destroy: close(12)           ← also removes fd 12 from epoll/kqueue
         delete(conns, 12)
         handler.OnClose(c, err)
EOF / read or write error / slow consumer → destroy immediately
```

### 5.7 Shutdown: `Shutdown`, `loop.stop`

`Shutdown` pushes `taskStop` to every loop. Each loop exits `run`, sends a
1001 close frame to its connections (best effort), closes all fds and its
poller. `Serve` returns when all loops have exited.

## 6. Rules that keep it correct

1. **Only the owning loop touches a conn's fd, buffers and state.** Other
   goroutines only use `SendOrClose`/`Close`, which go through the inbox.
   Hence no locks on the hot path.
2. **Handlers must not block.** `OnMessage` runs inside `run()`. A slow
   handler freezes every connection on that loop.
3. **Never keep `loop.buf`.** Copy what you need to keep (`c.in`). The decoder
   copies payloads, and `OnMessage` payloads are valid only during the call.
4. **Every socket is non-blocking.** One blocking call would stop the loop.

## 7. Map of the code

| File | What lives there |
|---|---|
| `server.go` | `Config`, `New`, `Listen`, `Serve`, `Shutdown`, `pick` |
| `loop.go` | `loop` struct, `run`, `drain`, `push`, `accept`, `register`, `stop` |
| `conn.go` | `conn` struct, read path, `flush`, closing, `destroy` |
| `socket.go` | `listenSocket`, sockaddr conversion |
| `sys_linux.go` / `sys_darwin.go` | OS-specific `newSocket`, `accept`, `reusePort` |
| `internal/poller` | epoll / kqueue wrapper |

## 8. Not done yet (PR 3.3)

- Timers: ping, idle timeout, handshake timeout (a silent client stays forever).
- Waiting for the client's close reply instead of closing right after ours.
- `korbd -transport=evloop`.
