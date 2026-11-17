//go:build linux || darwin

package evloop

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/unix"

	"github.com/xzwache/korb-m/internal/poller"
	"github.com/xzwache/korb-m/internal/transport"
	"github.com/xzwache/korb-m/internal/ws"
)

const keepBufferSize = 4 << 10 // larger in/out buffers are dropped when empty

type connState uint8

const (
	stateHandshake connState = iota
	stateOpen
	stateClosing // close frame queued; the socket closes once out is flushed
	stateDead
)

// conn is owned by its loop. Fields above the line are touched only by the
// loop goroutine; the rest are safe from any goroutine.
type conn struct {
	loop      *loop
	fd        int
	state     connState
	dec       *ws.Decoder
	in        []byte // unparsed tail: part of a handshake or a frame header
	out       []byte // bytes the kernel has not accepted yet
	wantWrite bool   // write interest is registered
	dirty     bool   // in loop.dirty
	opened    bool   // OnOpen was called, so OnClose is owed
	// ---
	id      uint64
	remote  net.Addr
	pending atomic.Int64 // queued + unsent bytes, for the slow-consumer check
	closed  atomic.Bool

	closeOnce   sync.Once
	closeCode   ws.CloseCode
	closeReason string
	closeErr    error
}

func newConn(l *loop, fd int, remote net.Addr) *conn {
	return &conn{loop: l, fd: fd, id: l.srv.nextID.Add(1), remote: remote}
}

func (c *conn) ID() uint64           { return c.id }
func (c *conn) RemoteAddr() net.Addr { return c.remote }

func (c *conn) SendOrClose(op ws.Opcode, payload []byte) error {
	if c.closed.Load() {
		return transport.ErrClosed
	}
	if c.pending.Add(frameSize(len(payload))) > c.loop.srv.cfg.MaxPendingBytes {
		c.closeAsync(ws.ClosePolicyViolation, "slow consumer", transport.ErrSlowConsumer)
		return transport.ErrSlowConsumer
	}
	if !c.loop.push(task{kind: taskSend, conn: c, op: op, payload: payload}) {
		return transport.ErrClosed
	}
	return nil
}

func (c *conn) Close(code ws.CloseCode, reason string) { c.closeAsync(code, reason, nil) }

// closeAsync records the close reason and asks the loop to close.
func (c *conn) closeAsync(code ws.CloseCode, reason string, cause error) {
	if c.closeWith(code, reason, cause) {
		c.loop.push(task{kind: taskClose, conn: c})
	}
}

// closeWith records why the connection closes; only the first call counts.
func (c *conn) closeWith(code ws.CloseCode, reason string, cause error) (first bool) {
	c.closeOnce.Do(func() {
		c.closeCode, c.closeReason, c.closeErr = code, reason, cause
		c.closed.Store(true)
		first = true
	})
	return first
}

// frameSize is the size of a server frame (unmasked header + payload).
func frameSize(n int) int64 {
	switch {
	case n <= 125:
		return int64(n) + 2
	case n <= 0xFFFF:
		return int64(n) + 4
	default:
		return int64(n) + 10
	}
}

// --- everything below runs on the loop goroutine ---

func (l *loop) onReadable(c *conn, hangup bool) {
	if c.state == stateClosing {
		if hangup {
			l.destroy(c) // the peer is gone; nobody will read the rest of out
		}
		return // reads are disabled while closing
	}

	n, err := unix.Read(c.fd, l.buf)
	switch {
	case errors.Is(err, unix.EAGAIN), errors.Is(err, unix.EINTR):
		return
	case err != nil:
		c.closeWith(ws.CloseGoingAway, "", err)
		l.destroy(c)
		return
	case n == 0:
		c.closeWith(ws.CloseGoingAway, "", io.EOF)
		l.destroy(c)
		return
	}
	l.onData(c, l.buf[:n])
}

// onData parses data and keeps the unparsed tail in c.in for the next read.
func (l *loop) onData(c *conn, data []byte) {
	if len(c.in) > 0 {
		c.in = append(c.in, data...)
		data = c.in
	}
	used := l.parse(c, data)
	if c.state == stateDead {
		return
	}
	rest := data[used:]
	switch {
	case len(rest) > 0:
		c.in = append(c.in[:0], rest...) // copy out of the shared buffer
	case cap(c.in) > keepBufferSize:
		c.in = nil
	default:
		c.in = c.in[:0]
	}
}

func (l *loop) parse(c *conn, data []byte) (used int) {
	if c.state == stateHandshake {
		n, ok := l.handshake(c, data)
		if !ok {
			return 0
		}
		used = n
	}
	for c.state == stateOpen {
		m, n, err := c.dec.Next(data[used:])
		used += n
		if errors.Is(err, ws.ErrNeedMore) {
			break
		}
		if err != nil {
			c.closeWith(ws.CloseCodeFor(err), "", err)
			l.beginClose(c)
			break
		}
		l.handle(c, m)
	}
	return used
}

// handshake parses the upgrade request. ok is false while it is incomplete
// or if it was rejected (then c is dead).
func (l *loop) handshake(c *conn, data []byte) (used int, ok bool) {
	cfg := &l.srv.cfg
	req, n, err := ws.ParseRequest(data)
	if errors.Is(err, ws.ErrNeedMore) {
		return 0, false
	}
	if err == nil && cfg.CheckOrigin != nil && !cfg.CheckOrigin(req) {
		err = ws.ErrOriginForbidden
	}
	if err != nil {
		cfg.Logger.Debug("handshake failed", "remote", c.remote, "err", err)
		var he *ws.HandshakeError
		if errors.As(err, &he) {
			_, _ = unix.Write(c.fd, ws.AppendErrorResponse(nil, he)) // best effort
		}
		l.destroy(c)
		return 0, false
	}

	l.appendOut(c, func(b []byte) []byte { return ws.AppendAcceptResponse(b, req.Key) })
	c.state = stateOpen
	c.dec = ws.NewDecoder(ws.SideServer, cfg.MaxMessageSize)
	c.opened = true
	l.srv.handler.OnOpen(c, req)
	return n, true
}

func (l *loop) handle(c *conn, m ws.Message) {
	switch m.Opcode {
	case ws.OpPing:
		l.appendOut(c, func(b []byte) []byte { return ws.AppendFrame(b, ws.OpPong, m.Payload) })
	case ws.OpPong:
	case ws.OpClose:
		code, _, _ := ws.ParseClose(m.Payload) // already validated by the decoder
		if code == ws.CloseNoStatus {
			code = ws.CloseNormal
		}
		c.closeWith(code, "", nil)
		l.beginClose(c)
	default:
		if !c.closed.Load() {
			l.srv.handler.OnMessage(c, m.Opcode, m.Payload)
		}
	}
}

// appendOut adds bytes to c.out and schedules a flush.
func (l *loop) appendOut(c *conn, add func([]byte) []byte) {
	before := len(c.out)
	c.out = add(c.out)
	c.pending.Add(int64(len(c.out) - before))
	if !c.dirty {
		c.dirty = true
		l.dirty = append(l.dirty, c)
	}
}

func (l *loop) send(c *conn, op ws.Opcode, payload []byte) {
	if c.state != stateOpen {
		return
	}
	c.out = ws.AppendFrame(c.out, op, payload) // already counted in pending by SendOrClose
	if !c.dirty {
		c.dirty = true
		l.dirty = append(l.dirty, c)
	}
}

// flush writes as much of c.out as the kernel takes. When the kernel buffer
// is full it registers write interest and continues on the next writable event.
func (l *loop) flush(c *conn) {
	for len(c.out) > 0 {
		n, err := unix.Write(c.fd, c.out)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if errors.Is(err, unix.EAGAIN) {
			break
		}
		if err != nil {
			c.closeWith(ws.CloseGoingAway, "", err)
			l.destroy(c)
			return
		}
		c.out = c.out[n:]
		c.pending.Add(-int64(n))
	}

	if len(c.out) > 0 {
		l.setWriteInterest(c, true)
		return
	}
	if cap(c.out) > keepBufferSize {
		c.out = nil
	} else {
		c.out = c.out[:0]
	}
	if c.state == stateClosing {
		l.destroy(c) // TODO(3.3): wait for the client's close reply, with a timeout
		return
	}
	l.setWriteInterest(c, false)
}

func (l *loop) setWriteInterest(c *conn, on bool) {
	if c.wantWrite == on {
		return
	}
	c.wantWrite = on
	interest := poller.Read
	switch {
	case c.state == stateClosing:
		interest = poller.Write // stop reading; level-triggered reads would spin
	case on:
		interest |= poller.Write
	}
	if err := l.poller.Modify(c.fd, interest); err != nil {
		c.closeWith(ws.CloseInternalError, "", err)
		l.destroy(c)
	}
}

// closeRequested handles Close or SendOrClose called from a handler.
func (l *loop) closeRequested(c *conn) {
	if errors.Is(c.closeErr, transport.ErrSlowConsumer) {
		// out is full of data the client is not reading; a close frame
		// queued behind it would never arrive.
		l.destroy(c)
		return
	}
	l.beginClose(c)
}

// beginClose queues a close frame with the recorded code and stops reading.
func (l *loop) beginClose(c *conn) {
	switch c.state {
	case stateHandshake:
		l.destroy(c)
		return
	case stateClosing, stateDead:
		return
	}
	l.appendOut(c, func(b []byte) []byte {
		return ws.AppendFrame(b, ws.OpClose, ws.AppendClose(nil, c.closeCode, c.closeReason))
	})
	c.state = stateClosing
	c.wantWrite = false // force setWriteInterest to switch reads off
	l.setWriteInterest(c, true)
}

// shutdownConn sends close 1001 best-effort and closes the socket.
func (l *loop) shutdownConn(c *conn) {
	c.closeWith(ws.CloseGoingAway, "server shutdown", transport.ErrShutdown)
	if c.state == stateOpen {
		c.out = ws.AppendFrame(c.out, ws.OpClose, ws.AppendClose(nil, c.closeCode, c.closeReason))
		c.state = stateClosing
	}
	if c.state == stateClosing && len(c.out) > 0 {
		_, _ = unix.Write(c.fd, c.out)
	}
	l.destroy(c)
}

// destroy closes the socket and reports OnClose. Closing the fd also removes
// it from epoll/kqueue.
func (l *loop) destroy(c *conn) {
	if c.state == stateDead {
		return
	}
	c.state = stateDead
	c.closed.Store(true)
	_ = unix.Close(c.fd)
	delete(l.conns, c.fd)
	c.in, c.out, c.dec = nil, nil, nil
	if c.opened {
		l.srv.handler.OnClose(c, c.closeErr)
	}
}
