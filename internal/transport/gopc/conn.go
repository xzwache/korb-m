package gopc

import (
	"bytes"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xzwache/korb-m/internal/transport"
	"github.com/xzwache/korb-m/internal/ws"
)

const (
	readBufferSize = 4096
	maxBatchSize   = 64 << 10
	closeTimeout   = time.Second // how long to wait for the client's close reply
)

type outMsg struct {
	op      ws.Opcode
	payload []byte
}

type conn struct {
	id         uint64
	connection net.Conn
	srv        *Server

	send       chan outMsg   // never closed: a send on a closed channel panics
	done       chan struct{} // closed when the connection starts closing
	writerDone chan struct{}
	lastRead   atomic.Int64 // unix nanos of the last successful read

	closeOnce   sync.Once
	closeCode   ws.CloseCode
	closeReason string
	closeErr    error
}

func newConnection(s *Server, connection net.Conn) *conn {
	c := &conn{
		id:         s.nextID.Add(1),
		connection: connection,
		srv:        s,
		send:       make(chan outMsg, s.cfg.SendQueueSize),
		done:       make(chan struct{}),
		writerDone: make(chan struct{}),
	}
	lastRead := time.Now().UnixNano()
	c.lastRead.Store(lastRead)
	return c
}

func (c *conn) ID() uint64           { return c.id }
func (c *conn) RemoteAddr() net.Addr { return c.connection.RemoteAddr() }

func (c *conn) SendOrClose(op ws.Opcode, payload []byte) error {
	if c.closing() {
		return transport.ErrClosed
	}
	select {
	case c.send <- outMsg{op, payload}:
		return nil
	default:
		c.closeWith(ws.ClosePolicyViolation, "slow consumer", transport.ErrSlowConsumer)
		return transport.ErrSlowConsumer
	}
}

func (c *conn) Close(code ws.CloseCode, reason string) { c.closeWith(code, reason, nil) }

// closeWith records why the connection closes; only the first call counts.
func (c *conn) closeWith(code ws.CloseCode, reason string, cause error) {
	c.closeOnce.Do(func() {
		c.closeCode, c.closeReason, c.closeErr = code, reason, cause
		close(c.done)
	})
}

func (c *conn) closing() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// readLoop reads from the socket and dispatches messages until the
// connection closes. It always ends with closeWith called.
func (c *conn) readLoop(pending []byte) {
	buf := make([]byte, readBufferSize)
	n := copy(buf, pending)
	dec := ws.NewDecoder(ws.SideServer, c.srv.cfg.MaxMessageSize)

	var readErr error
	for {
		start := 0
		for {
			m, used, err := dec.Next(buf[start:n])
			start += used
			if errors.Is(err, ws.ErrNeedMore) {
				break
			}
			if err != nil {
				c.closeWith(ws.CloseCodeFor(err), "", err)
				return
			}
			if stop := c.handle(m); stop {
				return
			}
		}
		if readErr != nil {
			c.closeWith(ws.CloseGoingAway, "", readErr)
			return
		}

		n = copy(buf, buf[start:n]) // keep a partial header for the next round
		var r int
		r, readErr = c.connection.Read(buf[n:])
		n += r
		c.lastRead.Store(time.Now().UnixNano())
	}
}

func (c *conn) handle(m ws.Message) (stop bool) {
	switch m.Opcode {
	case ws.OpPing:
		_ = c.SendOrClose(ws.OpPong, bytes.Clone(m.Payload))
	case ws.OpPong:
		// lastRead is already updated
	case ws.OpClose:
		code, _, _ := ws.ParseClose(m.Payload) // already validated by the decoder
		if code == ws.CloseNoStatus {
			code = ws.CloseNormal
		}
		c.closeWith(code, "", nil)
		return true
	default:
		if !c.closing() {
			c.srv.handler.OnMessage(c, m.Opcode, m.Payload)
		}
	}
	return false
}

// writeLoop is the only goroutine that writes to the socket.
func (c *conn) writeLoop() {
	defer close(c.writerDone)
	cfg := &c.srv.cfg
	ping := time.NewTicker(cfg.PingInterval)
	defer ping.Stop()

	var buf []byte
	for {
		select {
		case m := <-c.send:
			buf = ws.AppendFrame(buf[:0], m.op, m.payload)
			buf = c.appendQueued(buf)
		case <-ping.C:
			if time.Since(time.Unix(0, c.lastRead.Load())) > cfg.IdleTimeout {
				c.closeWith(ws.CloseGoingAway, "idle timeout", transport.ErrIdleTimeout)
				continue
			}
			buf = ws.AppendFrame(buf[:0], ws.OpPing, nil)
		case <-c.done:
			buf = ws.AppendFrame(buf[:0], ws.OpClose, ws.AppendClose(nil, c.closeCode, c.closeReason))
			wait := closeTimeout
			if err := c.write(buf); err != nil {
				wait = 0 // the client can't get our close frame, so don't wait for a reply
			}
			_ = c.connection.SetReadDeadline(time.Now().Add(wait))
			return
		}

		if err := c.write(buf); err != nil {
			c.closeWith(ws.CloseGoingAway, "", err)
			_ = c.connection.SetReadDeadline(time.Now()) // wake the reader
			return
		}
		if cap(buf) > readBufferSize {
			buf = nil // don't keep a large buffer on an idle connection
		}
	}
}

// appendQueued batches already-queued messages into one write (one syscall).
func (c *conn) appendQueued(buf []byte) []byte {
	for len(buf) < maxBatchSize {
		select {
		case m := <-c.send:
			buf = ws.AppendFrame(buf, m.op, m.payload)
		default:
			return buf
		}
	}
	return buf
}

func (c *conn) write(b []byte) error {
	if err := c.connection.SetWriteDeadline(time.Now().Add(c.srv.cfg.WriteTimeout)); err != nil {
		return err
	}
	_, err := c.connection.Write(b)
	return err
}
