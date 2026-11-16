package gopc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/xzwache/korb-m/internal/transport"
	"github.com/xzwache/korb-m/internal/ws"
)

const waitTimeout = 2 * time.Second

// --- test handler ---

type event struct {
	kind string // "open" or "close"
	conn transport.Conn
	err  error
}

type recorder struct {
	events    chan event
	onMessage func(c transport.Conn, op ws.Opcode, p []byte) // nil = echo
}

func newRecorder() *recorder { return &recorder{events: make(chan event, 100)} }

func (r *recorder) OnOpen(c transport.Conn, _ ws.Request) { r.events <- event{kind: "open", conn: c} }
func (r *recorder) OnClose(c transport.Conn, err error) {
	r.events <- event{kind: "close", conn: c, err: err}
}
func (r *recorder) OnMessage(c transport.Conn, op ws.Opcode, p []byte) {
	if r.onMessage != nil {
		r.onMessage(c, op, p)
		return
	}
	_ = c.SendOrClose(op, bytes.Clone(p))
}

func (r *recorder) wait(t *testing.T, kind string) event {
	t.Helper()
	select {
	case ev := <-r.events:
		if ev.kind != kind {
			t.Fatalf("got %q event, want %q", ev.kind, kind)
		}
		return ev
	case <-time.After(waitTimeout):
		t.Fatalf("timeout waiting for %q event", kind)
		return event{}
	}
}

func startServer(t *testing.T, cfg Config, h transport.Handler) (*Server, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := New(h, cfg)
	errc := make(chan error, 1)
	go func() { errc <- s.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()
		_ = s.Shutdown(ctx)
		select {
		case err := <-errc:
			if !errors.Is(err, ErrServerClosed) {
				t.Errorf("Serve returned %v", err)
			}
		case <-time.After(waitTimeout):
			t.Error("Serve did not return")
		}
	})
	return s, ln.Addr().String()
}

// --- test client ---

type client struct {
	t   *testing.T
	nc  net.Conn
	dec *ws.Decoder
	buf []byte
}

func dial(t *testing.T, addr string) *client {
	t.Helper()
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nc.Close() })

	c := &client{t: t, nc: nc, dec: ws.NewDecoder(ws.SideClient, 1<<20)}
	key := ws.NewKey()
	c.write(ws.AppendRequest(nil, "test", "/ws", key))
	for {
		n, err := ws.ParseResponse(c.buf, key)
		if err == nil {
			c.buf = c.buf[n:]
			return c
		}
		if !errors.Is(err, ws.ErrNeedMore) {
			t.Fatalf("handshake: %v", err)
		}
		if err := c.fill(); err != nil {
			t.Fatalf("handshake read: %v", err)
		}
	}
}

func (c *client) write(b []byte) {
	c.t.Helper()
	if _, err := c.nc.Write(b); err != nil {
		c.t.Fatal(err)
	}
}

func (c *client) send(op ws.Opcode, payload string) {
	c.t.Helper()
	key := [4]byte{1, 2, 3, 4}
	b := ws.AppendHeader(nil, ws.Header{Fin: true, Opcode: op, Masked: true, Mask: key, Length: int64(len(payload))})
	start := len(b)
	b = append(b, payload...)
	ws.Mask(key, 0, b[start:])
	c.write(b)
}

func (c *client) fill() error {
	_ = c.nc.SetReadDeadline(time.Now().Add(waitTimeout))
	tmp := make([]byte, 4096)
	n, err := c.nc.Read(tmp)
	c.buf = append(c.buf, tmp[:n]...)
	if n > 0 {
		return nil
	}
	return err
}

func (c *client) read() (ws.Opcode, string, error) {
	for {
		m, n, err := c.dec.Next(c.buf)
		c.buf = c.buf[n:]
		if err == nil {
			return m.Opcode, string(m.Payload), nil
		}
		if !errors.Is(err, ws.ErrNeedMore) {
			return 0, "", err
		}
		if err := c.fill(); err != nil {
			return 0, "", err
		}
	}
}

func (c *client) expect(op ws.Opcode, payload string) {
	c.t.Helper()
	gotOp, got, err := c.read()
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	if gotOp != op || got != payload {
		c.t.Fatalf("got %v %q, want %v %q", gotOp, got, op, payload)
	}
}

func (c *client) expectClose(code ws.CloseCode) {
	c.t.Helper()
	op, p, err := c.read()
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	if op != ws.OpClose {
		c.t.Fatalf("got %v, want close", op)
	}
	if got, _, _ := ws.ParseClose([]byte(p)); got != code {
		c.t.Fatalf("close code = %d, want %d", got, code)
	}
}

// --- tests ---

func TestEcho(t *testing.T) {
	rec := newRecorder()
	_, addr := startServer(t, Config{}, rec)
	c := dial(t, addr)
	rec.wait(t, "open")

	c.send(ws.OpText, "hello")
	c.expect(ws.OpText, "hello")
	c.send(ws.OpBinary, "\x00\x01")
	c.expect(ws.OpBinary, "\x00\x01")
}

func TestPingPong(t *testing.T) {
	_, addr := startServer(t, Config{}, newRecorder())
	c := dial(t, addr)
	c.send(ws.OpPing, "abc")
	c.expect(ws.OpPong, "abc")
}

func TestClientClose(t *testing.T) {
	rec := newRecorder()
	_, addr := startServer(t, Config{}, rec)
	c := dial(t, addr)
	open := rec.wait(t, "open")

	c.send(ws.OpClose, string(ws.AppendClose(nil, ws.CloseNormal, "bye")))
	c.expectClose(ws.CloseNormal)
	if ev := rec.wait(t, "close"); ev.err != nil {
		t.Errorf("OnClose err = %v, want nil", ev.err)
	}
	if _, _, err := c.read(); !errors.Is(err, io.EOF) {
		t.Errorf("after close: err = %v, want EOF", err)
	}
	if err := open.conn.SendOrClose(ws.OpText, []byte("x")); !errors.Is(err, transport.ErrClosed) {
		t.Errorf("Send after close: err = %v, want ErrClosed", err)
	}
}

func TestProtocolError(t *testing.T) {
	rec := newRecorder()
	_, addr := startServer(t, Config{}, rec)
	c := dial(t, addr)
	rec.wait(t, "open")

	c.write(ws.AppendFrame(nil, ws.OpText, []byte("unmasked")))
	c.expectClose(ws.CloseProtocolError)
	if ev := rec.wait(t, "close"); !errors.Is(ev.err, ws.ErrMaskMismatch) {
		t.Errorf("OnClose err = %v", ev.err)
	}
}

func TestMessageTooBig(t *testing.T) {
	_, addr := startServer(t, Config{MaxMessageSize: 10}, newRecorder())
	c := dial(t, addr)
	c.send(ws.OpText, "12345678901")
	c.expectClose(ws.CloseMessageTooBig)
}

func TestSlowConsumer(t *testing.T) {
	rec := newRecorder()
	sendErr := make(chan error, 1)
	rec.onMessage = func(c transport.Conn, _ ws.Opcode, _ []byte) {
		big := make([]byte, 64<<10)
		for range 100_000 {
			if err := c.SendOrClose(ws.OpBinary, big); err != nil {
				sendErr <- err
				return
			}
		}
		sendErr <- nil
	}
	_, addr := startServer(t, Config{SendQueueSize: 4, WriteTimeout: 200 * time.Millisecond}, rec)
	c := dial(t, addr)
	rec.wait(t, "open")

	c.send(ws.OpText, "flood me") // and never read
	if err := <-sendErr; !errors.Is(err, transport.ErrSlowConsumer) {
		t.Fatalf("Send err = %v, want ErrSlowConsumer", err)
	}
	if ev := rec.wait(t, "close"); !errors.Is(ev.err, transport.ErrSlowConsumer) {
		t.Errorf("OnClose err = %v", ev.err)
	}
}

func TestIdleTimeout(t *testing.T) {
	rec := newRecorder()
	cfg := Config{PingInterval: 20 * time.Millisecond, IdleTimeout: 100 * time.Millisecond}
	_, addr := startServer(t, cfg, rec)
	dial(t, addr) // never answers pings
	rec.wait(t, "open")

	if ev := rec.wait(t, "close"); !errors.Is(ev.err, transport.ErrIdleTimeout) {
		t.Errorf("OnClose err = %v, want ErrIdleTimeout", ev.err)
	}
}

func TestActiveClientStaysOpen(t *testing.T) {
	rec := newRecorder()
	cfg := Config{PingInterval: 20 * time.Millisecond, IdleTimeout: 100 * time.Millisecond}
	_, addr := startServer(t, cfg, rec)
	c := dial(t, addr)
	rec.wait(t, "open")

	for deadline := time.Now().Add(400 * time.Millisecond); time.Now().Before(deadline); {
		op, p, err := c.read()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if op == ws.OpPing {
			c.send(ws.OpPong, p)
		}
	}
	select {
	case ev := <-rec.events:
		t.Fatalf("unexpected %q event: %v", ev.kind, ev.err)
	default:
	}
}

func TestHandshakeTimeout(t *testing.T) {
	_, addr := startServer(t, Config{HandshakeTimeout: 50 * time.Millisecond}, newRecorder())
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = nc.Close() }()

	_ = nc.SetReadDeadline(time.Now().Add(waitTimeout))
	if _, err := nc.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF (server closed silent client)", err)
	}
}

func TestHandshakeRejected(t *testing.T) {
	cfg := Config{CheckOrigin: func(r ws.Request) bool { return r.Origin == "https://good.example" }}
	_, addr := startServer(t, cfg, newRecorder())

	tests := []struct {
		name, req, status string
	}{
		{"not websocket", "GET / HTTP/1.1\r\nHost: x\r\n\r\n", "400"},
		{"bad origin", "GET /ws HTTP/1.1\r\nHost: x\r\nOrigin: https://evil.example\r\n" +
			"Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\n" +
			"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n", "403"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nc, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = nc.Close() }()
			if _, err := nc.Write([]byte(tc.req)); err != nil {
				t.Fatal(err)
			}
			_ = nc.SetReadDeadline(time.Now().Add(waitTimeout))
			resp, _ := io.ReadAll(nc)
			if want := "HTTP/1.1 " + tc.status; !strings.HasPrefix(string(resp), want) {
				t.Fatalf("response %q, want prefix %q", resp, want)
			}
		})
	}
}

func TestShutdown(t *testing.T) {
	rec := newRecorder()
	s, addr := startServer(t, Config{}, rec)
	c := dial(t, addr)
	rec.wait(t, "open")

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()
		done <- s.Shutdown(ctx)
	}()

	c.expectClose(ws.CloseGoingAway)
	c.send(ws.OpClose, string(ws.AppendClose(nil, ws.CloseGoingAway, "")))
	if err := <-done; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if ev := rec.wait(t, "close"); !errors.Is(ev.err, transport.ErrShutdown) {
		t.Errorf("OnClose err = %v, want ErrShutdown", ev.err)
	}
}

func TestShutdownDoesNotWaitForBlockedHandler(t *testing.T) {
	rec := newRecorder()
	release := make(chan struct{})
	defer close(release) // lets the stuck handler finish before cleanup
	rec.onMessage = func(transport.Conn, ws.Opcode, []byte) { <-release }

	s, addr := startServer(t, Config{}, rec)
	c := dial(t, addr)
	rec.wait(t, "open")
	c.send(ws.OpText, "block")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := s.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown err = %v, want DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Shutdown took %v after its deadline", elapsed)
	}
}
