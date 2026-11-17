//go:build linux || darwin

package evloop

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xzwache/korb-m/internal/transport"
	"github.com/xzwache/korb-m/internal/ws"
	"github.com/xzwache/korb-m/internal/wstest"
)

const waitTimeout = 2 * time.Second

type event struct {
	kind string // "open" or "close"
	conn transport.Conn
	err  error
}

type recorder struct {
	events    chan event
	onMessage func(c transport.Conn, op ws.Opcode, p []byte) // nil = echo
}

func newRecorder() *recorder { return &recorder{events: make(chan event, 1000)} }

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
			t.Fatalf("got %q event (%v), want %q", ev.kind, ev.err, kind)
		}
		return ev
	case <-time.After(waitTimeout):
		t.Fatalf("timeout waiting for %q event", kind)
		return event{}
	}
}

func startServer(t *testing.T, cfg Config, h transport.Handler) (*Server, string) {
	t.Helper()
	s := New(h, cfg)
	addr, err := s.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- s.Serve() }()
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
	return s, addr.String()
}

func TestEcho(t *testing.T) {
	rec := newRecorder()
	_, addr := startServer(t, Config{}, rec)
	c := wstest.Dial(t, addr)
	rec.wait(t, "open")

	c.Send(ws.OpText, "hello")
	c.Expect(ws.OpText, "hello")
	c.Send(ws.OpBinary, "\x00\x01")
	c.Expect(ws.OpBinary, "\x00\x01")
}

func TestLargeMessage(t *testing.T) {
	_, addr := startServer(t, Config{MaxMessageSize: 1 << 20}, newRecorder())
	c := wstest.Dial(t, addr)
	big := strings.Repeat("x", 300<<10) // larger than the read buffer and the socket buffers
	c.Send(ws.OpBinary, big)
	c.Expect(ws.OpBinary, big)
}

// TestByteByByte sends the handshake and a frame one byte per write, so
// every parser state must survive a partial read.
func TestByteByByte(t *testing.T) {
	_, addr := startServer(t, Config{}, newRecorder())
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = nc.Close() }()

	key := ws.NewKey()
	frame := ws.AppendHeader(nil, ws.Header{Fin: true, Opcode: ws.OpText, Masked: true, Length: 2})
	frame = append(frame, "hi"...)
	for _, b := range append(ws.AppendRequest(nil, "test", "/ws", key), frame...) {
		if _, err := nc.Write([]byte{b}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Microsecond) // let the server read each byte separately
	}

	_ = nc.SetReadDeadline(time.Now().Add(waitTimeout))
	resp, err := io.ReadAll(io.LimitReader(nc, 1024))
	if err != nil && !errors.Is(err, io.EOF) {
		var ne net.Error
		if !errors.As(err, &ne) || !ne.Timeout() {
			t.Fatal(err)
		}
	}
	n, err := ws.ParseResponse(resp, key)
	if err != nil {
		t.Fatalf("handshake response: %v (%q)", err, resp)
	}
	if want := string(ws.AppendFrame(nil, ws.OpText, []byte("hi"))); string(resp[n:]) != want {
		t.Fatalf("got %q, want %q", resp[n:], want)
	}
}

func TestPingPong(t *testing.T) {
	_, addr := startServer(t, Config{}, newRecorder())
	c := wstest.Dial(t, addr)
	c.Send(ws.OpPing, "abc")
	c.Expect(ws.OpPong, "abc")
}

func TestClientClose(t *testing.T) {
	rec := newRecorder()
	_, addr := startServer(t, Config{}, rec)
	c := wstest.Dial(t, addr)
	open := rec.wait(t, "open")

	c.Send(ws.OpClose, string(ws.AppendClose(nil, ws.CloseNormal, "bye")))
	c.ExpectClose(ws.CloseNormal)
	if ev := rec.wait(t, "close"); ev.err != nil {
		t.Errorf("OnClose err = %v, want nil", ev.err)
	}
	if _, _, err := c.Read(); !errors.Is(err, io.EOF) {
		t.Errorf("after close: err = %v, want EOF", err)
	}
	if err := open.conn.SendOrClose(ws.OpText, []byte("x")); !errors.Is(err, transport.ErrClosed) {
		t.Errorf("Send after close: err = %v, want ErrClosed", err)
	}
}

func TestServerClose(t *testing.T) {
	rec := newRecorder()
	_, addr := startServer(t, Config{}, rec)
	c := wstest.Dial(t, addr)
	open := rec.wait(t, "open")

	open.conn.Close(ws.CloseNormal, "done") // from a foreign goroutine
	c.ExpectClose(ws.CloseNormal)
	if ev := rec.wait(t, "close"); ev.err != nil {
		t.Errorf("OnClose err = %v, want nil", ev.err)
	}
}

func TestClientDisconnect(t *testing.T) {
	rec := newRecorder()
	_, addr := startServer(t, Config{}, rec)
	c := wstest.Dial(t, addr)
	rec.wait(t, "open")

	_ = c.Raw.Close()
	if ev := rec.wait(t, "close"); !errors.Is(ev.err, io.EOF) {
		t.Errorf("OnClose err = %v, want EOF", ev.err)
	}
}

func TestProtocolError(t *testing.T) {
	rec := newRecorder()
	_, addr := startServer(t, Config{}, rec)
	c := wstest.Dial(t, addr)
	rec.wait(t, "open")

	c.Write(ws.AppendFrame(nil, ws.OpText, []byte("unmasked")))
	c.ExpectClose(ws.CloseProtocolError)
	if ev := rec.wait(t, "close"); !errors.Is(ev.err, ws.ErrMaskMismatch) {
		t.Errorf("OnClose err = %v", ev.err)
	}
}

func TestMessageTooBig(t *testing.T) {
	_, addr := startServer(t, Config{MaxMessageSize: 10}, newRecorder())
	c := wstest.Dial(t, addr)
	c.Send(ws.OpText, "12345678901")
	c.ExpectClose(ws.CloseMessageTooBig)
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
	_, addr := startServer(t, Config{MaxPendingBytes: 256 << 10}, rec)
	c := wstest.Dial(t, addr)
	rec.wait(t, "open")

	c.Send(ws.OpText, "flood me") // and never read
	if err := <-sendErr; !errors.Is(err, transport.ErrSlowConsumer) {
		t.Fatalf("Send err = %v, want ErrSlowConsumer", err)
	}
	if ev := rec.wait(t, "close"); !errors.Is(ev.err, transport.ErrSlowConsumer) {
		t.Errorf("OnClose err = %v", ev.err)
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
		{"head too large", "GET / HTTP/1.1\r\nX: " + strings.Repeat("a", ws.MaxHeadSize), "431"},
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

// TestBroadcastAcrossLoops sends from one loop's goroutine to connections
// owned by other loops, which goes through their inboxes and Wake.
func TestBroadcastAcrossLoops(t *testing.T) {
	const clients = 40
	var (
		mu    sync.Mutex
		conns []transport.Conn
	)
	rec := newRecorder()
	rec.onMessage = func(_ transport.Conn, op ws.Opcode, p []byte) {
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.SendOrClose(op, bytes.Clone(p))
		}
	}
	_, addr := startServer(t, Config{Loops: 4}, rec)

	cs := make([]*wstest.Client, clients)
	for i := range cs {
		cs[i] = wstest.Dial(t, addr)
		ev := rec.wait(t, "open")
		mu.Lock()
		conns = append(conns, ev.conn)
		mu.Unlock()
	}

	for round := range 3 {
		msg := fmt.Sprintf("round %d", round)
		cs[round].Send(ws.OpText, msg)
		for _, c := range cs {
			c.Expect(ws.OpText, msg)
		}
	}
}

func TestShutdown(t *testing.T) {
	rec := newRecorder()
	s, addr := startServer(t, Config{}, rec)
	c := wstest.Dial(t, addr)
	rec.wait(t, "open")

	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	c.ExpectClose(ws.CloseGoingAway)
	if ev := rec.wait(t, "close"); !errors.Is(ev.err, transport.ErrShutdown) {
		t.Errorf("OnClose err = %v, want ErrShutdown", ev.err)
	}
	if _, err := net.Dial("tcp", addr); err == nil {
		t.Error("listener still accepts after Shutdown")
	}
}

func TestShutdownDoesNotWaitForBlockedHandler(t *testing.T) {
	rec := newRecorder()
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	rec.onMessage = func(transport.Conn, ws.Opcode, []byte) { close(entered); <-release }

	s, addr := startServer(t, Config{Loops: 1}, rec)
	c := wstest.Dial(t, addr)
	rec.wait(t, "open")
	c.Send(ws.OpText, "block")
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := s.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown err = %v, want DeadlineExceeded", err)
	}
}

func TestShutdownBeforeServe(t *testing.T) {
	s := New(newRecorder(), Config{})
	if _, err := s.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Serve(); !errors.Is(err, ErrServerClosed) {
		t.Fatalf("Serve after Shutdown = %v", err)
	}
}
