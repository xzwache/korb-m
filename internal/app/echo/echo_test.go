package echo

import (
	"log/slog"
	"net"
	"testing"

	"github.com/xzwache/korb-m/internal/transport"
	"github.com/xzwache/korb-m/internal/ws"
)

type fakeConn struct {
	id   uint64
	sent []string
}

func (f *fakeConn) ID() uint64                 { return f.id }
func (f *fakeConn) RemoteAddr() net.Addr       { return &net.TCPAddr{} }
func (f *fakeConn) Close(ws.CloseCode, string) {}
func (f *fakeConn) SendOrClose(_ ws.Opcode, p []byte) error {
	f.sent = append(f.sent, string(p))
	return nil
}

var _ transport.Conn = (*fakeConn)(nil)

func setup(mode Mode, n int) (*App, []*fakeConn) {
	a := New(mode, slog.New(slog.DiscardHandler))
	conns := make([]*fakeConn, n)
	for i := range conns {
		conns[i] = &fakeConn{id: uint64(i + 1)}
		a.OnOpen(conns[i], ws.Request{})
	}
	return a, conns
}

func TestEcho(t *testing.T) {
	a, c := setup(ModeEcho, 2)
	a.OnMessage(c[0], ws.OpText, []byte("hi"))
	if len(c[0].sent) != 1 || c[0].sent[0] != "hi" {
		t.Errorf("sender got %q", c[0].sent)
	}
	if len(c[1].sent) != 0 {
		t.Errorf("other conn got %q", c[1].sent)
	}
}

func TestBroadcast(t *testing.T) {
	a, c := setup(ModeBroadcast, 3)
	a.OnClose(c[2], nil)
	a.OnMessage(c[0], ws.OpText, []byte("hi"))

	for i, want := range []int{1, 1, 0} {
		if got := len(c[i].sent); got != want {
			t.Errorf("conn %d got %d messages, want %d", i, got, want)
		}
	}
	if a.Len() != 2 {
		t.Errorf("Len = %d, want 2", a.Len())
	}
}

func TestPayloadIsCopied(t *testing.T) {
	a := New(ModeEcho, slog.New(slog.DiscardHandler))
	var got []byte
	c := &captureConn{fakeConn: fakeConn{id: 1}, got: &got}
	a.OnOpen(c, ws.Request{})

	buf := []byte("hello")
	a.OnMessage(c, ws.OpText, buf)
	buf[0] = 'X' // the transport reuses its buffer after OnMessage returns
	if string(got) != "hello" {
		t.Fatalf("queued payload changed to %q", got)
	}
}

type captureConn struct {
	fakeConn
	got *[]byte
}

func (c *captureConn) SendOrClose(_ ws.Opcode, p []byte) error { *c.got = p; return nil }

func TestParseMode(t *testing.T) {
	if m, err := ParseMode("broadcast"); err != nil || m != ModeBroadcast {
		t.Errorf("broadcast: %v %v", m, err)
	}
	if _, err := ParseMode("nope"); err == nil {
		t.Error("expected error for unknown mode")
	}
}
