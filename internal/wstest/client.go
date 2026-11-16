// Package wstest provides a minimal WebSocket client for tests.
package wstest

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/xzwache/korb-m/internal/ws"
)

// Timeout bounds every read the client does.
const Timeout = 2 * time.Second

// Client is a test WebSocket client. Any I/O failure fails the test.
type Client struct {
	t   testing.TB
	Raw net.Conn
	dec *ws.Decoder
	buf []byte
}

// Dial connects to addr and completes the handshake.
func Dial(t testing.TB, addr string) *Client {
	t.Helper()
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nc.Close() })

	c := &Client{t: t, Raw: nc, dec: ws.NewDecoder(ws.SideClient, 1<<20)}
	key := ws.NewKey()
	c.Write(ws.AppendRequest(nil, "test", "/ws", key))
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

// Write sends raw bytes.
func (c *Client) Write(b []byte) {
	c.t.Helper()
	if _, err := c.Raw.Write(b); err != nil {
		c.t.Fatal(err)
	}
}

// Send sends one masked frame.
func (c *Client) Send(op ws.Opcode, payload string) {
	c.t.Helper()
	key := [4]byte{1, 2, 3, 4}
	b := ws.AppendHeader(nil, ws.Header{Fin: true, Opcode: op, Masked: true, Mask: key, Length: int64(len(payload))})
	start := len(b)
	b = append(b, payload...)
	ws.Mask(key, 0, b[start:])
	c.Write(b)
}

// Read returns the next message or control frame.
func (c *Client) Read() (ws.Opcode, string, error) {
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

// Expect reads the next frame and fails unless it matches.
func (c *Client) Expect(op ws.Opcode, payload string) {
	c.t.Helper()
	gotOp, got, err := c.Read()
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	if gotOp != op || got != payload {
		c.t.Fatalf("got %v %q, want %v %q", gotOp, got, op, payload)
	}
}

// ExpectClose reads the next frame and fails unless it is a close with code.
func (c *Client) ExpectClose(code ws.CloseCode) {
	c.t.Helper()
	op, p, err := c.Read()
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

// Sync does a ping round trip. When it returns, the server has finished
// OnOpen for this connection (OnOpen runs before the server reads frames).
func (c *Client) Sync() {
	c.t.Helper()
	c.Send(ws.OpPing, "sync")
	c.Expect(ws.OpPong, "sync")
}

func (c *Client) fill() error {
	_ = c.Raw.SetReadDeadline(time.Now().Add(Timeout))
	tmp := make([]byte, 4096)
	n, err := c.Raw.Read(tmp)
	c.buf = append(c.buf, tmp[:n]...)
	if n > 0 {
		return nil
	}
	return err
}
