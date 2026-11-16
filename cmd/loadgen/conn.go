package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/xzwache/korb-m/internal/ws"
)

const handshakeTimeout = 10 * time.Second

// clientConn is one load-generating WebSocket connection.
type clientConn struct {
	nc  net.Conn
	cfg *config
	st  *stats
	wmu sync.Mutex // reader (pongs) and writer (messages) both write
}

func runConn(ctx context.Context, cfg *config, st *stats, source net.IP) {
	st.dialing.Add(1)
	nc, err := dial(ctx, cfg, source)
	st.dialing.Add(-1)
	if err != nil {
		st.fail(err)
		return
	}
	defer func() { _ = nc.Close() }()

	st.connected.Add(1)
	defer st.connected.Add(-1)

	c := &clientConn{nc: nc, cfg: cfg, st: st}
	stop := context.AfterFunc(ctx, func() { _ = nc.Close() })
	defer stop()

	if cfg.interval > 0 {
		go c.writeLoop(ctx)
	}
	if err := c.readLoop(); err != nil && ctx.Err() == nil {
		st.fail(err)
	}
}

func dial(ctx context.Context, cfg *config, source net.IP) (net.Conn, error) {
	d := net.Dialer{Timeout: handshakeTimeout}
	if source != nil {
		d.LocalAddr = &net.TCPAddr{IP: source}
	}
	nc, err := d.DialContext(ctx, "tcp", cfg.addr)
	if err != nil {
		return nil, err
	}
	if err := handshake(nc, cfg); err != nil {
		_ = nc.Close()
		return nil, fmt.Errorf("handshake: %w", err)
	}
	return nc, nil
}

func handshake(nc net.Conn, cfg *config) error {
	if err := nc.SetDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		return err
	}
	key := ws.NewKey()
	if _, err := nc.Write(ws.AppendRequest(nil, cfg.host, cfg.path, key)); err != nil {
		return err
	}
	buf := make([]byte, 1024)
	n := 0
	for {
		r, err := nc.Read(buf[n:])
		n += r
		if _, perr := ws.ParseResponse(buf[:n], key); !errors.Is(perr, ws.ErrNeedMore) {
			if perr != nil {
				return perr
			}
			return nc.SetDeadline(time.Time{})
		}
		if err != nil {
			return err
		}
		if n == len(buf) {
			return errors.New("response too large")
		}
	}
}

func (c *clientConn) readLoop() error {
	buf := make([]byte, 1024)
	n := 0
	dec := ws.NewDecoder(ws.SideClient, 1<<20)
	for {
		r, err := c.nc.Read(buf[n:])
		n += r
		start := 0
		for {
			m, used, derr := dec.Next(buf[start:n])
			start += used
			if errors.Is(derr, ws.ErrNeedMore) {
				break
			}
			if derr != nil {
				return derr
			}
			switch m.Opcode {
			case ws.OpPing:
				if err := c.write(ws.OpPong, m.Payload); err != nil {
					return err
				}
			case ws.OpClose:
				return errors.New("closed by server")
			case ws.OpText, ws.OpBinary:
				c.st.received.Add(1)
				if len(m.Payload) >= 8 {
					sent := int64(binary.BigEndian.Uint64(m.Payload)) //nolint:gosec // our own timestamp
					c.st.observe(time.Duration(time.Now().UnixNano() - sent))
				}
			}
		}
		n = copy(buf, buf[start:n])
		if err != nil {
			return err
		}
	}
}

func (c *clientConn) writeLoop(ctx context.Context) {
	// Spread connections over the interval so they don't all send at once.
	jitter := time.Duration(rand.Int64N(int64(c.cfg.interval))) //nolint:gosec // timing jitter, not security
	select {
	case <-time.After(jitter):
	case <-ctx.Done():
		return
	}
	t := time.NewTicker(c.cfg.interval)
	defer t.Stop()
	payload := make([]byte, max(c.cfg.size, 8))
	for {
		binary.BigEndian.PutUint64(payload, uint64(time.Now().UnixNano())) //nolint:gosec // positive
		if err := c.write(ws.OpBinary, payload); err != nil {
			return
		}
		c.st.sent.Add(1)
		select {
		case <-t.C:
		case <-ctx.Done():
			return
		}
	}
}

// write sends one masked frame (clients must mask, RFC 6455 §5.3).
func (c *clientConn) write(op ws.Opcode, payload []byte) error {
	key := [4]byte{}
	binary.LittleEndian.PutUint32(key[:], rand.Uint32()) //nolint:gosec // load tool; the SDK will use crypto/rand
	frame := ws.AppendHeader(nil, ws.Header{Fin: true, Opcode: op, Masked: true, Mask: key, Length: int64(len(payload))})
	start := len(frame)
	frame = append(frame, payload...)
	ws.Mask(key, 0, frame[start:])

	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, err := c.nc.Write(frame)
	return err
}
