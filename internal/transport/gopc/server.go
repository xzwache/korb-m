// Package gopc is the goroutine-per-connection transport: each connection
// has a reader goroutine and a writer goroutine.
package gopc

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xzwache/korb-m/internal/transport"
	"github.com/xzwache/korb-m/internal/ws"
)

// Config configures a Server. Zero values get defaults.
type Config struct {
	MaxMessageSize   int64         // default 64 KiB
	SendQueueSize    int           // messages; default 64
	HandshakeTimeout time.Duration // default 5s
	PingInterval     time.Duration // default 30s
	IdleTimeout      time.Duration // close if nothing received for this long; default 60s
	WriteTimeout     time.Duration // per write; default 10s

	// CheckOrigin rejects the handshake when it returns false. nil allows all.
	CheckOrigin func(r ws.Request) bool

	Logger *slog.Logger
}

func (c *Config) setDefaults() {
	if c.MaxMessageSize == 0 {
		c.MaxMessageSize = 64 << 10
	}
	if c.SendQueueSize == 0 {
		c.SendQueueSize = 64
	}
	if c.HandshakeTimeout == 0 {
		c.HandshakeTimeout = 5 * time.Second
	}
	if c.PingInterval == 0 {
		c.PingInterval = 30 * time.Second
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = 60 * time.Second
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = 10 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// ErrServerClosed is returned by Serve after Shutdown.
var ErrServerClosed = errors.New("gopc: server closed")

// Server accepts WebSocket connections and dispatches them to a Handler.
type Server struct {
	cfg     Config
	handler transport.Handler
	nextID  atomic.Uint64

	mu          sync.Mutex
	closing     bool
	listeners   map[net.Listener]struct{}
	connections map[*conn]struct{}
	wg          sync.WaitGroup
}

// New returns a server that delivers connection events to h.
func New(h transport.Handler, cfg Config) *Server {
	cfg.setDefaults()
	return &Server{
		cfg:         cfg,
		handler:     h,
		listeners:   make(map[net.Listener]struct{}),
		connections: make(map[*conn]struct{}),
	}
}

// Serve accepts connections on ln until Shutdown is called.
func (s *Server) Serve(listener net.Listener) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		_ = listener.Close()
		return ErrServerClosed
	}
	s.listeners[listener] = struct{}{}
	s.mu.Unlock()

	var backoff time.Duration
	for {
		netConnection, err := listener.Accept()
		if err != nil {
			if s.isClosing() {
				return ErrServerClosed
			}
			if errors.Is(err, net.ErrClosed) {
				return err
			}
			// Usually EMFILE (out of file descriptors). Back off instead of spinning.
			backoff = min(max(2*backoff, 5*time.Millisecond), time.Second)
			s.cfg.Logger.Warn("accept failed", "err", err, "retry_in", backoff)
			time.Sleep(backoff)
			continue
		}
		backoff = 0

		s.mu.Lock()
		if s.closing {
			s.mu.Unlock()
			_ = netConnection.Close()
			return ErrServerClosed
		}
		s.wg.Go(func() { s.serveConnection(netConnection) })
		s.mu.Unlock()
	}
}

// Shutdown stops accepting, sends close 1001 to every connection and waits
// for them to finish. When ctx expires the remaining sockets are closed hard
// and Shutdown returns without waiting for blocked handlers.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closing = true
	for listener := range s.listeners {
		_ = listener.Close()
	}
	for connection := range s.connections {
		connection.closeWith(ws.CloseGoingAway, "server shutdown", transport.ErrShutdown)
	}
	s.mu.Unlock()

	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		for c := range s.connections {
			_ = c.connection.Close()
		}
		s.mu.Unlock()
		return ctx.Err()
	}
}

func (s *Server) isClosing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closing
}

func (s *Server) serveConnection(netConnection net.Conn) {
	request, pendingMessages, err := s.handshake(netConnection)
	if err != nil {
		s.cfg.Logger.Debug("handshake failed", "remote", netConnection.RemoteAddr(), "err", err)
		_ = netConnection.Close()
		return
	}

	connection := newConnection(s, netConnection)
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		_ = netConnection.Close()
		return
	}
	s.connections[connection] = struct{}{}
	s.mu.Unlock()

	s.handler.OnOpen(connection, request)
	go connection.writeLoop()
	connection.readLoop(pendingMessages)
	<-connection.writerDone
	_ = netConnection.Close()

	s.mu.Lock()
	delete(s.connections, connection)
	s.mu.Unlock()
	s.handler.OnClose(connection, connection.closeErr)
}

// handshake reads the upgrade request and answers it. It returns any bytes
// received after the request head (the client may already send frames).
func (s *Server) handshake(netConnection net.Conn) (ws.Request, []byte, error) {
	if err := netConnection.SetDeadline(time.Now().Add(s.cfg.HandshakeTimeout)); err != nil {
		return ws.Request{}, nil, err
	}
	buf := make([]byte, ws.MaxHeadSize)
	n := 0
	for {
		r, readErr := netConnection.Read(buf[n:])
		n += r

		req, hn, err := ws.ParseRequest(buf[:n])
		if err == nil && s.cfg.CheckOrigin != nil && !s.cfg.CheckOrigin(req) {
			err = ws.ErrOriginForbidden
		}
		switch {
		case err == nil:
			if _, err := netConnection.Write(ws.AppendAcceptResponse(nil, req.Key)); err != nil {
				return ws.Request{}, nil, err
			}
			return req, buf[hn:n], netConnection.SetDeadline(time.Time{})
		case !errors.Is(err, ws.ErrNeedMore):
			var he *ws.HandshakeError
			if errors.As(err, &he) {
				_, _ = netConnection.Write(ws.AppendErrorResponse(nil, he))
			}
			return ws.Request{}, nil, err
		case readErr != nil:
			return ws.Request{}, nil, readErr
		}
	}
}
