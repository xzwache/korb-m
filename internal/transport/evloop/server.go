//go:build linux || darwin

package evloop

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/xzwache/korb-m/internal/transport"
	"github.com/xzwache/korb-m/internal/ws"
)

// Config configures a Server. Zero values get defaults.
type Config struct {
	Loops           int   // default GOMAXPROCS
	MaxMessageSize  int64 // default 64 KiB
	MaxPendingBytes int64 // unsent bytes per connection before it is kicked as slow; default 1 MiB

	// CheckOrigin rejects the handshake when it returns false. nil allows all.
	CheckOrigin func(r ws.Request) bool

	Logger *slog.Logger
}

func (c *Config) setDefaults() {
	if c.Loops <= 0 {
		c.Loops = runtime.GOMAXPROCS(0)
	}
	if c.MaxMessageSize == 0 {
		c.MaxMessageSize = 64 << 10
	}
	if c.MaxPendingBytes == 0 {
		c.MaxPendingBytes = 1 << 20
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// Errors returned by Server.
var (
	ErrServerClosed = errors.New("evloop: server closed")
	ErrNotListening = errors.New("evloop: Serve called before Listen")
)

// Server runs the event loops. Call Listen, then Serve.
type Server struct {
	cfg     Config
	handler transport.Handler
	nextID  atomic.Uint64
	nextRR  atomic.Uint64 // round-robin counter when one loop accepts for all

	mu      sync.Mutex
	loops   []*loop
	serving bool
	closing bool
	done    chan struct{} // closed when all loops have exited
}

// New returns a server that delivers connection events to h.
func New(h transport.Handler, cfg Config) *Server {
	cfg.setDefaults()
	return &Server{cfg: cfg, handler: h, done: make(chan struct{})}
}

// Listen creates the loops and their listening sockets and returns the
// bound address (useful with port 0).
func (s *Server) Listen(address string) (net.Addr, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return nil, ErrServerClosed
	}
	if s.loops != nil {
		return nil, errors.New("evloop: Listen called twice")
	}

	loops := make([]*loop, s.cfg.Loops)
	closeAll := func() {
		for _, l := range loops {
			if l != nil {
				l.closeResources()
			}
		}
	}
	for i := range loops {
		if loops[i], err = newLoop(s, i); err != nil {
			closeAll()
			return nil, err
		}
	}

	var bound *net.TCPAddr
	for i, l := range loops {
		if i > 0 && !reusePort {
			break // one listener, see sys_darwin.go
		}
		fd, err := listenSocket(tcpAddr)
		if err == nil {
			err = l.setListener(fd)
		}
		if err == nil && i == 0 {
			// With port 0 the kernel picks a port; the other sockets must reuse it.
			bound, err = localAddr(fd)
			tcpAddr = bound
		}
		if err != nil {
			closeAll()
			return nil, err
		}
	}
	s.loops = loops
	return bound, nil
}

// Serve runs the loops and blocks until Shutdown. It returns ErrServerClosed.
func (s *Server) Serve() error {
	s.mu.Lock()
	switch {
	case s.closing:
		s.mu.Unlock()
		return ErrServerClosed
	case s.loops == nil:
		s.mu.Unlock()
		return ErrNotListening
	case s.serving:
		s.mu.Unlock()
		return errors.New("evloop: Serve called twice")
	}
	s.serving = true
	loops := s.loops
	s.mu.Unlock()

	var wg sync.WaitGroup
	for _, l := range loops {
		wg.Go(l.run)
	}
	wg.Wait()
	close(s.done)
	return ErrServerClosed
}

// Shutdown stops accepting, sends close 1001 to every connection and
// closes them. If ctx expires first (a handler is blocking a loop) it
// returns ctx.Err() without waiting.
//
// TODO(3.3): wait for the clients' close replies, bounded by a timer.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil
	}
	s.closing = true
	loops, serving := s.loops, s.serving
	s.mu.Unlock()

	if !serving {
		for _, l := range loops {
			l.closeResources()
		}
		return nil
	}
	for _, l := range loops {
		l.push(task{kind: taskStop})
	}
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// pick returns the loop that will own a connection accepted by from.
func (s *Server) pick(from *loop) *loop {
	if reusePort {
		return from
	}
	return s.loops[s.nextRR.Add(1)%uint64(len(s.loops))]
}
