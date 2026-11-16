// Package echo is a demo application: it echoes messages back to the sender
// or broadcasts them to every connected client.
package echo

import (
	"bytes"
	"fmt"
	"log/slog"
	"sync"

	"github.com/xzwache/korb-m/internal/transport"
	"github.com/xzwache/korb-m/internal/ws"
)

// Mode selects what happens to a received message.
type Mode int

// Modes.
const (
	ModeEcho      Mode = iota // send back to the sender
	ModeBroadcast             // send to every connection, including the sender
)

// ParseMode parses "echo" or "broadcast".
func ParseMode(s string) (Mode, error) {
	switch s {
	case "echo":
		return ModeEcho, nil
	case "broadcast":
		return ModeBroadcast, nil
	}
	return 0, fmt.Errorf("unknown mode %q (want echo or broadcast)", s)
}

// App implements transport.Handler.
type App struct {
	mode Mode
	log  *slog.Logger

	mu    sync.RWMutex
	conns map[uint64]transport.Conn
}

// New returns an App in the given mode. A nil log uses slog.Default().
func New(mode Mode, log *slog.Logger) *App {
	if log == nil {
		log = slog.Default()
	}
	return &App{mode: mode, log: log, conns: make(map[uint64]transport.Conn)}
}

// Len returns the number of open connections.
func (a *App) Len() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.conns)
}

// OnOpen registers the connection.
func (a *App) OnOpen(c transport.Conn, r ws.Request) {
	a.mu.Lock()
	a.conns[c.ID()] = c
	a.mu.Unlock()
	a.log.Debug("open", "conn", c.ID(), "remote", c.RemoteAddr().String(), "path", r.Path)
}

// OnClose unregisters the connection.
func (a *App) OnClose(c transport.Conn, err error) {
	a.mu.Lock()
	delete(a.conns, c.ID())
	a.mu.Unlock()
	a.log.Debug("close", "conn", c.ID(), "err", err)
}

// OnMessage echoes or broadcasts the message.
func (a *App) OnMessage(c transport.Conn, op ws.Opcode, payload []byte) {
	// payload is only valid during this call; one copy is shared by all receivers.
	msg := bytes.Clone(payload)

	if a.mode == ModeEcho {
		_ = c.SendOrClose(op, msg)
		return
	}

	// TODO(phase 4): holding the lock while sending to every connection
	// blocks OnOpen/OnClose; rooms will replace this.
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, other := range a.conns {
		_ = other.SendOrClose(op, msg)
	}
}
