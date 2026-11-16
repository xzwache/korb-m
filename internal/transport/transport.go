// Package transport defines the contract between a connection transport
// (goroutine-per-connection or event loop) and the application.
package transport

import (
	"errors"
	"net"

	"github.com/xzwache/korb-m/internal/ws"
)

// Conn is one WebSocket connection.
type Conn interface {
	ID() uint64
	RemoteAddr() net.Addr

	// SendOrClose queues a message and never blocks. The caller must not modify
	// payload afterwards. If the queue is full the connection is closed
	// as a slow consumer and ErrSlowConsumer is returned.
	SendOrClose(op ws.Opcode, payload []byte) error

	// Close starts the close handshake with the given code.
	Close(code ws.CloseCode, reason string)
}

// Handler receives connection events. For one connection the calls are
// sequential: OnOpen, OnMessage..., OnClose.
//
// Handlers must not block: in an event-loop transport a blocking handler
// stalls every connection on that loop.
type Handler interface {
	OnOpen(c Conn, r ws.Request)
	OnMessage(c Conn, op ws.Opcode, payload []byte) // payload is valid only during the call
	OnClose(c Conn, err error)                      // err is nil after a normal close
}

// Reasons a connection was closed, passed to Handler.OnClose.
var (
	ErrClosed       = errors.New("transport: connection closed")
	ErrSlowConsumer = errors.New("transport: send queue full")
	ErrIdleTimeout  = errors.New("transport: idle timeout")
	ErrShutdown     = errors.New("transport: server shutdown")
)
