// Package poller wraps the OS readiness API: epoll on Linux, kqueue on macOS.
//
// It is level-triggered: Wait keeps reporting a socket while it has unread
// data (or room to write), so a caller may read only part of it and get
// the rest on the next Wait.
package poller

import "errors"

// Interest is what a caller wants to be told about.
type Interest uint8

// Interests; combine with |.
const (
	Read Interest = 1 << iota
	Write
)

// Event reports the readiness of one fd. With kqueue, read and write
// readiness of the same fd may come as two separate events.
type Event struct {
	FileDescriptor int
	Readable       bool
	Writable       bool
	Hangup         bool // peer closed or error; reading returns EOF or the error
}

// ErrUnsupported is returned by New on platforms without epoll or kqueue.
var ErrUnsupported = errors.New("poller: unsupported platform")
