//go:build linux || darwin

package evloop

import (
	"errors"
	"os"
	"runtime"
	"sync"

	"golang.org/x/sys/unix"

	"github.com/xzwache/korb-m/internal/poller"
	"github.com/xzwache/korb-m/internal/ws"
)

const (
	readBufferSize = 64 << 10
	maxEvents      = 256
	acceptBatch    = 64 // accepts per readiness event, so one loop can't starve its conns
)

type taskKind uint8

const (
	taskSend   taskKind = iota // queue a frame on conn
	taskClose                  // conn.closeWith was called from outside the loop
	taskAccept                 // a connection accepted by another loop
	taskStop                   // server shutdown
)

type task struct {
	kind    taskKind
	conn    *conn
	op      ws.Opcode
	payload []byte
	fd      int
	sa      unix.Sockaddr
}

type loop struct {
	srv      *Server
	id       int
	poller   *poller.Poller
	listenFd int // -1 if this loop does not accept
	spareFd  int // reserved fd, released to shed connections on EMFILE
	conns    map[int]*conn
	buf      []byte // read buffer shared by all conns of this loop
	events   []poller.Event
	dirty    []*conn // conns with new bytes in out since the last flush

	mu      sync.Mutex
	inbox   []task
	spare   []task // the other inbox slice, swapped in on drain
	stopped bool
}

func newLoop(s *Server, id int) (*loop, error) {
	p, err := poller.New()
	if err != nil {
		return nil, err
	}
	spare, err := unix.Open("/dev/null", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		_ = p.Close()
		return nil, os.NewSyscallError("open /dev/null", err)
	}
	return &loop{
		srv:      s,
		id:       id,
		poller:   p,
		listenFd: -1,
		spareFd:  spare,
		conns:    make(map[int]*conn),
		buf:      make([]byte, readBufferSize),
		events:   make([]poller.Event, maxEvents),
	}, nil
}

func (l *loop) setListener(fd int) error {
	if err := l.poller.Add(fd, poller.Read); err != nil {
		_ = unix.Close(fd)
		return err
	}
	l.listenFd = fd
	return nil
}

// push hands a task to the loop from any goroutine. It reports false if
// the loop has stopped.
func (l *loop) push(t task) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		return false
	}
	l.inbox = append(l.inbox, t)
	if len(l.inbox) == 1 {
		// Only the first task needs a wake; the loop drains the whole inbox.
		// Waking under mu guarantees the poller is not closed meanwhile.
		if err := l.poller.Wake(); err != nil {
			l.srv.cfg.Logger.Error("poller wake failed", "loop", l.id, "err", err)
		}
	}
	return true
}

func (l *loop) run() {
	// The loop blocks in raw syscalls; keep it on one thread.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	defer l.stop()

	for {
		n, err := l.poller.Wait(l.events, -1)
		if err != nil {
			l.srv.cfg.Logger.Error("poller wait failed", "loop", l.id, "err", err)
			return
		}
		for _, e := range l.events[:n] {
			if e.FileDescriptor == l.listenFd {
				l.accept()
				continue
			}
			c := l.conns[e.FileDescriptor]
			if c == nil {
				continue
			}
			if e.Readable || e.Hangup {
				l.onReadable(c, e.Hangup)
			}
			if e.Writable && c.state != stateDead {
				l.flush(c)
			}
		}
		if stop := l.drain(); stop {
			return
		}
		l.flushDirty()
	}
}

// drain runs the queued tasks. It reports true on taskStop.
func (l *loop) drain() (stop bool) {
	l.mu.Lock()
	tasks := l.inbox
	l.inbox = l.spare[:0]
	l.mu.Unlock()

	for i := range tasks {
		t := &tasks[i]
		switch t.kind {
		case taskSend:
			l.send(t.conn, t.op, t.payload)
		case taskClose:
			l.closeRequested(t.conn)
		case taskAccept:
			l.register(t.fd, t.sa)
		case taskStop:
			stop = true
		}
		*t = task{} // drop references so payloads can be collected
	}
	l.spare = tasks[:0]
	return stop
}

func (l *loop) accept() {
	for range acceptBatch {
		fd, sa, err := accept(l.listenFd)
		switch {
		case err == nil:
		case errors.Is(err, unix.EAGAIN):
			return
		case errors.Is(err, unix.EINTR), errors.Is(err, unix.ECONNABORTED):
			continue
		case errors.Is(err, unix.EMFILE), errors.Is(err, unix.ENFILE):
			l.srv.cfg.Logger.Warn("accept: out of file descriptors, shedding a connection", "loop", l.id, "err", err)
			l.shed()
			return
		default:
			l.srv.cfg.Logger.Error("accept failed", "loop", l.id, "err", os.NewSyscallError("accept", err))
			return
		}

		target := l.srv.pick(l)
		if target == l {
			l.register(fd, sa)
		} else if !target.push(task{kind: taskAccept, fd: fd, sa: sa}) {
			_ = unix.Close(fd)
		}
	}
}

// shed handles EMFILE. The listener stays readable while connections wait
// in the backlog, so a level-triggered loop would spin at 100% CPU. Free the
// spare fd, accept one connection and close it at once, then re-reserve.
func (l *loop) shed() {
	_ = unix.Close(l.spareFd)
	if fd, _, err := accept(l.listenFd); err == nil {
		_ = unix.Close(fd)
	}
	l.spareFd, _ = unix.Open("/dev/null", unix.O_RDONLY|unix.O_CLOEXEC, 0)
}

func (l *loop) register(fd int, sa unix.Sockaddr) {
	_ = unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_NODELAY, 1)
	if err := l.poller.Add(fd, poller.Read); err != nil {
		l.srv.cfg.Logger.Error("poller add failed", "loop", l.id, "err", err)
		_ = unix.Close(fd)
		return
	}
	c := newConn(l, fd, fromSockaddr(sa))
	l.conns[fd] = c
}

func (l *loop) flushDirty() {
	for i, c := range l.dirty {
		c.dirty = false
		if c.state != stateDead {
			l.flush(c)
		}
		l.dirty[i] = nil
	}
	l.dirty = l.dirty[:0]
}

// stop closes everything the loop owns. Runs on the loop goroutine.
func (l *loop) stop() {
	if l.listenFd >= 0 {
		_ = unix.Close(l.listenFd)
		l.listenFd = -1
	}
	for _, c := range l.conns {
		l.shutdownConn(c)
	}

	l.mu.Lock()
	l.stopped = true
	leftover := l.inbox
	l.inbox = nil
	l.mu.Unlock()
	for _, t := range leftover {
		if t.kind == taskAccept {
			_ = unix.Close(t.fd)
		}
	}
	l.closeResources()
}

// closeResources releases the poller and the reserved fds. The caller makes
// sure nobody can push anymore (stopped is set, or the loop never ran).
func (l *loop) closeResources() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stopped = true
	if l.listenFd >= 0 {
		_ = unix.Close(l.listenFd)
		l.listenFd = -1
	}
	if l.spareFd >= 0 {
		_ = unix.Close(l.spareFd)
		l.spareFd = -1
	}
	if l.poller != nil {
		_ = l.poller.Close()
		l.poller = nil
	}
}
