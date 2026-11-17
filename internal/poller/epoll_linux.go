//go:build linux

package poller

import (
	"encoding/binary"
	"errors"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// Poller is an epoll instance plus an eventfd used by Wake.
type Poller struct {
	epfd   int
	wakefd int
	buf    []unix.EpollEvent
}

// New creates a poller.
func New() (*Poller, error) {
	epfd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, os.NewSyscallError("epoll_create1", err)
	}
	wakefd, err := unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC)
	if err != nil {
		_ = unix.Close(epfd)
		return nil, os.NewSyscallError("eventfd", err)
	}
	p := &Poller{epfd: epfd, wakefd: wakefd}
	if err := p.Add(wakefd, Read); err != nil {
		_ = p.Close()
		return nil, err
	}
	return p, nil
}

func toEpoll(in Interest) uint32 {
	var ev uint32
	if in&Read != 0 {
		ev |= unix.EPOLLIN | unix.EPOLLRDHUP
	}
	if in&Write != 0 {
		ev |= unix.EPOLLOUT
	}
	return ev
}

// Add starts watching fd.
func (p *Poller) Add(fd int, interest Interest) error {
	epollEvent := &unix.EpollEvent{Events: toEpoll(interest), Fd: int32(fd)} //nolint:gosec // fds fit in int32
	return os.NewSyscallError("epoll_ctl add", unix.EpollCtl(p.epfd, unix.EPOLL_CTL_ADD, fd, epollEvent))
}

// Modify replaces the interest for fd.
func (p *Poller) Modify(fd int, in Interest) error {
	err := unix.EpollCtl(p.epfd, unix.EPOLL_CTL_MOD, fd, &unix.EpollEvent{Events: toEpoll(in), Fd: int32(fd)}) //nolint:gosec // fds fit in int32
	return os.NewSyscallError("epoll_ctl mod", err)
}

// Delete stops watching fd. Closing fd also removes it.
func (p *Poller) Delete(fd int) error {
	return os.NewSyscallError("epoll_ctl del", unix.EpollCtl(p.epfd, unix.EPOLL_CTL_DEL, fd, nil))
}

// Wait blocks until events arrive, Wake is called or timeout passes
// (timeout < 0 waits forever). It fills events and returns how many.
func (p *Poller) Wait(events []Event, timeout time.Duration) (int, error) {
	if cap(p.buf) < len(events) {
		p.buf = make([]unix.EpollEvent, len(events))
	}
	msec := -1
	if timeout >= 0 {
		msec = int((timeout + time.Millisecond - 1) / time.Millisecond) // round up: 0.5ms must not become 0 (busy loop)
	}
	n, err := unix.EpollWait(p.epfd, p.buf[:len(events)], msec)
	if errors.Is(err, unix.EINTR) {
		return 0, nil
	}
	if err != nil {
		return 0, os.NewSyscallError("epoll_wait", err)
	}

	out := 0
	for _, e := range p.buf[:n] {
		if int(e.Fd) == p.wakefd {
			var b [8]byte
			_, _ = unix.Read(p.wakefd, b[:]) // reset the counter
			continue
		}
		events[out] = Event{
			FileDescriptor: int(e.Fd),
			Readable:       e.Events&unix.EPOLLIN != 0,
			Writable:       e.Events&unix.EPOLLOUT != 0,
			Hangup:         e.Events&(unix.EPOLLHUP|unix.EPOLLRDHUP|unix.EPOLLERR) != 0,
		}
		out++
	}
	return out, nil
}

// Wake makes a blocked Wait return. Safe to call from any goroutine.
func (p *Poller) Wake() error {
	var b [8]byte
	binary.NativeEndian.PutUint64(b[:], 1)
	_, err := unix.Write(p.wakefd, b[:])
	if errors.Is(err, unix.EAGAIN) {
		return nil // counter is full, a wake is already pending
	}
	return os.NewSyscallError("eventfd write", err)
}

// Close releases the poller.
func (p *Poller) Close() error {
	return errors.Join(unix.Close(p.wakefd), unix.Close(p.epfd))
}
