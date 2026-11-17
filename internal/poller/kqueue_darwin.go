//go:build darwin

package poller

import (
	"errors"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

const wakeIdent = 0 // EVFILT_USER identifier used by Wake

// Poller is a kqueue instance.
type Poller struct {
	kq  int
	buf []unix.Kevent_t
}

// New creates a poller.
func New() (*Poller, error) {
	kq, err := unix.Kqueue()
	if err != nil {
		return nil, os.NewSyscallError("kqueue", err)
	}
	unix.CloseOnExec(kq)
	p := &Poller{kq: kq}
	wake := unix.Kevent_t{Ident: wakeIdent, Filter: unix.EVFILT_USER, Flags: unix.EV_ADD | unix.EV_CLEAR}
	if err := p.apply(wake); err != nil {
		_ = unix.Close(kq)
		return nil, err
	}
	return p, nil
}

// kqueue watches read and write as two separate filters. Both are always
// registered; interest just enables or disables each one.
func changes(fd int, in Interest, flags int) [2]unix.Kevent_t {
	var ch [2]unix.Kevent_t
	for i, f := range [2]struct {
		filter int
		want   bool
	}{{unix.EVFILT_READ, in&Read != 0}, {unix.EVFILT_WRITE, in&Write != 0}} {
		state := unix.EV_DISABLE
		if f.want {
			state = unix.EV_ENABLE
		}
		unix.SetKevent(&ch[i], fd, f.filter, flags|state)
	}
	return ch
}

// Add starts watching fd.
func (p *Poller) Add(fd int, in Interest) error {
	ch := changes(fd, in, unix.EV_ADD)
	return p.apply(ch[:]...)
}

// Modify replaces the interest for fd.
func (p *Poller) Modify(fd int, in Interest) error {
	ch := changes(fd, in, 0)
	return p.apply(ch[:]...)
}

// Delete stops watching fd. Closing fd also removes it.
func (p *Poller) Delete(fd int) error {
	var ch [2]unix.Kevent_t
	unix.SetKevent(&ch[0], fd, unix.EVFILT_READ, unix.EV_DELETE)
	unix.SetKevent(&ch[1], fd, unix.EVFILT_WRITE, unix.EV_DELETE)
	return p.apply(ch[:]...)
}

func (p *Poller) apply(ch ...unix.Kevent_t) error {
	_, err := unix.Kevent(p.kq, ch, nil, nil)
	return os.NewSyscallError("kevent", err)
}

// Wait blocks until events arrive, Wake is called or timeout passes
// (timeout < 0 waits forever). It fills events and returns how many.
func (p *Poller) Wait(events []Event, timeout time.Duration) (int, error) {
	if cap(p.buf) < len(events) {
		p.buf = make([]unix.Kevent_t, len(events))
	}
	var ts *unix.Timespec
	if timeout >= 0 {
		t := unix.NsecToTimespec(timeout.Nanoseconds())
		ts = &t
	}
	n, err := unix.Kevent(p.kq, nil, p.buf[:len(events)], ts)
	if errors.Is(err, unix.EINTR) {
		return 0, nil
	}
	if err != nil {
		return 0, os.NewSyscallError("kevent wait", err)
	}

	out := 0
	for _, e := range p.buf[:n] {
		if e.Filter == unix.EVFILT_USER {
			continue
		}
		events[out] = Event{
			FileDescriptor: int(e.Ident), //nolint:gosec // ident is the fd we registered
			Readable:       e.Filter == unix.EVFILT_READ,
			Writable:       e.Filter == unix.EVFILT_WRITE,
			Hangup:         e.Flags&(unix.EV_EOF|unix.EV_ERROR) != 0,
		}
		out++
	}
	return out, nil
}

// Wake makes a blocked Wait return. Safe to call from any goroutine.
func (p *Poller) Wake() error {
	return p.apply(unix.Kevent_t{Ident: wakeIdent, Filter: unix.EVFILT_USER, Fflags: unix.NOTE_TRIGGER})
}

// Close releases the poller.
func (p *Poller) Close() error {
	return unix.Close(p.kq)
}
