//go:build linux || darwin

package poller

import (
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func newPoller(t *testing.T) *Poller {
	t.Helper()
	p, err := New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// socketpair returns two connected non-blocking sockets.
func socketpair(t *testing.T) (a, b int) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, fd := range fds {
		if err := unix.SetNonblock(fd, true); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _ = unix.Close(fds[0]); _ = unix.Close(fds[1]) })
	return fds[0], fds[1]
}

// wait collects events for fd, merging kqueue's separate read/write events.
func wait(t *testing.T, p *Poller, fd int, timeout time.Duration) (Event, bool) {
	t.Helper()
	events := make([]Event, 8)
	n, err := p.Wait(events, timeout)
	if err != nil {
		t.Fatal(err)
	}
	var got Event
	found := false
	for _, e := range events[:n] {
		if e.FileDescriptor == fd {
			found = true
			got.FileDescriptor = fd
			got.Readable = got.Readable || e.Readable
			got.Writable = got.Writable || e.Writable
			got.Hangup = got.Hangup || e.Hangup
		}
	}
	return got, found
}

func write(t *testing.T, fd int, s string) {
	t.Helper()
	if _, err := unix.Write(fd, []byte(s)); err != nil {
		t.Fatal(err)
	}
}

func TestReadable(t *testing.T) {
	p := newPoller(t)
	a, b := socketpair(t)
	if err := p.Add(a, Read); err != nil {
		t.Fatal(err)
	}

	if _, ok := wait(t, p, a, 20*time.Millisecond); ok {
		t.Fatal("event before any data")
	}
	write(t, b, "hello")
	if ev, ok := wait(t, p, a, time.Second); !ok || !ev.Readable {
		t.Fatalf("got %+v, want readable", ev)
	}
}

func TestLevelTriggered(t *testing.T) {
	p := newPoller(t)
	a, b := socketpair(t)
	if err := p.Add(a, Read); err != nil {
		t.Fatal(err)
	}
	write(t, b, "hello")

	// Read only part of the data: the next Wait must report it again.
	buf := make([]byte, 2)
	for range 2 {
		if ev, ok := wait(t, p, a, time.Second); !ok || !ev.Readable {
			t.Fatalf("got %+v, want readable", ev)
		}
		if _, err := unix.Read(a, buf); err != nil {
			t.Fatal(err)
		}
	}
}

func TestWriteInterest(t *testing.T) {
	p := newPoller(t)
	a, _ := socketpair(t)
	if err := p.Add(a, Read|Write); err != nil {
		t.Fatal(err)
	}
	if ev, ok := wait(t, p, a, time.Second); !ok || !ev.Writable {
		t.Fatalf("got %+v, want writable (empty send buffer)", ev)
	}

	if err := p.Modify(a, Read); err != nil {
		t.Fatal(err)
	}
	if ev, ok := wait(t, p, a, 20*time.Millisecond); ok {
		t.Fatalf("got %+v after removing write interest", ev)
	}
}

func TestHangup(t *testing.T) {
	p := newPoller(t)
	a, b := socketpair(t)
	if err := p.Add(a, Read); err != nil {
		t.Fatal(err)
	}
	_ = unix.Close(b)
	if ev, ok := wait(t, p, a, time.Second); !ok || !ev.Hangup {
		t.Fatalf("got %+v, want hangup", ev)
	}
}

func TestDelete(t *testing.T) {
	p := newPoller(t)
	a, b := socketpair(t)
	if err := p.Add(a, Read); err != nil {
		t.Fatal(err)
	}
	if err := p.Delete(a); err != nil {
		t.Fatal(err)
	}
	write(t, b, "x")
	if ev, ok := wait(t, p, a, 20*time.Millisecond); ok {
		t.Fatalf("got %+v after Delete", ev)
	}
}

func TestWake(t *testing.T) {
	p := newPoller(t)
	go func() {
		time.Sleep(20 * time.Millisecond)
		_ = p.Wake()
	}()

	start := time.Now()
	n, err := p.Wait(make([]Event, 8), -1) // would block forever without Wake
	if err != nil || n != 0 {
		t.Fatalf("n=%d err=%v, want 0 events", n, err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("Wake did not interrupt Wait")
	}

	// A consumed wake must not fire again.
	if n, _ := p.Wait(make([]Event, 8), 20*time.Millisecond); n != 0 {
		t.Fatalf("got %d events after wake was consumed", n)
	}
}

func TestTimeout(t *testing.T) {
	p := newPoller(t)
	start := time.Now()
	if n, err := p.Wait(make([]Event, 8), 30*time.Millisecond); n != 0 || err != nil {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if d := time.Since(start); d < 25*time.Millisecond {
		t.Fatalf("returned after %v, want ~30ms", d)
	}
}
