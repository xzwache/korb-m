//go:build darwin

package evloop

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// macOS accepts SO_REUSEPORT but does not balance between sockets, so one
// loop accepts and hands connections to the others round-robin.
const reusePort = false

// macOS has no SOCK_NONBLOCK/SOCK_CLOEXEC flags or accept4. ForkLock stops a
// concurrent fork+exec from inheriting the fd before CloseOnExec is set.
func newSocket(family int) (int, error) {
	syscall.ForkLock.RLock()
	fd, err := unix.Socket(family, unix.SOCK_STREAM, 0)
	if err == nil {
		unix.CloseOnExec(fd)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return -1, err
	}
	return fd, setNonblock(fd)
}

func accept(lfd int) (int, unix.Sockaddr, error) {
	syscall.ForkLock.RLock()
	fd, sa, err := unix.Accept(lfd)
	if err == nil {
		unix.CloseOnExec(fd)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return -1, nil, err
	}
	return fd, sa, setNonblock(fd)
}

func setNonblock(fd int) error {
	if err := unix.SetNonblock(fd, true); err != nil {
		_ = unix.Close(fd)
		return err
	}
	return nil
}
