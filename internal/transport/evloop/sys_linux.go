//go:build linux

package evloop

import "golang.org/x/sys/unix"

// Linux spreads new connections over sockets bound with SO_REUSEPORT,
// so every loop gets its own listening socket.
const reusePort = true

func newSocket(family int) (int, error) {
	return unix.Socket(family, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
}

func accept(fd int) (int, unix.Sockaddr, error) {
	return unix.Accept4(fd, unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC)
}
