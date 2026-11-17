//go:build linux || darwin

package evloop

import (
	"fmt"
	"net"
	"os"

	"golang.org/x/sys/unix"
)

const listenBacklog = 65535 // the kernel caps it at net.core.somaxconn

// listenSocket returns a non-blocking listening socket bound to addr.
func listenSocket(addr *net.TCPAddr) (int, error) {
	sa, family, err := toSockaddr(addr)
	if err != nil {
		return -1, err
	}
	fd, err := newSocket(family)
	if err != nil {
		return -1, os.NewSyscallError("socket", err)
	}
	if err := setupListener(fd, sa); err != nil {
		_ = unix.Close(fd)
		return -1, err
	}
	return fd, nil
}

func setupListener(fd int, sa unix.Sockaddr) error {
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		return os.NewSyscallError("setsockopt SO_REUSEADDR", err)
	}
	if reusePort {
		if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1); err != nil {
			return os.NewSyscallError("setsockopt SO_REUSEPORT", err)
		}
	}
	if err := unix.Bind(fd, sa); err != nil {
		return os.NewSyscallError("bind", err)
	}
	return os.NewSyscallError("listen", unix.Listen(fd, listenBacklog))
}

func localAddr(fd int) (*net.TCPAddr, error) {
	sa, err := unix.Getsockname(fd)
	if err != nil {
		return nil, os.NewSyscallError("getsockname", err)
	}
	return fromSockaddr(sa), nil
}

// toSockaddr converts addr. An empty host (":8080") listens on all IPv4
// addresses.
func toSockaddr(addr *net.TCPAddr) (unix.Sockaddr, int, error) {
	if addr.IP == nil || addr.IP.To4() != nil {
		sa := &unix.SockaddrInet4{Port: addr.Port}
		if addr.IP != nil {
			copy(sa.Addr[:], addr.IP.To4())
		}
		return sa, unix.AF_INET, nil
	}
	if ip := addr.IP.To16(); ip != nil {
		sa := &unix.SockaddrInet6{Port: addr.Port}
		copy(sa.Addr[:], ip)
		return sa, unix.AF_INET6, nil
	}
	return nil, 0, fmt.Errorf("evloop: bad address %v", addr)
}

func fromSockaddr(sa unix.Sockaddr) *net.TCPAddr {
	switch sa := sa.(type) {
	case *unix.SockaddrInet4:
		return &net.TCPAddr{IP: net.IP(sa.Addr[:]).To16(), Port: sa.Port}
	case *unix.SockaddrInet6:
		return &net.TCPAddr{IP: net.IP(sa.Addr[:]), Port: sa.Port}
	}
	return &net.TCPAddr{}
}
