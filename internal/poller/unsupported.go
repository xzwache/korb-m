//go:build !linux && !darwin

package poller

import "time"

// Poller is not available on this platform.
type Poller struct{}

// New returns ErrUnsupported.
func New() (*Poller, error) { return nil, ErrUnsupported }

func (*Poller) Add(int, Interest) error                  { return ErrUnsupported }
func (*Poller) Modify(int, Interest) error               { return ErrUnsupported }
func (*Poller) Delete(int) error                         { return ErrUnsupported }
func (*Poller) Wait([]Event, time.Duration) (int, error) { return 0, ErrUnsupported }
func (*Poller) Wake() error                              { return ErrUnsupported }
func (*Poller) Close() error                             { return nil }
