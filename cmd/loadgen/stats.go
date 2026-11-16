package main

import (
	"fmt"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"
)

const numBuckets = 32

// stats is updated concurrently by all connections without locks.
type stats struct {
	dialing, connected, failed atomic.Int64
	sent, received             atomic.Int64

	// latency[i] counts samples in [2^(i-1), 2^i) microseconds.
	latency [numBuckets]atomic.Int64

	errMu   sync.Mutex
	lastErr string
}

func (s *stats) fail(err error) {
	s.failed.Add(1)
	s.errMu.Lock()
	s.lastErr = err.Error()
	s.errMu.Unlock()
}

func (s *stats) observe(d time.Duration) {
	us := max(d.Microseconds(), 0)
	b := min(bits.Len64(uint64(us)), numBuckets-1) //nolint:gosec // us >= 0
	s.latency[b].Add(1)
}

// percentile returns the upper bound of the bucket holding the p-th percentile.
func (s *stats) percentile(p float64) time.Duration {
	var counts [numBuckets]int64
	var total int64
	for i := range counts {
		counts[i] = s.latency[i].Load()
		total += counts[i]
	}
	if total == 0 {
		return 0
	}
	target := int64(float64(total) * p)
	var seen int64
	for i, c := range counts {
		seen += c
		if seen > target {
			return time.Duration(1<<i) * time.Microsecond
		}
	}
	return time.Duration(1<<(numBuckets-1)) * time.Microsecond
}

type snapshot struct {
	connected, dialing, failed, sent, received int64
}

func (s *stats) snapshot() snapshot {
	return snapshot{
		connected: s.connected.Load(),
		dialing:   s.dialing.Load(),
		failed:    s.failed.Load(),
		sent:      s.sent.Load(),
		received:  s.received.Load(),
	}
}

func (s *stats) report(prev snapshot, elapsed time.Duration) (string, snapshot) {
	cur := s.snapshot()
	sec := elapsed.Seconds()
	line := fmt.Sprintf("conns=%d dialing=%d failed=%d sent/s=%.0f recv/s=%.0f p50<%v p99<%v",
		cur.connected, cur.dialing, cur.failed,
		float64(cur.sent-prev.sent)/sec, float64(cur.received-prev.received)/sec,
		s.percentile(0.50), s.percentile(0.99))
	s.errMu.Lock()
	if s.lastErr != "" {
		line += fmt.Sprintf(" last_err=%q", s.lastErr)
	}
	s.errMu.Unlock()
	return line, cur
}
