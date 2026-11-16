package main

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/xzwache/korb-m/internal/app/echo"
	"github.com/xzwache/korb-m/internal/transport/gopc"
)

func TestPercentile(t *testing.T) {
	var s stats
	for range 90 {
		s.observe(100 * time.Microsecond) // bucket upper bound 128µs
	}
	for range 10 {
		s.observe(3 * time.Millisecond) // bucket upper bound 4.096ms
	}
	if got := s.percentile(0.50); got != 128*time.Microsecond {
		t.Errorf("p50 = %v, want 128µs", got)
	}
	if got := s.percentile(0.95); got != 4096*time.Microsecond {
		t.Errorf("p95 = %v, want 4.096ms", got)
	}
}

func TestParseFlagsSources(t *testing.T) {
	cfg, err := parseFlags([]string{"-sources", "127.0.0.2, 127.0.0.3"}, io.Discard)
	if err != nil || len(cfg.sources) != 2 {
		t.Fatalf("got %v, %v", cfg.sources, err)
	}
	if _, err := parseFlags([]string{"-sources", "nope"}, io.Discard); err == nil {
		t.Error("expected error for bad IP")
	}
}

// TestAgainstServer runs loadgen against a real echo server.
func TestAgainstServer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := gopc.New(echo.New(echo.ModeEcho, nil), gopc.Config{})
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Shutdown(context.Background()) }()

	var out strings.Builder
	args := []string{"-addr", ln.Addr().String(), "-conns", "20", "-rate", "1000",
		"-interval", "50ms", "-duration", "1s", "-report", "10s"}
	if err := run(context.Background(), args, &out, io.Discard); err != nil {
		t.Fatal(err)
	}
	final := out.String()
	if !strings.Contains(final, "failed=0") || strings.Contains(final, "recv/s=0 ") {
		t.Fatalf("unexpected result: %s", final)
	}
}
