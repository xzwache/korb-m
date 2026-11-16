// Command loadgen opens many WebSocket connections to a korb-m server and
// reports connection counts, message rates and latency.
//
// One source IP can open roughly as many connections to one server address
// as there are ephemeral ports (~28K on Linux by default). Pass several
// -sources (e.g. 127.0.0.2,127.0.0.3 on Linux) to go beyond that.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

type config struct {
	addr, host, path string
	conns, rate      int
	interval         time.Duration
	size             int
	duration         time.Duration
	report           time.Duration
	sources          []net.IP
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func parseFlags(args []string, stderr io.Writer) (config, error) {
	var c config
	var sources string
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&c.addr, "addr", "127.0.0.1:8080", "server address")
	fs.StringVar(&c.path, "path", "/ws", "request path")
	fs.IntVar(&c.conns, "conns", 1000, "number of connections")
	fs.IntVar(&c.rate, "rate", 1000, "new connections per second")
	fs.DurationVar(&c.interval, "interval", 0, "per-connection send interval; 0 = idle connections")
	fs.IntVar(&c.size, "size", 64, "message size in bytes (min 8, holds the timestamp)")
	fs.DurationVar(&c.duration, "duration", 0, "stop after this long; 0 = until Ctrl+C")
	fs.DurationVar(&c.report, "report", 2*time.Second, "report interval")
	fs.StringVar(&sources, "sources", "", "comma-separated local IPs to dial from")
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	if c.conns <= 0 || c.rate <= 0 {
		return c, errors.New("-conns and -rate must be positive")
	}
	c.host = c.addr
	for s := range strings.SplitSeq(sources, ",") {
		if s = strings.TrimSpace(s); s == "" {
			continue
		}
		ip := net.ParseIP(s)
		if ip == nil {
			return c, fmt.Errorf("bad source IP %q", s)
		}
		c.sources = append(c.sources, ip)
	}
	return c, nil
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	cfg, err := parseFlags(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	if cfg.duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.duration)
		defer cancel()
	}

	st := &stats{}
	var wg sync.WaitGroup
	wg.Go(func() { spawn(ctx, &cfg, st, &wg) })

	start := time.Now()
	last := start
	prev := st.snapshot()
	tick := time.NewTicker(cfg.report)
	defer tick.Stop()
	for {
		select {
		case now := <-tick.C:
			var line string
			line, prev = st.report(prev, now.Sub(last))
			last = now
			_, _ = fmt.Fprintf(stdout, "%6.0fs %s\n", now.Sub(start).Seconds(), line)
		case <-ctx.Done():
			wg.Wait()
			line, _ := st.report(prev, time.Since(last))
			_, _ = fmt.Fprintf(stdout, "final  %s\n", line)
			return nil
		}
	}
}

// spawn starts cfg.conns connections at cfg.rate per second.
func spawn(ctx context.Context, cfg *config, st *stats, wg *sync.WaitGroup) {
	tick := time.NewTicker(time.Second / time.Duration(min(cfg.rate, int(time.Second))))
	defer tick.Stop()
	for i := range cfg.conns {
		var source net.IP
		if len(cfg.sources) > 0 {
			source = cfg.sources[i%len(cfg.sources)]
		}
		wg.Go(func() { runConn(ctx, cfg, st, source) })
		select {
		case <-tick.C:
		case <-ctx.Done():
			return
		}
	}
}
