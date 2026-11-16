// Command korbd is the korb-m realtime server.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/xzwache/korb-m/internal/app/echo"
	"github.com/xzwache/korb-m/internal/transport/gopc"
	"github.com/xzwache/korb-m/internal/ws"
)

// version is set at build time: -ldflags "-X main.version=v0.1.0"
var version = "dev"

func main() {
	// ctx is cancelled on Ctrl+C or SIGTERM (sent by Docker/Kubernetes).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stdout, os.Stderr, nil)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "korbd:", err)
		os.Exit(1)
	}
}

type config struct {
	addr            string
	debugAddr       string
	mode            string
	allowedOrigins  string
	shutdownTimeout time.Duration
	logLevel        slog.Level
	showVersion     bool
	server          gopc.Config
}

func parseFlags(args []string, stderr io.Writer) (config, error) {
	var c config
	flagSet := flag.NewFlagSet("korbd", flag.ContinueOnError)
	flagSet.SetOutput(stderr)
	flagSet.StringVar(&c.addr, "addr", ":8080", "listen address")
	flagSet.StringVar(&c.debugAddr, "debug-addr", "", "pprof/expvar/stats address, e.g. 127.0.0.1:6060; empty disables. Never expose publicly")
	flagSet.StringVar(&c.mode, "mode", "broadcast", "demo mode: echo or broadcast")
	flagSet.StringVar(&c.allowedOrigins, "allowed-origins", "", "comma-separated allowed Origin values; empty allows all")
	flagSet.DurationVar(&c.shutdownTimeout, "shutdown-timeout", 10*time.Second, "max time to wait for connections on shutdown")
	flagSet.TextVar(&c.logLevel, "log-level", slog.LevelInfo, "debug, info, warn or error")
	flagSet.BoolVar(&c.showVersion, "version", false, "print version and exit")
	flagSet.Int64Var(&c.server.MaxMessageSize, "max-message", 64<<10, "max message size in bytes")
	flagSet.IntVar(&c.server.SendQueueSize, "send-queue", 64, "per-connection send queue length")
	flagSet.DurationVar(&c.server.HandshakeTimeout, "handshake-timeout", 5*time.Second, "handshake deadline")
	flagSet.DurationVar(&c.server.PingInterval, "ping", 30*time.Second, "ping interval")
	flagSet.DurationVar(&c.server.IdleTimeout, "idle", 60*time.Second, "close connections silent for this long")
	flagSet.DurationVar(&c.server.WriteTimeout, "write-timeout", 10*time.Second, "per-write deadline")
	err := flagSet.Parse(args)
	return c, err
}

// run starts the server and blocks until ctx is cancelled. ready, if not
// nil, receives the listen address once the server accepts connections.
func run(ctx context.Context, args []string, stdout, stderr io.Writer, ready func(net.Addr)) error {
	cfg, err := parseFlags(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	if cfg.showVersion {
		_, err := fmt.Fprintln(stdout, version)
		return err
	}
	mode, err := echo.ParseMode(cfg.mode)
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{Level: cfg.logLevel}))
	cfg.server.Logger = logger
	cfg.server.CheckOrigin = originChecker(cfg.allowedOrigins)

	app := echo.New(mode, logger)
	srv := gopc.New(app, cfg.server)
	ln, err := net.Listen("tcp", cfg.addr)
	if err != nil {
		return err
	}

	if cfg.debugAddr != "" {
		dln, err := net.Listen("tcp", cfg.debugAddr)
		if err != nil {
			_ = ln.Close()
			return fmt.Errorf("debug listener: %w", err)
		}
		dsrv := newDebugServer(app.Len)
		go func() { _ = dsrv.Serve(dln) }()
		defer func() { _ = dsrv.Close() }()
		logger.Info("debug server listening", "addr", dln.Addr().String())
	}
	logger.Info("korbd listening", "addr", ln.Addr().String(), "mode", cfg.mode, "version", version)
	if ready != nil {
		ready(ln.Addr())
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()
	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}

	logger.Info("shutting down", "timeout", cfg.shutdownTimeout.String())
	sctx, cancel := context.WithTimeout(context.Background(), cfg.shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	<-serveErr
	logger.Info("bye")
	return nil
}

// originChecker allows the listed origins. Requests without an Origin
// header (non-browser clients) are allowed: only browsers send it, and
// the check exists to stop other websites' pages from connecting.
func originChecker(list string) func(ws.Request) bool {
	if list == "" {
		return nil
	}
	allowed := make(map[string]bool)
	for o := range strings.SplitSeq(list, ",") {
		allowed[strings.TrimSpace(o)] = true
	}
	return func(r ws.Request) bool { return r.Origin == "" || allowed[r.Origin] }
}
