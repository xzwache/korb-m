// Command korbd is the korb-m realtime server.
//
// Phase 0: this is only a skeleton that wires up logging, flags and
// signal-driven shutdown. Transports are added in later phases.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

// version is overwritten at build time via:
//
//	-ldflags "-X main.version=v0.1.0"
var version = "dev"

func main() {
	if err := run(); err != nil {
		slog.Error("korbd exited with error", "err", err)
		os.Exit(1)
	}
}

// run holds the real logic so it can return errors instead of calling
// os.Exit deep inside the program (deferred cleanups would be skipped).
func run() error {
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return nil
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	// ctx is cancelled on SIGINT (Ctrl+C) or SIGTERM (what Docker and
	// Kubernetes send). Every long-running part of the server will take this
	// ctx, which is how graceful shutdown propagates.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info("korbd starting", "version", version)
	<-ctx.Done()
	slog.Info("korbd shutting down")
	return nil
}
