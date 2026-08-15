// Command gobnc-keeper is the real keeper process: it owns uplink
// TCP/TLS sockets so they survive a gobnc (brain) restart. See
// docs/keeper-design.md for the split this completes. Deliberately thin —
// all the actual behavior lives in internal/keeper; this just wires
// Manager + Listener to a socket path and a signal handler.
//
// It is not daemonized by this binary itself: a caller that wants it
// running detached (internal/keeperboot, when no keeper is already
// listening) is responsible for Setsid/redirected stdio/its own spawn
// bookkeeping. Run directly, gobnc-keeper just stays in the foreground of
// whatever process context it's given.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/daemon"
	"github.com/MasterBOFH/GoBNC/internal/keeper"
	gobnclog "github.com/MasterBOFH/GoBNC/internal/log"
	"github.com/MasterBOFH/GoBNC/internal/version"
)

func main() {
	defaultSocket := filepath.Join(config.DefaultStateDir(), "keeper.sock")
	defaultPidFile := filepath.Join(config.DefaultStateDir(), "keeper.pid")

	sockPath := flag.String("socket", defaultSocket, "unix socket path to serve the keeper<->brain protocol on")
	pidFile := flag.String("pidfile", defaultPidFile, "PID file path")
	ringCapacity := flag.Int("ring-capacity", 8192, "per-network ring buffer capacity, in lines")
	maxLine := flag.Int("max-line", 8192, "max accepted line length in bytes")
	logFile := flag.String("log-file", "", "write logs here instead of stderr")
	debug := flag.Bool("debug", false, "debug-level logging, including keeper<->brain control frames (metadata only, never line/blob content)")
	flag.BoolVar(debug, "d", false, "short for -debug")
	quitMessage := flag.String("quit-message", "", "QUIT reason sent to every connected network on shutdown (default: version.QuitMessage())")
	quitTimeout := flag.Duration("quit-timeout", 5*time.Second, "per-network bound on the shutdown QUIT write")
	quitOverallTimeout := flag.Duration("quit-overall-timeout", 10*time.Second, "overall bound on shutdown across every network")
	flag.Parse()

	logLevel := "info"
	if *debug {
		logLevel = "debug"
	}
	// Matches this binary's prior exclusive-redirect behavior: -log-file
	// replaces stderr output, it doesn't add to it (unlike cmd/gobnc, whose
	// own Setup call always keeps a console handler too) — this binary
	// isn't daemonized by itself (see the package doc), so someone running
	// it directly with -log-file set still expects a quiet terminal.
	logConsole := io.Writer(os.Stderr)
	if *logFile != "" {
		logConsole = io.Discard
	}
	logOpts := gobnclog.Options{Level: logLevel, Console: logConsole, File: *logFile}
	logger, logSink, err := gobnclog.Setup(logOpts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gobnc-keeper: log: %v\n", err)
		os.Exit(2)
	}
	defer func() { _ = logSink.Close() }()

	if err := daemon.WritePidFile(*pidFile, os.Getpid()); err != nil {
		logger.Error("write pidfile", "err", err)
		os.Exit(1)
	}
	defer daemon.RemovePidFile(*pidFile)

	reason := *quitMessage
	if reason == "" {
		reason = version.QuitMessage()
	}

	mgr := keeper.NewManager(*maxLine, *ringCapacity, logger)
	l := keeper.NewListener(mgr, logger)

	serveCtx, serveCancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- l.Serve(serveCtx, *sockPath) }()

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Log destination reload — the one keeper-process-level config value
	// meant to be reloadable without a restart (see docs/keeper-design.md's
	// Keeper struct doc comment: a process designed to never need
	// restarting cannot ship with an unrotatable log). Reuses
	// internal/log's Sink/Reload exactly as cmd/gobnc does, not a second
	// mechanism: reopens -log-file at the same path (the logrotate case —
	// there is no other config here to change without restarting the
	// process, since every other flag is only ever read once at startup).
	// Reload swaps the same *slog.Logger's underlying handler in place, so
	// nothing holding logger (Manager, Listener, every Keeper instance)
	// needs to be told about this.
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	go func() {
		for {
			select {
			case <-sigCtx.Done():
				return
			case <-hup:
				logger.Info("SIGHUP received; reloading log destination")
				if err := logSink.Reload(logOpts); err != nil {
					logger.Error("log reload failed", "err", err)
				}
			}
		}
	}()

	logger.Info("gobnc-keeper started", "socket", *sockPath, "pid", os.Getpid())

	select {
	case <-sigCtx.Done():
		logger.Info("signal received, disconnecting every network before exit", "reason", reason)
		mgr.QuitCloseAll(reason, *quitTimeout, *quitOverallTimeout)
		serveCancel()
		<-serveDone
	case err := <-serveDone:
		if err != nil && serveCtx.Err() == nil {
			logger.Error("listener exited unexpectedly", "err", err)
			os.Exit(1)
		}
	}
	logger.Info("gobnc-keeper stopped")
}
