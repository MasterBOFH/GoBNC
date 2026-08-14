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
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/daemon"
	"github.com/MasterBOFH/GoBNC/internal/keeper"
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
	quitMessage := flag.String("quit-message", "", "QUIT reason sent to every connected network on shutdown (default: version.QuitMessage())")
	quitTimeout := flag.Duration("quit-timeout", 5*time.Second, "per-network bound on the shutdown QUIT write")
	quitOverallTimeout := flag.Duration("quit-overall-timeout", 10*time.Second, "overall bound on shutdown across every network")
	flag.Parse()

	logOut := io.Writer(os.Stderr)
	if *logFile != "" {
		f, err := os.OpenFile(*logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			fmt.Fprintf(os.Stderr, "gobnc-keeper: log-file: %v\n", err)
			os.Exit(2)
		}
		defer f.Close()
		logOut = f
	}
	logger := slog.New(slog.NewTextHandler(logOut, nil))

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
