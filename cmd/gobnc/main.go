package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/control"
	"github.com/MasterBOFH/GoBNC/internal/daemon"
	"github.com/MasterBOFH/GoBNC/internal/keeper"
	gobnclog "github.com/MasterBOFH/GoBNC/internal/log"
	"github.com/MasterBOFH/GoBNC/internal/server"
	"github.com/MasterBOFH/GoBNC/internal/version"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "serve":
			os.Args = append([]string{os.Args[0]}, os.Args[2:]...)
			runServe()
			return
		case "network", "auth", "rehash", "reload", "die", "stop", "status", "can-upgrade", "help", "-h", "--help":
			if err := runCLI(os.Args[1:]); err != nil {
				if errors.Is(err, errMustUpgrade) {
					os.Exit(2)
				}
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				os.Exit(1)
			}
			return
		}
	}
	runServe()
}

func runServe() {
	// Resolved once, now, rather than at reload time: os.Executable() asks
	// the OS to resolve the file *this process* exec'd from, and on
	// FreeBSD that resolution can fail outright (ENOENT) once the on-disk
	// binary has since been replaced by a rebuild — confirmed live
	// ("reload spawn failed","err":"executable: no such file or
	// directory"). The path string itself stays valid for exec.Command
	// regardless of what's since been rebuilt at that path; it's only the
	// OS-level lookup of "what did I start from" that degrades over a long
	// process lifetime, so do that lookup once, early, while it's certain
	// to still succeed, and reuse the string later.
	exe, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "executable: %v\n", err)
		os.Exit(1)
	}
	cfgPath := flag.String("config", "gobnc.json", "path to bootstrap JSON config")
	foreground := flag.Bool("foreground", false, "run in the foreground (no fork; for systemd/rc.d)")
	flag.BoolVar(foreground, "f", false, "short for -foreground")
	debug := flag.Bool("debug", false, "foreground + debug console (file keeps config log_level)")
	flag.BoolVar(debug, "d", false, "short for -debug")
	reloadHandoff := flag.String("reload-handoff", "", "internal: set by daemon.SpawnReplacementForHandoff on a reload-spawned replacement; the control socket path to confirm handoff with the predecessor over before touching the keeper or the pidfile")
	flag.Parse()

	fg := *foreground || *debug

	cfg, err := config.LoadJSON(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}

	pidPath := cfg.ResolvedPidFile()
	isChild := os.Getenv(daemon.EnvChild) != ""
	if daemon.ShouldDaemonize(fg) {
		if err := daemon.Reborn(daemon.Options{
			PidFile: pidPath,
			Args:    []string{os.Args[0], "serve", "-config", *cfgPath, "-foreground"},
		}); err != nil {
			fmt.Fprintf(os.Stderr, "daemonize: %v\n", err)
			os.Exit(1)
		}
		// Parent exits inside Reborn; child continues with EnvChild set.
	}

	// A reload-handoff child must not claim the pidfile (or, below, attach
	// to the keeper) until it has actually confirmed the handoff with its
	// predecessor — see runReloadHandoff's own doc comment on why the
	// pidfile has to keep naming the still-running old brain until then.
	// Both happen further down, once confirmReloadHandoff succeeds.
	if *reloadHandoff == "" {
		if err := daemon.WritePidFile(pidPath, os.Getpid()); err != nil {
			fmt.Fprintf(os.Stderr, "pid file: %v\n", err)
			os.Exit(1)
		}
		defer daemon.RemovePidFile(pidPath)
	}

	// Daemon children (no TTY) default logs to the state dir when log_file is unset.
	logFile := cfg.ResolvedLogFile(isChild)
	consoleLevel := cfg.LogLevel
	if *debug {
		consoleLevel = "debug"
	}
	logger, logSink, err := gobnclog.Setup(gobnclog.Options{
		Level:     consoleLevel,
		FileLevel: cfg.LogLevel,
		Console:   os.Stderr,
		File:      logFile,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "log: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = logSink.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv, err := server.New(cfg, logger)
	if err != nil {
		logger.Error("server init", "err", err)
		os.Exit(1)
	}
	defer srv.Close()
	srv.SetConfigPath(*cfgPath)
	srv.SetLogSink(logSink, os.Stderr, *debug, isChild)
	srv.SetReloadConfig(exe)

	if *reloadHandoff != "" {
		if err := confirmReloadHandoff(ctx, *reloadHandoff); err != nil {
			logger.Error("reload handoff failed", "err", err)
			os.Exit(1)
		}
		srv.SetReloadHandoffChild(true)
		if err := daemon.WritePidFile(pidPath, os.Getpid()); err != nil {
			logger.Error("pid file", "err", err)
			os.Exit(1)
		}
		defer daemon.RemovePidFile(pidPath)
		logger.Info("reload handoff confirmed")
	}

	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-hup:
				logger.Info("SIGHUP received; rehashing")
				if err := srv.Rehash(*cfgPath); err != nil {
					logger.Error("rehash failed", "err", err)
				}
			}
		}
	}()

	logger.Info("gobnc starting",
		"version", version.DisplayVersion(),
		"brain_version", version.BrainVersion,
		"keeper_version", version.KeeperVersion,
		"listen", cfg.ListenAddr,
		"db", cfg.DBPath,
		"log_file", logFile,
		"pid_file", pidPath,
		"daemon", isChild,
	)
	if err := srv.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Error("server stopped", "err", err)
		os.Exit(1)
	}
	if srv.WantReload() {
		// The replacement was already spawned, and confirmed it took over,
		// inside runReloadHandoff — nothing left to do here. See that
		// function's own doc comment for why this is no longer the place
		// that spawns anything (the old unconditional-detach-then-spawn
		// order here was the incident this whole change exists to fix).
		logger.Info("reload handoff already complete")
		return
	}
	if srv.WantDie() {
		if err := stopKeeperProcess(); err != nil {
			logger.Error("die: stop keeper", "err", err)
		} else {
			logger.Info("keeper stopped")
		}
	}
	logger.Info("gobnc stopped")
}

// confirmReloadHandoff is the child half of internal/server's
// runReloadHandoff: probe the keeper in validate mode (internal/keeper's
// Probe — the same primitive cmdCanUpgrade uses; never touches the live
// slot) to confirm it's reachable and version-compatible without needing
// the predecessor to have released anything yet, then ask the predecessor
// over handoffSock (its own control socket, passed via -reload-handoff)
// to actually detach. That request blocks until the predecessor replies —
// see runReloadHandoff's own doc comment — so a plain single-shot
// control.Client call is enough here; there is nothing to stream.
func confirmReloadHandoff(ctx context.Context, handoffSock string) error {
	keeperSock := filepath.Join(config.DefaultStateDir(), "keeper.sock")
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := keeper.Probe(pctx, keeperSock); err != nil {
		return fmt.Errorf("probe keeper: %w", err)
	}
	resp, err := control.Client(handoffSock, control.CmdReloadHandoff)
	if err != nil {
		return fmt.Errorf("handoff request: %w", err)
	}
	if resp != "OK" {
		return fmt.Errorf("handoff rejected: %s", resp)
	}
	return nil
}
