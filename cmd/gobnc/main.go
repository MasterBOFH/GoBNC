package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/daemon"
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
	cfgPath := flag.String("config", "gobnc.json", "path to bootstrap JSON config")
	foreground := flag.Bool("foreground", false, "run in the foreground (no fork; for systemd/rc.d)")
	flag.BoolVar(foreground, "f", false, "short for -foreground")
	debug := flag.Bool("debug", false, "foreground + debug console (file keeps config log_level)")
	flag.BoolVar(debug, "d", false, "short for -debug")
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

	if err := daemon.WritePidFile(pidPath, os.Getpid()); err != nil {
		fmt.Fprintf(os.Stderr, "pid file: %v\n", err)
		os.Exit(1)
	}
	defer daemon.RemovePidFile(pidPath)

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
		path := srv.ConfigPath()
		if path == "" {
			path = *cfgPath
		}
		inherit := !isChild || srv.DebugConsole()
		pid, err := daemon.SpawnReplacement(path, pidPath, srv.DebugConsole(), inherit)
		if err != nil {
			logger.Error("reload spawn failed", "err", err)
			os.Exit(1)
		}
		logger.Info("spawned replacement brain", "pid", pid)
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
