package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/MasterBOFH/GoBNC/internal/config"
	gobnclog "github.com/MasterBOFH/GoBNC/internal/log"
	"github.com/MasterBOFH/GoBNC/internal/server"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "serve":
			os.Args = append([]string{os.Args[0]}, os.Args[2:]...)
			runServe()
			return
		case "network", "auth", "help", "-h", "--help":
			if err := runCLI(os.Args[1:]); err != nil {
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
	flag.Parse()

	cfg, err := config.LoadJSON(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}

	logger, closeLog, err := gobnclog.Setup(gobnclog.Options{
		Level:   cfg.LogLevel,
		Console: os.Stderr,
		File:    cfg.LogFile,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "log: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = closeLog() }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv, err := server.New(cfg, logger)
	if err != nil {
		logger.Error("server init", "err", err)
		os.Exit(1)
	}
	defer srv.Close()

	logger.Info("gobnc starting", "listen", cfg.ListenAddr, "db", cfg.DBPath, "log_file", cfg.LogFile)
	if err := srv.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Error("server stopped", "err", err)
		os.Exit(1)
	}
	logger.Info("gobnc stopped")
}
