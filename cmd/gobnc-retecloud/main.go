// Command gobnc-retecloud is the GoBNC module serving the ReteCloud WebSocket protocol in lite
// mode, so Rete on iOS can use the bouncer directly. The brain starts it (gobnc.json "modules");
// see docs/module-api-design.md.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/MasterBOFH/GoBNC/modules/retecloud"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil && ctx.Err() == nil {
		fmt.Fprintln(os.Stderr, "gobnc-retecloud:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	cfg, err := retecloud.ParseConfig(moduleapi.Config())
	if err != nil {
		return err
	}
	brain, err := moduleapi.Dial(ctx, moduleapi.Options{Features: retecloud.Features})
	if err != nil {
		return err
	}
	defer brain.Close()
	m := retecloud.New(cfg, brain, log)
	l, err := retecloud.NewListener(cfg, log, m.Accept)
	if err != nil {
		return err
	}
	errc := make(chan error, 2)
	go func() { errc <- m.Run(ctx) }()
	go func() { errc <- l.Serve(ctx) }()
	return <-errc
}
