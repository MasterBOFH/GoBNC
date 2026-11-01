// Command gobnc-retecloud is the GoBNC module serving the ReteCloud WebSocket protocol in lite
// mode, so Rete on iOS can use the bouncer directly. The brain starts it (gobnc.json "modules");
// see docs/module-api-design.md.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/MasterBOFH/GoBNC/modules/retecloud"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
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
	ln, handoff, err := listen(cfg.ListenAddr)
	if err != nil {
		return err
	}
	brain, err := moduleapi.Dial(ctx, moduleapi.Options{Features: retecloud.Features, Handoff: true})
	if err != nil {
		ln.Close()
		return err
	}
	m := retecloud.New(cfg, brain, log)
	l, err := retecloud.NewListener(cfg, log, m.Accept)
	if err != nil {
		ln.Close()
		brain.Close()
		return err
	}
	lctx, stopListening := context.WithCancel(ctx)
	defer stopListening()
	errc := make(chan error, 2)
	serve := func() {
		go func() {
			err := l.ServeListener(lctx, ln)
			if ctx.Err() == nil && lctx.Err() != nil {
				return // stopped by a handoff: Run reports how it ends
			}
			errc <- err
		}()
	}
	if handoff {
		// The previous process keeps serving until this one confirms: accept only after that.
		m.OnServing(func() {
			if err := brain.ConfirmHandoff(); err != nil {
				errc <- err
				return
			}
			log.Info("took over from the previous process")
			serve()
		})
	} else {
		serve()
	}
	m.OnReload(func(ev wire.ReloadEvent) error {
		f, err := ln.(*net.TCPListener).File()
		if err != nil {
			return err
		}
		pid, err := brain.Handoff(ctx, ev, []moduleapi.HandoffFile{{Name: "wss", File: f}})
		f.Close()
		if err != nil {
			return err
		}
		log.Info("replacement confirmed; no longer accepting", "pid", pid)
		stopListening() // hijacked WebSockets are untouched
		return nil
	})
	go func() { errc <- m.Run(ctx) }()
	err = <-errc
	if errors.Is(err, retecloud.ErrHandedOff) {
		brain.Release() // the replacement shares the module lock: never unlock it
		return nil
	}
	brain.Close()
	return err
}

// listen returns the WSS listener: the one a previous process handed over (handoff true), or a
// new one on addr. An inherited listener on another address (listen_addr changed) is replaced.
func listen(addr string) (ln net.Listener, handoff bool, err error) {
	ln, err = moduleapi.InheritedListener("wss")
	if err != nil {
		return nil, false, err
	}
	handoff = moduleapi.Inherited("confirm") != nil
	if ln != nil {
		want, rerr := net.ResolveTCPAddr("tcp", addr)
		if rerr == nil && sameAddr(ln.Addr(), want) {
			return ln, handoff, nil
		}
		ln.Close()
	}
	ln, err = net.Listen("tcp", addr)
	return ln, handoff, err
}

func sameAddr(a net.Addr, b *net.TCPAddr) bool {
	ta, ok := a.(*net.TCPAddr)
	return ok && ta.Port == b.Port && (ta.IP.Equal(b.IP) || (ta.IP.IsUnspecified() && b.IP == nil) ||
		(ta.IP.IsUnspecified() && b.IP.IsUnspecified()))
}
