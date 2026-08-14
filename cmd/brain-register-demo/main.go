// Command brain-register-demo is the runnable, cross-process proof
// internal/brain/driver.go's package doc already pointed at: it finds or
// starts a real gobnc-keeper process (internal/keeperboot), attaches to
// it over the real wire protocol, dials one real network, and drives a
// real registration through internal/registration — end to end, with
// nothing running in the same process except this thin CLI. See
// docs/keeper-design.md's process-orchestration section.
//
// The point isn't just registering — it's what this program does NOT do
// on exit. SIGINT/SIGTERM here just stop this process; nothing calls
// Driver.QuitNetwork. Run this, kill it, and run it again: the second run
// attaches to the SAME keeper (no new one spawned — see its "attached to
// existing keeper" log line) and the network comes back already
// Connected, because the keeper never went anywhere. That's the actual
// survives-a-restart proof this whole project exists to produce.
//
// Also exercises the rest of stage 2a: -channels auto-joins on
// registration complete, every post-registration line is logged from
// Driver.Lines(), and SIGUSR1 forces a Driver.Reconnect (close + redial +
// fresh registration on the same tracked network) on demand.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/brain"
	"github.com/MasterBOFH/GoBNC/internal/keeper"
	"github.com/MasterBOFH/GoBNC/internal/keeperboot"
	"github.com/MasterBOFH/GoBNC/internal/registration"
)

// netID is the only network this demo drives — one brain, one network is
// enough to prove the cross-process claim; nothing about it depends on
// multi-network support, which the keeper already provides regardless.
const netID keeper.NetworkID = 1

func main() {
	host := flag.String("host", "", "uplink host (required)")
	port := flag.Int("port", 6667, "uplink port")
	useTLS := flag.Bool("tls", false, "use TLS")
	tlsNoVerify := flag.Bool("tls-no-verify", false, "skip TLS certificate verification")
	nick := flag.String("nick", "gobncbrain", "nick to register")
	channelsFlag := flag.String("channels", "", "comma-separated channels to auto-join, e.g. #foo,#bar:key")
	keeperSocket := flag.String("keeper-socket", "", "keeper socket path (empty: keeperboot default)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	if *host == "" {
		fmt.Fprintln(os.Stderr, "-host is required")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	bootCtx, bootCancel := context.WithTimeout(ctx, 30*time.Second)
	res, err := keeperboot.EnsureRunning(bootCtx, keeperboot.Options{
		SocketPath: *keeperSocket,
		Hello:      keeper.HelloMsg{Mode: keeper.ModeLive},
	})
	bootCancel()
	if err != nil {
		logger.Error("keeperboot.EnsureRunning", "err", err)
		os.Exit(1)
	}
	client := res.Client
	defer client.Close()
	if res.Spawned {
		logger.Info("spawned a new keeper", "pid", res.KeeperPID)
	} else {
		logger.Info("attached to an existing keeper")
	}
	for _, n := range client.Networks {
		logger.Info("keeper already holds", "network", n.ID, "state", n.State, "epoch", n.Epoch, "last_seq", n.LastSeq)
	}

	if err := client.SendLiveReady(); err != nil {
		logger.Error("SendLiveReady", "err", err)
		os.Exit(1)
	}

	driver := brain.NewDriver(client)

	runErr := make(chan error, 1)
	go func() { runErr <- driver.Run(ctx) }()
	go logEvents(logger, driver)
	go logLines(logger, driver)

	// SIGUSR1 forces a deliberate Reconnect on demand — for exercising
	// the redial path without restarting this process, same role
	// cmd/keeper-soak's own SIGUSR1 handler plays for the lower-level
	// Keeper API. Only meaningful once RegisterNetwork has actually been
	// called below (Reconnect errors clearly otherwise) — in the resumed
	// branch this session never registers the network at all, so SIGUSR1
	// there just reports that plainly rather than doing something
	// surprising.
	forceReconnect := make(chan os.Signal, 1)
	signal.Notify(forceReconnect, syscall.SIGUSR1)
	defer signal.Stop(forceReconnect)
	go func() {
		for range forceReconnect {
			logger.Info("SIGUSR1 received, forcing Reconnect")
			if err := driver.Reconnect(netID); err != nil {
				logger.Error("Reconnect", "err", err)
				continue
			}
			if !awaitDialAndRegister(ctx, logger, driver) {
				return
			}
		}
	}()

	// This is the actual survives-a-restart check: if the keeper already
	// has netID connected (a prior run of this same demo dialed it, then
	// exited without ever calling QuitNetwork), there is nothing to dial
	// or register — the uplink never went away. Skip straight to sitting
	// connected. A fresh keeper (or a keeper that never held this
	// network) falls through to the normal dial+register path below.
	//
	// Deliberately does NOT call RegisterNetwork in the resumed case.
	// Found by running this demo live: RegisterNetwork installs a fresh
	// registration.State, and the keeper delivers this network's full
	// retained backlog (all its old lines, replayed) through the exact
	// same live Line events a genuinely new line would arrive on — there
	// is no wire-level distinction between the two. A fresh State fed
	// that backlog steps through 001..376 all over again, reaching
	// PhaseComplete a second time and re-firing auto-join/nick-recovery
	// against a connection that was never actually re-registered. This
	// is the same class of hazard registration.Start's replay guard
	// exists for (see docs/keeper-design.md) — a real resume path needs
	// the same deliberate care, which is blob-store/stage-2b work, not
	// this demo's. The honest behavior in the meantime: a resumed
	// network is watched at the wire level (this process still holds the
	// attach) but isn't re-tracked, so Lines()/recovery/auto-join simply
	// don't apply to it this session — logged plainly, not masked.
	alreadyConnected := false
	for _, n := range client.Networks {
		if n.ID == netID && n.State == keeper.Connected {
			alreadyConnected = true
		}
	}

	if alreadyConnected {
		logger.Info("network already connected on this keeper — resuming, not redialing or re-registering")
	} else {
		driver.RegisterNetwork(netID, brain.NetworkConfig{
			PrimaryNick:  *nick,
			AltNick:      *nick + "_",
			NickRecovery: true,
		})
		if channels := parseChannels(*channelsFlag); len(channels) > 0 {
			driver.SetChannels(netID, channels)
			logger.Info("configured auto-join", "channels", channels)
		}
		if err := driver.Dial(netID, keeper.DialConfig{Host: *host, Port: *port, TLS: *useTLS, TLSNoVerify: *tlsNoVerify}, 0); err != nil {
			logger.Error("Dial", "err", err)
			os.Exit(1)
		}
		if !awaitDialAndRegister(ctx, logger, driver) {
			return
		}
	}

	// Sit connected until signaled. Deliberately no QuitNetwork call
	// anywhere in this function or its shutdown path — see the package
	// doc comment above.
	select {
	case <-ctx.Done():
		logger.Info("signal received, exiting — no QuitNetwork call; the keeper keeps the uplink")
	case err := <-runErr:
		logger.Error("driver.Run exited unexpectedly", "err", err)
	}
}

// awaitDialAndRegister waits for the DialResult and registration Result
// following a Dial or Reconnect call — shared by the initial dial and
// every SIGUSR1-triggered Reconnect. Returns false if ctx was canceled
// first (caller should stop, not treat it as an error).
func awaitDialAndRegister(ctx context.Context, logger *slog.Logger, driver *brain.Driver) bool {
	select {
	case dr := <-driver.DialResults():
		if !dr.OK {
			logger.Error("dial failed", "err", dr.Error)
			os.Exit(1)
		}
		logger.Info("dial ok", "epoch", dr.Epoch)
	case <-ctx.Done():
		logger.Info("signal received before dial completed; exiting — no QuitNetwork call")
		return false
	}

	if err := driver.StartRegistration(netID); err != nil {
		logger.Error("StartRegistration", "err", err)
		os.Exit(1)
	}

	select {
	case res := <-driver.Results():
		if res.State.Phase == registration.PhaseComplete {
			logger.Info("registered", "nick", res.State.Nick)
		} else {
			logger.Error("registration failed", "err", res.State.Err)
		}
	case <-ctx.Done():
		logger.Info("signal received before registration completed; exiting — no QuitNetwork call")
		return false
	}
	return true
}

func parseChannels(flag string) []brain.ChannelJoin {
	var out []brain.ChannelJoin
	for _, tok := range strings.Split(flag, ",") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		name, key, _ := strings.Cut(tok, ":")
		out = append(out, brain.ChannelJoin{Name: name, Key: key})
	}
	return out
}

func logEvents(logger *slog.Logger, driver *brain.Driver) {
	for ev := range driver.NetworkEvents() {
		logger.Info("network event", "kind", ev.Kind, "epoch", ev.Epoch, "err", ev.Error)
	}
}

// logLines proves Driver.Lines() actually relays post-registration
// traffic — every line a network says, forever, not just during
// registration — visibly, in this demo's own log.
func logLines(logger *slog.Logger, driver *brain.Driver) {
	for line := range driver.Lines() {
		logger.Info("line", "network", line.Network, "seq", line.Seq, "raw", string(line.Raw))
	}
}
