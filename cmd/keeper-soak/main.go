// Command keeper-soak is a minimal, throwaway harness for soaking
// internal/keeper against a real ircd over long unattended runs. It is not
// the brain — it does the bare minimum registration and channel joins
// needed to hold a real, joined connection, and reconnects on drop with a
// fixed short backoff rather than any real policy. Deliberately kept out of
// internal/keeper: this is test infrastructure, not part of the keeper API.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/keeper"
)

func main() {
	host := flag.String("host", "", "uplink host (required)")
	port := flag.Int("port", 6667, "uplink port")
	useTLS := flag.Bool("tls", false, "use TLS")
	tlsNoVerify := flag.Bool("tls-no-verify", false, "skip TLS certificate verification")
	nick := flag.String("nick", "gobnc-soak", "nick to register")
	channels := flag.String("channels", "", "comma-separated channels to join after registration")
	interval := flag.Duration("interval", 5*time.Minute, "stats log interval")
	label := flag.String("label", "soak", "label prefixed on every log line")
	logFile := flag.String("logfile", "", "write logs here with size-capped rotation instead of stdout")
	logMaxBytes := flag.Int64("logmax", 10<<20, "rotate -logfile after it reaches this many bytes (keeps one .1 backup)")
	flag.Parse()

	if *host == "" {
		fmt.Fprintln(os.Stderr, "-host is required")
		os.Exit(2)
	}

	logOut := io.Writer(os.Stdout)
	if *logFile != "" {
		rw, err := newRotatingWriter(*logFile, *logMaxBytes)
		if err != nil {
			fmt.Fprintf(os.Stderr, "logfile: %v\n", err)
			os.Exit(2)
		}
		defer rw.Close()
		logOut = rw
	}
	logger := log.New(logOut, "", log.LstdFlags|log.Lmicroseconds)

	var chans []string
	for _, c := range strings.Split(*channels, ",") {
		c = strings.TrimSpace(c)
		if c != "" {
			chans = append(chans, c)
		}
	}

	k := keeper.New(8192, 8192, nil)
	dialCfg := keeper.DialConfig{
		Host:        *host,
		Port:        *port,
		TLS:         *useTLS,
		TLSNoVerify: *tlsNoVerify,
	}

	connectAndRegister := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := k.Dial(ctx, dialCfg); err != nil {
			return fmt.Errorf("dial: %w", err)
		}
		if err := k.WriteLine("NICK " + *nick); err != nil {
			return fmt.Errorf("write NICK: %w", err)
		}
		if err := k.WriteLine("USER " + *nick + " 0 * :GoBNC keeper soak harness"); err != nil {
			return fmt.Errorf("write USER: %w", err)
		}
		if !waitForWelcome(k, 30*time.Second) {
			if st, _ := k.State(); st != keeper.Connected {
				return fmt.Errorf("dropped mid-registration: %v", k.LastError())
			}
			return fmt.Errorf("no 001 welcome within 30s")
		}
		for _, ch := range chans {
			if err := k.WriteLine("JOIN " + ch); err != nil {
				return fmt.Errorf("join %s: %w", ch, err)
			}
		}
		return nil
	}

	events, _ := k.Subscribe()

	if err := connectAndRegister(); err != nil {
		logger.Fatalf("[%s] initial connect failed: %v", *label, err)
	}
	logger.Printf("[%s] connected and registered as %q, channels=%v", *label, *nick, chans)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// SIGUSR1 forces a deliberate close-and-redial without restarting this
	// process — for exercising the reconnect path on demand (e.g. against
	// a real remote network that might otherwise sit at epoch 1 for days),
	// distinct from the automatic redial on an unexpected drop. Reuses the
	// same redialLoop the EventDisconnected case below does, since Close
	// here publishes exactly the same event that triggers it.
	forceReconnect := make(chan os.Signal, 1)
	signal.Notify(forceReconnect, syscall.SIGUSR1)
	defer signal.Stop(forceReconnect)

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Printf("[%s] signal received, closing", *label)
			_ = k.Close()
			return

		case <-forceReconnect:
			logger.Printf("[%s] SIGUSR1 received, forcing reconnect", *label)
			// A deliberate Close does not publish an EventDisconnected
			// (see Keeper's readLoop: the event only fires when the
			// socket dies on its own) — this case has to kick redialLoop
			// itself rather than relying on the events case below.
			_ = k.Close()
			go redialLoop(ctx, logger, *label, connectAndRegister)

		case ev, ok := <-events:
			if !ok {
				return
			}
			logger.Printf("[%s] event kind=%v epoch=%d err=%v", *label, ev.Kind, ev.Epoch, ev.Err)
			if ev.Kind == keeper.EventDisconnected {
				go redialLoop(ctx, logger, *label, connectAndRegister)
			}

		case <-ticker.C:
			logStats(k, logger, *label)
		}
	}
}

// waitForWelcome polls Since for a 001 numeric. Minimal on purpose — this
// harness stands in for just enough of the brain to hold a joined session,
// not the real registration state machine.
func waitForWelcome(k *keeper.Keeper, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	var after uint64
	for time.Now().Before(deadline) {
		entries, _ := k.Since(after)
		for _, e := range entries {
			after = e.Seq
			if strings.Contains(e.Line, " 001 ") {
				return true
			}
		}
		if st, _ := k.State(); st != keeper.Connected {
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// redialLoop retries connectAndRegister with a fixed short backoff. Not a
// real reconnect policy (no backoff growth, no give-up) — just enough to
// keep an unattended multi-day soak going across transient drops.
func redialLoop(ctx context.Context, logger *log.Logger, label string, connectAndRegister func() error) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
		if err := connectAndRegister(); err != nil {
			logger.Printf("[%s] redial failed: %v; retrying in 10s", label, err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
			}
			continue
		}
		logger.Printf("[%s] redialed and registered", label)
		return
	}
}

func logStats(k *keeper.Keeper, logger *log.Logger, label string) {
	st, epoch := k.State()
	stats := k.Stats()
	var sinceLast string
	if stats.LastLineTime.IsZero() {
		sinceLast = "n/a"
	} else {
		sinceLast = time.Since(stats.LastLineTime).Round(time.Second).String()
	}
	logger.Printf("[%s] state=%v epoch=%d lastErr=%v goroutines=%d ring_occupancy=%d/%d lines_total=%d since_last_line=%s dropped=%d",
		label, st, epoch, k.LastError(), runtime.NumGoroutine(),
		stats.Occupancy, stats.Capacity, stats.LastSeq, sinceLast, stats.Dropped)
}
