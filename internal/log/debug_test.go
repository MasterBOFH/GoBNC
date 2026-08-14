package log

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"
)

// fakeDebugTarget records DeliverRaw/DeliverLog calls, safe for concurrent
// use since DebugRegistry.Run delivers from its own goroutine.
type fakeDebugTarget struct {
	mu   sync.Mutex
	raw  []string // "dir line"
	logs []string // "level msg"
}

func (f *fakeDebugTarget) DeliverRaw(dir, line string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.raw = append(f.raw, dir+" "+line)
}

func (f *fakeDebugTarget) DeliverLog(level, msg string, attrs map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logs = append(f.logs, level+" "+msg)
}

func (f *fakeDebugTarget) rawCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.raw)
}

func (f *fakeDebugTarget) logCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.logs)
}

func waitFor(t *testing.T, desc string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", desc)
}

func TestDebugRegistryRoutesRawByPeerNetwork(t *testing.T) {
	var buf bytes.Buffer
	l, sink, err := Setup(Options{Level: "info", Console: &buf})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sink.DebugRegistry().Run(ctx)

	ergo := &fakeDebugTarget{}
	other := &fakeDebugTarget{}
	sink.DebugRegistry().Subscribe("ergo", ergo, DebugRaw)
	sink.DebugRegistry().Subscribe("libera", other, DebugRaw)

	IRC(l, "ergo", ">>", "NICK foo")            // uplink-style peer (bare network name)
	IRC(l, "ergo/c1", "<<", ":ergo.test 001 x") // downlink-style peer (network/clientID)

	waitFor(t, "ergo target sees 2 raw deliveries", func() bool { return ergo.rawCount() == 2 })
	if other.rawCount() != 0 {
		t.Fatalf("libera subscriber got %d raw deliveries, want 0 (cross-network leak)", other.rawCount())
	}
}

func TestDebugRegistryRawModeExcludesLog(t *testing.T) {
	var buf bytes.Buffer
	l, sink, err := Setup(Options{Level: "info", Console: &buf})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sink.DebugRegistry().Run(ctx)

	target := &fakeDebugTarget{}
	sink.DebugRegistry().Subscribe("ergo", target, DebugRaw)

	IRC(l, "ergo", ">>", "PING")
	waitFor(t, "raw delivered", func() bool { return target.rawCount() == 1 })

	netLog := With(l, "network", "ergo")
	netLog.Info("uplink registered", "nick", "foo")
	time.Sleep(50 * time.Millisecond) // give Run a chance if it were (wrongly) delivered
	if target.logCount() != 0 {
		t.Fatalf("raw-only subscriber got %d log deliveries, want 0", target.logCount())
	}
}

func TestDebugRegistryLogModeMatchesBoundNetworkAttr(t *testing.T) {
	var buf bytes.Buffer
	l, sink, err := Setup(Options{Level: "info", Console: &buf})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sink.DebugRegistry().Run(ctx)

	ergo := &fakeDebugTarget{}
	other := &fakeDebugTarget{}
	sink.DebugRegistry().Subscribe("ergo", ergo, DebugLog)
	sink.DebugRegistry().Subscribe("libera", other, DebugLog)

	netLog := With(l, "network", "ergo")
	netLog.Info("uplink registered", "nick", "foo")

	waitFor(t, "ergo target sees log delivery", func() bool { return ergo.logCount() == 1 })
	if other.logCount() != 0 {
		t.Fatalf("libera subscriber got %d log deliveries, want 0 (cross-network leak)", other.logCount())
	}
}

// TestDebugRegistryLogModeFallsBackToNameAttr proves the "name" alias
// internal/server.Server's own (non network-bound) logger relies on for
// its network-lifecycle lines ("network started", etc.) — see
// teeHandler.tap's doc comment.
func TestDebugRegistryLogModeFallsBackToNameAttr(t *testing.T) {
	var buf bytes.Buffer
	l, sink, err := Setup(Options{Level: "info", Console: &buf})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sink.DebugRegistry().Run(ctx)

	target := &fakeDebugTarget{}
	sink.DebugRegistry().Subscribe("ergo", target, DebugLog)

	l.Info("network started", "name", "ergo")
	waitFor(t, "name-attr fallback delivered", func() bool { return target.logCount() == 1 })
}

func TestDebugRegistryLogModeExcludesRaw(t *testing.T) {
	var buf bytes.Buffer
	l, sink, err := Setup(Options{Level: "info", Console: &buf})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sink.DebugRegistry().Run(ctx)

	target := &fakeDebugTarget{}
	sink.DebugRegistry().Subscribe("ergo", target, DebugLog)

	netLog := With(l, "network", "ergo")
	netLog.Info("some event")
	waitFor(t, "log delivered", func() bool { return target.logCount() == 1 })

	IRC(l, "ergo", ">>", "PING")
	time.Sleep(50 * time.Millisecond)
	if target.rawCount() != 0 {
		t.Fatalf("log-only subscriber got %d raw deliveries, want 0", target.rawCount())
	}
}

func TestDebugRegistryAllModeGetsBoth(t *testing.T) {
	var buf bytes.Buffer
	l, sink, err := Setup(Options{Level: "info", Console: &buf})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sink.DebugRegistry().Run(ctx)

	target := &fakeDebugTarget{}
	sink.DebugRegistry().Subscribe("ergo", target, DebugAll)

	IRC(l, "ergo", ">>", "PING")
	With(l, "network", "ergo").Info("some event")

	waitFor(t, "raw delivered", func() bool { return target.rawCount() == 1 })
	waitFor(t, "log delivered", func() bool { return target.logCount() == 1 })
}

func TestDebugRegistryUnsubscribeStopsDelivery(t *testing.T) {
	var buf bytes.Buffer
	l, sink, err := Setup(Options{Level: "info", Console: &buf})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sink.DebugRegistry().Run(ctx)

	target := &fakeDebugTarget{}
	sink.DebugRegistry().Subscribe("ergo", target, DebugRaw)
	IRC(l, "ergo", ">>", "one")
	waitFor(t, "first delivered", func() bool { return target.rawCount() == 1 })

	sink.DebugRegistry().Unsubscribe("ergo", target)
	IRC(l, "ergo", ">>", "two")
	time.Sleep(50 * time.Millisecond)
	if target.rawCount() != 1 {
		t.Fatalf("raw count = %d after Unsubscribe, want unchanged 1", target.rawCount())
	}
}

// TestDebugRegistrySurvivesReload proves a live subscription (and its
// registry) is untouched by Reload — the whole point of teeHandler
// wrapping the swap root once in Setup rather than being rebuilt inside
// buildHandler.
func TestDebugRegistrySurvivesReload(t *testing.T) {
	var buf bytes.Buffer
	l, sink, err := Setup(Options{Level: "info", Console: &buf})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sink.DebugRegistry().Run(ctx)

	target := &fakeDebugTarget{}
	sink.DebugRegistry().Subscribe("ergo", target, DebugRaw)

	var buf2 bytes.Buffer
	if err := sink.Reload(Options{Level: "warn", Console: &buf2}); err != nil {
		t.Fatal(err)
	}

	IRC(l, "ergo", ">>", "still here")
	waitFor(t, "delivery survives reload", func() bool { return target.rawCount() == 1 })
}

// TestDebugSubscriptionWorksBelowConfiguredLevel proves /bnc debug doesn't
// require -debug mode: a subscriber still gets raw (Debug-level) traffic
// even when the console/file level is configured well above Debug.
func TestDebugSubscriptionWorksBelowConfiguredLevel(t *testing.T) {
	var buf bytes.Buffer
	l, sink, err := Setup(Options{Level: "error", Console: &buf})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sink.DebugRegistry().Run(ctx)

	target := &fakeDebugTarget{}
	sink.DebugRegistry().Subscribe("ergo", target, DebugRaw)

	IRC(l, "ergo", ">>", "hi")
	waitFor(t, "delivered despite error-level console", func() bool { return target.rawCount() == 1 })
	if buf.Len() != 0 {
		t.Fatalf("expected nothing written to the real console at error level, got: %s", buf.String())
	}
}
