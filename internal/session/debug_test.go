package session

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	gobnclog "github.com/MasterBOFH/GoBNC/internal/log"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

func newDebugTestSession(t *testing.T, network string) (*Session, *gobnclog.Sink) {
	t.Helper()
	var buf bytes.Buffer
	logger, sink, err := gobnclog.Setup(gobnclog.Options{Level: "info", Console: &buf})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go sink.DebugRegistry().Run(ctx)

	s := New(store.Network{Name: network, Nick: "me"}, nil, nil, logger.With("network", network), nil)
	s.SetDebugRegistry(sink.DebugRegistry())
	return s, sink
}

// attachFake registers d directly into s.downlinks without going through
// Attach's full replay-burst logic (irrelevant noise for these tests) —
// sessionDebugTarget.deliver deliberately re-looks-up the downlink here on
// every delivery (see its own doc comment), so a subscription with nothing
// registered here would silently never deliver.
func attachFake(s *Session, d *fakeDL) {
	s.mu.Lock()
	s.downlinks[d.ID()] = d
	s.mu.Unlock()
}

func waitForSent(t *testing.T, desc string, d *fakeDL, n int) []irc.Message {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if sent := d.snapshot(); len(sent) >= n {
			return sent
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s (have %d)", desc, len(d.snapshot()))
	return nil
}

func TestBNCDebugRawDeliversAndScopesByNetwork(t *testing.T) {
	s1, _ := newDebugTestSession(t, "n1")
	s2, _ := newDebugTestSession(t, "n2")

	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	attachFake(s1, d)
	if err := s1.HandleClientMessage(d, irc.Message{Command: "BNC", Params: []string{"debug", "raw"}}); err != nil {
		t.Fatal(err)
	}
	sent := d.snapshot()
	if len(sent) != 1 || sent[0].Command != "NOTICE" {
		t.Fatalf("expected one confirmation NOTICE, got %#v", sent)
	}
	d.clearSent()

	// A raw line for a DIFFERENT network must never reach this client.
	gobnclog.IRC(s2.log, "n2", ">>", "PING")
	time.Sleep(50 * time.Millisecond)
	if len(d.snapshot()) != 0 {
		t.Fatalf("cross-network raw leak: %#v", d.snapshot())
	}

	// A raw line for THIS network (n1) must arrive as a >debug PRIVMSG.
	gobnclog.IRC(s1.log, "n1", ">>", "NICK foo")
	sent = waitForSent(t, "raw delivery", d, 1)
	if sent[0].Command != "PRIVMSG" || sent[0].Source != ">debug" || sent[0].Param(0) != "me" {
		t.Fatalf("unexpected raw delivery: %#v", sent[0])
	}
	if got := sent[0].Trailing(); got != ">> NICK foo" {
		t.Fatalf("raw text = %q, want %q", got, ">> NICK foo")
	}
}

func TestBNCDebugLogModeExcludesRawAndVersa(t *testing.T) {
	s, _ := newDebugTestSession(t, "n1")
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	attachFake(s, d)
	if err := s.HandleClientMessage(d, irc.Message{Command: "BNC", Params: []string{"debug", "log"}}); err != nil {
		t.Fatal(err)
	}
	d.clearSent()

	gobnclog.IRC(s.log, "n1", ">>", "PING")
	time.Sleep(50 * time.Millisecond)
	if len(d.snapshot()) != 0 {
		t.Fatalf("log-mode subscriber got raw traffic: %#v", d.snapshot())
	}

	s.log.Info("uplink registered", "nick", "foo")
	sent := waitForSent(t, "log delivery", d, 1)
	if sent[0].Command != "PRIVMSG" || sent[0].Source != ">debug" {
		t.Fatalf("unexpected log delivery: %#v", sent[0])
	}
}

func TestBNCDebugAllGetsBoth(t *testing.T) {
	s, _ := newDebugTestSession(t, "n1")
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	attachFake(s, d)
	if err := s.HandleClientMessage(d, irc.Message{Command: "BNC", Params: []string{"debug", "all"}}); err != nil {
		t.Fatal(err)
	}
	d.clearSent()

	gobnclog.IRC(s.log, "n1", ">>", "PING")
	s.log.Info("uplink registered", "nick", "foo")
	waitForSent(t, "both raw and log delivered", d, 2)
}

func TestBNCDebugOffStopsDelivery(t *testing.T) {
	s, _ := newDebugTestSession(t, "n1")
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	attachFake(s, d)
	if err := s.HandleClientMessage(d, irc.Message{Command: "BNC", Params: []string{"debug", "raw"}}); err != nil {
		t.Fatal(err)
	}
	d.clearSent()
	gobnclog.IRC(s.log, "n1", ">>", "one")
	waitForSent(t, "first delivery", d, 1)
	d.clearSent()

	if err := s.HandleClientMessage(d, irc.Message{Command: "BNC", Params: []string{"debug", "off"}}); err != nil {
		t.Fatal(err)
	}
	sent := d.snapshot()
	if len(sent) != 1 || sent[0].Trailing() != "debug: disabled" {
		t.Fatalf("expected off confirmation, got %#v", sent)
	}
	d.clearSent()

	gobnclog.IRC(s.log, "n1", ">>", "two")
	time.Sleep(50 * time.Millisecond)
	if len(d.snapshot()) != 0 {
		t.Fatalf("delivery continued after /bnc debug off: %#v", d.snapshot())
	}
}

func TestBNCDebugDetachUnsubscribes(t *testing.T) {
	s, _ := newDebugTestSession(t, "n1")
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.HandleClientMessage(d, irc.Message{Command: "BNC", Params: []string{"debug", "raw"}}); err != nil {
		t.Fatal(err)
	}
	s.mu.RLock()
	n := len(s.debugTargets)
	s.mu.RUnlock()
	if n != 1 {
		t.Fatalf("debugTargets len = %d after subscribe, want 1", n)
	}

	s.Detach(d.ID())

	s.mu.RLock()
	n = len(s.debugTargets)
	s.mu.RUnlock()
	if n != 0 {
		t.Fatalf("debugTargets len = %d after Detach, want 0 (leaked subscription)", n)
	}
}

func TestBNCDebugUnavailableWithoutRegistry(t *testing.T) {
	s := New(store.Network{Name: "n1", Nick: "me"}, nil, nil, nil, nil)
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.HandleClientMessage(d, irc.Message{Command: "BNC", Params: []string{"debug", "raw"}}); err != nil {
		t.Fatal(err)
	}
	sent := d.snapshot()
	if len(sent) != 1 || sent[0].Trailing() != "debug unavailable" {
		t.Fatalf("%#v", sent)
	}
}

func TestBNCDebugBadArgUsage(t *testing.T) {
	s, _ := newDebugTestSession(t, "n1")
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.HandleClientMessage(d, irc.Message{Command: "BNC", Params: []string{"debug", "bogus"}}); err != nil {
		t.Fatal(err)
	}
	sent := d.snapshot()
	if len(sent) != 1 || sent[0].Trailing() != "usage: /bnc debug raw|log|all|off" {
		t.Fatalf("%#v", sent)
	}
}
