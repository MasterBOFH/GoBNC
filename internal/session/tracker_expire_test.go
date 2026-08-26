package session

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// setClock installs a test clock on the tracker under its lock (the
// tracker reads rt.now from routeLocked on every uplink line).
func (rt *RequestTracker) setClock(now func() time.Time) {
	rt.mu.Lock()
	rt.now = now
	rt.mu.Unlock()
}

// A request whose reply never arrives (server silently drops a remote
// STATS, netsplit eats the end numeric, an ircd quirk we don't know about)
// must stop pinning its route after requestTTL — and must stop blocking
// the same-letter request held behind it.
func TestRequestTrackerStaleHeadExpiresAndReleasesHeldWrite(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	base := time.Now()
	var offset atomic.Int64
	rt.setClock(func() time.Time { return base.Add(time.Duration(offset.Load())) })

	_, _, w1 := rt.Begin(BeginOpts{
		Client: "c1", Cmd: "STATS", StatsLetter: "p", Remote: "silent.example",
		Outbound: irc.Message{Command: "STATS", Params: []string{"p", "silent.example"}},
	})
	if !w1 {
		t.Fatal("first STATS should write")
	}
	offset.Store(int64(5 * time.Minute))
	_, _, w2 := rt.Begin(BeginOpts{
		Client: "c2", Cmd: "STATS", StatsLetter: "p",
		Outbound: irc.Message{Command: "STATS", Params: []string{"p"}},
	})
	if w2 {
		t.Fatal("STATS p behind an in-flight STATS p server must be held")
	}
	// Just under the TTL: nothing changes, c2 stays held.
	offset.Store(int64(requestTTL - time.Second))
	_, _, w3 := rt.Begin(BeginOpts{
		Client: "c3", Cmd: "STATS", StatsLetter: "p",
		Outbound: irc.Message{Command: "STATS", Params: []string{"p"}},
	})
	if w3 {
		t.Fatal("c3 should coalesce onto held c2, not write")
	}
	if ready := rt.TakeReady(); len(ready) != 0 {
		t.Fatalf("nothing should be released before the TTL: %+v", ready)
	}
	// Past the TTL: the next Begin sweeps c1; c2's held line is released,
	// c4 coalesces onto c2.
	offset.Store(int64(requestTTL + time.Second))
	_, _, w4 := rt.Begin(BeginOpts{
		Client: "c4", Cmd: "STATS", StatsLetter: "p",
		Outbound: irc.Message{Command: "STATS", Params: []string{"p"}},
	})
	if w4 {
		t.Fatal("c4 should coalesce onto the now-in-flight c2")
	}
	ready := rt.TakeReady()
	if len(ready) != 1 || ready[0].Command != "STATS" || ready[0].Param(1) != "" {
		t.Fatalf("expired head must release the held local STATS p: %+v", ready)
	}
	got := rt.RouteAll(irc.Message{Command: "219", Params: []string{"me", "p", "End"}}, cm)
	requireDests(t, got, "c2", "c3", "c4")
	if got = rt.RouteAll(irc.Message{Command: "219", Params: []string{"me", "p", "End"}}, cm); len(got) != 0 {
		t.Fatalf("second 219: %v", destIDs(got))
	}
}

// Stale routes are disregarded on the reply side too: a numeric arriving
// after the TTL is not delivered to the original requester (serial,
// keyed, labeled and WHOX paths).
func TestRequestTrackerStaleRoutesDisregarded(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	base := time.Now()
	var offset atomic.Int64
	rt.setClock(func() time.Time { return base.Add(time.Duration(offset.Load())) })

	rt.Begin(BeginOpts{Client: "c1", Cmd: "LIST", Outbound: irc.Message{Command: "LIST"}})
	rt.Begin(BeginOpts{Client: "c1", Cmd: "WHOIS", WhoisTargets: []string{"bob"}})
	label, _, _ := rt.Begin(BeginOpts{Client: "c1", Cmd: "TIME", PreferLabel: true})
	_, tok, _ := rt.Begin(BeginOpts{Client: "c1", Cmd: "WHO", PreferWHOX: true, WHOMask: "#a"})
	if label == "" || tok == "" {
		t.Fatal(label, tok)
	}

	offset.Store(int64(requestTTL + time.Second))
	if got := rt.RouteAll(irc.Message{Command: "322", Params: []string{"me", "#c", "1", "t"}}, cm); len(got) != 0 {
		t.Fatalf("stale LIST route used: %v", destIDs(got))
	}
	if got := rt.RouteAll(irc.Message{Command: "311", Params: []string{"me", "bob", "u", "h", "*", "r"}}, cm); len(got) != 0 {
		t.Fatalf("stale WHOIS route used: %v", destIDs(got))
	}
	lbl := irc.Message{Tags: map[string]string{"label": label}, Command: "391", Params: []string{"me", "srv", "now"}}
	if got := rt.RouteAll(lbl, cm); len(got) != 0 {
		t.Fatalf("stale labeled route used: %v", destIDs(got))
	}
	if got := rt.RouteAll(irc.Message{Command: "354", Params: []string{"me", tok, "#a", "n"}}, cm); len(got) != 0 {
		t.Fatalf("stale WHOX route used: %v", destIDs(got))
	}
	if _, ok := rt.ActiveClient(); ok {
		t.Fatal("no request should still be active")
	}
}

// End-to-end through a Session: a held LIST behind a head whose 323 never
// comes goes upstream as soon as the head expires — triggered by the next
// downlink request, with no uplink traffic needed to prompt it.
func TestSolicitousStaleHeadReleasesHeldWriteOnBegin(t *testing.T) {
	ln, host, port := newFakeIRCListener(t)
	netCfg := store.Network{Name: "test", Host: "pipe", Port: 1, Nick: "me", Username: "u", Realname: "r"}
	s := New(netCfg, nil, nil, nil, nil)

	uplinkLIST := make(chan string, 8)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		t.Cleanup(func() { _ = conn.Close() })
		_ = runSolicitousServer(conn, time.Now().Add(12*time.Second), make(chan string, 8), uplinkLIST)
	}()
	newTestUplink(t, s, netCfg, host, port)
	waitUntil(t, 5*time.Second, s.Registered)

	base := time.Now()
	var offset atomic.Int64
	s.mu.RLock()
	rt := s.tracker
	s.mu.RUnlock()
	rt.setClock(func() time.Time { return base.Add(time.Duration(offset.Load())) })

	d1 := &fakeDL{id: "c1", caps: map[string]bool{}}
	d2 := &fakeDL{id: "c2", caps: map[string]bool{}}
	d3 := &fakeDL{id: "c3", caps: map[string]bool{}}
	for _, d := range []*fakeDL{d1, d2, d3} {
		if err := s.Attach(d); err != nil {
			t.Fatal(err)
		}
	}

	expectLIST := func(desc string) {
		t.Helper()
		select {
		case line := <-uplinkLIST:
			if !strings.HasPrefix(line, "LIST") {
				t.Fatalf("%s: want LIST, got %q", desc, line)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s: LIST not sent upstream", desc)
		}
	}
	expectNoLIST := func(desc string) {
		t.Helper()
		select {
		case line := <-uplinkLIST:
			t.Fatalf("%s: unexpected uplink line %q", desc, line)
		case <-time.After(150 * time.Millisecond):
		}
	}

	if err := s.HandleClientMessage(d1, irc.Message{Command: "LIST"}); err != nil {
		t.Fatal(err)
	}
	expectLIST("first LIST")
	// d2 asks five minutes later, so it is still fresh when d1 expires.
	offset.Store(int64(5 * time.Minute))
	if err := s.HandleClientMessage(d2, irc.Message{Command: "LIST"}); err != nil {
		t.Fatal(err)
	}
	expectNoLIST("second LIST while first in flight")

	// The server never sends 323 for the first LIST. Past the TTL, d3's
	// request is what triggers the sweep; d2's LIST must go out right then.
	offset.Store(int64(requestTTL + time.Second))
	if err := s.HandleClientMessage(d3, irc.Message{Command: "LIST"}); err != nil {
		t.Fatal(err)
	}
	expectLIST("held LIST released by expiry")
	expectNoLIST("third LIST must queue behind the released second")

	// Replies now belong to d2, not the expired d1.
	s.HandleMessage(irc.Message{Command: "322", Params: []string{"me", "#c", "1", "topic"}})
	waitUntil(t, 2*time.Second, func() bool { return countCmd(d2, "322") == 1 })
	if countCmd(d1, "322") != 0 || countCmd(d3, "322") != 0 {
		t.Fatalf("322 misrouted: d1=%d d3=%d", countCmd(d1, "322"), countCmd(d3, "322"))
	}
}
