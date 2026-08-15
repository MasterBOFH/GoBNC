package session

import (
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

func destIDs(dests []RouteDest) []ClientID {
	out := make([]ClientID, len(dests))
	for i, d := range dests {
		out[i] = d.Client
	}
	return out
}

func requireDests(t *testing.T, dests []RouteDest, want ...ClientID) {
	t.Helper()
	got := destIDs(dests)
	if len(got) != len(want) {
		t.Fatalf("dests %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("dests %v want %v", got, want)
		}
	}
}

func TestRequestTrackerMODEDifferentListLetters(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	_, _, w1 := rt.Begin(BeginOpts{Client: "c1", Cmd: "MODE", EnquiryTarget: "#c", ModeLetters: "b"})
	_, _, w2 := rt.Begin(BeginOpts{Client: "c2", Cmd: "MODE", EnquiryTarget: "#c", ModeLetters: "e"})
	if !w1 || !w2 {
		t.Fatal("MODE #c b vs #c e must both write", w1, w2)
	}
	got := rt.RouteAll(irc.Message{Command: "367", Params: []string{"me", "#c", "*!*@x"}}, cm)
	requireDests(t, got, "c1")
	got = rt.RouteAll(irc.Message{Command: "348", Params: []string{"me", "#c", "*!*@y"}}, cm)
	requireDests(t, got, "c2")
	got = rt.RouteAll(irc.Message{Command: "368", Params: []string{"me", "#c", "End"}}, cm)
	requireDests(t, got, "c1")
	got = rt.RouteAll(irc.Message{Command: "349", Params: []string{"me", "#c", "End"}}, cm)
	requireDests(t, got, "c2")
}

func TestRequestTrackerMODESameListLettersCoalesce(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	_, _, w1 := rt.Begin(BeginOpts{Client: "c1", Cmd: "MODE", EnquiryTarget: "#c", ModeLetters: "+b"})
	_, _, w2 := rt.Begin(BeginOpts{Client: "c2", Cmd: "MODE", EnquiryTarget: "#c", ModeLetters: "b"})
	_, _, w3 := rt.Begin(BeginOpts{Client: "c3", Cmd: "MODE", EnquiryTarget: "#c", ModeLetters: "-b"})
	if !w1 || w2 || w3 {
		t.Fatal("MODE #c +b / b / -b are the same list enquiry", w1, w2, w3)
	}
	got := rt.RouteAll(irc.Message{Command: "367", Params: []string{"me", "#c", "*!*@x"}}, cm)
	requireDests(t, got, "c1", "c2", "c3")
	got = rt.RouteAll(irc.Message{Command: "368", Params: []string{"me", "#c", "End"}}, cm)
	requireDests(t, got, "c1", "c2", "c3")
}

func TestRequestTrackerWHOISPartialWire(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	_, _, w1 := rt.Begin(BeginOpts{Client: "c1", Cmd: "WHOIS", WhoisTargets: []string{"bob"}})
	if !w1 {
		t.Fatal("first WHOIS bob must write")
	}
	var wire []string
	_, _, w2 := rt.Begin(BeginOpts{
		Client: "c2", Cmd: "WHOIS", WhoisTargets: []string{"bob", "alice"}, WhoisWire: &wire,
	})
	if !w2 {
		t.Fatal("WHOIS bob,alice must still write alice")
	}
	if len(wire) != 1 || wire[0] != "alice" {
		t.Fatalf("WhoisWire=%v want [alice]", wire)
	}
	got := rt.RouteAll(irc.Message{Command: "311", Params: []string{"me", "bob"}}, cm)
	requireDests(t, got, "c1", "c2")
	got = rt.RouteAll(irc.Message{Command: "311", Params: []string{"me", "alice"}}, cm)
	requireDests(t, got, "c2")
	got = rt.RouteAll(irc.Message{Command: "318", Params: []string{"me", "bob"}}, cm)
	requireDests(t, got, "c1", "c2")
	got = rt.RouteAll(irc.Message{Command: "318", Params: []string{"me", "alice"}}, cm)
	requireDests(t, got, "c2")
}

func TestRequestTrackerWHOXSameSpecCoalesce(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	mask := cm.Canonical("#c")
	_, tok, w1 := rt.Begin(BeginOpts{
		Client: "c1", Cmd: "WHO", PreferWHOX: true, WHOMask: mask,
		WHOXFlags: "o", WHOXFields: "nuhs",
	})
	_, tok2, w2 := rt.Begin(BeginOpts{
		Client: "c2", Cmd: "WHO", PreferWHOX: true, WHOMask: mask,
		WHOXFlags: "o", WHOXFields: "nuhs",
	})
	if !w1 || w2 {
		t.Fatal("identical WHOX must skip the second write", w1, w2)
	}
	if tok == "" || tok2 != "" {
		t.Fatalf("token first=%q second=%q", tok, tok2)
	}
	rt.SetWHOXClientFix(tok, true, "")
	got := rt.RouteAll(irc.Message{Command: "354", Params: []string{"me", tok, "x"}}, cm)
	requireDests(t, got, "c1", "c2")
	if !got[0].StripWHOX || !got[1].StripWHOX {
		t.Fatal("both dests must strip the injected token")
	}
	got = rt.RouteAll(irc.Message{Command: "315", Params: []string{"me", mask, "End"}}, cm)
	requireDests(t, got, "c1", "c2")
}

func TestRequestTrackerWHOXTokenNormalizedCoalesce(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	mask := cm.Canonical("#c")
	_, tok, w1 := rt.Begin(BeginOpts{
		Client: "c1", Cmd: "WHO", PreferWHOX: true, WHOMask: mask,
		WHOXFlags: "o", WHOXFields: "nuhst", WHOXClientToken: "9",
	})
	_, tok2, w2 := rt.Begin(BeginOpts{
		Client: "c2", Cmd: "WHO", PreferWHOX: true, WHOMask: mask,
		WHOXFlags: "o", WHOXFields: "nuhst", WHOXClientToken: "7",
	})
	if !w1 || w2 {
		t.Fatal("WHOX that differ only by token must coalesce", w1, w2)
	}
	if tok == "" || tok2 != "" {
		t.Fatalf("one uplink token: first=%q second=%q", tok, tok2)
	}
	rt.SetWHOXClientFix(tok, false, "9")
	got := rt.RouteAll(irc.Message{Command: "354", Params: []string{"me", tok, "rest"}}, cm)
	requireDests(t, got, "c1", "c2")
	if got[0].StripWHOX || got[0].RestoreWHOX != "9" {
		t.Fatalf("c1 rewrite strip=%v restore=%q", got[0].StripWHOX, got[0].RestoreWHOX)
	}
	if got[1].StripWHOX || got[1].RestoreWHOX != "7" {
		t.Fatalf("c2 rewrite strip=%v restore=%q", got[1].StripWHOX, got[1].RestoreWHOX)
	}
}

func TestRequestTrackerWHOXFieldsWithoutTStillCoalesce(t *testing.T) {
	rt := NewRequestTracker()
	mask := irc.CaseRFC1459.Canonical("#c")
	_, tok, w1 := rt.Begin(BeginOpts{
		Client: "c1", Cmd: "WHO", PreferWHOX: true, WHOMask: mask,
		WHOXFlags: "o", WHOXFields: "nuhs",
	})
	_, _, w2 := rt.Begin(BeginOpts{
		Client: "c2", Cmd: "WHO", PreferWHOX: true, WHOMask: mask,
		WHOXFlags: "o", WHOXFields: "nuhst", WHOXClientToken: "7",
	})
	if !w1 || w2 || tok == "" {
		t.Fatal("nuhs vs nuhst,7 differ only by token machinery", w1, w2, tok)
	}
	rt.SetWHOXClientFix(tok, true, "")
	got := rt.RouteAll(irc.Message{Command: "354", Params: []string{"me", tok, "x"}}, irc.CaseRFC1459)
	requireDests(t, got, "c1", "c2")
	if !got[0].StripWHOX {
		t.Fatal("c1 sent no token; strip injected t")
	}
	if got[1].StripWHOX || got[1].RestoreWHOX != "7" {
		t.Fatalf("c2 must get its token back: strip=%v restore=%q", got[1].StripWHOX, got[1].RestoreWHOX)
	}
}

func TestRequestTrackerWHOXFlagsOrTargetMismatchNoCoalesce(t *testing.T) {
	rt := NewRequestTracker()
	mask := irc.CaseRFC1459.Canonical("#c")
	_, t1, w1 := rt.Begin(BeginOpts{
		Client: "c1", Cmd: "WHO", PreferWHOX: true, WHOMask: mask,
		WHOXFlags: "o", WHOXFields: "nuhs",
	})
	_, t2, w2 := rt.Begin(BeginOpts{
		Client: "c2", Cmd: "WHO", PreferWHOX: true, WHOMask: mask,
		WHOXFlags: "x", WHOXFields: "nuhs",
	})
	if !w1 || !w2 || t1 == "" || t2 == "" || t1 == t2 {
		t.Fatal("different flags must not coalesce", w1, w2, t1, t2)
	}
	_, t3, w3 := rt.Begin(BeginOpts{
		Client: "c3", Cmd: "WHO", PreferWHOX: true, WHOMask: irc.CaseRFC1459.Canonical("#d"),
		WHOXFlags: "o", WHOXFields: "nuhs",
	})
	if !w3 || t3 == t1 {
		t.Fatal("different target must not coalesce", w3, t3, t1)
	}
}

func TestWHOXCoalesceRestoresEachClientToken(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	s.registered = true
	a := &fakeDL{id: "a", caps: map[string]bool{}}
	b := &fakeDL{id: "b", caps: map[string]bool{}}
	s.mu.Lock()
	s.downlinks[a.ID()] = a
	s.downlinks[b.ID()] = b
	s.mu.Unlock()

	mask := s.isupport.CaseMapping.Canonical("#c")
	_, tok, w1 := s.tracker.Begin(BeginOpts{
		Client: a.ID(), Cmd: "WHO", PreferWHOX: true, WHOMask: mask,
		WHOXFlags: "o", WHOXFields: "nuhst", WHOXClientToken: "9",
	})
	_, _, w2 := s.tracker.Begin(BeginOpts{
		Client: b.ID(), Cmd: "WHO", PreferWHOX: true, WHOMask: mask,
		WHOXFlags: "o", WHOXFields: "nuhst", WHOXClientToken: "7",
	})
	if !w1 || w2 || tok == "" {
		t.Fatal(w1, w2, tok)
	}
	s.tracker.SetWHOXClientFix(tok, false, "9")
	s.HandleMessage(irc.Message{Command: "354", Params: []string{"me", tok, "nick"}})

	as, bs := a.snapshot(), b.snapshot()
	if countCmds(as, "354") != 1 || countCmds(bs, "354") != 1 {
		t.Fatalf("a=%+v b=%+v", as, bs)
	}
	if as[0].Param(1) != "9" {
		t.Fatalf("a token=%q want 9", as[0].Param(1))
	}
	if bs[0].Param(1) != "7" {
		t.Fatalf("b token=%q want 7", bs[0].Param(1))
	}
}

func TestNAMESCoalesceFanoutToDownlinks(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	s.registered = true
	a := &fakeDL{id: "a", caps: map[string]bool{}}
	b := &fakeDL{id: "b", caps: map[string]bool{}}
	s.mu.Lock()
	s.downlinks[a.ID()] = a
	s.downlinks[b.ID()] = b
	s.mu.Unlock()

	_, _, w1 := s.tracker.Begin(BeginOpts{Client: a.ID(), Cmd: "NAMES", EnquiryTarget: "#c"})
	_, _, w2 := s.tracker.Begin(BeginOpts{Client: b.ID(), Cmd: "NAMES", EnquiryTarget: "#c"})
	if !w1 || w2 {
		t.Fatal(w1, w2)
	}

	s.HandleMessage(irc.Message{Command: "353", Params: []string{"me", "=", "#c", "n1 n2"}})
	s.HandleMessage(irc.Message{Command: "366", Params: []string{"me", "#c", "End"}})

	if countCmds(a.snapshot(), "353") != 1 || countCmds(a.snapshot(), "366") != 1 {
		t.Fatalf("a got %+v", a.snapshot())
	}
	if countCmds(b.snapshot(), "353") != 1 || countCmds(b.snapshot(), "366") != 1 {
		t.Fatalf("b got %+v", b.snapshot())
	}
}

func TestRequestTrackerWHOISLocalVsRemote(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	_, _, w1 := rt.Begin(BeginOpts{Client: "c1", Cmd: "WHOIS", WhoisTargets: []string{"mriron"}})
	_, _, w2 := rt.Begin(BeginOpts{
		Client: "c2", Cmd: "WHOIS", WhoisTargets: []string{"mriron"}, Remote: whoisRemoteNick,
	})
	if !w1 || w2 {
		t.Fatal("WHOIS nick vs WHOIS nick nick must not coalesce", w1, w2)
	}
	got := rt.RouteAll(irc.Message{Command: "311", Params: []string{"me", "mriron", "u", "h", "*", "r"}}, cm)
	requireDests(t, got, "c1")
	got = rt.RouteAll(irc.Message{Command: "318", Params: []string{"me", "mriron", "End"}}, cm)
	requireDests(t, got, "c1")
	ready := rt.TakeReady()
	if len(ready) != 1 || ready[0].Command != "WHOIS" || len(ready[0].Params) != 2 || ready[0].Params[0] != "mriron" || ready[0].Params[1] != "mriron" {
		t.Fatalf("held remote WHOIS: %+v", ready)
	}
}

func TestRequestTrackerWHOISRemoteCoalesce(t *testing.T) {
	rt := NewRequestTracker()
	_, _, w1 := rt.Begin(BeginOpts{
		Client: "c1", Cmd: "WHOIS", WhoisTargets: []string{"mriron"}, Remote: whoisRemoteNick,
	})
	_, _, w2 := rt.Begin(BeginOpts{
		Client: "c2", Cmd: "WHOIS", WhoisTargets: []string{"mriron"}, Remote: whoisRemoteNick,
	})
	if !w1 || w2 {
		t.Fatal("two WHOIS nick nick must coalesce", w1, w2)
	}
}

func TestRequestTrackerWHOISExplicitServer(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	_, _, w1 := rt.Begin(BeginOpts{Client: "c1", Cmd: "WHOIS", WhoisTargets: []string{"mriron"}, Remote: "eu.undernet.org"})
	_, _, w2 := rt.Begin(BeginOpts{Client: "c2", Cmd: "WHOIS", WhoisTargets: []string{"mriron"}})
	if !w1 || w2 {
		t.Fatal("WHOIS server nick vs local must not coalesce", w1, w2)
	}
	got := rt.RouteAll(irc.Message{Command: "311", Params: []string{"me", "mriron"}}, cm)
	requireDests(t, got, "c1")
}

func TestRequestTrackerSTATSLocalVsRemote(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	_, _, w1 := rt.Begin(BeginOpts{Client: "c1", Cmd: "STATS", StatsLetter: "c"})
	_, _, w2 := rt.Begin(BeginOpts{
		Client: "c2", Cmd: "STATS", StatsLetter: "c", Remote: "eu.undernet.org",
		Outbound: irc.Message{Command: "STATS", Params: []string{"c", "eu.undernet.org"}},
	})
	if !w1 || w2 {
		t.Fatal("STATS c vs STATS c server must not coalesce", w1, w2)
	}
	got := rt.RouteAll(irc.Message{Command: "219", Params: []string{"me", "c", "End"}}, cm)
	requireDests(t, got, "c1")
	ready := rt.TakeReady()
	if len(ready) != 1 || ready[0].Param(1) != "eu.undernet.org" {
		t.Fatalf("held remote STATS: %+v", ready)
	}
}

func TestRequestTrackerSTATSRemoteCoalesce(t *testing.T) {
	rt := NewRequestTracker()
	_, _, w1 := rt.Begin(BeginOpts{Client: "c1", Cmd: "STATS", StatsLetter: "c", Remote: "eu.undernet.org"})
	_, _, w2 := rt.Begin(BeginOpts{Client: "c2", Cmd: "STATS", StatsLetter: "c", Remote: "eu.undernet.org"})
	if !w1 || w2 {
		t.Fatal("two STATS c server must coalesce", w1, w2)
	}
}

func TestRequestTrackerNAMESLocalVsRemote(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	_, _, w1 := rt.Begin(BeginOpts{Client: "c1", Cmd: "NAMES", EnquiryTarget: "#c"})
	_, _, w2 := rt.Begin(BeginOpts{
		Client: "c2", Cmd: "NAMES", EnquiryTarget: "#c", Remote: "eu.undernet.org",
		Outbound: irc.Message{Command: "NAMES", Params: []string{"#c", "eu.undernet.org"}},
	})
	if !w1 || w2 {
		t.Fatal("NAMES #c vs NAMES #c server must not coalesce", w1, w2)
	}
	got := rt.RouteAll(irc.Message{Command: "353", Params: []string{"me", "=", "#c", "n"}}, cm)
	requireDests(t, got, "c1")
	got = rt.RouteAll(irc.Message{Command: "366", Params: []string{"me", "#c", "End"}}, cm)
	requireDests(t, got, "c1")
	ready := rt.TakeReady()
	if len(ready) != 1 || ready[0].Param(1) != "eu.undernet.org" {
		t.Fatalf("held remote NAMES: %+v", ready)
	}
}

func TestRequestTrackerTIMELocalVsRemote(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	_, _, w1 := rt.Begin(BeginOpts{Client: "c1", Cmd: "TIME"})
	_, _, w2 := rt.Begin(BeginOpts{
		Client: "c2", Cmd: "TIME", Remote: "eu.undernet.org",
		Outbound: irc.Message{Command: "TIME", Params: []string{"eu.undernet.org"}},
	})
	if !w1 || w2 {
		t.Fatal("TIME vs TIME server must not both send", w1, w2)
	}
	c, only, _, _, _ := rt.RouteMessage(irc.Message{Command: "391", Params: []string{"me", "irc.local", "now"}}, cm)
	if !only || c != "c1" {
		t.Fatal(c, only)
	}
	ready := rt.TakeReady()
	if len(ready) != 1 || ready[0].Param(0) != "eu.undernet.org" {
		t.Fatalf("held remote TIME: %+v", ready)
	}
}
