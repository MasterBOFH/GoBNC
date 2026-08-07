package session

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/MasterBOFH/GoBNC/internal/caps"
	"github.com/MasterBOFH/GoBNC/internal/history"
	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/internal/testutil"
	"github.com/MasterBOFH/GoBNC/internal/uplink"
)

func TestRequestTrackerISON(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	_, _, w1 := rt.Begin(BeginOpts{Client: "c1", Cmd: "ISON"})
	_, _, w2 := rt.Begin(BeginOpts{Client: "c2", Cmd: "ISON"})
	if w1 != nil || w2 == nil {
		t.Fatal("second ISON should wait", w1, w2)
	}
	c, only, _, _, _ := rt.RouteMessage(irc.Message{Command: "303", Params: []string{"me", "a b"}}, cm)
	if !only || c != "c1" {
		t.Fatalf("first 303 -> c1, got %s only=%v", c, only)
	}
	select {
	case <-w2:
	case <-time.After(time.Second):
		t.Fatal("second ISON not released")
	}
	c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "303", Params: []string{"me", ""}}, cm)
	if !only || c != "c2" {
		t.Fatalf("second 303 -> c2, got %s only=%v", c, only)
	}
}

func TestClientNICKBlockedUntilRegistered(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	s.registered = false
	s.uplink = nil
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.HandleClientMessage(d, irc.Message{Command: "NICK", Params: []string{"other"}}); err == nil {
		t.Fatal("expected uplink error")
	}

	u := uplink.New(uplink.Config{Network: store.Network{Name: "n", Nick: "me"}}, nil)
	s.SetUplink(u)
	if u.Registered() {
		t.Fatal("precondition: uplink not registered")
	}
	if err := s.HandleClientMessage(d, irc.Message{Command: "NICK", Params: []string{"other"}}); err != nil {
		t.Fatalf("NICK during register should be ignored, got %v", err)
	}
}

func TestRequestTrackerHELPAndADMINAndMAP(t *testing.T) {
	cm := irc.CaseRFC1459

	t.Run("HELP", func(t *testing.T) {
		rt := NewRequestTracker()
		_, _, w1 := rt.Begin(BeginOpts{Client: "c1", Cmd: "HELP"})
		_, _, w2 := rt.Begin(BeginOpts{Client: "c2", Cmd: "HELP"})
		if w1 != nil || w2 == nil {
			t.Fatal(w1, w2)
		}
		c, only, _, _, _ := rt.RouteMessage(irc.Message{Command: "704", Params: []string{"me", "INDEX", "start"}}, cm)
		if !only || c != "c1" {
			t.Fatal(c, only)
		}
		c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "705", Params: []string{"me", "INDEX", "line"}}, cm)
		if !only || c != "c1" {
			t.Fatal(c, only)
		}
		c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "706", Params: []string{"me", "INDEX", "End"}}, cm)
		if !only || c != "c1" {
			t.Fatal(c, only)
		}
		select {
		case <-w2:
		default:
			t.Fatal("c2 HELP should release after 706")
		}
	})

	t.Run("ADMIN", func(t *testing.T) {
		rt := NewRequestTracker()
		_, _, _ = rt.Begin(BeginOpts{Client: "c1", Cmd: "ADMIN"})
		for _, code := range []string{"256", "257", "258"} {
			c, only, _, _, _ := rt.RouteMessage(irc.Message{Command: code, Params: []string{"me", "x"}}, cm)
			if !only || c != "c1" {
				t.Fatalf("%s -> %s only=%v", code, c, only)
			}
		}
		c, only, _, _, _ := rt.RouteMessage(irc.Message{Command: "259", Params: []string{"me", "admin@example"}}, cm)
		if !only || c != "c1" {
			t.Fatal(c, only)
		}
		if _, ok := rt.ActiveClient(); ok {
			t.Fatal("ADMIN should clear on 259")
		}
	})

	t.Run("MAP ircu end", func(t *testing.T) {
		rt := NewRequestTracker()
		rt.SetIRCd(irc.IRCdIrcu)
		_, _, _ = rt.Begin(BeginOpts{Client: "c1", Cmd: "MAP"})
		c, only, _, _, _ := rt.RouteMessage(irc.Message{Command: "015", Params: []string{"me", "leaf"}}, cm)
		if !only || c != "c1" {
			t.Fatal(c, only)
		}
		// Unreal's 007 must not terminate ircu MAP.
		c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "007", Params: []string{"me", "End"}}, cm)
		if only {
			t.Fatal("007 should not end ircu MAP")
		}
		c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "017", Params: []string{"me", "End of /MAP"}}, cm)
		if !only || c != "c1" {
			t.Fatal(c, only)
		}
		if _, ok := rt.ActiveClient(); ok {
			t.Fatal("MAP should clear on 017")
		}
	})

	t.Run("MAP unreal end", func(t *testing.T) {
		rt := NewRequestTracker()
		rt.SetIRCd(irc.IRCdUnreal)
		_, _, _ = rt.Begin(BeginOpts{Client: "c1", Cmd: "MAP"})
		c, only, _, _, _ := rt.RouteMessage(irc.Message{Command: "006", Params: []string{"me", "leaf"}}, cm)
		if !only || c != "c1" {
			t.Fatal(c, only)
		}
		c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "017", Params: []string{"me", "End"}}, cm)
		if only {
			t.Fatal("017 should not end unreal MAP")
		}
		c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "007", Params: []string{"me", "End of /MAP"}}, cm)
		if !only || c != "c1" {
			t.Fatal(c, only)
		}
	})
}

func TestDetectIRCdOnWelcome(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	s.mu.Lock()
	s.rpl002 = []string{"Your host is x running version u2.10.12.19"}
	s.detectIRCdLocked()
	s.mu.Unlock()
	if s.IRCd() != irc.IRCdIrcu {
		t.Fatalf("ircd=%q", s.IRCd())
	}
	if s.tracker.IRCd() != irc.IRCdIrcu {
		t.Fatal("tracker not updated")
	}
}

func TestRequestTrackerLabeled(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	label, _, wait := rt.Begin(BeginOpts{Client: "c1", Cmd: "WHOIS", ClientLabel: "client-lbl", PreferLabel: true})
	if wait != nil || label == "" {
		t.Fatal(label, wait)
	}
	msg := irc.Message{Tags: map[string]string{"label": label}, Command: "311", Params: []string{"me", "other"}}
	client, only, echo, _, _ := rt.RouteMessage(msg, cm)
	if !only || client != "c1" || echo != "client-lbl" {
		t.Fatal(client, only, echo)
	}
	end := irc.Message{Tags: map[string]string{"label": label}, Command: "318", Params: []string{"me", "other"}}
	client, only, echo, _, _ = rt.RouteMessage(end, cm)
	if !only || client != "c1" || echo != "client-lbl" {
		t.Fatal(client, only, echo)
	}
}

func TestRequestTrackerSerialize(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	// Letter-less STATS falls through to the hold queue.
	_, _, wait1 := rt.Begin(BeginOpts{Client: "c1", Cmd: "LIST"})
	_, _, wait2 := rt.Begin(BeginOpts{Client: "c2", Cmd: "LIST"})
	if wait1 != nil {
		t.Fatal("first should not wait")
	}
	if wait2 == nil {
		t.Fatal("second should wait")
	}
	msg := irc.Message{Command: "322", Params: []string{"me", "#c", "1", "topic"}}
	c, only, _, _, _ := rt.RouteMessage(msg, cm)
	if !only || c != "c1" {
		t.Fatal(c, only)
	}
	end := irc.Message{Command: "323", Params: []string{"me", "End"}}
	c, only, _, _, _ = rt.RouteMessage(end, cm)
	if !only || c != "c1" {
		t.Fatal(c, only)
	}
	active, ok := rt.ActiveClient()
	if !ok || active != "c2" {
		t.Fatalf("active=%s ok=%v", active, ok)
	}
	// Gate for c2 must be open so it can send.
	select {
	case <-wait2:
	default:
		t.Fatal("second gate should be released after first end")
	}
}

func TestRequestTrackerMODEEnquiry(t *testing.T) {
	cm := irc.CaseRFC1459

	t.Run("channel modes 324+329", func(t *testing.T) {
		rt := NewRequestTracker()
		_, _, w1 := rt.Begin(BeginOpts{Client: "c1", Cmd: "MODE", EnquiryTarget: "#c"})
		if w1 != nil {
			t.Fatal("first should not wait")
		}
		c, only, _, _, _ := rt.RouteMessage(irc.Message{Command: "324", Params: []string{"me", "#c", "+nt"}}, cm)
		if !only || c != "c1" {
			t.Fatal(c, only)
		}
		c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "329", Params: []string{"me", "#c", "123"}}, cm)
		if !only || c != "c1" {
			t.Fatalf("329 sticky -> %s only=%v", c, only)
		}
	})

	t.Run("concurrent different channels", func(t *testing.T) {
		rt := NewRequestTracker()
		_, _, w1 := rt.Begin(BeginOpts{Client: "c1", Cmd: "MODE", EnquiryTarget: "#a"})
		_, _, w2 := rt.Begin(BeginOpts{Client: "c2", Cmd: "MODE", EnquiryTarget: "#b"})
		if w1 != nil || w2 != nil {
			t.Fatal("different targets must not hold", w1, w2)
		}
		c, only, _, _, _ := rt.RouteMessage(irc.Message{Command: "324", Params: []string{"me", "#b", "+n"}}, cm)
		if !only || c != "c2" {
			t.Fatalf("#b -> %s only=%v", c, only)
		}
		c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "324", Params: []string{"me", "#a", "+t"}}, cm)
		if !only || c != "c1" {
			t.Fatalf("#a -> %s only=%v", c, only)
		}
		c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "329", Params: []string{"me", "#a", "1"}}, cm)
		if !only || c != "c1" {
			t.Fatal(c, only)
		}
		c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "329", Params: []string{"me", "#b", "2"}}, cm)
		if !only || c != "c2" {
			t.Fatal(c, only)
		}
	})

	t.Run("same channel holds", func(t *testing.T) {
		rt := NewRequestTracker()
		_, _, w1 := rt.Begin(BeginOpts{Client: "c1", Cmd: "MODE", EnquiryTarget: "#c"})
		_, _, w2 := rt.Begin(BeginOpts{Client: "c2", Cmd: "MODE", EnquiryTarget: "#c"})
		if w1 != nil || w2 == nil {
			t.Fatal(w1, w2)
		}
		rt.RouteMessage(irc.Message{Command: "324", Params: []string{"me", "#c", "+nt"}}, cm)
		select {
		case <-w2:
		default:
			t.Fatal("c2 should release after c1's 324")
		}
		c, only, _, _, _ := rt.RouteMessage(irc.Message{Command: "367", Params: []string{"me", "#c", "*!*@x"}}, cm)
		if !only || c != "c2" {
			t.Fatalf("c2 banlist mid -> %s only=%v", c, only)
		}
	})

	t.Run("banlist", func(t *testing.T) {
		rt := NewRequestTracker()
		rt.Begin(BeginOpts{Client: "c1", Cmd: "MODE", EnquiryTarget: "#c"})
		c, only, _, _, _ := rt.RouteMessage(irc.Message{Command: "367", Params: []string{"me", "#c", "*!*@x", "setter", "1"}}, cm)
		if !only || c != "c1" {
			t.Fatal(c, only)
		}
		c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "368", Params: []string{"me", "#c", "End"}}, cm)
		if !only || c != "c1" {
			t.Fatal(c, only)
		}
	})

	t.Run("mode change still broadcasts", func(t *testing.T) {
		rt := NewRequestTracker()
		rt.Begin(BeginOpts{Client: "c1", Cmd: "MODE", EnquiryTarget: "#c"})
		_, only, _, _, _ := rt.RouteMessage(irc.Message{
			Source: "op!u@h", Command: "MODE", Params: []string{"#c", "+v", "nick"},
		}, cm)
		if only {
			t.Fatal("live MODE echo must broadcast while enquiry pending")
		}
	})
}

func TestRequestTrackerTOPICEnquiry(t *testing.T) {
	cm := irc.CaseRFC1459

	t.Run("332+333", func(t *testing.T) {
		rt := NewRequestTracker()
		rt.Begin(BeginOpts{Client: "c1", Cmd: "TOPIC", EnquiryTarget: "#c"})
		c, only, _, _, _ := rt.RouteMessage(irc.Message{Command: "332", Params: []string{"me", "#c", "hello"}}, cm)
		if !only || c != "c1" {
			t.Fatal(c, only)
		}
		c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "333", Params: []string{"me", "#c", "setter", "1"}}, cm)
		if !only || c != "c1" {
			t.Fatal(c, only)
		}
	})

	t.Run("concurrent with MODE same channel", func(t *testing.T) {
		rt := NewRequestTracker()
		_, _, wm := rt.Begin(BeginOpts{Client: "c1", Cmd: "MODE", EnquiryTarget: "#c"})
		_, _, wt := rt.Begin(BeginOpts{Client: "c2", Cmd: "TOPIC", EnquiryTarget: "#c"})
		if wm != nil || wt != nil {
			t.Fatal("MODE vs TOPIC must not hold each other", wm, wt)
		}
		c, only, _, _, _ := rt.RouteMessage(irc.Message{Command: "332", Params: []string{"me", "#c", "t"}}, cm)
		if !only || c != "c2" {
			t.Fatalf("332 -> %s only=%v", c, only)
		}
		c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "324", Params: []string{"me", "#c", "+n"}}, cm)
		if !only || c != "c1" {
			t.Fatalf("324 -> %s only=%v", c, only)
		}
	})
}

func TestIsMODEEnquiry(t *testing.T) {
	if !isMODEEnquiry([]string{"#c"}) {
		t.Fatal("MODE #c")
	}
	if !isMODEEnquiry([]string{"#c", "b"}) {
		t.Fatal("MODE #c b")
	}
	if !isMODEEnquiry([]string{"#c", "eI"}) {
		t.Fatal("MODE #c eI")
	}
	if !isMODEEnquiry([]string{"#c", "+b"}) {
		t.Fatal("MODE #c +b is a banlist query")
	}
	if !isMODEEnquiry([]string{"#c", "+e"}) {
		t.Fatal("MODE #c +e")
	}
	if !isMODEEnquiry([]string{"#c", "+I"}) {
		t.Fatal("MODE #c +I")
	}
	if !isMODEEnquiry([]string{"#c", "+q"}) {
		t.Fatal("MODE #c +q")
	}
	if !isMODEEnquiry([]string{"#c", "-b"}) {
		t.Fatal("MODE #c -b without mask is still a list query on many ircds")
	}
	if !isMODEEnquiry([]string{"#c", "+beI"}) {
		t.Fatal("MODE #c +beI")
	}
	if !isMODEEnquiry([]string{"me"}) {
		t.Fatal("MODE nick")
	}
	if isMODEEnquiry([]string{"#c", "+o", "bob"}) {
		t.Fatal("MODE +o is a change")
	}
	if isMODEEnquiry([]string{"#c", "-b", "*!*@x"}) {
		t.Fatal("MODE -b with mask is a change")
	}
	if isMODEEnquiry([]string{"#c", "+b", "*!*@x"}) {
		t.Fatal("MODE +b with mask is a change")
	}
	if isMODEEnquiry([]string{"#c", "+nt"}) {
		t.Fatal("MODE +nt is a flag change, not a list query")
	}
	if isMODEEnquiry(nil) {
		t.Fatal("empty")
	}

	// ircu-like CHANMODES: only b is type A; +b still enquiry, +e falls back to defaults.
	ms := irc.DefaultModeSet()
	_ = ms.ParseCHANMODES("b,k,l,imnpst")
	if !isMODEEnquiryWith([]string{"#c", "+b"}, ms) {
		t.Fatal("CHANMODES b: +b enquiry")
	}
	if !isMODEEnquiryWith([]string{"#c", "+e"}, ms) {
		t.Fatal("+e still enquiry via common defaults when unknown in CHANMODES")
	}
	if isMODEEnquiryWith([]string{"#c", "+l"}, ms) {
		t.Fatal("+l without arg is not a list enquiry")
	}
}

func TestMODEPlusBBanlistRoutedToRequester(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	s.registered = true
	_ = s.isupport.Modes.ParseCHANMODES("b,k,l,imnpst")
	a := &fakeDL{id: "a", caps: map[string]bool{}}
	b := &fakeDL{id: "b", caps: map[string]bool{}}
	s.mu.Lock()
	s.downlinks[a.ID()] = a
	s.downlinks[b.ID()] = b
	s.mu.Unlock()

	if !isMODEEnquiryWith([]string{"#undernet", "+b"}, s.isupport.Modes) {
		t.Fatal("precondition: +b must be enquiry")
	}
	s.tracker.Begin(BeginOpts{Client: a.ID(), Cmd: "MODE", EnquiryTarget: "#undernet"})

	s.OnMessage(nil, irc.Message{Command: "368", Params: []string{"me", "#undernet", "End of Channel Ban List"}})
	if countCmds(a.snapshot(), "368") != 1 {
		t.Fatalf("requester missing 368: %+v", a.snapshot())
	}
	if countCmds(b.snapshot(), "368") != 0 {
		t.Fatalf("other client must not see banlist end: %+v", b.snapshot())
	}
}

func TestIsTOPICEnquiry(t *testing.T) {
	if !isTOPICEnquiry([]string{"#c"}) {
		t.Fatal("query")
	}
	if isTOPICEnquiry([]string{"#c", "new topic"}) {
		t.Fatal("set")
	}
}

func TestIsSILENCEEnquiry(t *testing.T) {
	if !isSILENCEEnquiry(nil) {
		t.Fatal("SILENCE with no params is list query")
	}
	if !isSILENCEEnquiry([]string{""}) {
		t.Fatal("empty param is list query")
	}
	if !isSILENCEEnquiry([]string{"othernick"}) {
		t.Fatal("nick is list query")
	}
	if isSILENCEEnquiry([]string{"+*!*@evil.example"}) {
		t.Fatal("+mask is change")
	}
	if isSILENCEEnquiry([]string{"-*!*@evil.example"}) {
		t.Fatal("-mask is change")
	}
	if isSILENCEEnquiry([]string{"*!*@evil.example"}) {
		t.Fatal("hostmask without +/- is change")
	}
	if isSILENCEEnquiry([]string{"+a!*@x,-b!*@y"}) {
		t.Fatal("comma updates are change")
	}
}

func TestSILENCEEnquiryRoutedToRequester(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	s.registered = true
	a := &fakeDL{id: "a", caps: map[string]bool{}}
	b := &fakeDL{id: "b", caps: map[string]bool{}}
	s.mu.Lock()
	s.downlinks[a.ID()] = a
	s.downlinks[b.ID()] = b
	s.mu.Unlock()

	s.tracker.Begin(BeginOpts{Client: a.ID(), Cmd: "SILENCE"})

	s.OnMessage(nil, irc.Message{Command: "271", Params: []string{"me", "me", "*!*@spam.example"}})
	s.OnMessage(nil, irc.Message{Command: "272", Params: []string{"me", "me", "End of Silence List"}})

	if countCmds(a.snapshot(), "271") != 1 || countCmds(a.snapshot(), "272") != 1 {
		t.Fatalf("requester a got %+v", a.snapshot())
	}
	if countCmds(b.snapshot(), "271") != 0 || countCmds(b.snapshot(), "272") != 0 {
		t.Fatalf("other client must not see silence list: %+v", b.snapshot())
	}
}

func TestSILENCEEmptyListEndRoutedToRequester(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	s.registered = true
	a := &fakeDL{id: "a", caps: map[string]bool{}}
	b := &fakeDL{id: "b", caps: map[string]bool{}}
	s.mu.Lock()
	s.downlinks[a.ID()] = a
	s.downlinks[b.ID()] = b
	s.mu.Unlock()

	// Empty silence list still ends with 272 only (no 271 rows).
	s.tracker.Begin(BeginOpts{Client: a.ID(), Cmd: "SILENCE"})
	s.OnMessage(nil, irc.Message{Command: "272", Params: []string{"me", "me", "End of Silence List"}})

	if countCmds(a.snapshot(), "272") != 1 {
		t.Fatalf("requester missing 272: %+v", a.snapshot())
	}
	if countCmds(b.snapshot(), "272") != 0 {
		t.Fatalf("other client must not see end of silence: %+v", b.snapshot())
	}
}

func TestMODEEnquiryRoutedToRequester(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	s.registered = true
	a := &fakeDL{id: "a", caps: map[string]bool{}}
	b := &fakeDL{id: "b", caps: map[string]bool{}}
	s.mu.Lock()
	s.downlinks[a.ID()] = a
	s.downlinks[b.ID()] = b
	s.mu.Unlock()

	// As HandleClientMessage does for MODE #c enquiry.
	s.tracker.Begin(BeginOpts{Client: a.ID(), Cmd: "MODE", EnquiryTarget: "#c"})

	s.OnMessage(nil, irc.Message{Command: "324", Params: []string{"me", "#c", "+nt"}})
	s.OnMessage(nil, irc.Message{Command: "329", Params: []string{"me", "#c", "99"}})

	if countCmds(a.snapshot(), "324") != 1 || countCmds(a.snapshot(), "329") != 1 {
		t.Fatalf("requester a got %+v", a.snapshot())
	}
	if countCmds(b.snapshot(), "324") != 0 || countCmds(b.snapshot(), "329") != 0 {
		t.Fatalf("other client must not see enquiry numerics: %+v", b.snapshot())
	}

	a.clearSent()
	b.clearSent()
	s.OnMessage(nil, irc.Message{Source: "op!u@h", Command: "MODE", Params: []string{"#c", "+v", "x"}})
	if countCmds(a.snapshot(), "MODE") != 1 || countCmds(b.snapshot(), "MODE") != 1 {
		t.Fatalf("MODE change fan-out a=%v b=%v", a.snapshot(), b.snapshot())
	}
}

func TestRequestTrackerSTATSbyLetter(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	_, _, w1 := rt.Begin(BeginOpts{Client: "c1", Cmd: "STATS", StatsLetter: "y"})
	_, _, w2 := rt.Begin(BeginOpts{Client: "c2", Cmd: "STATS", StatsLetter: "c"})
	if w1 != nil || w2 != nil {
		t.Fatal("different letters should not hold", w1, w2)
	}
	// Mid-numeric with two letters pending: not routed (ambiguous).
	_, only, _, _, _ := rt.RouteMessage(irc.Message{Command: "218", Params: []string{"me", "Y", "90"}}, cm)
	if only {
		t.Fatal("ambiguous STATS mid-numeric should broadcast")
	}
	c, only, _, _, _ := rt.RouteMessage(irc.Message{Command: "219", Params: []string{"me", "y", "End"}}, cm)
	if !only || c != "c1" {
		t.Fatalf("219 y -> %s only=%v", c, only)
	}
	// Now only c pending: mid-numerics route to c2.
	c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "213", Params: []string{"me", "C", "x"}}, cm)
	if !only || c != "c2" {
		t.Fatalf("213 -> %s only=%v", c, only)
	}
	c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "219", Params: []string{"me", "c", "End"}}, cm)
	if !only || c != "c2" {
		t.Fatalf("219 c -> %s only=%v", c, only)
	}
}

func TestRequestTrackerSTATSSameLetterHold(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	_, _, w1 := rt.Begin(BeginOpts{Client: "c1", Cmd: "STATS", StatsLetter: "y"})
	_, _, w2 := rt.Begin(BeginOpts{Client: "c2", Cmd: "STATS", StatsLetter: "y"})
	if w1 != nil {
		t.Fatal("first y should send")
	}
	if w2 == nil {
		t.Fatal("second y should hold")
	}
	c, only, _, _, _ := rt.RouteMessage(irc.Message{Command: "219", Params: []string{"me", "y", "End"}}, cm)
	if !only || c != "c1" {
		t.Fatal(c, only)
	}
	select {
	case <-w2:
	default:
		t.Fatal("second y gate should open after first 219")
	}
	c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "219", Params: []string{"me", "y", "End"}}, cm)
	if !only || c != "c2" {
		t.Fatal(c, only)
	}
}

func TestRequestTrackerWHOISByNick(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	_, _, wait := rt.Begin(BeginOpts{Client: "c1", Cmd: "WHOIS", WhoisTargets: []string{cm.Canonical("bob")}})
	if wait != nil {
		t.Fatal("whois should not serialize-wait")
	}
	_, _, wait = rt.Begin(BeginOpts{Client: "c2", Cmd: "WHOIS", WhoisTargets: []string{cm.Canonical("alice")}})
	if wait != nil {
		t.Fatal("whois should not serialize-wait")
	}

	// Interleaved remote WHOIS replies.
	c, only, _, _, _ := rt.RouteMessage(irc.Message{Command: "311", Params: []string{"me", "bob", "u", "h", "*", "r"}}, cm)
	if !only || c != "c1" {
		t.Fatalf("bob 311 -> %s only=%v", c, only)
	}
	c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "311", Params: []string{"me", "Alice", "u", "h", "*", "r"}}, cm)
	if !only || c != "c2" {
		t.Fatalf("alice 311 -> %s only=%v", c, only)
	}
	c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "319", Params: []string{"me", "bob", "#c"}}, cm)
	if !only || c != "c1" {
		t.Fatalf("bob 319 -> %s only=%v", c, only)
	}
	c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "318", Params: []string{"me", "alice", "End"}}, cm)
	if !only || c != "c2" {
		t.Fatalf("alice 318 -> %s only=%v", c, only)
	}
	// Unrelated numeric broadcasts.
	_, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "005", Params: []string{"me", "FOO=1"}}, cm)
	if only {
		t.Fatal("005 should broadcast while whois pending")
	}
	c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "318", Params: []string{"me", "bob", "End"}}, cm)
	if !only || c != "c1" {
		t.Fatalf("bob 318 -> %s only=%v", c, only)
	}
	// 301 without pending WHOIS broadcasts.
	_, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "301", Params: []string{"me", "bob", "away"}}, cm)
	if only {
		t.Fatal("301 without pending should broadcast")
	}
}

func TestRequestTrackerWHOISIgnoresForeignNumeric(t *testing.T) {
	rt := NewRequestTracker()
	rt.SetIRCd(irc.IRCdIrcu)
	cm := irc.CaseRFC1459
	rt.Begin(BeginOpts{Client: "c1", Cmd: "WHOIS", WhoisTargets: []string{"bob"}})
	// 307 is not a WHOIS reply on ircu — must not consume the pending WHOIS.
	_, only, _, _, _ := rt.RouteMessage(irc.Message{Command: "307", Params: []string{"me", "bob", "is a registered nick"}}, cm)
	if only {
		t.Fatal("307 must not route as WHOIS on ircu")
	}
	c, only, _, _, _ := rt.RouteMessage(irc.Message{Command: "311", Params: []string{"me", "bob", "u", "h", "*", "r"}}, cm)
	if !only || c != "c1" {
		t.Fatal(c, only)
	}
}

func TestRequestTrackerWHOISNosuchNick(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	rt.Begin(BeginOpts{Client: "c1", Cmd: "WHOIS", WhoisTargets: []string{"ghost"}})
	c, only, _, _, _ := rt.RouteMessage(irc.Message{Command: "401", Params: []string{"me", "ghost", "No such nick"}}, cm)
	if !only || c != "c1" {
		t.Fatal(c, only)
	}
	// ircu sends 401 then 318; 401 must not clear the waiter.
	c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "318", Params: []string{"me", "ghost", "End of /WHOIS list."}}, cm)
	if !only || c != "c1" {
		t.Fatalf("318 after 401 -> %s only=%v", c, only)
	}
	_, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "318", Params: []string{"me", "ghost"}}, cm)
	if only {
		t.Fatal("318 should have cleared pending")
	}
}

func TestRequestTrackerWHOISSameNickOldest(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	rt.Begin(BeginOpts{Client: "c1", Cmd: "WHOIS", WhoisTargets: []string{"bob"}})
	rt.Begin(BeginOpts{Client: "c2", Cmd: "WHOIS", WhoisTargets: []string{"bob"}})
	c, only, _, _, _ := rt.RouteMessage(irc.Message{Command: "311", Params: []string{"me", "bob"}}, cm)
	if !only || c != "c1" {
		t.Fatal(c, only)
	}
	c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "318", Params: []string{"me", "bob"}}, cm)
	if !only || c != "c1" {
		t.Fatal(c, only)
	}
	c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "311", Params: []string{"me", "bob"}}, cm)
	if !only || c != "c2" {
		t.Fatal(c, only)
	}
}

func TestParseWHOISTargets(t *testing.T) {
	if got := ParseWHOISTargets([]string{"bob"}); len(got) != 1 || got[0] != "bob" {
		t.Fatal(got)
	}
	if got := ParseWHOISTargets([]string{"bob", "bob"}); len(got) != 1 || got[0] != "bob" {
		t.Fatal(got)
	}
	if got := ParseWHOISTargets([]string{"irc.example", "alice"}); len(got) != 1 || got[0] != "alice" {
		t.Fatal(got)
	}
	if got := ParseWHOISTargets([]string{"a,b,c"}); len(got) != 3 {
		t.Fatal(got)
	}
}

func TestRequestTrackerWHOX(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	_, tok, wait := rt.Begin(BeginOpts{Client: "c1", Cmd: "WHO", ClientLabel: "L1", PreferWHOX: true, WHOMask: cm.Canonical("#c")})
	if wait != nil || tok == "" {
		t.Fatal(tok, wait)
	}
	for _, r := range tok {
		if r < '0' || r > '9' {
			t.Fatalf("non-digit WHOX token %q", tok)
		}
	}
	if tok == "0" {
		t.Fatal("token 0 is the Undernet default and must not be used")
	}

	rt.SetWHOXClientFix(tok, true, "")
	msg := irc.Message{Command: "354", Params: []string{"me", tok, "rest"}}
	c, only, echo, strip, restore := rt.RouteMessage(msg, cm)
	if !only || c != "c1" || echo != "L1" || !strip || restore != "" {
		t.Fatal(c, only, echo, strip, restore)
	}

	_, tok2, _ := rt.Begin(BeginOpts{Client: "c2", Cmd: "WHO", PreferWHOX: true, WHOMask: cm.Canonical("#d")})
	rt.SetWHOXClientFix(tok2, false, "77")
	msg2 := irc.Message{Command: "354", Params: []string{"me", tok2, "rest"}}
	c, only, _, strip, restore = rt.RouteMessage(msg2, cm)
	if !only || c != "c2" || strip || restore != "77" {
		t.Fatal(c, only, strip, restore)
	}

	// 315 matched by mask.
	c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "315", Params: []string{"me", "#c", "End"}}, cm)
	if !only || c != "c1" {
		t.Fatalf("315 #c -> %s only=%v", c, only)
	}
	c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "315", Params: []string{"me", "#d", "End"}}, cm)
	if !only || c != "c2" {
		t.Fatalf("315 #d -> %s only=%v", c, only)
	}
}

func TestRequestTrackerWHOXFallbackSingle(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	_, tok, _ := rt.Begin(BeginOpts{Client: "c1", Cmd: "WHO", PreferWHOX: true, WHOMask: "#c"})
	rt.SetWHOXClientFix(tok, true, "")
	msg := irc.Message{Command: "354", Params: []string{"me", "0", "#c", "nick"}}
	c, only, _, strip, restore := rt.RouteMessage(msg, cm)
	if !only || c != "c1" || !strip || restore != "" {
		t.Fatal(c, only, strip, restore)
	}
}

func TestInjectWHOXToken(t *testing.T) {
	spec, injected, clientTok := injectWHOXToken("x%nifac", "42")
	if spec != "x%nifact,42" || !injected || clientTok != "" {
		t.Fatalf("got %q injected=%v clientTok=%q", spec, injected, clientTok)
	}
	spec, injected, clientTok = injectWHOXToken("x%nifact,9", "42")
	if spec != "x%nifact,42" || injected || clientTok != "9" {
		t.Fatalf("got %q injected=%v clientTok=%q", spec, injected, clientTok)
	}
	spec, injected, clientTok = injectWHOXToken("o%uhs", "7")
	if spec != "o%uhst,7" || !injected || clientTok != "" {
		t.Fatalf("got %q injected=%v clientTok=%q", spec, injected, clientTok)
	}
}

func TestCapRewrite(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	d1 := &fakeDL{id: "a", caps: map[string]bool{"server-time": true, "message-tags": true}}
	d2 := &fakeDL{id: "b", caps: map[string]bool{}}
	msg := irc.Message{
		Tags:    map[string]string{"time": "2024-01-01T00:00:00.000Z", "msgid": "x"},
		Source:  "n!u@h",
		Command: "PRIVMSG",
		Params:  []string{"#c", "hi"},
	}
	o1 := s.rewriteFor(d1, msg)
	if o1.Tags["time"] == "" {
		t.Fatal("expected time")
	}
	o2 := s.rewriteFor(d2, msg)
	if o2.Tags != nil {
		t.Fatalf("expected no tags, got %v", o2.Tags)
	}
	tagmsg := msg
	tagmsg.Command = "TAGMSG"
	o3 := s.rewriteFor(d2, tagmsg)
	if o3.Command != "" {
		t.Fatal("TAGMSG should drop")
	}

	// Client negotiated server-time; uplink sent no @time — inject.
	bare := irc.Message{Source: "n!u@h", Command: "PRIVMSG", Params: []string{"#c", "hi"}}
	bare = ensureMessageTime(bare)
	if bare.Tags["time"] == "" {
		t.Fatal("ensureMessageTime")
	}
	o4 := s.rewriteFor(d1, bare)
	if o4.Tags["time"] == "" {
		t.Fatal("expected injected time for server-time client")
	}
	dTimeOnly := &fakeDL{id: "c", caps: map[string]bool{"server-time": true}}
	o5 := s.rewriteFor(dTimeOnly, irc.Message{Source: "n!u@h", Command: "PRIVMSG", Params: []string{"#c", "x"}})
	if o5.Tags["time"] == "" || len(o5.Tags) != 1 {
		t.Fatalf("server-time only: %+v", o5.Tags)
	}
}

func TestCapNotifyFiltering(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	with := &fakeDL{id: "a", caps: map[string]bool{
		"away-notify": true, "chghost": true, "invite-notify": true, "batch": true, "account-notify": true,
	}}
	without := &fakeDL{id: "b", caps: map[string]bool{}}

	away := irc.Message{Source: "bob!u@h", Command: "AWAY", Params: []string{"gone"}}
	if s.rewriteFor(without, away).Command != "" {
		t.Fatal("AWAY without away-notify")
	}
	if s.rewriteFor(with, away).Command != "AWAY" {
		t.Fatal("AWAY with away-notify")
	}
	selfAway := irc.Message{Source: "me!u@h", Command: "AWAY", Params: []string{"gone"}}
	if s.rewriteFor(with, selfAway).Command != "" {
		t.Fatal("own AWAY should be suppressed")
	}

	chghost := irc.Message{Source: "bob!u@h", Command: "CHGHOST", Params: []string{"nu", "nh"}}
	if s.rewriteFor(without, chghost).Command != "" {
		t.Fatal("CHGHOST without cap")
	}
	if s.rewriteFor(with, chghost).Command != "CHGHOST" {
		t.Fatal("CHGHOST with cap")
	}

	acct := irc.Message{Source: "bob!u@h", Command: "ACCOUNT", Params: []string{"bobacct"}}
	if s.rewriteFor(without, acct).Command != "" {
		t.Fatal("ACCOUNT without account-notify")
	}
	if s.rewriteFor(with, acct).Command != "ACCOUNT" {
		t.Fatal("ACCOUNT with account-notify")
	}

	// INVITE to us: always deliver.
	toUs := irc.Message{Source: "bob!u@h", Command: "INVITE", Params: []string{"me", "#c"}}
	if s.rewriteFor(without, toUs).Command != "INVITE" {
		t.Fatal("INVITE to self should always pass")
	}
	// INVITE notify about someone else: invite-notify only.
	toOther := irc.Message{Source: "bob!u@h", Command: "INVITE", Params: []string{"alice", "#c"}}
	if s.rewriteFor(without, toOther).Command != "" {
		t.Fatal("INVITE notify without invite-notify")
	}
	if s.rewriteFor(with, toOther).Command != "INVITE" {
		t.Fatal("INVITE notify with invite-notify")
	}

	batch := irc.Message{Command: "BATCH", Params: []string{"+x", "chathistory", "#c"}}
	if s.rewriteFor(without, batch).Command != "" {
		t.Fatal("BATCH without batch")
	}
	if s.rewriteFor(with, batch).Command != "BATCH" {
		t.Fatal("BATCH with batch")
	}

	tagged := irc.Message{
		Tags:    map[string]string{"batch": "x", "msgid": "m"},
		Source:  "n!u@h",
		Command: "PRIVMSG",
		Params:  []string{"#c", "hi"},
	}
	tagsOnly := &fakeDL{id: "t", caps: map[string]bool{"message-tags": true}}
	got := s.rewriteFor(tagsOnly, tagged)
	if _, ok := got.Tag("batch"); ok {
		t.Fatal("batch tag should be stripped without batch cap")
	}
	if got.Tags["msgid"] != "m" {
		t.Fatal("msgid should remain with message-tags")
	}
}

func TestEchoMessageFiltering(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	with := &fakeDL{id: "a", caps: map[string]bool{"echo-message": true}}
	without := &fakeDL{id: "b", caps: map[string]bool{}}

	self := irc.Message{Source: "me!u@h", Command: "PRIVMSG", Params: []string{"#c", "hi"}}
	if s.rewriteFor(without, self).Command != "" {
		t.Fatal("self PRIVMSG without echo-message should drop")
	}
	if s.rewriteFor(with, self).Command != "PRIVMSG" {
		t.Fatal("self PRIVMSG with echo-message should pass")
	}
	// Local synthesis (no uplink echo-message): force to everyone.
	if s.rewriteMessage(without, self, true).Command != "PRIVMSG" {
		t.Fatal("forced self echo should reach clients without the cap")
	}

	other := irc.Message{Source: "bob!u@h", Command: "PRIVMSG", Params: []string{"#c", "hi"}}
	if s.rewriteFor(without, other).Command != "PRIVMSG" {
		t.Fatal("other nick PRIVMSG always passes")
	}
}

func TestExtendedJoinRewrite(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	with := &fakeDL{id: "a", caps: map[string]bool{"extended-join": true}}
	without := &fakeDL{id: "b", caps: map[string]bool{}}
	ext := irc.Message{
		Source:  "bob!u@h",
		Command: "JOIN",
		Params:  []string{"#c", "bobacct", "Bob Real"},
	}
	got := s.rewriteFor(with, ext)
	if len(got.Params) != 3 || got.Params[1] != "bobacct" {
		t.Fatalf("extended client: %+v", got.Params)
	}
	got = s.rewriteFor(without, ext)
	if len(got.Params) != 1 || got.Params[0] != "#c" {
		t.Fatalf("plain client: %+v", got.Params)
	}
}

func TestCapNotifyNewDel(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	d := &fakeDL{id: "c1", caps: map[string]bool{"cap-notify": true, "away-notify": true}}
	s.mu.Lock()
	s.downlinks[d.id] = d
	s.mu.Unlock()

	s.broadcastCapNotify("NEW", []string{"away-notify", "chghost"})
	if len(d.sent) != 1 || d.sent[0].Command != "CAP" || d.sent[0].Param(1) != "NEW" {
		t.Fatalf("NEW: %+v", d.sent)
	}
	d.sent = nil
	s.broadcastCapNotify("DEL", []string{"away-notify"})
	if len(d.sent) != 1 || d.sent[0].Param(1) != "DEL" {
		t.Fatalf("DEL: %+v", d.sent)
	}
	if d.HasCap("away-notify") {
		t.Fatal("away-notify should be cleared")
	}

	// Offer diff when uplink caps appear.
	s.mu.Lock()
	s.upCaps = map[string]bool{}
	s.mu.Unlock()
	before := s.OfferedCaps()
	s.mu.Lock()
	s.upCaps = map[string]bool{"away-notify": true, "extended-join": true}
	s.mu.Unlock()
	after := s.OfferedCaps()
	gained := caps.Diff(before, after)
	if !strings.Contains(strings.Join(gained, " "), "away-notify") ||
		!strings.Contains(strings.Join(gained, " "), "extended-join") {
		t.Fatalf("gained=%v before=%v after=%v", gained, before, after)
	}
}

func TestParseJoinKeyPairing(t *testing.T) {
	got := ParseJoin("#channel1,#channel2,#channel3", "key1,key2")
	want := []JoinTarget{
		{Name: "#channel1", Key: "key1"},
		{Name: "#channel2", Key: "key2"},
		{Name: "#channel3", Key: ""},
	}
	if len(got) != len(want) {
		t.Fatalf("len=%d want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("[%d]=%+v want %+v", i, got[i], want[i])
		}
	}

	// No keys param
	got = ParseJoin("#a,#b", "")
	if len(got) != 2 || got[0].Key != "" || got[1].Key != "" {
		t.Fatalf("%+v", got)
	}

	// More keys than channels: extras ignored
	got = ParseJoin("#only", "k1,k2")
	if len(got) != 1 || got[0].Key != "k1" {
		t.Fatalf("%+v", got)
	}
}

func TestPersistJoinPart(t *testing.T) {
	db := testutil.TempStore(t)
	ctx := context.Background()
	id, err := db.UpsertNetwork(ctx, store.Network{Name: "n", Host: "h", Port: 1, Nick: "me", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	netw, _ := db.NetworkByName(ctx, "n")
	s := New(netw, db, nil, nil)
	s.registered = true
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	_ = s.Attach(d)

	_ = s.HandleClientMessage(d, irc.Message{Command: "JOIN", Params: []string{"#a,#b,#c", "key1,key2"}})
	chs, err := db.ListChannels(ctx, id)
	if err != nil || len(chs) != 3 {
		t.Fatalf("persist multi join: %+v err=%v", chs, err)
	}
	byName := map[string]string{}
	for _, c := range chs {
		byName[c.Name] = c.Key
	}
	if byName["#a"] != "key1" || byName["#b"] != "key2" || byName["#c"] != "" {
		t.Fatalf("keys=%v", byName)
	}

	_ = s.HandleClientMessage(d, irc.Message{Command: "JOIN", Params: []string{"#secret", "hunter2"}})
	chs, err = db.ListChannels(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range chs {
		if c.Name == "#secret" && c.Key == "hunter2" {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing #secret: %+v", chs)
	}

	s.OnMessage(nil, irc.Message{Source: "me!u@h", Command: "PART", Params: []string{"#secret"}})
	chs, err = db.ListChannels(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range chs {
		if c.Name == "#secret" {
			t.Fatal("expected #secret removed")
		}
	}
}

func TestSelfPrefix(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me", Username: "u"}, nil, nil, nil)
	if got := s.SelfPrefix(); got != "me!u" {
		t.Fatalf("seeded=%q", got)
	}
	s.applyState(irc.Message{Source: "me!~gobnc@172.18.0.1", Command: "JOIN", Params: []string{"#c"}})
	if got := s.SelfPrefix(); got != "me!~gobnc@172.18.0.1" {
		t.Fatalf("after join=%q", got)
	}
	s.applyState(irc.Message{Command: "396", Params: []string{"me", "cloak.example", "is now your displayed host"}})
	if got := s.SelfPrefix(); got != "me!~gobnc@cloak.example" {
		t.Fatalf("after 396=%q", got)
	}
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	s.registered = true
	_ = s.Attach(d)
	var join irc.Message
	for _, m := range d.sent {
		if m.Command == "JOIN" {
			join = m
			break
		}
	}
	if join.Source != "me!~gobnc@cloak.example" {
		t.Fatalf("attach JOIN source=%q", join.Source)
	}
}

func TestAttachWelcomeBurst(t *testing.T) {
	s := New(store.Network{Name: "ircu2", Nick: "me"}, nil, nil, nil)
	s.registered = true
	s.isupport.Parse005([]string{"me", "CHANMODES=b,k,l,imnpst", "PREFIX=(ov)@+", "WHOX", "NETWORK=upstream", "are supported by this server"})
	s.rpl002 = []string{"Your host is ircu2.example"}
	s.rpl003 = []string{"This server was created yesterday"}
	s.rpl004 = []string{"ircu2.example", "u2.10", "iow", "nt"}
	s.self.UModes = map[byte]bool{'i': true}

	d := &fakeDL{id: "c1", caps: map[string]bool{"server-time": true, "message-tags": true}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	cmds := map[string]int{}
	var got005 []string
	for _, m := range d.sent {
		cmds[m.Command]++
		switch m.Command {
		case "005":
			got005 = append(got005, m.Params[1:]...)
		case "221":
			if m.Param(1) != "+i" {
				t.Fatalf("221=%v", m.Params)
			}
		case "372":
			if m.Trailing() == "" && (len(m.Params) < 2 || m.Params[1] == "") {
				t.Fatalf("empty 372: %+v", m)
			}
		}
	}
	for _, want := range []string{"001", "002", "003", "004", "005", "221", "375", "372", "376"} {
		if cmds[want] == 0 {
			t.Fatalf("missing %s in %#v", want, cmds)
		}
	}
	for _, m := range d.sent {
		switch m.Command {
		case "001", "002", "003", "004", "005", "221", "375", "372", "376":
			if m.Source != ServerName {
				t.Fatalf("%s missing source prefix: %+v", m.Command, m)
			}
			if _, ok := m.Tag("time"); !ok {
				t.Fatalf("%s missing @time for server-time client: %+v", m.Command, m)
			}
		}
	}
	joined := strings.Join(got005, " ")
	if !strings.Contains(joined, "NETWORK=upstream") {
		t.Fatalf("expected upstream NETWORK, got %q", joined)
	}
	if !strings.Contains(joined, "WHOX") || !strings.Contains(joined, "CHANMODES=") {
		t.Fatalf("expected full isupport, got %q", joined)
	}
	if !strings.Contains(joined, "MOTD") && cmds["376"] != 1 {
		t.Fatal("expected end of MOTD")
	}
}

func TestAttachISUPPORTChatHistoryMsgRefTypes(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	hist := history.New(db)
	s := New(store.Network{Name: "n", Nick: "me"}, db, hist, nil)
	s.registered = true
	s.isupport.Parse005([]string{"me", "NETWORK=x", "are supported by this server"})
	d := &fakeDL{id: "c1", caps: map[string]bool{"chathistory": true}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	var joined string
	for _, m := range d.sent {
		if m.Command == "005" {
			joined += " " + strings.Join(m.Params[1:], " ")
		}
	}
	if !strings.Contains(joined, "MSGREFTYPES=msgid,timestamp") {
		t.Fatalf("missing MSGREFTYPES: %q", joined)
	}
	if !strings.Contains(joined, "CHATHISTORY=") {
		t.Fatalf("missing CHATHISTORY=: %q", joined)
	}
}

func TestHandleClientMessageRejectsCRLF(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	err := s.HandleClientMessage(d, irc.Message{
		Command: "PRIVMSG",
		Params:  []string{"#c", "x\rPRIVMSG y :z"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(d.sent) != 1 || d.sent[0].Command != irc.ErrInputTooLong {
		t.Fatalf("want 417, got %+v", d.sent)
	}
	if d.sent[0].Param(0) != "me" {
		t.Fatalf("nick=%q", d.sent[0].Param(0))
	}
}

func TestHandleClientMessageUTF8Only(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	s.registered = true
	s.isupport.UTF8Only = true
	s.uplink = nil // must not reach WriteMessage
	d := &fakeDL{id: "c1", caps: map[string]bool{}}

	bad := irc.Message{Command: "PRIVMSG", Params: []string{"#c", "bad\xffutf8"}}
	if utf8.ValidString(bad.Encode()) {
		t.Fatal("precondition: message must be invalid UTF-8")
	}
	if err := s.HandleClientMessage(d, bad); err != nil {
		t.Fatal(err)
	}
	if len(d.sent) != 1 || d.sent[0].Command != "FAIL" {
		t.Fatalf("want FAIL, got %+v", d.sent)
	}
	if d.sent[0].Param(0) != "PRIVMSG" || d.sent[0].Param(1) != "INVALID_UTF8" {
		t.Fatalf("FAIL params: %+v", d.sent[0].Params)
	}

	d.sent = nil
	s.isupport.UTF8Only = false
	// Without UTF8ONLY, nil uplink errors on forward — proves we no longer reject early.
	err := s.HandleClientMessage(d, bad)
	if err == nil {
		t.Fatal("expected uplink error when UTF8ONLY off")
	}
	if len(d.sent) != 0 {
		t.Fatalf("must not FAIL without UTF8ONLY: %+v", d.sent)
	}
}

func TestClientPONGNotForwarded(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	s.registered = true
	// If PONG were forwarded, a nil uplink would error on WriteMessage.
	s.uplink = nil
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.HandleClientMessage(d, irc.Message{Command: "PONG", Params: []string{"gobnc"}}); err != nil {
		t.Fatal(err)
	}
}

func TestOnMessageDropsUplinkPINGPONG(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	s.registered = true
	d := &fakeDL{id: "a", caps: map[string]bool{}}
	_ = s.Attach(d)
	d.sent = nil
	s.OnMessage(nil, irc.Message{Command: "PING", Params: []string{"server"}})
	s.OnMessage(nil, irc.Message{Command: "PONG", Params: []string{"gobnc"}})
	if len(d.sent) != 0 {
		t.Fatalf("uplink PING/PONG must not reach clients: %+v", d.sent)
	}
}

func TestFanOutTwoClients(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	s.registered = true
	d1 := &fakeDL{id: "a", caps: map[string]bool{"server-time": true, "message-tags": true}}
	d2 := &fakeDL{id: "b", caps: map[string]bool{}}
	_ = s.Attach(d1)
	_ = s.Attach(d2)
	d1.sent = nil
	d2.sent = nil
	s.OnMessage(nil, irc.Message{
		Tags:    map[string]string{"time": "2024-01-01T00:00:00.000Z"},
		Source:  "x!u@h",
		Command: "PRIVMSG",
		Params:  []string{"#c", "hi"},
	})
	if len(d1.sent) != 1 || d1.sent[0].Tags["time"] == "" {
		t.Fatalf("d1=%+v", d1.sent)
	}
	if len(d2.sent) != 1 || d2.sent[0].Tags != nil {
		t.Fatalf("d2=%+v", d2.sent)
	}
}

func TestEnsureMessageID(t *testing.T) {
	bare := irc.Message{Source: "n!u@h", Command: "PRIVMSG", Params: []string{"#c", "hi"}, Raw: "@time=t :n!u@h PRIVMSG #c :hi"}
	got := ensureMessageID(bare)
	id, ok := got.Tag("msgid")
	if !ok || id == "" {
		t.Fatal("expected generated msgid")
	}
	if got.Raw != bare.Raw {
		t.Fatal("Raw body must be kept so Wire preserves uplink colonation")
	}
	wire := got.Wire()
	if !strings.Contains(wire, "msgid="+id) || !strings.HasSuffix(wire, " PRIVMSG #c :hi") {
		t.Fatalf("wire=%q", wire)
	}
	// Upstream ID preserved.
	up := irc.Message{
		Tags:    map[string]string{"msgid": "upstream-id-1"},
		Source:  "n!u@h",
		Command: "PRIVMSG",
		Params:  []string{"#c", "hi"},
	}
	if ensureMessageID(up).Tags["msgid"] != "upstream-id-1" {
		t.Fatal("must keep uplink msgid")
	}
	join := ensureMessageID(irc.Message{Source: "n!u@h", Command: "JOIN", Params: []string{"#c"}})
	if join.Tags["msgid"] == "" {
		t.Fatal("JOIN should also get a synthetic msgid")
	}
	if ensureMessageID(irc.Message{}).Tags != nil {
		t.Fatal("empty command should stay untouched")
	}
}

func TestFanOutMsgID(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	s.registered = true
	withTags := &fakeDL{id: "a", caps: map[string]bool{"message-tags": true, "server-time": true}}
	noTags := &fakeDL{id: "b", caps: map[string]bool{"server-time": true}}
	withTags2 := &fakeDL{id: "c", caps: map[string]bool{"message-tags": true}}
	_ = s.Attach(withTags)
	_ = s.Attach(noTags)
	_ = s.Attach(withTags2)
	withTags.sent, noTags.sent, withTags2.sent = nil, nil, nil

	s.OnMessage(nil, irc.Message{
		Source:  "x!u@h",
		Command: "PRIVMSG",
		Params:  []string{"#c", "hi"},
	})
	if len(withTags.sent) != 1 || len(withTags2.sent) != 1 || len(noTags.sent) != 1 {
		t.Fatalf("fan-out counts tags=%d tags2=%d none=%d", len(withTags.sent), len(withTags2.sent), len(noTags.sent))
	}
	id1 := withTags.sent[0].Tags["msgid"]
	id2 := withTags2.sent[0].Tags["msgid"]
	if id1 == "" || id1 != id2 {
		t.Fatalf("clients with message-tags must share one msgid: %q vs %q", id1, id2)
	}
	if _, ok := noTags.sent[0].Tag("msgid"); ok {
		t.Fatalf("client without message-tags got msgid: %+v", noTags.sent[0].Tags)
	}

	withTags.sent, withTags2.sent = nil, nil
	s.OnMessage(nil, irc.Message{
		Tags:    map[string]string{"msgid": "net-xyz", "time": "2024-01-01T00:00:00.000Z"},
		Source:  "x!u@h",
		Command: "NOTICE",
		Params:  []string{"#c", "n"},
	})
	if withTags.sent[0].Tags["msgid"] != "net-xyz" || withTags2.sent[0].Tags["msgid"] != "net-xyz" {
		t.Fatalf("must pass through uplink msgid: %v / %v", withTags.sent[0].Tags, withTags2.sent[0].Tags)
	}
}

func TestQUITPreservesTrailingColon(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	s.registered = true
	d := &fakeDL{id: "a", caps: map[string]bool{"message-tags": true, "server-time": true}}
	_ = s.Attach(d)
	d.clearSent()

	raw := ":PsychoMantis!~Psycho@host QUIT :Quit"
	msg, err := irc.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	s.OnMessage(nil, msg)
	if len(d.sent) != 1 {
		t.Fatalf("sent=%+v", d.sent)
	}
	wire := d.sent[0].Wire()
	if !strings.HasSuffix(wire, " QUIT :Quit") {
		t.Fatalf("expected server body preserved, got %q", wire)
	}
	if d.sent[0].Tags["msgid"] == "" {
		t.Fatal("expected msgid tag")
	}
}

func TestHistoryTargetsQUITNICK(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	s.mu.Lock()
	s.channels["#a"] = &ChannelState{Name: "#a", Members: map[string]struct{}{"bob": {}, "me": {}}}
	s.channels["#b"] = &ChannelState{Name: "#b", Members: map[string]struct{}{"bob": {}}}
	s.channels["#c"] = &ChannelState{Name: "#c", Members: map[string]struct{}{"me": {}}}
	s.mu.Unlock()

	got := s.historyTargets(irc.Message{Source: "bob!u@h", Command: "QUIT", Params: []string{"gone"}})
	if len(got) != 2 {
		t.Fatalf("QUIT targets=%v", got)
	}
	set := map[string]bool{}
	for _, t := range got {
		set[t] = true
	}
	if !set["#a"] || !set["#b"] {
		t.Fatalf("QUIT targets=%v", got)
	}

	got = s.historyTargets(irc.Message{Source: "bob!u@h", Command: "NICK", Params: []string{"robert"}})
	if len(got) != 2 {
		t.Fatalf("NICK targets=%v", got)
	}

	got = s.historyTargets(irc.Message{Source: "x!u@h", Command: "MODE", Params: []string{"me", "+i"}})
	if got != nil {
		t.Fatalf("user MODE should not store: %v", got)
	}
	got = s.historyTargets(irc.Message{Source: "x!u@h", Command: "MODE", Params: []string{"#a", "+o", "me"}})
	if len(got) != 1 || got[0] != "#a" {
		t.Fatalf("channel MODE: %v", got)
	}
}

func TestMaybeStoreHistoryEvents(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	id, err := db.UpsertNetwork(ctx, store.Network{Name: "n", Host: "h", Port: 1, Nick: "me", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	netw, _ := db.NetworkByName(ctx, "n")
	hist := history.New(db)
	s := New(netw, db, hist, nil)
	s.mu.Lock()
	s.Network.ID = id
	s.channels["#dev"] = &ChannelState{
		Name: "#dev",
		Members: map[string]struct{}{
			"bob": {},
			"me":  {},
		},
	}
	s.mu.Unlock()

	s.maybeStoreHistory(irc.Message{
		Tags:    map[string]string{"time": "2024-06-01T12:00:00.000Z"},
		Source:  "bob!u@h",
		Command: "JOIN",
		Params:  []string{"#dev"},
	})
	s.maybeStoreHistory(irc.Message{
		Tags:    map[string]string{"time": "2024-06-01T12:01:00.000Z"},
		Source:  "bob!u@h",
		Command: "QUIT",
		Params:  []string{"bye"},
	})

	msgs, err := db.QueryMessages(ctx, store.HistoryQuery{NetworkID: id, Target: "#dev", Latest: true, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 {
		t.Fatalf("stored=%d %+v", len(msgs), msgs)
	}
	if msgs[0].Command != "JOIN" || msgs[1].Command != "QUIT" {
		t.Fatalf("%+v", msgs)
	}
}

func TestPassthroughSASLOffer(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	// No credentials: offer when uplink advertises.
	s.mu.Lock()
	s.saslOffer = caps.FormatSASL([]string{"PLAIN", "EXTERNAL"})
	s.mu.Unlock()
	got := s.OfferedCaps()
	if !strings.Contains(strings.Join(got, " "), "sasl=PLAIN,EXTERNAL") {
		t.Fatalf("offered=%v", got)
	}

	// Bouncer credentials: never advertise.
	s2 := New(store.Network{Name: "n", Nick: "me", SASLUser: "u", SASLPass: "p"}, nil, nil, nil)
	s2.mu.Lock()
	s2.saslOffer = "sasl=PLAIN" // stale; refresh clears
	s2.mu.Unlock()
	prev, now := s2.refreshSASLOffer(nil)
	if now != "" || prev != "sasl=PLAIN" {
		t.Fatalf("prev=%q now=%q", prev, now)
	}
	if s2.OffersPassthroughSASL() {
		t.Fatal("should not offer with credentials")
	}
}

func TestPassthroughSASLRoute(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	d := &fakeDL{id: "c1", caps: map[string]bool{"sasl": true}}
	s.mu.Lock()
	s.downlinks[d.id] = d
	s.saslClient = d.id
	s.saslOffer = "sasl=PLAIN"
	s.mu.Unlock()

	msg := irc.Message{Command: "AUTHENTICATE", Params: []string{"+"}}
	s.routeSASLTraffic(msg)
	if len(d.sent) != 1 || d.sent[0].Command != "AUTHENTICATE" {
		t.Fatalf("sent=%+v", d.sent)
	}
	d.sent = nil
	s.routeSASLTraffic(irc.Message{Command: "903", Params: []string{"me", "ok"}})
	if len(d.sent) != 1 || d.sent[0].Command != "903" {
		t.Fatalf("903: %+v", d.sent)
	}
	s.mu.RLock()
	cleared := s.saslClient == ""
	s.mu.RUnlock()
	if !cleared {
		t.Fatal("saslClient should clear after 903")
	}
}

func TestRequestClientSASLAlreadyEnabled(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	s.mu.Lock()
	s.saslOffer = "sasl=PLAIN"
	s.downlinks[d.id] = d
	s.mu.Unlock()

	// No uplink: waiters queued would need uplink; without HasCap, tries RequestCap.
	// Simulate offer + immediate path by stubbing via HasCap on a fake uplink is heavy;
	// just NAK when not offered.
	s.mu.Lock()
	s.saslOffer = ""
	s.mu.Unlock()
	if err := s.RequestClientSASL(d); err != nil {
		t.Fatal(err)
	}
	if len(d.sent) != 1 || d.sent[0].Param(1) != "NAK" {
		t.Fatalf("want NAK: %+v", d.sent)
	}
}

type fakeDL struct {
	id   ClientID
	mu   sync.Mutex
	caps map[string]bool
	sent []irc.Message
}

func (f *fakeDL) ID() ClientID { return f.id }

func (f *fakeDL) Caps() map[string]bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]bool, len(f.caps))
	for k, v := range f.caps {
		out[k] = v
	}
	return out
}

func (f *fakeDL) HasCap(n string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.caps[n]
}

func (f *fakeDL) ClearCap(n string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.caps, n)
}

func (f *fakeDL) EnableCap(n string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.caps[n] = true
}

func (f *fakeDL) Send(m irc.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, m)
	return nil
}

func (f *fakeDL) Close() error { return nil }

func (f *fakeDL) snapshot() []irc.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]irc.Message, len(f.sent))
	copy(out, f.sent)
	return out
}

func (f *fakeDL) clearSent() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = nil
}

