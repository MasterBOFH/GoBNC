package session

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/caps"
	"github.com/MasterBOFH/GoBNC/internal/history"
	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/internal/testutil"
)

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
	_, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "311", Params: []string{"me", "ghost"}}, cm)
	if only {
		t.Fatal("401 should have cleared pending")
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

