package session

import (
	"strings"
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// These drive handleCAPLine/routeSASLTraffic directly against a
// registered Session with bouncer SASL configured, one per rule in
// bouncerOwnsSASLLocked's doc comment. The wire-level twin (real Driver,
// real AUTHENTICATE forwarding) is TestBouncerSASLFailureHandsSASLToClient.

func newBouncerSASLSession(t *testing.T) (*Session, *fakeDL) {
	t.Helper()
	s := New(store.Network{Name: "n", Nick: "me", SASL: true, SASLUser: "acct", SASLPass: "secret"}, nil, nil, nil, nil)
	s.registered = true
	d := &fakeDL{id: "c1", caps: map[string]bool{"cap-notify": true}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	d.clearSent()
	return s, d
}

func upLine(t *testing.T, s *Session, line string) {
	t.Helper()
	msg, err := irc.Parse(line)
	if err != nil {
		t.Fatal(err)
	}
	switch msg.Command {
	case "CAP":
		s.handleCAPLine(msg, s.Registered())
	default:
		s.routeSASLTraffic(msg)
	}
}

// capNotices returns the trailing of every CAP <sub> the client was sent.
func capNotices(msgs []irc.Message, sub string) []string {
	var out []string
	for _, m := range msgs {
		if m.Command == "CAP" && m.Param(1) == sub {
			out = append(out, m.Trailing())
		}
	}
	return out
}

func notices(msgs []irc.Message, substr string) int {
	n := 0
	for _, m := range msgs {
		if m.Command == "NOTICE" && strings.Contains(m.Trailing(), substr) {
			n++
		}
	}
	return n
}

func (s *Session) bouncerSASLFlags() (pending, failed bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bouncerSASLPending, s.bouncerSASLFailed
}

// Rule 2: CAP NEW sasl with bouncer SASL configured is held, not relayed;
// a client REQ meanwhile is NAK'd; the ACK is the bouncer's ("starting").
func TestBouncerSASLHoldsCapNewFromClients(t *testing.T) {
	s, d := newBouncerSASLSession(t)
	upLine(t, s, "CAP * NEW :sasl=PLAIN")
	if got := capNotices(d.snapshot(), "NEW"); len(got) != 0 {
		t.Fatalf("CAP NEW sasl must be held while the bouncer tries: %v", got)
	}
	if pending, _ := s.bouncerSASLFlags(); !pending {
		t.Fatal("bouncer exchange must be pending after CAP NEW")
	}
	if err := s.RequestClientSASL(d); err != nil {
		t.Fatal(err)
	}
	if got := capNotices(d.snapshot(), "NAK"); len(got) != 1 || got[0] != "sasl" {
		t.Fatalf("client REQ during bouncer exchange must be NAK'd: %+v", d.snapshot())
	}
	upLine(t, s, "CAP * ACK :sasl")
	if notices(d.snapshot(), "SASL authentication starting") != 1 {
		t.Fatalf("missing bouncer starting NOTICE: %+v", d.snapshot())
	}
	if got := capNotices(d.snapshot(), "NEW"); len(got) != 0 {
		t.Fatalf("still nothing offered after the bouncer's ACK: %v", got)
	}
}

// Rule 4: the bouncer's failure relays sasl to clients and opens passthrough.
func TestBouncerSASLFailureRelaysCapNew(t *testing.T) {
	s, d := newBouncerSASLSession(t)
	upLine(t, s, "CAP * NEW :sasl=PLAIN")
	upLine(t, s, "CAP * ACK :sasl")
	d.clearSent()
	upLine(t, s, ":server 904 me :SASL authentication failed")
	if notices(d.snapshot(), "SASL authentication failed") != 1 {
		t.Fatalf("missing failure NOTICE: %+v", d.snapshot())
	}
	if got := capNotices(d.snapshot(), "NEW"); len(got) != 1 || got[0] != "sasl=PLAIN" {
		t.Fatalf("failure must relay CAP NEW sasl=PLAIN: %+v", d.snapshot())
	}
	if pending, failed := s.bouncerSASLFlags(); pending || !failed {
		t.Fatalf("flags after 904: pending=%v failed=%v", pending, failed)
	}
	if !s.OffersPassthroughSASL() {
		t.Fatal("passthrough must be offered after the bouncer failed")
	}
	d.clearSent()
	if err := s.RequestClientSASL(d); err != nil {
		t.Fatal(err)
	}
	if got := capNotices(d.snapshot(), "ACK"); len(got) != 1 || got[0] != "sasl" {
		t.Fatalf("client REQ after bouncer failure must be ACK'd (uplink still has sasl): %+v", d.snapshot())
	}
	if !d.HasCap("sasl") {
		t.Fatal("client sasl cap must be enabled")
	}
}

// Rule 3: once a login succeeds, an offer left over from the bouncer's
// failure is withdrawn — after the outcome numeric, never between a
// client's 900 and 903.
func TestBouncerSASLLoginWithdrawsOffer(t *testing.T) {
	s, d := newBouncerSASLSession(t)
	upLine(t, s, "CAP * NEW :sasl=PLAIN")
	upLine(t, s, "CAP * ACK :sasl")
	upLine(t, s, ":server 904 me :SASL authentication failed")
	if err := s.RequestClientSASL(d); err != nil {
		t.Fatal(err)
	}
	other := &fakeDL{id: "c2", caps: map[string]bool{"cap-notify": true}}
	if err := s.Attach(other); err != nil {
		t.Fatal(err)
	}
	d.clearSent()
	other.clearSent()

	// d drives a passthrough exchange (forwardClientAuthenticate needs a
	// driver; the client-side bookkeeping is the part under test here).
	s.mu.Lock()
	s.saslClient = d.ID()
	s.mu.Unlock()
	upLine(t, s, ":server 900 me me!u@h acct :You are now logged in as acct")
	if len(capNotices(d.snapshot(), "DEL")) != 0 {
		t.Fatalf("CAP DEL must not land between a client's 900 and 903: %+v", d.snapshot())
	}
	upLine(t, s, ":server 903 me :SASL authentication successful")

	for _, c := range []*fakeDL{d, other} {
		if got := capNotices(c.snapshot(), "DEL"); len(got) != 1 || got[0] != "sasl" {
			t.Fatalf("%s: login must withdraw the offer with CAP DEL sasl: %+v", c.id, c.snapshot())
		}
	}
	if countMsgCmd(d.snapshot(), "903") != 1 || countMsgCmd(other.snapshot(), "903") != 0 {
		t.Fatalf("903 goes to the initiator only: d=%+v other=%+v", d.snapshot(), other.snapshot())
	}
	if s.OffersPassthroughSASL() {
		t.Fatal("nothing offered once logged in")
	}
	if _, failed := s.bouncerSASLFlags(); failed {
		t.Fatal("failed flag must clear on login")
	}
}

// Rule 5: with a client's exchange in flight, a re-advertised sasl must
// not start the bouncer's own — and, the bouncer having failed, it is
// relayed straight to clients.
func TestBouncerSASLDoesNotStartOverClientExchange(t *testing.T) {
	s, d := newBouncerSASLSession(t)
	upLine(t, s, "CAP * NEW :sasl=PLAIN")
	upLine(t, s, "CAP * ACK :sasl")
	upLine(t, s, ":server 904 me :SASL authentication failed")
	s.mu.Lock()
	s.saslClient = d.ID()
	s.mu.Unlock()
	d.clearSent()

	upLine(t, s, "CAP * DEL :sasl")
	upLine(t, s, "CAP * NEW :sasl=PLAIN,SCRAM-SHA-256")
	if pending, _ := s.bouncerSASLFlags(); pending {
		t.Fatal("bouncer must not start while a client exchange is in flight")
	}
	if notices(d.snapshot(), "SASL authentication starting") != 0 {
		t.Fatalf("no bouncer starting NOTICE expected: %+v", d.snapshot())
	}
	if got := capNotices(d.snapshot(), "NEW"); len(got) != 1 || got[0] != "sasl=PLAIN,SCRAM-SHA-256" {
		t.Fatalf("bouncer having failed, CAP NEW sasl is relayed immediately: %+v", d.snapshot())
	}
}

// Rule 4's "no usable mechanism" clause: credentials configured but only
// EXTERNAL on offer — the bouncer can't try, so clients get sasl at once.
func TestBouncerSASLUnusableMechanismRelaysCapNew(t *testing.T) {
	s, d := newBouncerSASLSession(t)
	upLine(t, s, "CAP * NEW :sasl=EXTERNAL")
	if pending, failed := s.bouncerSASLFlags(); pending || !failed {
		t.Fatalf("flags: pending=%v failed=%v", pending, failed)
	}
	if got := capNotices(d.snapshot(), "NEW"); len(got) != 1 || got[0] != "sasl=EXTERNAL" {
		t.Fatalf("unusable mechanism must relay CAP NEW immediately: %+v", d.snapshot())
	}
}

// A bouncer still logged in never re-tries and never relays: the ircd
// re-advertising sasl around a services restart changes nothing for
// clients (the previous commit's guard, restated under the new rules).
func TestBouncerSASLLoggedInHoldsAndDoesNotRelay(t *testing.T) {
	s, d := newBouncerSASLSession(t)
	s.mu.Lock()
	s.loggedIn = true
	s.mu.Unlock()
	upLine(t, s, "CAP * DEL :sasl")
	upLine(t, s, "CAP * NEW :sasl=PLAIN")
	if pending, failed := s.bouncerSASLFlags(); pending || failed {
		t.Fatalf("flags: pending=%v failed=%v", pending, failed)
	}
	if got := capNotices(d.snapshot(), "NEW"); len(got) != 0 {
		t.Fatalf("nothing relayed while logged in: %v", got)
	}
}

// Registration-time failure: the bouncer's 904 during registration
// makes sasl part of the offer the moment registration completes — the
// awaiting client gets it in the CAP NEW burst and CAP LS carries it.
// A registration that ACK'd sasl but never produced an outcome (no
// mechanism the bouncer could use, so registration sent CAP END) counts
// as failed too.
func TestBouncerSASLRegistrationFailureOffersSASL(t *testing.T) {
	for _, outcome := range []string{":server 904 me :SASL authentication failed", ""} {
		s := New(store.Network{Name: "n", Nick: "me", SASL: true, SASLUser: "acct", SASLPass: "secret"}, nil, nil, nil, nil)
		d := &fakeDL{id: "c1", caps: map[string]bool{"cap-notify": true}}
		if err := s.Attach(d); err != nil {
			t.Fatal(err)
		}
		reg := func(line string) {
			msg, err := irc.Parse(line)
			if err != nil {
				t.Fatal(err)
			}
			s.HandleRegistrationLine(msg)
		}
		reg("CAP * LS :sasl=PLAIN cap-notify")
		reg("CAP * ACK :sasl cap-notify")
		if outcome != "" {
			reg(outcome)
		}
		if pending, failed := s.bouncerSASLFlags(); outcome != "" && (pending || !failed) {
			t.Fatalf("%q: flags before completion: pending=%v failed=%v", outcome, pending, failed)
		}
		d.clearSent()
		s.completeRegistration()
		if _, failed := s.bouncerSASLFlags(); !failed {
			t.Fatalf("%q: bouncer must count as failed after completion", outcome)
		}
		var sawSASL bool
		for _, n := range capNotices(d.snapshot(), "NEW") {
			if strings.Contains(n, "sasl=PLAIN") {
				sawSASL = true
			}
		}
		if !sawSASL {
			t.Fatalf("%q: awaiting client must be offered sasl on completion: %+v", outcome, d.snapshot())
		}
		var inLS bool
		for _, c := range s.OfferedCaps() {
			if c == "sasl=PLAIN" {
				inLS = true
			}
		}
		if !inLS {
			t.Fatalf("%q: OfferedCaps must carry sasl: %v", outcome, s.OfferedCaps())
		}
	}
}
