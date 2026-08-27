package session

import (
	"strings"
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/registration"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// resumableSession builds a registered session with the resume cap enabled,
// a stored token flag set, and one attached client — the state a held
// resume keys off. It uses no real uplink; HandleDisconnect/completeRegistration
// are driven directly.
func resumableSession(t *testing.T, withCapAndToken bool) (*Session, *fakeDL) {
	t.Helper()
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.registered = true
	if withCapAndToken {
		s.upCaps[registration.ResumeCap] = true
		s.resumeTokenHeld = true
	}
	s.mu.Unlock()
	d.clearSent()
	return s, d
}

func sentCommands(d *fakeDL) []string {
	var out []string
	for _, m := range d.snapshot() {
		out = append(out, m.Command)
	}
	return out
}

func hasCmd(d *fakeDL, cmd string) bool {
	for _, m := range d.snapshot() {
		if m.Command == cmd {
			return true
		}
	}
	return false
}

// A resumable uplink drop must keep the client attached (no ERROR, no
// Close), mark the session resuming, and send a "resuming" NOTICE.
func TestHeldResumeKeepsClientOnDrop(t *testing.T) {
	s, d := resumableSession(t, true)
	s.HandleDisconnect(irc.ErrLineTooLong)

	if d.wasClosed() {
		t.Fatal("client was closed on a resumable drop; want held")
	}
	if hasCmd(d, "ERROR") {
		t.Fatalf("client got ERROR on a resumable drop: %v", sentCommands(d))
	}
	if !hasCmd(d, "NOTICE") {
		t.Fatalf("client got no resuming NOTICE: %v", sentCommands(d))
	}
	s.mu.RLock()
	_, stillAttached := s.downlinks[d.id]
	resuming := s.resuming
	held := s.heldAcrossResume[d.id]
	s.mu.RUnlock()
	if !stillAttached || !resuming || !held {
		t.Fatalf("attached=%v resuming=%v held=%v, want all true", stillAttached, resuming, held)
	}
}

// Without the cap or a token, a registered drop kicks as before.
func TestNonResumableDropStillKicks(t *testing.T) {
	s, d := resumableSession(t, false)
	s.HandleDisconnect(irc.ErrLineTooLong)
	if !hasCmd(d, "ERROR") || !d.wasClosed() {
		t.Fatalf("non-resumable registered drop: ERROR=%v closed=%v, want kick", hasCmd(d, "ERROR"), d.wasClosed())
	}
	s.mu.RLock()
	resuming := s.resuming
	n := len(s.downlinks)
	s.mu.RUnlock()
	if resuming || n != 0 {
		t.Fatalf("resuming=%v downlinks=%d, want false/0", resuming, n)
	}
}

// While resuming, the replayed welcome numerics are not re-sent to a held
// client, but 376 still completes registration and the client is kept with
// a "resumed" NOTICE.
func TestHeldResumeSuppressesWelcomeAndKeepsClient(t *testing.T) {
	s, d := resumableSession(t, true)
	s.HandleDisconnect(irc.ErrLineTooLong)
	d.clearSent()

	// Drive the resumed reconnect's line stream directly through HandleLine.
	feed := func(line string) {
		msg, err := irc.Parse(line)
		if err != nil {
			t.Fatalf("parse %q: %v", line, err)
		}
		s.applyState(msg)
		s.HandleRegistrationLine(msg)
	}
	feed(":srv RESUME SUCCESS :me")
	feed(":srv 001 me :Welcome")
	feed(":srv 005 me NICKLEN=30 :are supported by this server")
	feed(":srv 376 me :End of MOTD")

	if hasCmd(d, "001") || hasCmd(d, "005") {
		t.Fatalf("held client saw the duplicate welcome burst: %v", sentCommands(d))
	}
	if !s.Registered() {
		t.Fatal("session did not complete registration on resumed 376")
	}
	if d.wasClosed() {
		t.Fatal("held client was closed after a successful resume")
	}
	sawResumed := false
	for _, m := range d.snapshot() {
		if m.Command == "NOTICE" && strings.Contains(m.Trailing(), "resumed") {
			sawResumed = true
		}
	}
	if !sawResumed {
		t.Fatalf("no 'Session resumed' NOTICE: %v", d.snapshot())
	}
}

// If the reconnect registers fresh (no RESUME SUCCESS), the held client's
// state is stale, so it is kicked with an ERROR to reattach.
func TestHeldResumeKicksClientWhenResumeFails(t *testing.T) {
	s, d := resumableSession(t, true)
	s.HandleDisconnect(irc.ErrLineTooLong)
	d.clearSent()

	feed := func(line string) {
		msg, _ := irc.Parse(line)
		s.applyState(msg)
		s.HandleRegistrationLine(msg)
	}
	// No RESUME SUCCESS — a fresh registration burst instead.
	feed(":srv 001 me :Welcome")
	feed(":srv 376 me :End of MOTD")

	if !hasCmd(d, "ERROR") || !d.wasClosed() {
		t.Fatalf("resume-failed fallback: ERROR=%v closed=%v, want kick", hasCmd(d, "ERROR"), d.wasClosed())
	}
}

// While resuming, the channel/roster burst (332 topic, 353/366 NAMES) IS
// relayed to a held client — a NAMES reply is authoritative, so it is how
// the client reconciles anything that changed during the gap — while the
// welcome preamble (001..005) and the client's own umode MODE stay
// suppressed.
func TestHeldResumeRelaysRosterButNotWelcome(t *testing.T) {
	s, d := resumableSession(t, true)
	s.HandleDisconnect(irc.ErrLineTooLong)
	d.clearSent()

	feed := func(line string) {
		msg, err := irc.Parse(line)
		if err != nil {
			t.Fatalf("parse %q: %v", line, err)
		}
		s.applyState(msg)
		s.HandleRegistrationLine(msg)
	}
	feed(":srv RESUME SUCCESS :me")
	feed(":srv 001 me :Welcome")
	feed(":srv 005 me NICKLEN=30 :are supported by this server")
	feed(":me MODE me :+iw")                       // own umodes: preamble, suppress
	feed(":srv 332 me #chan :the topic")           // relay
	feed(":srv 353 me = #chan :me +bob @carol")    // relay (authoritative roster)
	feed(":srv 366 me #chan :End of /NAMES list.") // relay
	feed(":srv 376 me :End of MOTD")

	if hasCmd(d, "001") || hasCmd(d, "005") {
		t.Fatalf("welcome preamble leaked to held client: %v", sentCommands(d))
	}
	for _, m := range d.snapshot() {
		if m.Command == "MODE" && len(m.Params) > 0 && m.Param(0) == "me" {
			t.Fatalf("self-umode MODE leaked to held client: %+v", m)
		}
	}
	if !hasCmd(d, "332") || !hasCmd(d, "353") || !hasCmd(d, "366") {
		t.Fatalf("roster/topic burst not relayed to held client: %v", sentCommands(d))
	}
}
