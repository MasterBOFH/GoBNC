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
	s.mu.Lock()
	s.self.UModes = map[byte]bool{'i': true, 'w': true} // unchanged across the gap → self-MODE suppressed
	s.mu.Unlock()
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

// The resumed burst re-sends topic and self-umode; a held client should see
// them only when they actually changed during the gap.
func TestHeldResumeDiffsTopicAndUmode(t *testing.T) {
	setup := func(t *testing.T) (*Session, *fakeDL) {
		s, d := resumableSession(t, true)
		// Seed pre-drop state: a channel with a topic, and self umodes.
		s.mu.Lock()
		s.channels["#chan"] = &ChannelState{Name: "#chan", Topic: "old topic", Members: map[string]struct{}{}, Modes: irc.NewChannelModes()}
		s.self.UModes = map[byte]bool{'i': true, 'w': true}
		s.mu.Unlock()
		s.HandleDisconnect(irc.ErrLineTooLong)
		d.clearSent()
		return s, d
	}
	feed := func(s *Session, line string) {
		msg, err := irc.Parse(line)
		if err != nil {
			t.Fatalf("parse %q: %v", line, err)
		}
		s.applyState(msg)
		s.HandleRegistrationLine(msg)
	}

	t.Run("unchanged suppressed", func(t *testing.T) {
		s, d := setup(t)
		feed(s, ":srv RESUME SUCCESS :me")
		feed(s, ":me JOIN #chan")
		feed(s, ":srv 332 me #chan :old topic")
		feed(s, ":srv 333 me #chan setter 123")
		feed(s, ":me MODE me :+iw")
		feed(s, ":srv 376 me :End of MOTD")
		if hasCmd(d, "332") || hasCmd(d, "333") {
			t.Fatalf("unchanged topic relayed: %v", sentCommands(d))
		}
		for _, m := range d.snapshot() {
			if m.Command == "MODE" && m.Param(0) == "me" {
				t.Fatalf("unchanged umode relayed: %+v", m)
			}
		}
	})

	t.Run("changed relayed", func(t *testing.T) {
		s, d := setup(t)
		feed(s, ":srv RESUME SUCCESS :me")
		feed(s, ":me JOIN #chan")
		feed(s, ":srv 332 me #chan :NEW topic")
		feed(s, ":srv 333 me #chan setter 456")
		feed(s, ":me MODE me :-w") // dropped +w during the gap
		feed(s, ":srv 376 me :End of MOTD")
		if !hasCmd(d, "332") {
			t.Fatalf("changed topic not relayed: %v", sentCommands(d))
		}
		sawMode := false
		for _, m := range d.snapshot() {
			if m.Command == "MODE" && m.Param(0) == "me" {
				sawMode = true
			}
		}
		if !sawMode {
			t.Fatalf("changed umode not relayed: %v", d.snapshot())
		}
	})
}

// The ircd's pre-welcome connection preamble (NOTICE AUTH "*** Looking up
// your hostname" etc.) is re-sent on every redial; a held client already
// saw it on the original connection, so it must not be relayed — while a
// NOTICE after 001 (services, a server notice in the burst) still is.
func TestHeldResumeSuppressesPreWelcomeNotices(t *testing.T) {
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
	feed("NOTICE AUTH :*** Looking up your hostname")
	feed("NOTICE AUTH :*** Found your hostname")
	feed(":srv RESUME SUCCESS :me")
	feed(":srv 001 me :Welcome")
	feed(":NickServ!s@services NOTICE me :You are now identified")
	feed(":srv 376 me :End of MOTD")

	for _, m := range d.snapshot() {
		if m.Command == "NOTICE" && strings.Contains(m.Trailing(), "hostname") {
			t.Fatalf("pre-welcome NOTICE AUTH leaked to held client: %+v", m)
		}
	}
	found := false
	for _, m := range d.snapshot() {
		if m.Command == "NOTICE" && strings.Contains(m.Trailing(), "identified") {
			found = true
		}
	}
	if !found {
		t.Fatalf("post-welcome NOTICE not relayed to held client: %v", sentCommands(d))
	}
}

// A client held across a resume already has the caps the fresh uplink
// re-ACKs — from its side nothing changed, so it must get no CAP NEW for
// them. A cap the new uplink offers that the client never saw is still
// announced, and a cap withdrawn (CAP DEL) and later re-offered is
// announced again.
func TestHeldResumeNoCapNewForCapsClientAlreadyHas(t *testing.T) {
	s, d := resumableSession(t, true)
	d.caps["cap-notify"] = true
	for _, c := range []string{"cap-notify", "message-tags", "server-time", "account-tag", "away-notify"} {
		d.MarkSeenCap(c)
	}
	s.mu.Lock()
	s.upCaps["account-tag"] = true
	s.upCaps["away-notify"] = true
	s.mu.Unlock()
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
	feed(":srv CAP me ACK :account-tag away-notify")
	feed(":srv RESUME SUCCESS :me")
	feed(":srv 001 me :Welcome")
	feed(":srv 376 me :End of MOTD")

	for _, m := range d.snapshot() {
		if m.Command == "CAP" {
			t.Fatalf("held client got a CAP notification for caps it already had: %+v", m)
		}
	}

	// A cap the client has never seen still gets announced …
	s.broadcastCapNotify("NEW", []string{"chghost"})
	if !hasCmd(d, "CAP") {
		t.Fatal("CAP NEW for a never-seen cap was not sent")
	}
	// … once: a second announcement of the same cap is a no-op …
	d.clearSent()
	s.broadcastCapNotify("NEW", []string{"chghost"})
	if hasCmd(d, "CAP") {
		t.Fatal("CAP NEW re-announced a cap the client had already seen")
	}
	// … until a CAP DEL withdraws it.
	s.broadcastCapNotify("DEL", []string{"chghost"})
	d.clearSent()
	s.broadcastCapNotify("NEW", []string{"chghost"})
	if !hasCmd(d, "CAP") {
		t.Fatal("CAP NEW after CAP DEL was not re-announced")
	}
}
