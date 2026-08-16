package session

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/keeper"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// TestDownlinkAfterResumeGetsJoinThenRealNames answers a direct question
// about the resumed-brain path: once Session.SeedFromBlob has run (a brain
// process reattaching to a keeper that already held the network live —
// see docs/keeper-design.md's "Blob store and gap-only resume"), a client
// connecting afterward must see JOIN for every channel the blob says it's
// on — but not a fabricated NAMES burst. The blob never carried a member
// roster (seedChannelLocked's own doc comment), so a resumed
// ChannelState's Members has only self; sending a self-only 353/366 as if
// it were real would misinform the client rather than honestly show
// "don't know yet". Once the uplink's real NAMES reply lands
// (RefreshResumedChannelNames' job, exercised end-to-end in
// internal/server's TestResumeFetchesLiveChannelRoster; here simulated
// directly via HandleMessage), the already-attached downlink must get it
// through the ordinary broadcast path, and RosterKnown flips true.
func TestDownlinkAfterResumeGetsJoinThenRealNames(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)

	// Mirrors what a real HelloAck delivers: a "channel:#resumed" blob entry
	// this brain never joined itself (the keeper already held the network
	// live before this brain attached — no local JOIN, no local echo).
	s.SeedFromBlob([]keeper.BlobEntry{
		{Key: "self-nick", Values: [][]byte{[]byte("me")}},
		{Key: "channel:#resumed", Values: [][]byte{[]byte("")}},
	})
	if !s.Registered() {
		t.Fatal("SeedFromBlob must mark the session registered")
	}

	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}

	sent := d.snapshot()
	var gotJoin, gotFabricatedNames bool
	for _, m := range sent {
		if m.Command == "JOIN" && m.Param(0) == "#resumed" {
			gotJoin = true
		}
		if (m.Command == "353" || m.Command == "366") && len(m.Params) > 1 && m.Params[len(m.Params)-2] == "#resumed" {
			gotFabricatedNames = true
		}
	}
	if !gotJoin {
		t.Fatalf("attach burst missing JOIN #resumed: %+v", sent)
	}
	if gotFabricatedNames {
		t.Fatalf("attach burst must not fabricate NAMES before the roster is known: %+v", sent)
	}

	// The real reply, as if RefreshResumedChannelNames' NAMES query just
	// got answered by the uplink.
	d.clearSent()
	s.HandleMessage(irc.Message{Source: "server", Command: "353", Params: []string{"me", "=", "#resumed", "me bob"}})
	s.HandleMessage(irc.Message{Source: "server", Command: "366", Params: []string{"me", "#resumed", "End of /NAMES list."}})

	sent = d.snapshot()
	var got353WithBob, got366 bool
	for _, m := range sent {
		if m.Command == "353" && len(m.Params) >= 4 && m.Params[2] == "#resumed" && strings.Contains(m.Params[3], "bob") {
			got353WithBob = true
		}
		if m.Command == "366" && m.Param(1) == "#resumed" {
			got366 = true
		}
	}
	if !got353WithBob {
		t.Fatalf("already-attached downlink never got the real NAMES (with bob): %+v", sent)
	}
	if !got366 {
		t.Fatalf("already-attached downlink never got end-of-NAMES: %+v", sent)
	}

	s.mu.RLock()
	ch := s.channels[s.isupport.CaseMapping.Canonical("#resumed")]
	s.mu.RUnlock()
	if ch == nil || !ch.RosterKnown {
		t.Fatal("RosterKnown should be true once the real 366 has been processed")
	}
}

// TestDownlinkAfterResumeGetsSelfUMode is the usermode analogue of
// TestDownlinkAfterResumeGetsJoinThenRealNames: the blob never carried
// umodes (SeedFromBlob leaves UModes empty), so a client attaching right
// after resume must not get a fabricated own-MODE. Once the uplink's real
// 221 lands (RefreshSelfUModes' job, exercised end-to-end in
// internal/server's TestResumeFetchesLiveSelfUModes; here simulated
// directly via HandleMessage), the already-attached downlink must get the
// synthesized `:prefix MODE nick +modes` through broadcastSelfUMode, and a
// later Attach must include that MODE in its burst.
func TestDownlinkAfterResumeGetsSelfUMode(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me", Username: "u"}, nil, nil, nil, nil)
	s.SeedFromBlob([]keeper.BlobEntry{
		{Key: "self-nick", Values: [][]byte{[]byte("me")}},
		{Key: "cloak", Values: [][]byte{[]byte("h.example")}},
	})
	if !s.Registered() {
		t.Fatal("SeedFromBlob must mark the session registered")
	}

	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	for _, m := range d.snapshot() {
		if m.Command == "MODE" && m.Param(0) == "me" {
			t.Fatalf("attach burst must not fabricate own MODE before umodes are known: %+v", d.snapshot())
		}
	}

	d.clearSent()
	s.HandleMessage(irc.Message{Source: "server", Command: "221", Params: []string{"me", "+iw"}})

	sent := d.snapshot()
	var gotMODE bool
	for _, m := range sent {
		if m.Command == "MODE" && m.Param(0) == "me" && m.Param(1) == "+iw" && m.Source == "me!u@h.example" {
			gotMODE = true
		}
		if m.Command == "221" {
			t.Fatalf("unsolicited 221 must not be forwarded as 221: %+v", sent)
		}
	}
	if !gotMODE {
		t.Fatalf("already-attached downlink never got synthesized own MODE: %+v", sent)
	}

	late := &fakeDL{id: "c2", caps: map[string]bool{}}
	if err := s.Attach(late); err != nil {
		t.Fatal(err)
	}
	var burstMODE, burst221 bool
	var after376 bool
	for _, m := range late.snapshot() {
		switch m.Command {
		case "376":
			after376 = true
		case "MODE":
			if !after376 {
				t.Fatal("own MODE must come after 376")
			}
			burstMODE = true
			if m.Param(0) != "me" || m.Param(1) != "+iw" {
				t.Fatalf("late attach MODE=%v", m.Params)
			}
			if m.Source != "me!u@h.example" {
				t.Fatalf("late attach MODE source=%q", m.Source)
			}
		case "221":
			burst221 = true
		}
	}
	if !burstMODE {
		t.Fatalf("later attach missing own MODE: %+v", late.snapshot())
	}
	if burst221 {
		t.Fatalf("later attach must send MODE not 221: %+v", late.snapshot())
	}
}

// TestDownlinkAfterResumeGetsLoggedIn is the regression test for the
// resumed-brain gap where SeedFromBlob restored the account blob key onto
// self.Account but left Session.loggedIn false. Attach's synthesized
// RPL_LOGGEDIN (rplLoggedInLocked) requires both, so a client connecting
// after a brain restart never saw 900 even though the previous brain had
// SASL-authenticated and pushed the account into the keeper blob.
func TestDownlinkAfterResumeGetsLoggedIn(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me", Username: "u"}, nil, nil, nil, nil)
	s.SeedFromBlob([]keeper.BlobEntry{
		{Key: "self-nick", Values: [][]byte{[]byte("me")}},
		{Key: "account", Values: [][]byte{[]byte("acct")}},
	})
	if !s.Registered() {
		t.Fatal("SeedFromBlob must mark the session registered")
	}

	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}

	var saw376, saw900 bool
	for _, m := range d.snapshot() {
		switch m.Command {
		case "376":
			saw376 = true
			if saw900 {
				t.Fatal("900 must come after registration (376)")
			}
		case "900":
			if !saw376 {
				t.Fatal("900 before 376")
			}
			saw900 = true
			if m.Param(2) != "acct" {
				t.Fatalf("%+v", m)
			}
		}
	}
	if !saw900 {
		t.Fatalf("attach burst after resume missing 900: %+v", d.snapshot())
	}
}

// TestDownlinkAfterResumeOmitsLoggedInWhenLoggedOut is the 901 counterpart:
// applyAccountFromSASL replaces the account blob with a nil value, which
// SeedFromBlob must not treat as a still-logged-in session.
func TestDownlinkAfterResumeOmitsLoggedInWhenLoggedOut(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	s.SeedFromBlob([]keeper.BlobEntry{
		{Key: "self-nick", Values: [][]byte{[]byte("me")}},
		{Key: "account", Values: [][]byte{nil}},
	})

	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	for _, m := range d.snapshot() {
		if m.Command == "900" {
			t.Fatalf("must not replay 900 after a logged-out account blob: %+v", d.snapshot())
		}
	}
}

// TestDownlinkAfterResumeGetsWelcomeNumerics is the regression for the
// resumed-brain gap where 002/003/004 (and the 001 source prefix) were
// learned live during registration but never pushed to the blob. Those
// numerics cannot be re-queried, so a client attaching after a brain
// restart saw 001+005+MOTD with no 002/003/004 — not enough for the
// client to detect which ircd it is on. SeedFromBlob must restore them
// (and detectIRCdLocked from 002/004) so Attach bursts them.
func TestDownlinkAfterResumeGetsWelcomeNumerics(t *testing.T) {
	rpl002, err := json.Marshal([]string{"Your host is irc.example running version UnrealIRCd-6.1.4"})
	if err != nil {
		t.Fatal(err)
	}
	rpl003, err := json.Marshal([]string{"This server was created Jan 1 2020 at 00:00:00 UTC"})
	if err != nil {
		t.Fatal(err)
	}
	rpl004, err := json.Marshal([]string{"irc.example", "UnrealIRCd-6.1.4", "iowz", "biklmnopst"})
	if err != nil {
		t.Fatal(err)
	}

	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	s.SeedFromBlob([]keeper.BlobEntry{
		{Key: "self-nick", Values: [][]byte{[]byte("me")}},
		{Key: "uplink-server", Values: [][]byte{[]byte("irc.example")}},
		{Key: "rpl002", Values: [][]byte{rpl002}},
		{Key: "rpl003", Values: [][]byte{rpl003}},
		{Key: "rpl004", Values: [][]byte{rpl004}},
	})
	if !s.Registered() {
		t.Fatal("SeedFromBlob must mark the session registered")
	}
	if s.IRCd() != irc.IRCdUnreal {
		t.Fatalf("ircd=%q want unrealircd from restored 002", s.IRCd())
	}

	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}

	var saw002, saw003, saw004 bool
	for _, m := range d.snapshot() {
		if m.Source != "irc.example" && (m.Command == "002" || m.Command == "003" || m.Command == "004" || m.Command == "001") {
			t.Fatalf("%s source=%q want irc.example: %+v", m.Command, m.Source, m)
		}
		switch m.Command {
		case "002":
			saw002 = true
			if m.Param(1) != "Your host is irc.example running version UnrealIRCd-6.1.4" {
				t.Fatalf("002 params=%v", m.Params)
			}
		case "003":
			saw003 = true
			if m.Param(1) != "This server was created Jan 1 2020 at 00:00:00 UTC" {
				t.Fatalf("003 params=%v", m.Params)
			}
		case "004":
			saw004 = true
			if m.Param(1) != "irc.example" || m.Param(2) != "UnrealIRCd-6.1.4" {
				t.Fatalf("004 params=%v", m.Params)
			}
		}
	}
	if !saw002 || !saw003 || !saw004 {
		t.Fatalf("attach burst after resume missing welcome numerics 002=%v 003=%v 004=%v: %+v", saw002, saw003, saw004, d.snapshot())
	}
}

// TestDownlinkAfterResumeUsesAssignedNickNotConfigured is the regression
// for attach bursting 001/002/… to Network.Nick (the nick we requested at
// registration) after a resume. self-nick used to be pushed only on a
// later NICK line (stateNICKLocked), never on 001, so a session whose
// assigned nick differed from the configured one (collision, guest nick,
// …) silently fell back to the configured nick on SeedFromBlob. Attach
// must use the assigned nick in every numeric; 001 is the nick source of
// truth (no self-NICK).
func TestDownlinkAfterResumeUsesAssignedNickNotConfigured(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "primary"}, nil, nil, nil, nil)
	s.SeedFromBlob([]keeper.BlobEntry{
		{Key: "self-nick", Values: [][]byte{[]byte("primary_")}},
		{Key: "uplink-server", Values: [][]byte{[]byte("irc.example")}},
		{Key: "rpl002", Values: [][]byte{mustJSON(t, []string{"Your host is irc.example"})}},
	})
	if got := s.Nick(); got != "primary_" {
		t.Fatalf("session nick after resume=%q want primary_", got)
	}

	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	sent := d.snapshot()
	assertNoSelfNICK(t, sent)
	var saw001, saw002 bool
	for _, m := range sent {
		switch m.Command {
		case "001":
			saw001 = true
			if m.Param(0) != "primary_" {
				t.Fatalf("001 target=%q want assigned nick primary_, not configured primary: %+v", m.Param(0), m)
			}
		case "002":
			saw002 = true
			if m.Param(0) != "primary_" {
				t.Fatalf("002 target=%q want assigned nick: %+v", m.Param(0), m)
			}
		}
	}
	if !saw001 || !saw002 {
		t.Fatalf("attach missing 001/002: %+v", sent)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
