package session

import (
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
