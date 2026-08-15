package session

import (
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/keeper"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// TestDownlinkAfterResumeGetsJoinAndNames answers a direct question about
// the resumed-brain path: once Session.SeedFromBlob has run (a brain
// process reattaching to a keeper that already held the network live —
// see docs/keeper-design.md's "Blob store and gap-only resume"), a client
// connecting afterward must see the normal channel-attach burst (JOIN,
// NAMES/353, end-of-NAMES/366) for every channel the blob says it's on —
// exactly like attaching to a session that registered normally, since
// SeedFromBlob's whole job is to make a resumed Session observably
// equivalent to one that just finished a live registration burst.
func TestDownlinkAfterResumeGetsJoinAndNames(t *testing.T) {
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
	var gotJoin, got353, got366 bool
	for _, m := range sent {
		if m.Command == "JOIN" && m.Param(0) == "#resumed" {
			gotJoin = true
		}
		if m.Command == "353" && len(m.Params) >= 3 && m.Params[2] == "#resumed" {
			got353 = true
		}
		if m.Command == "366" && m.Param(1) == "#resumed" {
			got366 = true
		}
	}
	if !gotJoin {
		t.Fatalf("attach burst missing JOIN #resumed: %+v", sent)
	}
	if !got353 {
		t.Fatalf("attach burst missing NAMES (353) for #resumed: %+v", sent)
	}
	if !got366 {
		t.Fatalf("attach burst missing end-of-NAMES (366) for #resumed: %+v", sent)
	}
}
