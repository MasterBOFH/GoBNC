package server

import (
	"path/filepath"
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/store"
)

// TestResumeReplaysWelcomeNumerics proves that 002/003/004 (and the 001
// source prefix) survive a brain restart. Those numerics are
// registration-only and cannot be re-queried, so without pushing them into
// the blob a client attaching after a reload sees 001+005+MOTD with no
// 002/003/004 — not enough to detect which ircd it is on. This drives a
// real resumed brain (resumeTestKeeper) through a real fake-ircd welcome
// that includes 002/003/004, then attaches a downlink to the second brain
// and checks the burst came from the blob, not from a replayed
// registration (gap-only: that burst is never replayed).
func TestResumeReplaysWelcomeNumerics(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "t.db")
	rk := startResumeTestKeeper(t)

	s1 := newResumeTestServer(t, dbPath)
	fake := newDemuxFakeIRC(t)
	h, p := fake.addr(t)
	closeAfter := make(chan struct{})
	go fake.serveOne(t, "fake.example", closeAfter)

	if _, err := s1.Store().UpsertNetwork(s1.runCtx, store.Network{
		Name: "net1", Host: h, Port: p, Nick: "alice", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	n, err := s1.Store().NetworkByName(s1.runCtx, "net1")
	if err != nil {
		t.Fatal(err)
	}
	rk.attach(t, s1, []store.Network{n})

	sess1, err := s1.Session("net1")
	if err != nil {
		t.Fatal(err)
	}
	waitForRegistered(t, "net1", sess1.Registered)

	// First brain must have learned 002/003/004 live — otherwise this test
	// would pass vacuously after resume by asserting absence.
	probe := newFakeServerDL("probe")
	if err := sess1.Attach(probe); err != nil {
		t.Fatal(err)
	}
	if !attachHasWelcome(probe) {
		t.Fatalf("first brain attach missing 002/003/004 (fake ircd never delivered them?): %+v", probe.snapshot())
	}

	_ = s1.keeperClient.Close()

	s2 := newResumeTestServer(t, dbPath)
	rk.attach(t, s2, []store.Network{n})

	sess2, err := s2.Session("net1")
	if err != nil {
		t.Fatal(err)
	}
	waitForRegistered(t, "net1 (resumed)", sess2.Registered)

	late := newFakeServerDL("late")
	if err := sess2.Attach(late); err != nil {
		t.Fatal(err)
	}
	if !attachHasWelcome(late) {
		t.Fatalf("resumed attach missing 002/003/004: %+v", late.snapshot())
	}
	for _, m := range late.snapshot() {
		switch m.Command {
		case "001", "002", "003", "004":
			if m.Source != "fake.example" {
				t.Fatalf("%s source=%q want fake.example: %+v", m.Command, m.Source, m)
			}
		}
		switch m.Command {
		case "002":
			if m.Param(1) != "Your host is fake.example" {
				t.Fatalf("002 params=%v", m.Params)
			}
		case "003":
			if m.Param(1) != "This server was created today" {
				t.Fatalf("003 params=%v", m.Params)
			}
		case "004":
			if m.Param(1) != "fake.example" || m.Param(2) != "test-1.0" {
				t.Fatalf("004 params=%v", m.Params)
			}
		}
	}

	close(closeAfter)
}

func attachHasWelcome(d *fakeServerDL) bool {
	var saw002, saw003, saw004 bool
	for _, m := range d.snapshot() {
		switch m.Command {
		case "002":
			saw002 = true
		case "003":
			saw003 = true
		case "004":
			saw004 = true
		}
	}
	return saw002 && saw003 && saw004
}

// TestResumeAttachUsesAssignedNick proves that 001's assigned nick is
// pushed into the self-nick blob (not only a later NICK line), so a
// resumed attach burst addresses numerics to the nick the server gave us,
// not Network.Nick (the nick we requested at registration). The fake ircd
// 001s as alice_ while the client/config nick is alice — no NICK command
// is ever sent, which is exactly the gap stateNICKLocked alone couldn't
// cover.
func TestResumeAttachUsesAssignedNick(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "t.db")
	rk := startResumeTestKeeper(t)

	s1 := newResumeTestServer(t, dbPath)
	fake := newDemuxFakeIRC(t)
	fake.welcomeNick = "alice_"
	h, p := fake.addr(t)
	closeAfter := make(chan struct{})
	go fake.serveOne(t, "fake.example", closeAfter)

	if _, err := s1.Store().UpsertNetwork(s1.runCtx, store.Network{
		Name: "net1", Host: h, Port: p, Nick: "alice", Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	n, err := s1.Store().NetworkByName(s1.runCtx, "net1")
	if err != nil {
		t.Fatal(err)
	}
	rk.attach(t, s1, []store.Network{n})

	sess1, err := s1.Session("net1")
	if err != nil {
		t.Fatal(err)
	}
	waitForRegistered(t, "net1", sess1.Registered)
	if got := sess1.Nick(); got != "alice_" {
		t.Fatalf("sess1 nick=%q want alice_ (assigned by 001, not configured alice)", got)
	}

	_ = s1.keeperClient.Close()

	s2 := newResumeTestServer(t, dbPath)
	rk.attach(t, s2, []store.Network{n})

	sess2, err := s2.Session("net1")
	if err != nil {
		t.Fatal(err)
	}
	waitForRegistered(t, "net1 (resumed)", sess2.Registered)
	if got := sess2.Nick(); got != "alice_" {
		t.Fatalf("resumed session nick=%q want assigned alice_, not configured alice", got)
	}

	late := newFakeServerDL("late")
	if err := sess2.Attach(late); err != nil {
		t.Fatal(err)
	}
	var saw001 bool
	for _, m := range late.snapshot() {
		if m.Command == "001" {
			saw001 = true
			if m.Param(0) != "alice_" {
				t.Fatalf("resumed attach 001 target=%q want assigned alice_: %+v", m.Param(0), m)
			}
		}
		if (m.Command == "002" || m.Command == "003" || m.Command == "004" || m.Command == "005" || m.Command == "376") && m.Param(0) != "alice_" {
			t.Fatalf("%s target=%q want assigned alice_: %+v", m.Command, m.Param(0), m)
		}
	}
	if !saw001 {
		t.Fatalf("resumed attach missing 001: %+v", late.snapshot())
	}

	close(closeAfter)
}
