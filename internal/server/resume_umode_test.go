package server

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/store"
)

// TestResumeFetchesLiveSelfUModes is the usermode analogue of
// TestResumeFetchesLiveChannelRoster: a resumed brain doesn't know its own
// usermodes (the blob never carried them — SeedFromBlob leaves UModes
// empty), so Session.Attach must not fabricate a `:prefix MODE nick +modes`
// for a client that connects before the answer is in. Instead
// Session.RefreshSelfUModes polls MODE nick after LiveReady, the uplink's
// 221 lands through the ordinary HandleMessage path, and that unsolicited
// numeric is rewritten to the same own-MODE line Attach would have burst
// — so both an already-attached downlink and a later connecting one learn
// the modes from the wire, not a guess.
func TestResumeFetchesLiveSelfUModes(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "t.db")
	rk := startResumeTestKeeper(t)

	s1 := newResumeTestServer(t, dbPath)
	fake := newDemuxFakeIRC(t)
	h, p := fake.addr(t)
	closeAfter := make(chan struct{})
	go fake.serveOne(t, "fake.example", closeAfter)

	if _, err := s1.Store().UpsertNetwork(s1.runCtx, store.Network{
		Name: "net1", Host: h, Port: p, Nick: "alice", Username: "ident", Enabled: true,
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

	conn := fake.lastConn(t)

	// "Restart": detach s1, attach a brand-new Server to the same
	// still-running keeper — conn stays the live uplink socket throughout.
	_ = s1.keeperClient.Close()

	s2 := newResumeTestServer(t, dbPath)
	rk.attach(t, s2, []store.Network{n})

	sess2, err := s2.Session("net1")
	if err != nil {
		t.Fatal(err)
	}
	waitForRegistered(t, "net1 (resumed)", sess2.Registered)

	early := newFakeServerDL("early")
	if err := sess2.Attach(early); err != nil {
		t.Fatal(err)
	}
	for _, m := range early.snapshot() {
		if m.Command == "MODE" && m.Param(0) == "alice" {
			t.Fatalf("resumed attach must not fabricate own MODE before umodes are known: %+v", early.snapshot())
		}
	}

	if !waitForLineOnConn(t, conn, 5*time.Second, func(line string) bool {
		return strings.HasPrefix(line, "MODE") && strings.Contains(line, "alice")
	}) {
		t.Fatal("resumed brain never sent MODE alice to the uplink")
	}

	fakeSend(t, conn, "fake.example", ":fake.example 221 alice +iw")

	deadline := time.Now().Add(5 * time.Second)
	var gotLiveMODE bool
	for {
		for _, m := range early.snapshot() {
			if m.Command == "MODE" && m.Param(0) == "alice" && m.Param(1) == "+iw" {
				gotLiveMODE = true
			}
			if m.Command == "221" {
				t.Fatalf("unsolicited 221 must not be forwarded as 221: %+v", early.snapshot())
			}
		}
		if gotLiveMODE {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("already-attached downlink never got synthesized own MODE: %+v", early.snapshot())
		}
		time.Sleep(10 * time.Millisecond)
	}

	late := newFakeServerDL("late")
	if err := sess2.Attach(late); err != nil {
		t.Fatal(err)
	}
	var burstMODE bool
	for _, m := range late.snapshot() {
		if m.Command == "MODE" && m.Param(0) == "alice" && m.Param(1) == "+iw" {
			burstMODE = true
		}
		if m.Command == "221" {
			t.Fatalf("later attach must send MODE not 221: %+v", late.snapshot())
		}
	}
	if !burstMODE {
		t.Fatalf("later attach missing own MODE: %+v", late.snapshot())
	}

	close(closeAfter)
}
