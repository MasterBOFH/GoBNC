package server

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/store"
)

// TestResumeLearnsSelfUserHost reproduces the bug directly: a resumed
// session's own self.Host is unknown (the "cloak" blob key is only ever
// pushed from a later CHGHOST/396 — see Session.RefreshSelfUserHost's doc
// comment — never from the self-JOIN a channel was originally learned
// through), and without a real answer, every JOIN this session replays to
// a newly-attaching client uses User.Prefix()'s "nick!user@ServerName"
// fallback — never the invalid bare "nick!user" a real client was observed
// rejecting ("[JOIN]: Invalid syntax: Cannot read prefix"), but also never
// the real host, until Session.RefreshSelfUserHost's proactive USERHOST
// query gets a real answer. This drives that whole path against a real
// resumed brain and a real fake ircd connection: before the answer, the
// fallback is valid but synthetic; after it, the replayed JOIN carries the
// real prefix.
func TestResumeLearnsSelfUserHost(t *testing.T) {
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

	// Uplink auto-joins #chan — the shape TestAutoJoinRepopulatesChannelBlob
	// (internal/session) and TestResumeFetchesLiveChannelRoster exercise —
	// this is what leaves "channel:#chan" in the blob for the resumed brain
	// below to seed from. Deliberately a bare "alice" source, not a real
	// "alice!ident@host" one: a real self-JOIN echo always carries a full
	// prefix, which Session.applyState's generic self-sourced-line rule
	// (state.go) would learn from directly, defeating the point of this
	// test — it exists specifically to exercise the fallback for when
	// nothing ever revealed the host live at all, which does happen (no
	// CHGHOST/396, and — as here — a network or proxy that doesn't
	// include full userhost on every line).
	fakeSend(t, conn, "fake.example", ":alice JOIN #chan")
	deadline := time.Now().Add(2 * time.Second)
	for {
		d := newFakeServerDL("probe")
		if err := sess1.Attach(d); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, m := range d.snapshot() {
			if m.Command == "JOIN" && m.Param(0) == "#chan" {
				found = true
			}
		}
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("sess1 never saw #chan")
		}
		time.Sleep(10 * time.Millisecond)
	}

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

	// Before the real answer lands, the resumed session's self.Host is
	// unknown — a downlink attaching right now must still get a
	// syntactically valid JOIN, the fallback form, never bare "nick!user".
	early := newFakeServerDL("early")
	if err := sess2.Attach(early); err != nil {
		t.Fatal(err)
	}
	var fallbackJoin, ok bool
	var fallbackSrc string
	for _, m := range early.snapshot() {
		if m.Command == "JOIN" && m.Param(0) == "#chan" {
			fallbackJoin = true
			fallbackSrc = m.Source
		}
	}
	if !fallbackJoin {
		t.Fatalf("resumed attach missing JOIN #chan: %+v", early.snapshot())
	}
	if !strings.Contains(fallbackSrc, "!") || !strings.Contains(fallbackSrc, "@") {
		t.Fatalf("fallback JOIN prefix not RFC-valid: %q", fallbackSrc)
	}
	if strings.HasSuffix(fallbackSrc, "!ident") {
		t.Fatalf("fallback JOIN prefix is the invalid bare nick!user form: %q", fallbackSrc)
	}

	// Resume must have asked the uplink for the real answer too.
	if !waitForLineOnConn(t, conn, 5*time.Second, func(line string) bool {
		return strings.HasPrefix(line, "USERHOST") && strings.Contains(line, "alice")
	}) {
		t.Fatal("resumed brain never queried USERHOST for itself")
	}
	fakeSend(t, conn, "fake.example", ":fake.example 302 alice :alice=+ident@real.example.net")

	// Once the real answer lands, a newly-attaching client sees the real
	// prefix, not the synthetic fallback.
	deadline = time.Now().Add(5 * time.Second)
	for {
		late := newFakeServerDL("late")
		if err := sess2.Attach(late); err != nil {
			t.Fatal(err)
		}
		for _, m := range late.snapshot() {
			if m.Command == "JOIN" && m.Param(0) == "#chan" && m.Source == "alice!ident@real.example.net" {
				ok = true
			}
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("never saw JOIN with the real learned prefix, last snapshot: %+v", late.snapshot())
		}
		time.Sleep(10 * time.Millisecond)
	}

	close(closeAfter)
}
