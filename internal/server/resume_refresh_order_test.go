package server

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/store"
)

// TestResumeQueriesSelfBeforeChannelNames is the regression test for a live
// report: on resume, dialNetworkLocked's resumedAtBoot branch used to send
// RefreshResumedChannelNames' NAMES query to the uplink before
// RefreshSelfUModes' MODE nick / RefreshSelfUserHost's USERHOST — the
// opposite of a normal live registration, where a real ircd tells us our
// own modes essentially immediately after welcome, well before anything
// channel-shaped. Reordered so self (MODE, USERHOST) goes out first and
// channels (NAMES) last, matching that convention.
func TestResumeQueriesSelfBeforeChannelNames(t *testing.T) {
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

	// Leave behind a "channel:#chan" blob entry (same as
	// TestResumeFetchesLiveChannelRoster) so the resumed brain below has a
	// channel to query NAMES for at all.
	conn := fake.lastConn(t)
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

	// Collect raw lines off the uplink until MODE, USERHOST, and NAMES have
	// each shown up once (or timeout), recording arrival order.
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 4096)
	var pending string
	var modeAt, userhostAt, namesAt int = -1, -1, -1
	for i := 0; ; i++ {
		if modeAt >= 0 && userhostAt >= 0 && namesAt >= 0 {
			break
		}
		idx := indexCRLFDemux(pending)
		if idx < 0 {
			n, err := conn.Read(buf)
			if err != nil {
				t.Fatalf("read: %v (mode=%d userhost=%d names=%d)", err, modeAt, userhostAt, namesAt)
			}
			pending += string(buf[:n])
			continue
		}
		line := pending[:idx]
		pending = pending[idx+2:]
		switch {
		case strings.HasPrefix(line, "MODE") && strings.Contains(line, "alice") && modeAt < 0:
			modeAt = i
		case strings.HasPrefix(line, "USERHOST") && userhostAt < 0:
			userhostAt = i
		case strings.HasPrefix(line, "NAMES") && strings.Contains(line, "#chan") && namesAt < 0:
			namesAt = i
		}
	}

	if modeAt > namesAt {
		t.Fatalf("MODE (self umodes) sent after NAMES: mode=%d names=%d", modeAt, namesAt)
	}
	if userhostAt > namesAt {
		t.Fatalf("USERHOST (self host) sent after NAMES: userhost=%d names=%d", userhostAt, namesAt)
	}

	close(closeAfter)
}
