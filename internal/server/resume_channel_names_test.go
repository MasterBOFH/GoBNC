package server

import (
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// fakeServerDL is a minimal session.Downlink for tests in this package —
// resume_history_test.go's fixtures only need Registered()/Nick(), never a
// real attached client, so nothing like this existed here yet.
type fakeServerDL struct {
	id   session.ClientID
	mu   sync.Mutex
	caps map[string]bool
	sent []irc.Message
}

func newFakeServerDL(id string) *fakeServerDL {
	return &fakeServerDL{id: session.ClientID(id), caps: map[string]bool{}}
}

func (f *fakeServerDL) ID() session.ClientID { return f.id }
func (f *fakeServerDL) Caps() map[string]bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]bool, len(f.caps))
	for k, v := range f.caps {
		out[k] = v
	}
	return out
}
func (f *fakeServerDL) HasCap(n string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.caps[n]
}
func (f *fakeServerDL) ClearCap(n string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.caps, n)
}
func (f *fakeServerDL) HasSeenCap(string) bool { return false }
func (f *fakeServerDL) MarkSeenCap(string)     {}
func (f *fakeServerDL) Send(msg irc.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, msg)
	return nil
}
func (f *fakeServerDL) Close() error { return nil }
func (f *fakeServerDL) snapshot() []irc.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]irc.Message(nil), f.sent...)
}

// TestResumeFetchesLiveChannelRoster proves the other half of "a downlink
// connecting after a reload should see JOIN and NAMES": a resumed brain
// doesn't know who's actually in a channel (the blob only ever carried a
// channel's name and key — seedChannelLocked's own doc comment says so
// explicitly), so Session.Attach must not fabricate a self-only 353/366
// for it. Instead a downlink attaching before the roster is known should
// see JOIN with no NAMES at all (ChannelState.RosterKnown gates it), and
// only get the real 353/366 once Session.RefreshResumedChannelNames'
// live NAMES query actually comes back from the uplink — proof the
// eventual roster came from the wire, not a guess. This drives a real
// resumed brain (resumeTestKeeper, the same fixture resume_history_test.go's
// resume tests use) through a real NAMES round trip on the real fake ircd
// connection to confirm both halves.
func TestResumeFetchesLiveChannelRoster(t *testing.T) {
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

	// Uplink auto-joins #chan — nothing a client asked for, the same shape
	// TestAutoJoinRepopulatesChannelBlob (internal/session) exercises at
	// the session level. This is what leaves the "channel:#chan" blob entry
	// behind for the resumed brain below to seed from.
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
	// still-running keeper — the keeper never redials, so conn (above)
	// stays the live uplink socket throughout.
	_ = s1.keeperClient.Close()

	s2 := newResumeTestServer(t, dbPath)
	rk.attach(t, s2, []store.Network{n})

	sess2, err := s2.Session("net1")
	if err != nil {
		t.Fatal(err)
	}
	waitForRegistered(t, "net1 (resumed)", sess2.Registered)

	// A downlink attaching right after resume, before the live NAMES round
	// trip below completes, must still see the JOIN (this is
	// TestDownlinkAfterResumeGetsJoinAndNames's property, exercised here
	// against the real resumed brain rather than a hand-built blob) but
	// must NOT get a fabricated NAMES burst — RosterKnown is false until
	// the real reply below lands, and Attach must respect that.
	early := newFakeServerDL("early")
	if err := sess2.Attach(early); err != nil {
		t.Fatal(err)
	}
	sawJoin, sawFabricatedNames := false, false
	for _, m := range early.snapshot() {
		if m.Command == "JOIN" && m.Param(0) == "#chan" {
			sawJoin = true
		}
		if (m.Command == "353" || m.Command == "366") && len(m.Params) > 2 && m.Params[len(m.Params)-2] == "#chan" {
			sawFabricatedNames = true
		}
	}
	if !sawJoin {
		t.Fatalf("resumed attach missing JOIN #chan: %+v", early.snapshot())
	}
	if sawFabricatedNames {
		t.Fatalf("resumed attach must not fabricate NAMES before the roster is known: %+v", early.snapshot())
	}

	// Read whatever the resumed brain sent the uplink and find the NAMES
	// query RefreshResumedChannelNames should have fired — no client ever
	// asked for it, so if this is missing, nothing repopulates the roster
	// beyond the self-only blob seed. Message.Encode renders a single
	// trailing param with a leading colon ("NAMES :#chan"), so match on
	// content, not exact wire form.
	if !waitForLineOnConn(t, conn, 5*time.Second, func(line string) bool {
		return strings.HasPrefix(line, "NAMES") && strings.Contains(line, "#chan")
	}) {
		t.Fatal("resumed brain never sent NAMES #chan to the uplink")
	}

	// Answer as a real ircd would: two members, not just self.
	fakeSend(t, conn, "fake.example", ":fake.example 353 alice = #chan :alice bob")
	fakeSend(t, conn, "fake.example", ":fake.example 366 alice #chan :End of /NAMES list.")

	// The live reply must reach an attached downlink (broadcast — no
	// client solicited it, see RequestTracker.RouteMessage's fallback) and
	// must actually contain bob, proving it's the real roster and not a
	// second copy of the self-only synthesized one.
	deadline = time.Now().Add(5 * time.Second)
	for {
		for _, m := range early.snapshot() {
			if m.Command == "353" && len(m.Params) >= 4 && m.Params[2] == "#chan" &&
				sliceContains(strings.Fields(m.Params[3]), "bob") {
				close(closeAfter)
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("downlink never got live NAMES with bob: %+v", early.snapshot())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func sliceContains(fields []string, word string) bool {
	for _, f := range fields {
		if f == word {
			return true
		}
	}
	return false
}

// waitForLineOnConn reads lines directly off conn (nothing else is reading
// it once demuxFakeIRC.serveOne's own registration loop has returned) until
// match reports true or timeout elapses.
func waitForLineOnConn(t *testing.T, conn interface {
	Read([]byte) (int, error)
	SetReadDeadline(time.Time) error
}, timeout time.Duration, match func(line string) bool) bool {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	buf := make([]byte, 4096)
	var pending string
	for {
		if i := indexCRLFDemux(pending); i >= 0 {
			line := pending[:i]
			pending = pending[i+2:]
			if match(line) {
				return true
			}
			continue
		}
		n, err := conn.Read(buf)
		if err != nil {
			return false
		}
		pending += string(buf[:n])
	}
}
