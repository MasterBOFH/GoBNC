package server

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/brain"
	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/keeper"
	gobnclog "github.com/MasterBOFH/GoBNC/internal/log"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

func fakeSend(t *testing.T, conn net.Conn, _ string, line string) {
	t.Helper()
	if _, err := conn.Write([]byte(line + "\r\n")); err != nil {
		t.Fatalf("fakeSend: %v", err)
	}
}

func historyCount(t *testing.T, st *store.Store, networkID int64, target string) int {
	t.Helper()
	msgs, err := st.QueryMessages(context.Background(), store.HistoryQuery{
		NetworkID: networkID,
		Target:    target,
		Limit:     -1,
		Commands:  []string{"PRIVMSG"},
	})
	if err != nil {
		t.Fatalf("QueryMessages: %v", err)
	}
	return len(msgs)
}

func waitForHistoryCount(t *testing.T, st *store.Store, networkID int64, target string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if historyCount(t, st, networkID, target) >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("history count for %s never reached %d (got %d)", target, want, historyCount(t, st, networkID, target))
}

// resumeTestKeeper is a keeper.Manager + keeper.Listener that outlives
// multiple simulated brain restarts within one test — the in-process
// equivalent of a real gobnc-keeper process staying up across repeated
// `gobnc` (re)starts, which is the exact scenario
// TestResumeDoesNotDuplicateHistory needs and attachTestKeeper's
// one-Manager-per-Server shape can't provide.
type resumeTestKeeper struct {
	sockPath string
}

func startResumeTestKeeper(t *testing.T) *resumeTestKeeper {
	t.Helper()
	mgr := keeper.NewManager(1<<20, 4096, nil)
	sockDir := filepath.Join(t.TempDir(), "sock")
	if err := os.Mkdir(sockDir, 0700); err != nil {
		t.Fatalf("mkdir sockDir: %v", err)
	}
	sockPath := filepath.Join(sockDir, "keeper.sock")

	listenerCtx, cancelListener := context.WithCancel(context.Background())
	t.Cleanup(cancelListener)
	l := keeper.NewListener(mgr, nil)
	ready := make(chan struct{})
	go func() {
		close(ready)
		_ = l.Serve(listenerCtx, sockPath)
	}()
	<-ready
	for i := 0; i < 200; i++ {
		if _, err := os.Stat(sockPath); err == nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	return &resumeTestKeeper{sockPath: sockPath}
}

// attach simulates one brain (re)start against rk's persistent keeper —
// the in-process equivalent of Server.bootstrapKeeper followed by Run's
// register-everything-then-SendLiveReady-then-dial-everything sequence
// (see Run's own comment for why that order matters), using the real
// unexported registerNetworkLocked/dialNetworkLocked so this test
// exercises the actual production code path, not a hand-rolled
// simplification of it. Detaching (closing client, per detachFromKeeper's
// own contract) is the caller's job via t.Cleanup, matching how a real
// brain process exiting only ever closes its attach, never anything on
// the keeper itself.
func (rk *resumeTestKeeper) attach(t *testing.T, s *Server, nets []store.Network) *keeper.AttachClient {
	t.Helper()
	attachCtx, cancelAttach := context.WithTimeout(context.Background(), 5*time.Second)
	client, err := keeper.Attach(attachCtx, rk.sockPath, keeper.HelloMsg{Mode: keeper.ModeLive})
	cancelAttach()
	if err != nil {
		t.Fatalf("keeper.Attach: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	s.keeperClient = client
	s.driver = brain.NewDriver(client)
	s.resumedAtBoot = make(map[keeper.NetworkID]bool, len(client.Networks))
	for _, st := range client.Networks {
		if st.State == keeper.Connected {
			s.resumedAtBoot[st.ID] = true
		}
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	t.Cleanup(cancelRun)
	go func() { _ = s.driver.Run(runCtx) }()
	go s.runDemux(runCtx)

	type pending struct {
		n    store.Network
		sess *session.Session
	}
	var toDial []pending
	s.mu.Lock()
	for _, n := range nets {
		sess, err := s.registerNetworkLocked(n)
		if err != nil {
			s.mu.Unlock()
			t.Fatalf("registerNetworkLocked(%s): %v", n.Name, err)
		}
		toDial = append(toDial, pending{n: n, sess: sess})
	}
	s.mu.Unlock()

	if err := client.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}

	s.mu.Lock()
	for _, p := range toDial {
		if err := s.dialNetworkLocked(p.n, p.sess); err != nil {
			s.mu.Unlock()
			t.Fatalf("dialNetworkLocked(%s): %v", p.n.Name, err)
		}
	}
	s.mu.Unlock()

	return client
}

func newResumeTestServer(t *testing.T, dbPath string) *Server {
	t.Helper()
	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.ControlSocket = filepath.Join(filepath.Dir(dbPath), "c.sock")

	s, err := New(cfg, gobnclog.New("error", nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s.runCtx = ctx
	s.cancel = cancel
	return s
}

// TestResumeDoesNotDuplicateHistory reproduces the exact bug found via
// live testing: a brain that restarts against a keeper still holding a
// network's connection live replays that network's entire retained
// backlog from seq 0 unconditionally (see keeper.HelloMsg.FromSeq's doc
// comment for why replay itself is never reduced — Session's state
// reconstruction depends on seeing it all), so without
// store.Message.KeeperSeq's idempotent-insert index, every already-stored
// PRIVMSG got re-inserted as a brand-new row on every resume (confirmed
// live: 500 messages -> 581 rows after one idle restart, on a network
// with no ring eviction pressure at all). This test proves the fix: full
// replay still happens (sess2 reaches Registered() the same way it always
// has, by watching the replayed transcript), but the history count never
// moves.
func TestResumeDoesNotDuplicateHistory(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "t.db")
	rk := startResumeTestKeeper(t)

	// First "brain": register+dial fresh, complete registration, receive
	// and durably store a handful of channel messages.
	s1 := newResumeTestServer(t, dbPath)
	fake := newDemuxFakeIRC(t)
	h, p := fake.addr(t)
	closeAfter := make(chan struct{})
	go fake.serveOne(t, "fake.example", closeAfter)

	if _, err := s1.Store().UpsertNetwork(s1.runCtx, store.Network{
		Name: "net1", Host: h, Port: p, Nick: "alice", Enabled: true, NickRecovery: true,
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

	const nMsgs = 20
	conn := fake.lastConn(t)
	for i := 0; i < nMsgs; i++ {
		fakeSend(t, conn, "fake.example", ":alice PRIVMSG #chan :msg"+strconv.Itoa(i))
	}
	waitForHistoryCount(t, s1.Store(), n.ID, "#chan", nMsgs)

	// "Restart": detach s1 without QUITting the network (matches
	// Server.detachFromKeeper's own contract — see its doc comment), then
	// attach a brand-new Server instance to the same still-running keeper.
	_ = s1.keeperClient.Close()

	s2 := newResumeTestServer(t, dbPath)
	rk.attach(t, s2, []store.Network{n})

	sess2, err := s2.Session("net1")
	if err != nil {
		t.Fatal(err)
	}
	waitForRegistered(t, "net1 (resumed)", sess2.Registered)

	// Give the resumed attach a moment to finish any (incorrect) replay it
	// might still be doing, then assert the count never moved.
	time.Sleep(300 * time.Millisecond)
	got := historyCount(t, s2.Store(), n.ID, "#chan")
	if got != nMsgs {
		t.Fatalf("history rows for #chan after resume = %d, want %d (resume replay duplicated already-stored messages)", got, nMsgs)
	}

	close(closeAfter)
}
