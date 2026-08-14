package server

import (
	"context"
	"net"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/config"
	gobnclog "github.com/MasterBOFH/GoBNC/internal/log"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// demuxFakeIRC accepts one connection and answers a minimal, real
// registration handshake driven by whatever the client actually sends —
// same shape as internal/brain/driver_test.go's fakeIRCServer, duplicated
// here (rather than exported and shared) because it's a handful of lines
// and these are two different packages' test-only fixtures.
type demuxFakeIRC struct {
	ln net.Listener
}

func newDemuxFakeIRC(t *testing.T) *demuxFakeIRC {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return &demuxFakeIRC{ln: ln}
}

func (f *demuxFakeIRC) addr(t *testing.T) (host string, port int) {
	t.Helper()
	h, p, err := net.SplitHostPort(f.ln.Addr().String())
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	portN, err := strconv.Atoi(p)
	if err != nil {
		t.Fatalf("port %q: %v", p, err)
	}
	return h, portN
}

// serveOne accepts one connection, completes a minimal registration using
// whatever nick the client sends (so the fake ircd never itself decides
// the identity — the client's own NICK/USER traffic does), then holds the
// connection open until closeAfter fires or the test ends.
func (f *demuxFakeIRC) serveOne(t *testing.T, server string, closeAfter <-chan struct{}) {
	t.Helper()
	conn, err := f.ln.Accept()
	if err != nil {
		return
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	send := func(line string) { _, _ = conn.Write([]byte(line + "\r\n")) }
	buf := make([]byte, 4096)
	var pending string
	readLine := func() (string, bool) {
		for {
			if i := indexCRLFDemux(pending); i >= 0 {
				line := pending[:i]
				pending = pending[i+2:]
				return line, true
			}
			n, err := conn.Read(buf)
			if err != nil {
				return "", false
			}
			pending += string(buf[:n])
		}
	}

	nick := "nick"
	gotNick, gotUser := false, false
	for {
		line, ok := readLine()
		if !ok {
			return
		}
		switch {
		case hasPrefixDemux(line, "CAP LS"):
			send(":" + server + " CAP * LS :")
		case hasPrefixDemux(line, "NICK "):
			nick = line[len("NICK "):]
			gotNick = true
		case hasPrefixDemux(line, "USER "):
			gotUser = true
		}
		if gotNick && gotUser {
			break
		}
	}
	send(":" + server + " 001 " + nick + " :Welcome")
	send(":" + server + " 002 " + nick + " :Your host is " + server)
	send(":" + server + " 003 " + nick + " :This server was created today")
	send(":" + server + " 004 " + nick + " " + server + " test-1.0 a a")
	send(":" + server + " 005 " + nick + " NICKLEN=30 :are supported by this server")
	send(":" + server + " 376 " + nick + " :End of MOTD")

	if closeAfter == nil {
		return
	}
	<-closeAfter
}

func indexCRLFDemux(s string) int {
	for i := 0; i+1 < len(s); i++ {
		if s[i] == '\r' && s[i+1] == '\n' {
			return i
		}
	}
	return -1
}

func hasPrefixDemux(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func newDemuxTestServer(t *testing.T) *Server {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "t.db")
	cfg.ControlSocket = filepath.Join(dir, "c.sock")

	s, err := New(cfg, gobnclog.New("error", nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	attachTestKeeper(t, s)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s.runCtx = ctx
	s.cancel = cancel
	return s
}

func waitForRegistered(t *testing.T, label string, registered func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if registered() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s: never registered", label)
}

// TestDemuxRoutesLinesToCorrectSession runs two networks through the real
// Server.runDemux (via attachTestKeeper, the same shared Driver+demux shape
// Server.Run sets up for real) against two independent fake ircds, and
// proves demux's per-line routing (sessionByNetwork, keyed on the
// keeper.NetworkID every Driver event carries) delivers each network's
// traffic only to its own Session. If routing were broken — e.g. every
// line handed to whichever Session happened to be registered first,
// instead of the one the line's Network field actually names — one of the
// two Sessions below would end up with the other network's nick instead
// of its own.
func TestDemuxRoutesLinesToCorrectSession(t *testing.T) {
	s := newDemuxTestServer(t)

	fake1 := newDemuxFakeIRC(t)
	fake2 := newDemuxFakeIRC(t)
	h1, p1 := fake1.addr(t)
	h2, p2 := fake2.addr(t)

	// Held open (rather than closed right after the registration burst)
	// so the fake ircd never closes its socket with the driver's own
	// post-registration traffic (further CAP/other lines) still sitting
	// unread in the kernel receive buffer — Close on a socket in that
	// state can send RST instead of a clean FIN, which can silently
	// discard the just-written 001..376 burst before the client ever
	// reads it.
	closeAfter := make(chan struct{})
	t.Cleanup(func() { close(closeAfter) })
	go fake1.serveOne(t, "fake1.example", closeAfter)
	go fake2.serveOne(t, "fake2.example", closeAfter)

	if _, err := s.Store().UpsertNetwork(s.runCtx, store.Network{
		Name: "net1", Host: h1, Port: p1, Nick: "alice", Enabled: true, NickRecovery: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Store().UpsertNetwork(s.runCtx, store.Network{
		Name: "net2", Host: h2, Port: p2, Nick: "bob", Enabled: true, NickRecovery: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.StartNetworkByName("net1"); err != nil {
		t.Fatal(err)
	}
	if err := s.StartNetworkByName("net2"); err != nil {
		t.Fatal(err)
	}

	sess1, err := s.Session("net1")
	if err != nil {
		t.Fatal(err)
	}
	sess2, err := s.Session("net2")
	if err != nil {
		t.Fatal(err)
	}

	waitForRegistered(t, "net1", sess1.Registered)
	waitForRegistered(t, "net2", sess2.Registered)

	if sess1.Nick() != "alice" {
		t.Fatalf("net1 session got nick %q, want alice (cross-wired with net2?)", sess1.Nick())
	}
	if sess2.Nick() != "bob" {
		t.Fatalf("net2 session got nick %q, want bob (cross-wired with net1?)", sess2.Nick())
	}
}

// TestDemuxRoutesDisconnectToSession proves a keeper.EventDisconnected for
// one network is routed (via runDemux's NetworkEvents case) to that
// network's own Session.HandleDisconnect, not dropped or misdirected.
func TestDemuxRoutesDisconnectToSession(t *testing.T) {
	s := newDemuxTestServer(t)

	fake := newDemuxFakeIRC(t)
	h, p := fake.addr(t)
	closeAfter := make(chan struct{})
	go fake.serveOne(t, "fake.example", closeAfter)

	if _, err := s.Store().UpsertNetwork(s.runCtx, store.Network{
		Name: "net1", Host: h, Port: p, Nick: "alice", Enabled: true, NickRecovery: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.StartNetworkByName("net1"); err != nil {
		t.Fatal(err)
	}
	sess, err := s.Session("net1")
	if err != nil {
		t.Fatal(err)
	}
	waitForRegistered(t, "net1", sess.Registered)

	// Prevent the driver's own auto-reconnect from succeeding and flipping
	// Registered() back to true out from under this assertion — closing
	// the listener means any redial attempt fails at the TCP level.
	_ = fake.ln.Close()
	close(closeAfter)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !sess.Registered() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("session still registered after uplink disconnect")
}
