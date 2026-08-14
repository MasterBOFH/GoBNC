package brain

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/keeper"
	"github.com/MasterBOFH/GoBNC/internal/registration"
)

// fakeIRCServer accepts one connection and answers a minimal, real
// registration handshake driven by whatever the client (the Driver, via
// the keeper) actually sends — it isn't a script of canned lines, it reads
// NICK/USER and replies with real numerics, so a bug in what the Driver
// sends would show up as a stuck handshake, not a mismatched fixture.
type fakeIRCServer struct {
	ln net.Listener
}

func newFakeIRCServer(t *testing.T) *fakeIRCServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return &fakeIRCServer{ln: ln}
}

func (s *fakeIRCServer) addr() (host string, port int) {
	h, p, _ := net.SplitHostPort(s.ln.Addr().String())
	fmt.Sscanf(p, "%d", &port)
	return h, port
}

func (s *fakeIRCServer) close() { _ = s.ln.Close() }

// serveOne accepts one connection and drives a real, minimal registration:
// CAP LS -> (no caps requested back, in this minimal fixture) -> NICK/USER
// -> 001..005 -> 376. It only reacts to what actually arrives.
func (s *fakeIRCServer) serveOne(t *testing.T) {
	t.Helper()
	conn, err := s.ln.Accept()
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
			if i := indexCRLF(pending); i >= 0 {
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
		case hasPrefix(line, "CAP LS"):
			send(":fake.example CAP * LS :")
		case hasPrefix(line, "NICK "):
			nick = line[len("NICK "):]
			gotNick = true
		case hasPrefix(line, "USER "):
			gotUser = true
		}
		if gotNick && gotUser {
			break
		}
	}
	send(":fake.example 001 " + nick + " :Welcome")
	send(":fake.example 002 " + nick + " :Your host is fake.example")
	send(":fake.example 003 " + nick + " :This server was created today")
	send(":fake.example 004 " + nick + " fake.example test-1.0 a a")
	send(":fake.example 005 " + nick + " NICKLEN=30 :are supported by this server")
	send(":fake.example 376 " + nick + " :End of MOTD")
}

// serveOneThenExtra is serveOne plus one additional post-registration
// line, held open briefly afterward instead of closing immediately —
// proving Lines() delivers traffic that arrives after registration
// completes needs a server that actually sends some.
func (s *fakeIRCServer) serveOneThenExtra(t *testing.T, extra string) {
	t.Helper()
	conn, err := s.ln.Accept()
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
			if i := indexCRLF(pending); i >= 0 {
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
		case hasPrefix(line, "CAP LS"):
			send(":fake.example CAP * LS :")
		case hasPrefix(line, "NICK "):
			nick = line[len("NICK "):]
			gotNick = true
		case hasPrefix(line, "USER "):
			gotUser = true
		}
		if gotNick && gotUser {
			break
		}
	}
	send(":fake.example 001 " + nick + " :Welcome")
	send(":fake.example 002 " + nick + " :Your host is fake.example")
	send(":fake.example 003 " + nick + " :This server was created today")
	send(":fake.example 004 " + nick + " fake.example test-1.0 a a")
	send(":fake.example 005 " + nick + " NICKLEN=30 :are supported by this server")
	send(":fake.example 376 " + nick + " :End of MOTD")
	send(extra)
	time.Sleep(500 * time.Millisecond)
}

// serveOneCaptureAfterRegistration is serveOne, but after completing
// registration it keeps reading and sends every further line the client
// sends to out — for proving something the Driver does automatically
// post-registration (auto-join) actually reaches the real wire, not just
// that Driver believes it sent it.
func (s *fakeIRCServer) serveOneCaptureAfterRegistration(t *testing.T, out chan<- string) {
	t.Helper()
	conn, err := s.ln.Accept()
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
			if i := indexCRLF(pending); i >= 0 {
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
		case hasPrefix(line, "CAP LS"):
			send(":fake.example CAP * LS :")
		case hasPrefix(line, "NICK "):
			nick = line[len("NICK "):]
			gotNick = true
		case hasPrefix(line, "USER "):
			gotUser = true
		}
		if gotNick && gotUser {
			break
		}
	}
	send(":fake.example 001 " + nick + " :Welcome")
	send(":fake.example 002 " + nick + " :Your host is fake.example")
	send(":fake.example 003 " + nick + " :This server was created today")
	send(":fake.example 004 " + nick + " fake.example test-1.0 a a")
	send(":fake.example 005 " + nick + " NICKLEN=30 :are supported by this server")
	send(":fake.example 376 " + nick + " :End of MOTD")

	for {
		line, ok := readLine()
		if !ok {
			return
		}
		out <- line
	}
}

func hasPrefix(s, p string) bool { return len(s) >= len(p) && s[:len(p)] == p }

func indexCRLF(s string) int {
	for i := 0; i+1 < len(s); i++ {
		if s[i] == '\r' && s[i+1] == '\n' {
			return i
		}
	}
	return -1
}

func testSockPath(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "sock")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	return filepath.Join(dir, "keeper.sock")
}

// TestDriverRegistersOverKeeperWire is the fake-server version of the 3a
// proof: a real Manager+Listener+AttachClient, a real (if minimal) ircd on
// the other end of the dialed TCP connection, and Driver in between —
// registration.Step's ActionSend output must survive a full round trip
// through WriteRequest/Keeper.WriteLine/the real socket and back as a
// parsed Line before the Driver reports ActionRegistered. See
// driver_live_test.go for the same proof against real remote ircds.
func TestDriverRegistersOverKeeperWire(t *testing.T) {
	mgr := keeper.NewManager(8192, 4096, nil)
	sockPath := testSockPath(t)

	listenerCtx, cancelListener := context.WithCancel(context.Background())
	defer cancelListener()
	l := keeper.NewListener(mgr, nil)
	ready := make(chan struct{})
	go func() {
		close(ready)
		_ = l.Serve(listenerCtx, sockPath)
	}()
	<-ready
	waitForSocket(t, sockPath)

	srv := newFakeIRCServer(t)
	defer srv.close()
	go srv.serveOne(t)
	host, port := srv.addr()

	attachCtx, cancelAttach := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelAttach()
	client, err := keeper.Attach(attachCtx, sockPath, keeper.HelloMsg{Mode: keeper.ModeLive})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer client.Close()

	const netID keeper.NetworkID = 1
	driver := NewDriver(client)
	driver.RegisterNetwork(netID, NetworkConfig{PrimaryNick: "gobncbrain", NickRecovery: true})

	if err := client.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	runErr := make(chan error, 1)
	go func() { runErr <- driver.Run(runCtx) }()

	if err := client.SendDial(netID, keeper.DialConfig{Host: host, Port: port}, 0); err != nil {
		t.Fatalf("SendDial: %v", err)
	}

	select {
	case dr := <-driver.DialResults():
		if !dr.OK {
			t.Fatalf("DialResult.OK=false: %+v", dr)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no DialResult within timeout")
	}
	if err := driver.StartRegistration(netID); err != nil {
		t.Fatalf("StartRegistration: %v", err)
	}

	select {
	case res := <-driver.Results():
		if res.Network != netID {
			t.Fatalf("Result.Network=%d, want %d", res.Network, netID)
		}
		if res.State.Phase != registration.PhaseComplete {
			t.Fatalf("Result.State.Phase=%v, want complete (err=%v)", res.State.Phase, res.State.Err)
		}
	case err := <-runErr:
		t.Fatalf("driver.Run exited before registering: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatalf("registration did not complete within timeout")
	}
}

// TestDriverLinesRepublishesPostRegistrationTraffic proves the gap this
// round closes: before Driver.Lines existed, a line arriving after
// registration.Step reached a terminal Phase was silently swallowed —
// Step no-ops on a terminal phase by design, and nothing else read the
// line. Here the fake server sends one real PRIVMSG after completing
// registration; it must arrive on driver.Lines(), byte-for-byte, exactly
// like a registration-phase line would.
func TestDriverLinesRepublishesPostRegistrationTraffic(t *testing.T) {
	mgr := keeper.NewManager(8192, 4096, nil)
	sockPath := testSockPath(t)

	listenerCtx, cancelListener := context.WithCancel(context.Background())
	defer cancelListener()
	l := keeper.NewListener(mgr, nil)
	ready := make(chan struct{})
	go func() {
		close(ready)
		_ = l.Serve(listenerCtx, sockPath)
	}()
	<-ready
	waitForSocket(t, sockPath)

	srv := newFakeIRCServer(t)
	defer srv.close()
	const extra = ":someone!u@h PRIVMSG gobncbrain :hello after registration"
	go srv.serveOneThenExtra(t, extra)
	host, port := srv.addr()

	attachCtx, cancelAttach := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelAttach()
	client, err := keeper.Attach(attachCtx, sockPath, keeper.HelloMsg{Mode: keeper.ModeLive})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer client.Close()

	const netID keeper.NetworkID = 1
	driver := NewDriver(client)
	driver.RegisterNetwork(netID, NetworkConfig{PrimaryNick: "gobncbrain", NickRecovery: true})

	if err := client.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	go func() { _ = driver.Run(runCtx) }()

	if err := client.SendDial(netID, keeper.DialConfig{Host: host, Port: port}, 0); err != nil {
		t.Fatalf("SendDial: %v", err)
	}
	select {
	case dr := <-driver.DialResults():
		if !dr.OK {
			t.Fatalf("DialResult.OK=false: %+v", dr)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no DialResult within timeout")
	}
	if err := driver.StartRegistration(netID); err != nil {
		t.Fatalf("StartRegistration: %v", err)
	}
	select {
	case res := <-driver.Results():
		if res.State.Phase != registration.PhaseComplete {
			t.Fatalf("Result.State.Phase=%v, want complete (err=%v)", res.State.Phase, res.State.Err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("registration did not complete within timeout")
	}

	timeout := time.After(5 * time.Second)
	for {
		select {
		case line := <-driver.Lines():
			if string(line.Raw) == extra {
				return // found it
			}
			// A registration-phase line delivered before completion —
			// keep looking.
		case <-timeout:
			t.Fatalf("post-registration line never arrived on driver.Lines()")
		}
	}
}

// TestDriverAutoJoinsOnRegistrationComplete proves SetChannels/joinChannels
// actually reach the wire — the fake server here doesn't send a fixed
// script, it captures whatever the client sends after 376 and the test
// asserts on that, so a bug that made Driver merely believe it joined
// (without a real SendWrite reaching the socket) would show up as no line
// ever arriving, not a false pass.
func TestDriverAutoJoinsOnRegistrationComplete(t *testing.T) {
	mgr := keeper.NewManager(8192, 4096, nil)
	sockPath := testSockPath(t)

	listenerCtx, cancelListener := context.WithCancel(context.Background())
	defer cancelListener()
	l := keeper.NewListener(mgr, nil)
	ready := make(chan struct{})
	go func() {
		close(ready)
		_ = l.Serve(listenerCtx, sockPath)
	}()
	<-ready
	waitForSocket(t, sockPath)

	srv := newFakeIRCServer(t)
	defer srv.close()
	captured := make(chan string, 8)
	go srv.serveOneCaptureAfterRegistration(t, captured)
	host, port := srv.addr()

	attachCtx, cancelAttach := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelAttach()
	client, err := keeper.Attach(attachCtx, sockPath, keeper.HelloMsg{Mode: keeper.ModeLive})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer client.Close()

	const netID keeper.NetworkID = 1
	driver := NewDriver(client)
	driver.RegisterNetwork(netID, NetworkConfig{PrimaryNick: "gobncbrain", NickRecovery: true})
	driver.SetChannels(netID, []ChannelJoin{
		{Name: "#nokey"},
		{Name: "#withkey", Key: "secret"},
	})

	if err := client.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	go func() { _ = driver.Run(runCtx) }()

	if err := client.SendDial(netID, keeper.DialConfig{Host: host, Port: port}, 0); err != nil {
		t.Fatalf("SendDial: %v", err)
	}
	select {
	case dr := <-driver.DialResults():
		if !dr.OK {
			t.Fatalf("DialResult.OK=false: %+v", dr)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no DialResult within timeout")
	}
	if err := driver.StartRegistration(netID); err != nil {
		t.Fatalf("StartRegistration: %v", err)
	}
	select {
	case res := <-driver.Results():
		if res.State.Phase != registration.PhaseComplete {
			t.Fatalf("Result.State.Phase=%v, want complete (err=%v)", res.State.Phase, res.State.Err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("registration did not complete within timeout")
	}

	want := map[string]bool{"JOIN #nokey": false, "JOIN #withkey secret": false}
	deadline := time.After(5 * time.Second)
	for {
		if want["JOIN #nokey"] && want["JOIN #withkey secret"] {
			return
		}
		select {
		case line := <-captured:
			if _, ok := want[line]; ok {
				want[line] = true
			}
		case <-deadline:
			t.Fatalf("did not see both JOINs on the wire within timeout, got: %+v", want)
		}
	}
}

// TestDriverReconnectRedialsAndReregisters proves Driver.Reconnect's two
// halves together: it actually produces a new epoch on the wire (a real
// close-then-redial, not a no-op), and registration.State is genuinely
// reset — a second real registration completes on the new connection,
// which would hang forever if the old, already-terminal State from the
// first connection were left in place (Step no-ops on a terminal phase).
func TestDriverReconnectRedialsAndReregisters(t *testing.T) {
	mgr := keeper.NewManager(8192, 4096, nil)
	sockPath := testSockPath(t)

	listenerCtx, cancelListener := context.WithCancel(context.Background())
	defer cancelListener()
	l := keeper.NewListener(mgr, nil)
	ready := make(chan struct{})
	go func() {
		close(ready)
		_ = l.Serve(listenerCtx, sockPath)
	}()
	<-ready
	waitForSocket(t, sockPath)

	srv := newFakeIRCServer(t)
	defer srv.close()
	go func() {
		srv.serveOne(t) // initial connection
		srv.serveOne(t) // post-reconnect connection, same listener
	}()
	host, port := srv.addr()

	attachCtx, cancelAttach := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelAttach()
	client, err := keeper.Attach(attachCtx, sockPath, keeper.HelloMsg{Mode: keeper.ModeLive})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer client.Close()

	const netID keeper.NetworkID = 1
	driver := NewDriver(client)
	driver.RegisterNetwork(netID, NetworkConfig{PrimaryNick: "gobncbrain", NickRecovery: true})

	if err := client.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	go func() { _ = driver.Run(runCtx) }()

	if err := driver.Dial(netID, keeper.DialConfig{Host: host, Port: port}, 0); err != nil {
		t.Fatalf("Dial: %v", err)
	}
	dr1 := awaitDialResult(t, driver, 5*time.Second)
	if !dr1.OK || dr1.Epoch != 1 {
		t.Fatalf("first DialResult=%+v, want OK=true epoch=1", dr1)
	}
	if err := driver.StartRegistration(netID); err != nil {
		t.Fatalf("StartRegistration: %v", err)
	}
	awaitComplete(t, driver, 10*time.Second)

	if err := driver.Reconnect(netID); err != nil {
		t.Fatalf("Reconnect: %v", err)
	}
	dr2 := awaitDialResult(t, driver, 5*time.Second)
	if !dr2.OK || dr2.Epoch != 2 {
		t.Fatalf("post-reconnect DialResult=%+v, want OK=true epoch=2", dr2)
	}
	if err := driver.StartRegistration(netID); err != nil {
		t.Fatalf("StartRegistration (post-reconnect): %v", err)
	}
	awaitComplete(t, driver, 10*time.Second)
}

func awaitDialResult(t *testing.T, driver *Driver, timeout time.Duration) keeper.DialResultMsg {
	t.Helper()
	select {
	case dr := <-driver.DialResults():
		return dr
	case <-time.After(timeout):
		t.Fatalf("no DialResult within %s", timeout)
		return keeper.DialResultMsg{}
	}
}

func awaitComplete(t *testing.T, driver *Driver, timeout time.Duration) {
	t.Helper()
	select {
	case res := <-driver.Results():
		if res.State.Phase != registration.PhaseComplete {
			t.Fatalf("Result.State.Phase=%v, want complete (err=%v)", res.State.Phase, res.State.Err)
		}
	case <-time.After(timeout):
		t.Fatalf("registration did not complete within %s", timeout)
	}
}

// newAttachedLiveClient stands up a real Manager+Listener on a fresh unix
// socket and returns an already-Hello'd, LiveReady live-mode AttachClient —
// the shared setup for the deadline/disconnect tests below, which only
// differ in what fake server they dial and what Driver option they pass.
func newAttachedLiveClient(t *testing.T) *keeper.AttachClient {
	t.Helper()
	client, _ := newAttachedLiveClientWithManager(t)
	return client
}

// newAttachedLiveClientWithManager is newAttachedLiveClient plus the
// Manager itself, for tests that need to check keeper-level state directly
// (e.g. confirming a network's connection survived something) rather than
// only observing the wire protocol.
func newAttachedLiveClientWithManager(t *testing.T) (*keeper.AttachClient, *keeper.Manager) {
	t.Helper()
	mgr := keeper.NewManager(8192, 4096, nil)
	sockPath := testSockPath(t)

	listenerCtx, cancelListener := context.WithCancel(context.Background())
	t.Cleanup(cancelListener)
	l := keeper.NewListener(mgr, nil)
	ready := make(chan struct{})
	go func() {
		close(ready)
		_ = l.Serve(listenerCtx, sockPath)
	}()
	<-ready
	waitForSocket(t, sockPath)

	attachCtx, cancelAttach := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelAttach()
	client, err := keeper.Attach(attachCtx, sockPath, keeper.HelloMsg{Mode: keeper.ModeLive})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	if err := client.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}
	return client, mgr
}

// bareServer is a TCP listener with no IRC behavior of its own — the two
// serve methods below give it just enough behavior to exercise one Driver
// failure path each: acceptAndHold for the registration-deadline path
// (server accepts the TCP connection and then says nothing, ever — the
// exact shape of a stalled or half-broken ircd), acceptAndHangUp for the
// mid-registration-disconnect path (server accepts and immediately closes).
type bareServer struct{ ln net.Listener }

func newBareServer(t *testing.T) *bareServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return &bareServer{ln: ln}
}

func (s *bareServer) addr() (host string, port int) {
	h, p, _ := net.SplitHostPort(s.ln.Addr().String())
	fmt.Sscanf(p, "%d", &port)
	return h, port
}

func (s *bareServer) close() { _ = s.ln.Close() }

func (s *bareServer) acceptAndHold(t *testing.T, stop <-chan struct{}) {
	t.Helper()
	conn, err := s.ln.Accept()
	if err != nil {
		return
	}
	<-stop
	_ = conn.Close()
}

// acceptAndHangUp accepts one connection, waits briefly, then closes it.
// The wait isn't padding for its own sake: Keeper.Subscribe (which the
// keeper's fan-in goroutine uses to turn a disconnect into a
// NetworkEventMsg) only starts receiving events from the moment it's
// called, with no backlog replay (see Keeper.Subscribe's doc comment —
// "State() is always authoritative" is the documented fallback for
// exactly this gap). Fan-in subscribes on its own goroutine, scheduled
// after Dial returns, so an instant hangup can race it and be missed —
// a real, if narrow, keeper-level property, not the Driver-level thing
// this test exists to prove. A brief, realistic delay (a genuinely
// instant post-accept RST is not what a stalling ircd looks like) keeps
// this test aimed at Driver.handleNetworkEvent instead of at that race.
func (s *bareServer) acceptAndHangUp(t *testing.T) {
	t.Helper()
	conn, err := s.ln.Accept()
	if err != nil {
		return
	}
	time.Sleep(100 * time.Millisecond)
	_ = conn.Close()
}

// awaitFailedResult waits up to timeout for a Result on ch, and fails the
// test unless it's a PhaseFailed result for netID whose error contains want.
func awaitFailedResult(t *testing.T, ch <-chan Result, netID keeper.NetworkID, want string, timeout time.Duration) registration.State {
	t.Helper()
	select {
	case res := <-ch:
		if res.Network != netID {
			t.Fatalf("Result.Network=%v, want %v", res.Network, netID)
		}
		if res.State.Phase != registration.PhaseFailed {
			t.Fatalf("Result.State.Phase=%v, want failed", res.State.Phase)
		}
		if res.State.Err == nil || !strings.Contains(res.State.Err.Error(), want) {
			t.Fatalf("Result.State.Err=%v, want it to contain %q", res.State.Err, want)
		}
		return res.State
	case <-time.After(timeout):
		t.Fatalf("no failed Result within %s", timeout)
		return registration.State{}
	}
}

// TestDriverFailsRegistrationOnDeadline proves the gap flagged in the
// pre-3b regression net is closed: internal/uplink had a flat registration
// timeout, the new path had none at any layer. Here the fake server
// completes the TCP handshake and then says nothing at all — exactly the
// case DefaultRegistrationTimeout exists to catch — and a short
// WithRegistrationTimeout keeps the test fast rather than waiting out the
// real 90s default.
func TestDriverFailsRegistrationOnDeadline(t *testing.T) {
	client := newAttachedLiveClient(t)

	srv := newBareServer(t)
	defer srv.close()
	stop := make(chan struct{})
	defer close(stop)
	go srv.acceptAndHold(t, stop)
	host, port := srv.addr()

	const netID keeper.NetworkID = 1
	driver := NewDriver(client, WithRegistrationTimeout(150*time.Millisecond))
	driver.RegisterNetwork(netID, NetworkConfig{PrimaryNick: "gobncbrain", NickRecovery: true})

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	go func() { _ = driver.Run(runCtx) }()

	if err := client.SendDial(netID, keeper.DialConfig{Host: host, Port: port}, 0); err != nil {
		t.Fatalf("SendDial: %v", err)
	}
	select {
	case dr := <-driver.DialResults():
		if !dr.OK {
			t.Fatalf("DialResult.OK=false: %+v", dr)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no DialResult within timeout")
	}
	if err := driver.StartRegistration(netID); err != nil {
		t.Fatalf("StartRegistration: %v", err)
	}

	awaitFailedResult(t, driver.Results(), netID, "deadline", 2*time.Second)
}

// TestDriverFailsRegistrationOnDisconnect proves the second gap from the
// pre-3b regression net is closed: Driver.Run used to only republish
// NetworkEvent{Disconnected} on d.netEvents, never resolving the pending
// registration.State — a network that dropped mid-handshake left its
// Result channel silent forever. The registration timeout here is
// deliberately long (30s) so a Result arriving well within a couple of
// seconds proves the *disconnect* path fired, not the deadline finally
// expiring.
func TestDriverFailsRegistrationOnDisconnect(t *testing.T) {
	client := newAttachedLiveClient(t)

	srv := newBareServer(t)
	defer srv.close()
	go srv.acceptAndHangUp(t)
	host, port := srv.addr()

	const netID keeper.NetworkID = 1
	driver := NewDriver(client, WithRegistrationTimeout(30*time.Second))
	driver.RegisterNetwork(netID, NetworkConfig{PrimaryNick: "gobncbrain", NickRecovery: true})

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	go func() { _ = driver.Run(runCtx) }()

	if err := client.SendDial(netID, keeper.DialConfig{Host: host, Port: port}, 0); err != nil {
		t.Fatalf("SendDial: %v", err)
	}
	select {
	case dr := <-driver.DialResults():
		if !dr.OK {
			t.Fatalf("DialResult.OK=false: %+v", dr)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no DialResult within timeout")
	}
	if err := driver.StartRegistration(netID); err != nil {
		t.Fatalf("StartRegistration: %v", err)
	}

	awaitFailedResult(t, driver.Results(), netID, "disconnected", 5*time.Second)
}

// TestDriverNoSpuriousResultAfterCompletion proves a network that finishes
// registering normally, before its deadline, never produces a second
// Result once that deadline's original wall-clock time passes — the
// correctness guard is failRegistration's Phase check (see its doc
// comment), not merely disarmDeadline stopping the timer as an
// optimization; this test pins that guard directly via revert-and-confirm.
func TestDriverNoSpuriousResultAfterCompletion(t *testing.T) {
	client := newAttachedLiveClient(t)

	srv := newFakeIRCServer(t)
	defer srv.close()
	go srv.serveOne(t)
	host, port := srv.addr()

	const netID keeper.NetworkID = 1
	driver := NewDriver(client, WithRegistrationTimeout(150*time.Millisecond))
	driver.RegisterNetwork(netID, NetworkConfig{PrimaryNick: "gobncbrain", NickRecovery: true})

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	go func() { _ = driver.Run(runCtx) }()

	if err := client.SendDial(netID, keeper.DialConfig{Host: host, Port: port}, 0); err != nil {
		t.Fatalf("SendDial: %v", err)
	}
	select {
	case dr := <-driver.DialResults():
		if !dr.OK {
			t.Fatalf("DialResult.OK=false: %+v", dr)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no DialResult within timeout")
	}
	if err := driver.StartRegistration(netID); err != nil {
		t.Fatalf("StartRegistration: %v", err)
	}

	select {
	case res := <-driver.Results():
		if res.State.Phase != registration.PhaseComplete {
			t.Fatalf("first Result.State.Phase=%v, want complete (err=%v)", res.State.Phase, res.State.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no Result within timeout")
	}

	// The deadline armed at StartRegistration was 150ms; wait comfortably
	// past that and confirm nothing else ever arrives on Results().
	select {
	case res := <-driver.Results():
		t.Fatalf("got a second Result after completion: %+v", res)
	case <-time.After(500 * time.Millisecond):
	}
}

// TestBrainExitSendsNoQuit is the test the whole keeper/brain split exists
// to make possible: canceling Driver.Run's context — standing in for the
// brain process itself exiting, e.g. for a code reload — must never send
// QUIT and must never disconnect the uplink. QuitNetwork is the only
// Driver method that can produce a QUIT (see its doc comment); Run
// returning does not call it, and this test proves that by inspection of
// the actual bytes the fake server receives, not just by reading the
// source. It also confirms the positive side of the same claim: after Run
// has returned, the keeper's Manager still reports the network Connected —
// the socket genuinely survived the brain going away.
func TestBrainExitSendsNoQuit(t *testing.T) {
	client, mgr := newAttachedLiveClientWithManager(t)

	srv := newFakeIRCServer(t)
	defer srv.close()
	acceptedCh := make(chan net.Conn, 1)
	go func() {
		conn, err := srv.ln.Accept()
		if err == nil {
			acceptedCh <- conn
		}
	}()
	host, port := srv.addr()

	const netID keeper.NetworkID = 1
	driver := NewDriver(client)

	runCtx, cancelRun := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- driver.Run(runCtx) }()

	if err := client.SendDial(netID, keeper.DialConfig{Host: host, Port: port}, 0); err != nil {
		t.Fatalf("SendDial: %v", err)
	}
	select {
	case dr := <-driver.DialResults():
		if !dr.OK {
			t.Fatalf("DialResult.OK=false: %+v", dr)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no DialResult within timeout")
	}

	var conn net.Conn
	select {
	case conn = <-acceptedCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("fake server never accepted a connection")
	}
	defer conn.Close()

	// Simulate the brain exiting — a code reload, nothing more. No
	// QuitNetwork call anywhere in this test. Canceling runCtx alone isn't
	// a faithful simulation: Driver.Run blocks in client.Next(), a plain
	// network read with no ctx-tied deadline, so ctx cancellation alone
	// cannot unblock it (Run only re-checks ctx.Err() between frames — see
	// Run's doc comment, added after this test caught it). A real brain
	// process exiting doesn't rely on that either: process teardown closes
	// every file descriptor, including the attach socket, which is exactly
	// what closing client here reproduces.
	cancelRun()
	client.Close()
	select {
	case <-runErr:
	case <-time.After(5 * time.Second):
		t.Fatalf("driver.Run did not return after its context was canceled and the connection closed")
	}

	// Nothing sent on the uplink at all, let alone QUIT — give the fake
	// server a bounded window to prove silence, not just absence of a
	// positive signal.
	_ = conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	if err == nil {
		t.Fatalf("uplink received %q after brain exit, want nothing at all", buf[:n])
	} else if !isTimeout(err) {
		t.Fatalf("unexpected read error (want a plain timeout, meaning silence): %v", err)
	}

	// The positive claim: the keeper is still holding the connection.
	k := mgr.Network(netID)
	if k == nil {
		t.Fatalf("Manager has no entry for network %d after brain exit", netID)
	}
	if st, _ := k.State(); st != keeper.Connected {
		t.Fatalf("network state after brain exit=%v, want Connected — the keeper must keep holding the uplink", st)
	}
}

func isTimeout(err error) bool {
	type timeouter interface{ Timeout() bool }
	te, ok := err.(timeouter)
	return ok && te.Timeout()
}

func waitForSocket(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.Dial("unix", path); err == nil {
			_ = c.Close()
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("socket %s never came up", path)
}
