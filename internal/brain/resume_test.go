package brain

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/keeper"
	"github.com/MasterBOFH/GoBNC/internal/registration"
)

// resumeFakeIRCServer accepts one connection, drives a real minimal
// registration handshake exactly like fakeIRCServer.serveOne — including
// real CAP negotiation (unlike fakeIRCServer's own empty CAP LS, which
// short-circuits straight to CAP END and never exercises the CAP REQ/ACK
// exchange at all) — and then, unlike serveOne, which closes right after,
// keeps the connection open, watching for anything further it receives.
//
// This is what makes the bug this test guards against observable exactly
// the way it was reported live: RegisterNetwork (fresh State) fed a
// resumed network's replayed backlog re-drives registration.Step, and its
// reaction to the replayed CAP LS is a genuine ActionSend{"CAP REQ ..."}
// written back out to this exact, still-live connection. A real ircd does
// not reject that as invalid just because the capabilities are already
// active — per IRCv3's CAP spec, re-requesting an already-enabled
// capability is valid and MUST be acknowledged the same as a first
// request — so it genuinely answers with a second CAP ACK, which is
// exactly what fakeAck below does, then the reply is pushed back through
// the keeper exactly like any other live line and reaches the resumed
// Driver's own Lines() stream, which is what a real Session downstream
// would see as "a second CAP ACK further down the buffer." A correct
// resume must never write CAP REQ (or anything else) here after the
// original handshake, so this exchange must never happen at all.
type resumeFakeIRCServer struct {
	ln    net.Listener
	extra chan string
}

func newResumeFakeIRCServer(t *testing.T) *resumeFakeIRCServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return &resumeFakeIRCServer{ln: ln, extra: make(chan string, 16)}
}

func (s *resumeFakeIRCServer) addr() (host string, port int) {
	h, p, _ := net.SplitHostPort(s.ln.Addr().String())
	fmt.Sscanf(p, "%d", &port)
	return h, port
}

func (s *resumeFakeIRCServer) close() { _ = s.ln.Close() }

// serveOneThenWatch registers the nick handshake, then keeps reading and
// publishing anything further received to s.extra until the connection
// closes or ctx is done.
func (s *resumeFakeIRCServer) serveOneThenWatch(ctx context.Context, t *testing.T) net.Conn {
	t.Helper()
	conn, err := s.ln.Accept()
	if err != nil {
		return nil
	}
	_ = conn.SetDeadline(time.Time{})

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

	// ackCapReq answers a "CAP REQ :<caps>" line the same way a real ircd
	// does — echoing the requested capabilities back as ACK'd, using "*"
	// as the target (the standard pre-registration placeholder; real
	// servers use this too, before a nick is assigned) — regardless of
	// whether they were already active, matching IRCv3's own
	// already-enabled-is-still-valid rule (see this type's own doc
	// comment). Returns false if line isn't a CAP REQ.
	ackCapReq := func(line string) bool {
		if !hasPrefix(line, "CAP REQ") {
			return false
		}
		trailing := ""
		if i := strings.Index(line, ":"); i >= 0 {
			trailing = line[i+1:]
		}
		send(":fake.example CAP * ACK :" + trailing)
		return true
	}

	nick := "nick"
	gotNick, gotUser, gotCapEnd := false, false, false
	for {
		line, ok := readLine()
		if !ok {
			return conn
		}
		switch {
		case hasPrefix(line, "CAP LS"):
			// Real capabilities, not an empty list: only this exercises
			// the CAP REQ/ACK round trip a resend would spuriously repeat —
			// an empty offer short-circuits straight to CAP END (see
			// registration.Step's stepCAP "LS" case), never producing the
			// duplicate CAP ACK actually reported live.
			send(":fake.example CAP * LS :message-tags server-time")
		case ackCapReq(line):
			// handled above
		case hasPrefix(line, "NICK "):
			nick = line[len("NICK "):]
			gotNick = true
		case hasPrefix(line, "USER "):
			gotUser = true
		case line == "CAP END":
			gotCapEnd = true
		}
		if gotNick && gotUser && gotCapEnd {
			break
		}
	}
	send(":fake.example 001 " + nick + " :Welcome")
	send(":fake.example 002 " + nick + " :Your host is fake.example")
	send(":fake.example 003 " + nick + " :This server was created today")
	send(":fake.example 004 " + nick + " fake.example test-1.0 a a")
	send(":fake.example 005 " + nick + " NICKLEN=30 :are supported by this server")
	send(":fake.example 376 " + nick + " :End of MOTD")

	go func() {
		for {
			line, ok := readLine()
			if !ok {
				return
			}
			// Answer a resent CAP REQ exactly like the handshake above did
			// — this is the step that turns a resent request into the
			// live, genuine second CAP ACK actually reported (see this
			// type's own doc comment): the reply gets read back by the
			// keeper's own ongoing readLoop on this same connection and
			// pushed live to whatever's subscribed, same as any other
			// traffic.
			ackCapReq(line)
			select {
			case s.extra <- line:
			case <-ctx.Done():
				return
			}
		}
	}()
	return conn
}

// TestResumeDoesNotRedriveRegistration reproduces, at the Driver level,
// exactly the bug reported live: a second brain attaching to a keeper that
// already holds a network's uplink connected saw a genuine second CAP ACK
// further down its buffer. Root cause: registerNetworkLocked called
// RegisterNetwork (fresh registration.State) unconditionally, including for
// a resumed network, so the keeper's unconditional full-backlog replay (see
// keeper.HelloMsg.FromSeq's doc comment) re-drove registration.Step over
// the original CAP/NICK/USER/welcome transcript, whose ActionSend actions
// really did get written back out to the still-live uplink — the real
// ircd answered for real, producing a second, spurious registration burst
// (a real second CAP ACK, real re-joins, etc.), compounding on every
// subsequent brain restart since each pass appends more duplicate content
// to the keeper's own retained ring.
//
// This test dials and registers a network through one Driver (simulating
// the first brain), then attaches a second Driver to the same keeper
// Manager without ever closing the underlying connection (simulating a
// brain restart against a keeper that kept the uplink alive), and uses
// RegisterResumedNetwork instead of RegisterNetwork on the second
// attach — the fix's whole point. It asserts the fake ircd on the other
// end of the wire never receives another byte, and the second Driver never
// republishes a Result — both would fire immediately if the bug were still
// present.
// attachLiveRetrying retries a ModeLive Attach while the keeper still
// reports the previous live attach as active. A prior client's Close()
// only closes that client's own socket; the keeper's own handleConn
// goroutine has to notice the resulting EOF and run its deferred cleanup
// (clearing liveAttached — see internal/keeper/listener.go) before a new
// ModeLive attach can succeed, and nothing about Close() returning waits
// for that server-side cleanup to finish. Confirmed flaking under
// `go test -race` (which slows and reorders scheduling enough to lose this
// race far more often than a fast local run does) — not a keeper bug,
// just this test assuming an ordering Close() never promised.
func attachLiveRetrying(t *testing.T, sockPath string) *keeper.AttachClient {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		client, err := keeper.Attach(ctx, sockPath, keeper.HelloMsg{Mode: keeper.ModeLive})
		cancel()
		if err == nil {
			return client
		}
		if !strings.Contains(err.Error(), "a live attach is already active") || time.Now().After(deadline) {
			t.Fatalf("Attach (second brain): %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestResumeDoesNotRedriveRegistration(t *testing.T) {
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

	srv := newResumeFakeIRCServer(t)
	defer srv.close()
	srvCtx, cancelSrv := context.WithCancel(context.Background())
	defer cancelSrv()
	connCh := make(chan net.Conn, 1)
	go func() { connCh <- srv.serveOneThenWatch(srvCtx, t) }()
	host, port := srv.addr()

	const netID keeper.NetworkID = 1

	// First brain: fresh dial, real registration.
	attachCtx1, cancelAttach1 := context.WithTimeout(context.Background(), 5*time.Second)
	client1, err := keeper.Attach(attachCtx1, sockPath, keeper.HelloMsg{Mode: keeper.ModeLive})
	cancelAttach1()
	if err != nil {
		t.Fatalf("Attach (first brain): %v", err)
	}

	driver1 := NewDriver(client1)
	driver1.RegisterNetwork(netID, NetworkConfig{PrimaryNick: "gobncbrain"})
	if err := client1.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady (first brain): %v", err)
	}
	run1Ctx, cancelRun1 := context.WithCancel(context.Background())
	go func() { _ = driver1.Run(run1Ctx) }()

	if err := client1.SendDial(netID, keeper.DialConfig{Host: host, Port: port}, 0); err != nil {
		t.Fatalf("SendDial: %v", err)
	}
	select {
	case dr := <-driver1.DialResults():
		if !dr.OK {
			t.Fatalf("DialResult.OK=false: %+v", dr)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no DialResult within timeout")
	}
	if err := driver1.StartRegistration(netID); err != nil {
		t.Fatalf("StartRegistration: %v", err)
	}
	select {
	case res := <-driver1.Results():
		if res.State.Phase != registration.PhaseComplete {
			t.Fatalf("first brain: Result.State.Phase=%v, want complete (err=%v)", res.State.Phase, res.State.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("first brain: registration did not complete within timeout")
	}

	// "Restart": detach the first brain's attach WITHOUT closing the
	// uplink — matches Server.detachFromKeeper's own contract (a brain
	// exiting never disconnects any uplink; see QuitNetwork's doc
	// comment) — the keeper keeps holding netID connected throughout.
	cancelRun1()
	if err := client1.Close(); err != nil {
		t.Fatalf("client1.Close: %v", err)
	}

	// Second brain resumes: attach fresh, and use RegisterResumedNetwork
	// (the fix) instead of RegisterNetwork.
	client2 := attachLiveRetrying(t, sockPath)
	defer client2.Close()

	foundResumed := false
	for _, st := range client2.Networks {
		if st.ID == netID {
			if st.State != keeper.Connected {
				t.Fatalf("second brain: netID state=%v, want Connected (keeper should still hold it live)", st.State)
			}
			foundResumed = true
		}
	}
	if !foundResumed {
		t.Fatalf("second brain: netID not reported in HelloAck.Networks")
	}

	driver2 := NewDriver(client2)
	driver2.RegisterResumedNetwork(netID, NetworkConfig{PrimaryNick: "gobncbrain"})
	// Deliberately no Dial and no StartRegistration — matches
	// Server.dialNetworkLocked's own "resumed: skip Dial" branch.

	run2Ctx, cancelRun2 := context.WithCancel(context.Background())
	defer cancelRun2()
	go func() { _ = driver2.Run(run2Ctx) }()

	if err := client2.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady (second brain): %v", err)
	}

	// Drain driver2.Lines() throughout — the resumed network's entire
	// retained backlog (including the original, once-real CAP ACK) is
	// *supposed* to arrive here; that's the intended, unconditional replay
	// Session's own state reconstruction depends on (see
	// keeper.HelloMsg.FromSeq's doc comment — replay is deliberately never
	// reduced), not a symptom of anything wrong. It must never be mistaken
	// for the bug: the bug is specifically new bytes reaching the real
	// ircd and a re-fired Result, which is what the two cases below check.
	go func() {
		for {
			select {
			case _, ok := <-driver2.Lines():
				if !ok {
					return
				}
			case <-run2Ctx.Done():
				return
			}
		}
	}()

	// Give the full backlog replay time to actually flow through the
	// second Driver — this is the window during which the bug's
	// re-registration writes would reach the wire (and, per this test's
	// fake ircd auto-ACKing a resent CAP REQ exactly like a real one
	// would — see resumeFakeIRCServer's doc comment — during which a
	// genuinely new CAP ACK would come back from the real ircd). Neither
	// of these two checks is looking at Lines() — a legitimate reappearance
	// of the original CAP ACK there is expected and correct, not the bug.
	select {
	case res := <-driver2.Results():
		t.Fatalf("second brain: got a Result from a resumed network that was never (re)dialed: %+v — registration was redriven", res)
	case line := <-srv.extra:
		t.Fatalf("fake ircd received an unexpected line after the original handshake: %q — registration was resent live", line)
	case <-time.After(500 * time.Millisecond):
		// Expected: nothing happened.
	}
}
