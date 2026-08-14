package keeper

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

const testNetID NetworkID = 1
const testNetID2 NetworkID = 2
const testNetID3 NetworkID = 3
const testNetID4 NetworkID = 4

// dialedTestNetwork registers id with mgr and dials it to a fakeServer,
// returning handles to drive traffic through it.
func dialedTestNetwork(t *testing.T, mgr *Manager, id NetworkID, ringCap int) (*Keeper, *fakeServer, net.Conn) {
	t.Helper()
	srv := newFakeServer(t)
	t.Cleanup(srv.close)
	k := mgr.EnsureNetwork(id)
	host, port := hostPort(srv.addr())
	acceptedCh := make(chan net.Conn, 1)
	go func() { acceptedCh <- srv.accept(t) }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := k.Dial(ctx, DialConfig{Host: host, Port: port}); err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = k.Close() })
	conn := <-acceptedCh
	return k, srv, conn
}

// dialedTestManager is the single-network convenience wrapper most tests
// use: one Manager, one network already dialed to a fake server.
func dialedTestManager(t *testing.T, ringCap int) (*Manager, *Keeper, *fakeServer, net.Conn) {
	t.Helper()
	mgr := NewManager(8192, ringCap, nil)
	k, srv, conn := dialedTestNetwork(t, mgr, testNetID, ringCap)
	return mgr, k, srv, conn
}

func startTestListener(t *testing.T, mgr *Manager, opts ...ListenerOption) (sockPath string) {
	t.Helper()
	// t.TempDir() is not 0700 by default (typically 0755) — Serve now
	// refuses to bind inside a directory that isn't, which is the whole
	// point of ensureSocketDir. Make a 0700 subdirectory rather than
	// loosen the check for tests.
	dir := filepath.Join(t.TempDir(), "sock")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	sockPath = filepath.Join(dir, "keeper.sock")
	l := NewListener(mgr, nil, opts...)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ready := make(chan struct{})
	go func() {
		// net.Listen inside Serve happens synchronously before Accept, but
		// give it a moment before tests dial to avoid a connection-refused
		// race on a slow CI box.
		close(ready)
		_ = l.Serve(ctx, sockPath)
	}()
	<-ready
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.Dial("unix", sockPath); err == nil {
			_ = c.Close()
			return sockPath
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("listener socket %s never came up", sockPath)
	return ""
}

func TestListenerLiveModeStreamsRealLines(t *testing.T) {
	mgr, k, srv, conn := dialedTestManager(t, 4096)
	sockPath := startTestListener(t, mgr)

	srv.send(t, conn, "NOTICE * :line1")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && k.LastSeq() < 1 {
		time.Sleep(5 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := Attach(ctx, sockPath, HelloMsg{Mode: ModeLive})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer client.Close()
	if client.NegotiatedVersion != ProtocolVersion {
		t.Fatalf("NegotiatedVersion=%d, want %d", client.NegotiatedVersion, ProtocolVersion)
	}
	if client.Mode != ModeLive {
		t.Fatalf("Mode=%v, want live", client.Mode)
	}
	if len(client.Networks) != 1 || client.Networks[0].ID != testNetID {
		t.Fatalf("Networks=%+v, want exactly [%d]", client.Networks, testNetID)
	}

	if err := client.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}

	// Backlog line first.
	line, err := client.NextLine()
	if err != nil {
		t.Fatalf("NextLine (backlog): %v", err)
	}
	if line.Network != testNetID || line.Seq != 1 || string(line.Raw) != "NOTICE * :line1" {
		t.Fatalf("backlog line = %+v, want network=%d seq=1 raw=%q", line, testNetID, "NOTICE * :line1")
	}

	// Now a genuinely live line, sent after LiveReady.
	srv.send(t, conn, "NOTICE * :line2")
	line, err = client.NextLine()
	if err != nil {
		t.Fatalf("NextLine (live): %v", err)
	}
	if line.Network != testNetID || line.Seq != 2 || string(line.Raw) != "NOTICE * :line2" {
		t.Fatalf("live line = %+v, want network=%d seq=2 raw=%q", line, testNetID, "NOTICE * :line2")
	}
}

// TestListenerMultiNetworkIsolationAndInterleave is the core multi-network
// claim: two networks share one connection, each keeps its own seq space,
// and the client can tell them apart. One network's traffic volume must not
// perturb the other's seq numbering.
func TestListenerMultiNetworkIsolationAndInterleave(t *testing.T) {
	mgr := NewManager(8192, 4096, nil)
	k1, srv1, conn1 := dialedTestNetwork(t, mgr, testNetID, 4096)
	k2, srv2, conn2 := dialedTestNetwork(t, mgr, testNetID2, 4096)
	sockPath := startTestListener(t, mgr)

	// Network 1 gets three lines before anyone attaches; network 2 gets
	// none yet — their seq spaces must not interact.
	for i := 0; i < 3; i++ {
		srv1.send(t, conn1, fmt.Sprintf("NOTICE * :net1-%d", i))
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && k1.LastSeq() < 3 {
		time.Sleep(5 * time.Millisecond)
	}
	if k2.LastSeq() != 0 {
		t.Fatalf("network 2 LastSeq=%d before any traffic, want 0", k2.LastSeq())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := Attach(ctx, sockPath, HelloMsg{Mode: ModeLive})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer client.Close()
	if len(client.Networks) != 2 {
		t.Fatalf("Networks=%+v, want 2 entries", client.Networks)
	}
	if err := client.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}

	srv2.send(t, conn2, "NOTICE * :net2-0")

	got := map[NetworkID][]string{}
	deadlineRecv := time.Now().Add(3 * time.Second)
	for len(got[testNetID]) < 3 || len(got[testNetID2]) < 1 {
		if time.Now().After(deadlineRecv) {
			t.Fatalf("timed out; got so far: %+v", got)
		}
		line, err := client.NextLine()
		if err != nil {
			t.Fatalf("NextLine: %v", err)
		}
		got[line.Network] = append(got[line.Network], string(line.Raw))
		if line.Network == testNetID && line.Seq != uint64(len(got[testNetID])) {
			t.Fatalf("network 1 seq out of order: got seq=%d as its %dth line", line.Seq, len(got[testNetID]))
		}
		if line.Network == testNetID2 && line.Seq != uint64(len(got[testNetID2])) {
			t.Fatalf("network 2 seq out of order: got seq=%d as its %dth line", line.Seq, len(got[testNetID2]))
		}
	}
	if got[testNetID][0] != "NOTICE * :net1-0" || got[testNetID][2] != "NOTICE * :net1-2" {
		t.Fatalf("network 1 lines wrong: %v", got[testNetID])
	}
	if got[testNetID2][0] != "NOTICE * :net2-0" {
		t.Fatalf("network 2 lines wrong: %v", got[testNetID2])
	}
}

func TestListenerValidateModeNeverDeliversLines(t *testing.T) {
	mgr, _, srv, conn := dialedTestManager(t, 4096)
	sockPath := startTestListener(t, mgr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := Attach(ctx, sockPath, HelloMsg{Mode: ModeValidate})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer client.Close()
	if client.Mode != ModeValidate {
		t.Fatalf("Mode=%v, want validate", client.Mode)
	}
	if err := client.SendValidateReady(); err != nil {
		t.Fatalf("SendValidateReady: %v", err)
	}

	// Real traffic flows through the keeper while this connection sits in
	// validate mode — none of it may ever reach this client, regardless of
	// what it asks for. Enforcement must be keeper-side, not by the client
	// simply not asking for lines.
	for i := 0; i < 5; i++ {
		srv.send(t, conn, fmt.Sprintf("NOTICE * :line%d", i))
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = client.NextLine() // must not return a msgLine
	}()
	select {
	case <-done:
		t.Fatalf("validate-mode connection received something despite no line ever being sent to it")
	case <-time.After(500 * time.Millisecond):
		// Correct: nothing arrived.
	}
}

func TestListenerRejectsSecondLiveAttach(t *testing.T) {
	mgr, _, _, _ := dialedTestManager(t, 4096)
	sockPath := startTestListener(t, mgr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	first, err := Attach(ctx, sockPath, HelloMsg{Mode: ModeLive})
	if err != nil {
		t.Fatalf("first Attach: %v", err)
	}
	defer first.Close()
	if err := first.SendLiveReady(); err != nil {
		t.Fatalf("first SendLiveReady: %v", err)
	}

	_, err = Attach(ctx, sockPath, HelloMsg{Mode: ModeLive})
	if err == nil {
		t.Fatalf("second live Attach succeeded, want rejection")
	}

	// A validate attach must still be allowed concurrently with the live one.
	v, err := Attach(ctx, sockPath, HelloMsg{Mode: ModeValidate})
	if err != nil {
		t.Fatalf("validate Attach while live attach active: %v", err)
	}
	v.Close()

	// After the live client goes away, a new live attach must succeed.
	first.Close()
	deadline := time.Now().Add(2 * time.Second)
	var third *AttachClient
	for time.Now().Before(deadline) {
		third, err = Attach(ctx, sockPath, HelloMsg{Mode: ModeLive})
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("live Attach after prior live client closed: %v", err)
	}
	third.Close()
}

// TestListenerVersionMismatchRejected sends a raw Hello over the wire
// rather than through AttachClient.Attach, which defaults ClientVersion==0
// to ProtocolVersion as a caller convenience — exactly the value this test
// needs to not be substituted, since it's testing what happens when a peer
// genuinely claims an incompatible version.
func TestListenerVersionMismatchRejected(t *testing.T) {
	mgr, _, _, _ := dialedTestManager(t, 4096)
	sockPath := startTestListener(t, mgr)

	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if err := writeFrame(conn, msgHello, HelloMsg{ClientVersion: 0, Mode: ModeLive}); err != nil {
		t.Fatalf("writeFrame: %v", err)
	}
	gotType, body, err := readFrame(conn)
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	if gotType != msgError {
		t.Fatalf("got frame type %v, want msgError", gotType)
	}
	errMsg, err := decodeFrame[ErrorMsg](gotType, msgError, body)
	if err != nil {
		t.Fatalf("decodeFrame: %v", err)
	}
	if errMsg.Reason == "" {
		t.Fatalf("ErrorMsg.Reason is empty")
	}
}

func TestListenerEvictedSeqReportsError(t *testing.T) {
	mgr, k, srv, conn := dialedTestManager(t, 3) // tiny ring
	sockPath := startTestListener(t, mgr)

	for i := 0; i < 10; i++ {
		srv.send(t, conn, fmt.Sprintf("NOTICE * :line%d", i))
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && k.LastSeq() < 10 {
		time.Sleep(5 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := Attach(ctx, sockPath, HelloMsg{Mode: ModeLive, FromSeq: map[NetworkID]uint64{testNetID: 1}})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer client.Close()
	if err := client.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}
	if _, err := client.NextLine(); err == nil {
		t.Fatalf("NextLine succeeded for an evicted seq, want an error")
	}
}

// TestListenerKillsConnectionOnSlowClientOverflow is the revised overflow
// policy at the protocol layer: a live client that can't keep up gets its
// connection killed with an explicit error, not a silently holed stream.
// The keeper's own ring/read loop must stay unaffected — proven by reading
// LastSeq keeps advancing normally throughout, from a second, well-behaved
// vantage point (Since, not the stalled connection).
func TestListenerKillsConnectionOnSlowClientOverflow(t *testing.T) {
	mgr, k, srv, conn := dialedTestManager(t, 100000) // large ring: the client buffer overflows first
	sockPath := startTestListener(t, mgr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := Attach(ctx, sockPath, HelloMsg{Mode: ModeLive})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer client.Close()
	if err := client.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}
	// Deliberately never call client.NextLine() in a loop — this client
	// stalls, simulating a brain that's wedged or too slow.

	for i := 0; i < 2000; i++ {
		srv.send(t, conn, fmt.Sprintf("NOTICE * :flood%d", i))
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && k.LastSeq() < 2000 {
		time.Sleep(5 * time.Millisecond)
	}
	if k.LastSeq() != 2000 {
		t.Fatalf("keeper's own read loop stalled: LastSeq=%d, want 2000 (a slow subscriber must never block reading)", k.LastSeq())
	}

	// The stalled connection must eventually be killed with an error frame,
	// not just silently stop delivering.
	sawErr := false
	killDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(killDeadline) {
		if _, err := client.NextLine(); err != nil {
			sawErr = true
			break
		}
	}
	if !sawErr {
		t.Fatalf("stalled connection was never killed")
	}
}

// TestRemoveNetworkWhileLiveStreamingStopsFanIn proves the fan-in goroutine
// for a network actually exits when that network is removed out from under
// an active live-attached connection — the leak Keeper.Retire exists to
// prevent. Asserted two ways: goroutine count returns to baseline, and the
// connection keeps working normally for the network that wasn't removed.
func TestRemoveNetworkWhileLiveStreamingStopsFanIn(t *testing.T) {
	mgr := NewManager(8192, 4096, nil)
	k1, srv1, conn1 := dialedTestNetwork(t, mgr, testNetID, 4096)
	_, srv2, conn2 := dialedTestNetwork(t, mgr, testNetID2, 4096)
	sockPath := startTestListener(t, mgr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := Attach(ctx, sockPath, HelloMsg{Mode: ModeLive})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer client.Close()
	if err := client.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}

	srv1.send(t, conn1, "NOTICE * :net1-before-remove")
	line, err := client.NextLine()
	if err != nil || line.Network != testNetID {
		t.Fatalf("NextLine before remove: line=%+v err=%v", line, err)
	}

	// Baseline is taken with both fan-in goroutines already running (one
	// per network). RemoveNetwork->Retire always stops the removed
	// network's own readLoop goroutine (that's plain Keeper.Close, true
	// regardless of this test's fix) — that alone drops the count by 1 and
	// would make a real fan-in leak look fixed if that's all this test
	// checked. The fan-in goroutine is a second, separate goroutine in
	// listener.go; catching its leak specifically requires asserting a
	// drop of at least 2, not just "any drop." Confirmed empirically: with
	// the Retire fix reverted to a plain Close, this scenario drops the
	// count by exactly 1 (readLoop only) and a >=1 assertion would have
	// passed anyway — the >=2 threshold below is load-bearing, not
	// decorative.
	runtime.GC()
	baseline := runtime.NumGoroutine()

	mgr.RemoveNetwork(testNetID)
	// k1's uplink is now closed as a side effect of Retire; further traffic
	// on the underlying test connection is moot, but confirm the Keeper
	// itself reflects it.
	if st, _ := k1.State(); st != NotConnected {
		t.Fatalf("removed network state=%v, want NotConnected", st)
	}

	// The surviving network must be completely unaffected.
	srv2.send(t, conn2, "NOTICE * :net2-after-remove")
	line, err = client.NextLine()
	if err != nil || line.Network != testNetID2 || string(line.Raw) != "NOTICE * :net2-after-remove" {
		t.Fatalf("NextLine after remove: line=%+v err=%v", line, err)
	}

	const minDrop = 2 // readLoop + fan-in; see comment above baseline
	deadline := time.Now().Add(2 * time.Second)
	after := baseline
	for time.Now().Before(deadline) {
		runtime.GC()
		after = runtime.NumGoroutine()
		if baseline-after >= minDrop {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if baseline-after < minDrop {
		t.Fatalf("goroutine count dropped by %d (baseline=%d after=%d), want >= %d — fan-in goroutine leaked (only readLoop exited)",
			baseline-after, baseline, after, minDrop)
	}
}

// TestListenerThreeNetworksSimultaneously extends the two-network isolation
// test to three, since two can pass on pairwise-only assumptions in the
// fan-in (e.g. an off-by-one in how many goroutines feed the shared out
// channel) that three would catch.
func TestListenerThreeNetworksSimultaneously(t *testing.T) {
	mgr := NewManager(8192, 4096, nil)
	ids := []NetworkID{testNetID, testNetID2, testNetID3}
	type netHandles struct {
		k    *Keeper
		srv  *fakeServer
		conn net.Conn
	}
	nets := make(map[NetworkID]netHandles, len(ids))
	for _, id := range ids {
		k, srv, conn := dialedTestNetwork(t, mgr, id, 4096)
		nets[id] = netHandles{k, srv, conn}
	}
	sockPath := startTestListener(t, mgr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := Attach(ctx, sockPath, HelloMsg{Mode: ModeLive})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer client.Close()
	if len(client.Networks) != 3 {
		t.Fatalf("Networks=%+v, want 3 entries", client.Networks)
	}
	if err := client.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}

	const perNet = 5
	for _, id := range ids {
		for i := 0; i < perNet; i++ {
			nets[id].srv.send(t, nets[id].conn, fmt.Sprintf("NOTICE * :net%d-%d", id, i))
		}
	}

	got := map[NetworkID][]uint64{}
	deadline := time.Now().Add(3 * time.Second)
	total := 0
	for total < len(ids)*perNet {
		if time.Now().After(deadline) {
			t.Fatalf("timed out; got so far: %+v", got)
		}
		line, err := client.NextLine()
		if err != nil {
			t.Fatalf("NextLine: %v", err)
		}
		got[line.Network] = append(got[line.Network], line.Seq)
		total++
	}

	for _, id := range ids {
		seqs := got[id]
		if len(seqs) != perNet {
			t.Fatalf("network %d: got %d lines, want %d", id, len(seqs), perNet)
		}
		for i, s := range seqs {
			if s != uint64(i+1) {
				t.Fatalf("network %d: seq out of order: %v", id, seqs)
			}
		}
	}
}
