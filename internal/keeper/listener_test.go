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

	// Hold a line subscription open across the write burst below so the
	// ring-overflow-case-2 self-close (Keeper.readLoop: eviction with zero
	// subscribers) doesn't fire and close the uplink out from under this
	// test — this test targets a different scenario: a client attaching
	// *after* eviction and requesting an already-evicted seq, which should
	// fail via fanInNetwork's own since()-ok=false check, not via the
	// uplink having been closed already.
	sub, unsub := k.SubscribeLines()
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		for range sub.Lines {
		}
	}()

	sockPath := startTestListener(t, mgr)

	for i := 0; i < 10; i++ {
		srv.send(t, conn, fmt.Sprintf("NOTICE * :line%d", i))
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && k.LastSeq() < 10 {
		time.Sleep(5 * time.Millisecond)
	}
	unsub()
	<-drainDone

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

// TestGapOnlyResumeSkipsAckedLines pins the core new resume property: a
// fresh attach with no explicit FromSeq entry for a network starts from
// that network's own tracked resume watermark (advanced only by explicit
// SeqAck, never by bare delivery), not from oldest-retained — so a second
// attach after the first one acked everything sees only what arrived
// since, never the lines the first attach already consumed.
func TestGapOnlyResumeSkipsAckedLines(t *testing.T) {
	mgr, k, srv, conn := dialedTestManager(t, 1000)
	sockPath := startTestListener(t, mgr)

	for i := 1; i <= 3; i++ {
		srv.send(t, conn, fmt.Sprintf("NOTICE * :old%d", i))
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && k.LastSeq() < 3 {
		time.Sleep(5 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client1, err := Attach(ctx, sockPath, HelloMsg{Mode: ModeLive})
	if err != nil {
		t.Fatalf("Attach client1: %v", err)
	}
	if err := client1.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}
	var lastSeq uint64
	for i := 0; i < 3; i++ {
		line, err := client1.NextLine()
		if err != nil {
			t.Fatalf("NextLine %d: %v", i, err)
		}
		lastSeq = line.Seq
		if err := client1.SendSeqAck(testNetID, line.Seq); err != nil {
			t.Fatalf("SendSeqAck: %v", err)
		}
	}
	// Give the keeper a moment to actually process the acks (fire-and-
	// forget, no result to synchronize on) before detaching.
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && k.DeliveredSeq() < lastSeq {
		time.Sleep(5 * time.Millisecond)
	}
	if got := k.DeliveredSeq(); got != lastSeq {
		t.Fatalf("DeliveredSeq=%d, want %d", got, lastSeq)
	}
	if err := client1.Close(); err != nil {
		t.Fatalf("client1.Close: %v", err)
	}

	// Traffic arrives while nobody is attached — the brain-down gap.
	srv.send(t, conn, "NOTICE * :new1")
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && k.LastSeq() < 4 {
		time.Sleep(5 * time.Millisecond)
	}

	client2, err := Attach(ctx, sockPath, HelloMsg{Mode: ModeLive})
	if err != nil {
		t.Fatalf("Attach client2: %v", err)
	}
	defer client2.Close()
	if err := client2.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}
	line, err := client2.NextLine()
	if err != nil {
		t.Fatalf("NextLine: %v", err)
	}
	if line.Seq != 4 || string(line.Raw) != "NOTICE * :new1" {
		t.Fatalf("first line to client2 = %+v, want seq 4 \"NOTICE * :new1\" (no replay of already-acked lines)", line)
	}
}

// TestListenerLiveBurstDoesNotKillAttach is the policy that replaced
// killing the live attach on SubscribeLines overflow: a WHO/NAMES-sized
// burst (thousands of lines arriving faster than the unix-socket JSON
// framing can drain) must not tear down the keeper↔brain link. The live
// subscriber buffer is deliberately tiny here so Overflow actually fires;
// the ring is the real buffer, and fanInNetwork must catch up from it
// rather than s.kill. Confirmed: reverting that catch-up to the old
// kill-on-overflow path fails this test with NextLine erroring after a
// partial drain (the broken-pipe the burst used to cause).
func TestListenerLiveBurstDoesNotKillAttach(t *testing.T) {
	old := lineSubBuffer
	lineSubBuffer = 64
	t.Cleanup(func() { lineSubBuffer = old })

	const n = 3000
	mgr, k, srv, conn := dialedTestManager(t, 8192)
	sockPath := startTestListener(t, mgr)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, err := Attach(ctx, sockPath, HelloMsg{Mode: ModeLive})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer client.Close()
	if err := client.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}

	// Don't read during the flood — the live buffer (64) overflows long
	// before the ring (8192) does. Old policy killed the attach here.
	for i := 0; i < n; i++ {
		srv.send(t, conn, fmt.Sprintf("NOTICE * :flood%d", i))
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && k.LastSeq() < uint64(n) {
		time.Sleep(5 * time.Millisecond)
	}
	if k.LastSeq() != uint64(n) {
		t.Fatalf("keeper's own read loop stalled: LastSeq=%d, want %d (a slow subscriber must never block reading)", k.LastSeq(), n)
	}

	got := 0
	for got < n {
		line, err := client.NextLine()
		if err != nil {
			t.Fatalf("attach killed after %d/%d lines: %v", got, n, err)
		}
		if line.Seq != uint64(got+1) {
			t.Fatalf("line %d: seq=%d, want %d (gap in live stream)", got, line.Seq, got+1)
		}
		got++
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
