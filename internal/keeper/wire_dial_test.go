package keeper

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

// TestWireDialAndStream proves the core claim of this round: Dial is a wire
// operation, not just a local Go call. A brain attaches to an empty
// Manager, asks the keeper (over the socket) to dial a network, and gets
// both a result and, once dialed, that network's live line stream — all
// without the test ever calling k.Dial directly.
func TestWireDialAndStream(t *testing.T) {
	mgr := NewManager(8192, 4096, nil)
	sockPath := startTestListener(t, mgr)

	srv := newFakeServer(t)
	defer srv.close()
	host, port := hostPort(srv.addr())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := Attach(ctx, sockPath, HelloMsg{Mode: ModeLive})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer client.Close()
	if len(client.Networks) != 0 {
		t.Fatalf("Networks=%+v, want none — manager started empty", client.Networks)
	}
	if err := client.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}

	acceptedCh := make(chan net.Conn, 1)
	go func() { acceptedCh <- srv.accept(t) }()

	if err := client.SendDial(testNetID, DialConfig{Host: host, Port: port}, 0); err != nil {
		t.Fatalf("SendDial: %v", err)
	}

	ev, err := client.Next()
	if err != nil {
		t.Fatalf("Next (dial result): %v", err)
	}
	if ev.DialResult == nil {
		t.Fatalf("got %+v, want a DialResult", ev)
	}
	if !ev.DialResult.OK {
		t.Fatalf("DialResult.OK=false, Error=%q", ev.DialResult.Error)
	}
	if ev.DialResult.Network != testNetID || ev.DialResult.Epoch != 1 {
		t.Fatalf("DialResult=%+v, want network=%d epoch=1", ev.DialResult, testNetID)
	}

	conn := <-acceptedCh
	defer conn.Close()
	srv.send(t, conn, "NOTICE * :hello-over-the-wire")

	line, err := client.NextLine()
	if err != nil {
		t.Fatalf("NextLine: %v", err)
	}
	if line.Network != testNetID || string(line.Raw) != "NOTICE * :hello-over-the-wire" {
		t.Fatalf("line=%+v, want network=%d raw=%q", line, testNetID, "NOTICE * :hello-over-the-wire")
	}

	if got := mgr.Network(testNetID); got == nil {
		t.Fatalf("Manager has no entry for %d after wire Dial — EnsureNetwork should have created it", testNetID)
	}
}

// TestWireDialFailureReported proves a dial failure comes back as data
// (DialResultMsg.OK=false), not a dropped connection or a silent nothing.
func TestWireDialFailureReported(t *testing.T) {
	mgr := NewManager(8192, 4096, nil)
	sockPath := startTestListener(t, mgr)

	// A port nothing is listening on: bind then immediately release it.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	host, port := hostPort(ln.Addr().String())
	ln.Close()

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

	dialCfg := DialConfig{Host: host, Port: port, DialTimeout: 2 * time.Second}
	if err := client.SendDial(testNetID, dialCfg, 0); err != nil {
		t.Fatalf("SendDial: %v", err)
	}
	ev, err := client.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if ev.DialResult == nil {
		t.Fatalf("got %+v, want a DialResult", ev)
	}
	if ev.DialResult.OK {
		t.Fatalf("DialResult.OK=true dialing a closed port, want false")
	}
	if ev.DialResult.Error == "" {
		t.Fatalf("DialResult.Error is empty on failure")
	}
	if ev.DialResult.Network != testNetID {
		t.Fatalf("DialResult.Network=%d, want %d", ev.DialResult.Network, testNetID)
	}
}

// TestWireCloseAndRedial exercises Close as a wire op distinct from
// RemoveNetwork (the network stays known to the Manager), and confirms a
// second wire Dial for the same network on the same live connection
// reuses the existing fan-in rather than starting a duplicate one — a
// duplicate would double every line.
func TestWireCloseAndRedial(t *testing.T) {
	mgr := NewManager(8192, 4096, nil)
	sockPath := startTestListener(t, mgr)

	srv1 := newFakeServer(t)
	defer srv1.close()
	host, port := hostPort(srv1.addr())

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

	acceptedCh := make(chan net.Conn, 1)
	go func() { acceptedCh <- srv1.accept(t) }()
	if err := client.SendDial(testNetID, DialConfig{Host: host, Port: port}, 0); err != nil {
		t.Fatalf("SendDial: %v", err)
	}
	ev, err := client.Next()
	if err != nil || ev.DialResult == nil || !ev.DialResult.OK {
		t.Fatalf("dial: ev=%+v err=%v", ev, err)
	}
	conn1 := <-acceptedCh
	srv1.send(t, conn1, "NOTICE * :before-close")
	line, err := client.NextLine()
	if err != nil || string(line.Raw) != "NOTICE * :before-close" {
		t.Fatalf("NextLine before close: line=%+v err=%v", line, err)
	}

	if err := client.SendClose(testNetID); err != nil {
		t.Fatalf("SendClose: %v", err)
	}
	ev, err = client.Next()
	if err != nil {
		t.Fatalf("Next (close result): %v", err)
	}
	if ev.CloseResult == nil || !ev.CloseResult.OK {
		t.Fatalf("CloseResult=%+v, want OK=true", ev.CloseResult)
	}
	if st, _ := mgr.Network(testNetID).State(); st != NotConnected {
		t.Fatalf("state after wire Close=%v, want NotConnected", st)
	}

	// Redial the same network to a fresh server, same live connection.
	srv2 := newFakeServer(t)
	defer srv2.close()
	host2, port2 := hostPort(srv2.addr())
	acceptedCh2 := make(chan net.Conn, 1)
	go func() { acceptedCh2 <- srv2.accept(t) }()
	if err := client.SendDial(testNetID, DialConfig{Host: host2, Port: port2}, 0); err != nil {
		t.Fatalf("SendDial (redial): %v", err)
	}
	// The DialResult (from the request-handling goroutine) and the
	// redial's own Connected NetworkEvent (from the fan-in goroutine that's
	// been running since the first Dial — Close doesn't tear it down, only
	// Retire does, so it's still subscribed to connect/disconnect events)
	// are produced by two independent goroutines feeding the same shared
	// out channel. Nothing orders one before the other; both must be
	// accepted in either order.
	dialResult := awaitDialResultAndConnectedEvent(t, client, testNetID, 2)
	if !dialResult.OK {
		t.Fatalf("redial DialResult.OK=false: %+v", dialResult)
	}

	conn2 := <-acceptedCh2
	srv2.send(t, conn2, "NOTICE * :after-redial")

	// Exactly one line must arrive, not two (which a duplicate fan-in would produce).
	line, err = client.NextLine()
	if err != nil || string(line.Raw) != "NOTICE * :after-redial" {
		t.Fatalf("NextLine after redial: line=%+v err=%v", line, err)
	}
	select {
	case err := <-linesOnly(t, client):
		t.Fatalf("got an unexpected extra frame after redial (duplicate fan-in?): err=%v", err)
	case <-time.After(300 * time.Millisecond):
		// Correct: nothing more arrived.
	}
}

// linesOnly is a one-shot helper: it starts a goroutine that calls NextLine
// once and reports on the returned channel, for a "nothing more should
// arrive" negative assertion with a timeout in the caller.
func linesOnly(t *testing.T, c *AttachClient) <-chan error {
	t.Helper()
	ch := make(chan error, 1)
	go func() {
		_, err := c.NextLine()
		ch <- err
	}()
	return ch
}

// awaitDialResultAndConnectedEvent reads exactly two frames and requires
// them to be one DialResultMsg for network and one NetworkEventMsg{
// Connected, wantEpoch} for network, in either order — see the comment at
// its call sites for why order isn't guaranteed between them. Returns the
// DialResultMsg for the caller to check OK/Error on.
func awaitDialResultAndConnectedEvent(t *testing.T, c *AttachClient, network NetworkID, wantEpoch uint64) DialResultMsg {
	t.Helper()
	var dialResult *DialResultMsg
	var netEvent *NetworkEventMsg
	for i := 0; i < 2; i++ {
		ev, err := c.Next()
		if err != nil {
			t.Fatalf("Next (%d/2): %v", i+1, err)
		}
		switch {
		case ev.DialResult != nil && dialResult == nil:
			dialResult = ev.DialResult
		case ev.NetworkEvent != nil && netEvent == nil:
			netEvent = ev.NetworkEvent
		default:
			t.Fatalf("unexpected or duplicate frame (%d/2): %+v", i+1, ev)
		}
	}
	if dialResult.Network != network || dialResult.Epoch != wantEpoch {
		t.Fatalf("DialResult=%+v, want network=%d epoch=%d", dialResult, network, wantEpoch)
	}
	if netEvent.Network != network || netEvent.Kind != EventConnected || netEvent.Epoch != wantEpoch {
		t.Fatalf("NetworkEvent=%+v, want network=%d Connected epoch=%d", netEvent, network, wantEpoch)
	}
	return *dialResult
}

func TestWireCloseUnknownNetwork(t *testing.T) {
	mgr := NewManager(8192, 4096, nil)
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

	if err := client.SendClose(NetworkID(999)); err != nil {
		t.Fatalf("SendClose: %v", err)
	}
	ev, err := client.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if ev.CloseResult == nil || ev.CloseResult.OK {
		t.Fatalf("CloseResult=%+v, want OK=false for an unknown network", ev.CloseResult)
	}
}

// TestWireSocketDeathDeliversNetworkEvent proves the brain learns about a
// dropped uplink by the keeper pushing it, not by polling. The fake server
// hangs up server-side — the client never calls Close.
func TestWireSocketDeathDeliversNetworkEvent(t *testing.T) {
	mgr := NewManager(8192, 4096, nil)
	sockPath := startTestListener(t, mgr)

	srv := newFakeServer(t)
	defer srv.close()
	host, port := hostPort(srv.addr())

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

	acceptedCh := make(chan net.Conn, 1)
	go func() { acceptedCh <- srv.accept(t) }()
	if err := client.SendDial(testNetID, DialConfig{Host: host, Port: port}, 0); err != nil {
		t.Fatalf("SendDial: %v", err)
	}
	ev, err := client.Next()
	if err != nil || ev.DialResult == nil || !ev.DialResult.OK {
		t.Fatalf("dial: ev=%+v err=%v", ev, err)
	}
	conn := <-acceptedCh
	_ = conn.Close() // server-side hangup — the brain never asked for this

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ev, err := client.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if ev.NetworkEvent == nil {
			continue
		}
		if ev.NetworkEvent.Network != testNetID {
			t.Fatalf("NetworkEvent.Network=%d, want %d", ev.NetworkEvent.Network, testNetID)
		}
		if ev.NetworkEvent.Kind != EventDisconnected {
			t.Fatalf("NetworkEvent.Kind=%v, want EventDisconnected", ev.NetworkEvent.Kind)
		}
		if ev.NetworkEvent.Error == "" {
			t.Fatalf("NetworkEvent.Error empty for a socket that died on its own")
		}
		return // success
	}
	t.Fatalf("no NetworkEvent delivered within timeout after server-side hangup")
}

// TestWireFinalLineDeliveredBeforeDisconnectEvent proves fanInNetwork
// preserves publish order across its two independent channels
// (SubscribeLines' Lines and Subscribe's events): Keeper.readLoop always
// calls publishLine for a connection's last line strictly before
// publish(EventDisconnected) for that same connection, but select's choice
// among simultaneously-ready cases is unspecified — without the ordering
// fix in fanInNetwork, a disconnect published microseconds after a final
// line can race ahead of it over the wire. This matters beyond the wire
// protocol itself: internal/brain.Driver treats a NetworkEvent{Disconnected}
// arriving during registration as a failure signal, so a completion line
// delivered out of order behind its own connection's disconnect would
// spuriously fail a registration that actually succeeded. The server here
// writes its final line and closes immediately after — deliberately no
// gap — and the test is run many times in one process to give the
// underlying race a real chance to manifest if the fix regresses.
func TestWireFinalLineDeliveredBeforeDisconnectEvent(t *testing.T) {
	mgr := NewManager(8192, 4096, nil)
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

	const iterations = 50
	for i := 0; i < iterations; i++ {
		srv := newFakeServer(t)
		host, port := hostPort(srv.addr())

		acceptedCh := make(chan net.Conn, 1)
		go func() { acceptedCh <- srv.accept(t) }()
		if err := client.SendDial(testNetID, DialConfig{Host: host, Port: port}, 0); err != nil {
			t.Fatalf("iteration %d: SendDial: %v", i, err)
		}
		// On iteration 0 there's no fan-in goroutine running yet, so only
		// a DialResult is expected; from iteration 1 on, the fan-in
		// started by the first Dial survives Close (see
		// TestWireCloseAndRedial's comment) and independently emits its
		// own Connected event racing the request-handler's DialResult —
		// both must be accepted in either order.
		var dialResult DialResultMsg
		if i == 0 {
			ev, err := client.Next()
			if err != nil || ev.DialResult == nil || !ev.DialResult.OK {
				t.Fatalf("iteration %d: dial: ev=%+v err=%v", i, ev, err)
			}
			dialResult = *ev.DialResult
		} else {
			dialResult = awaitDialResultAndConnectedEvent(t, client, testNetID, uint64(i+1))
		}
		if !dialResult.OK {
			t.Fatalf("iteration %d: DialResult.OK=false: %+v", i, dialResult)
		}

		conn := <-acceptedCh
		// Write the final line and close with no deliberate gap — the
		// exact shape that races publishLine against publish(EventDisconnected)
		// inside fanInNetwork if the ordering fix isn't there.
		srv.send(t, conn, "NOTICE * :final-line-before-hangup")
		_ = conn.Close()

		var sawLine, sawDisconnect bool
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			ev, err := client.Next()
			if err != nil {
				t.Fatalf("iteration %d: Next: %v", i, err)
			}
			if ev.Line != nil {
				if sawDisconnect {
					t.Fatalf("iteration %d: line delivered after its own connection's disconnect event: %+v", i, ev.Line)
				}
				sawLine = true
			}
			if ev.NetworkEvent != nil && ev.NetworkEvent.Kind == EventDisconnected {
				if !sawLine {
					t.Fatalf("iteration %d: disconnect event delivered before the final line", i)
				}
				sawDisconnect = true
				break
			}
		}
		if !sawLine || !sawDisconnect {
			t.Fatalf("iteration %d: didn't see both line and disconnect within timeout (line=%v disconnect=%v)", i, sawLine, sawDisconnect)
		}

		srv.close()
	}
}

// TestWireRedialDeliversPreDropBacklogAcrossEpoch is the ordinary-reconnect
// case: a network the keeper already holds accumulates lines nobody has
// consumed yet (the keeper was dialed to it before any brain attached, or a
// prior brain never got around to reading them), then the brain attaches,
// asks for everything from seq 0, and must receive that pre-existing
// backlog — then, after a Close and a redial on the same live connection,
// new lines at the new epoch. The epoch boundary must be visible directly
// on the delivered LineMsg values, not something the test (or a real
// brain) has to infer from timing or reconnection order.
func TestWireRedialDeliversPreDropBacklogAcrossEpoch(t *testing.T) {
	mgr := NewManager(8192, 4096, nil)

	// The keeper already holds this network — dialed directly, not over
	// the wire — representing state that predates this brain's attach.
	srv1 := newFakeServer(t)
	defer srv1.close()
	host1, port1 := hostPort(srv1.addr())
	k := mgr.EnsureNetwork(testNetID)
	acceptedCh1 := make(chan net.Conn, 1)
	go func() { acceptedCh1 <- srv1.accept(t) }()
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := k.Dial(dialCtx, DialConfig{Host: host1, Port: port1}); err != nil {
		dialCancel()
		t.Fatalf("initial local Dial: %v", err)
	}
	dialCancel()
	conn1 := <-acceptedCh1

	// Two lines land in the ring with nobody subscribed yet.
	srv1.send(t, conn1, "NOTICE * :pre-drop-1")
	srv1.send(t, conn1, "NOTICE * :pre-drop-2")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && k.LastSeq() < 2 {
		time.Sleep(5 * time.Millisecond)
	}
	if k.LastSeq() != 2 {
		t.Fatalf("LastSeq=%d before any brain attached, want 2", k.LastSeq())
	}

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

	// Both pre-existing lines must arrive, tagged epoch 1, in order —
	// "the brain may not have consumed them; it should be able to ask."
	line1, err := client.NextLine()
	if err != nil {
		t.Fatalf("NextLine (pre-drop-1): %v", err)
	}
	if line1.Epoch != 1 || string(line1.Raw) != "NOTICE * :pre-drop-1" {
		t.Fatalf("line1=%+v, want epoch=1 raw=%q", line1, "NOTICE * :pre-drop-1")
	}
	line2, err := client.NextLine()
	if err != nil {
		t.Fatalf("NextLine (pre-drop-2): %v", err)
	}
	if line2.Epoch != 1 || string(line2.Raw) != "NOTICE * :pre-drop-2" {
		t.Fatalf("line2=%+v, want epoch=1 raw=%q", line2, "NOTICE * :pre-drop-2")
	}

	// Close, then redial on the same live connection.
	if err := client.SendClose(testNetID); err != nil {
		t.Fatalf("SendClose: %v", err)
	}
	ev, err := client.Next()
	if err != nil || ev.CloseResult == nil || !ev.CloseResult.OK {
		t.Fatalf("close: ev=%+v err=%v", ev, err)
	}

	srv2 := newFakeServer(t)
	defer srv2.close()
	host2, port2 := hostPort(srv2.addr())
	acceptedCh2 := make(chan net.Conn, 1)
	go func() { acceptedCh2 <- srv2.accept(t) }()
	if err := client.SendDial(testNetID, DialConfig{Host: host2, Port: port2}, 0); err != nil {
		t.Fatalf("SendDial (redial): %v", err)
	}
	// DialResult and the redial's own Connected NetworkEvent come from two
	// independent goroutines with no ordering guarantee between them (see
	// awaitDialResultAndConnectedEvent's doc comment).
	dialResult := awaitDialResultAndConnectedEvent(t, client, testNetID, 2)
	if !dialResult.OK {
		t.Fatalf("redial DialResult.OK=false: %+v", dialResult)
	}
	conn2 := <-acceptedCh2

	srv2.send(t, conn2, "NOTICE * :post-redial")
	line3, err := client.NextLine()
	if err != nil {
		t.Fatalf("NextLine (post-redial): %v", err)
	}
	if line3.Epoch != 2 || string(line3.Raw) != "NOTICE * :post-redial" {
		t.Fatalf("line3=%+v, want epoch=2 raw=%q — the epoch boundary must be visible on the line itself", line3, "NOTICE * :post-redial")
	}
	if line3.Seq <= line2.Seq {
		t.Fatalf("post-redial seq=%d did not advance past pre-drop seq=%d", line3.Seq, line2.Seq)
	}
}

// TestWireWriteRequestReachesUplink proves WriteRequest is the only path a
// brain-side driver needs to actually send anything: a line submitted over
// the wire arrives at the real uplink socket byte-for-byte, and the keeper
// reports success back over the same connection.
func TestWireWriteRequestReachesUplink(t *testing.T) {
	mgr := NewManager(8192, 4096, nil)
	sockPath := startTestListener(t, mgr)

	srv := newFakeServer(t)
	defer srv.close()
	host, port := hostPort(srv.addr())

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

	acceptedCh := make(chan net.Conn, 1)
	go func() { acceptedCh <- srv.accept(t) }()
	if err := client.SendDial(testNetID, DialConfig{Host: host, Port: port}, 0); err != nil {
		t.Fatalf("SendDial: %v", err)
	}
	ev, err := client.Next()
	if err != nil || ev.DialResult == nil || !ev.DialResult.OK {
		t.Fatalf("dial: ev=%+v err=%v", ev, err)
	}
	conn := <-acceptedCh

	if err := client.SendWrite(testNetID, "NICK gobncbrain"); err != nil {
		t.Fatalf("SendWrite: %v", err)
	}
	ev, err = client.Next()
	if err != nil {
		t.Fatalf("Next (write result): %v", err)
	}
	if ev.WriteResult == nil || !ev.WriteResult.OK || ev.WriteResult.Network != testNetID {
		t.Fatalf("WriteResult=%+v, want OK=true network=%d", ev.WriteResult, testNetID)
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	r := bufio.NewReader(conn)
	got, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read from uplink: %v", err)
	}
	if got := strings.TrimRight(got, "\r\n"); got != "NICK gobncbrain" {
		t.Fatalf("uplink received %q, want %q", got, "NICK gobncbrain")
	}
}

func TestWireWriteRequestUnknownNetwork(t *testing.T) {
	mgr := NewManager(8192, 4096, nil)
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
	if err := client.SendWrite(NetworkID(999), "NICK nope"); err != nil {
		t.Fatalf("SendWrite: %v", err)
	}
	ev, err := client.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if ev.WriteResult == nil || ev.WriteResult.OK {
		t.Fatalf("WriteResult=%+v, want OK=false for an unknown network", ev.WriteResult)
	}
	if !ev.WriteResult.Refused {
		t.Fatalf("WriteResult=%+v, want Refused=true for an unknown network — nothing to write to, same category as not-connected", ev.WriteResult)
	}
}

// TestWireWriteRequestRefusedWhenNotConnected proves the backpressure
// signal a brain-side flood pacer needs (see WriteResultMsg's doc comment)
// distinguishes "nothing to write to" from an ordinary write failure: a
// network the Manager knows about but that has no live connection (Close
// was called, no redial yet) must come back Refused=true, not just OK=false
// indistinguishable from a write that started and failed midway.
func TestWireWriteRequestRefusedWhenNotConnected(t *testing.T) {
	mgr := NewManager(8192, 4096, nil)
	// EnsureNetwork without ever dialing: known to the Manager, never connected.
	mgr.EnsureNetwork(testNetID)
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
	if err := client.SendWrite(testNetID, "NICK nope"); err != nil {
		t.Fatalf("SendWrite: %v", err)
	}
	ev, err := client.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if ev.WriteResult == nil || ev.WriteResult.OK {
		t.Fatalf("WriteResult=%+v, want OK=false for a known-but-not-connected network", ev.WriteResult)
	}
	if !ev.WriteResult.Refused {
		t.Fatalf("WriteResult=%+v, want Refused=true — no live connection to write to", ev.WriteResult)
	}
}

// TestWireQuitCloseWritesLineThenCloses proves QuitCloseRequest is the
// deliberate-disconnect primitive: the final line reaches the real uplink
// socket byte-for-byte, and the connection is actually torn down afterward
// (both from the keeper's own State() and observably at the fake server's
// end of the socket) — not just a WriteRequest that happens to say QUIT.
func TestWireQuitCloseWritesLineThenCloses(t *testing.T) {
	mgr := NewManager(8192, 4096, nil)
	sockPath := startTestListener(t, mgr)

	srv := newFakeServer(t)
	defer srv.close()
	host, port := hostPort(srv.addr())

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

	acceptedCh := make(chan net.Conn, 1)
	go func() { acceptedCh <- srv.accept(t) }()
	if err := client.SendDial(testNetID, DialConfig{Host: host, Port: port}, 0); err != nil {
		t.Fatalf("SendDial: %v", err)
	}
	ev, err := client.Next()
	if err != nil || ev.DialResult == nil || !ev.DialResult.OK {
		t.Fatalf("dial: ev=%+v err=%v", ev, err)
	}
	conn := <-acceptedCh

	if err := client.SendQuitClose(testNetID, "QUIT :reload", time.Second); err != nil {
		t.Fatalf("SendQuitClose: %v", err)
	}
	ev, err = client.Next()
	if err != nil {
		t.Fatalf("Next (quit-close result): %v", err)
	}
	if ev.QuitCloseResult == nil || !ev.QuitCloseResult.OK || ev.QuitCloseResult.Network != testNetID {
		t.Fatalf("QuitCloseResult=%+v, want OK=true network=%d", ev.QuitCloseResult, testNetID)
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	r := bufio.NewReader(conn)
	got, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read from uplink: %v", err)
	}
	if got := strings.TrimRight(got, "\r\n"); got != "QUIT :reload" {
		t.Fatalf("uplink received %q, want %q", got, "QUIT :reload")
	}

	if st, _ := mgr.Network(testNetID).State(); st != NotConnected {
		t.Fatalf("state after QuitClose=%v, want NotConnected", st)
	}

	// The fake server's end of the socket should observe the close too —
	// a further read must hit EOF, not hang, confirming QuitClose actually
	// closed the connection rather than merely writing and returning.
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 16)
	if n, err := conn.Read(buf); err == nil {
		t.Fatalf("read after QuitClose got %d bytes with no error, want EOF", n)
	}
}

func TestWireQuitCloseUnknownNetwork(t *testing.T) {
	mgr := NewManager(8192, 4096, nil)
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
	if err := client.SendQuitClose(NetworkID(999), "QUIT :nope", 0); err != nil {
		t.Fatalf("SendQuitClose: %v", err)
	}
	ev, err := client.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if ev.QuitCloseResult == nil || ev.QuitCloseResult.OK {
		t.Fatalf("QuitCloseResult=%+v, want OK=false for an unknown network", ev.QuitCloseResult)
	}
}
