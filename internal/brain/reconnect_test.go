package brain

import (
	"context"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/keeper"
)

// TestDriverAutoReconnectsAfterRegistrationDeadline proves failRegistration's
// own SendClose+armReconnect treatment: a network whose registration times
// out while the underlying connection is still fully alive (the fake server
// here holds the TCP connection open, saying nothing, well past the
// deadline) must not be left connected-but-stuck forever — nothing else
// would ever close or redial it, since the deadline firing is the only
// signal anything went wrong at all. Removing failRegistration's SendClose
// call (leaving the connection open) reproduces this test hanging forever
// waiting for a second DialResult, confirmed by hand while writing it.
func TestDriverAutoReconnectsAfterRegistrationDeadline(t *testing.T) {
	client, _ := newAttachedLiveClientWithManager(t)

	srv := newFakeIRCServer(t)
	defer srv.close()
	go func() {
		conn, err := srv.ln.Accept()
		if err != nil {
			return
		}
		// Outlive the 100ms registration deadline below while saying
		// nothing at all — proving the close has to come from Driver
		// itself, not from the (silent) far end — but not so long that
		// it eats into the *second* attempt's own fresh 100ms deadline,
		// which starts ticking once the redial's StartRegistration call
		// fires (~120ms after this goroutine started).
		time.Sleep(150 * time.Millisecond)
		conn.Close()
		srv.serveOne(t) // the auto-redialed connection actually registers
	}()
	host, port := srv.addr()

	const netID keeper.NetworkID = 1
	driver := NewDriver(client,
		WithRegistrationTimeout(100*time.Millisecond),
		WithBackoff(20*time.Millisecond, 20*time.Millisecond),
	)
	driver.RegisterNetwork(netID, NetworkConfig{PrimaryNick: "gobncbrain"})

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	go func() { _ = driver.Run(runCtx) }()

	if err := driver.Dial(netID, keeper.DialConfig{Host: host, Port: port}, 0); err != nil {
		t.Fatalf("Dial: %v", err)
	}
	awaitDialResult(t, driver, 5*time.Second)
	if err := driver.StartRegistration(netID); err != nil {
		t.Fatalf("StartRegistration: %v", err)
	}
	awaitFailedResult(t, driver.Results(), netID, "deadline", 2*time.Second)

	dr2 := awaitDialResult(t, driver, 5*time.Second)
	if !dr2.OK {
		t.Fatalf("auto-reconnect DialResult=%+v, want OK=true", dr2)
	}
	if err := driver.StartRegistration(netID); err != nil {
		t.Fatalf("StartRegistration (post-auto-reconnect): %v", err)
	}
	awaitComplete(t, driver, 10*time.Second)
}

// TestDriverAutoReconnectsAfterDisconnect proves the new auto-reconnect
// behavior this cutover adds: a network that disconnects on its own (the
// fake server here simply closes after registering, the same shape a real
// ircd hangup or network blip produces) gets redialed automatically,
// without any caller-driven Reconnect call — this did not exist anywhere
// in the keeper/brain stack before (see reconnect.go's package doc), and
// the redialed connection genuinely re-registers, not just reconnects at
// the wire level. Removing the armReconnect call from handleNetworkEvent
// reproduces this test hanging until timeout; removing armReconnect's
// resetStateLocked call reproduces the second registration never
// completing (Step no-ops forever on the stale PhaseComplete state) — both
// confirmed by hand while writing this test.
func TestDriverAutoReconnectsAfterDisconnect(t *testing.T) {
	client, _ := newAttachedLiveClientWithManager(t)

	srv := newFakeIRCServer(t)
	defer srv.close()
	go func() {
		srv.serveOne(t) // initial connection; closes once registered
		srv.serveOne(t) // auto-reconnected connection, same listener
	}()
	host, port := srv.addr()

	const netID keeper.NetworkID = 1
	driver := NewDriver(client, WithBackoff(20*time.Millisecond, 20*time.Millisecond))
	driver.RegisterNetwork(netID, NetworkConfig{PrimaryNick: "gobncbrain"})

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

	// No Reconnect call here — the fake server's first serveOne returns
	// (closing the connection) right after sending 376; the auto-reconnect
	// path alone must notice and redial.
	dr2 := awaitDialResult(t, driver, 5*time.Second)
	if !dr2.OK || dr2.Epoch != 2 {
		t.Fatalf("auto-reconnect DialResult=%+v, want OK=true epoch=2", dr2)
	}
	if err := driver.StartRegistration(netID); err != nil {
		t.Fatalf("StartRegistration (post-auto-reconnect): %v", err)
	}
	awaitComplete(t, driver, 10*time.Second)
}

// TestDriverStopNetworkBlocksArmReconnect proves StopNetwork's guard
// directly: once StopNetwork has been called for a network, a call to
// armReconnect (the internal entry point Run uses on a failed dial or a
// genuine disconnect) must not redial it. Exercised directly rather than
// through a real disconnect because a deliberate Close (which StopNetwork
// itself triggers) never publishes a NetworkEvent in the first place — see
// StopNetwork's doc comment — so this is the only way to reach the guard
// itself rather than the (also-true, but different) fact that nothing
// calls armReconnect for a deliberate close.
//
// armReconnect actually checks d.stopped[id] twice — once before arming the
// timer, once again inside it right before SendDial, guarding the window
// where StopNetwork races the wait — so disabling only one of the two
// checks by hand does not reproduce a failure here; disabling both together
// does (confirmed by hand: a second connection reaches srv within the
// 300ms window). Left as two checks deliberately (the second guards a real
// race the first alone can't).
func TestDriverStopNetworkBlocksArmReconnect(t *testing.T) {
	client, _ := newAttachedLiveClientWithManager(t)

	srv := newFakeIRCServer(t)
	defer srv.close()
	secondConn := make(chan struct{}, 1)
	go func() {
		srv.serveOne(t)
		// A second Accept would only succeed if armReconnect wrongly redialed.
		conn, err := srv.ln.Accept()
		if err != nil {
			return
		}
		conn.Close()
		secondConn <- struct{}{}
	}()
	host, port := srv.addr()

	const netID keeper.NetworkID = 1
	driver := NewDriver(client, WithBackoff(20*time.Millisecond, 20*time.Millisecond))
	driver.RegisterNetwork(netID, NetworkConfig{PrimaryNick: "gobncbrain"})

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	go func() { _ = driver.Run(runCtx) }()

	if err := driver.Dial(netID, keeper.DialConfig{Host: host, Port: port}, 0); err != nil {
		t.Fatalf("Dial: %v", err)
	}
	awaitDialResult(t, driver, 5*time.Second)
	if err := driver.StartRegistration(netID); err != nil {
		t.Fatalf("StartRegistration: %v", err)
	}
	awaitComplete(t, driver, 10*time.Second)

	if err := driver.StopNetwork(netID); err != nil {
		t.Fatalf("StopNetwork: %v", err)
	}
	// Call the internal entry point directly — see doc comment above for why.
	driver.armReconnect(netID)

	select {
	case <-secondConn:
		t.Fatalf("armReconnect redialed a stopped network")
	case <-time.After(300 * time.Millisecond):
	}
}
