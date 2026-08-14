package brain

import (
	"context"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/keeper"
)

// TestDriverWriteRawPacesBurstThenWaits is the Driver-side proof
// internal/uplink/flood_test.go's TestFloodQueueAndPONGBypass provided for
// the old raw-socket path: the first line within burst reaches the wire
// immediately, and a second line that exceeds the bucket waits for tokens
// to refill rather than being sent back-to-back — proving WriteRaw actually
// paces traffic through the bucket, not just queues and immediately drains
// it. Reverting WriteRaw to call client.SendWrite directly (bypassing
// floodStateFor entirely) reproduces line2 arriving immediately, confirmed
// by hand while writing this test.
func TestDriverWriteRawPacesBurstThenWaits(t *testing.T) {
	client, mgr := newAttachedLiveClientWithManager(t)
	_ = mgr

	srv := newFakeIRCServer(t)
	defer srv.close()
	out := make(chan string, 8)
	go srv.serveOneCaptureAfterRegistration(t, out)
	host, port := srv.addr()

	const netID keeper.NetworkID = 1
	driver := NewDriver(client)
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

	driver.SetFloodParams(netID, 20, 20) // 20 bytes burst, 20 B/s

	line1 := "PRIVMSG #c :a" // wire: 15+2=17 bytes... within one refill of headroom
	line2 := "PRIVMSG #c :b"
	if err := driver.WriteRaw(netID, line1); err != nil {
		t.Fatalf("WriteRaw line1: %v", err)
	}
	if err := driver.WriteRaw(netID, line2); err != nil {
		t.Fatalf("WriteRaw line2: %v", err)
	}

	if got := awaitPRIVMSG(t, out, 3*time.Second); got != line1 {
		t.Fatalf("first=%q, want %q", got, line1)
	}

	start := time.Now()
	if got := awaitPRIVMSG(t, out, 3*time.Second); got != line2 {
		t.Fatalf("second=%q, want %q", got, line2)
	}
	if elapsed := time.Since(start); elapsed < 200*time.Millisecond {
		t.Fatalf("line2 arrived after only %s; flood pacing not applied", elapsed)
	}
}

// awaitPRIVMSG reads from a serveOneCaptureAfterRegistration channel until a
// PRIVMSG line arrives, ignoring anything else — the fake fixture never
// waits for the client's own CAP END before sending the welcome burst, so a
// CAP END the Driver sends in reaction to CAP LS can legitimately land in
// the "post-registration" capture window race-dependently; this is the same
// tolerance TestDriverAutoJoinsOnRegistrationComplete's want-map already
// relies on for JOIN, applied here for a single expected line.
func awaitPRIVMSG(t *testing.T, out <-chan string, timeout time.Duration) string {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case line := <-out:
			if hasPrefix(line, "PRIVMSG") {
				return line
			}
		case <-deadline:
			t.Fatalf("no PRIVMSG line within %s", timeout)
			return ""
		}
	}
}

// TestDriverWriteRawUnpacedWhenDisabled proves WriteRaw writes immediately,
// with no bucket wait, for a network that never had SetFloodParams called
// (pacing disabled) — mirrors internal/uplink's TestFloodDisabledImmediate.
func TestDriverWriteRawUnpacedWhenDisabled(t *testing.T) {
	client, _ := newAttachedLiveClientWithManager(t)

	srv := newFakeIRCServer(t)
	defer srv.close()
	out := make(chan string, 8)
	go srv.serveOneCaptureAfterRegistration(t, out)
	host, port := srv.addr()

	const netID keeper.NetworkID = 1
	driver := NewDriver(client)
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

	if err := driver.WriteRaw(netID, "PRIVMSG #c :fast"); err != nil {
		t.Fatalf("WriteRaw: %v", err)
	}
	if got := awaitPRIVMSG(t, out, time.Second); got != "PRIVMSG #c :fast" {
		t.Fatalf("got=%q", got)
	}
}
