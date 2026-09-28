//go:build ircd

package ircd_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/brain"
	"github.com/MasterBOFH/GoBNC/internal/keeper"
	"github.com/MasterBOFH/GoBNC/internal/registration"
)

// A stock ircu2 charges each client line 2s + 1s per 120 bytes, stops
// reading a client whose charge runs 10s ahead of real time, and closes the
// link with Excess Flood once more than 1024 unread bytes (CLIENT_FLOOD)
// pile up. A byte-rate pacer can't keep under that for short lines, so
// this checks GoBNC's per-line pacing (brain.FloodParams.Lines) against a
// real ircu2 built with the default CLIENT_FLOOD (docker/ircd/ircu2).
//
// Ported off internal/uplink (deleted in the keeper/brain cutover) onto
// internal/brain.Driver directly — the same in-process keeper.Manager +
// keeper.Listener + keeper.AttachClient + brain.Driver harness
// internal/session/harness_test.go and internal/server/keeper_harness_test.go
// use, just dialing a real external ircd (this container) instead of a fake
// one. Driver.WriteRaw is now the paced entry point Uplink.WriteRaw used to
// be, and Driver.SetFloodParams/RegisterNetwork replace
// uplink.Config's FloodBurst/FloodRate and per-network identity fields.
func TestIrcu2FloodRecvQ(t *testing.T) {
	if os.Getenv("GOBNC_IRCD") == "0" {
		t.Skip("GOBNC_IRCD=0")
	}
	const addr = "127.0.0.1:4443"
	if c, err := net.DialTimeout("tcp", addr, 3*time.Second); err != nil {
		t.Skipf("ircu2 not reachable at %s: %v", addr, err)
	} else {
		_ = c.Close()
	}

	nick := sanitizeNick(fmt.Sprintf("f%d", time.Now().Unix()%100000))

	mgr := keeper.NewManager(1<<20, 4096, nil)
	sockDir := filepath.Join(t.TempDir(), "sock")
	if err := os.Mkdir(sockDir, 0700); err != nil {
		t.Fatalf("mkdir sockDir: %v", err)
	}
	sockPath := filepath.Join(sockDir, "keeper.sock")

	listenerCtx, cancelListener := context.WithCancel(context.Background())
	defer cancelListener()
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

	// The socket file existing doesn't mean it's accepting yet: net.Listen
	// bind()s (creating the file) before it listen()s, and a connect in
	// between gets ECONNREFUSED. Retry the attach rather than trusting the
	// os.Stat above — same as internal/session's harness_test.go.
	var client *keeper.AttachClient
	var err error
	deadline := time.Now().Add(5 * time.Second)
	for {
		attachCtx, cancelAttach := context.WithTimeout(context.Background(), time.Second)
		client, err = keeper.Attach(attachCtx, sockPath, keeper.HelloMsg{Mode: keeper.ModeLive})
		cancelAttach()
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("keeper.Attach: %v", err)
	}
	defer client.Close()
	if err := client.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}

	driver := brain.NewDriver(client)
	const netID keeper.NetworkID = 1
	driver.RegisterNetwork(netID, brain.NetworkConfig{
		PrimaryNick: nick,
		Username:    "gobnc",
		Realname:    "floodtest",
	})
	driver.SetFloodParams(netID, brain.FloodParams{Lines: true})

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	go func() { _ = driver.Run(runCtx) }()

	registered := make(chan struct{})
	disconnected := make(chan error, 1)
	go func() {
		var closedRegistered bool
		for {
			select {
			case <-runCtx.Done():
				return
			case dr, ok := <-driver.DialResults():
				if !ok {
					return
				}
				if dr.Network == netID && dr.OK {
					_ = driver.StartRegistration(netID)
				}
			case res, ok := <-driver.Results():
				if !ok {
					return
				}
				if res.Network == netID && res.State.Phase == registration.PhaseComplete && !closedRegistered {
					closedRegistered = true
					close(registered)
				}
			case ev, ok := <-driver.NetworkEvents():
				if !ok {
					return
				}
				if ev.Network == netID && ev.Kind == keeper.EventDisconnected {
					var derr error
					if ev.Error != "" {
						derr = fmt.Errorf("%s", ev.Error)
					}
					select {
					case disconnected <- derr:
					default:
					}
				}
			}
		}
	}()

	if err := driver.Dial(netID, keeper.DialConfig{Host: "127.0.0.1", Port: 4443, DialTimeout: 5 * time.Second}, 0); err != nil {
		t.Fatalf("Dial: %v", err)
	}

	select {
	case <-registered:
	case err := <-disconnected:
		t.Fatalf("disconnected before registering: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for registration")
	}

	// Enqueue ~1.6KB in one shot: sent unpaced, ircu parses the first few
	// lines and the rest overflows its 1024-byte recvq. Paced per line,
	// every line must drain (4 at once, then one per ~2s) with no drop.
	const n = 25
	payload := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" // 40 bytes body
	for i := 0; i < n; i++ {
		line := fmt.Sprintf("PRIVMSG %s :%s%d", nick, payload, i)
		if err := driver.WriteRaw(netID, line); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	drained := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(runCtx, 90*time.Second)
		defer cancel()
		driver.WaitFloodDrained(ctx, netID)
		close(drained)
	}()
	select {
	case err := <-disconnected:
		t.Fatalf("uplink dropped during paced flood (likely recvq/Excess Flood): %v", err)
	case <-drained:
	}

	// Still able to send after drain.
	if err := driver.WriteRaw(netID, "PRIVMSG "+nick+" :still-alive"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-disconnected:
		t.Fatalf("dropped after flood drain: %v", err)
	case <-time.After(2 * time.Second):
	}
}
