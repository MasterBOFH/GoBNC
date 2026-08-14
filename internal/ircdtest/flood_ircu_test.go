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

// ircu2 default recvq is 1024 bytes. Without pacing, dumping >1KiB quickly
// typically gets Excess Flood / kill. With burst under recvq, we should survive.
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

	attachCtx, cancelAttach := context.WithTimeout(context.Background(), 5*time.Second)
	client, err := keeper.Attach(attachCtx, sockPath, keeper.HelloMsg{Mode: keeper.ModeLive})
	cancelAttach()
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
	driver.SetFloodParams(netID, 512, 256) // burst under 1024 recvq, 256 B/s sustained

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

	// Enqueue well over recvq in one shot; pacing must keep us under 1024 on the wire.
	const n = 40
	payload := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" // 40 bytes body
	for i := 0; i < n; i++ {
		line := fmt.Sprintf("PRIVMSG %s :%s%d", nick, payload, i)
		if err := driver.WriteRaw(netID, line); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	// ~40 * (~15+len(nick)+40) ≈ 3KB+ queued; at 256 B/s needs ~12s after burst.
	select {
	case err := <-disconnected:
		t.Fatalf("uplink dropped during paced flood (likely recvq/Excess Flood): %v", err)
	case <-time.After(20 * time.Second):
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
