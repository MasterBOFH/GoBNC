package brain

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/keeper"
	"github.com/MasterBOFH/GoBNC/internal/registration"
)

// TestDriverLogsRawUplinkTraffic proves Driver.sendLine/handleLine restore
// the raw-traffic debug logging internal/uplink.Uplink used to get for
// free by attaching connio.Conn.SetLogger directly — lost when the brain
// stopped owning a raw socket at all (see Driver.log's doc comment for
// why the keeper itself deliberately never logs this). Both directions
// must appear: outgoing (registration's own NICK/USER, via sendLine) and
// incoming (the fake ircd's 001, via handleLine).
func TestDriverLogsRawUplinkTraffic(t *testing.T) {
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

	var logBuf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	const netID keeper.NetworkID = 1
	driver := NewDriver(client, WithLogger(log))
	driver.RegisterNetwork(netID, NetworkConfig{PrimaryNick: "gobncbrain", NickRecovery: true, Name: "TestNet"})

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

	logged := logBuf.String()
	if !strings.Contains(logged, "TestNet") {
		t.Fatalf("log output missing network display name %q:\n%s", "TestNet", logged)
	}
	if !strings.Contains(logged, "NICK gobncbrain") {
		t.Fatalf("log output missing outgoing NICK line:\n%s", logged)
	}
	if !strings.Contains(logged, "001") {
		t.Fatalf("log output missing incoming 001 welcome line:\n%s", logged)
	}
}
