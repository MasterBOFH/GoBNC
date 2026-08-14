//go:build ircd

// Live integration test: a real Manager+Listener+AttachClient (the actual
// wire protocol, not the local Keeper API) driving Driver against real
// ircds — docker/ircd's ergo service and the configured remote Undernet
// network. This is the part 3a proof: registration.Step's ActionSend
// output must survive a full round trip through WriteRequest, a real unix
// socket, a real keeper process' write to a real TCP (or TLS) uplink, and
// the reply must survive the trip back as a LineMsg before Driver reports
// ActionRegistered — no shortcuts through the keeper package's internal
// API. internal/uplink is untouched; this exercises only new code.
//
//	Run: (cd docker/ircd && docker compose up -d ergo) && \
//	  go test -tags ircd ./internal/brain/... -run Live -v
package brain

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/keeper"
	"github.com/MasterBOFH/GoBNC/internal/registration"
)

const (
	ergoAddr    = "127.0.0.1"
	ergoPort    = 6667
	ergoTLSPort = 6697

	remoteAddr = "192.168.171.1"
	remotePort = 6667
)

var liveNickCounter atomic.Uint64

func liveNick(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("%s%d%d", prefix, time.Now().Unix()%100000, liveNickCounter.Add(1))
}

// startLiveListener brings up a real Manager+Listener on a real unix
// socket in a fresh 0700 directory, returning the socket path. Mirrors
// internal/keeper's own test helpers, but this lives in internal/brain
// because it's exercising the wire protocol as an external client would,
// not the keeper package's internals.
func startLiveListener(t *testing.T) string {
	t.Helper()
	mgr := keeper.NewManager(8192, 4096, nil)
	dir := filepath.Join(t.TempDir(), "sock")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	sockPath := filepath.Join(dir, "keeper.sock")

	l := keeper.NewListener(mgr, nil)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ready := make(chan struct{})
	go func() {
		close(ready)
		_ = l.Serve(ctx, sockPath)
	}()
	<-ready
	waitForSocket(t, sockPath)
	return sockPath
}

// runLiveRegistration dials host:port for network id via the wire protocol
// (DialRequest, not Keeper.Dial directly), sends the opening registration
// lines via Driver.StartRegistration, and waits for a terminal Result. It
// asserts completion — the caller picks which real ircd to point it at.
func runLiveRegistration(t *testing.T, client *keeper.AttachClient, driver *Driver, id keeper.NetworkID, host string, port int, tlsConf *keeper.DialConfig, nick string) {
	t.Helper()

	cfg := keeper.DialConfig{Host: host, Port: port}
	if tlsConf != nil {
		cfg = *tlsConf
		cfg.Host, cfg.Port = host, port
	}
	driver.RegisterNetwork(id, NetworkConfig{PrimaryNick: nick, AltNick: nick + "_", NickRecovery: true})

	if err := client.SendDial(id, cfg, 0); err != nil {
		t.Fatalf("SendDial: %v", err)
	}

	select {
	case dr := <-driver.DialResults():
		if !dr.OK {
			t.Fatalf("DialResult.OK=false: %+v", dr)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("no DialResult within timeout")
	}

	if err := driver.StartRegistration(id); err != nil {
		t.Fatalf("StartRegistration: %v", err)
	}

	select {
	case res := <-driver.Results():
		if res.Network != id {
			t.Fatalf("Result.Network=%v, want %v", res.Network, id)
		}
		if res.State.Phase != registration.PhaseComplete {
			t.Fatalf("Result.State.Phase=%v, want complete (err=%v)", res.State.Phase, res.State.Err)
		}
		if !res.State.GotWelcome {
			t.Fatalf("GotWelcome=false at completion")
		}
		t.Logf("registered as %q on network %v", res.State.Nick, id)
	case <-time.After(30 * time.Second):
		t.Fatalf("registration did not complete within timeout")
	}

	if err := client.SendClose(id); err != nil {
		t.Fatalf("SendClose: %v", err)
	}
}

// TestLiveRegistrationAgainstErgo proves 3a against docker/ircd's ergo
// service, plaintext, entirely through the keeper wire protocol.
func TestLiveRegistrationAgainstErgo(t *testing.T) {
	sockPath := startLiveListener(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := keeper.Attach(ctx, sockPath, keeper.HelloMsg{Mode: keeper.ModeLive})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer client.Close()
	if err := client.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}

	driver := NewDriver(client)
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	go func() { _ = driver.Run(runCtx) }()

	runLiveRegistration(t, client, driver, 1, ergoAddr, ergoPort, nil, liveNick(t, "gbbrain"))
}

// TestLiveRegistrationAgainstErgoTLS proves the same thing over the TLS
// listener added to close the soak TLS gap — same keeper, same wire
// protocol, only DialConfig.TLS differs.
func TestLiveRegistrationAgainstErgoTLS(t *testing.T) {
	sockPath := startLiveListener(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := keeper.Attach(ctx, sockPath, keeper.HelloMsg{Mode: keeper.ModeLive})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer client.Close()
	if err := client.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}

	driver := NewDriver(client)
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	go func() { _ = driver.Run(runCtx) }()

	tlsCfg := keeper.DialConfig{TLS: true, TLSNoVerify: true}
	runLiveRegistration(t, client, driver, 1, ergoAddr, ergoTLSPort, &tlsCfg, liveNick(t, "gbtls"))
}

// TestLiveRegistrationAgainstRemote proves 3a against the real remote
// Undernet network used throughout this project's soak testing — the
// hardest case, since it's a real production network with real other
// users and server behavior no fixture captures perfectly.
func TestLiveRegistrationAgainstRemote(t *testing.T) {
	sockPath := startLiveListener(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := keeper.Attach(ctx, sockPath, keeper.HelloMsg{Mode: keeper.ModeLive})
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer client.Close()
	if err := client.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}

	driver := NewDriver(client)
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	go func() { _ = driver.Run(runCtx) }()

	runLiveRegistration(t, client, driver, 1, remoteAddr, remotePort, nil, liveNick(t, "gbbrain"))
}
