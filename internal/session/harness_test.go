package session

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/brain"
	"github.com/MasterBOFH/GoBNC/internal/keeper"
	"github.com/MasterBOFH/GoBNC/internal/registration"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// testUplink stands up a real keeper.Manager + keeper.Listener + AttachClient
// + brain.Driver, and a minimal demux goroutine forwarding Driver's event
// stream into a Session — the same shape internal/server.runDemux uses for
// real, just scoped to one network and one test. This is the replacement
// for what these tests used to do by constructing a real *uplink.Uplink and
// calling u.Run(ctx) directly: the uplink layer is gone, but the "drive a
// fake ircd through net.Pipe/net.Listen and watch Session react" pattern
// these tests depend on still works, one layer further down, because
// keeper.DialConfig.Dial is the exact same test-only dial override
// internal/uplink.Config.Dial used to be (see keeper.go's dialRaw).
type testUplink struct {
	t      *testing.T
	sess   *Session
	driver *brain.Driver
	netID  keeper.NetworkID
	cancel context.CancelFunc
}

// newTestUplink registers netCfg on driver/sess and dials host:port — a
// real TCP address, typically a net.Listen("tcp", "127.0.0.1:0") fake ircd
// in the same test (see internal/brain/driver_test.go's fakeIRCServer for
// the established pattern). This can't be a net.Pipe or a
// keeper.DialConfig.Dial override the way internal/uplink.Config.Dial
// tests used to work: DialConfig crosses a real wire (AttachClient over a
// real unix socket to the keeper, even when the keeper is running
// in-process in the same test binary) and Dial is tagged `json:"-"` for
// exactly this reason (see protocol.go's doc comment) — a func value
// cannot survive that round trip and silently becomes nil on the other
// side, so the keeper falls back to actually resolving "host" as a real
// DNS name instead of using the override at all (found by hand: every
// e2e test in this file failed with a real DNS lookup error on the
// placeholder host before this was switched to a real TCP listener).
// Registration is NOT started automatically — call StartRegistration once
// the caller is ready (mirrors brain.Driver's own contract).
func newTestUplink(t *testing.T, sess *Session, netCfg store.Network, host string, port int) *testUplink {
	t.Helper()
	mgr := keeper.NewManager(1<<20, 4096, nil)
	// keeper.Listener.Serve requires its socket directory to be mode 0700
	// (see security.go's ensureSocketDir) — t.TempDir() itself is 0700 on
	// some platforms but not guaranteed to be (observed 0775 here), so a
	// dedicated subdirectory with an explicit mode is needed rather than
	// relying on t.TempDir()'s own mode.
	sockDir := filepath.Join(t.TempDir(), "sock")
	if err := os.Mkdir(sockDir, 0700); err != nil {
		t.Fatalf("mkdir sockDir: %v", err)
	}
	sockPath := filepath.Join(sockDir, "keeper.sock")

	listenerCtx, cancelListener := context.WithCancel(context.Background())
	l := keeper.NewListener(mgr, nil)
	ready := make(chan struct{})
	serveErr := make(chan error, 1)
	go func() {
		close(ready)
		serveErr <- l.Serve(listenerCtx, sockPath)
	}()
	<-ready
	for i := 0; i < 200; i++ {
		if _, err := os.Stat(sockPath); err == nil {
			break
		}
		select {
		case err := <-serveErr:
			t.Fatalf("Listener.Serve exited early: %v (sockPath=%s)", err, sockPath)
		default:
		}
		time.Sleep(5 * time.Millisecond)
	}

	attachCtx, cancelAttach := context.WithTimeout(context.Background(), 5*time.Second)
	client, err := keeper.Attach(attachCtx, sockPath, keeper.HelloMsg{Mode: keeper.ModeLive})
	cancelAttach()
	if err != nil {
		cancelListener()
		t.Fatalf("keeper.Attach: %v", err)
	}
	if err := client.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}

	driver := brain.NewDriver(client)
	// Session's own s.driver field (private, set via New's last arg) needs
	// to point at this same Driver too — New was called with nil (the
	// production Driver didn't exist yet at that point), so anything
	// Session writes outbound through (CAP REQ, AUTHENTICATE, WHO, ...)
	// would otherwise silently no-op with "uplink not ready" even though
	// registration itself works fine via the demux below (which only
	// needs tu.driver, not sess.driver, to relay inbound lines).
	sess.driver = driver
	tu := &testUplink{t: t, sess: sess, driver: driver, netID: sess.NetworkID()}

	driver.RegisterNetwork(tu.netID, brain.NetworkConfig{
		PrimaryNick:  netCfg.Nick,
		AltNick:      netCfg.AltNick,
		NickRecovery: netCfg.NickRecovery,
		Username:     netCfg.Username,
		Realname:     netCfg.Realname,
		Pass:         netCfg.Pass,
		SASL: registration.SASLConfig{
			Wanted:   netCfg.SASL,
			Required: netCfg.SASLRequired,
			User:     netCfg.SASLUser,
			Pass:     netCfg.SASLPass,
		},
	})
	driver.SetChannels(tu.netID, nil)

	runCtx, cancelRun := context.WithCancel(context.Background())
	tu.cancel = func() {
		cancelRun()
		_ = client.Close()
		cancelListener()
	}
	t.Cleanup(tu.cancel)

	go func() { _ = driver.Run(runCtx) }()
	go tu.demux(runCtx)

	if err := driver.Dial(tu.netID, keeper.DialConfig{Host: host, Port: port, DialTimeout: 5 * time.Second}, 0); err != nil {
		t.Fatalf("Dial: %v", err)
	}
	return tu
}

// newFakeIRCListener opens a real loopback TCP listener for a test's fake
// ircd script to Accept() on, and returns the host/port newTestUplink
// needs to dial it — see newTestUplink's doc comment for why this replaces
// the old net.Pipe()-based fixtures.
func newFakeIRCListener(t *testing.T) (ln net.Listener, host string, port int) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	h, p, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	portN, err := strconv.Atoi(p)
	if err != nil {
		t.Fatalf("port %q: %v", p, err)
	}
	return ln, h, portN
}

// demux is internal/server.runDemux's shape, scoped to this one test
// network/session.
func (tu *testUplink) demux(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case line, ok := <-tu.driver.Lines():
			if !ok {
				return
			}
			if line.Network == tu.netID {
				tu.sess.HandleLine(line.Raw, line.Seq)
			}
		case res, ok := <-tu.driver.Results():
			if !ok {
				return
			}
			if res.Network == tu.netID && res.State.Phase == registration.PhaseFailed {
				tu.sess.HandleDisconnect(res.State.Err)
			}
		case ev, ok := <-tu.driver.NetworkEvents():
			if !ok {
				return
			}
			if ev.Network == tu.netID && ev.Kind == keeper.EventDisconnected {
				var err error
				if ev.Error != "" {
					err = fmt.Errorf("%s", ev.Error)
				}
				tu.sess.HandleDisconnect(err)
			}
		case dr, ok := <-tu.driver.DialResults():
			if !ok {
				return
			}
			if dr.Network == tu.netID && dr.OK {
				_ = tu.driver.StartRegistration(dr.Network)
			}
		}
	}
}
