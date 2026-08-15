//go:build ircd

// Live integration test: a real gobnc-keeper subprocess, spawned via the
// real internal/keeperboot.EnsureRunning spawn path (exec.Command, not the
// stubbed spawn every other keeperboot test uses), driving a real
// internal/server.Server through a resume across two separate Server
// instances attached to the same still-running keeper process — the
// two-OS-process proof cmd/brain-register-demo gave informally, made into
// a real, repeatable test. Proves: the real uplink to a real ircd survives
// a "brain restart" (a fresh Server instance, same keeper), the epoch
// never changes, no registration burst reaches the real ircd a second
// time, and the resumed Session's state (nick) is correct from the blob
// alone — gap-only delivery means there is nothing else it could come
// from.
//
//	Run: (cd docker/ircd && docker compose up -d ergo) && \
//	  go test -tags ircd ./internal/server/... -run KeeperProcess -v
package server

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/brain"
	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/keeper"
	"github.com/MasterBOFH/GoBNC/internal/keeperboot"
	gobnclog "github.com/MasterBOFH/GoBNC/internal/log"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

const (
	ergoLiveAddr = "127.0.0.1"
	ergoLivePort = 6667
)

// buildKeeperBinary compiles a real gobnc-keeper into dir, once per test —
// there is no pre-built binary to rely on in a test environment, so this
// test builds the exact thing it means to exercise.
func buildKeeperBinary(t *testing.T, dir string) string {
	t.Helper()
	bin := filepath.Join(dir, "gobnc-keeper")
	cmd := exec.Command("go", "build", "-o", bin, "github.com/MasterBOFH/GoBNC/cmd/keeper")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go build cmd/keeper: %v\n%s", err, out)
	}
	return bin
}

// attachRealKeeper wires s to a real, externally-spawned keeper via
// keeperboot.EnsureRunning — the same call internal/server.Server.Run
// makes in production (bootstrapKeeper), duplicated here rather than
// calling bootstrapKeeper directly so the test controls opts (a
// test-local socket/pidfile/lockfile, the freshly built binary) without
// touching the real production entry point's own default paths.
func attachRealKeeper(t *testing.T, s *Server, opts keeperboot.Options) keeperboot.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	res, err := keeperboot.EnsureRunning(ctx, opts)
	if err != nil {
		t.Fatalf("keeperboot.EnsureRunning: %v", err)
	}
	s.keeperClient = res.Client
	s.driver = brain.NewDriver(res.Client)
	s.resumedAtBoot = make(map[keeper.NetworkID]bool, len(res.Client.Networks))
	for _, st := range res.Client.Networks {
		if st.State == keeper.Connected {
			s.resumedAtBoot[st.ID] = true
		}
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	t.Cleanup(cancelRun)
	go func() { _ = s.driver.Run(runCtx) }()
	go s.runDemux(runCtx)
	return res
}

// queryNetworkStatus opens a short-lived validate-mode attach (allowed
// concurrently with the one live attach — see docs/keeper-design.md's
// single-live-attach section, which is scoped to live mode only) purely
// to read current NetworkStatus (epoch, state) without disturbing the
// live session under test. res1's own Client.Networks is an attach-time
// snapshot only (see HelloAckMsg's doc comment) — useless here, since
// the network in this test doesn't exist yet at either attach's own Hello
// time (fresh dial for s1; still being registered for s2).
func queryNetworkStatus(t *testing.T, sockPath string, id keeper.NetworkID) keeper.NetworkStatus {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := keeper.Attach(ctx, sockPath, keeper.HelloMsg{Mode: keeper.ModeValidate})
	if err != nil {
		t.Fatalf("validate attach: %v", err)
	}
	defer client.Close()
	for _, st := range client.Networks {
		if st.ID == id {
			return st
		}
	}
	return keeper.NetworkStatus{}
}

func newKeeperProcessTestServer(t *testing.T, dbPath string) *Server {
	t.Helper()
	cfg := config.Default()
	cfg.DBPath = dbPath
	cfg.ControlSocket = filepath.Join(filepath.Dir(dbPath), "c.sock")
	s, err := New(cfg, gobnclog.New("error", nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s.runCtx = ctx
	s.cancel = cancel
	return s
}

// TestKeeperProcessSurvivesServerRestart is the real, two-process version
// of TestGapOnlyResumeSeedsStateFromBlobNotReplay: same property, but
// against a genuinely separate gobnc-keeper OS process (spawned for real,
// not an in-process Manager+Listener) and a real ircd, closing the gap
// nothing else in this codebase's test suite covers — every other resume
// test exercises the wire protocol or the in-process keeper API, never
// the actual exec.Command spawn path together with a real uplink.
func TestKeeperProcessSurvivesServerRestart(t *testing.T) {
	stateDir := t.TempDir()
	keeperBin := buildKeeperBinary(t, stateDir)

	// t.TempDir() itself is not 0700 (0775 in this environment) — the
	// keeper's own ensureSocketDir refuses to bind inside a directory that
	// isn't (see docs/keeper-design.md's IPC security section: a wrong
	// mode is a deployment error to surface, not correct out from under
	// whoever set it up), so the spawned keeper silently exits with
	// nothing to show for it beyond a timeout, unless the socket gets its
	// own properly-permissioned subdirectory.
	sockDir := filepath.Join(stateDir, "sock")
	if err := os.Mkdir(sockDir, 0o700); err != nil {
		t.Fatalf("mkdir sockDir: %v", err)
	}

	nick := fmt.Sprintf("gbkp%d", time.Now().UnixNano()%1000000)

	baseOpts := keeperboot.Options{
		SocketPath:    filepath.Join(sockDir, "keeper.sock"),
		PidFile:       filepath.Join(stateDir, "keeper.pid"),
		LockFile:      filepath.Join(stateDir, "keeper.lock"),
		KeeperBinary:  keeperBin,
		KeeperLogFile: filepath.Join(stateDir, "keeper.log"),
		Hello:         keeper.HelloMsg{Mode: keeper.ModeLive},
	}

	dbDir := t.TempDir()
	dbPath := filepath.Join(dbDir, "t1.db")

	s1 := newKeeperProcessTestServer(t, dbPath)
	attachRealKeeper(t, s1, baseOpts)
	t.Cleanup(func() {
		// Best-effort: stop the real keeper subprocess so it doesn't leak
		// past this test. SIGTERM triggers its own graceful QuitCloseAll.
		if pidData, err := os.ReadFile(baseOpts.PidFile); err == nil {
			var pid int
			fmt.Sscanf(string(pidData), "%d", &pid)
			if pid > 0 {
				if proc, err := os.FindProcess(pid); err == nil {
					_ = proc.Signal(os.Interrupt)
				}
			}
		}
	})

	if _, err := s1.Store().UpsertNetwork(s1.runCtx, store.Network{
		Name: "ergo", Host: ergoLiveAddr, Port: ergoLivePort, Nick: nick, Enabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	n, err := s1.Store().NetworkByName(s1.runCtx, "ergo")
	if err != nil {
		t.Fatal(err)
	}

	s1.mu.Lock()
	sess1, err := s1.registerNetworkLocked(n)
	if err != nil {
		s1.mu.Unlock()
		t.Fatalf("registerNetworkLocked: %v", err)
	}
	s1.mu.Unlock()
	if err := s1.keeperClient.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady: %v", err)
	}
	s1.mu.Lock()
	if err := s1.dialNetworkLocked(n, sess1); err != nil {
		s1.mu.Unlock()
		t.Fatalf("dialNetworkLocked: %v", err)
	}
	s1.mu.Unlock()

	waitForRegistered(t, "ergo (fresh, real keeper subprocess)", sess1.Registered)
	if got := sess1.Nick(); got != nick {
		t.Fatalf("sess1 nick=%q, want %q", got, nick)
	}
	epoch1 := queryNetworkStatus(t, baseOpts.SocketPath, sess1.NetworkID()).Epoch
	if epoch1 == 0 {
		t.Fatalf("epoch1=%d, want >0 (real dial should have produced a real epoch)", epoch1)
	}
	t.Logf("registered as %q on real ergo, epoch=%d", nick, epoch1)

	// "Restart": detach s1 without QUITting the network — the real keeper
	// subprocess keeps holding the real TCP connection to ergo throughout.
	_ = s1.keeperClient.Close()

	s2 := newKeeperProcessTestServer(t, dbPath)
	attachRealKeeper(t, s2, baseOpts) // same socket: attaches to the still-running keeper, spawns nothing new

	s2.mu.Lock()
	sess2, err := s2.registerNetworkLocked(n)
	if err != nil {
		s2.mu.Unlock()
		t.Fatalf("registerNetworkLocked (resume): %v", err)
	}
	s2.mu.Unlock()
	if err := s2.keeperClient.SendLiveReady(); err != nil {
		t.Fatalf("SendLiveReady (resume): %v", err)
	}
	s2.mu.Lock()
	if err := s2.dialNetworkLocked(n, sess2); err != nil {
		s2.mu.Unlock()
		t.Fatalf("dialNetworkLocked (resume): %v", err)
	}
	s2.mu.Unlock()

	waitForRegistered(t, "ergo (resumed, real keeper subprocess)", sess2.Registered)

	epoch2 := queryNetworkStatus(t, baseOpts.SocketPath, sess2.NetworkID()).Epoch
	if epoch2 != epoch1 {
		t.Fatalf("epoch after resume=%d, want %d (unchanged — the real uplink must have survived, not redialed)", epoch2, epoch1)
	}
	if got := sess2.Nick(); got != nick {
		t.Fatalf("sess2 nick after resume=%q, want %q (blob-seeded, not a redial)", got, nick)
	}
	t.Logf("resumed on the same real keeper subprocess, epoch unchanged at %d, nick %q from the blob", epoch2, nick)
}
