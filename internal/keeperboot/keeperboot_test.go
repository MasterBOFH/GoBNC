package keeperboot

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/daemon"
	"github.com/MasterBOFH/GoBNC/internal/keeper"
)

// testDir returns a fresh 0700 directory for a test's socket/pidfile/lock —
// ensureSocketDir (internal/keeper) refuses anything looser, matching the
// real deployment requirement.
func testDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	return dir
}

// startTestKeeper brings up a real Manager+Listener on sockPath — the
// "a keeper is already running" half of these tests uses the real thing,
// not a fake, since attach behavior is exactly what's under test.
func startTestKeeper(t *testing.T, sockPath string) {
	t.Helper()
	mgr := keeper.NewManager(8192, 4096, nil)
	l := keeper.NewListener(mgr, nil)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ready := make(chan struct{})
	go func() {
		close(ready)
		_ = l.Serve(ctx, sockPath)
	}()
	<-ready
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := attachOnceRaw(sockPath); err == nil {
			c.Close()
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("test keeper at %s never came up", sockPath)
}

func attachOnceRaw(sockPath string) (*keeper.AttachClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return keeper.Attach(ctx, sockPath, keeper.HelloMsg{Mode: keeper.ModeValidate})
}

func failingSpawn(t *testing.T) func(string, []string) (int, error) {
	return func(binary string, args []string) (int, error) {
		t.Fatalf("spawn called unexpectedly: binary=%q args=%v", binary, args)
		return 0, nil
	}
}

// TestEnsureRunningAttachesWhenAlreadyRunning proves the common case never
// spawns: a keeper already listening is just attached to, and spawn is
// never invoked (failingSpawn fails the test if it is).
func TestEnsureRunningAttachesWhenAlreadyRunning(t *testing.T) {
	dir := testDir(t)
	sockPath := filepath.Join(dir, "keeper.sock")
	startTestKeeper(t, sockPath)

	res, err := EnsureRunning(context.Background(), Options{
		SocketPath: sockPath,
		PidFile:    filepath.Join(dir, "keeper.pid"),
		LockFile:   filepath.Join(dir, "keeper.lock"),
		Hello:      keeper.HelloMsg{Mode: keeper.ModeValidate},
		spawn:      failingSpawn(t),
	})
	if err != nil {
		t.Fatalf("EnsureRunning: %v", err)
	}
	defer res.Client.Close()
	if res.Spawned {
		t.Fatalf("Spawned=true, want false — a keeper was already running")
	}
}

// TestEnsureRunningSpawnsWhenNoneRunning proves the spawn path: no keeper
// listening, EnsureRunning calls spawn, and once the (stubbed) spawn
// brings a real listener up at the expected socket, EnsureRunning's retry
// loop finds it and returns a real, attached client.
func TestEnsureRunningSpawnsWhenNoneRunning(t *testing.T) {
	dir := testDir(t)
	sockPath := filepath.Join(dir, "keeper.sock")

	var spawnCalls atomic.Int32
	spawn := func(binary string, args []string) (int, error) {
		spawnCalls.Add(1)
		startTestKeeper(t, sockPath)
		return 4242, nil
	}

	res, err := EnsureRunning(context.Background(), Options{
		SocketPath: sockPath,
		PidFile:    filepath.Join(dir, "keeper.pid"),
		LockFile:   filepath.Join(dir, "keeper.lock"),
		Hello:      keeper.HelloMsg{Mode: keeper.ModeValidate},
		spawn:      spawn,
	})
	if err != nil {
		t.Fatalf("EnsureRunning: %v", err)
	}
	defer res.Client.Close()
	if !res.Spawned {
		t.Fatalf("Spawned=false, want true — no keeper was running")
	}
	if res.KeeperPID != 4242 {
		t.Fatalf("KeeperPID=%d, want 4242 (from the stub)", res.KeeperPID)
	}
	if got := spawnCalls.Load(); got != 1 {
		t.Fatalf("spawn called %d times, want exactly 1", got)
	}
}

// TestEnsureRunningRefusesWhenPidfileAliveButSocketDead proves the
// inconsistent-state guard: a pidfile naming a genuinely live process
// (this test process's own PID) but a socket nothing is listening on must
// be treated as an error, not "spawn a second keeper" — spawn must never
// be called.
func TestEnsureRunningRefusesWhenPidfileAliveButSocketDead(t *testing.T) {
	dir := testDir(t)
	sockPath := filepath.Join(dir, "keeper.sock")
	pidFile := filepath.Join(dir, "keeper.pid")
	if err := daemon.WritePidFile(pidFile, os.Getpid()); err != nil {
		t.Fatalf("WritePidFile: %v", err)
	}

	_, err := EnsureRunning(context.Background(), Options{
		SocketPath: sockPath,
		PidFile:    pidFile,
		LockFile:   filepath.Join(dir, "keeper.lock"),
		Hello:      keeper.HelloMsg{Mode: keeper.ModeValidate},
		spawn:      failingSpawn(t),
	})
	if err == nil {
		t.Fatalf("EnsureRunning succeeded, want a refusal error")
	}
	if !strings.Contains(err.Error(), "refusing to spawn a second keeper") {
		t.Fatalf("err=%v, want it to mention refusing to spawn a second keeper", err)
	}
}

// TestEnsureRunningLockPreventsDoubleSpawn proves the lock's actual value,
// not just its presence: two concurrent EnsureRunning calls against the
// same not-yet-running keeper must result in exactly one spawn, not two —
// the second caller's double-checked attach (after acquiring the lock)
// must find the first caller's already-spawned keeper instead of racing
// it.
func TestEnsureRunningLockPreventsDoubleSpawn(t *testing.T) {
	dir := testDir(t)
	sockPath := filepath.Join(dir, "keeper.sock")

	var spawnCalls atomic.Int32
	spawn := func(binary string, args []string) (int, error) {
		spawnCalls.Add(1)
		startTestKeeper(t, sockPath)
		return os.Getpid(), nil
	}

	opts := Options{
		SocketPath: sockPath,
		PidFile:    filepath.Join(dir, "keeper.pid"),
		LockFile:   filepath.Join(dir, "keeper.lock"),
		Hello:      keeper.HelloMsg{Mode: keeper.ModeValidate},
		spawn:      spawn,
	}

	type outcome struct {
		res Result
		err error
	}
	results := make(chan outcome, 2)
	for i := 0; i < 2; i++ {
		go func() {
			res, err := EnsureRunning(context.Background(), opts)
			results <- outcome{res, err}
		}()
	}

	for i := 0; i < 2; i++ {
		select {
		case o := <-results:
			if o.err != nil {
				t.Fatalf("EnsureRunning: %v", o.err)
			}
			defer o.res.Client.Close()
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for concurrent EnsureRunning calls")
		}
	}

	if got := spawnCalls.Load(); got != 1 {
		t.Fatalf("spawn called %d times across two concurrent callers, want exactly 1", got)
	}
}
