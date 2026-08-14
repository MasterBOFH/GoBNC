// Package keeperboot is how a brain (gobnc) finds or starts the keeper
// process it needs to attach to. See docs/keeper-design.md for the split
// this completes. internal/keeper and cmd/keeper own the keeper itself;
// this package owns none of that — it only answers "is one already
// running, and if not, start one" for a caller that just wants an
// attached *keeper.AttachClient back.
//
// This is new, standalone infrastructure. Nothing in cmd/gobnc's real
// startup path calls it yet — internal/uplink hasn't been cut over to the
// keeper, so there is nothing yet for the real bouncer to attach to a
// keeper for. cmd/brain-register-demo is its first real caller.
package keeperboot

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/daemon"
	"github.com/MasterBOFH/GoBNC/internal/keeper"
)

// Options configures EnsureRunning. Every field has a sensible default
// (see withDefaults) — a caller that just wants "give me a keeper" can
// pass a zero Options.
type Options struct {
	// SocketPath / PidFile / LockFile default to
	// config.DefaultStateDir()/{keeper.sock,keeper.pid,keeper.lock} — the
	// same convention gobnc's own pid_file/control_socket already follow,
	// not internal/keeper.DefaultRuntimeDir() alone (no ~/.gobnc fallback,
	// not gobnc-path-aware).
	SocketPath string
	PidFile    string
	LockFile   string

	// KeeperBinary is the gobnc-keeper executable to spawn when none is
	// running. Empty resolves to a binary named "gobnc-keeper" next to
	// this process's own executable, falling back to PATH.
	KeeperBinary string
	// KeeperLogFile is passed to a spawned keeper's -log-file. Empty
	// defaults to config.DefaultStateDir()/keeper.log — deliberately not
	// discarded to /dev/null, so a keeper started this way is still
	// debuggable after the spawning process exits.
	KeeperLogFile string

	// Hello is sent on attach. Zero value (Mode "") is invalid; callers
	// should set at least Mode.
	Hello keeper.HelloMsg

	// AttachTimeout bounds a single attach attempt. Default 5s.
	AttachTimeout time.Duration
	// SpawnWaitTimeout bounds how long EnsureRunning polls for a
	// freshly-spawned keeper's socket to become attachable. Default 10s.
	SpawnWaitTimeout time.Duration
	// LockTimeout bounds how long EnsureRunning waits to acquire the
	// cross-process spawn lock before giving up. Default 10s.
	LockTimeout time.Duration

	// spawn overrides how a new keeper process is started — test-only
	// seam (unexported: only this package's own tests can set it), same
	// pattern as keeper.DialConfig.Dial. nil uses the real
	// implementation.
	spawn func(binary string, args []string) (pid int, err error)
}

// Result is what EnsureRunning found or built.
type Result struct {
	Client *keeper.AttachClient
	// Spawned is true if this call started a new keeper process rather
	// than attaching to one that was already running.
	Spawned bool
	// KeeperPID is the spawned keeper's PID, best-effort — 0 when Spawned
	// is false (an already-running keeper's PID isn't otherwise learned
	// by this package; read its pidfile directly if needed).
	KeeperPID int
}

func withDefaults(o Options) Options {
	dir := config.DefaultStateDir()
	if o.SocketPath == "" {
		o.SocketPath = filepath.Join(dir, "keeper.sock")
	}
	if o.PidFile == "" {
		o.PidFile = filepath.Join(dir, "keeper.pid")
	}
	if o.LockFile == "" {
		o.LockFile = filepath.Join(dir, "keeper.lock")
	}
	if o.KeeperLogFile == "" {
		o.KeeperLogFile = filepath.Join(dir, "keeper.log")
	}
	if o.KeeperBinary == "" {
		o.KeeperBinary = defaultKeeperBinary()
	}
	if o.AttachTimeout <= 0 {
		o.AttachTimeout = 5 * time.Second
	}
	if o.SpawnWaitTimeout <= 0 {
		o.SpawnWaitTimeout = 10 * time.Second
	}
	if o.LockTimeout <= 0 {
		o.LockTimeout = 10 * time.Second
	}
	return o
}

// defaultKeeperBinary looks for "gobnc-keeper" next to this process's own
// executable first (the expected install layout — companion binaries
// shipped together), then falls back to PATH.
func defaultKeeperBinary() string {
	const name = "gobnc-keeper"
	if exe, err := os.Executable(); err == nil {
		candidate := filepath.Join(filepath.Dir(exe), name)
		if st, err := os.Stat(candidate); err == nil && !st.IsDir() {
			return candidate
		}
	}
	return name // resolved via PATH by exec.Command/exec.LookPath at spawn time
}

// EnsureRunning returns an attached client to a running keeper, starting
// one first if none is already listening on opts.SocketPath.
//
// The sequence: try to attach; if that fails, acquire a cross-process
// lock (opts.LockFile) before doing anything else, re-check attach (a
// racing caller may have just finished spawning one while this one waited
// for the lock), then either report an inconsistent state (pidfile says a
// keeper is alive but its socket isn't accepting — refuses to spawn a
// second one rather than risk it) or spawn a new keeper and wait for it
// to become attachable.
//
// The lock is load-bearing, not defensive extra: Listener.Serve
// unconditionally removes any existing file at the socket path before
// listening, so two callers racing to spawn a keeper at the same time
// would have the second one delete the first keeper's live socket out
// from under it, orphaning it. See docs/keeper-design.md.
func EnsureRunning(ctx context.Context, opts Options) (Result, error) {
	opts = withDefaults(opts)

	if client, err := attachOnce(ctx, opts); err == nil {
		return Result{Client: client}, nil
	}

	lock, err := acquireLock(ctx, opts.LockFile, opts.LockTimeout)
	if err != nil {
		return Result{}, fmt.Errorf("keeperboot: acquire spawn lock: %w", err)
	}
	defer lock.release()

	if client, err := attachOnce(ctx, opts); err == nil {
		return Result{Client: client}, nil
	}

	if pid, err := daemon.ReadPidFile(opts.PidFile); err == nil && daemon.Alive(pid) {
		return Result{}, fmt.Errorf("keeperboot: pidfile %s names live pid %d, but %s is not accepting connections — refusing to spawn a second keeper", opts.PidFile, pid, opts.SocketPath)
	}

	spawn := opts.spawn
	if spawn == nil {
		spawn = realSpawn
	}
	pid, err := spawn(opts.KeeperBinary, spawnArgs(opts))
	if err != nil {
		return Result{}, fmt.Errorf("keeperboot: spawn %s: %w", opts.KeeperBinary, err)
	}

	client, err := attachWithRetry(ctx, opts)
	if err != nil {
		return Result{}, fmt.Errorf("keeperboot: keeper spawned (pid %d) but never became attachable: %w", pid, err)
	}
	return Result{Client: client, Spawned: true, KeeperPID: pid}, nil
}

func spawnArgs(opts Options) []string {
	return []string{
		"-socket", opts.SocketPath,
		"-pidfile", opts.PidFile,
		"-log-file", opts.KeeperLogFile,
	}
}

func attachOnce(ctx context.Context, opts Options) (*keeper.AttachClient, error) {
	actx, cancel := context.WithTimeout(ctx, opts.AttachTimeout)
	defer cancel()
	return keeper.Attach(actx, opts.SocketPath, opts.Hello)
}

// attachWithRetry polls attachOnce until it succeeds or
// opts.SpawnWaitTimeout elapses — a freshly spawned keeper needs a moment
// to create its socket dir and start listening.
func attachWithRetry(ctx context.Context, opts Options) (*keeper.AttachClient, error) {
	deadline := time.Now().Add(opts.SpawnWaitTimeout)
	var lastErr error
	for time.Now().Before(deadline) {
		client, err := attachOnce(ctx, opts)
		if err == nil {
			return client, nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	return nil, fmt.Errorf("no attachable keeper within %s (last error: %w)", opts.SpawnWaitTimeout, lastErr)
}
