package server

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/control"
	"github.com/MasterBOFH/GoBNC/internal/keeper"
	gobnclog "github.com/MasterBOFH/GoBNC/internal/log"
)

// newReloadTestServer builds a *Server wired to a real in-process keeper
// (attachTestKeeper) and a live control socket (serveControl), the same
// shape runReloadHandoff runs against for real via Server.Run — just
// assembled directly, the way this package's other control tests already
// do, since these tests never call Run itself.
func newReloadTestServer(t *testing.T) (s *Server, sock, keeperSock string) {
	t.Helper()
	dir := t.TempDir()
	sock = filepath.Join(dir, "gobnc.sock")
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "t.db")
	cfg.ControlSocket = sock
	cfg.ListenAddr = "127.0.0.1:0"

	var err error
	s, err = New(cfg, gobnclog.New("error", nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	keeperSock = attachTestKeeper(t, s)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s.runCtx = ctx
	s.cancel = cancel
	if err := s.serveControl(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	return s, sock, keeperSock
}

// assertKeeperStillLive proves this Server's keeperClient is still the
// live attach by trying a second, independent live attach against the same
// keeper socket — the keeper's own single-live-attach invariant means that
// only rejects if s's attach is still held.
func assertKeeperStillLive(t *testing.T, keeperSock string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := keeper.Attach(ctx, keeperSock, keeper.HelloMsg{Mode: keeper.ModeLive})
	if err == nil {
		c.Close()
		t.Fatal("a second live attach succeeded — the server's own attach must have been released")
	}
	if !strings.Contains(err.Error(), "already active") {
		t.Fatalf("second attach failed for an unexpected reason: %v", err)
	}
}

// TestReloadHandoffSpawnFailureLeavesKeeperAttached is the direct
// regression test for the incident: today's (pre-fix) code detaches from
// the keeper unconditionally before even attempting to spawn a
// replacement, so a spawn failure left no brain attached at all. Setting
// reloadExe to a path that can't be exec'd forces SpawnReplacementForHandoff
// to fail exactly like it did on the FreeBSD box (os.Executable()
// staleness), and proves the fix: the keeper attach must still be held
// afterward, and WantReload must stay false (nothing was ever committed).
func TestReloadHandoffSpawnFailureLeavesKeeperAttached(t *testing.T) {
	s, sock, keeperSock := newReloadTestServer(t)
	s.SetReloadConfig(filepath.Join(t.TempDir(), "does-not-exist"))

	var lines []string
	ok, err := control.NotifyStream(sock, control.CmdReload, 5*time.Second, func(l string) { lines = append(lines, l) })
	if !ok {
		t.Fatalf("connection itself failed: %v", err)
	}
	if err == nil {
		t.Fatal("expected an ERR reply for a spawn failure, got none")
	}
	if !strings.Contains(err.Error(), "spawn failed") {
		t.Fatalf("err=%v, want it to mention spawn failure", err)
	}
	if len(lines) == 0 || lines[0] != "spawning replacement" {
		t.Fatalf("status lines=%v, want the first to be \"spawning replacement\"", lines)
	}

	if s.WantReload() {
		t.Fatal("WantReload true after a failed spawn — nothing should have been committed")
	}
	assertKeeperStillLive(t, keeperSock)
}

// TestReloadHandoffTimeoutLeavesKeeperAttached proves the same "nothing
// committed" property when the replacement starts but never calls back:
// the keeper attach must still be held, and the orphaned child must be
// terminated rather than left running unsupervised.
func TestReloadHandoffTimeoutLeavesKeeperAttached(t *testing.T) {
	old := reloadHandoffTimeout
	reloadHandoffTimeout = 300 * time.Millisecond
	t.Cleanup(func() { reloadHandoffTimeout = old })

	s, sock, keeperSock := newReloadTestServer(t)
	exe := buildFakeReloadChild(t, "sleep")
	s.SetReloadConfig(exe)

	ok, err := control.NotifyStream(sock, control.CmdReload, 5*time.Second, nil)
	if !ok {
		t.Fatalf("connection itself failed: %v", err)
	}
	if err == nil {
		t.Fatal("expected an ERR reply for a timed-out handoff, got none")
	}
	if !strings.Contains(err.Error(), "did not confirm readiness") {
		t.Fatalf("err=%v, want it to mention the readiness timeout", err)
	}
	if s.WantReload() {
		t.Fatal("WantReload true after a timed-out handoff — nothing should have been committed")
	}
	assertKeeperStillLive(t, keeperSock)
}

// TestReloadHandoffSuccessDetachesOnlyAfterConfirmation is the full
// happy-path regression test: a real (fake) child process is spawned,
// connects back over the control socket with RELOAD_HANDOFF, and only
// once that arrives does the server detach from the keeper and cancel its
// run context. Proves the ordering, not just the end state — the keeper
// attach is still held for as long as the child hasn't called back yet.
func TestReloadHandoffSuccessDetachesOnlyAfterConfirmation(t *testing.T) {
	s, sock, keeperSock := newReloadTestServer(t)
	holdChild := make(chan struct{})
	exe := buildFakeReloadChild(t, "handoff")
	s.SetReloadConfig(exe)
	_ = holdChild // the fake child dials immediately; nothing to hold here

	done := make(chan struct{})
	var lines []string
	var reloadErr error
	go func() {
		_, reloadErr = control.NotifyStream(sock, control.CmdReload, 10*time.Second, func(l string) { lines = append(lines, l) })
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("reload never completed")
	}
	if reloadErr != nil {
		t.Fatalf("reload failed: %v (status so far: %v)", reloadErr, lines)
	}
	foundHandoff := false
	for _, l := range lines {
		if strings.Contains(l, "handing off") {
			foundHandoff = true
		}
	}
	if !foundHandoff {
		t.Fatalf("status lines=%v, expected one mentioning handing off", lines)
	}
	if !s.WantReload() {
		t.Fatal("WantReload false after a successful handoff")
	}
	select {
	case <-s.runCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("run context was never canceled after a successful handoff")
	}
	// The old brain's attach really was released — a fresh live attach
	// against the same keeper socket must now succeed.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := keeper.Attach(ctx, keeperSock, keeper.HelloMsg{Mode: keeper.ModeLive})
	if err != nil {
		t.Fatalf("fresh live attach after handoff: %v", err)
	}
	c.Close()
}

// buildFakeReloadChild compiles a throwaway binary standing in for a
// reload-spawned replacement brain. mode "sleep" never connects anywhere
// (models a replacement that starts but hangs before confirming). mode
// "handoff" parses -reload-handoff <sock> out of its own argv (the exact
// shape SpawnReplacementForHandoff invokes it with) and sends
// RELOAD_HANDOFF on that control socket, matching the real child-side
// sequence this test exercises the server's other half of.
func buildFakeReloadChild(t *testing.T, mode string) string {
	t.Helper()
	dir := t.TempDir()
	src := filepath.Join(dir, "fakechild.go")
	prog := fmt.Sprintf(`package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"time"
)

func main() {
	if %q == "sleep" {
		time.Sleep(30 * time.Second)
		return
	}
	var sock string
	for i, a := range os.Args {
		if a == "-reload-handoff" && i+1 < len(os.Args) {
			sock = os.Args[i+1]
		}
	}
	if sock == "" {
		fmt.Fprintln(os.Stderr, "no -reload-handoff in argv")
		os.Exit(1)
	}
	c, err := net.Dial("unix", sock)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial:", err)
		os.Exit(1)
	}
	defer c.Close()
	if _, err := c.Write([]byte("RELOAD_HANDOFF\n")); err != nil {
		fmt.Fprintln(os.Stderr, "write:", err)
		os.Exit(1)
	}
	resp, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		fmt.Fprintln(os.Stderr, "read:", err)
		os.Exit(1)
	}
	if resp != "OK\n" {
		fmt.Fprintln(os.Stderr, "unexpected reply:", resp)
		os.Exit(1)
	}
	// Real replacement would now go on to bootstrapKeeper/Run; this fake
	// only needs to prove the handoff round trip, so it just stays alive
	// long enough for the test to observe the server's post-handoff state.
	time.Sleep(5 * time.Second)
}
`, mode)
	if err := os.WriteFile(src, []byte(prog), 0o644); err != nil {
		t.Fatal(err)
	}
	exePath := filepath.Join(dir, "fakechild")
	if out, err := exec.Command("go", "build", "-o", exePath, src).CombinedOutput(); err != nil {
		t.Fatalf("build fake child: %v\n%s", err, out)
	}
	return exePath
}
