package moduleapi

import (
	"bufio"
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// TestMain doubles as a handoff replacement: with GO_HANDOFF_CHILD set, the test binary runs
// that child mode instead of the tests.
func TestMain(m *testing.M) {
	if mode := os.Getenv("GO_HANDOFF_CHILD"); mode != "" {
		handoffChild(mode)
		return
	}
	os.Exit(m.Run())
}

// handoffChild modes: serve (adopt lock and "wss", confirm, answer "child\n" on each TCP
// connection), exit1, hang, closeearly (dial, Close, exit 1), config (write GOBNC_MODULE_CONFIG to GO_HANDOFF_OUT, confirm).
func handoffChild(mode string) {
	switch mode {
	case "exit1":
		os.Exit(1)
	case "hang":
		time.Sleep(time.Minute)
		os.Exit(0)
	case "config":
		_ = os.WriteFile(os.Getenv("GO_HANDOFF_OUT"), []byte(os.Getenv("GOBNC_MODULE_CONFIG")), 0o600)
	}
	c, err := Dial(context.Background(), Options{})
	if err != nil {
		os.Exit(3)
	}
	if mode == "closeearly" { // gives up before confirming, as a replacement failing to bind does
		c.Close()
		os.Exit(1)
	}
	ln, err := InheritedListener("wss")
	if mode == "serve" && (err != nil || ln == nil) {
		os.Exit(4)
	}
	if err := c.ConfirmHandoff(); err != nil {
		os.Exit(5)
	}
	if ln == nil {
		time.Sleep(time.Minute)
		os.Exit(0)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			os.Exit(0)
		}
		_, _ = conn.Write([]byte("child\n"))
		conn.Close()
	}
}

// lockHeldElsewhere reports whether another open file holds path's flock.
func lockHeldElsewhere(t *testing.T, path string) bool {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return true
	}
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return false
}

func childEvent(t *testing.T, mode string) wire.ReloadEvent {
	t.Helper()
	t.Setenv("GO_HANDOFF_CHILD", mode)
	return wire.ReloadEvent{Exec: os.Args[0], Config: []byte(`{"v":7}`)}
}

func killPID(t *testing.T, pid int) {
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
}

func TestHandoffPassesListenerAndLock(t *testing.T) {
	sock := sockPath(t)
	startBrain(t, sock)
	c, err := Dial(context.Background(), Options{Socket: sock, Module: "mod"})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f, err := ln.(*net.TCPListener).File()
	if err != nil {
		t.Fatal(err)
	}
	pid, err := c.Handoff(context.Background(), childEvent(t, "serve"), []HandoffFile{{Name: "wss", File: f}})
	f.Close()
	if err != nil {
		t.Fatalf("Handoff: %v", err)
	}
	killPID(t, pid)
	if pid == os.Getpid() || pid <= 0 {
		t.Fatalf("pid = %d", pid)
	}
	addr := ln.Addr().String()
	ln.Close()
	c.Release()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial after handoff: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, _ := bufio.NewReader(conn).ReadString('\n')
	conn.Close()
	if line != "child\n" {
		t.Fatalf("listener answered %q, want the replacement", line)
	}
	if !lockHeldElsewhere(t, LockPath(sock, "mod")) {
		t.Fatal("module lock released after the handoff")
	}
}

func TestHandoffChildExitFails(t *testing.T) {
	sock := sockPath(t)
	startBrain(t, sock)
	c, err := Dial(context.Background(), Options{Socket: sock, Module: "mod"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	start := time.Now()
	_, err = c.Handoff(context.Background(), childEvent(t, "exit1"), nil)
	if err == nil || !strings.Contains(err.Error(), "exited before confirming") || time.Since(start) > 2*time.Second {
		t.Fatalf("Handoff = %v after %v", err, time.Since(start))
	}
	if !lockHeldElsewhere(t, LockPath(sock, "mod")) {
		t.Fatal("old process lost its lock")
	}
}

func TestHandoffTimeoutKillsChild(t *testing.T) {
	sock := sockPath(t)
	startBrain(t, sock)
	c, err := Dial(context.Background(), Options{Socket: sock, Module: "mod"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	old := HandoffTimeout
	HandoffTimeout = 500 * time.Millisecond
	t.Cleanup(func() { HandoffTimeout = old })
	_, err = c.Handoff(context.Background(), childEvent(t, "hang"), nil)
	if err == nil || !strings.Contains(err.Error(), "did not confirm within 500ms") {
		t.Fatalf("Handoff = %v", err)
	}
	// The killed child is reaped by Handoff's Wait; nothing of it may hold the lock.
	if !lockHeldElsewhere(t, LockPath(sock, "mod")) {
		t.Fatal("old process lost its lock")
	}
}

func TestHandoffChildGetsFreshConfig(t *testing.T) {
	sock := sockPath(t)
	startBrain(t, sock)
	c, err := Dial(context.Background(), Options{Socket: sock, Module: "mod"})
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "cfg")
	t.Setenv("GO_HANDOFF_OUT", out)
	t.Setenv("GOBNC_MODULE_CONFIG", `{"v":"stale"}`)
	pid, err := c.Handoff(context.Background(), childEvent(t, "config"), nil)
	if err != nil {
		t.Fatalf("Handoff: %v", err)
	}
	killPID(t, pid)
	c.Release()
	if b, _ := os.ReadFile(out); string(b) != `{"v":7}` {
		t.Fatalf("replacement saw config %q", b)
	}
}

func TestHandoffRefusesBadNames(t *testing.T) {
	sock := sockPath(t)
	startBrain(t, sock)
	c, err := Dial(context.Background(), Options{Socket: sock, Module: "mod"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, name := range []string{"lock", "confirm", "Bad", ""} {
		if _, err := c.Handoff(context.Background(), childEvent(t, "hang"), []HandoffFile{{Name: name, File: os.Stdin}}); err == nil {
			t.Fatalf("name %q accepted", name)
		}
	}
	nolock, err := Dial(context.Background(), Options{Socket: sock, Module: "mod", SkipLock: true})
	if err != nil {
		t.Fatal(err)
	}
	defer nolock.Close()
	if _, err := nolock.Handoff(context.Background(), childEvent(t, "hang"), nil); err == nil {
		t.Fatal("handoff without the lock accepted")
	}
}

func TestHandoffFailedReplacementLeavesLock(t *testing.T) {
	sock := sockPath(t)
	startBrain(t, sock)
	c, err := Dial(context.Background(), Options{Socket: sock, Module: "mod"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Handoff(context.Background(), childEvent(t, "closeearly"), nil); err == nil {
		t.Fatal("Handoff succeeded")
	}
	if !lockHeldElsewhere(t, LockPath(sock, "mod")) {
		t.Fatal("a failed replacement released the old process's lock")
	}
}
