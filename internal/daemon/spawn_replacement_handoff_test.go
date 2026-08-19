//go:build unix

package daemon_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/daemon"
)

// buildFakeBinary compiles a throwaway binary that records its own argv to
// argsFile, then either exits immediately or sleeps, depending on whether
// GOBNC_TEST_SLEEP is set in its environment — used below to exercise both
// halves of SpawnReplacementForHandoff's exited channel.
func buildFakeHandoffBinary(t *testing.T, dir, argsFile string) string {
	t.Helper()
	src := filepath.Join(dir, "fakegobnc.go")
	prog := fmt.Sprintf(`package main

import (
	"os"
	"time"
)

func main() {
	f, err := os.Create(%q)
	if err != nil {
		panic(err)
	}
	for _, a := range os.Args {
		f.WriteString(a + "\n")
	}
	f.Close()
	if os.Getenv("GOBNC_TEST_SLEEP") != "" {
		time.Sleep(10 * time.Second)
	}
}
`, argsFile)
	if err := os.WriteFile(src, []byte(prog), 0o644); err != nil {
		t.Fatal(err)
	}
	exePath := filepath.Join(dir, "fakegobnc")
	if out, err := exec.Command("go", "build", "-o", exePath, src).CombinedOutput(); err != nil {
		t.Fatalf("build fake binary: %v\n%s", out, err)
	}
	return exePath
}

// TestSpawnReplacementForHandoffArgs proves the child is told
// -reload-handoff <handoffSock> (so it runs the probe-then-confirm
// sequence instead of attaching live right away) and that SpawnReplacementForHandoff
// takes no pidFile at all — unlike a normal reload, the pidfile must keep
// naming the still-running old brain until the handoff is actually
// confirmed, which happens on the child's own side, not here.
func TestSpawnReplacementForHandoffArgs(t *testing.T) {
	dir := t.TempDir()
	argsFile := filepath.Join(dir, "args.txt")
	exePath := buildFakeHandoffBinary(t, dir, argsFile)

	_, proc, exited, err := daemon.SpawnReplacementForHandoff(exePath, "cfg.json", "/tmp/handoff.sock", false)
	if err != nil {
		t.Fatalf("SpawnReplacementForHandoff: %v", err)
	}
	if proc == nil {
		t.Fatal("expected a non-nil *os.Process")
	}

	deadline := time.Now().Add(3 * time.Second)
	var got string
	for {
		if b, err := os.ReadFile(argsFile); err == nil {
			got = string(b)
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fake binary never ran (args file never appeared)")
		}
		time.Sleep(10 * time.Millisecond)
	}
	lines := strings.Split(strings.TrimRight(got, "\n"), "\n")
	want := []string{exePath, "serve", "-config", "cfg.json", "-foreground", "-reload-handoff", "/tmp/handoff.sock"}
	if strings.Join(lines, " ") != strings.Join(want, " ") {
		t.Fatalf("argv=%v, want %v", lines, want)
	}

	select {
	case err := <-exited:
		if err != nil {
			t.Fatalf("fake binary exited with error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("exited channel never fired for a binary that exits immediately")
	}
}

// TestSpawnReplacementForHandoffExitedChannelBlocksWhileRunning proves the
// exited channel doesn't fire while the child is still alive — the
// handoff-timeout path (Server.handleReloadRequest) depends on being able
// to tell "still starting up" apart from "already died" via a select.
func TestSpawnReplacementForHandoffExitedChannelBlocksWhileRunning(t *testing.T) {
	dir := t.TempDir()
	argsFile := filepath.Join(dir, "args.txt")
	exePath := buildFakeHandoffBinary(t, dir, argsFile)

	// SpawnReplacementForHandoff builds the child's env from this test
	// process's own os.Environ(), so setting it here before spawning is
	// what makes the fake binary sleep instead of exiting immediately.
	t.Setenv("GOBNC_TEST_SLEEP", "1")

	_, proc, exited, err := daemon.SpawnReplacementForHandoff(exePath, "cfg.json", "/tmp/handoff.sock", false)
	if err != nil {
		t.Fatalf("SpawnReplacementForHandoff: %v", err)
	}
	t.Cleanup(func() { _ = proc.Kill() })

	select {
	case err := <-exited:
		t.Fatalf("exited channel fired too early (err=%v) for a process that should still be sleeping", err)
	case <-time.After(300 * time.Millisecond):
	}
	_ = proc.Kill()
	<-exited
}
