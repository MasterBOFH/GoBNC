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

// TestSpawnReplacementUsesGivenExePath is the regression test for a live
// FreeBSD failure: SpawnReplacement used to call os.Executable() itself to
// find "the on-disk binary" to re-exec, but that resolves the file *this
// process* originally exec'd from — once a long-running daemon's on-disk
// binary is replaced by a rebuild (exactly what `make build`/`go build -o`
// does), FreeBSD's os.Executable() can fail outright with ENOENT
// ("executable: no such file or directory", confirmed live), because the
// original inode it's trying to resolve is gone. SpawnReplacement now
// takes the exe path as a parameter instead, resolved once by the caller
// at process startup (while the original file still definitely exists),
// not re-derived here on every reload. This proves it execs exactly the
// path it's given and nothing self-derived.
func TestSpawnReplacementUsesGivenExePath(t *testing.T) {
	dir := t.TempDir()
	argsFile := filepath.Join(dir, "args.txt")
	src := filepath.Join(dir, "fakegobnc.go")
	// A minimal stand-in for the real gobnc binary: records its own argv
	// then exits immediately, rather than actually serving.
	prog := fmt.Sprintf(`package main

import "os"

func main() {
	f, err := os.Create(%q)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	for _, a := range os.Args {
		f.WriteString(a + "\n")
	}
}
`, argsFile)
	if err := os.WriteFile(src, []byte(prog), 0o644); err != nil {
		t.Fatal(err)
	}
	exePath := filepath.Join(dir, "fakegobnc")
	if out, err := exec.Command("go", "build", "-o", exePath, src).CombinedOutput(); err != nil {
		t.Fatalf("build fake binary: %v\n%s", err, out)
	}

	pidFile := filepath.Join(dir, "gobnc.pid")
	pid, err := daemon.SpawnReplacement(exePath, "cfg.json", pidFile, false, false)
	if err != nil {
		t.Fatalf("SpawnReplacement: %v", err)
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
	if len(lines) < 1 || lines[0] != exePath {
		t.Fatalf("argv[0]=%q, want the exact exe path %q (not something SpawnReplacement re-derived itself): %v", lines[0], exePath, lines)
	}
	want := []string{exePath, "serve", "-config", "cfg.json", "-foreground"}
	if strings.Join(lines, " ") != strings.Join(want, " ") {
		t.Fatalf("argv=%v, want %v", lines, want)
	}

	gotPID, err := daemon.ReadPidFile(pidFile)
	if err != nil {
		t.Fatalf("ReadPidFile: %v", err)
	}
	if gotPID != pid {
		t.Fatalf("pid file has %d, want %d", gotPID, pid)
	}
}

// TestSpawnReplacementRequiresExe proves the exe param is mandatory rather
// than silently falling back to some internally-resolved path (the whole
// point of removing SpawnReplacement's own os.Executable() call).
func TestSpawnReplacementRequiresExe(t *testing.T) {
	dir := t.TempDir()
	if _, err := daemon.SpawnReplacement("", "cfg.json", filepath.Join(dir, "gobnc.pid"), false, false); err == nil {
		t.Fatal("expected an error for empty exe, got nil")
	}
}
