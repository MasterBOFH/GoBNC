//go:build unix

package server

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/config"
)

// buildFakeModule compiles a stand-in module: it records the attempt in <arg>.attempts, takes
// its lock (as pkg/moduleapi does; a second copy exits), appends "<pid> <GOBNC_MODULE_NAME>
// <GOBNC_MODULE_CONFIG>" to the file named by its first argument, and stays alive holding the
// lock.
func buildFakeModule(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	src := filepath.Join(dir, "fakemodule.go")
	prog := `package main

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

func main() {
	sock, name := os.Getenv("GOBNC_MODULE_SOCKET"), os.Getenv("GOBNC_MODULE_NAME")
	lock := filepath.Join(filepath.Dir(sock), "gobnc-module-"+name+".lock")
	if a, err := os.OpenFile(os.Args[1]+".attempts", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600); err == nil {
		fmt.Fprintf(a, "%d\n", os.Getpid())
		a.Close()
	}
	f, err := os.OpenFile(lock, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		os.Exit(1)
	}
	if syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		os.Exit(3) // already running
	}
	out, err := os.OpenFile(os.Args[1], os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		os.Exit(1)
	}
	fmt.Fprintf(out, "%d %s %s\n", os.Getpid(), name, os.Getenv("GOBNC_MODULE_CONFIG"))
	out.Close()
	time.Sleep(30 * time.Second)
}
`
	if err := os.WriteFile(src, []byte(prog), 0o644); err != nil {
		t.Fatal(err)
	}
	exe := filepath.Join(dir, "fakemodule")
	if out, err := exec.Command("go", "build", "-o", exe, src).CombinedOutput(); err != nil {
		t.Fatalf("build fake module: %v\n%s", err, out)
	}
	return exe
}

func TestModulesStartedDetachedOnce(t *testing.T) {
	exe := buildFakeModule(t)
	s, sock := moduleTestServer(t)
	marker := filepath.Join(filepath.Dir(sock), "started.txt")
	s.cfg.Modules.List = []config.ModuleConfig{{Name: "fake", Exec: exe, Args: []string{marker},
		Features: []string{"events"}, Config: []byte(`{"listen_addr":"127.0.0.1:0"}`)}}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel) // before s.Close's cleanup, which waits for the module server
	s.startModules(ctx)

	var pid int
	deadline := time.Now().Add(10 * time.Second)
	for {
		if b, err := os.ReadFile(marker); err == nil && len(b) > 0 {
			line := strings.TrimSpace(string(b))
			parts := strings.SplitN(line, " ", 3)
			if len(parts) != 3 || parts[1] != "fake" || parts[2] != `{"listen_addr":"127.0.0.1:0"}` {
				t.Fatalf("module saw %q", line)
			}
			pid, _ = strconv.Atoi(parts[0])
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("module never started")
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })

	// The module holds its lock: a second start pass starts nothing.
	s.spawnModules(s.cfg.ResolvedModules())
	time.Sleep(500 * time.Millisecond)
	if b, _ := os.ReadFile(marker + ".attempts"); strings.Count(string(b), "\n") != 1 {
		t.Fatalf("module started more than once (attempts by pid):\n%s", b)
	}

	// It outlives the brain's context.
	cancel()
	time.Sleep(200 * time.Millisecond)
	if err := syscall.Kill(pid, 0); err != nil {
		t.Fatalf("module died with the brain's context: %v", err)
	}
}
