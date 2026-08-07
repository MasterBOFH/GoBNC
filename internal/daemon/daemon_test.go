//go:build unix

package daemon_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/daemon"
)

func TestShouldDaemonize(t *testing.T) {
	t.Setenv(daemon.EnvChild, "")
	t.Setenv("INVOCATION_ID", "")
	if !daemon.ShouldDaemonize(false) {
		t.Fatal("expected daemonize by default on unix")
	}
	if daemon.ShouldDaemonize(true) {
		t.Fatal("foreground disables daemonize")
	}
	t.Setenv(daemon.EnvChild, "1")
	if daemon.ShouldDaemonize(false) {
		t.Fatal("child must not daemonize again")
	}
	t.Setenv(daemon.EnvChild, "")
	t.Setenv("INVOCATION_ID", "abc")
	if daemon.ShouldDaemonize(false) {
		t.Fatal("systemd invocation stays foreground")
	}
}

func TestPidFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gobnc.pid")
	if err := daemon.WritePidFile(path, 4242); err != nil {
		t.Fatal(err)
	}
	pid, err := daemon.ReadPidFile(path)
	if err != nil || pid != 4242 {
		t.Fatalf("pid=%d err=%v", pid, err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm()&0o077 != 0 {
		t.Fatalf("pid file perms too open: %v", st.Mode())
	}
}
