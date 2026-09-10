//go:build unix

package keeperboot

import (
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/daemon"
)

// TestRealSpawnReapsExitedChild proves realSpawn reaps its keeper child
// once that child exits. The spawning brain stays the keeper's parent for
// as long as the brain lives (Setsid detaches the session, not the parent
// link), and an exited-but-unreaped child is a zombie that kill(pid, 0) —
// daemon.Alive, which daemon.Stop polls — still reports as live. gobnc die
// then waited its full stop timeout and logged "still running" for a
// keeper that had already exited.
func TestRealSpawnReapsExitedChild(t *testing.T) {
	pid, err := realSpawn("/bin/sh", []string{"-c", "exit 0"})
	if err != nil {
		t.Fatalf("realSpawn: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if !daemon.Alive(pid) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("pid %d still reported alive 3s after exiting: the child was never reaped (zombie)", pid)
}
