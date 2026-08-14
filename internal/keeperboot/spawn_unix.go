//go:build unix

package keeperboot

import (
	"os"
	"os/exec"
	"syscall"
)

// realSpawn starts binary detached — Setsid so it survives this process
// exiting, stdio redirected to /dev/null (the spawned keeper writes its
// own logs via -log-file, passed in args; nothing meaningful would arrive
// on inherited stdio anyway). Mirrors internal/daemon.Reborn's re-exec
// pattern, adapted for spawning a different binary rather than
// re-executing this one.
func realSpawn(binary string, args []string) (pid int, err error) {
	cmd := exec.Command(binary, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	null, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return 0, err
	}
	defer null.Close()
	cmd.Stdin = null
	cmd.Stdout = null
	cmd.Stderr = null

	if err := cmd.Start(); err != nil {
		return 0, err
	}
	pid = cmd.Process.Pid
	_ = cmd.Process.Release()
	return pid, nil
}
