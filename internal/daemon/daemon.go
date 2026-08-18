//go:build unix

// Package daemon provides Unix backgrounding for gobnc serve.
//
// Go cannot safely double-fork after the runtime has started, so we re-exec
// ourselves with Setsid and an environment mark. Supervisors (systemd, rc.d)
// should pass -foreground so the process stays attached.
package daemon

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// EnvChild is set on the re-exec'd child so we do not daemonize again.
const EnvChild = "GOBNC_DAEMON_CHILD"

// Options controls backgrounding.
type Options struct {
	// PidFile is written with the child PID (0600). Required when daemonizing.
	PidFile string
	Args    []string // os.Args used for re-exec (usually os.Args)
}

// ShouldDaemonize reports whether serve should background itself.
// False when already a child, under systemd, or foreground requested.
func ShouldDaemonize(foreground bool) bool {
	if foreground {
		return false
	}
	if os.Getenv(EnvChild) != "" {
		return false
	}
	// systemd / user services set INVOCATION_ID; stay foreground (Type=simple).
	if os.Getenv("INVOCATION_ID") != "" {
		return false
	}
	return true
}

// Reborn starts a background child and exits the parent with status 0.
// On success this function does not return in the parent. The child returns nil
// and continues as the daemon.
func Reborn(opts Options) error {
	if opts.PidFile == "" {
		return fmt.Errorf("pid_file required for daemon mode")
	}
	if err := os.MkdirAll(filepath.Dir(opts.PidFile), 0o700); err != nil {
		return fmt.Errorf("pid dir: %w", err)
	}
	if err := checkStalePid(opts.PidFile); err != nil {
		return err
	}

	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("executable: %w", err)
	}
	args := opts.Args
	if len(args) == 0 {
		args = os.Args
	}
	cmd := exec.Command(exe, args[1:]...)
	cmd.Args[0] = args[0]
	cmd.Dir, _ = os.Getwd()
	cmd.Env = append(os.Environ(), EnvChild+"=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	null, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer null.Close()
	cmd.Stdin = null
	cmd.Stdout = null
	cmd.Stderr = null

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start daemon: %w", err)
	}
	if err := WritePidFile(opts.PidFile, cmd.Process.Pid); err != nil {
		_ = cmd.Process.Kill()
		return err
	}
	fmt.Fprintf(os.Stderr, "gobnc: started daemon pid %d (pid file %s)\n", cmd.Process.Pid, opts.PidFile)
	_ = cmd.Process.Release()
	os.Exit(0)
	return nil
}

// WritePidFile writes pid with mode 0600.
func WritePidFile(path string, pid int) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.Itoa(pid)+"\n"), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// RemovePidFile deletes path if it contains our PID (best-effort).
func RemovePidFile(path string) {
	if path == "" {
		return
	}
	pid, err := ReadPidFile(path)
	if err != nil || pid != os.Getpid() {
		return
	}
	_ = os.Remove(path)
}

// ReadPidFile returns the PID stored in path.
func ReadPidFile(path string) (int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	s := strings.TrimSpace(string(b))
	if s == "" {
		return 0, fmt.Errorf("empty pid file")
	}
	pid, err := strconv.Atoi(s)
	if err != nil || pid <= 0 {
		return 0, fmt.Errorf("invalid pid file")
	}
	return pid, nil
}

// Alive reports whether pid is a live process we can signal.
func Alive(pid int) bool {
	if pid <= 0 {
		return false
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = p.Signal(syscall.Signal(0))
	return err == nil
}

// Stop sends SIGTERM to the process in pidfile and waits briefly for exit.
func Stop(pidPath string, wait time.Duration) error {
	pid, err := ReadPidFile(pidPath)
	if err != nil {
		return err
	}
	if !Alive(pid) {
		_ = os.Remove(pidPath)
		return fmt.Errorf("process %d not running", pid)
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	if err := p.Signal(syscall.SIGTERM); err != nil {
		return err
	}
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		if !Alive(pid) {
			_ = os.Remove(pidPath)
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("process %d still running after %s", pid, wait)
}

func checkStalePid(path string) error {
	pid, err := ReadPidFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		_ = os.Remove(path)
		return nil
	}
	if Alive(pid) {
		return fmt.Errorf("already running (pid %d, %s)", pid, path)
	}
	_ = os.Remove(path)
	return nil
}

// SpawnReplacement starts a new foreground serve process from exe (the
// on-disk binary path — see its callers' own comments on why that must be
// resolved once at process startup, not freshly via os.Executable() here)
// and writes pidFile with the child's PID. Used by brain reload: this
// process detaches from the keeper first, then the child attaches.
// EnvChild is set so the child does not daemonize again. stdio goes to
// /dev/null unless inheritStdio is true (foreground/-debug).
func SpawnReplacement(exe, cfgPath, pidFile string, debug, inheritStdio bool) (int, error) {
	if pidFile == "" {
		return 0, fmt.Errorf("pid_file required")
	}
	if exe == "" {
		return 0, fmt.Errorf("exe required")
	}
	args := []string{"serve", "-config", cfgPath, "-foreground"}
	if debug {
		args = append(args, "-debug")
	}
	cmd := exec.Command(exe, args...)
	cmd.Args[0] = exe
	cmd.Dir, _ = os.Getwd()
	cmd.Env = append(os.Environ(), EnvChild+"=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	if inheritStdio {
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	} else {
		null, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
		if err != nil {
			return 0, err
		}
		defer null.Close()
		cmd.Stdin = null
		cmd.Stdout = null
		cmd.Stderr = null
	}

	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("start replacement: %w", err)
	}
	pid := cmd.Process.Pid
	if err := WritePidFile(pidFile, pid); err != nil {
		_ = cmd.Process.Kill()
		return 0, err
	}
	// Release, not Wait: the child is meant to outlive this process.
	// Must read Pid before this call, not after — for historical reasons
	// or Process.Release's own doc comment, Release sets Pid to -1 on
	// every non-Windows platform, so returning cmd.Process.Pid here
	// instead of the pid captured above always returned -1 (caught by
	// this fix's own test asserting the returned pid matches the pid
	// file's).
	_ = cmd.Process.Release()
	return pid, nil
}
