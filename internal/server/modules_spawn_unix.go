//go:build unix

package server

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"github.com/MasterBOFH/GoBNC/internal/config"
)

// ModuleLockPath is the lock file a running module holds (flock) so the brain never starts it
// twice; pkg/moduleapi takes it in the module process.
func ModuleLockPath(socket, name string) string {
	return filepath.Join(filepath.Dir(socket), "gobnc-module-"+name+".lock")
}

// spawnModules starts each configured module with exec set that is neither connected nor
// holding its lock — detached (own session, stdio on /dev/null) so it outlives this brain, the
// same pattern as internal/keeperboot's keeper spawn.
func (s *Server) spawnModules(cfg config.ModulesConfig) {
	srv := s.modules.Load()
	for _, m := range cfg.List {
		if m.Exec == "" || (srv != nil && srv.Connected(m.Name)) || lockHeld(ModuleLockPath(cfg.Socket, m.Name)) {
			continue
		}
		pid, err := spawnModule(m, cfg.Socket)
		if err != nil {
			s.log.Error("module start failed", "module", m.Name, "exec", m.Exec, "err", err)
			continue
		}
		s.log.Info("module started", "module", m.Name, "pid", pid)
	}
}

// lockHeld reports whether another process holds path's flock.
func lockHeld(path string) bool {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return false
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return true
	}
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return false
}

func spawnModule(m config.ModuleConfig, socket string) (int, error) {
	modCfg := []byte(m.Config)
	if len(modCfg) == 0 {
		modCfg = []byte("{}")
	} else if !json.Valid(modCfg) {
		modCfg = []byte("{}")
	}
	cmd := exec.Command(m.Exec, m.Args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Env = append(os.Environ(),
		"GOBNC_MODULE_SOCKET="+socket,
		"GOBNC_MODULE_NAME="+m.Name,
		"GOBNC_MODULE_CONFIG="+string(modCfg))
	null, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return 0, err
	}
	defer null.Close()
	cmd.Stdin, cmd.Stdout, cmd.Stderr = null, null, null
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	// Reap it if it exits while we live (see keeperboot.realSpawn).
	go func() { _ = cmd.Wait() }()
	return cmd.Process.Pid, nil
}
