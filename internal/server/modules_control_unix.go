//go:build unix

package server

import (
	"fmt"
	"strings"
	"syscall"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/module"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi"
)

// Module process control (gobnc module list|start|stop|restart|reload). Start and stop work for
// any module; reload hands over to a replacement the module starts itself (see moduleReload).

// moduleStopTimeout is how long stop waits after SIGTERM before SIGKILL.
var moduleStopTimeout = 10 * time.Second

// moduleKillWait is how long stop waits after SIGKILL.
var moduleKillWait = 2 * time.Second

// moduleOp marks name busy for the duration of one operation; release with the returned func.
func (s *Server) moduleOp(name string) (func(), error) {
	s.moduleOpsMu.Lock()
	defer s.moduleOpsMu.Unlock()
	if s.moduleOps[name] {
		return nil, fmt.Errorf("module %q: another operation is in progress", name)
	}
	if s.moduleOps == nil {
		s.moduleOps = map[string]bool{}
	}
	s.moduleOps[name] = true
	return func() {
		s.moduleOpsMu.Lock()
		delete(s.moduleOps, name)
		s.moduleOpsMu.Unlock()
	}, nil
}

func (s *Server) modulesConfig() config.ModulesConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cfg.ResolvedModules()
}

// moduleConns lists the connections of module name.
func (s *Server) moduleConns(name string) []module.ModuleConn {
	srv := s.modules.Load()
	if srv == nil {
		return nil
	}
	var out []module.ModuleConn
	for _, c := range srv.Modules() {
		if c.Module == name {
			out = append(out, c)
		}
	}
	return out
}

// moduleList describes every configured module.
func (s *Server) moduleList() ([]string, error) {
	cfg := s.modulesConfig()
	lines := []string{}
	for _, m := range cfg.List {
		conns := s.moduleConns(m.Name)
		if len(conns) == 0 {
			line := m.Name + ": not running"
			if lockHeld(moduleapi.LockPath(cfg.Socket, m.Name)) {
				line += " (lock held by another process)"
			}
			lines = append(lines, line)
			continue
		}
		for _, c := range conns {
			line := fmt.Sprintf("%s: running, pid %d", m.Name, c.PID)
			if c.Handoff {
				line += ", handoff"
			}
			lines = append(lines, line+", features "+strings.Join(c.Granted, ","))
		}
	}
	return lines, nil
}

// moduleEntry returns name's config entry, which must have an exec.
func (s *Server) moduleEntry(name string) (config.ModulesConfig, config.ModuleConfig, error) {
	cfg := s.modulesConfig()
	m, ok := cfg.Module(name)
	if !ok {
		return cfg, m, fmt.Errorf("module %q is not configured", name)
	}
	if m.Exec == "" {
		return cfg, m, fmt.Errorf("module %q has no exec", name)
	}
	return cfg, m, nil
}

func (s *Server) moduleStart(name string) ([]string, error) {
	done, err := s.moduleOp(name)
	if err != nil {
		return nil, err
	}
	defer done()
	pid, err := s.startModuleLocked(name)
	if err != nil {
		return nil, err
	}
	return []string{fmt.Sprintf("started %s: pid %d", name, pid)}, nil
}

// startModuleLocked starts name unless it runs; the caller holds name's moduleOp.
func (s *Server) startModuleLocked(name string) (int, error) {
	cfg, m, err := s.moduleEntry(name)
	if err != nil {
		return 0, err
	}
	if len(s.moduleConns(name)) > 0 || lockHeld(moduleapi.LockPath(cfg.Socket, name)) {
		return 0, fmt.Errorf("module %q is already running", name)
	}
	pid, err := spawnModule(m, cfg.Socket)
	if err != nil {
		return 0, fmt.Errorf("module %q: %w", name, err)
	}
	s.log.Info("module started", "module", name, "pid", pid)
	return pid, nil
}

func (s *Server) moduleStop(name string) ([]string, error) {
	done, err := s.moduleOp(name)
	if err != nil {
		return nil, err
	}
	defer done()
	if _, err := s.stopModuleLocked(name); err != nil {
		return nil, err
	}
	return []string{"stopped " + name}, nil
}

// stopModuleLocked SIGTERMs every process of name, waits for its connections and lock to go
// (SIGKILL after moduleStopTimeout) and returns the pids it stopped; the caller holds name's
// moduleOp.
func (s *Server) stopModuleLocked(name string) ([]int, error) {
	conns := s.moduleConns(name)
	if len(conns) == 0 {
		return nil, fmt.Errorf("module %q is not running", name)
	}
	var pids []int
	for _, c := range conns {
		if c.PID <= 0 {
			return nil, fmt.Errorf("module %q did not report its pid; stop it by hand", name)
		}
		pids = append(pids, c.PID)
	}
	for _, pid := range pids {
		_ = syscall.Kill(pid, syscall.SIGTERM)
	}
	lock := moduleapi.LockPath(s.modulesConfig().Socket, name)
	gone := func() bool { return len(s.moduleConns(name)) == 0 && !lockHeld(lock) }
	if s.waitModules(moduleStopTimeout, gone) {
		return pids, nil
	}
	for _, pid := range pids {
		if syscall.Kill(pid, 0) == nil {
			s.log.Warn("module ignored SIGTERM; killing it", "module", name, "pid", pid)
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	}
	if s.waitModules(moduleKillWait, gone) {
		return pids, nil
	}
	return nil, fmt.Errorf("module %q did not stop", name)
}

func (s *Server) moduleRestart(name string) ([]string, error) {
	done, err := s.moduleOp(name)
	if err != nil {
		return nil, err
	}
	defer done()
	return s.restartModuleLocked(name)
}

func (s *Server) restartModuleLocked(name string) ([]string, error) {
	if _, _, err := s.moduleEntry(name); err != nil {
		return nil, err
	}
	old, err := s.stopModuleLocked(name)
	if err != nil {
		return nil, err
	}
	pid, err := s.startModuleLocked(name)
	if err != nil {
		return nil, err
	}
	return []string{fmt.Sprintf("restarted %s: pid %d → %d", name, old[0], pid)}, nil
}

// waitModules waits up to d for cond, rechecking on every module connection change and every
// 100 ms (a lock is not visible through Changed).
func (s *Server) waitModules(d time.Duration, cond func() bool) bool {
	srv := s.modules.Load()
	deadline := time.NewTimer(d)
	defer deadline.Stop()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		var changed <-chan struct{}
		if srv != nil {
			changed = srv.Changed()
		}
		if cond() {
			return true
		}
		select {
		case <-changed:
		case <-tick.C:
		case <-deadline.C:
			return cond()
		}
	}
}
