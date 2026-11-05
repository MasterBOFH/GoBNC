//go:build unix

package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/module"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
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
		if c.PID == 1 || c.PID == os.Getpid() {
			return nil, fmt.Errorf("module %q reported pid %d; stop it by hand", name, c.PID)
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

// moduleReloadTimeout is how long reload waits for the replacement to take over.
var moduleReloadTimeout = 30 * time.Second

// moduleReload replaces a running module without a brain reload. A module that said handoff in
// hello gets a reload event with its current gobnc.json entry, starts its replacement itself and
// hands over to it; reload waits for a newer connection of that name and for the old one to
// close. A module without handoff support is restarted instead.
func (s *Server) moduleReload(name string) ([]string, error) {
	done, err := s.moduleOp(name)
	if err != nil {
		return nil, err
	}
	defer done()
	if !s.modulesReady.Load() {
		return nil, errors.New("brain is not ready; try again shortly")
	}
	_, entry, err := s.moduleEntry(name)
	if err != nil {
		return nil, err
	}
	conns := s.moduleConns(name)
	switch {
	case len(conns) == 0:
		return nil, fmt.Errorf("module %q is not running", name)
	case len(conns) > 1:
		return nil, fmt.Errorf("module %q has %d connections; use restart", name, len(conns))
	}
	old := conns[0]
	if !old.Handoff {
		lines, err := s.restartModuleLocked(name)
		if err != nil {
			return nil, err
		}
		lines[0] = "no handoff support; " + lines[0]
		return lines, nil
	}
	cfg := []byte(entry.Config)
	if len(cfg) == 0 || !json.Valid(cfg) {
		cfg = []byte("{}")
	}
	ev := wire.ReloadEvent{Exec: entry.Exec, Args: entry.Args, Config: cfg}
	if err := s.modules.Load().SendEvent(old.ID, wire.EventReload, ev); err != nil {
		return nil, fmt.Errorf("module %q: %w", name, err)
	}
	s.log.Info("module reload requested", "module", name, "pid", old.PID)
	var next module.ModuleConn
	oldGone := true
	tookOver := func() bool {
		next, oldGone = module.ModuleConn{}, true
		for _, c := range s.moduleConns(name) {
			switch {
			case c.ID == old.ID:
				oldGone = false
			case c.ID > old.ID && next.ID == 0:
				next = c
			}
		}
		return next.ID != 0 && oldGone
	}
	if s.waitModules(moduleReloadTimeout, tookOver) {
		s.log.Info("module reloaded", "module", name, "old_pid", old.PID, "pid", next.PID)
		return []string{fmt.Sprintf("reloaded %s: pid %d → %d", name, old.PID, next.PID)}, nil
	}
	if next.ID != 0 {
		return nil, fmt.Errorf("module %q: replacement pid %d connected but pid %d did not exit", name, next.PID, old.PID)
	}
	return nil, fmt.Errorf("module %q: replacement did not take over within %s; pid %d still serving",
		name, moduleReloadTimeout, old.PID)
}

// moduleCtlUsage names the subcommand only, as the other usage lines do, so it reads right after
// both `gobnc` and `/bnc`; MODULECTL is the control socket's verb, which no one types.
const moduleCtlUsage = "usage: module <list|start|stop|restart|reload> [name]"

// moduleCtl runs one module control verb (gobnc module ..., BNC MODULE ..., MODULECTL ...).
func (s *Server) moduleCtl(args []string) ([]string, error) {
	if len(args) == 0 {
		return nil, errors.New(moduleCtlUsage)
	}
	verb := strings.ToLower(args[0])
	if verb == "list" {
		if len(args) != 1 {
			return nil, errors.New(moduleCtlUsage)
		}
		return s.moduleList()
	}
	if len(args) != 2 {
		return nil, errors.New(moduleCtlUsage)
	}
	switch name := args[1]; verb {
	case "start":
		return s.moduleStart(name)
	case "stop":
		return s.moduleStop(name)
	case "restart":
		return s.moduleRestart(name)
	case "reload":
		return s.moduleReload(name)
	}
	return nil, errors.New(moduleCtlUsage)
}

// moduleCtlReply answers the control socket's MODULECTL: OK <json lines> or ERR <text>.
func (s *Server) moduleCtlReply(args []string) string {
	lines, err := s.moduleCtl(args)
	if err != nil {
		return "ERR " + err.Error()
	}
	b, err := json.Marshal(lines)
	if err != nil {
		return "ERR " + err.Error()
	}
	return "OK " + string(b)
}
