//go:build unix

package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/control"
	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/module"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// TestMain doubles as a fake module: started with GOBNC_FAKE_MODULE=1 (inherited from a test)
// the test binary dials the module socket as the module named in GOBNC_MODULE_NAME, appends
// "<pid> <GOBNC_MODULE_CONFIG>" to the file named by its first argument, and exits 0 on SIGTERM
// (with a second argument "ignoreterm", it ignores SIGTERM), keeping its lock 300 ms past its
// brain connection.
func TestMain(m *testing.M) {
	if os.Getenv("GOBNC_FAKE_MODULE") == "1" && os.Getenv("GOBNC_MODULE_NAME") != "" {
		fakeModuleMain()
		return
	}
	os.Exit(m.Run())
}

func fakeModuleMain() {
	ctx := context.Background()
	if len(os.Args) > 2 && os.Args[2] == "ignoreterm" {
		signal.Ignore(syscall.SIGTERM)
	} else {
		var stop context.CancelFunc
		ctx, stop = signal.NotifyContext(ctx, syscall.SIGTERM)
		defer stop()
	}
	// The lock is taken by hand so it can outlive the brain connection by 300 ms on SIGTERM,
	// as a module draining its own clients does.
	lock, err := os.OpenFile(moduleapi.LockPath(os.Getenv("GOBNC_MODULE_SOCKET"), os.Getenv("GOBNC_MODULE_NAME")), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil || syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		os.Exit(3)
	}
	c, err := moduleapi.Dial(ctx, moduleapi.Options{SkipLock: true})
	if err != nil {
		os.Exit(3)
	}
	if len(os.Args) > 1 {
		if out, err := os.OpenFile(os.Args[1], os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600); err == nil {
			fmt.Fprintf(out, "%d %s\n", os.Getpid(), os.Getenv("GOBNC_MODULE_CONFIG"))
			out.Close()
		}
	}
	<-ctx.Done()
	c.Close()
	time.Sleep(300 * time.Millisecond)
	os.Exit(0)
}

// controlTestServer serves the module socket for entries (no module is started).
func controlTestServer(t *testing.T, entries ...config.ModuleConfig) (*Server, string) {
	t.Helper()
	t.Setenv("GOBNC_FAKE_MODULE", "1")
	s, sock := moduleTestServer(t)
	s.cfg.Modules.List = entries
	srv := module.NewServer(s.cfg.ResolvedModules(), moduleBackend{s}, s.log)
	s.modules.Store(srv)
	listening := make(chan struct{})
	srv.OnListening(func() { close(listening) })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = srv.Serve(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	<-listening
	return s, sock
}

// fakeEntry is a config entry running this test binary as a fake module writing to out.
func fakeEntry(name, out string, extra ...string) config.ModuleConfig {
	return config.ModuleConfig{Name: name, Exec: os.Args[0], Args: append([]string{out}, extra...),
		Config: []byte(`{"v":1}`)}
}

// waitStarted waits for the n-th line in out and returns its pid and config.
func waitStarted(t *testing.T, out string, n int) (int, string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		b, _ := os.ReadFile(out)
		lines := strings.Split(strings.TrimSpace(string(b)), "\n")
		if len(b) > 0 && len(lines) >= n {
			pid, cfg, _ := strings.Cut(lines[n-1], " ")
			p, _ := strconv.Atoi(pid)
			t.Cleanup(func() { _ = syscall.Kill(p, syscall.SIGKILL) })
			return p, cfg
		}
		if time.Now().After(deadline) {
			t.Fatalf("fake module start %d never happened (%q)", n, b)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// waitConnected waits until name has a connection.
func waitConnected(t *testing.T, s *Server, name string) module.ModuleConn {
	t.Helper()
	if !s.waitModules(10*time.Second, func() bool { return len(s.moduleConns(name)) > 0 }) {
		t.Fatalf("module %s never connected", name)
	}
	return s.moduleConns(name)[0]
}

func TestModuleListShowsRunningAndStopped(t *testing.T) {
	s, sock := controlTestServer(t)
	out := filepath.Join(filepath.Dir(sock), "a.out")
	s.cfg.Modules.List = []config.ModuleConfig{fakeEntry("a", out), {Name: "b"}}
	s.modules.Load().SetConfig(s.cfg.ResolvedModules())
	if _, err := s.moduleStart("a"); err != nil {
		t.Fatal(err)
	}
	pid, _ := waitStarted(t, out, 1)
	waitConnected(t, s, "a")
	lines, err := s.moduleList()
	want := []string{fmt.Sprintf("a: running, pid %d, features ", pid), "b: not running"}
	if err != nil || strings.Join(lines, "|") != strings.Join(want, "|") {
		t.Fatalf("list = %q, %v; want %q", lines, err, want)
	}
}

func TestModuleStartSpawnsWhenStopped(t *testing.T) {
	s, sock := controlTestServer(t)
	out := filepath.Join(filepath.Dir(sock), "a.out")
	setEntries(s, fakeEntry("a", out))
	lines, err := s.moduleStart("a")
	if err != nil || len(lines) != 1 || !strings.HasPrefix(lines[0], "started a: pid ") {
		t.Fatalf("start = %q, %v", lines, err)
	}
	pid, cfg := waitStarted(t, out, 1)
	if lines[0] != fmt.Sprintf("started a: pid %d", pid) || cfg != `{"v":1}` {
		t.Fatalf("start = %q; module saw pid %d config %q", lines, pid, cfg)
	}
}

func TestModuleStartRefusesWhenRunning(t *testing.T) {
	s, sock := controlTestServer(t)
	out := filepath.Join(filepath.Dir(sock), "a.out")
	setEntries(s, fakeEntry("a", out), config.ModuleConfig{Name: "noexec"})
	if _, err := s.moduleStart("a"); err != nil {
		t.Fatal(err)
	}
	waitStarted(t, out, 1)
	waitConnected(t, s, "a")
	for name, want := range map[string]string{
		"a":      `module "a" is already running`,
		"noexec": `module "noexec" has no exec`,
		"nope":   `module "nope" is not configured`,
	} {
		if _, err := s.moduleStart(name); err == nil || err.Error() != want {
			t.Fatalf("start %s: %v, want %q", name, err, want)
		}
	}
}

func TestModuleStopTerminatesAndWaitsForLock(t *testing.T) {
	s, sock := controlTestServer(t)
	out := filepath.Join(filepath.Dir(sock), "a.out")
	setEntries(s, fakeEntry("a", out))
	if _, err := s.moduleStart("a"); err != nil {
		t.Fatal(err)
	}
	pid, _ := waitStarted(t, out, 1)
	waitConnected(t, s, "a")
	if lines, err := s.moduleStop("a"); err != nil || len(lines) != 1 || lines[0] != "stopped a" {
		t.Fatalf("stop = %q, %v", lines, err)
	}
	if len(s.moduleConns("a")) != 0 || lockHeld(moduleapi.LockPath(sock, "a")) {
		t.Fatal("still connected or locked after stop")
	}
	if !waitGone(pid) {
		t.Fatalf("pid %d still alive", pid)
	}
	if _, err := s.moduleStop("a"); err == nil || err.Error() != `module "a" is not running` {
		t.Fatalf("second stop: %v", err)
	}
}

func TestModuleStopKillsAfterTimeout(t *testing.T) {
	s, sock := controlTestServer(t)
	out := filepath.Join(filepath.Dir(sock), "a.out")
	setEntries(s, fakeEntry("a", out, "ignoreterm"))
	old := moduleStopTimeout
	moduleStopTimeout = 300 * time.Millisecond
	t.Cleanup(func() { moduleStopTimeout = old })
	if _, err := s.moduleStart("a"); err != nil {
		t.Fatal(err)
	}
	pid, _ := waitStarted(t, out, 1)
	waitConnected(t, s, "a")
	start := time.Now()
	if _, err := s.moduleStop("a"); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if time.Since(start) < moduleStopTimeout {
		t.Fatalf("stopped after %v: SIGTERM was not ignored", time.Since(start))
	}
	if !waitGone(pid) {
		t.Fatalf("pid %d still alive", pid)
	}
}

func TestModuleRestartNewPID(t *testing.T) {
	s, sock := controlTestServer(t)
	out := filepath.Join(filepath.Dir(sock), "a.out")
	setEntries(s, fakeEntry("a", out))
	if _, err := s.moduleStart("a"); err != nil {
		t.Fatal(err)
	}
	first, _ := waitStarted(t, out, 1)
	waitConnected(t, s, "a")
	e := fakeEntry("a", out)
	e.Config = []byte(`{"v":2}`)
	setEntries(s, e) // a rehash changed the module's config
	lines, err := s.moduleRestart("a")
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	second, cfg := waitStarted(t, out, 2)
	if want := fmt.Sprintf("restarted a: pid %d → %d", first, second); len(lines) != 1 || lines[0] != want || first == second {
		t.Fatalf("restart = %q, want %q", lines, want)
	}
	if cfg != `{"v":2}` {
		t.Fatalf("restarted module saw config %q", cfg)
	}
}

func TestModuleOpsAreExclusive(t *testing.T) {
	s, _ := controlTestServer(t)
	done, err := s.moduleOp("a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.moduleStart("a"); err == nil || err.Error() != `module "a": another operation is in progress` {
		t.Fatalf("start during an op: %v", err)
	}
	done()
	if _, err := s.moduleOp("a"); err != nil {
		t.Fatalf("op after release: %v", err)
	}
}

// setEntries replaces the configured modules (as a rehash does).
func setEntries(s *Server, entries ...config.ModuleConfig) {
	s.mu.Lock()
	s.cfg.Modules.List = entries
	cfg := s.cfg.ResolvedModules()
	s.mu.Unlock()
	s.modules.Load().SetConfig(cfg)
}

// waitGone waits up to 3 s for pid to exit.
func waitGone(pid int) bool {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if syscall.Kill(pid, 0) != nil {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// rawModule is a module connection the test drives by hand.
type rawModule struct {
	c net.Conn
	r *bufio.Reader
}

// dialRaw connects as module name with pid, saying handoff.
func dialRaw(t *testing.T, sock, name string, pid int, handoff bool) *rawModule {
	t.Helper()
	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	params, _ := json.Marshal(wire.HelloParams{API: wire.APIVersion, Module: name, PID: pid, Handoff: handoff})
	if err := wire.WriteFrame(c, wire.Request{ID: 1, Method: wire.MethodHello, Params: params}); err != nil {
		t.Fatal(err)
	}
	m := &rawModule{c: c, r: bufio.NewReader(c)}
	if f := m.next(t); f.Kind != wire.KindResponse || f.Response.Error != nil {
		t.Fatalf("hello: %+v", f)
	}
	return m
}

func (m *rawModule) next(t *testing.T) wire.Frame {
	t.Helper()
	_ = m.c.SetReadDeadline(time.Now().Add(5 * time.Second))
	f, err := wire.ReadFrame(m.r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return f
}

// reloadEvent waits for the reload event.
func (m *rawModule) reloadEvent(t *testing.T) wire.ReloadEvent {
	t.Helper()
	for {
		f := m.next(t)
		if f.Kind == wire.KindEvent && f.Event.Event == wire.EventReload {
			var ev wire.ReloadEvent
			if err := json.Unmarshal(f.Event.Data, &ev); err != nil {
				t.Fatal(err)
			}
			return ev
		}
	}
}

// reloadAsync runs moduleReload in the background.
func reloadAsync(s *Server, name string) <-chan []any {
	res := make(chan []any, 1)
	go func() {
		lines, err := s.moduleReload(name)
		res <- []any{lines, err}
	}()
	return res
}

func readyControlServer(t *testing.T, entries ...config.ModuleConfig) (*Server, string) {
	s, sock := controlTestServer(t, entries...)
	s.modulesReady.Store(true)
	return s, sock
}

func TestModuleReloadSendsEventAndWaitsForReplacement(t *testing.T) {
	entry := config.ModuleConfig{Name: "a", Exec: "/opt/a", Args: []string{"-x"}, Config: []byte(`{"v":3}`)}
	s, sock := readyControlServer(t, entry)
	a := dialRaw(t, sock, "a", 111, true)
	waitConnected(t, s, "a")
	res := reloadAsync(s, "a")
	ev := a.reloadEvent(t)
	if ev.Exec != "/opt/a" || len(ev.Args) != 1 || ev.Args[0] != "-x" || string(ev.Config) != `{"v":3}` {
		t.Fatalf("reload event = %+v", ev)
	}
	dialRaw(t, sock, "a", 222, true)
	a.c.Close()
	r := <-res
	if lines, err := r[0].([]string), r[1]; err != nil || len(lines) != 1 || lines[0] != "reloaded a: pid 111 → 222" {
		t.Fatalf("reload = %q, %v", lines, err)
	}
}

func TestModuleReloadTimeoutKeepsOld(t *testing.T) {
	s, sock := readyControlServer(t, config.ModuleConfig{Name: "a", Exec: "/opt/a"})
	old := moduleReloadTimeout
	moduleReloadTimeout = 300 * time.Millisecond
	t.Cleanup(func() { moduleReloadTimeout = old })
	a := dialRaw(t, sock, "a", 111, true)
	waitConnected(t, s, "a")
	res := reloadAsync(s, "a")
	if ev := a.reloadEvent(t); string(ev.Config) != "{}" {
		t.Fatalf("config = %s, want {}", ev.Config)
	}
	r := <-res
	err, _ := r[1].(error)
	if err == nil || !strings.Contains(err.Error(), "replacement did not take over within 300ms; pid 111 still serving") {
		t.Fatalf("reload = %v, %v", r[0], err)
	}
	if len(s.moduleConns("a")) != 1 {
		t.Fatal("old connection gone")
	}
}

func TestModuleReloadOldDidNotExit(t *testing.T) {
	s, sock := readyControlServer(t, config.ModuleConfig{Name: "a", Exec: "/opt/a"})
	old := moduleReloadTimeout
	moduleReloadTimeout = 300 * time.Millisecond
	t.Cleanup(func() { moduleReloadTimeout = old })
	a := dialRaw(t, sock, "a", 111, true)
	waitConnected(t, s, "a")
	res := reloadAsync(s, "a")
	a.reloadEvent(t)
	dialRaw(t, sock, "a", 222, true)
	r := <-res
	err, _ := r[1].(error)
	if err == nil || err.Error() != `module "a": replacement pid 222 connected but pid 111 did not exit` {
		t.Fatalf("reload = %v, %v", r[0], err)
	}
}

func TestModuleReloadFallsBackToRestart(t *testing.T) {
	s, sock := readyControlServer(t)
	out := filepath.Join(filepath.Dir(sock), "a.out")
	setEntries(s, fakeEntry("a", out))
	if _, err := s.moduleStart("a"); err != nil {
		t.Fatal(err)
	}
	first, _ := waitStarted(t, out, 1)
	waitConnected(t, s, "a")
	lines, err := s.moduleReload("a")
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	second, _ := waitStarted(t, out, 2)
	if want := fmt.Sprintf("no handoff support; restarted a: pid %d → %d", first, second); len(lines) != 1 || lines[0] != want {
		t.Fatalf("reload = %q, want %q", lines, want)
	}
}

func TestModuleReloadRefusedBeforeReady(t *testing.T) {
	s, sock := controlTestServer(t, config.ModuleConfig{Name: "a", Exec: "/opt/a"})
	dialRaw(t, sock, "a", 111, true)
	waitConnected(t, s, "a")
	if _, err := s.moduleReload("a"); err == nil || err.Error() != "brain is not ready; try again shortly" {
		t.Fatalf("reload before ready: %v", err)
	}
	s.modulesReady.Store(true)
	dialRaw(t, sock, "a", 112, true)
	s.waitModules(2*time.Second, func() bool { return len(s.moduleConns("a")) == 2 })
	if _, err := s.moduleReload("a"); err == nil || err.Error() != `module "a" has 2 connections; use restart` {
		t.Fatalf("reload with two connections: %v", err)
	}
}

func TestModuleCtlReplyUsage(t *testing.T) {
	s, _ := controlTestServer(t, config.ModuleConfig{Name: "a"})
	for _, args := range [][]string{nil, {"bogus", "a"}, {"list", "a"}, {"stop"}, {"reload", "a", "b"}} {
		if got := s.moduleCtlReply(args); got != "ERR "+moduleCtlUsage {
			t.Fatalf("MODULECTL %q = %q", args, got)
		}
	}
	if got := s.moduleCtlReply([]string{"stop", "a"}); got != `ERR module "a" is not running` {
		t.Fatalf("MODULECTL stop a = %q", got)
	}
}

func TestModuleCtlReplyList(t *testing.T) {
	s, _ := controlTestServer(t, config.ModuleConfig{Name: "a"})
	if got := s.moduleCtlReply([]string{"LIST"}); got != `OK ["a: not running"]` {
		t.Fatalf("MODULECTL list = %q", got)
	}
}

func TestControlModuleCtlDispatch(t *testing.T) {
	s, sock := controlTestServer(t, config.ModuleConfig{Name: "a"})
	path := filepath.Join(filepath.Dir(sock), "ctl.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		if c, err := ln.Accept(); err == nil {
			s.handleControl(c)
		}
	}()
	resp, err := control.Client(path, control.CmdModuleCtl+" list")
	if err != nil || resp != `OK ["a: not running"]` {
		t.Fatalf("control reply = %q, %v", resp, err)
	}
}

func TestBNCModuleList(t *testing.T) {
	s, _ := controlTestServer(t, config.ModuleConfig{Name: "a"})
	sess := session.New(store.Network{ID: 1, Name: "n", Nick: "me"}, nil, nil, nil, nil)
	sess.SetRegisteredForTest(true)
	s.attachAdmin(sess)
	dl := newFakeServerDL("c1")
	msg, _ := irc.Parse("BNC MODULE list")
	if err := sess.HandleClientMessage(dl, msg); err != nil {
		t.Fatal(err)
	}
	for _, m := range dl.snapshot() {
		if m.Command == "NOTICE" && m.Trailing() == "a: not running" {
			return
		}
	}
	t.Fatalf("BNC MODULE list: client got %+v", dl.snapshot())
}
