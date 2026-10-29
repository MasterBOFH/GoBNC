package server

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// modTestClient is a raw module connection for server-level tests.
type modTestClient struct {
	t      *testing.T
	c      net.Conn
	r      *bufio.Reader
	nextID uint64
	events []wire.Event // events read while waiting for responses
}

func dialTestModule(t *testing.T, path string) *modTestClient {
	t.Helper()
	var c net.Conn
	var err error
	deadline := time.Now().Add(3 * time.Second)
	for {
		if c, err = net.Dial("unix", path); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial module socket: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Cleanup(func() { c.Close() })
	return &modTestClient{t: t, c: c, r: bufio.NewReader(c)}
}

func (m *modTestClient) read(timeout time.Duration) (wire.Frame, error) {
	_ = m.c.SetReadDeadline(time.Now().Add(timeout))
	return wire.ReadFrame(m.r)
}

func (m *modTestClient) call(method string, params, result any) *wire.Error {
	m.t.Helper()
	m.nextID++
	b, _ := json.Marshal(params)
	if err := wire.WriteFrame(m.c, wire.Request{ID: m.nextID, Method: method, Params: b}); err != nil {
		m.t.Fatal(err)
	}
	for {
		f, err := m.read(5 * time.Second)
		if err != nil {
			m.t.Fatalf("%s: %v", method, err)
		}
		if f.Kind == wire.KindEvent {
			m.events = append(m.events, *f.Event)
			continue
		}
		if f.Response.ID != m.nextID {
			continue
		}
		if f.Response.Error != nil {
			return f.Response.Error
		}
		if result != nil {
			if err := json.Unmarshal(f.Response.Result, result); err != nil {
				m.t.Fatal(err)
			}
		}
		return nil
	}
}

// waitEvent returns the first event (already read or arriving within timeout) matching ok.
func (m *modTestClient) waitEvent(name string, timeout time.Duration, ok func(json.RawMessage) bool) json.RawMessage {
	m.t.Helper()
	for i, ev := range m.events {
		if ev.Event == name && (ok == nil || ok(ev.Data)) {
			m.events = append(m.events[:i:i], m.events[i+1:]...)
			return ev.Data
		}
	}
	deadline := time.Now().Add(timeout)
	for {
		f, err := m.read(time.Until(deadline))
		if err != nil {
			m.t.Fatalf("waiting for %s event: %v (seen %+v)", name, err, m.events)
		}
		if f.Kind != wire.KindEvent {
			continue
		}
		if f.Event.Event == name && (ok == nil || ok(f.Event.Data)) {
			return f.Event.Data
		}
		m.events = append(m.events, *f.Event)
	}
}

// moduleTestServer is a Server with one module "m" configured, its socket under a short path.
func moduleTestServer(t *testing.T, features ...string) (*Server, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "gbs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	s := newResumeTestServer(t, filepath.Join(dir, "t.db"))
	sock := filepath.Join(dir, "m.sock")
	s.cfg.Modules = config.ModulesConfig{Socket: sock, List: []config.ModuleConfig{{Name: "m", Features: features}}}
	return s, sock
}

func TestModuleBootReadyAfterLiveReadyAndNetworkState(t *testing.T) {
	rk := startResumeTestKeeper(t)
	s, sock := moduleTestServer(t, "networks", "events")
	s.startModules(s.runCtx)

	fake := newDemuxFakeIRC(t)
	h, p := fake.addr(t)
	closeAfter := make(chan struct{})
	defer close(closeAfter)
	go fake.serveOne(t, "fake.example", closeAfter)
	if _, err := s.Store().UpsertNetwork(s.runCtx, store.Network{Name: "net1", Host: h, Port: p, Nick: "alice", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	n, err := s.Store().NetworkByName(s.runCtx, "net1")
	if err != nil {
		t.Fatal(err)
	}

	// A module connecting during boot, before LiveReady.
	mod := dialTestModule(t, sock)
	var hello wire.HelloResult
	if werr := mod.call(wire.MethodHello, wire.HelloParams{API: 1, Module: "m", Features: []string{"networks", "events"}}, &hello); werr != nil {
		t.Fatal(werr)
	}
	if hello.Ready {
		t.Fatal("hello said ready before LiveReady")
	}

	rk.attach(t, s, []store.Network{n}) // bootNetworks: register, SendLiveReady, ready, dial
	mod.waitEvent(wire.EventReady, 5*time.Second, nil)

	var list wire.NetworksListResult
	if werr := mod.call(wire.MethodNetworksList, wire.Empty{}, &list); werr != nil {
		t.Fatal(werr)
	}
	if len(list.Networks) != 1 || list.Networks[0].ID != n.ID || list.Networks[0].Name != "net1" {
		t.Fatalf("networks.list = %+v", list)
	}
	mod.waitEvent(wire.EventNetwork, 10*time.Second, func(d json.RawMessage) bool {
		var ev wire.NetworkEvent
		return json.Unmarshal(d, &ev) == nil && ev.Network == n.ID && ev.Change == wire.ChangeState && ev.State == wire.NetRegistered
	})
	for _, ev := range mod.events {
		if ev.Event == wire.EventReady {
			t.Fatal("ready event delivered twice")
		}
	}
}

func TestModuleHelloReadyWhenConnectingAfterBoot(t *testing.T) {
	rk := startResumeTestKeeper(t)
	s, sock := moduleTestServer(t, "networks")
	s.startModules(s.runCtx)
	rk.attach(t, s, nil)
	mod := dialTestModule(t, sock)
	var hello wire.HelloResult
	if werr := mod.call(wire.MethodHello, wire.HelloParams{API: 1, Module: "m", Features: []string{"networks"}}, &hello); werr != nil {
		t.Fatal(werr)
	}
	if !hello.Ready {
		t.Fatal("hello after LiveReady said not ready")
	}
	if _, err := mod.read(300 * time.Millisecond); err == nil {
		t.Fatal("unexpected frame (a second ready?) after a ready hello")
	}
}
