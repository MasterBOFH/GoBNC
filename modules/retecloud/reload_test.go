package retecloud

import (
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// sendReload sends the module a reload event as the brain does.
func (e *testEnv) sendReload() {
	e.t.Helper()
	e.be.mu.Lock()
	srv := e.srv
	e.be.mu.Unlock()
	mods := srv.Modules()
	if len(mods) != 1 {
		e.t.Fatalf("module connections = %+v", mods)
	}
	if err := srv.SendEvent(mods[0].ID, wire.EventReload, wire.ReloadEvent{Exec: "/opt/next"}); err != nil {
		e.t.Fatal(err)
	}
}

// closedWith waits for the connection to end and returns its close frame.
func (c *wsClient) closedWith(timeout time.Duration) *websocket.CloseError {
	c.t.Helper()
	select {
	case <-c.done:
	case <-time.After(timeout):
		c.t.Fatal("connection still open")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var ce *websocket.CloseError
	if !errors.As(c.closeErr, &ce) {
		c.t.Fatalf("connection ended with %v, not a close frame", c.closeErr)
	}
	return ce
}

// answered sends an unknown request and waits for its error reply.
func (c *wsClient) answered(timeout time.Duration) bool {
	c.t.Helper()
	mark := c.count()
	rid := c.request("bogus", map[string]any{}, "")
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, f := range c.since(mark) {
			if f["request_id"] == rid {
				return true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func TestClientNameHasPID(t *testing.T) {
	c := newWSConn(&Module{}, 7, nil, "", "")
	if want := "p" + itoa(uint64(os.Getpid())) + "-ws7"; c.clientName != want {
		t.Fatalf("clientName = %q, want %q", c.clientName, want)
	}
}

func TestReloadRunsOffLoop(t *testing.T) {
	release := make(chan error)
	started := make(chan wire.ReloadEvent, 1)
	e := newTestEnvWith(t, func(m *Module) {
		m.OnReload(func(ev wire.ReloadEvent) error { started <- ev; return <-release })
	}, "net")
	// Runs before the env's cleanup: a reloader blocking the loop must not hang it.
	t.Cleanup(func() {
		select {
		case release <- errors.New("no"):
		case <-time.After(time.Second):
		}
	})
	c := e.dial(false)
	c.authenticate("dev1")
	e.sendReload()
	select {
	case ev := <-started:
		if ev.Exec != "/opt/next" {
			t.Fatalf("reloader got %+v", ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reloader not called")
	}
	if !c.answered(2 * time.Second) {
		t.Fatal("request not answered while the reloader runs")
	}
}

func TestReloadSuccessClosesWith1012(t *testing.T) {
	e := newTestEnvWith(t, func(m *Module) {
		m.OnReload(func(wire.ReloadEvent) error { return nil })
	}, "net")
	a, b := e.dial(false), e.dial(false)
	a.authenticate("dev1")
	b.authenticate("dev2")
	e.sendReload()
	for _, c := range []*wsClient{a, b} {
		if ce := c.closedWith(5 * time.Second); ce.Code != 1012 || ce.Text != "service restart" {
			t.Fatalf("close = %d %q, want 1012 service restart", ce.Code, ce.Text)
		}
	}
	select {
	case err := <-e.runErr:
		if !errors.Is(err, ErrHandedOff) {
			t.Fatalf("Run = %v, want ErrHandedOff", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
}

func TestReloadFailureKeepsServing(t *testing.T) {
	var calls atomic.Int32
	e := newTestEnvWith(t, func(m *Module) {
		m.OnReload(func(wire.ReloadEvent) error { calls.Add(1); return errors.New("replacement exited") })
	}, "net")
	c := e.dial(false)
	c.authenticate("dev1")
	for i := int32(1); i <= 2; i++ {
		e.sendReload()
		deadline := time.Now().Add(5 * time.Second)
		for calls.Load() < i && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if calls.Load() != i {
			t.Fatalf("reloader calls = %d, want %d", calls.Load(), i)
		}
		if !c.answered(2 * time.Second) {
			t.Fatal("not serving after a failed reload")
		}
	}
	select {
	case err := <-e.runErr:
		t.Fatalf("Run returned %v", err)
	default:
	}
}

func TestShutdownClosesWith1001(t *testing.T) {
	e := newTestEnv(t, "net")
	c := e.dial(false)
	c.authenticate("dev1")
	e.cancel()
	if ce := c.closedWith(5 * time.Second); ce.Code != 1001 || !strings.Contains(ce.Text, "going away") {
		t.Fatalf("close = %d %q, want 1001 going away", ce.Code, ce.Text)
	}
}
