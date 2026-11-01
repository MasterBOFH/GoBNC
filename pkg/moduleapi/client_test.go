package moduleapi

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/module"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

type backend struct{ ready atomic.Bool }

func (b *backend) BrainVersion() string                   { return "test" }
func (b *backend) Ready() bool                            { return b.ready.Load() }
func (b *backend) Networks() []wire.NetworkInfo           { return []wire.NetworkInfo{{ID: 1, Name: "net"}} }
func (b *backend) Session(int64) (*session.Session, bool) { return nil, false }
func (b *backend) Store() *store.Store                    { return nil }
func (b *backend) Config() config.Config                  { return config.Config{} }
func (b *backend) ConnectNetwork(int64) error             { return nil }
func (b *backend) DisconnectNetwork(int64) error          { return nil }
func (b *backend) ReconnectNetwork(int64) error           { return nil }

// brain runs a module server on a short socket path; stop ends it (a brain going away).
type brain struct {
	srv  *module.Server
	be   *backend
	stop func()
}

func startBrain(t *testing.T, sock string) *brain {
	t.Helper()
	be := &backend{}
	srv := module.NewServer(config.ModulesConfig{Socket: sock, List: []config.ModuleConfig{
		{Name: "mod", Features: []string{"networks", "events"}}}}, be, nil)
	ctx, cancel := context.WithCancel(context.Background())
	listening := make(chan struct{})
	srv.OnListening(func() { close(listening) })
	done := make(chan struct{})
	go func() { _ = srv.Serve(ctx); close(done) }()
	<-listening
	b := &brain{srv: srv, be: be}
	b.stop = sync.OnceFunc(func() { cancel(); <-done })
	t.Cleanup(b.stop)
	return b
}

func sockPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "gbc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "m.sock")
}

func TestCallEventsAndErrors(t *testing.T) {
	sock := sockPath(t)
	b := startBrain(t, sock)
	c, err := Dial(context.Background(), Options{Socket: sock, Module: "mod", Features: []string{"networks", "events", "send"}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if h := c.Hello(); len(h.Granted) != 2 || len(h.Denied) != 1 || c.Ready() {
		t.Fatalf("hello = %+v ready=%v", h, c.Ready())
	}
	var nets wire.NetworksListResult
	if err := c.Call(context.Background(), wire.MethodNetworksList, wire.Empty{}, &nets); err != nil || len(nets.Networks) != 1 {
		t.Fatalf("networks.list = %+v, %v", nets, err)
	}
	err = c.Call(context.Background(), wire.MethodSendLine, wire.SendLineParams{}, nil)
	var we *wire.Error
	if !errors.As(err, &we) || we.Code != wire.CodeNotGranted {
		t.Fatalf("send.line error = %v, want *wire.Error not_granted", err)
	}
	b.be.ready.Store(true)
	b.srv.MarkReady()
	select {
	case ev := <-c.Events():
		if ev.Event != wire.EventReady {
			t.Fatalf("event = %+v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no ready event")
	}
	if !c.Ready() {
		t.Fatal("Ready false after the ready event")
	}
}

func TestLockRefusesSecondInstance(t *testing.T) {
	sock := sockPath(t)
	startBrain(t, sock)
	c, err := Dial(context.Background(), Options{Socket: sock, Module: "mod"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := Dial(context.Background(), Options{Socket: sock, Module: "mod"}); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("second Dial: %v, want ErrAlreadyRunning", err)
	}
}

func TestRunReconnectsAndCallsOnConnectAgain(t *testing.T) {
	sock := sockPath(t)
	b := startBrain(t, sock)
	c, err := Dial(context.Background(), Options{Socket: sock, Module: "mod", Features: []string{"networks"}, SkipLock: true})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	connects := make(chan int, 4)
	var n atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = c.Run(ctx, func(ctx context.Context, c *Client) error {
			connects <- int(n.Add(1))
			return c.Call(ctx, wire.MethodNetworksList, wire.Empty{}, nil)
		})
	}()
	waitConnect := func(want int) {
		t.Helper()
		select {
		case got := <-connects:
			if got != want {
				t.Fatalf("onConnect #%d, want #%d", got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("onConnect #%d never ran", want)
		}
	}
	waitConnect(1)
	b.stop() // the brain goes away (reload)
	deadline := time.Now().Add(2 * time.Second)
	for !errors.Is(c.Call(context.Background(), wire.MethodNetworksList, wire.Empty{}, nil), ErrNotConnected) {
		if time.Now().After(deadline) {
			t.Fatal("Call never reported ErrNotConnected after the brain went away")
		}
		time.Sleep(10 * time.Millisecond)
	}
	startBrain(t, sock) // the new brain
	waitConnect(2)
}

// TestHelloSendsPIDAndHandoff reads the raw hello a client sends.
func TestHelloSendsPIDAndHandoff(t *testing.T) {
	for _, handoff := range []bool{true, false} {
		sock := sockPath(t)
		ln, err := net.Listen("unix", sock)
		if err != nil {
			t.Fatal(err)
		}
		got := make(chan []byte, 1)
		go func() {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			defer c.Close()
			line, _ := bufio.NewReader(c).ReadBytes('\n')
			got <- line
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		_, _ = Dial(ctx, Options{Socket: sock, Module: "mod", Handoff: handoff, SkipLock: true})
		cancel()
		ln.Close()
		var req struct {
			Params map[string]any `json:"params"`
		}
		if err := json.Unmarshal(<-got, &req); err != nil {
			t.Fatal(err)
		}
		if pid, _ := req.Params["pid"].(float64); int(pid) != os.Getpid() {
			t.Fatalf("hello pid = %v, want %d", req.Params["pid"], os.Getpid())
		}
		if _, present := req.Params["handoff"]; present != handoff {
			t.Fatalf("handoff=%v: hello params %v", handoff, req.Params)
		}
	}
}
