package module

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

type fakeBackend struct {
	ready    bool
	nets     []wire.NetworkInfo
	sessions map[int64]*session.Session
	db       *store.Store
	cfg      config.Config
}

func (b *fakeBackend) Store() *store.Store   { return b.db }
func (b *fakeBackend) Config() config.Config { return b.cfg }

func (b *fakeBackend) ConnectNetwork(int64) error    { return nil }
func (b *fakeBackend) DisconnectNetwork(int64) error { return nil }
func (b *fakeBackend) ReconnectNetwork(int64) error  { return nil }

func (b *fakeBackend) BrainVersion() string         { return "test-brain" }
func (b *fakeBackend) Ready() bool                  { return b.ready }
func (b *fakeBackend) Networks() []wire.NetworkInfo { return b.nets }
func (b *fakeBackend) Session(id int64) (*session.Session, bool) {
	s, ok := b.sessions[id]
	return s, ok
}

// shortSocketPath keeps the socket path under the 104-byte sun_path limit (t.TempDir embeds
// the test name).
func shortSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "gbm")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "m.sock")
}

func startServer(t *testing.T, mods []config.ModuleConfig, backend Backend) (*Server, string) {
	t.Helper()
	return startServerWith(t, mods, backend, nil)
}

// startServerWith is startServer with setup run on the Server before it serves.
func startServerWith(t *testing.T, mods []config.ModuleConfig, backend Backend, setup func(*Server)) (*Server, string) {
	t.Helper()
	path := shortSocketPath(t)
	srv := NewServer(config.ModulesConfig{Socket: path, List: mods}, backend, nil)
	if setup != nil {
		setup(srv)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	deadline := time.Now().Add(2 * time.Second)
	for {
		if c, err := net.Dial("unix", path); err == nil {
			c.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("module socket %s never came up", path)
		}
		time.Sleep(5 * time.Millisecond)
	}
	return srv, path
}

// testConn is a raw module connection speaking wire frames.
type testConn struct {
	t       *testing.T
	c       net.Conn
	r       *bufio.Reader
	nextID  uint64
	pending []wire.Event // events read while waiting for a response
}

func dialModule(t *testing.T, path string) *testConn {
	t.Helper()
	c, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return &testConn{t: t, c: c, r: bufio.NewReader(c)}
}

func (tc *testConn) send(method string, params any) uint64 {
	tc.t.Helper()
	tc.nextID++
	b, err := json.Marshal(params)
	if err != nil {
		tc.t.Fatal(err)
	}
	if err := wire.WriteFrame(tc.c, wire.Request{ID: tc.nextID, Method: method, Params: b}); err != nil {
		tc.t.Fatal(err)
	}
	return tc.nextID
}

// read returns the next frame (2 s deadline).
func (tc *testConn) read() (wire.Frame, error) {
	_ = tc.c.SetReadDeadline(time.Now().Add(2 * time.Second))
	return wire.ReadFrame(tc.r)
}

// response skips events until the response to id.
func (tc *testConn) response(id uint64) *wire.Response {
	tc.t.Helper()
	for {
		f, err := tc.read()
		if err != nil {
			tc.t.Fatalf("waiting for response %d: %v", id, err)
		}
		if f.Kind == wire.KindResponse && f.Response.ID == id {
			return f.Response
		}
		if f.Kind == wire.KindEvent {
			tc.pending = append(tc.pending, *f.Event)
		}
	}
}

// call sends a request and decodes its result; it fails the test on an error response.
func (tc *testConn) call(method string, params, result any) {
	tc.t.Helper()
	resp := tc.response(tc.send(method, params))
	if resp.Error != nil {
		tc.t.Fatalf("%s: %v", method, resp.Error)
	}
	if result != nil {
		if err := json.Unmarshal(resp.Result, result); err != nil {
			tc.t.Fatal(err)
		}
	}
}

// callErr sends a request and returns its error code ("" on success).
func (tc *testConn) callErr(method string, params any) string {
	tc.t.Helper()
	resp := tc.response(tc.send(method, params))
	if resp.Error == nil {
		return ""
	}
	return resp.Error.Code
}

func (tc *testConn) hello(module string, features ...string) wire.HelloResult {
	tc.t.Helper()
	var res wire.HelloResult
	tc.call(wire.MethodHello, wire.HelloParams{API: wire.APIVersion, Module: module, Features: features}, &res)
	return res
}

// expectClosed asserts the brain closed the connection.
func (tc *testConn) expectClosed() {
	tc.t.Helper()
	for {
		_, err := tc.read()
		if err == nil {
			continue
		}
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			tc.t.Fatal("connection still open")
		}
		return
	}
}

func TestSocketMode0600(t *testing.T) {
	_, path := startServer(t, nil, &fakeBackend{})
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode = %o, want 600", fi.Mode().Perm())
	}
}

func TestFirstFrameMustBeHello(t *testing.T) {
	_, path := startServer(t, []config.ModuleConfig{{Name: "m", Features: []string{"events"}}}, &fakeBackend{})
	tc := dialModule(t, path)
	if code := tc.callErr(wire.MethodNetworksList, wire.Empty{}); code != wire.CodeHelloRequired {
		t.Fatalf("code = %q, want hello_required", code)
	}
	tc.expectClosed()
}

func TestHelloUnsupportedAPI(t *testing.T) {
	_, path := startServer(t, []config.ModuleConfig{{Name: "m", Features: []string{"events"}}}, &fakeBackend{})
	tc := dialModule(t, path)
	if code := tc.callErr(wire.MethodHello, wire.HelloParams{API: 2, Module: "m"}); code != wire.CodeUnsupportedAPI {
		t.Fatalf("code = %q, want unsupported_api", code)
	}
	tc.expectClosed()
}

func TestHelloUnknownModule(t *testing.T) {
	_, path := startServer(t, []config.ModuleConfig{{Name: "m", Features: []string{"events"}}}, &fakeBackend{})
	tc := dialModule(t, path)
	if code := tc.callErr(wire.MethodHello, wire.HelloParams{API: 1, Module: "other"}); code != wire.CodeNotGranted {
		t.Fatalf("code = %q, want not_granted", code)
	}
	tc.expectClosed()
}

func TestHelloGrantsConfiguredFeaturesOnly(t *testing.T) {
	_, path := startServer(t, []config.ModuleConfig{{Name: "m", Features: []string{"events"}}}, &fakeBackend{ready: true})
	tc := dialModule(t, path)
	res := tc.hello("m", "events", "send")
	want := wire.HelloResult{API: 1, BrainVersion: "test-brain", Granted: []string{"events"}, Denied: []string{"send"}, Ready: true}
	if !reflect.DeepEqual(res, want) {
		t.Fatalf("hello = %+v, want %+v", res, want)
	}
}

// connOf returns the registered connection of module (nil when none).
func connOf(s *Server, module string) *Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.conns {
		if c.module == module {
			return c
		}
	}
	return nil
}

func TestHelloAcceptsPIDAndHandoff(t *testing.T) {
	srv, path := startServer(t, []config.ModuleConfig{{Name: "a"}, {Name: "b"}}, &fakeBackend{ready: true})
	tc := dialModule(t, path)
	var res wire.HelloResult
	tc.call(wire.MethodHello, wire.HelloParams{API: wire.APIVersion, Module: "a", PID: 4242, Handoff: true}, &res)
	dialModule(t, path).hello("b")
	if c := connOf(srv, "a"); c == nil || c.pid != 4242 || !c.handoff {
		t.Fatalf("conn a = %+v, want pid 4242 handoff", c)
	}
	if c := connOf(srv, "b"); c == nil || c.pid != 0 || c.handoff {
		t.Fatalf("conn b = %+v, want pid 0, no handoff", c)
	}
}
