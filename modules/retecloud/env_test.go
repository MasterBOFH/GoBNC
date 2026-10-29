package retecloud

import (
	"compress/flate"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/MasterBOFH/GoBNC/internal/auth"
	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/history"
	"github.com/MasterBOFH/GoBNC/internal/module"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/internal/testutil"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// testBackend is a module.Backend over real Sessions and a real store, with lines fed to the
// Sessions directly (standing in for the uplink).
type testBackend struct {
	mu       sync.Mutex
	db       *store.Store
	sessions map[int64]*session.Session
	cfg      config.Config
	ready    atomic.Bool
}

func (b *testBackend) BrainVersion() string { return "test" }
func (b *testBackend) Ready() bool          { return b.ready.Load() }
func (b *testBackend) Store() *store.Store  { return b.db }
func (b *testBackend) Config() config.Config {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.cfg
}
func (b *testBackend) Networks() []wire.NetworkInfo {
	nets, _ := b.db.ListNetworks(context.Background())
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []wire.NetworkInfo
	for _, n := range nets {
		info := wire.NetworkInfo{ID: n.ID, Name: n.Name, State: wire.NetStopped, Nick: n.Nick}
		if s := b.sessions[n.ID]; s != nil {
			info.State, info.Nick = s.State(), s.Nick()
		}
		out = append(out, info)
	}
	return out
}
func (b *testBackend) Session(id int64) (*session.Session, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.sessions[id]
	return s, ok
}
func (b *testBackend) ConnectNetwork(int64) error    { return nil }
func (b *testBackend) DisconnectNetwork(int64) error { return nil }
func (b *testBackend) ReconnectNetwork(int64) error  { return nil }

// testEnv: brain (module server + backend), the module, and its WSS listener.
type testEnv struct {
	t       *testing.T
	be      *testBackend
	srv     *module.Server
	sock    string
	fx      *testutil.TLSFixture
	url     string
	mod     *Module
	seq     atomic.Uint64
	nets    map[string]int64
	cancel  context.CancelFunc
	stopSrv func()
}

const testPassword = "s3cret"

func newTestEnv(t *testing.T, networks ...string) *testEnv {
	t.Helper()
	dir, err := os.MkdirTemp("", "grc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	db, err := store.Open(filepath.Join(dir, "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	hash, _ := auth.HashPassword(testPassword)
	_ = db.SetPasswordHash(context.Background(), hash)
	e := &testEnv{t: t, sock: filepath.Join(dir, "m.sock"), fx: testutil.NewTLSFixture(t), nets: map[string]int64{},
		be: &testBackend{db: db, sessions: map[int64]*session.Session{},
			cfg: config.Config{AllowPasswordAuth: true, AllowCertAuth: true}}}
	e.be.ready.Store(true)
	for _, name := range networks {
		e.addNetwork(name)
	}
	e.startBrain()

	brain, err := moduleapi.Dial(context.Background(), moduleapi.Options{Socket: e.sock, Module: "retecloud", Features: Features, SkipLock: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { brain.Close() })
	cfg := Config{ListenAddr: "127.0.0.1:0", TLSCert: filepath.Join(e.fx.Dir, "server.crt"),
		TLSKey: filepath.Join(e.fx.Dir, "server.key"), MaxConnections: 16}
	e.mod = New(cfg, brain, nil)
	l, err := NewListener(cfg, nil, e.mod.Accept)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	done := make(chan struct{}, 2)
	go func() { _ = e.mod.Run(ctx); done <- struct{}{} }()
	go func() { _ = l.ServeListener(ctx, ln); done <- struct{}{} }()
	t.Cleanup(func() { cancel(); <-done; <-done })
	e.url = "wss://" + ln.Addr().String() + "/ws"
	return e
}

// startBrain (re)starts the module server on e.sock.
func (e *testEnv) startBrain() {
	e.t.Helper()
	srv := module.NewServer(config.ModulesConfig{Socket: e.sock, List: []config.ModuleConfig{{Name: "retecloud", Features: Features}}}, e.be, nil)
	ctx, cancel := context.WithCancel(context.Background())
	listening := make(chan struct{})
	srv.OnListening(func() { close(listening) })
	done := make(chan struct{})
	go func() { _ = srv.Serve(ctx); close(done) }()
	<-listening
	e.be.mu.Lock()
	e.srv = srv
	e.be.mu.Unlock()
	var once sync.Once
	e.stopSrv = func() { once.Do(func() { cancel(); <-done }) }
	e.t.Cleanup(e.stopSrv)
}

// addNetwork creates a network in the store and a registered Session "me" for it.
func (e *testEnv) addNetwork(name string) int64 {
	e.t.Helper()
	id, err := e.be.db.UpsertNetwork(context.Background(), store.Network{Name: name, Host: "h", Port: 1, Nick: "me", Enabled: true})
	if err != nil {
		e.t.Fatal(err)
	}
	n, _ := e.be.db.NetworkByName(context.Background(), name)
	sess := session.New(n, e.be.db, history.New(e.be.db), nil, nil)
	sess.SetStateHook(func(state string) { // what internal/server does
		e.be.mu.Lock()
		srv := e.srv
		e.be.mu.Unlock()
		if srv != nil {
			srv.NetworkChanged(wire.NetworkEvent{Network: id, Name: name, State: state, Nick: sess.Nick(), Change: wire.ChangeState})
		}
	})
	e.be.mu.Lock()
	e.be.sessions[id] = sess
	e.be.mu.Unlock()
	e.nets[name] = id
	e.feed(name, ":srv.example 001 me :Welcome", ":srv.example 005 me CHANTYPES=# PREFIX=(ov)@+ STATUSMSG=@+ CASEMAPPING=rfc1459 :ok",
		":srv.example 376 me :End")
	return id
}

func (e *testEnv) session(name string) *session.Session {
	s, _ := e.be.Session(e.nets[name])
	return s
}

// feed hands uplink lines to a network's Session.
func (e *testEnv) feed(network string, lines ...string) {
	s := e.session(network)
	for _, l := range lines {
		s.HandleLine([]byte(l), 1000+e.seq.Add(1))
	}
}

func (e *testEnv) sid(network string) string { return sessionID(e.nets[network]) }

// wsClient is a WebSocket client recording every frame.
type wsClient struct {
	t      *testing.T
	ws     *websocket.Conn
	mu     sync.Mutex
	frames []map[string]any
	notify chan struct{}
	rid    int
	pw     *io.PipeWriter // deflate stream, once binary frames start
	binary atomic.Int32   // binary frames seen
	text   atomic.Int32   // text frames seen
}

func (e *testEnv) dial(withCert bool) *wsClient {
	e.t.Helper()
	tlsCfg := e.fx.ClientTLS.Clone()
	if !withCert {
		tlsCfg.Certificates = nil
	}
	d := websocket.Dialer{TLSClientConfig: tlsCfg, HandshakeTimeout: 5 * time.Second}
	ws, _, err := d.Dial(e.url, nil)
	if err != nil {
		e.t.Fatal(err)
	}
	c := &wsClient{t: e.t, ws: ws, notify: make(chan struct{}, 1)}
	e.t.Cleanup(func() { ws.Close() })
	go c.readLoop()
	return c
}

func (c *wsClient) readLoop() {
	for {
		kind, data, err := c.ws.ReadMessage()
		if err != nil {
			if c.pw != nil {
				c.pw.Close()
			}
			return
		}
		if kind == websocket.BinaryMessage {
			c.binary.Add(1)
			if c.pw == nil {
				pr, pw := io.Pipe()
				c.pw = pw
				go c.inflateLoop(pr)
			}
			if _, err := c.pw.Write(data); err != nil {
				return
			}
			continue
		}
		c.text.Add(1)
		var frame map[string]any
		if err := json.Unmarshal(data, &frame); err != nil {
			c.t.Errorf("text frame: %v", err)
			return
		}
		c.add(frame)
	}
}

// inflateLoop decodes the connection's single deflate stream, one JSON object per frame.
func (c *wsClient) inflateLoop(r io.Reader) {
	dec := json.NewDecoder(flate.NewReader(r))
	for {
		var frame map[string]any
		if err := dec.Decode(&frame); err != nil {
			return
		}
		c.add(frame)
	}
}

func (c *wsClient) add(frame map[string]any) {
	c.mu.Lock()
	c.frames = append(c.frames, frame)
	c.mu.Unlock()
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

// request sends a frame and returns its request_id.
func (c *wsClient) request(typ string, data any, sessionID string) string {
	c.t.Helper()
	c.rid++
	rid := fmt.Sprintf("r%d", c.rid)
	frame := map[string]any{"type": typ, "request_id": rid, "data": data}
	if sessionID != "" {
		frame["session_id"] = sessionID
	}
	if err := c.ws.WriteJSON(frame); err != nil {
		c.t.Fatal(err)
	}
	return rid
}

func (c *wsClient) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.frames)
}

func (c *wsClient) since(i int) []map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]map[string]any(nil), c.frames[i:]...)
}

// waitFor returns the first frame from index from matching ok.
func (c *wsClient) waitFor(from int, timeout time.Duration, ok func(map[string]any) bool) map[string]any {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		for _, f := range c.since(from) {
			if ok(f) {
				return f
			}
		}
		select {
		case <-c.notify:
		case <-time.After(time.Until(deadline)):
			c.t.Fatalf("timed out; frames since %d: %v", from, c.since(from))
		}
	}
}

func isType(typ string) func(map[string]any) bool {
	return func(f map[string]any) bool { return f["type"] == typ }
}

func reply(rid string, types ...string) func(map[string]any) bool {
	return func(f map[string]any) bool {
		if f["request_id"] != rid {
			return false
		}
		for _, t := range types {
			if f["type"] == t {
				return true
			}
		}
		return false
	}
}

// authenticate logs in with the password and returns authenticate_success's data.
func (c *wsClient) authenticate(clientID string) map[string]any {
	c.t.Helper()
	mark := c.count()
	rid := c.request("authenticate", map[string]any{"password": testPassword, "client_id": clientID}, "")
	f := c.waitFor(mark, 5*time.Second, reply(rid, "authenticate_success", "authenticate_error"))
	if f["type"] != "authenticate_success" {
		c.t.Fatalf("authenticate: %v", f)
	}
	return f["data"].(map[string]any)
}
