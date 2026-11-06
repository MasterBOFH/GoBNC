package retecloud

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/modules/retecloud/push"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// fakeRelay is ReteCloud's relay: it records each request, opened with the phone's keys.
type fakeRelay struct {
	t      *testing.T
	srv    *httptest.Server
	mu     sync.Mutex
	keys   map[string]*ecdh.PrivateKey // relay key → the phone's private key
	got    []relayed
	status int // answer status (200 by default)
	notify chan struct{}
}

type relayed struct {
	relayKey string
	req      push.RelayRequest
	content  map[string]any
}

func newFakeRelay(t *testing.T) *fakeRelay {
	r := &fakeRelay{t: t, keys: map[string]*ecdh.PrivateKey{}, status: 200, notify: make(chan struct{}, 64)}
	r.srv = httptest.NewTLSServer(http.HandlerFunc(r.serve))
	t.Cleanup(r.srv.Close)
	return r
}

func (r *fakeRelay) url() string { return r.srv.URL + "/api/push/relay" }

func (r *fakeRelay) serve(w http.ResponseWriter, hr *http.Request) {
	var req push.RelayRequest
	_ = json.NewDecoder(hr.Body).Decode(&req)
	key := strings.TrimPrefix(hr.Header.Get("Authorization"), "Bearer ")
	r.mu.Lock()
	priv := r.keys[key]
	status := r.status
	r.mu.Unlock()
	rec := relayed{relayKey: key, req: req}
	if blob, err := base64.StdEncoding.DecodeString(req.Enc); err == nil && priv != nil {
		if !push.ValidBlobLen(len(blob)) {
			r.t.Errorf("blob length %d", len(blob))
		}
		if pt, err := push.Open(priv, blob); err == nil {
			_ = json.Unmarshal(pt, &rec.content)
		}
	}
	r.mu.Lock()
	r.got = append(r.got, rec)
	r.mu.Unlock()
	select {
	case r.notify <- struct{}{}:
	default:
	}
	w.WriteHeader(status)
	switch status {
	case 200:
		_, _ = w.Write([]byte(`{"status":"sent"}`))
	case 410:
		_, _ = w.Write([]byte(`{"error":"device_gone"}`))
	case 503:
		_, _ = w.Write([]byte(`{"error":"push_unavailable"}`))
	}
}

func (r *fakeRelay) setStatus(s int) {
	r.mu.Lock()
	r.status = s
	r.mu.Unlock()
}

func (r *fakeRelay) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.got)
}

func (r *fakeRelay) since(i int) []relayed {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]relayed(nil), r.got[i:]...)
}

// wait returns the requests since from once there are want of them.
func (r *fakeRelay) wait(from, want int) []relayed { return r.waitType(from, "", want) }

// waitType is wait counting only requests of type typ ("" = any).
func (r *fakeRelay) waitType(from int, typ string, want int) []relayed {
	r.t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		if got := ofType(r.since(from), typ); len(got) >= want {
			return got
		}
		select {
		case <-r.notify:
		case <-deadline:
			r.t.Fatalf("relay: %d requests since %d, want %d %q: %v", r.count()-from, from, want, typ, r.since(from))
		}
	}
}

func ofType(in []relayed, typ string) []relayed {
	if typ == "" {
		return in
	}
	var out []relayed
	for _, x := range in {
		if x.req.Type == typ {
			out = append(out, x)
		}
	}
	return out
}

// none checks that no request of type typ ("" = any) reaches the relay for a while.
func (r *fakeRelay) none(from int, typ, why string) {
	r.t.Helper()
	time.Sleep(250 * time.Millisecond)
	if got := ofType(r.since(from), typ); len(got) > 0 {
		r.t.Fatalf("%s: relayed %v", why, got)
	}
}

// phoneKey makes a device key; the relay opens what is sealed to it under relayKey.
func (r *fakeRelay) phoneKey(relayKey string) string {
	r.t.Helper()
	k, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		r.t.Fatal(err)
	}
	r.mu.Lock()
	r.keys[relayKey] = k
	r.mu.Unlock()
	return base64.StdEncoding.EncodeToString(k.PublicKey().Bytes())
}

// testClock is a settable clock for the module.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// pushEnv is a test env whose module relays to a fake relay with a settable clock.
func newPushEnv(t *testing.T, networks ...string) (*testEnv, *fakeRelay, *testClock) {
	t.Helper()
	relay := newFakeRelay(t)
	clock := &testClock{now: time.Now()}
	e := newTestEnvWith(t, pushSetup(relay, clock), networks...)
	return e, relay, clock
}

func pushSetup(relay *fakeRelay, clock *testClock) func(*Module) {
	return func(m *Module) {
		m.now = clock.Now
		m.relay = &push.Relay{Client: relay.srv.Client(), Backoff: []time.Duration{time.Millisecond}}
	}
}

// phone authenticates as device, syncs network (turning it on), registers for push with
// relayKey and goes to the background.
func (e *testEnv) phone(relay *fakeRelay, device, relayKey string, networks ...string) *wsClient {
	e.t.Helper()
	c := e.dial(false)
	c.authenticate(device)
	for _, n := range networks {
		c.liteSync(e.sid(n), device, nil)
	}
	f := c.call("push_register", "", map[string]any{"relay_url": relay.url(), "relay_key": relayKey,
		"public_key": relay.phoneKey(relayKey), "profile": "prof-" + device})
	if f["type"] != "push_register_success" {
		e.t.Fatalf("push_register: %v", f)
	}
	if f := c.call("push_presence", "", map[string]any{"state": "background"}); f["type"] != "push_presence_success" {
		e.t.Fatalf("push_presence: %v", f)
	}
	return c
}

func TestPushHighlightDMAndAction(t *testing.T) {
	e, relay, _ := newPushEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #rete")
	e.phone(relay, "ios-1", "rrk_1", "undernet")

	mark := relay.count()
	at := time.Now().Add(-time.Minute).Truncate(time.Second).UTC()
	e.feed("undernet", "@time="+at.Format("2006-01-02T15:04:05.000Z")+" :alice!a@h PRIVMSG #rete :\x02me\x02: are you there?")
	got := relay.wait(mark, 1)[0]
	if got.relayKey != "rrk_1" || got.req.V != 1 || got.req.Type != "message" || got.req.Time != at.Unix() {
		t.Fatalf("request = %+v", got.req)
	}
	c := got.content
	want := map[string]any{"v": float64(1), "type": "message", "eid": got.req.EventID, "src": "gobnc",
		"profile": "prof-ios-1", "session_id": e.sid("undernet"), "network": "undernet", "buffer": "#rete",
		"kind": "highlight", "sender": "alice", "text": "me: are you there?", "time": float64(at.Unix())}
	for k, v := range want {
		if c[k] != v {
			t.Errorf("%s = %v, want %v", k, c[k], v)
		}
	}
	if id, _ := c["id"].(float64); id <= 0 {
		t.Errorf("id = %v", c["id"])
	}
	if _, ok := c["action"]; ok {
		t.Errorf("action present: %v", c)
	}

	// The id is the stored row's, as history pages carry it.
	var hist wire.HistoryMessagesResult
	before := int64(1 << 40)
	brain := e.mod.brain
	if err := brain.Call(context.Background(), wire.MethodHistoryMessages, wire.HistoryMessagesParams{Network: e.nets["undernet"],
		Buffer: "#rete", BeforeID: &before, Limit: 1}, &hist); err != nil || len(hist.Messages) != 1 || float64(hist.Messages[0].ID) != c["id"] {
		t.Fatalf("history %v %v vs id %v", hist, err, c["id"])
	}

	mark = relay.count()
	e.feed("undernet", ":bob!b@h PRIVMSG me :\x01ACTION waves\x01")
	c = relay.wait(mark, 1)[0].content
	if c["kind"] != "dm" || c["buffer"] != "bob" || c["text"] != "waves" || c["action"] != true {
		t.Fatalf("DM action = %v", c)
	}
}

func TestPushSuppressed(t *testing.T) {
	e, relay, clock := newPushEnv(t, "undernet", "libera")
	e.feed("undernet", ":me!u@h JOIN #a", ":me!u@h JOIN #b")
	e.feed("libera", ":me!u@h JOIN #l")
	phone := e.phone(relay, "ios-1", "rrk_1", "undernet")
	sid := e.sid("undernet")

	mark := relay.count()
	e.feed("undernet", ":x!x@h PRIVMSG #a :nothing for us", ":me!u@h PRIVMSG #a :me myself",
		":srv.example NOTICE #a :me server notice", ":x!x@h PRIVMSG #a :\x01VERSION\x01")
	e.feed("libera", ":x!x@h PRIVMSG #l :me on a network the phone has off")
	relay.none(mark, "message", "not highlights, or a network toggled off")

	// The switch: off means off, a DM included.
	desk := e.dial(false)
	desk.authenticate("desk")
	desk.call("set_buffer_highlight", sid, map[string]any{"buffer": "#A", "highlight": false})
	desk.call("set_buffer_highlight", sid, map[string]any{"buffer": "Peer", "highlight": false})
	mark = relay.count()
	e.feed("undernet", ":x!x@h PRIVMSG #a :me there", ":peer!p@h PRIVMSG me :dm")
	relay.none(mark, "message", "switched off")

	// Foreground.
	phone.call("push_presence", "", map[string]any{"state": "foreground"})
	mark = relay.count()
	e.feed("undernet", ":x!x@h PRIVMSG #b :me fg")
	relay.none(mark, "message", "foreground")
	phone.call("push_presence", "", map[string]any{"state": "background"})
	e.feed("undernet", ":x!x@h PRIVMSG #b :me bg")
	relay.waitType(mark, "message", 1)

	// Someone reading at a desk (a lite read_cursor from another device) for 5 minutes.
	desk.liteSync(sid, "desk", nil)
	last := lastStoredID(t, e, "undernet", "#b")
	desk.call("read_cursor", sid, map[string]any{"buffer": "#b", "last_message_id": last})
	mark = relay.count()
	e.feed("undernet", ":x!x@h PRIVMSG #b :me while desk reads")
	relay.none(mark, "message", "reading elsewhere")
	clock.advance(readingElsewhereWindow + time.Second)
	e.feed("undernet", ":x!x@h PRIVMSG #b :me after the window")
	relay.waitType(mark, "message", 1)

	// An IRC client's MARKREAD counts too.
	e.session("undernet").SetReadMarkerFromModule("#b", time.Now().UTC().Format("2006-01-02T15:04:05.000Z"))
	time.Sleep(100 * time.Millisecond)
	mark = relay.count()
	e.feed("undernet", ":x!x@h PRIVMSG #b :me while an IRC client reads")
	relay.none(mark, "message", "IRC client reading")
	clock.advance(readingElsewhereWindow + time.Second)

	// The phone's own read does not count.
	phone.liteSync(sid, "ios-1", nil)
	last = lastStoredID(t, e, "undernet", "#b")
	phone.call("read_cursor", sid, map[string]any{"buffer": "#b", "last_message_id": last})
	mark = relay.count()
	e.feed("undernet", ":x!x@h PRIVMSG #b :me after my own read")
	relay.waitType(mark, "message", 1)

	// Toggling the network off on the phone.
	phone.call("unsync_session", sid, map[string]any{"client_id": "ios-1"})
	mark = relay.count()
	e.feed("undernet", ":x!x@h PRIVMSG #b :me toggled off")
	relay.none(mark, "message", "toggled off")

	// An unregistered device is never pushed.
	phone.call("push_unregister", "", map[string]any{})
	phone.liteSync(sid, "ios-1", nil)
	phone.call("push_presence", "", map[string]any{"state": "background"})
	e.feed("undernet", ":x!x@h PRIVMSG #b :me unregistered")
	relay.none(mark, "message", "unregistered")
}

// lastStoredID is the newest stored id of a buffer.
func lastStoredID(t *testing.T, e *testEnv, network, buffer string) int64 {
	t.Helper()
	var hist wire.HistoryMessagesResult
	before := int64(1 << 40)
	if err := e.mod.brain.Call(context.Background(), wire.MethodHistoryMessages, wire.HistoryMessagesParams{Network: e.nets[network],
		Buffer: buffer, BeforeID: &before, Limit: 1}, &hist); err != nil || len(hist.Messages) == 0 {
		t.Fatalf("history of %s: %v %v", buffer, hist, err)
	}
	return hist.Messages[0].ID
}

func TestPushRegisterErrorsAndPresence(t *testing.T) {
	e, relay, _ := newPushEnv(t, "undernet")
	c := e.dial(false)
	mark := c.count()
	rid := c.request("push_register", map[string]any{}, "")
	if f := c.waitFor(mark, 5*time.Second, reply(rid, "push_register_error")); f["code"] != "not_authenticated" {
		t.Fatalf("unauthenticated: %v", f)
	}
	c.authenticate("ios-1")
	key := relay.phoneKey("k")
	good := map[string]any{"relay_url": relay.url(), "relay_key": "k", "public_key": key, "profile": "p"}
	with := func(k string, v any) map[string]any {
		out := map[string]any{}
		for kk, vv := range good {
			out[kk] = vv
		}
		out[k] = v
		return out
	}
	cases := []struct {
		data map[string]any
		text string
	}{
		{with("relay_url", "http://relay.example/api/push/relay"), "relay_url must be https"},
		{with("relay_url", ""), "relay_url must be https"},
		{with("relay_key", ""), "relay_key is required"},
		{with("public_key", base64.StdEncoding.EncodeToString(make([]byte, 31))), "public_key must be 32 bytes, base64"},
		{with("profile", ""), "profile is required"},
	}
	for _, tc := range cases {
		if f := c.call("push_register", "", tc.data); f["error"] != tc.text || f["code"] != "invalid_request" {
			t.Errorf("%s: %v", tc.text, f)
		}
	}
	if f := c.call("push_register", "", good); f["type"] != "push_register_success" {
		t.Fatalf("register: %v", f)
	}
	v, found, err := e.be.db.ModuleKVGet(context.Background(), "retecloud", "push/ios-1")
	var reg pushRegistration
	if err != nil || !found || json.Unmarshal([]byte(v), &reg) != nil || reg.RelayKey != "k" || reg.RelayURL != relay.url() ||
		reg.PublicKey != key || reg.Profile != "p" || reg.RegisteredAt == "" || reg.Rejected {
		t.Fatalf("stored %q %v %v", v, found, err)
	}

	if f := c.call("push_presence", "", map[string]any{"state": "asleep"}); f["error"] != "state must be foreground or background" || f["code"] != "invalid_request" {
		t.Fatalf("bad state: %v", f)
	}
	if f := c.call("push_presence", "", map[string]any{"state": "background"}); f["data"].(map[string]any)["state"] != "background" {
		t.Fatalf("presence: %v", f)
	}
	if f := c.call("push_unregister", "", map[string]any{}); f["type"] != "push_unregister_success" {
		t.Fatalf("unregister: %v", f)
	}
	if _, found, _ := e.be.db.ModuleKVGet(context.Background(), "retecloud", "push/ios-1"); found {
		t.Fatal("registration kept after push_unregister")
	}
}

// A 410 marks the registration rejected: no more pushes, the same key is refused, a new key
// pairs the device again.
func TestRelayRejectedKey(t *testing.T) {
	e, relay, _ := newPushEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #a")
	phone := e.phone(relay, "ios-1", "rrk_old", "undernet")
	relay.setStatus(410)
	mark := relay.count()
	e.feed("undernet", ":x!x@h PRIVMSG #a :me one")
	relay.wait(mark, 1)
	deadline := time.Now().Add(5 * time.Second)
	for {
		v, _, _ := e.be.db.ModuleKVGet(context.Background(), "retecloud", "push/ios-1")
		if strings.Contains(v, `"rejected":true`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("registration not marked rejected: %s", v)
		}
		time.Sleep(20 * time.Millisecond)
	}
	relay.setStatus(200)
	mark = relay.count()
	e.feed("undernet", ":x!x@h PRIVMSG #a :me two")
	relay.none(mark, "message", "rejected key")

	key := relay.phoneKey("rrk_old")
	f := phone.call("push_register", "", map[string]any{"relay_url": relay.url(), "relay_key": "rrk_old", "public_key": key, "profile": "p"})
	if f["error"] != "Relay key rejected" || f["code"] != "relay_key_rejected" {
		t.Fatalf("same key: %v", f)
	}
	key = relay.phoneKey("rrk_new")
	if f := phone.call("push_register", "", map[string]any{"relay_url": relay.url(), "relay_key": "rrk_new", "public_key": key, "profile": "p"}); f["type"] != "push_register_success" {
		t.Fatalf("new key: %v", f)
	}
	e.feed("undernet", ":x!x@h PRIVMSG #a :me three")
	if got := relay.wait(mark, 1)[0]; got.relayKey != "rrk_new" || got.content["text"] != "me three" {
		t.Fatalf("after re-pairing: %+v", got)
	}
}

// A 503 stops pushes to that relay for an hour.
func TestRelayUnavailablePauses(t *testing.T) {
	e, relay, clock := newPushEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #a")
	e.phone(relay, "ios-1", "rrk_1", "undernet")
	relay.setStatus(503)
	mark := relay.count()
	e.feed("undernet", ":x!x@h PRIVMSG #a :me one")
	relay.wait(mark, 1)
	time.Sleep(100 * time.Millisecond)
	relay.setStatus(200)
	mark = relay.count()
	e.feed("undernet", ":x!x@h PRIVMSG #a :me two")
	relay.none(mark, "message", "paused")
	clock.advance(relayUnavailablePause + time.Second)
	e.feed("undernet", "@time="+clock.Now().UTC().Format("2006-01-02T15:04:05.000Z")+" :x!x@h PRIVMSG #a :me three")
	relay.wait(mark, 1)
}

// A registered device's networks are followed with no connection open: after the phone
// disconnects, after a brain reconnect, after a module reload, and when it toggles a network on.
func TestPushFollowsNetworksWithoutConnections(t *testing.T) {
	e, relay, clock := newPushEnv(t, "undernet", "libera")
	e.feed("undernet", ":me!u@h JOIN #a")
	e.feed("libera", ":me!u@h JOIN #l")
	phone := e.phone(relay, "ios-1", "rrk_1", "undernet")
	phone.ws.Close()
	time.Sleep(100 * time.Millisecond)

	mark := relay.count()
	e.feed("undernet", ":x!x@h PRIVMSG #a :me no connection")
	relay.wait(mark, 1)

	e.stopSrv() // brain reconnect
	time.Sleep(100 * time.Millisecond)
	e.startBrain()
	time.Sleep(500 * time.Millisecond)
	mark = relay.count()
	e.feed("undernet", ":x!x@h PRIVMSG #a :me after the brain came back")
	relay.wait(mark, 1)

	// A toggle turned on elsewhere (another module, or the phone on a connection that is gone).
	if err := e.be.db.SetDeviceNetwork(context.Background(), "ios-1", e.nets["libera"], true); err != nil {
		t.Fatal(err)
	}
	e.srv.Broadcast(wire.FeatureDevices, wire.EventDeviceNetwork, wire.DeviceNetworkEvent{ClientID: "ios-1", Network: e.nets["libera"], Subscribed: true})
	time.Sleep(300 * time.Millisecond)
	mark = relay.count()
	e.feed("libera", ":x!x@h PRIVMSG #l :me on libera")
	relay.wait(mark, 1)

	// Module reload: a new module process over the same brain and store.
	e.restartModule(pushSetup(relay, clock))
	time.Sleep(500 * time.Millisecond)
	mark = relay.count()
	e.feed("undernet", ":x!x@h PRIVMSG #a :me after the module reload")
	if got := relay.wait(mark, 1)[0]; got.content["text"] != "me after the module reload" || got.relayKey != "rrk_1" {
		t.Fatalf("after reload: %+v", got)
	}
}

// restartModule replaces the module with a new one over a new brain connection (a module
// reload: same store, nothing kept in memory).
func (e *testEnv) restartModule(setup func(*Module)) {
	e.t.Helper()
	e.cancel()
	select {
	case <-e.runErr:
	case <-time.After(5 * time.Second):
		e.t.Fatal("module did not stop")
	}
	if c, ok := e.mod.brain.(*moduleapi.Client); ok {
		c.Close()
	}
	brain, err := moduleapi.Dial(context.Background(), moduleapi.Options{Socket: e.sock, Module: "retecloud", Features: Features, SkipLock: true})
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { brain.Close() })
	m := New(e.mod.cfg, brain, nil)
	if setup != nil {
		setup(m)
	}
	ctx, cancel := context.WithCancel(context.Background())
	e.t.Cleanup(cancel)
	go func() { _ = m.Run(ctx) }()
	e.mod = m
}

func TestClearsReadElsewhere(t *testing.T) {
	old := clearWindow
	clearWindow = 400 * time.Millisecond
	t.Cleanup(func() { clearWindow = old }) // after the module stops (cleanups run last-in first-out)
	e, relay, clock := newPushEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #a", ":me!u@h JOIN #b")
	phone := e.phone(relay, "ios-1", "rrk_1", "undernet")
	sid := e.sid("undernet")
	desk := e.dial(false)
	desk.authenticate("desk")
	desk.liteSync(sid, "desk", nil)

	mark := relay.count()
	e.feed("undernet", ":x!x@h PRIVMSG #a :me one")
	pushed := relay.wait(mark, 1)[0]
	pushedID := int64(pushed.content["id"].(float64))
	e.feed("undernet", ":x!x@h PRIVMSG #b :not a highlight")

	// A buffer nothing was pushed in gets no clear.
	mark = relay.count()
	desk.call("read_cursor", sid, map[string]any{"buffer": "#b", "last_message_id": lastStoredID(t, e, "undernet", "#b")})
	relay.none(mark, "clear", "clear for a buffer never pushed")

	// The desk reads #a: one clear to the phone, a background push.
	desk.call("read_cursor", sid, map[string]any{"buffer": "#a", "last_message_id": pushedID})
	got := relay.wait(mark, 1)[0]
	c := got.content
	if got.req.Type != "clear" || c["type"] != "clear" || c["buffer"] != "#a" || c["read_id"] != float64(pushedID) ||
		c["session_id"] != sid || c["profile"] != "prof-ios-1" || c["eid"] != got.req.EventID {
		t.Fatalf("clear = %+v %v", got.req, c)
	}
	// Reading again with nothing new pushed: no clear.
	mark = relay.count()
	e.feed("undernet", ":x!x@h PRIVMSG #a :plain")
	desk.call("read_cursor", sid, map[string]any{"buffer": "#a", "last_message_id": lastStoredID(t, e, "undernet", "#a")})
	relay.none(mark, "clear", "nothing pushed since the last clear")

	// After the window, a clear goes at once; reads within the window wait for its end and are
	// sent once, with the newest cursor.
	clock.advance(readingElsewhereWindow + time.Second)
	var ids []int64
	for i := 2; i <= 4; i++ {
		m := relay.count()
		e.feed("undernet", fmt.Sprintf(":x!x@h PRIVMSG #a :me %d", i))
		ids = append(ids, int64(relay.waitType(m, "message", 1)[0].content["id"].(float64)))
	}
	mark = relay.count()
	desk.call("read_cursor", sid, map[string]any{"buffer": "#a", "last_message_id": ids[0]})
	if c := relay.wait(mark, 1)[0].content; c["read_id"] != float64(ids[0]) {
		t.Fatalf("clear after the window = %v", c)
	}
	desk.call("read_cursor", sid, map[string]any{"buffer": "#a", "last_message_id": ids[1]})
	desk.call("read_cursor", sid, map[string]any{"buffer": "#a", "last_message_id": ids[2]})
	time.Sleep(clearWindow / 4)
	if n := relay.count() - mark; n != 1 {
		t.Fatalf("%d clears inside the window", n)
	}
	got = relay.wait(mark, 2)[1]
	time.Sleep(clearWindow)
	if relay.count()-mark != 2 || got.content["read_id"] != float64(ids[2]) {
		t.Fatalf("coalesced clears: %v", relay.since(mark))
	}

	// The phone reading itself: no clear to it, and it counts as cleared (the desk reading the
	// same line later sends nothing either).
	clock.advance(readingElsewhereWindow + time.Second)
	mark = relay.count()
	e.feed("undernet", ":x!x@h PRIVMSG #a :me five")
	five := int64(relay.waitType(mark, "message", 1)[0].content["id"].(float64))
	clock.advance(time.Minute)
	phone.liteSync(sid, "ios-1", nil)
	mark = relay.count()
	phone.call("read_cursor", sid, map[string]any{"buffer": "#a", "last_message_id": five})
	relay.none(mark, "clear", "the phone's own read")
	desk.call("read_cursor", sid, map[string]any{"buffer": "#a", "last_message_id": five})
	relay.none(mark, "clear", "already cleared by the phone")
}

// A clear names the buffer by its folded key (as read_cursor broadcasts do), whatever spelling
// the message push carried.
func TestClearNamesTheFoldedBuffer(t *testing.T) {
	e, relay, _ := newPushEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #Rete[1]")
	e.phone(relay, "ios-1", "rrk_1", "undernet")
	sid := e.sid("undernet")
	desk := e.dial(false)
	desk.authenticate("desk")
	desk.liteSync(sid, "desk", nil)

	mark := relay.count()
	e.feed("undernet", ":x!x@h PRIVMSG #Rete[1] :me there")
	msg := relay.waitType(mark, "message", 1)[0]
	if msg.content["buffer"] != "#Rete[1]" {
		t.Fatalf("message buffer = %v", msg.content["buffer"])
	}
	desk.call("read_cursor", sid, map[string]any{"buffer": "#RETE{1}", "last_message_id": int64(msg.content["id"].(float64))})
	if c := relay.waitType(mark, "clear", 1)[0].content; c["buffer"] != "#rete{1}" {
		t.Fatalf("clear buffer = %v", c["buffer"])
	}
}
