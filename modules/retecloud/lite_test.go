package retecloud

import (
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// liteSync sends sync_session lite and returns its frames up to sync_end (and the request id).
func (c *wsClient) liteSync(sid, clientID string, extra map[string]any) (string, []map[string]any) {
	c.t.Helper()
	data := map[string]any{"client_id": clientID, "sync_mode": "lite"}
	for k, v := range extra {
		data[k] = v
	}
	mark := c.count()
	rid := c.request("sync_session", data, sid)
	end := c.waitFor(mark, 5*time.Second, func(f map[string]any) bool {
		return f["request_id"] == rid && (f["type"] == "sync_end" || f["type"] == "sync_session_error")
	})
	if end["type"] != "sync_end" {
		c.t.Fatalf("sync_session: %v", end)
	}
	var out []map[string]any
	for _, f := range c.since(mark) {
		if f["request_id"] == rid {
			out = append(out, f)
		}
		if f["request_id"] == rid && f["type"] == "sync_end" {
			break
		}
	}
	return rid, out
}

func successData(t *testing.T, frames []map[string]any) map[string]any {
	t.Helper()
	for _, f := range frames {
		if f["type"] == "sync_session_success" {
			return f["data"].(map[string]any)
		}
	}
	t.Fatalf("no sync_session_success in %v", frames)
	return nil
}

func TestLiteBootstrapShape(t *testing.T) {
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":me!u@cloak JOIN #rete", ":srv.example 353 me = #rete :@me peer", ":srv.example 366 me #rete :End")
	c := e.dial(false)
	c.authenticate("ios-1")
	_, frames := c.liteSync(e.sid("undernet"), "ios-1", nil)
	types := []string{}
	for _, f := range frames {
		types = append(types, f["type"].(string))
	}
	if len(types) != 3 || types[0] != "sync_start" || types[1] != "sync_session_success" || types[2] != "sync_end" {
		t.Fatalf("frame types = %v", types)
	}
	start := frames[0]["data"].(map[string]any)
	if start["sync_mode"] != "lite" || start["total_messages"] != float64(0) || start["total_bytes"].(float64) <= 0 {
		t.Fatalf("sync_start = %v", start)
	}
	d := frames[1]["data"].(map[string]any)
	for _, k := range []string{"client_id", "connected", "registered", "capabilities", "capabilities_hash", "isupport",
		"isupport_hash", "identity", "identity_hash", "buffers", "as_of_id"} {
		if _, ok := d[k]; !ok {
			t.Errorf("bootstrap lacks %q: %v", k, d)
		}
	}
	if d["client_id"] != "ios-1" || d["connected"] != true || d["registered"] != true {
		t.Fatalf("bootstrap = %v", d)
	}
	id := d["identity"].(map[string]any)
	if id["nick"] != "me" || id["username"] != "u" || id["hostname"] != "cloak" {
		t.Fatalf("identity = %v", id)
	}
	bufs := d["buffers"].([]any)
	if len(bufs) != 1 {
		t.Fatalf("buffers = %v", bufs)
	}
	b := bufs[0].(map[string]any)
	if b["name"] != "#rete" || b["kind"] != "channel" || b["joined"] != true {
		t.Fatalf("buffer = %v", b)
	}
	if d["isupport"].(map[string]any)["CHANTYPES"] != "#" {
		t.Fatalf("isupport = %v", d["isupport"])
	}
}

func TestLiteBootstrapCacheOmitsUnchangedSections(t *testing.T) {
	e := newTestEnv(t, "undernet")
	c := e.dial(false)
	c.authenticate("ios-1")
	_, frames := c.liteSync(e.sid("undernet"), "ios-1", nil)
	d := successData(t, frames)
	cache := map[string]any{"isupport": d["isupport_hash"], "identity": d["identity_hash"], "capabilities": "stale"}
	_, frames = c.liteSync(e.sid("undernet"), "ios-1", map[string]any{"cache": cache})
	d2 := successData(t, frames)
	if _, ok := d2["isupport"]; ok {
		t.Fatal("isupport sent although its hash matched")
	}
	if _, ok := d2["identity"]; ok {
		t.Fatal("identity sent although its hash matched")
	}
	if _, ok := d2["capabilities"]; !ok {
		t.Fatal("capabilities left out although the cached hash was stale")
	}
	if d2["isupport_hash"] != d["isupport_hash"] {
		t.Fatal("hash missing or changed")
	}
}

func TestLiteSyncStoresToggleAndUnsyncStopsFrames(t *testing.T) {
	e := newTestEnv(t, "undernet")
	c := e.dial(false)
	c.authenticate("ios-1")
	c.liteSync(e.sid("undernet"), "ios-1", nil)
	if s := e.dial(false).authenticate("ios-1")["sessions"].([]any)[0].(map[string]any); s["subscribed"] != true {
		t.Fatalf("toggle after sync = %v", s)
	}
	mark := c.count()
	rid := c.request("unsync_session", map[string]any{"client_id": "ios-1"}, e.sid("undernet"))
	c.waitFor(mark, 5*time.Second, reply(rid, "unsync_session_success"))
	if s := e.dial(false).authenticate("ios-1")["sessions"].([]any)[0].(map[string]any); s["subscribed"] != false {
		t.Fatalf("toggle after unsync = %v", s)
	}
	mark = c.count()
	e.session("undernet").HandleDisconnect(nil) // a state change on the unsynced network
	time.Sleep(300 * time.Millisecond)
	for _, f := range c.since(mark) {
		if f["session_id"] == e.sid("undernet") {
			t.Fatalf("frame for the unsynced session: %v", f)
		}
	}
}

func TestLiteSyncFollowerGetsStateFrame(t *testing.T) {
	e := newTestEnv(t, "undernet")
	c := e.dial(false)
	c.authenticate("ios-1")
	c.liteSync(e.sid("undernet"), "ios-1", nil)
	mark := c.count()
	e.session("undernet").HandleDisconnect(nil)
	f := c.waitFor(mark, 5*time.Second, isType("state"))
	if d := f["data"].(map[string]any); d["connected"] != false || d["registered"] != false || f["session_id"] != e.sid("undernet") {
		t.Fatalf("state = %v", f)
	}
}

func TestPipelinedSyncsAnswerInOrder(t *testing.T) {
	e := newTestEnv(t, "undernet", "libera")
	c := e.dial(false)
	c.authenticate("ios-1")
	mark := c.count()
	r1 := c.request("sync_session", map[string]any{"client_id": "ios-1", "sync_mode": "lite"}, e.sid("undernet"))
	r2 := c.request("sync_session", map[string]any{"client_id": "ios-1", "sync_mode": "lite"}, e.sid("libera"))
	c.waitFor(mark, 5*time.Second, reply(r2, "sync_end"))
	var order []string
	for _, f := range c.since(mark) {
		if f["type"] == "sync_end" {
			order = append(order, f["request_id"].(string))
		}
	}
	if len(order) != 2 || order[0] != r1 || order[1] != r2 {
		t.Fatalf("sync_end order = %v, want [%s %s]", order, r1, r2)
	}
}

func TestOnlyLiteModeAndSessionCreated(t *testing.T) {
	e := newTestEnv(t, "undernet")
	c := e.dial(false)
	c.authenticate("ios-1")
	rid := c.request("sync_session", map[string]any{"client_id": "ios-1", "sync_mode": "full"}, e.sid("undernet"))
	if f := c.waitFor(0, 5*time.Second, reply(rid, "sync_session_error")); f["error"] != "Only lite mode is supported" {
		t.Fatalf("full mode: %v", f)
	}
	mark := c.count()
	id := e.addNetwork("oftc")
	e.srv.NetworkChanged(wire.NetworkEvent{Network: id, Name: "oftc", State: wire.NetConnecting, Change: wire.ChangeAdded})
	f := c.waitFor(mark, 5*time.Second, isType("session_created"))
	if f["session_id"] != sessionID(id) || f["data"].(map[string]any)["name"] != "oftc" || f["data"].(map[string]any)["state"] != "connecting" {
		t.Fatalf("session_created = %v", f)
	}
	mark = c.count()
	e.srv.NetworkChanged(wire.NetworkEvent{Network: id, Name: "oftc", Change: wire.ChangeRemoved})
	if f := c.waitFor(mark, 5*time.Second, isType("session_deleted")); f["session_id"] != sessionID(id) {
		t.Fatalf("session_deleted = %v", f)
	}
}
