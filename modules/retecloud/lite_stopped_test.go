package retecloud

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/history"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// stopNetwork does what internal/server's StopNetwork does for a module: the network's Session
// goes away (network-scoped brain calls answer not_found) and a stopped state change is sent.
func (e *testEnv) stopNetwork(name string) {
	e.t.Helper()
	id := e.nets[name]
	e.be.mu.Lock()
	delete(e.be.sessions, id)
	srv := e.srv
	e.be.mu.Unlock()
	srv.NetworkChanged(wire.NetworkEvent{Network: id, Name: name, State: wire.NetStopped, Nick: "me", Change: wire.ChangeState})
}

// A lite sync_session for a network disconnect_session stopped bootstraps it as disconnected
// (connected/registered false, buffers from the device's open buffers, no isupport, identity or
// capabilities), rather than answering "Bouncer is reloading".
func TestLiteSyncStoppedNetworkBootstrapsDisconnected(t *testing.T) {
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #c")
	sid := e.sid("undernet")
	c := e.dial(false)
	c.authenticate("ios-1")
	c.liteSync(sid, "ios-1", nil)
	mark := c.count()
	rid := c.request("open_buffer", map[string]any{"buffer": "friend"}, sid)
	c.waitFor(mark, 5*time.Second, reply(rid, "open_buffer_success"))

	mark = c.count()
	e.stopNetwork("undernet")
	c.waitFor(mark, 5*time.Second, func(f map[string]any) bool {
		d, _ := f["data"].(map[string]any)
		return f["type"] == "session_state" && d["state"] == "disconnected"
	})

	_, frames := c.liteSync(sid, "ios-1", nil) // fails the test on sync_session_error
	d := successData(t, frames)
	if d["connected"] != false || d["registered"] != false || d["client_id"] != "ios-1" {
		t.Fatalf("bootstrap = %v, want connected and registered false", d)
	}
	for _, k := range []string{"capabilities", "isupport", "identity"} {
		if _, ok := d[k]; ok {
			t.Errorf("a disconnected bootstrap carries %q: %v", k, d)
		}
	}
	var names []string
	for _, b := range d["buffers"].([]any) {
		bb := b.(map[string]any)
		names = append(names, bb["name"].(string))
		if bb["joined"] == true {
			t.Errorf("buffer %v joined on a disconnected session", bb)
		}
	}
	if len(names) != 1 || names[0] != "friend" {
		t.Fatalf("buffers = %v, want the open query only", names)
	}

	// connect_session again: the brain starts a new Session; the connection follows it live.
	n, _ := e.be.db.NetworkByName(context.Background(), "undernet")
	sess := session.New(n, e.be.db, history.New(e.be.db), nil, nil)
	e.be.mu.Lock()
	e.be.sessions[n.ID] = sess
	e.be.mu.Unlock()
	e.feed("undernet", ":srv.example 001 me :Welcome", ":srv.example 376 me :End")
	mark = c.count()
	e.srv.NetworkChanged(wire.NetworkEvent{Network: n.ID, Name: "undernet", State: wire.NetRegistered, Nick: "me", Change: wire.ChangeAdded})
	deadline := time.Now().Add(5 * time.Second)
	for sess.Subscribers() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the restarted network was never followed again")
		}
		time.Sleep(5 * time.Millisecond)
	}
	e.feed("undernet", ":srv.example NOTICE me :back")
	c.waitFor(mark, 5*time.Second, func(f map[string]any) bool {
		d, _ := f["data"].(map[string]any)
		raw, _ := d["raw"].(string)
		return f["type"] == "irc_line" && strings.HasSuffix(raw, ":back")
	})
}

// liteBuffers indexes a bootstrap's buffers by name.
func liteBuffers(t *testing.T, d map[string]any) map[string]map[string]any {
	t.Helper()
	out := map[string]map[string]any{}
	for _, b := range d["buffers"].([]any) {
		bb := b.(map[string]any)
		out[bb["name"].(string)] = bb
	}
	return out
}

// A stopped network's lite bootstrap carries the unread and highlight counts and read ids
// counted from the store, so the device still shows its badges: the network's channels (its
// stored channel list) and its open queries.
func TestLiteSyncStoppedNetworkCarriesUnreadCounts(t *testing.T) {
	e := newTestEnv(t, "undernet")
	if err := e.be.db.AddChannel(context.Background(), e.nets["undernet"], "#c", ""); err != nil {
		t.Fatal(err)
	}
	e.feed("undernet", ":me!u@h JOIN #c")
	sid := e.sid("undernet")
	c := e.dial(false)
	c.authenticate("ios-1")
	e.knownDevice("ios-1", "undernet") // not a first sync: the backdated lines below stay unread
	c.liteSync(sid, "ios-1", nil)
	mark := c.count()
	rid := c.request("open_buffer", map[string]any{"buffer": "friend"}, sid)
	c.waitFor(mark, 5*time.Second, reply(rid, "open_buffer_success"))
	// Stored history a second apart (the read marker is a time): hello read, the rest not.
	t0 := time.Now().Add(-time.Hour).UTC()
	var ids []int64
	for i, l := range []struct{ target, src, text string }{
		{"#c", "peer!p@h", "hello"},
		{"#c", "peer!p@h", "me: ping"},
		{"friend", "friend!f@h", "psst"},
	} {
		id, err := e.be.db.InsertMessage(context.Background(), store.Message{NetworkID: e.nets["undernet"], Target: l.target,
			Time: t0.Add(time.Duration(i) * time.Second), Command: "PRIVMSG", Source: l.src, Raw: "x", Text: l.text})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if _, _, err := e.be.db.SetReadMarkerIfNewer(context.Background(), e.nets["undernet"], "#c", t0.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}

	mark = c.count()
	e.stopNetwork("undernet")
	c.waitFor(mark, 5*time.Second, func(f map[string]any) bool {
		d, _ := f["data"].(map[string]any)
		return f["type"] == "session_state" && d["state"] == "disconnected"
	})
	_, frames := c.liteSync(sid, "ios-1", nil)
	d := successData(t, frames)
	if d["connected"] != false {
		t.Fatalf("bootstrap = %v, want disconnected", d)
	}
	bufs := liteBuffers(t, d)
	ch, q := bufs["#c"], bufs["friend"]
	if ch == nil || ch["kind"] != "channel" || ch["joined"] == true || ch["unread"] != float64(1) || ch["highlights"] != float64(1) ||
		ch["read_id"] != float64(ids[0]) {
		t.Errorf("#c = %v, want 1 unread, 1 highlight, read_id %d, not joined", ch, ids[0])
	}
	if q == nil || q["kind"] != "query" || q["unread"] != float64(1) || q["highlights"] != float64(1) {
		t.Errorf("friend = %v, want 1 unread, 1 highlight", q)
	}
	if asOf, _ := d["as_of_id"].(float64); int64(asOf) < ids[2] {
		t.Errorf("as_of_id = %v, want at least %d (the newest line counted)", d["as_of_id"], ids[2])
	}
}

// subscribeData subscribes c to buffer on sid and returns subscribe_buffer_success's data,
// failing the test on subscribe_buffer_error.
func subscribeData(t *testing.T, c *wsClient, sid string, req map[string]any) map[string]any {
	t.Helper()
	mark := c.count()
	rid := c.request("subscribe_buffer", req, sid)
	f := c.waitFor(mark, 5*time.Second, reply(rid, "subscribe_buffer_success", "subscribe_buffer_error"))
	if f["type"] != "subscribe_buffer_success" {
		t.Fatalf("subscribe_buffer %v: %v", req, f)
	}
	return f["data"].(map[string]any)
}

func messageIDs(d map[string]any) []int64 {
	var out []int64
	for _, m := range d["messages"].([]any) {
		out = append(out, int64(m.(map[string]any)["id"].(float64)))
	}
	return out
}

// subscribe_buffer and get_messages on a stopped network answer from the stored history, with
// the spec's state for a session that is not connected (a channel not joined, a query, Server).
// Live lines arrive again once the network restarts.
func TestLiteSubscribeStoppedNetworkServesStoredHistory(t *testing.T) {
	e := newTestEnv(t, "undernet")
	nid := e.nets["undernet"]
	e.feed("undernet", ":me!u@h JOIN #c")
	sid := e.sid("undernet")
	c := e.dial(false)
	c.authenticate("ios-1")
	c.liteSync(sid, "ios-1", nil)
	mark := c.count()
	rid := c.request("open_buffer", map[string]any{"buffer": "friend"}, sid)
	c.waitFor(mark, 5*time.Second, reply(rid, "open_buffer_success"))
	t0 := time.Now().Add(-time.Hour).UTC()
	var ids []int64
	for i, l := range []struct{ target, raw string }{
		{"#c", ":peer!p@h PRIVMSG #c :one"},
		{"#c", ":peer!p@h PRIVMSG #c :two"},
		{"#c", ":peer!p@h PRIVMSG #c :three"},
		{"friend", ":friend!f@h PRIVMSG me :psst"},
	} {
		id, err := e.be.db.InsertMessage(context.Background(), store.Message{NetworkID: nid, Target: l.target,
			Time: t0.Add(time.Duration(i) * time.Second), Command: "PRIVMSG", Source: "peer!p@h", Raw: l.raw, Text: "x"})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}

	mark = c.count()
	e.stopNetwork("undernet")
	c.waitFor(mark, 5*time.Second, func(f map[string]any) bool {
		d, _ := f["data"].(map[string]any)
		return f["type"] == "session_state" && d["state"] == "disconnected"
	})
	// An inline subscribe in the stopped bootstrap gets its history too.
	mark = c.count()
	c.liteSync(sid, "ios-1", map[string]any{"subscribe": []any{map[string]any{"buffer": "friend", "history": 5}}})
	f := c.waitFor(mark, 5*time.Second, isType("subscribe_buffer_success"))
	d := f["data"].(map[string]any)
	if st := d["state"].(map[string]any); st["buffer"] != "friend" || st["kind"] != "query" || len(st) != 2 {
		t.Errorf("friend state = %v, want {buffer, kind: query}", st)
	}
	if got := messageIDs(d); len(got) != 1 || got[0] != ids[3] {
		t.Errorf("friend history = %v, want [%d]", got, ids[3])
	}

	d = subscribeData(t, c, sid, map[string]any{"buffer": "#c", "history": 2})
	if st := d["state"].(map[string]any); st["buffer"] != "#c" || st["kind"] != "channel" || st["joined"] != false || len(st) != 3 {
		t.Errorf("#c state = %v, want {buffer, kind: channel, joined: false}", st)
	}
	if got := messageIDs(d); len(got) != 2 || got[0] != ids[1] || got[1] != ids[2] || d["has_more"] != true {
		t.Errorf("#c history = %v has_more %v, want [%d %d] and more", got, d["has_more"], ids[1], ids[2])
	}
	d = subscribeData(t, c, sid, map[string]any{"buffer": "Server", "history": 0})
	if st := d["state"].(map[string]any); st["kind"] != "server" {
		t.Errorf("Server state = %v", st)
	}

	mark = c.count()
	rid = c.request("get_messages", map[string]any{"buffer": "#c", "limit": 2, "before_id": ids[2]}, sid)
	f = c.waitFor(mark, 5*time.Second, reply(rid, "get_messages_success", "get_messages_error"))
	if f["type"] != "get_messages_success" {
		t.Fatalf("get_messages: %v", f)
	}
	if got := messageIDs(f["data"].(map[string]any)); len(got) != 2 || got[1] != ids[1] {
		t.Errorf("get_messages = %v, want the two lines before %d", got, ids[2])
	}

	// connect_session again: the subscription carries on live.
	n, _ := e.be.db.NetworkByName(context.Background(), "undernet")
	sess := session.New(n, e.be.db, history.New(e.be.db), nil, nil)
	e.be.mu.Lock()
	e.be.sessions[n.ID] = sess
	e.be.mu.Unlock()
	e.feed("undernet", ":srv.example 001 me :Welcome", ":srv.example 376 me :End", ":me!u@h JOIN #c")
	mark = c.count()
	e.srv.NetworkChanged(wire.NetworkEvent{Network: n.ID, Name: "undernet", State: wire.NetRegistered, Nick: "me", Change: wire.ChangeAdded})
	deadline := time.Now().Add(5 * time.Second)
	for sess.Subscribers() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the restarted network was never followed again")
		}
		time.Sleep(5 * time.Millisecond)
	}
	e.feed("undernet", ":peer!p@h PRIVMSG #c :back")
	c.waitFor(mark, 5*time.Second, func(f map[string]any) bool {
		d, _ := f["data"].(map[string]any)
		raw, _ := d["raw"].(string)
		return f["type"] == "irc_line" && strings.HasSuffix(raw, "PRIVMSG #c :back")
	})
}
