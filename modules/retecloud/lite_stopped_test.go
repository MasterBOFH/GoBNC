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
