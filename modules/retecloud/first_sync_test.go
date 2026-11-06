package retecloud

import (
	"context"
	"testing"
	"time"
)

// knownDevice records device as already seeded on network (it synced before), so its next lite
// sync is not a first sync and counts history from the read markers as they are.
func (e *testEnv) knownDevice(device, network string) {
	e.t.Helper()
	if err := e.be.db.MarkDeviceSeeded(context.Background(), device, e.nets[network]); err != nil {
		e.t.Fatal(err)
	}
}

// bootstrapBuffer returns the bootstrap's entry for name, or nil.
func bootstrapBuffer(d map[string]any, name string) map[string]any {
	for _, b := range d["buffers"].([]any) {
		if e := b.(map[string]any); e["name"] == name {
			return e
		}
	}
	return nil
}

// A new device's first lite sync: history nobody has read (no read marker) counts as read, on
// every device. The DM is not listed, the channel has no unread, the other connection hears
// read_cursor for both, and a line after the sync counts as usual.
func TestFirstSyncMarksNeverReadBuffersRead(t *testing.T) {
	e := newTestEnv(t, "undernet")
	other := e.dial(false)
	other.authenticate("desktop")
	other.liteSync(e.sid("undernet"), "desktop", nil) // synced before any line: it counts them all
	e.feed("undernet", ":me!u@h JOIN #rete", ":srv.example 353 me = #rete :@me peer", ":srv.example 366 me #rete :End",
		":peer!p@h PRIVMSG #rete :old channel line", ":dm!d@h PRIVMSG me :old private line")
	other.waitFor(0, 5*time.Second, func(f map[string]any) bool {
		return f["type"] == "open_buffer" && f["data"].(map[string]any)["buffer"] == "dm"
	})
	mark := other.count()

	c := e.dial(false)
	c.authenticate("ios-new")
	_, frames := c.liteSync(e.sid("undernet"), "ios-new", nil)
	d := successData(t, frames)
	if b := bootstrapBuffer(d, "dm"); b != nil {
		t.Fatalf("never-read query listed on the new device: %v", b)
	}
	ch := bootstrapBuffer(d, "#rete")
	if ch == nil || ch["unread"] != nil || ch["highlights"] != nil || ch["read_id"] == nil {
		t.Fatalf("#rete = %v, want no unread, no highlights, a read_id", ch)
	}
	for _, key := range []string{"#rete", "dm"} {
		other.waitFor(mark, 5*time.Second, func(f map[string]any) bool {
			if f["type"] != "read_cursor" {
				return false
			}
			data := f["data"].(map[string]any)
			return data["buffer"] == key && data["last_message_id"].(float64) > 0
		})
	}

	// After the first sync: a new DM counts and opens the query as today.
	// The syncing connection learns the cursors from its bootstrap, not from broadcasts.
	for _, f := range c.since(0) {
		if f["type"] == "read_cursor" {
			t.Fatalf("the syncing connection got %v", f)
		}
	}
	cm := c.count()
	e.feed("undernet", ":dm!d@h PRIVMSG me :new private line")
	c.waitFor(cm, 5*time.Second, func(f map[string]any) bool {
		return f["type"] == "open_buffer" && f["data"].(map[string]any)["buffer"] == "dm"
	})
	act := c.waitFor(cm, 5*time.Second, isType("activity"))
	entry := act["data"].(map[string]any)["buffers"].([]any)[0].(map[string]any)
	if entry["buffer"] != "dm" || entry["unread"] != float64(1) {
		t.Fatalf("activity = %v, want dm with 1 unread", act)
	}

	// A later sync of the device is not a first sync: the DM is listed with its 1 unread.
	_, frames = c.liteSync(e.sid("undernet"), "ios-new", nil)
	if b := bootstrapBuffer(successData(t, frames), "dm"); b == nil || b["unread"] != float64(1) {
		t.Fatalf("dm on the next sync = %v, want 1 unread", b)
	}
}

// A buffer that already has a read marker counts from it on a new device's first sync, and its
// query is listed there with the lines after it.
func TestFirstSyncKeepsExistingMarkers(t *testing.T) {
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #rete")
	other := e.dial(false)
	other.authenticate("desktop")
	other.liteSync(e.sid("undernet"), "desktop", nil)
	e.feed("undernet", ":dm!d@h PRIVMSG me :read line")
	rid := other.request("subscribe_buffer", map[string]any{"buffer": "dm", "history": 5}, e.sid("undernet"))
	sub := other.waitFor(0, 5*time.Second, reply(rid, "subscribe_buffer_success", "subscribe_buffer_error"))
	msgs := sub["data"].(map[string]any)["messages"].([]any)
	readID := msgs[len(msgs)-1].(map[string]any)["id"]
	rc := other.request("read_cursor", map[string]any{"buffer": "dm", "last_message_id": readID}, e.sid("undernet"))
	other.waitFor(0, 5*time.Second, reply(rc, "read_cursor_success", "read_cursor_error"))
	e.feed("undernet", ":dm!d@h PRIVMSG me :unread one", ":dm!d@h PRIVMSG me :unread two")

	c := e.dial(false)
	c.authenticate("ios-new")
	_, frames := c.liteSync(e.sid("undernet"), "ios-new", nil)
	b := bootstrapBuffer(successData(t, frames), "dm")
	if b == nil || b["unread"] != float64(2) || b["read_id"] != readID {
		t.Fatalf("dm = %v, want 2 unread counted from read_id %v", b, readID)
	}
}
