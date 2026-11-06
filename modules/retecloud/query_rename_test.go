package retecloud

import (
	"strings"
	"testing"
	"time"
)

// A query peer's NICK: rename_buffer to every connection of each device that had the query
// (ordered before the line), renamed_from in a device's next bootstrap only, and the NICK in the
// query's history under the new nick.

func isNickLine(f map[string]any) bool {
	if f["type"] != "irc_line" {
		return false
	}
	raw, _ := f["data"].(map[string]any)["raw"].(string)
	return strings.Contains(raw, " NICK ")
}

func TestPeerNickRenamesTheQueryLiveAndAtTheNextBootstrap(t *testing.T) {
	e := newTestEnv(t, "undernet")
	sid := e.sid("undernet")
	// Both devices synced before bob's line, which opens his query on each.
	live := e.syncedClient("ios-live", "undernet")
	away := e.dial(false)
	away.authenticate("ios-away")
	away.liteSync(sid, "ios-away", nil)
	e.feed("undernet", ":bob!b@h PRIVMSG me :hello") // gives bob a query
	for _, c := range []*wsClient{live, away} {
		c.waitFor(0, 5*time.Second, func(f map[string]any) bool {
			return f["type"] == "open_buffer" && f["data"].(map[string]any)["buffer"] == "bob"
		})
	}
	rid := live.request("subscribe_buffer", map[string]any{"buffer": "bob", "history": 0}, sid) // bob subscribed
	live.waitFor(0, 5*time.Second, reply(rid, "subscribe_buffer_success"))
	_, frames := away.liteSync(sid, "ios-away", nil)
	if _, ok := liteBuffers(t, successData(t, frames))["bob"]; !ok {
		t.Fatalf("ios-away should show bob: %v", frames)
	}
	away.ws.Close() // the phone goes away

	mark := live.count()
	e.feed("undernet", ":bob!b@h NICK :Robert")
	rn := live.waitFor(mark, 5*time.Second, isType("rename_buffer"))
	if d := rn["data"].(map[string]any); d["from"] != "bob" || d["to"] != "Robert" || rn["session_id"] != sid {
		t.Fatalf("rename_buffer = %v", rn)
	}
	nick := live.waitFor(mark, 5*time.Second, isNickLine)
	renameIdx, nickIdx := -1, -1
	for i, f := range live.since(mark) {
		if f["type"] == "rename_buffer" && renameIdx < 0 {
			renameIdx = i
		}
		if isNickLine(f) && nickIdx < 0 {
			nickIdx = i
		}
	}
	if renameIdx < 0 || nickIdx < 0 || renameIdx > nickIdx {
		t.Fatalf("rename_buffer at %d, NICK at %d: the rename must come first", renameIdx, nickIdx)
	}
	nd := nick["data"].(map[string]any)
	if nd["buffer"] != "Robert" || nd["id"] == nil {
		t.Fatalf("NICK line = %v, want it stored under the query's new name", nd)
	}

	// The query's history under the new nick holds the NICK.
	rid = live.request("get_messages", map[string]any{"buffer": "robert"}, sid)
	msgs := live.waitFor(mark, 5*time.Second, reply(rid, "get_messages_success"))["data"].(map[string]any)["messages"].([]any)
	if len(msgs) != 1 || !strings.Contains(msgs[0].(map[string]any)["event_data"].(map[string]any)["raw"].(string), "NICK :Robert") {
		t.Fatalf("robert's history = %v", msgs)
	}

	// The away phone's next bootstrap lists the query under its new name, once, with renamed_from.
	back := e.dial(false)
	back.authenticate("ios-away")
	_, frames = back.liteSync(sid, "ios-away", nil)
	bufs := liteBuffers(t, successData(t, frames))
	if b, ok := bufs["Robert"]; !ok || b["renamed_from"] != "bob" || b["kind"] != "query" {
		t.Fatalf("bootstrap buffers = %v, want Robert renamed_from bob", bufs)
	}
	if _, ok := bufs["bob"]; ok {
		t.Fatalf("the old name is still listed: %v", bufs)
	}
	_, frames = back.liteSync(sid, "ios-away", nil)
	if b := liteBuffers(t, successData(t, frames))["Robert"]; b == nil || b["renamed_from"] != nil {
		t.Fatalf("second bootstrap = %v, want Robert without renamed_from", b)
	}
}

func TestPeerNickWithoutQueryRenamesNothingOnLite(t *testing.T) {
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #c", ":bob!b@h JOIN #c")
	c := e.syncedClient("ios-1", "undernet", "#c")
	mark := c.count()
	e.feed("undernet", ":bob!b@h NICK :rob")
	c.waitFor(mark, 5*time.Second, isNickLine)
	time.Sleep(100 * time.Millisecond)
	for _, f := range c.since(mark) {
		if f["type"] == "rename_buffer" {
			t.Fatalf("rename_buffer without a query: %v", f)
		}
	}
}
