package retecloud

import (
	"fmt"
	"testing"
	"time"
)

// subscribe_buffer { anchor_id }: a page centred on that row; anchor_missing falls back to the
// first-unread page.
func TestSubscribeAnchorID(t *testing.T) {
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #a", ":me!u@h JOIN #b")
	for i := 1; i <= 30; i++ {
		e.feed("undernet", fmt.Sprintf(":x!x@h PRIVMSG #a :line %d", i))
	}
	e.feed("undernet", ":x!x@h PRIVMSG #b :elsewhere")
	other := lastStoredID(t, e, "undernet", "#b")
	newestA := lastStoredID(t, e, "undernet", "#a")
	c := e.dial(false)
	c.authenticate("ios-1")
	sid := e.sid("undernet")
	c.liteSync(sid, "ios-1", nil)

	anchor := newestA - 15
	d := subscribeData(t, c, sid, map[string]any{"buffer": "#a", "history": 10, "anchor_id": anchor})
	ids := messageIDs(d)
	if len(ids) != 10 || d["anchor_id"] != float64(anchor) || d["has_more"] != true || d["has_more_after"] != true {
		t.Fatalf("page = %v (ids %v)", d, ids)
	}
	if _, missing := d["anchor_missing"]; missing {
		t.Fatalf("anchor_missing on a present row: %v", d)
	}
	pos := -1
	for i, id := range ids {
		if id == anchor {
			pos = i
		}
	}
	if pos < 3 || pos > 6 {
		t.Fatalf("row %d at position %d of %v: not centred", anchor, pos, ids)
	}

	// Another buffer's id with no read cursor: the newest page, no anchor_id.
	d = subscribeData(t, c, sid, map[string]any{"buffer": "#a", "history": 5, "anchor_id": other})
	ids = messageIDs(d)
	if d["anchor_missing"] != true || len(ids) != 5 || ids[4] != newestA {
		t.Fatalf("missing, no cursor: %v", d)
	}
	if _, ok := d["anchor_id"]; ok {
		t.Fatalf("anchor_id without a read cursor: %v", d)
	}

	// With a cursor: the first-unread page and its anchor.
	read := newestA - 20
	c.call("read_cursor", sid, map[string]any{"buffer": "#a", "last_message_id": read})
	d = subscribeData(t, c, sid, map[string]any{"buffer": "#a", "history": 6, "anchor_id": int64(1 << 40)})
	if d["anchor_missing"] != true || d["anchor_id"] != float64(read+1) {
		t.Fatalf("missing, with cursor: %v", d)
	}
	found := false
	for _, id := range messageIDs(d) {
		found = found || id == read+1
	}
	if !found {
		t.Fatalf("first unread %d not in %v", read+1, messageIDs(d))
	}

	// Not with after_id or anchor.
	for _, extra := range []map[string]any{{"after_id": 1}, {"anchor": "first_unread"}} {
		req := map[string]any{"buffer": "#a", "anchor_id": anchor}
		for k, v := range extra {
			req[k] = v
		}
		mark := c.count()
		rid := c.request("subscribe_buffer", req, sid)
		f := c.waitFor(mark, 5*time.Second, reply(rid, "subscribe_buffer_success", "subscribe_buffer_error"))
		if f["error"] != "anchor_id cannot be combined with after_id or anchor" {
			t.Fatalf("%v: %v", extra, f)
		}
	}
}
