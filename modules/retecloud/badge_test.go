package retecloud

import (
	"testing"
	"time"
)

// Rete A3353 (owner 2026-11-22): every push carries, sealed, this bouncer's count for the phone's
// app icon: the unread highlights and private messages on the device's networks, as the lite
// bootstrap counts them. A query's every unread line counts, a channel its highlights; plain
// lines and a buffer whose Highlight switch is off count nothing. A clear carries the count after
// the read.
func TestPushCarriesTheBadge(t *testing.T) {
	old := clearWindow
	clearWindow = 400 * time.Millisecond
	t.Cleanup(func() { clearWindow = old })
	e, relay, clock := newPushEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #a", ":me!u@h JOIN #quiet")
	e.phone(relay, "ios-1", "rrk_1", "undernet")
	sid := e.sid("undernet")
	desk := e.dial(false)
	desk.authenticate("desk")
	desk.liteSync(sid, "desk", nil)
	if f := desk.call("set_buffer_highlight", sid, map[string]any{"buffer": "#quiet", "highlight": false}); f["type"] != "set_buffer_highlight_success" {
		t.Fatalf("set_buffer_highlight: %v", f)
	}
	clock.advance(readingElsewhereWindow + time.Second) // the desk's sync is not someone reading

	badge := func(c map[string]any) any { return c["badge"] }
	mark := relay.count()
	e.feedSpaced("undernet", ":x!x@h PRIVMSG #a :plain line")
	e.feedSpaced("undernet", ":x!x@h PRIVMSG #quiet :me: muted")
	e.feedSpaced("undernet", ":x!x@h PRIVMSG #a :me: one")
	first := relay.waitType(mark, "message", 1)[0]
	if b := badge(first.content); b != float64(1) {
		t.Fatalf("badge after one highlight = %v (%v)", b, first.content)
	}
	mark = relay.count()
	e.feedSpaced("undernet", ":bob!b@h PRIVMSG me :hi")
	e.feedSpaced("undernet", ":bob!b@h PRIVMSG me :there")
	got := relay.waitType(mark, "message", 2)
	if b := badge(got[1].content); b != float64(3) {
		t.Fatalf("badge after two DM lines = %v (%v)", b, got[1].content)
	}

	// The desk reads #a: the clear carries the count left (bob's two lines).
	mark = relay.count()
	desk.call("read_cursor", sid, map[string]any{"buffer": "#a", "last_message_id": lastStoredID(t, e, "undernet", "#a")})
	cl := relay.waitType(mark, "clear", 1)[0]
	if b := badge(cl.content); b != float64(2) {
		t.Fatalf("clear badge = %v (%v)", b, cl.content)
	}
}
