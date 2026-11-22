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

// onLoop runs f on the module's loop and waits for it.
func onLoop(t *testing.T, m *Module, f func()) {
	t.Helper()
	done := make(chan struct{})
	if !m.post(func() { f(); close(done) }) {
		t.Fatal("module stopped")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("loop did not run")
	}
}

// A device on two networks: a push on one recounts that one and reuses the other's count, which
// a line or a read move on it, or badgeTTL, ends.
func TestPushBadgeReusesOtherNetworksCounts(t *testing.T) {
	old := badgeTTL
	badgeTTL = time.Hour
	t.Cleanup(func() { badgeTTL = old })
	e, relay, clock := newPushEnv(t, "undernet", "libera")
	e.feed("undernet", ":me!u@h JOIN #a")
	e.feed("libera", ":me!u@h JOIN #b")
	e.phone(relay, "ios-1", "rrk_1", "undernet", "libera")
	desk := e.dial(false)
	desk.authenticate("desk")
	desk.liteSync(e.sid("libera"), "desk", nil)
	clock.advance(readingElsewhereWindow + time.Second)

	mark := relay.count()
	e.feedSpaced("libera", ":x!x@h PRIVMSG #b :me: one")
	if b := relay.waitType(mark, "message", 1)[0].content["badge"]; b != float64(1) {
		t.Fatalf("libera push badge = %v", b)
	}
	// #b's Highlight switch goes off: no line, no read move, so libera's count is reused.
	if f := desk.call("set_buffer_highlight", e.sid("libera"), map[string]any{"buffer": "#b", "highlight": false}); f["type"] != "set_buffer_highlight_success" {
		t.Fatalf("set_buffer_highlight: %v", f)
	}
	clock.advance(readingElsewhereWindow + time.Second)
	mark = relay.count()
	e.feedSpaced("undernet", ":x!x@h PRIVMSG #a :me: two")
	if b := relay.waitType(mark, "message", 1)[0].content["badge"]; b != float64(2) {
		t.Fatalf("undernet push badge = %v, want 2 (libera's 1 reused)", b)
	}
	// After badgeTTL libera is counted again, with its switch off.
	badgeTTL = time.Minute
	clock.advance(2 * time.Minute)
	mark = relay.count()
	e.feedSpaced("undernet", ":y!y@h PRIVMSG #a :me: three")
	if b := relay.waitType(mark, "message", 1)[0].content["badge"]; b != float64(2) {
		t.Fatalf("after the TTL badge = %v, want 2 (undernet's two, libera's switch off)", b)
	}
}

// A toggle naming a network the bouncer no longer has is skipped (and logged once); the rest
// still count.
func TestBadgeSkipsAGoneNetwork(t *testing.T) {
	e, relay, _ := newPushEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #a")
	e.phone(relay, "ios-1", "rrk_1", "undernet")
	var first, second *int
	onLoop(t, e.mod, func() {
		d := e.mod.push.devices["ios-1"]
		d.networks[999] = true
		first = e.mod.deviceBadge("ios-1", d)
		second = e.mod.deviceBadge("ios-1", d)
		if !e.mod.push.badgeGone[999] {
			t.Error("gone network not recorded")
		}
	})
	if first == nil || second == nil || *first != 0 || *second != 0 {
		t.Fatalf("badges %v %v", first, second)
	}
}
