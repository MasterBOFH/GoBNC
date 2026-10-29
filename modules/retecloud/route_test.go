package retecloud

import (
	"strings"
	"testing"
	"time"
)

// syncedClient authenticates as device and syncs network, subscribing to subs.
func (e *testEnv) syncedClient(device, network string, subs ...string) *wsClient {
	e.t.Helper()
	c := e.dial(false)
	c.authenticate(device)
	var list []any
	for _, b := range subs {
		list = append(list, map[string]any{"buffer": b, "history": 0})
	}
	c.liteSync(e.sid(network), device, map[string]any{"subscribe": list})
	for range subs {
		c.waitFor(0, 5*time.Second, isType("subscribe_buffer_success"))
	}
	return c
}

func linesWith(c *wsClient, from int, text string) int {
	n := 0
	for _, f := range c.since(from) {
		if f["type"] == "irc_line" && strings.Contains(f["data"].(map[string]any)["raw"].(string), text) {
			n++
		}
	}
	return n
}

func TestSubscribedVsUnsubscribedBuffer(t *testing.T) {
	old := liteActivityFlushDelay
	liteActivityFlushDelay = 100 * time.Millisecond
	defer func() { liteActivityFlushDelay = old }()
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #a", ":me!u@h JOIN #b")
	c := e.syncedClient("ios-1", "undernet", "#a")
	mark := c.count()
	e.feed("undernet", ":peer!p@h PRIVMSG #b :in-unsubscribed", ":peer!p@h PRIVMSG #a :in-subscribed")
	c.waitFor(mark, 5*time.Second, func(f map[string]any) bool {
		return f["type"] == "irc_line" && strings.Contains(f["data"].(map[string]any)["raw"].(string), "in-subscribed")
	})
	act := c.waitFor(mark, 5*time.Second, isType("activity"))
	entry := act["data"].(map[string]any)["buffers"].([]any)[0].(map[string]any)
	if entry["buffer"] != "#b" || entry["unread"] != float64(1) || entry["last_id"].(float64) <= 0 {
		t.Fatalf("activity = %v", act)
	}
	if linesWith(c, mark, "in-unsubscribed") != 0 {
		t.Fatal("line of an unsubscribed buffer delivered")
	}
}

func TestNickReachesOnlySharedSubscribers(t *testing.T) {
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #a", ":me!u@h JOIN #b", ":peer!p@h JOIN #a")
	a := e.syncedClient("ios-a", "undernet", "#a")
	b := e.syncedClient("ios-b", "undernet", "#b")
	ma, mb := a.count(), b.count()
	e.feed("undernet", ":peer!p@h NICK :peer2", ":me!u@h NICK :me2")
	a.waitFor(ma, 5*time.Second, func(f map[string]any) bool {
		return f["type"] == "irc_line" && strings.Contains(f["data"].(map[string]any)["raw"].(string), "NICK :me2")
	})
	b.waitFor(mb, 5*time.Second, func(f map[string]any) bool {
		return f["type"] == "irc_line" && strings.Contains(f["data"].(map[string]any)["raw"].(string), "NICK :me2")
	})
	if linesWith(a, ma, "NICK :peer2") != 1 {
		t.Fatal("subscriber of the shared channel missed the NICK")
	}
	if linesWith(b, mb, "NICK :peer2") != 0 {
		t.Fatal("NICK reached a connection sharing no subscribed buffer")
	}
}

func TestHighlightFlushesActivityImmediately(t *testing.T) {
	old := liteActivityFlushDelay
	liteActivityFlushDelay = time.Hour
	defer func() { liteActivityFlushDelay = old }()
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #a", ":me!u@h JOIN #b")
	c := e.syncedClient("ios-1", "undernet", "#a")
	mark := c.count()
	e.feed("undernet", ":peer!p@h PRIVMSG #b :plain chatter")
	time.Sleep(200 * time.Millisecond)
	for _, f := range c.since(mark) {
		if f["type"] == "activity" {
			t.Fatalf("plain line flushed before the delay: %v", f)
		}
	}
	e.feed("undernet", ":peer!p@h PRIVMSG #b :me: look here")
	act := c.waitFor(mark, 2*time.Second, isType("activity"))
	entry := act["data"].(map[string]any)["buffers"].([]any)[0].(map[string]any)
	if entry["unread"] != float64(2) || entry["highlights"] != float64(1) {
		t.Fatalf("activity = %v", act)
	}
}
