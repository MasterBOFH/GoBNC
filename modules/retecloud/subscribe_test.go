package retecloud

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// textOf extracts "n-<i>" from a raw PRIVMSG line.
func textOf(raw string) string {
	if i := strings.LastIndex(raw, " :"); i >= 0 {
		return raw[i+2:]
	}
	return ""
}

func historyIDs(t *testing.T, msgs []any) map[float64]string {
	t.Helper()
	out := map[float64]string{}
	for _, m := range msgs {
		mm := m.(map[string]any)
		var ed struct{ Raw string }
		b, _ := json.Marshal(mm["event_data"])
		_ = json.Unmarshal(b, &ed)
		out[mm["id"].(float64)] = textOf(ed.Raw)
	}
	return out
}

func TestSubscribeUnderLineStreamNoDuplicateNoGap(t *testing.T) {
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #c")
	c := e.dial(false)
	c.authenticate("ios-1")
	c.liteSync(e.sid("undernet"), "ios-1", nil)

	const total = 300
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; i <= total; i++ {
			e.feed("undernet", fmt.Sprintf(":peer!p@h PRIVMSG #c :n-%d", i))
			if i == total/3 {
				time.Sleep(time.Millisecond)
			}
		}
	}()
	time.Sleep(2 * time.Millisecond)
	mark := c.count()
	rid := c.request("subscribe_buffer", map[string]any{"buffer": "#c", "history": 500}, e.sid("undernet"))
	sub := c.waitFor(mark, 5*time.Second, reply(rid, "subscribe_buffer_success", "subscribe_buffer_error"))
	if sub["type"] != "subscribe_buffer_success" {
		t.Fatalf("subscribe: %v", sub)
	}
	wg.Wait()

	seen := map[string]int{}
	for _, txt := range historyIDs(t, sub["data"].(map[string]any)["messages"].([]any)) {
		if strings.HasPrefix(txt, "n-") {
			seen[txt]++
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		live := map[string]int{}
		for _, f := range c.since(mark) {
			if f["type"] == "irc_line" {
				live[textOf(f["data"].(map[string]any)["raw"].(string))]++
			}
		}
		all := map[string]int{}
		for k, v := range seen {
			all[k] += v
		}
		for k, v := range live {
			all[k] += v
		}
		complete := true
		for i := 1; i <= total; i++ {
			k := fmt.Sprintf("n-%d", i)
			if all[k] > 1 {
				t.Fatalf("%s delivered %d times (history %d, live %d)", k, all[k], seen[k], live[k])
			}
			if all[k] == 0 {
				complete = false
			}
		}
		if complete {
			return
		}
		if time.Now().After(deadline) {
			for i := 1; i <= total; i++ {
				if k := fmt.Sprintf("n-%d", i); all[k] == 0 {
					t.Fatalf("%s never delivered (history has %d lines, live %d)", k, len(seen), len(live))
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestSubscribeAfterIDResume(t *testing.T) {
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #c")
	for i := 1; i <= 10; i++ {
		e.feed("undernet", fmt.Sprintf(":peer!p@h PRIVMSG #c :n-%d", i))
	}
	c := e.dial(false)
	c.authenticate("ios-1")
	c.liteSync(e.sid("undernet"), "ios-1", nil)
	rid := c.request("subscribe_buffer", map[string]any{"buffer": "#c", "history": 50}, e.sid("undernet"))
	first := c.waitFor(0, 5*time.Second, reply(rid, "subscribe_buffer_success"))
	ids := historyIDs(t, first["data"].(map[string]any)["messages"].([]any))
	var at float64 // id of n-7
	for id, txt := range ids {
		if txt == "n-7" {
			at = id
		}
	}

	mark := c.count()
	rid = c.request("subscribe_buffer", map[string]any{"buffer": "#c", "history": 50, "after_id": at}, e.sid("undernet"))
	d := c.waitFor(mark, 5*time.Second, reply(rid, "subscribe_buffer_success"))["data"].(map[string]any)
	got := historyIDs(t, d["messages"].([]any))
	if len(got) != 3 || d["gap"] != false {
		t.Fatalf("after n-7: %v gap=%v, want n-8..n-10 without gap", got, d["gap"])
	}
	for id := range got {
		if id <= at {
			t.Fatalf("id %v not after %v", id, at)
		}
	}

	mark = c.count()
	rid = c.request("subscribe_buffer", map[string]any{"buffer": "#c", "history": 2, "after_id": at - 3}, e.sid("undernet"))
	d = c.waitFor(mark, 5*time.Second, reply(rid, "subscribe_buffer_success"))["data"].(map[string]any)
	if got := historyIDs(t, d["messages"].([]any)); len(got) != 2 || d["gap"] != true {
		t.Fatalf("more missed than fits: %v gap=%v, want 2 lines and gap", got, d["gap"])
	}
}

func TestUnsubscribeStopsLiveLines(t *testing.T) {
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #c")
	c := e.dial(false)
	c.authenticate("ios-1")
	c.liteSync(e.sid("undernet"), "ios-1", map[string]any{"subscribe": []any{map[string]any{"buffer": "#c", "history": 0}}})
	c.waitFor(0, 5*time.Second, isType("subscribe_buffer_success"))
	mark := c.count()
	e.feed("undernet", ":peer!p@h PRIVMSG #c :live")
	c.waitFor(mark, 5*time.Second, isType("irc_line"))
	rid := c.request("unsubscribe_buffer", map[string]any{"buffer": "#C"}, e.sid("undernet"))
	c.waitFor(mark, 5*time.Second, reply(rid, "unsubscribe_buffer_success"))
	mark = c.count()
	e.feed("undernet", ":peer!p@h PRIVMSG #c :after unsubscribe")
	time.Sleep(300 * time.Millisecond)
	for _, f := range c.since(mark) {
		if f["type"] == "irc_line" {
			t.Fatalf("line after unsubscribe: %v", f)
		}
	}
}

// Every subscribe_buffer_error, standalone or answering an inline sync_session subscribe,
// carries the session_id and data.buffer as the client sent them.
func TestSubscribeBufferErrorsCarrySessionAndBuffer(t *testing.T) {
	e := newTestEnv(t, "undernet", "libera")
	synced, unsynced := e.sid("undernet"), e.sid("libera")
	c := e.dial(false)
	c.authenticate("ios-1")
	c.liteSync(synced, "ios-1", nil)
	check := func(name string, f map[string]any, sid, buffer, text string) {
		t.Helper()
		d, _ := f["data"].(map[string]any)
		gotSID, hasSID := f["session_id"]
		if f["type"] != "subscribe_buffer_error" || f["error"] != text || d == nil || d["buffer"] != buffer ||
			(sid == "" && hasSID) || (sid != "" && gotSID != sid) {
			t.Fatalf("%s: %v, want session_id %q, data.buffer %q, error %q", name, f, sid, buffer, text)
		}
	}
	standalone := func(name string, data any, sid, buffer, text string) {
		t.Helper()
		mark := c.count()
		rid := c.request("subscribe_buffer", data, sid)
		check(name, c.waitFor(mark, 5*time.Second, reply(rid, "subscribe_buffer_success", "subscribe_buffer_error")), sid, buffer, text)
	}
	standalone("blank buffer", map[string]any{"buffer": "  "}, synced, "  ", "buffer is required")
	standalone("unknown anchor", map[string]any{"buffer": "#c", "anchor": "middle"}, synced, "#c", "Unknown anchor")
	standalone("unsynced session", map[string]any{"buffer": "#c"}, unsynced, "#c", "sync_session with sync_mode lite required")
	standalone("unknown session", map[string]any{"buffer": "#c"}, "net-999", "#c", "sync_session with sync_mode lite required")
	standalone("bad data", map[string]any{"buffer": "#c", "members": "bogus"}, synced, "#c", "Invalid request data")
	standalone("no session_id", map[string]any{"buffer": "#c"}, "", "#c", "session_id is required")

	// Inline: 20 fresh subscriptions fill the cap; the 21st and a bad anchor fail.
	var subs []map[string]any
	for i := 0; i < liteMaxSubscriptions+1; i++ {
		subs = append(subs, map[string]any{"buffer": fmt.Sprintf("#b%d", i), "history": 0})
	}
	subs = append(subs, map[string]any{"buffer": "#z", "anchor": "middle"})
	mark := c.count()
	rid, _ := c.liteSync(unsynced, "ios-1", map[string]any{"subscribe": subs})
	inlineErr := func(buffer string) map[string]any {
		return c.waitFor(mark, 5*time.Second, func(f map[string]any) bool {
			d, _ := f["data"].(map[string]any)
			return f["request_id"] == rid && f["type"] == "subscribe_buffer_error" && d != nil && d["buffer"] == buffer
		})
	}
	check("inline over the cap", inlineErr(fmt.Sprintf("#b%d", liteMaxSubscriptions)), unsynced,
		fmt.Sprintf("#b%d", liteMaxSubscriptions), "Too many subscriptions")
	check("inline unknown anchor", inlineErr("#z"), unsynced, "#z", "Unknown anchor")
}
