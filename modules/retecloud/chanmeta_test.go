package retecloud

import (
	"testing"
	"time"
)

// A3031: a channel's modes and topic are in the lite state only once the brain knows them (""
// would read as cleared, as after a brain reload before the channel_meta fetch answers), and a
// reply that makes them known reaches subscribers as a buffer_state.
func TestLiteChannelStateOmitsUnknownAndPushesTheAnswer(t *testing.T) {
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #c") // no burst yet: modes and topic unknown
	c := e.syncedClient("ios-1", "undernet", "#c")
	var state map[string]any
	for _, f := range c.since(0) {
		if f["type"] == "subscribe_buffer_success" {
			state = f["data"].(map[string]any)["state"].(map[string]any)
		}
	}
	if state == nil || state["joined"] != true {
		t.Fatalf("state = %v", state)
	}
	for _, k := range []string{"modes", "topic", "topic_setter", "topic_timestamp"} {
		if v, ok := state[k]; ok {
			t.Fatalf("unknown %s sent as %v (state %v)", k, v, state)
		}
	}

	mark := c.count()
	e.feed("undernet", ":srv.example 324 me #c +nt")
	f := c.waitFor(mark, 5*time.Second, isType("buffer_state"))
	d := f["data"].(map[string]any)
	if d["buffer"] != "#c" || d["modes"] != "+nt" {
		t.Fatalf("buffer_state after 324 = %v", d)
	}
	if _, ok := d["topic"]; ok {
		t.Fatalf("topic still unknown, sent: %v", d)
	}

	mark = c.count()
	e.feed("undernet", ":srv.example 332 me #c :hello there", ":srv.example 333 me #c op!o@h 1700000001")
	f = c.waitFor(mark, 5*time.Second, func(f map[string]any) bool {
		d, _ := f["data"].(map[string]any)
		return f["type"] == "buffer_state" && d != nil && d["topic_setter"] != nil
	})
	d = f["data"].(map[string]any)
	if d["topic"] != "hello there" || d["topic_setter"] != "op!o@h" || d["topic_timestamp"] != float64(1700000001) || d["modes"] != "+nt" {
		t.Fatalf("buffer_state after 333 = %v", d)
	}

	// A topic known to be empty is sent as "" (cleared), with no setter.
	mark = c.count()
	e.feed("undernet", ":srv.example 331 me #c :No topic is set.")
	f = c.waitFor(mark, 5*time.Second, func(f map[string]any) bool {
		d, _ := f["data"].(map[string]any)
		return f["type"] == "buffer_state" && d != nil && d["topic"] == ""
	})
	if d := f["data"].(map[string]any); d["topic_setter"] != nil || d["topic_timestamp"] != nil {
		t.Fatalf("buffer_state after 331 = %v", d)
	}
}
