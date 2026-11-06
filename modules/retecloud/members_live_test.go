package retecloud

import (
	"testing"
	"time"
)

// A phone that subscribed a channel before GoBNC learned its members' flags (the brain's own
// WHOX seed, a client's WHO) gets them: a buffer_state with the members follows the WHO's end.

func memberEntry(t *testing.T, frame map[string]any, nick string) map[string]any {
	t.Helper()
	data, _ := frame["data"].(map[string]any)
	if st, ok := data["state"].(map[string]any); ok { // subscribe_buffer_success
		data = st
	}
	users, _ := data["users"].([]any)
	for _, u := range users {
		if m := u.(map[string]any); m["nick"] == nick {
			return m
		}
	}
	return nil
}

func TestSubscribedLiteGetsMembersAfterWHO(t *testing.T) {
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":srv.example 005 me WHOX PREFIX=(ov)@+ :are supported by this server",
		":me!u@cloak JOIN #rete", ":srv.example 353 me = #rete :@me peer",
		":srv.example 366 me #rete :End")
	c := e.dial(false)
	c.authenticate("ios-1")
	c.liteSync(e.sid("undernet"), "ios-1", nil)
	mark := c.count()
	rid := c.request("subscribe_buffer", map[string]any{"buffer": "#rete", "history": 0, "members": "full"}, e.sid("undernet"))
	sub := c.waitFor(mark, 5*time.Second, reply(rid, "subscribe_buffer_success", "subscribe_buffer_error"))
	if p := memberEntry(t, sub, "peer"); p == nil || p["away"] != false || p["oper"] != false {
		t.Fatalf("peer at subscribe = %v", p)
	}

	mark = c.count()
	e.feed("undernet", ":srv.example 352 me #rete p peer.example srv.example peer G* :0 Peer",
		":srv.example 352 me #rete u cloak srv.example me H@ :0 Me",
		":srv.example 315 me #rete :End of /WHO list.")
	f := c.waitFor(mark, 5*time.Second, func(f map[string]any) bool {
		return f["type"] == "buffer_state" && memberEntry(t, f, "peer") != nil
	})
	p := memberEntry(t, f, "peer")
	if p["away"] != true || p["oper"] != true || p["hostname"] != "peer.example" || p["username"] != "p" {
		t.Fatalf("peer after WHO = %v", p)
	}
	if f["session_id"] != e.sid("undernet") {
		t.Fatalf("buffer_state = %v", f)
	}
}
