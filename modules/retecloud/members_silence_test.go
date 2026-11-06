package retecloud

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

// A2992: lite member entries carry the metadata full sync has, the compact form is gone, and
// the SILENCE list travels in the sync and in a tier-0 silence frame.

func TestMembersOptionCompactIsFull(t *testing.T) {
	var m MembersOption
	if err := json.Unmarshal([]byte(`"compact"`), &m); err != nil || m.mode != membersFull {
		t.Fatalf(`"compact" -> %v, %v; want the full form`, m.mode, err)
	}
	if json.Unmarshal([]byte(`"bogus"`), &m) == nil {
		t.Fatal("bogus value must be rejected")
	}
}

func TestSubscribeMembersCarryMetadata(t *testing.T) {
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":srv.example 005 me WHOX PREFIX=(ov)@+ :are supported by this server",
		":me!u@cloak JOIN #rete", ":srv.example 353 me = #rete :@me peer!p@peer.example",
		":srv.example 366 me #rete :End",
		":srv.example 352 me #rete p peer.example srv.example peer G* :0 Peer")
	c := e.dial(false)
	c.authenticate("ios-1")
	c.liteSync(e.sid("undernet"), "ios-1", nil)
	for _, form := range []any{"full", "compact"} {
		mark := c.count()
		rid := c.request("subscribe_buffer", map[string]any{"buffer": "#rete", "history": 0, "members": form}, e.sid("undernet"))
		sub := c.waitFor(mark, 5*time.Second, reply(rid, "subscribe_buffer_success", "subscribe_buffer_error"))
		if sub["type"] != "subscribe_buffer_success" {
			t.Fatalf("subscribe (%v): %v", form, sub)
		}
		users := sub["data"].(map[string]any)["state"].(map[string]any)["users"].([]any)
		byNick := map[string]map[string]any{}
		for _, u := range users {
			m := u.(map[string]any)
			byNick[m["nick"].(string)] = m
		}
		want := map[string]any{"nick": "peer", "modes": "", "username": "p", "hostname": "peer.example",
			"account": "", "away": true, "oper": true, "bot": false}
		if !reflect.DeepEqual(byNick["peer"], want) {
			t.Fatalf("members %v: peer = %v", form, byNick["peer"])
		}
		if me := byNick["me"]; me["modes"] != "o" || me["username"] != "u" || me["hostname"] != "cloak" {
			t.Fatalf("members %v: me = %v", form, me)
		}
	}
}

func TestLiteSyncCarriesSilenceAndLiveChanges(t *testing.T) {
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":me!u@cloak JOIN #rete", ":me!u@cloak SILENCE +*!*@spam.example")
	c := e.dial(false)
	c.authenticate("ios-1")
	_, frames := c.liteSync(e.sid("undernet"), "ios-1", nil)
	d := successData(t, frames)
	if got := d["silence"]; !reflect.DeepEqual(got, []any{"*!*@spam.example"}) {
		t.Fatalf("sync silence = %#v", got)
	}

	mark := c.count()
	e.feed("undernet", ":me!u@cloak SILENCE -*!*@spam.example")
	f := c.waitFor(mark, 5*time.Second, isType("silence"))
	if f["session_id"] != e.sid("undernet") {
		t.Fatalf("silence frame = %v", f)
	}
	if masks := f["data"].(map[string]any)["masks"]; !reflect.DeepEqual(masks, []any{}) {
		t.Fatalf("masks = %#v, want []", masks)
	}
	// The SILENCE line itself follows the frame (tier 0).
	line := c.waitFor(mark, 5*time.Second, isType("irc_line"))
	frames = c.since(mark)
	si, li := -1, -1
	for i, fr := range frames {
		if fr["type"] == "silence" && si < 0 {
			si = i
		}
		if fr["type"] == "irc_line" && li < 0 {
			li = i
		}
	}
	if si < 0 || li < si {
		t.Fatalf("silence frame at %d, line %v at %d", si, line, li)
	}

	_, frames = c.liteSync(e.sid("undernet"), "ios-1", nil)
	if got := successData(t, frames)["silence"]; !reflect.DeepEqual(got, []any{}) {
		t.Fatalf("sync silence after removal = %#v, want []", got)
	}
}
