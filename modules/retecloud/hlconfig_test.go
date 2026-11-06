package retecloud

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// configItem finds key in a config_update frame's updates.
func configItem(f map[string]any, key string) map[string]any {
	d, _ := f["data"].(map[string]any)
	ups, _ := d["updates"].([]any)
	for _, u := range ups {
		if it := u.(map[string]any); it["key"] == key {
			return it
		}
	}
	return nil
}

// syncConfig syncs network on c and returns the config_update sent before sync_start.
func syncConfig(t *testing.T, e *testEnv, c *wsClient, device string) map[string]any {
	t.Helper()
	mark := c.count()
	c.liteSync(e.sid("undernet"), device, nil)
	for _, f := range c.since(mark) {
		if f["type"] == "config_update" {
			return f
		}
		if f["type"] == "sync_start" {
			break
		}
	}
	t.Fatalf("no config_update before sync_start: %v", c.since(mark))
	return nil
}

func (c *wsClient) call(typ, sid string, data any) map[string]any {
	c.t.Helper()
	mark := c.count()
	rid := c.request(typ, data, sid)
	return c.waitFor(mark, 5*time.Second, reply(rid, typ+"_success", typ+"_error"))
}

func TestSetBufferHighlightStoresFoldsAndBroadcasts(t *testing.T) {
	e := newTestEnv(t, "undernet")
	a := e.dial(false)
	a.authenticate("ios-a")
	// Never written: the sync's config_update has the name only (the seeding rule).
	first := syncConfig(t, e, a, "ios-a")
	if configItem(first, "name") == nil || configItem(first, "buffer_highlight_off") != nil || configItem(first, "highlight_rules") != nil {
		t.Fatalf("unwritten config = %v", first)
	}
	b := e.dial(false)
	b.authenticate("desk-b")
	b.liteSync(e.sid("undernet"), "desk-b", nil)
	mark := b.count()

	f := a.call("set_buffer_highlight", e.sid("undernet"), map[string]any{"buffer": "#Noisy[1]", "highlight": false})
	d, _ := f["data"].(map[string]any)
	if f["type"] != "set_buffer_highlight_success" || d["buffer"] != "#noisy{1}" || d["highlight"] != false {
		t.Fatalf("answer = %v", f)
	}
	up := b.waitFor(mark, 5*time.Second, isType("config_update"))
	it := configItem(up, "buffer_highlight_off")
	if it == nil || it["value"] != `["#noisy{1}"]` || it["updated_at"] == "" {
		t.Fatalf("broadcast = %v", up)
	}
	a.waitFor(0, 5*time.Second, func(f map[string]any) bool {
		return f["type"] == "config_update" && configItem(f, "buffer_highlight_off") != nil
	})

	// No change, no broadcast.
	mark = b.count()
	if f := a.call("set_buffer_highlight", e.sid("undernet"), map[string]any{"buffer": "#NOISY[1]", "highlight": false}); f["type"] != "set_buffer_highlight_success" {
		t.Fatalf("repeat = %v", f)
	}
	a.call("set_buffer_highlight", e.sid("undernet"), map[string]any{"buffer": "bot", "highlight": false})
	up = b.waitFor(mark, 5*time.Second, isType("config_update"))
	if it := configItem(up, "buffer_highlight_off"); it["value"] != `["#noisy{1}","bot"]` {
		t.Fatalf("second broadcast = %v (a repeat must not broadcast)", up)
	}

	// Stored in the module's key-value store.
	v, found, err := e.be.db.ModuleKVGet(context.Background(), "retecloud", fmt.Sprintf("highlight_off/%d", e.nets["undernet"]))
	if err != nil || !found || !strings.Contains(v, `"buffers":["#noisy{1}","bot"]`) {
		t.Fatalf("kv = %q %v %v", v, found, err)
	}
	// Removing.
	a.call("set_buffer_highlight", e.sid("undernet"), map[string]any{"buffer": "Bot", "highlight": true})
	c := e.dial(false)
	c.authenticate("ios-c")
	if it := configItem(syncConfig(t, e, c, "ios-c"), "buffer_highlight_off"); it == nil || it["value"] != `["#noisy{1}"]` {
		t.Fatalf("sync after writes = %v", it)
	}
}

func TestSetBufferHighlightErrors(t *testing.T) {
	e := newTestEnv(t, "undernet")
	a := e.dial(false)
	a.authenticate("ios-a")
	cases := []struct {
		sid  string
		data any
		text string
		code string
	}{
		{"", map[string]any{"buffer": "#a", "highlight": false}, "session_id is required", "invalid_request"},
		{e.sid("undernet"), map[string]any{"highlight": false}, "buffer is required", "invalid_request"},
		{e.sid("undernet"), map[string]any{"buffer": "#a"}, "highlight is required", "invalid_request"},
		{"net-999", map[string]any{"buffer": "#a", "highlight": false}, "No access to session", "no_access"},
	}
	for _, tc := range cases {
		f := a.call("set_buffer_highlight", tc.sid, tc.data)
		if f["type"] != "set_buffer_highlight_error" || f["error"] != tc.text || f["code"] != tc.code {
			t.Errorf("%v: %v", tc.data, f)
		}
	}
	// Unauthenticated.
	b := e.dial(false)
	mark := b.count()
	rid := b.request("set_buffer_highlight", map[string]any{"buffer": "#a", "highlight": false}, e.sid("undernet"))
	if f := b.waitFor(mark, 5*time.Second, reply(rid, "set_buffer_highlight_error")); f["error"] != "Not authenticated" || f["code"] != "not_authenticated" {
		t.Fatalf("unauthenticated: %v", f)
	}
}

func TestSetBufferHighlightLimit(t *testing.T) {
	e := newTestEnv(t, "undernet")
	var list []string
	for i := 0; i < maxHighlightOff; i++ {
		list = append(list, fmt.Sprintf("#c%03d", i))
	}
	v, _ := json.Marshal(storedHighlightOff{Buffers: list, UpdatedAt: "2026-11-06T12:00:00.000Z"})
	if err := e.be.db.ModuleKVSet(context.Background(), "retecloud", fmt.Sprintf("highlight_off/%d", e.nets["undernet"]), string(v)); err != nil {
		t.Fatal(err)
	}
	a := e.dial(false)
	a.authenticate("ios-a")
	if f := a.call("set_buffer_highlight", e.sid("undernet"), map[string]any{"buffer": "#new", "highlight": false}); f["code"] != "limit" {
		t.Fatalf("501st: %v", f)
	}
	if f := a.call("set_buffer_highlight", e.sid("undernet"), map[string]any{"buffer": "#c000", "highlight": true}); f["type"] != "set_buffer_highlight_success" {
		t.Fatalf("removing at the limit: %v", f)
	}
}

func TestSetHighlightRulesVersioning(t *testing.T) {
	e := newTestEnv(t, "undernet")
	a := e.dial(false)
	a.authenticate("ios-a")
	a.liteSync(e.sid("undernet"), "ios-a", nil)
	// Stored with exactly the rule fields, in the shape's order: an id is dropped, defaults filled.
	rules := []map[string]any{{"id": "local-1", "isEnabled": true, "ruleType": "contains_text", "matchType": map[string]any{"value": "rete"}},
		{"ruleType": "sender_is"}}

	f := a.call("set_highlight_rules", e.sid("undernet"), map[string]any{"rules": rules, "if_version": ""})
	if f["type"] != "set_highlight_rules_success" {
		t.Fatalf("first write: %v", f)
	}
	v1 := f["data"].(map[string]any)["version"].(string)
	up := a.waitFor(0, 5*time.Second, func(f map[string]any) bool {
		return f["type"] == "config_update" && configItem(f, "highlight_rules") != nil
	})
	it := configItem(up, "highlight_rules")
	if it["updated_at"] != v1 || it["value"] != `[{"isEnabled":true,"ruleType":"contains_text","matchType":{"value":"rete"}},{"isEnabled":false,"ruleType":"sender_is","matchType":{"value":""}}]` {
		t.Fatalf("broadcast = %v", it)
	}

	// A write from the never-written version now conflicts and names the stored one.
	f = a.call("set_highlight_rules", e.sid("undernet"), map[string]any{"rules": []any{}, "if_version": ""})
	if f["code"] != "version_conflict" || f["error"] != "Highlight rules changed" || f["data"].(map[string]any)["version"] != v1 {
		t.Fatalf("stale write: %v", f)
	}
	// A different instant conflicts, even within the same millisecond.
	t1, _ := time.Parse(time.RFC3339Nano, v1)
	f = a.call("set_highlight_rules", e.sid("undernet"), map[string]any{"rules": []any{},
		"if_version": t1.Add(time.Microsecond).Format(time.RFC3339Nano)})
	if f["code"] != "version_conflict" {
		t.Fatalf("a microsecond later: %v", f)
	}
	// From the current version, spelled differently (offset, more digits): a new, later version.
	other := t1.In(time.FixedZone("", 2*3600)).Format("2006-01-02T15:04:05.000000-07:00")
	f = a.call("set_highlight_rules", e.sid("undernet"), map[string]any{"rules": []any{}, "if_version": other})
	v2, _ := f["data"].(map[string]any)["version"].(string)
	if f["type"] != "set_highlight_rules_success" || v2 <= v1 {
		t.Fatalf("second write: %v (v1 %s)", f, v1)
	}
	// Written as empty: present (not absent) on the next sync.
	b := e.dial(false)
	b.authenticate("ios-b")
	if it := configItem(syncConfig(t, e, b, "ios-b"), "highlight_rules"); it == nil || it["value"] != "[]" || it["updated_at"] != v2 {
		t.Fatalf("sync = %v", it)
	}
}

func TestSetHighlightRulesErrors(t *testing.T) {
	e := newTestEnv(t, "undernet")
	a := e.dial(false)
	a.authenticate("ios-a")
	many := make([]any, 201)
	for i := range many {
		many[i] = map[string]any{"isEnabled": true, "ruleType": "contains_text", "matchType": map[string]any{"value": "x"}}
	}
	cases := []struct {
		sid  string
		data any
		text string
		code string
	}{
		{"", map[string]any{"rules": []any{}, "if_version": ""}, "session_id is required", "invalid_request"},
		{e.sid("undernet"), map[string]any{"if_version": ""}, "rules is required", "invalid_request"},
		{e.sid("undernet"), map[string]any{"rules": []any{}}, "if_version is required", "invalid_request"},
		{e.sid("undernet"), map[string]any{"rules": "[]", "if_version": ""}, "Invalid rule", "invalid_request"},
		{e.sid("undernet"), map[string]any{"rules": []any{map[string]any{"isEnabled": true}}, "if_version": ""}, "Invalid rule", "invalid_request"},
		{e.sid("undernet"), map[string]any{"rules": many, "if_version": ""}, "Too many highlight rules", "limit"},
		{"net-999", map[string]any{"rules": []any{}, "if_version": ""}, "No access to session", "no_access"},
	}
	for _, tc := range cases {
		f := a.call("set_highlight_rules", tc.sid, tc.data)
		if f["type"] != "set_highlight_rules_error" || f["error"] != tc.text || f["code"] != tc.code {
			t.Errorf("%v: %v", tc.text, f)
		}
	}
}

// The live activity counts honour the switch (a DM included) and the rules.
func TestActivityHighlightsHonourSwitchAndRules(t *testing.T) {
	old := liteActivityFlushDelay
	liteActivityFlushDelay = 50 * time.Millisecond
	defer func() { liteActivityFlushDelay = old }()
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #a", ":me!u@h JOIN #b")
	c := e.syncedClient("ios-1", "undernet")
	sid := e.sid("undernet")
	c.call("set_buffer_highlight", sid, map[string]any{"buffer": "#A", "highlight": false})
	c.call("set_buffer_highlight", sid, map[string]any{"buffer": "Peer", "highlight": false})

	activityFor := func(mark int, buffer string) map[string]any {
		for {
			f := c.waitFor(mark, 5*time.Second, isType("activity"))
			for _, b := range f["data"].(map[string]any)["buffers"].([]any) {
				if bb := b.(map[string]any); bb["buffer"] == buffer {
					return bb
				}
			}
			mark = c.count()
		}
	}
	mark := c.count()
	e.feed("undernet", ":x!x@h PRIVMSG #a :hey me")
	if a := activityFor(mark, "#a"); a["unread"] != float64(1) || a["highlights"] != float64(0) {
		t.Fatalf("#a (switched off) = %v", a)
	}
	mark = c.count()
	e.feed("undernet", ":peer!p@h PRIVMSG me :psst")
	if a := activityFor(mark, "peer"); a["unread"] != float64(1) || a["highlights"] != float64(0) {
		t.Fatalf("DM switched off = %v", a)
	}
	mark = c.count()
	e.feed("undernet", ":x!x@h PRIVMSG #b :hey me")
	if a := activityFor(mark, "#b"); a["highlights"] != float64(1) {
		t.Fatalf("#b default mention = %v", a)
	}

	// Rules replace the default mention.
	c.call("set_highlight_rules", sid, map[string]any{"if_version": "",
		"rules": []any{map[string]any{"isEnabled": true, "ruleType": "contains_text", "matchType": map[string]any{"value": "rete"}}}})
	mark = c.count()
	e.feed("undernet", ":x!x@h PRIVMSG #b :hey me again")
	if a := activityFor(mark, "#b"); a["unread"] != float64(2) || a["highlights"] != float64(1) {
		t.Fatalf("#b mention under rules = %v", a)
	}
	mark = c.count()
	e.feed("undernet", ":x!x@h PRIVMSG #b :about Rete")
	if a := activityFor(mark, "#b"); a["unread"] != float64(3) || a["highlights"] != float64(2) {
		t.Fatalf("#b rule match = %v", a)
	}
}

// Membership-based rules see the sender's modes, kept in step by MODE lines.
func TestActivityRulesUseMembership(t *testing.T) {
	old := liteActivityFlushDelay
	liteActivityFlushDelay = 50 * time.Millisecond
	defer func() { liteActivityFlushDelay = old }()
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #a", ":srv.example 353 me = #a :@me op voice", ":srv.example 366 me #a :End")
	c := e.syncedClient("ios-1", "undernet")
	sid := e.sid("undernet")
	c.call("set_highlight_rules", sid, map[string]any{"if_version": "",
		"rules": []any{map[string]any{"isEnabled": true, "ruleType": "sender_opped_on_common_channels", "matchType": map[string]any{"value": ""}}}})
	highlights := func(text string) float64 {
		mark := c.count()
		e.feed("undernet", text)
		f := c.waitFor(mark, 5*time.Second, isType("activity"))
		return f["data"].(map[string]any)["buffers"].([]any)[0].(map[string]any)["highlights"].(float64)
	}
	if h := highlights(":op!o@h PRIVMSG #a :one"); h != 0 {
		t.Fatalf("not yet opped: %v", h)
	}
	e.feed("undernet", ":me!u@h MODE #a +vo voice op")
	if h := highlights(":op!o@h PRIVMSG #a :two"); h != 1 {
		t.Fatalf("opped: %v", h)
	}
	if h := highlights(":voice!v@h PRIVMSG #a :three"); h != 1 {
		t.Fatalf("voiced only: %v", h)
	}
	e.feed("undernet", ":me!u@h MODE #a -o op")
	if h := highlights(":op!o@h PRIVMSG #a :four"); h != 1 {
		t.Fatalf("deopped: %v", h)
	}
}

// The bootstrap's counts honour the switch and the rules, and survive a brain reconnect (the
// settings are read again from the store).
func TestBootstrapHighlightsHonourSwitchAndRules(t *testing.T) {
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #a", ":me!u@h JOIN #b", ":me!u@h JOIN #c")
	sid := e.sid("undernet")
	setter := e.dial(false)
	setter.authenticate("desk")
	setter.call("set_buffer_highlight", sid, map[string]any{"buffer": "#a", "highlight": false})
	setter.call("set_highlight_rules", sid, map[string]any{"if_version": "",
		"rules": []any{map[string]any{"isEnabled": true, "ruleType": "contains_text", "matchType": map[string]any{"value": "rete"}}}})
	e.feed("undernet", ":x!x@h PRIVMSG #a :me one", ":x!x@h PRIVMSG #b :me two", ":x!x@h PRIVMSG #b :rete three",
		":x!x@h PRIVMSG #c :me four")

	check := func(device string) {
		t.Helper()
		c := e.dial(false)
		c.authenticate(device)
		_, frames := c.liteSync(sid, device, nil)
		bufs := liteBuffers(t, successData(t, frames))
		want := map[string][2]any{"#a": {float64(1), nil}, "#b": {float64(2), float64(1)}, "#c": {float64(1), nil}}
		for name, w := range want {
			if bufs[name]["unread"] != w[0] || bufs[name]["highlights"] != w[1] {
				t.Errorf("%s: %s = %v, want unread %v highlights %v", device, name, bufs[name], w[0], w[1])
			}
		}
	}
	check("ios-1")
	e.stopSrv()
	time.Sleep(100 * time.Millisecond)
	e.startBrain()
	time.Sleep(300 * time.Millisecond)
	check("ios-2")
}
