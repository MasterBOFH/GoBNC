package retecloud

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

func TestQuitFrameCarriesEachChannelsOwnID(t *testing.T) {
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #a", ":me!u@h JOIN #b", ":peer!p@h JOIN #a", ":peer!p@h JOIN #b")
	c := e.syncedClient("ios-1", "undernet", "#a")
	mark := c.count()
	e.feed("undernet", ":peer!p@h QUIT :gone")
	f := c.waitFor(mark, 5*time.Second, func(f map[string]any) bool {
		return f["type"] == "irc_line" && strings.Contains(f["data"].(map[string]any)["raw"].(string), "QUIT :gone")
	})
	d := f["data"].(map[string]any)
	targets, _ := d["targets"].([]any)
	if len(targets) != 2 {
		t.Fatalf("targets = %v (frame %v)", d["targets"], f)
	}
	ids := map[string]float64{}
	for _, raw := range targets {
		tg := raw.(map[string]any)
		ids[tg["buffer"].(string)] = tg["id"].(float64)
	}
	if ids["#a"] <= 0 || ids["#b"] <= 0 || ids["#a"] == ids["#b"] {
		t.Fatalf("per-buffer ids = %v", ids)
	}
	first := targets[0].(map[string]any)
	if d["buffer"] != first["buffer"] || d["id"] != first["id"] {
		t.Fatalf("buffer/id %v/%v differ from the first target %v", d["buffer"], d["id"], first)
	}
	// Each id is that buffer's own stored copy.
	for buf, id := range ids {
		m := c.count()
		rid := c.request("get_messages", map[string]any{"buffer": buf}, e.sid("undernet"))
		msgs := c.waitFor(m, 5*time.Second, reply(rid, "get_messages_success"))["data"].(map[string]any)["messages"].([]any)
		last := msgs[len(msgs)-1].(map[string]any)
		if last["id"] != id || !strings.Contains(fmt.Sprint(last["event_data"]), "QUIT :gone") {
			t.Fatalf("%s: newest stored row %v, target id %v", buf, last, id)
		}
	}
}

// ircLineOf waits for the irc_line whose raw contains text and returns its data.
func ircLineOf(t *testing.T, c *wsClient, mark int, text string) map[string]any {
	t.Helper()
	f := c.waitFor(mark, 5*time.Second, func(f map[string]any) bool {
		return f["type"] == "irc_line" && strings.Contains(f["data"].(map[string]any)["raw"].(string), text)
	})
	return f["data"].(map[string]any)
}

// A server-sourced NOTICE to a channel is the channel's line (owner 2026-11-05): a subscriber
// gets it with the channel's id, and a connection not subscribed counts it as activity.
func TestServerNoticeToChannelCarriesTheChannelsID(t *testing.T) {
	old := liteActivityFlushDelay
	liteActivityFlushDelay = 100 * time.Millisecond
	defer func() { liteActivityFlushDelay = old }()
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #a", ":me!u@h JOIN #b")
	c := e.syncedClient("ios-1", "undernet", "#a")
	mark := c.count()
	e.feed("undernet", ":irc.example.net NOTICE #a :in-subscribed", ":irc.example.net NOTICE #b :in-unsubscribed")
	d := ircLineOf(t, c, mark, "in-subscribed")
	if d["buffer"] != "#a" || d["id"] == nil || d["id"].(float64) <= 0 {
		t.Fatalf("irc_line = %v, want buffer #a with an id", d)
	}
	act := c.waitFor(mark, 5*time.Second, isType("activity"))
	entry := act["data"].(map[string]any)["buffers"].([]any)[0].(map[string]any)
	if entry["buffer"] != "#b" || entry["unread"] != float64(1) || entry["last_id"].(float64) <= 0 {
		t.Fatalf("activity = %v", act)
	}
	if linesWith(c, mark, "in-unsubscribed") != 0 {
		t.Fatal("line of an unsubscribed buffer delivered")
	}
}

// A server notice to us stays in the Server buffer, now with an id: the read cursor can be set
// on it and history has it.
func TestServerNoticeToUsCarriesAServerID(t *testing.T) {
	e := newTestEnv(t, "undernet")
	c := e.syncedClient("ios-1", "undernet")
	mark := c.count()
	e.feed("undernet", ":irc.example.net NOTICE me :*** Notice -- snotice")
	d := ircLineOf(t, c, mark, "snotice")
	if d["buffer"] != wire.ServerBuffer || d["id"] == nil || d["id"].(float64) <= 0 {
		t.Fatalf("irc_line = %v, want buffer Server with an id", d)
	}
	id := d["id"].(float64)
	m := c.count()
	rid := c.request("read_cursor", map[string]any{"buffer": wire.ServerBuffer, "last_message_id": id}, e.sid("undernet"))
	c.waitFor(m, 5*time.Second, reply(rid, "read_cursor_success"))
	m = c.count()
	rid = c.request("get_messages", map[string]any{"buffer": wire.ServerBuffer}, e.sid("undernet"))
	msgs := c.waitFor(m, 5*time.Second, reply(rid, "get_messages_success"))["data"].(map[string]any)["messages"].([]any)
	if len(msgs) == 0 || msgs[len(msgs)-1].(map[string]any)["id"] != id {
		t.Fatalf("Server history = %v, want the notice with id %v", msgs, id)
	}
}

// A STATUSMSG line (NOTICE @#a, PRIVMSG +#a) is #a's: its id is a row of #a, so read_cursor on
// #a with it succeeds and get_messages #a has it.
func TestStatusMsgLineIDIsTheChannels(t *testing.T) {
	for _, tc := range []struct{ line, text string }{
		{":peer!p@h NOTICE @#a :ops-only", "ops-only"},
		{":peer!p@h PRIVMSG +#a :voiced-only", "voiced-only"},
	} {
		t.Run(tc.text, func(t *testing.T) {
			e := newTestEnv(t, "undernet")
			e.feed("undernet", ":me!u@h JOIN #a")
			c := e.syncedClient("ios-1", "undernet", "#a")
			mark := c.count()
			e.feed("undernet", tc.line)
			d := ircLineOf(t, c, mark, tc.text)
			if d["buffer"] != "#a" || d["id"] == nil || d["id"].(float64) <= 0 {
				t.Fatalf("irc_line = %v, want buffer #a with an id", d)
			}
			id := d["id"].(float64)
			m := c.count()
			rid := c.request("read_cursor", map[string]any{"buffer": "#a", "last_message_id": id}, e.sid("undernet"))
			if f := c.waitFor(m, 5*time.Second, reply(rid, "read_cursor_success", "read_cursor_error")); f["type"] != "read_cursor_success" {
				t.Fatalf("read_cursor on #a with id %v = %v", id, f)
			}
			m = c.count()
			rid = c.request("get_messages", map[string]any{"buffer": "#a"}, e.sid("undernet"))
			msgs := c.waitFor(m, 5*time.Second, reply(rid, "get_messages_success"))["data"].(map[string]any)["messages"].([]any)
			if len(msgs) == 0 || msgs[len(msgs)-1].(map[string]any)["id"] != id {
				t.Fatalf("#a history = %v, want the line with id %v", msgs, id)
			}
		})
	}
}

func TestIRCLineFrameTargets(t *testing.T) {
	one := ircLineFrame(wire.LineEvent{Raw: ":p!u@h PRIVMSG #a :x", Buffers: []wire.LineBuffer{{Name: "#a", Key: "#a", ID: 7}}})
	if _, ok := one.Data.(map[string]interface{})["targets"]; ok {
		t.Fatalf("single stored buffer has targets: %v", one.Data)
	}
	fan := ircLineFrame(wire.LineEvent{Raw: ":p!u@h NICK q", Buffers: []wire.LineBuffer{
		{Name: "p", Key: "p"}, {Name: "#a", Key: "#a", ID: 7}, {Name: "#B", Key: "#b", ID: 8},
	}})
	d := fan.Data.(map[string]interface{})
	want := []lineTarget{{Buffer: "#a", ID: 7}, {Buffer: "#B", ID: 8}}
	got, _ := d["targets"].([]lineTarget)
	if fmt.Sprint(got) != fmt.Sprint(want) || d["id"] != int64(7) || d["buffer"] != "#a" {
		t.Fatalf("fan-out frame = %v", d)
	}
}

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

// A server notice to a channel counts as unread but is never a highlight, even when it names
// us (owner: highlights and push never come from server-sourced lines).
func TestServerNoticeNamingUsInChannelIsNotAHighlight(t *testing.T) {
	old := liteActivityFlushDelay
	liteActivityFlushDelay = 100 * time.Millisecond
	defer func() { liteActivityFlushDelay = old }()
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #a", ":me!u@h JOIN #b")
	c := e.syncedClient("ios-1", "undernet", "#a")
	mark := c.count()
	e.feed("undernet", ":irc.example.net NOTICE #b :me: netsplit ahead")
	act := c.waitFor(mark, 5*time.Second, isType("activity"))
	entry := act["data"].(map[string]any)["buffers"].([]any)[0].(map[string]any)
	if entry["buffer"] != "#b" || entry["unread"] != float64(1) || entry["highlights"] != float64(0) {
		t.Fatalf("activity = %v, want #b with 1 unread and no highlight", act)
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
