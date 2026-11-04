package retecloud

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

func TestGetMessagesPages(t *testing.T) {
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #c")
	for i := 1; i <= 30; i++ {
		e.feed("undernet", fmt.Sprintf(":peer!p@h PRIVMSG #c :n-%d", i))
	}
	c := e.syncedClient("ios-1", "undernet")
	rid := c.request("get_messages", map[string]any{"buffer": "#c", "limit": 10}, e.sid("undernet"))
	d := c.waitFor(0, 5*time.Second, reply(rid, "get_messages_success", "get_messages_error"))["data"].(map[string]any)
	msgs := d["messages"].([]any)
	if len(msgs) != 10 || d["has_more"] != true || d["has_more_before"] != true || d["has_more_after"] != false {
		t.Fatalf("latest page: %d msgs, %v", len(msgs), d)
	}
	oldest := d["oldest_id"].(float64)
	mark := c.count()
	rid = c.request("get_messages", map[string]any{"buffer": "#c", "limit": 100, "before_id": oldest}, e.sid("undernet"))
	d = c.waitFor(mark, 5*time.Second, reply(rid, "get_messages_success"))["data"].(map[string]any)
	if n := len(d["messages"].([]any)); n != 21 || d["has_more_after"] != true { // JOIN + n-1..n-20
		t.Fatalf("before page: %d msgs, %v", n, d)
	}
	m0 := d["messages"].([]any)[1].(map[string]any)
	if m0["event"] != "Raw" || !strings.Contains(fmt.Sprint(m0["event_data"]), "n-1") {
		t.Fatalf("message = %v", m0)
	}
}

func TestReadCursorBroadcastsToOtherFollowers(t *testing.T) {
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #c", ":peer!p@h PRIVMSG #c :read me")
	a := e.syncedClient("ios-a", "undernet")
	b := e.syncedClient("ios-b", "undernet")
	rid := a.request("get_messages", map[string]any{"buffer": "#c"}, e.sid("undernet"))
	msgs := a.waitFor(0, 5*time.Second, reply(rid, "get_messages_success"))["data"].(map[string]any)["messages"].([]any)
	last := msgs[len(msgs)-1].(map[string]any)["id"].(float64)
	ma, mb := a.count(), b.count()
	rid = a.request("read_cursor", map[string]any{"buffer": "#C", "last_message_id": last}, e.sid("undernet"))
	a.waitFor(ma, 5*time.Second, reply(rid, "read_cursor_success"))
	f := b.waitFor(mb, 5*time.Second, isType("read_cursor"))
	if d := f["data"].(map[string]any); d["buffer"] != "#c" || d["last_message_id"] != last || d["client_id"] != "ios-a" {
		t.Fatalf("read_cursor broadcast = %v", f)
	}
	time.Sleep(200 * time.Millisecond)
	for _, f := range a.since(ma) {
		if f["type"] == "read_cursor" {
			t.Fatalf("read_cursor echoed to its sender: %v", f)
		}
	}
}

func TestReadCursorErrorCarriesTheReason(t *testing.T) {
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #c", ":me!u@h JOIN #d", ":peer!p@h PRIVMSG #c :read me")
	a := e.syncedClient("ios-a", "undernet")
	rid := a.request("get_messages", map[string]any{"buffer": "#c"}, e.sid("undernet"))
	msgs := a.waitFor(0, 5*time.Second, reply(rid, "get_messages_success"))["data"].(map[string]any)["messages"].([]any)
	last := int64(msgs[len(msgs)-1].(map[string]any)["id"].(float64))
	mark := a.count()
	rid = a.request("read_cursor", map[string]any{"buffer": "#d", "last_message_id": last}, e.sid("undernet"))
	f := a.waitFor(mark, 5*time.Second, reply(rid, "read_cursor_success", "read_cursor_error"))
	if want := fmt.Sprintf("no message %d in #d", last); f["type"] != "read_cursor_error" || f["error"] != want {
		t.Fatalf("read_cursor = %v, want error %q", f, want)
	}
}

func TestReadCursorErrorText(t *testing.T) {
	long := strings.Repeat("x", 300)
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"reason", wire.Errorf(wire.CodeNotFound, "no message 9 in #x"), "no message 9 in #x"},
		{"control characters dropped", wire.Errorf(wire.CodeNotFound, "no message 9 in #a\r\nb\x00\x1b[1m"), "no message 9 in #ab[1m"},
		{"capped", wire.Errorf(wire.CodeNotFound, "%s", long), long[:readCursorErrorMax-3] + "..."},
		{"internal stays generic", wire.Errorf(wire.CodeInternal, "sql: database is locked"), "Failed to update read cursor"},
		{"empty message", &wire.Error{Code: wire.CodeNotFound}, "Failed to update read cursor"},
		{"transport", errors.New("module: not connected"), "Failed to update read cursor"},
	} {
		if got := readCursorErrorText(tc.err); got != tc.want {
			t.Errorf("%s: %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestCloseBufferThenDMReopens(t *testing.T) {
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":bob!b@h PRIVMSG me :first")
	c := e.syncedClient("ios-1", "undernet")
	rid := c.request("close_buffer", map[string]any{"buffer": "bob"}, e.sid("undernet"))
	c.waitFor(0, 5*time.Second, reply(rid, "close_buffer_success"))
	mark := c.count()
	e.feed("undernet", ":bob!b@h PRIVMSG me :again")
	f := c.waitFor(mark, 5*time.Second, isType("open_buffer"))
	if d := f["data"].(map[string]any); d["buffer"] != "bob" || d["reason"] != "message" || d["client_id"] != "" {
		t.Fatalf("open_buffer = %v", f)
	}
	rid = c.request("open_buffer", map[string]any{"buffer": "carol"}, e.sid("undernet"))
	c.waitFor(mark, 5*time.Second, reply(rid, "open_buffer_success"))
}

func TestSendAndWHOISReplyOnlyToAskingConnection(t *testing.T) {
	e := newTestEnv(t)
	_, ircd := e.addUplinkNetwork("undernet", false)
	a := e.syncedClient("ios-a", "undernet")
	b := e.syncedClient("ios-b", "undernet")
	ma, mb := a.count(), b.count()
	rid := a.request("send", map[string]any{"command": "WHOIS peer"}, e.sid("undernet"))
	if f := a.waitFor(ma, 5*time.Second, reply(rid, "send_success", "send_error")); f["type"] != "send_success" {
		t.Fatalf("send: %v", f)
	}
	a.waitFor(ma, 5*time.Second, func(f map[string]any) bool {
		return f["type"] == "irc_line" && strings.Contains(f["data"].(map[string]any)["raw"].(string), " 318 ")
	})
	time.Sleep(200 * time.Millisecond)
	if n := linesWith(b, mb, " 311 "); n != 0 {
		t.Fatal("WHOIS reply reached another connection")
	}
	if len(ircd.received("WHOIS")) != 1 {
		t.Fatalf("ircd got %v", ircd.received("WHOIS"))
	}
}

func TestSendBeforeBrainReadyIsReloading(t *testing.T) {
	e := newTestEnv(t, "undernet")
	c := e.syncedClient("ios-1", "undernet")
	e.be.ready.Store(false)
	rid := c.request("send", map[string]any{"command": "PRIVMSG #c :x"}, e.sid("undernet"))
	if f := c.waitFor(0, 5*time.Second, reply(rid, "send_success", "send_error")); f["error"] != "Bouncer is reloading" {
		t.Fatalf("send while not ready: %v", f)
	}
}

func TestConnectDisconnectSession(t *testing.T) {
	e := newTestEnv(t, "undernet")
	c := e.syncedClient("ios-1", "undernet")
	for _, typ := range []string{"disconnect_session", "connect_session"} {
		mark := c.count()
		rid := c.request(typ, map[string]any{}, e.sid("undernet"))
		if f := c.waitFor(mark, 5*time.Second, reply(rid, typ+"_success", typ+"_error")); f["type"] != typ+"_success" {
			t.Fatalf("%s: %v", typ, f)
		}
	}
	rid := c.request("connect_session", map[string]any{}, "net-999")
	if f := c.waitFor(0, 5*time.Second, reply(rid, "connect_session_error")); f["error"] != "No access to session" {
		t.Fatalf("unknown session: %v", f)
	}
}
