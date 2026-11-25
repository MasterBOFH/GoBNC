package retecloud

import (
	"strings"
	"testing"
	"time"
)

// A CTCP request or reply (not ACTION) is drawn by the device in its Server buffer, whatever
// the line's target, as an IRC client draws it; so it is Server-tier traffic every lite
// connection gets, not a line of the peer's query that only a subscriber of that query sees.
// Without this, /ctcp from a phone with no query open to the peer showed neither our request
// nor the reply.
func TestCTCPRequestAndReplyReachAConnectionWithoutThePeersQuery(t *testing.T) {
	for _, echo := range []bool{false, true} {
		name := "local echo"
		if echo {
			name = "echo-message"
		}
		t.Run(name, func(t *testing.T) {
			e := newTestEnv(t)
			e.addUplinkNetwork("undernet", echo)
			c := e.syncedClient("ios-1", "undernet") // no query with peer subscribed
			mark := c.count()
			rid := c.request("send", map[string]any{"command": "PRIVMSG peer :\x01VERSION\x01"}, e.sid("undernet"))
			if f := c.waitFor(mark, 5*time.Second, reply(rid, "send_success", "send_error")); f["type"] != "send_success" {
				t.Fatalf("send: %v", f)
			}
			req := ircLineOf(t, c, mark, "PRIVMSG peer :\x01VERSION\x01")
			if !strings.HasPrefix(req["raw"].(string), ":me!") && !strings.Contains(req["raw"].(string), " :me!") {
				t.Fatalf("our request's echo = %v", req)
			}
			e.feed("undernet", ":peer!p@h NOTICE me :\x01VERSION Foo 1.0\x01")
			ircLineOf(t, c, mark, "NOTICE me :\x01VERSION Foo 1.0\x01")
			e.feed("undernet", ":peer!p@h PRIVMSG me :\x01TIME\x01") // relayed (the default mode)
			ircLineOf(t, c, mark, "PRIVMSG me :\x01TIME\x01")
			time.Sleep(100 * time.Millisecond)
			for _, text := range []string{"PRIVMSG peer :\x01VERSION\x01", "VERSION Foo 1.0", "\x01TIME\x01"} {
				if n := linesWith(c, mark, text); n != 1 {
					t.Fatalf("%q delivered %d times, want once", text, n)
				}
			}
		})
	}
}

// A connection subscribed to the peer's query gets each CTCP line once, not once per tier.
func TestCTCPToASubscribedQueryIsDeliveredOnce(t *testing.T) {
	e := newTestEnv(t, "undernet")
	e.feed("undernet", ":peer!p@h PRIVMSG me :hello") // the query exists in history
	c := e.syncedClient("ios-1", "undernet", "peer")
	mark := c.count()
	e.feed("undernet", ":peer!p@h NOTICE me :\x01PING 123\x01")
	ircLineOf(t, c, mark, "\x01PING 123\x01")
	time.Sleep(100 * time.Millisecond)
	if n := linesWith(c, mark, "\x01PING 123\x01"); n != 1 {
		t.Fatalf("delivered %d times, want once", n)
	}
}
