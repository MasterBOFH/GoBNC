package retecloud

import (
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/session"
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

// End to end, ircu2-shaped (Undernet offers no away-notify, so WHO is the only source of away):
// a phone subscribed to #rete before GoBNC's seed; the seed's silent WHOX goes to the ircd over
// the keeper; its replies reach no client as lines, and the phone gets a buffer_state whose
// members carry away, oper and bot.
func TestSeedWHOXReachesSubscribedLiteEndToEnd(t *testing.T) {
	e := newTestEnv(t)
	_, ircd := e.addUplinkNetwork("undernet", false)
	ircd.setAnswer(func(msg irc.Message, nick string, send func(string)) {
		if msg.Command != "WHO" || msg.Param(0) != "#rete" || !strings.HasPrefix(msg.Param(1), "%") {
			return
		}
		spec := strings.SplitN(strings.TrimPrefix(msg.Param(1), "%"), ",", 2)
		if spec[0] != "tcuhnfa" || len(spec) != 2 {
			send(":fake.example 315 " + nick + " #rete :End of /WHO list.")
			return
		}
		tok := spec[1]
		send(":fake.example 354 " + nick + " " + tok + " #rete p peer.example peer G* 0")
		send(":fake.example 354 " + nick + " " + tok + " #rete b bot.example botty HB 0")
		send(":fake.example 354 " + nick + " " + tok + " #rete u h " + nick + " H@ 0")
		send(":fake.example 315 " + nick + " #rete :End of /WHO list.")
	})
	ircd.push(":fake.example 005 me WHOX PREFIX=(ov)@+ CHANTYPES=# :are supported by this server",
		":me!u@h JOIN #rete", ":fake.example 353 me = #rete :@me peer botty",
		":fake.example 366 me #rete :End of /NAMES list.")
	c := e.syncedClient("ios-1", "undernet")
	var sub map[string]any
	deadline := time.Now().Add(5 * time.Second)
	for { // until the JOIN burst has landed
		mark := c.count()
		rid := c.request("subscribe_buffer", map[string]any{"buffer": "#rete", "history": 0, "members": "full"}, e.sid("undernet"))
		sub = c.waitFor(mark, 5*time.Second, reply(rid, "subscribe_buffer_success", "subscribe_buffer_error"))
		if memberEntry(t, sub, "botty") != nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if p := memberEntry(t, sub, "peer"); p == nil || p["away"] != false || p["oper"] != false {
		t.Fatalf("peer at subscribe = %v (%v)", p, sub)
	}

	mark := c.count()
	pol := session.WHOPoll{Interval: 45 * time.Minute, SeedPace: 10 * time.Second, Pace: 90 * time.Second}
	if p := e.session("undernet").PollWHO(time.Now(), pol); p != "#rete" {
		t.Fatalf("seed polled %q", p)
	}
	f := c.waitFor(mark, 5*time.Second, func(f map[string]any) bool {
		if f["type"] != "buffer_state" {
			return false
		}
		p := memberEntry(t, f, "peer")
		return p != nil && p["away"] == true
	})
	if p := memberEntry(t, f, "peer"); p["oper"] != true || p["bot"] != false || p["hostname"] != "peer.example" {
		t.Fatalf("peer = %v", p)
	}
	if b := memberEntry(t, f, "botty"); b["bot"] != true || b["away"] != false || b["oper"] != false {
		t.Fatalf("botty = %v", b)
	}
	if me := memberEntry(t, f, "me"); me["modes"] != "o" || me["oper"] != false {
		t.Fatalf("me = %v", me)
	}
	got := ircd.received("WHO")
	if len(got) != 1 {
		t.Fatalf("ircd got %q", got)
	}
	if m, _ := irc.Parse(got[0]); m.Param(0) != "#rete" || !strings.HasPrefix(m.Param(1), "%tcuhnfa,") {
		t.Fatalf("ircd got %q", got)
	}
	time.Sleep(200 * time.Millisecond)
	for _, fr := range c.since(mark) {
		if fr["type"] == "irc_line" {
			if raw, _ := fr["data"].(map[string]any)["raw"].(string); strings.Contains(raw, " 354 ") || strings.Contains(raw, " 315 ") {
				t.Fatalf("the seed's reply reached the phone: %q", raw)
			}
		}
	}
}

// On a network with away-notify, an AWAY line reaches the phone subscribed to a shared channel
// as an irc_line (Rete applies it to the person); the next buffer_state carries it as well.
func TestAwayNotifyLineReachesSubscribedLite(t *testing.T) {
	e := newTestEnv(t, "libera")
	e.feed("libera", ":me!u@cloak JOIN #rete", ":srv.example 353 me = #rete :@me peer",
		":srv.example 366 me #rete :End")
	c := e.dial(false)
	c.authenticate("ios-1")
	c.liteSync(e.sid("libera"), "ios-1", nil)
	mark := c.count()
	rid := c.request("subscribe_buffer", map[string]any{"buffer": "#rete", "history": 0, "members": "full"}, e.sid("libera"))
	c.waitFor(mark, 5*time.Second, reply(rid, "subscribe_buffer_success"))
	mark = c.count()
	e.feed("libera", ":peer!p@peer.example AWAY :lunch")
	c.waitFor(mark, 5*time.Second, func(f map[string]any) bool {
		raw, _ := f["data"].(map[string]any)["raw"].(string)
		return f["type"] == "irc_line" && strings.Contains(raw, " AWAY ")
	})
	mark = c.count()
	rid = c.request("subscribe_buffer", map[string]any{"buffer": "#rete", "history": 0, "members": "full"}, e.sid("libera"))
	sub := c.waitFor(mark, 5*time.Second, reply(rid, "subscribe_buffer_success"))
	if p := memberEntry(t, sub, "peer"); p["away"] != true {
		t.Fatalf("peer = %v", p)
	}
}
