package retecloud

import (
	"sort"
	"testing"

	"github.com/MasterBOFH/GoBNC/modules/retecloud/push"
)

// byID orders relayed messages by their line id (requests run concurrently, so they reach the
// relay in any order).
func byID(in []relayed) []relayed {
	out := append([]relayed(nil), in...)
	sort.Slice(out, func(i, j int) bool { return out[i].content["id"].(float64) < out[j].content["id"].(float64) })
	return out
}

// A3033: a spammed query is one conversation: every push carries its collapse id, only the
// first sounds, the buffer's cap holds the rest back, and the next push after the window
// reports what was held back.
func TestPushCoalescesASpammedQuery(t *testing.T) {
	e, relay, clock := newPushEnv(t, "undernet")
	e.phone(relay, "ios-1", "rrk_1", "undernet")

	mark := relay.count()
	for i := 0; i < push.CapPerMinute+5; i++ {
		e.feed("undernet", ":spammer!s@h PRIVMSG me :spam")
	}
	got := byID(relay.waitType(mark, "message", push.CapPerMinute))
	relay.none(mark+push.CapPerMinute, "message", "over the buffer's cap")
	first := got[0].req
	if first.CollapseID == "" || first.CollapseID == first.EventID || first.Silent {
		t.Fatalf("first push %+v: want a conversation collapse id, sounding", first)
	}
	for i, r := range got {
		if r.req.CollapseID != first.CollapseID {
			t.Fatalf("push %d: collapse id %q, want %q", i, r.req.CollapseID, first.CollapseID)
		}
		if i > 0 && !r.req.Silent {
			t.Fatalf("push %d inside the cool-down sounds", i)
		}
		if _, ok := r.content["skipped"]; ok {
			t.Fatalf("push %d carries skipped: %v", i, r.content)
		}
	}

	clock.advance(push.CoolDown)
	mark = relay.count()
	e.feed("undernet", ":SPAMMER!s@h PRIVMSG me :still here")
	last := relay.waitType(mark, "message", 1)[0]
	if last.req.CollapseID != first.CollapseID || last.req.Silent || last.content["skipped"] != float64(5) {
		t.Fatalf("after the window: %+v %v", last.req, last.content)
	}
}

// A3033: in a channel each sender is its own conversation; the cool-down is the buffer's.
func TestPushChannelHighlightsCollapsePerSender(t *testing.T) {
	e, relay, _ := newPushEnv(t, "undernet")
	e.feed("undernet", ":me!u@h JOIN #rete")
	e.phone(relay, "ios-1", "rrk_1", "undernet")

	mark := relay.count()
	e.feed("undernet", ":alice!a@h PRIVMSG #rete :me: one")
	relay.waitType(mark, "message", 1)
	e.feed("undernet", ":bob!b@h PRIVMSG #rete :me: two")
	relay.waitType(mark, "message", 2)
	e.feed("undernet", ":alice!a@h PRIVMSG #rete :me: three")
	relay.waitType(mark, "message", 3)
	e.feed("undernet", ":alice!a@h PRIVMSG me :a query is its own buffer")
	got := byID(relay.waitType(mark, "message", 4))
	a1, b, a2, q := got[0].req, got[1].req, got[2].req, got[3].req
	if a1.CollapseID != a2.CollapseID || a1.CollapseID == b.CollapseID || q.CollapseID == a1.CollapseID {
		t.Fatalf("collapse ids %q %q %q %q", a1.CollapseID, b.CollapseID, a2.CollapseID, q.CollapseID)
	}
	if a1.Silent || !b.Silent || !a2.Silent || q.Silent {
		t.Fatalf("silent %v %v %v %v: only the first push per buffer sounds", a1.Silent, b.Silent, a2.Silent, q.Silent)
	}
}
