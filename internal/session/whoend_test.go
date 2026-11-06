package session

import (
	"testing"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// Member metadata a WHO teaches reaches modules: before this, the end of a WHO emitted nothing,
// so a lite client subscribed before the replies never saw the away/oper/bot flags they set.

// The WHO's end sends modules the channel's members with what the replies taught (a lite
// client already subscribed gets them as a buffer_state), for a client's WHO
// (GoBNC's own poll ends with the same 315, routed to nobody).
func TestWHOEndEmitsMembersToModules(t *testing.T) {
	s, d, sink, _ := newMetaSession(t)
	membersEvents := func() []wire.StateEvent {
		var out []wire.StateEvent
		for _, ev := range sink.events() {
			if st, ok := ev.(wire.StateEvent); ok && st.Kind == wire.StateMembers {
				out = append(out, st)
			}
		}
		return out
	}
	before := len(membersEvents())
	_, tok, _ := s.tracker.Begin(BeginOpts{Client: d.ID(), Cmd: "WHO", PreferWHOX: true, WHOMask: "#c", WHOXFields: "cuhnfa"})
	s.tracker.SetWHOXClientFix(tok, true, "")
	feed(s, ":srv 354 me "+tok+" #c al a.example alice G* 0")
	if n := len(membersEvents()); n != before {
		t.Fatalf("members emitted before the WHO's end (%d)", n-before)
	}
	feed(s, ":srv 315 me #c :End of /WHO list.")
	evs := membersEvents()[before:]
	if len(evs) != 1 || evs[0].Buffer != "#c" || !evs[0].Complete {
		t.Fatalf("members events = %+v", evs)
	}
	var alice *wire.Member
	for i := range evs[0].Members {
		if evs[0].Members[i].Nick == "alice" {
			alice = &evs[0].Members[i]
		}
	}
	if alice == nil || !alice.Away || !alice.Oper || alice.Host != "a.example" || alice.User != "al" {
		t.Fatalf("alice = %+v", alice)
	}

	// A client's plain WHO of #d: its replies go to the client, and the members to modules.
	_, _, _ = s.tracker.Begin(BeginOpts{Client: d.ID(), Cmd: "WHO", WHOMask: "#d"})
	before = len(membersEvents())
	feed(s, ":srv 352 me #d al a.example srv alice H :0 Alice", ":srv 315 me #d :End of /WHO list.")
	evs = membersEvents()[before:]
	if len(evs) != 1 || evs[0].Buffer != "#d" {
		t.Fatalf("members events after a client's WHO = %+v", evs)
	}
	// A WHO of a channel we are not in names nothing to send.
	before = len(membersEvents())
	feed(s, ":srv 352 me #elsewhere x x.example srv xavier H :0 X", ":srv 315 me #elsewhere :End of /WHO list.")
	if n := len(membersEvents()); n != before {
		t.Fatalf("a WHO of a channel we are not in emitted members")
	}
}

// away-notify: an AWAY line sets and clears the member's away flag, and is a line event modules
// get (a lite client subscribed to a shared channel gets the irc_line).
func TestAwayNotifyUpdatesMember(t *testing.T) {
	s, _, sink, _ := newMetaSession(t)
	before := len(sink.lines())
	feed(s, ":alice!al@a.example AWAY :lunch")
	snap := s.SubscribeWithSnapshot(9, &recSink{}, nil, nil)
	if m, _ := memberOf(snap, "#c", "alice"); !m.Away {
		t.Fatalf("alice after AWAY = %+v", m)
	}
	lines := sink.lines()[before:]
	if len(lines) != 1 || lines[0].Kind != wire.LineAway || len(lines[0].Buffers) != 2 {
		t.Fatalf("AWAY line events = %+v", lines)
	}
	feed(s, ":alice!al@a.example AWAY")
	snap = s.SubscribeWithSnapshot(10, &recSink{}, nil, nil)
	if m, _ := memberOf(snap, "#c", "alice"); m.Away {
		t.Fatalf("alice after AWAY with no text = %+v", m)
	}
}
