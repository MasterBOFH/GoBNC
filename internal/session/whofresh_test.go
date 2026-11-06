package session

import (
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// Owner ruling 2026-11-06: a WHO counts as polling channel X (so GoBNC's own poll and seed skip
// X for the interval) only if its target was X itself and it carried flags: a WHOX must have
// asked for 'f', a plain WHO (352) always does. A WHO of a nick, host or mask names one channel
// per reply but covers only that member. Every reply still teaches the users it names, and the
// WHO's end still sends the named channels' members to modules.

// whoFresh reports whether channel counts as recently WHO'd.
func (s *Session) whoFresh(channel string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.lastWHO[s.isupport.CaseMapping.Canonical(channel)]
	return ok
}

// clientWHOX begins a client WHOX of mask with fields (as forwardSolicitous does) and returns
// its uplink token.
func clientWHOX(s *Session, d *fakeDL, mask, fields string) string {
	_, tok, _ := s.tracker.Begin(BeginOpts{Client: d.ID(), Cmd: "WHO", PreferWHOX: true,
		WHOMask: s.isupport.CaseMapping.Canonical(mask), WHOXFields: fields})
	s.tracker.SetWHOXClientFix(tok, true, "")
	return tok
}

func TestWHOXWithoutFlagsDoesNotMakeChannelFresh(t *testing.T) {
	s, d, _, _ := newMetaSession(t)
	tok := clientWHOX(s, d, "#c", "cn")
	feed(s, ":srv 354 me "+tok+" #c alice", ":srv 315 me #c :End of /WHO list.")
	if s.whoFresh("#c") {
		t.Fatal("a WHOX without 'f' made #c fresh")
	}
}

func TestWHOXWithFlagsMakesChannelFresh(t *testing.T) {
	s, d, _, _ := newMetaSession(t)
	tok := clientWHOX(s, d, "#C", "cnf")
	feed(s, ":srv 354 me "+tok+" #c alice G", ":srv 315 me #c :End of /WHO list.")
	if !s.whoFresh("#c") {
		t.Fatal("WHO #c with fields cnf did not make #c fresh")
	}
	if !s.userForTest("alice").Away {
		t.Fatal("the reply's flags were not applied")
	}
}

func TestPlainWHOOfChannelMakesItFresh(t *testing.T) {
	s, d, _, _ := newMetaSession(t)
	s.tracker.Begin(BeginOpts{Client: d.ID(), Cmd: "WHO", WHOMask: "#c"})
	feed(s, ":srv 352 me #c al a.example srv alice H :0 Alice", ":srv 315 me #c :End of /WHO list.")
	if !s.whoFresh("#c") {
		t.Fatal("a plain WHO #c did not make #c fresh")
	}
}

// A WHO of a host mask teaches the user and still sends #c's members to modules at its end,
// but polled only alice, not #c.
func TestWHOXOfMaskTeachesUserButNotFreshness(t *testing.T) {
	s, d, sink, _ := newMetaSession(t)
	tok := clientWHOX(s, d, "*.example", "cnfu")
	before := len(sink.events())
	feed(s, ":srv 354 me "+tok+" #c al alice G*", ":srv 315 me *.example :End of /WHO list.")
	if a := s.userForTest("alice"); a.User != "al" || !a.Away || !a.Oper {
		t.Fatalf("alice = %+v", a)
	}
	if s.whoFresh("#c") {
		t.Fatal("a WHO of a host mask made #c fresh")
	}
	emitted := false
	for _, ev := range sink.events()[before:] {
		if st, ok := ev.(wire.StateEvent); ok && st.Kind == wire.StateMembers && st.Buffer == "#c" {
			emitted = true
		}
	}
	if !emitted {
		t.Fatal("the mask WHO's end did not send #c's members to modules")
	}
}

func TestPlainWHOOfNickTeachesUserButNotFreshness(t *testing.T) {
	s, d, _, _ := newMetaSession(t)
	s.tracker.Begin(BeginOpts{Client: d.ID(), Cmd: "WHO", WHOMask: "alice"})
	feed(s, ":srv 352 me #c al a.example srv alice G :0 Alice", ":srv 315 me alice :End of /WHO list.")
	if a := s.userForTest("alice"); !a.Away || a.Host != "a.example" {
		t.Fatalf("alice = %+v", a)
	}
	if s.whoFresh("#c") {
		t.Fatal("WHO alice made #c fresh")
	}
}

// A labeled WHO's replies are known by their label: its target decides, as for any other WHO.
func TestLabeledWHOCountsByItsTarget(t *testing.T) {
	s, d, _, _ := newMetaSession(t)
	lbl, _, _ := s.tracker.Begin(BeginOpts{Client: d.ID(), Cmd: "WHO", PreferLabel: true, WHOMask: "alice"})
	feed(s, "@label="+lbl+" :srv 352 me #c al a.example srv alice G :0 Alice")
	if s.whoFresh("#c") {
		t.Fatal("a labeled WHO alice made #c fresh")
	}
	lbl, _, _ = s.tracker.Begin(BeginOpts{Client: d.ID(), Cmd: "WHO", PreferLabel: true, WHOMask: "#d"})
	feed(s, "@label="+lbl+" :srv 352 me #d al a.example srv alice H :0 Alice")
	if !s.whoFresh("#d") {
		t.Fatal("a labeled WHO #d did not make #d fresh")
	}
}

// A reply GoBNC cannot tie to a request (no tracked WHO, or a labeled batch whose lines carry
// no label) teaches the user but polls nothing. A batch's line is not the plain WHO queue's,
// even while that queue's head is a WHO of the same channel.
func TestUntrackedWHOReplyIsNotAPoll(t *testing.T) {
	s, d, _, _ := newMetaSession(t)
	feed(s, ":srv 352 me #c al a.example srv alice G :0 Alice")
	if !s.userForTest("alice").Away {
		t.Fatal("an untracked reply's flags were not applied")
	}
	s.tracker.Begin(BeginOpts{Client: d.ID(), Cmd: "WHO", WHOMask: "#d"})
	feed(s, "@batch=b1 :srv 352 me #d al a.example srv alice H :0 Alice")
	if s.userForTest("alice").Away {
		t.Fatal("a batched reply's flags were not applied")
	}
	if s.whoFresh("#c") || s.whoFresh("#d") {
		t.Fatal("an untracked WHO reply made its channel fresh")
	}
}

// Two clients' identical WHOX share one uplink request: the shared request's target and fields
// decide, for a channel and for a mask alike.
func TestCoalescedWHOXCountsByTheSharedRequest(t *testing.T) {
	s, d, _, _ := newMetaSession(t)
	d2 := &fakeDL{id: "d2", caps: map[string]bool{}}
	if err := s.Attach(d2); err != nil {
		t.Fatal(err)
	}
	tok := clientWHOX(s, d, "#c", "cnf")
	if _, tok2, writeNow := s.tracker.Begin(BeginOpts{Client: d2.ID(), Cmd: "WHO", PreferWHOX: true,
		WHOMask: "#c", WHOXFields: "cnf"}); writeNow || tok2 != "" {
		t.Fatalf("second WHOX of #c was not coalesced (writeNow %v, tok %q)", writeNow, tok2)
	}
	feed(s, ":srv 354 me "+tok+" #c alice H", ":srv 315 me #c :End of /WHO list.")
	if !s.whoFresh("#c") {
		t.Fatal("a coalesced WHO #c with fields cnf did not make #c fresh")
	}

	tok = clientWHOX(s, d, "*.example", "cnf")
	if _, _, writeNow := s.tracker.Begin(BeginOpts{Client: d2.ID(), Cmd: "WHO", PreferWHOX: true,
		WHOMask: "*.example", WHOXFields: "cnf"}); writeNow {
		t.Fatal("second WHOX of *.example was not coalesced")
	}
	feed(s, ":srv 354 me "+tok+" #d alice G", ":srv 315 me *.example :End of /WHO list.")
	if s.whoFresh("#d") {
		t.Fatal("a coalesced mask WHOX made #d fresh")
	}
}

// The seed still polls every channel the clients' WHOs did not really poll.
func TestSeedPollsChannelsNotReallyPolled(t *testing.T) {
	s, d, _, w := newMetaSession(t)
	tok := clientWHOX(s, d, "#c", "cn")
	feed(s, ":srv 354 me "+tok+" #c alice", ":srv 315 me #c :End of /WHO list.")
	tok = clientWHOX(s, d, "*.example", "cnfu")
	feed(s, ":srv 354 me "+tok+" #d al alice H", ":srv 315 me *.example :End of /WHO list.")
	s.tracker.Begin(BeginOpts{Client: d.ID(), Cmd: "WHO", WHOMask: "alice"})
	feed(s, ":srv 352 me #c al a.example srv alice H :0 Alice", ":srv 315 me alice :End of /WHO list.")
	w.reset()
	t0 := time.Now()
	if p := s.PollWHO(t0, seedPolicy); p != "#c" {
		t.Fatalf("seed polled %q, want #c", p)
	}
	feed(s, ":srv 315 me #c :End of /WHO list.")
	if p := s.PollWHO(t0.Add(10*time.Second), seedPolicy); p != "#d" {
		t.Fatalf("seed polled %q, want #d", p)
	}
}
