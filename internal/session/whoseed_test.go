package session

import (
	"strings"
	"testing"
	"time"
)

// Owner ruling 2026-11-06: GoBNC seeds every joined channel's member flags once, with a silent
// WHOX per channel at the seed pace, after the brain starts (or a module first subscribes) and
// after each network (re)connect; a channel a client WHO'd within the interval is skipped; then
// the who_poll_minutes rule applies at its own pace. Undernet (ircu2) offers no away-notify, so
// WHO is the only source of away there.

var seedPolicy = WHOPoll{Interval: 45 * time.Minute, SeedPace: 10 * time.Second, Pace: 90 * time.Second}

// pollToken is the token of the last WHOX the Session wrote for channel.
func pollToken(t *testing.T, w *writeRec, channel string) string {
	t.Helper()
	got := w.lines()
	prefix := "WHO " + channel + " %" + whoPollFields + ","
	for i := len(got) - 1; i >= 0; i-- {
		if strings.HasPrefix(got[i], prefix) {
			return strings.TrimPrefix(got[i], prefix)
		}
	}
	t.Fatalf("no WHOX for %s in %q", channel, got)
	return ""
}

func TestWHOSeedIsPacedAndThenFollowsTheInterval(t *testing.T) {
	s, _, _, w := newMetaSession(t) // #c and #d, never WHO'd
	feed(s, ":me!u@h JOIN #e", ":srv 353 me = #e :me", ":srv 366 me #e :End of /NAMES list.")
	t0 := time.Now()
	poll := func(at time.Time) string { // one poll, answered (an unanswered one stays in flight)
		p := s.PollWHO(at, seedPolicy)
		if p != "" {
			feed(s, ":srv 315 me "+p+" :End of /WHO list.")
		}
		return p
	}
	if p := poll(t0); p != "#c" {
		t.Fatalf("first seed polled %q, want #c", p)
	}
	if p := poll(t0.Add(5 * time.Second)); p != "" {
		t.Fatalf("5 s later polled %q, want nothing (seed pace 10 s)", p)
	}
	if p := poll(t0.Add(10 * time.Second)); p != "#d" {
		t.Fatalf("10 s later polled %q, want #d", p)
	}
	if p := poll(t0.Add(20 * time.Second)); p != "#e" {
		t.Fatalf("20 s later polled %q, want #e", p)
	}
	// Seeded: nothing is due until the interval, and then one stale channel per 90 s.
	if p := poll(t0.Add(30 * time.Second)); p != "" {
		t.Fatalf("after the seed polled %q, want nothing", p)
	}
	at := t0.Add(46 * time.Minute)
	if p := poll(at); p != "#c" {
		t.Fatalf("after the interval polled %q, want #c", p)
	}
	if p := poll(at.Add(10 * time.Second)); p != "" {
		t.Fatalf("stale channels went at the seed pace: %q", p)
	}
	if p := poll(at.Add(90 * time.Second)); p != "#d" {
		t.Fatalf("90 s later polled %q, want #d", p)
	}
	if n := len(w.lines()); n != 5 {
		t.Fatalf("%d uplink writes, want 5: %q", n, w.lines())
	}
}

// A channel a client WHO'd within the interval is not seeded; the seed takes the others.
func TestWHOSeedSkipsAChannelAClientWHOd(t *testing.T) {
	s, d, _, w := newMetaSession(t)
	_, tok, _ := s.tracker.Begin(BeginOpts{Client: d.ID(), Cmd: "WHO", PreferWHOX: true, WHOMask: "#c", WHOXFields: "cuhnf"})
	s.tracker.SetWHOXClientFix(tok, true, "")
	feed(s, ":srv 354 me "+tok+" #c al a.example alice H", ":srv 315 me #c :End of /WHO list.")
	t0 := time.Now()
	if p := s.PollWHO(t0, seedPolicy); p != "#d" {
		t.Fatalf("seed polled %q, want #d (#c was just WHO'd by a client)", p)
	}
	if p := s.PollWHO(t0.Add(time.Minute), seedPolicy); p != "" {
		t.Fatalf("polled %q, want nothing", p)
	}
	if got := w.lines(); len(got) != 1 || !strings.HasPrefix(got[0], "WHO #d ") {
		t.Fatalf("uplink writes = %q", got)
	}
}

// A reconnect forgets which channels were WHO'd: the rejoined channels are seeded again.
func TestWHOSeedRunsAgainAfterReconnect(t *testing.T) {
	s, _, _, w := newMetaSession(t)
	t0 := time.Now()
	s.PollWHO(t0, seedPolicy)
	s.PollWHO(t0.Add(10*time.Second), seedPolicy)
	if p := s.PollWHO(t0.Add(20*time.Second), seedPolicy); p != "" {
		t.Fatalf("seeded network polled %q", p)
	}
	s.HandleDisconnect(nil)
	s.mu.Lock()
	s.registered = true // re-registered (the 001 burst is not this test's subject)
	s.isupport.WHOX = true
	s.mu.Unlock()
	feed(s, ":me!u@h JOIN #c", ":srv 353 me = #c :@me alice", ":srv 366 me #c :End of /NAMES list.")
	w.reset()
	if p := s.PollWHO(t0.Add(30*time.Second), seedPolicy); p != "#c" {
		t.Fatalf("after reconnect polled %q, want #c seeded again", p)
	}
	if got := w.lines(); len(got) != 1 {
		t.Fatalf("uplink writes = %q", got)
	}
}

// The seed's WHOX asks for the flags field, and its ircu-shaped replies set away (G), oper (*)
// and bot (B), self's included, and clear them (H, no '*').
func TestWHOSeedRepliesSetAwayOperBot(t *testing.T) {
	s, _, sink, w := newMetaSession(t)
	if !strings.Contains(whoPollFields, "f") || !strings.Contains(whoPollFields, "n") {
		t.Fatalf("whoPollFields %q lacks the flags or the nick", whoPollFields)
	}
	if p := s.PollWHO(time.Now(), seedPolicy); p != "#c" {
		t.Fatalf("polled %q", p)
	}
	tok := pollToken(t, w, "#c")
	before := len(sink.lines())
	feed(s, ":srv 354 me "+tok+" #c al a.example alice G* 0",
		":srv 354 me "+tok+" #c b b.example bob HB bobacct",
		":srv 354 me "+tok+" #c u h me H*@ 0",
		":srv 315 me #c :End of /WHO list.")
	if n := len(sink.lines()); n != before {
		t.Fatalf("modules got %d line events from the seed's replies", n-before)
	}
	if a := s.userForTest("alice"); !a.Away || !a.Oper || a.Bot || a.Account != "" {
		t.Fatalf("alice = %+v", a)
	}
	if b := s.userForTest("bob"); b.Away || b.Oper || !b.Bot || b.Account != "bobacct" {
		t.Fatalf("bob = %+v", b)
	}
	if me := s.userForTest("me"); !me.Oper || me.Away {
		t.Fatalf("me = %+v", me)
	}
	// A later reply clears what is no longer true.
	p := s.PollWHO(time.Now().Add(46*time.Minute), WHOPoll{Interval: 45 * time.Minute})
	if p != "#d" {
		t.Fatalf("polled %q, want #d", p)
	}
	tok = pollToken(t, w, "#d")
	feed(s, ":srv 354 me "+tok+" #d al a.example alice H 0", ":srv 315 me #d :End of /WHO list.")
	if a := s.userForTest("alice"); a.Away || a.Oper {
		t.Fatalf("alice after H = %+v", a)
	}
}
