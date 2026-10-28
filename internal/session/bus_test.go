package session

import (
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// recSink records every delivered event.
type recSink struct {
	mu   sync.Mutex
	evs  []any
	dead bool
}

func (r *recSink) Deliver(ev any) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.dead {
		return false
	}
	r.evs = append(r.evs, ev)
	return true
}

func (r *recSink) events() []any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]any(nil), r.evs...)
}

func (r *recSink) lines() []wire.LineEvent {
	var out []wire.LineEvent
	for _, ev := range r.events() {
		if l, ok := ev.(wire.LineEvent); ok {
			out = append(out, l)
		}
	}
	return out
}

// newBusSession is a registered Session as "me" in #c, with no store.
func newBusSession(t *testing.T) *Session {
	t.Helper()
	s := New(store.Network{ID: 1, Name: "n", Nick: "me"}, nil, nil, nil, nil)
	s.registered = true
	s.HandleLine([]byte(":me!u@h JOIN #c"), 1)
	return s
}

func feed(s *Session, lines ...string) {
	for i, l := range lines {
		s.HandleLine([]byte(l), uint64(100+i))
	}
}

func TestBusDeliversInLineOrderWithSeq(t *testing.T) {
	s := newBusSession(t)
	sink := &recSink{}
	s.Subscribe(1, sink, nil, nil)
	feed(s, ":a!u@h JOIN #c", ":a!u@h PRIVMSG #c :one", ":a!u@h PART #c")
	got := sink.lines()
	if len(got) != 3 {
		t.Fatalf("%d line events, want 3: %+v", len(got), got)
	}
	for i, want := range []string{"JOIN", "PRIVMSG", "PART"} {
		if got[i].Seq != uint64(i+1) || !strings.Contains(got[i].Raw, want) || got[i].Network != 1 {
			t.Fatalf("event %d = %+v, want seq %d with %s", i, got[i], i+1, want)
		}
	}
}

func TestBusRemovesDeadSink(t *testing.T) {
	s := newBusSession(t)
	dead := &recSink{dead: true}
	live := &recSink{}
	s.Subscribe(1, dead, nil, nil)
	s.Subscribe(2, live, nil, nil)
	feed(s, ":a!u@h JOIN #c")
	s.busMu.Lock()
	_, deadLeft := s.sinks[1]
	_, liveLeft := s.sinks[2]
	s.busMu.Unlock()
	if deadLeft || !liveLeft {
		t.Fatalf("dead sink left=%v live sink left=%v", deadLeft, liveLeft)
	}
}

func TestBusKindsFilter(t *testing.T) {
	s := newBusSession(t)
	sink := &recSink{}
	s.Subscribe(1, sink, []string{wire.LineOther}, nil)
	other := &recSink{}
	s.Subscribe(2, other, []string{"no-such-kind"}, nil)
	feed(s, ":a!u@h JOIN #c")
	if len(sink.lines()) != 1 || len(other.lines()) != 0 {
		t.Fatalf("matching sink got %d, filtered sink got %d", len(sink.lines()), len(other.lines()))
	}
}

func (s *Session) membersOf(channel string) map[string]bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := map[string]bool{}
	if ch := s.channels[s.isupport.CaseMapping.Canonical(channel)]; ch != nil {
		for n := range ch.Members {
			out[n] = true
		}
	}
	return out
}

func applyJoinPart(m map[string]bool, raw string) {
	msg, err := irc.Parse(raw)
	if err != nil {
		return
	}
	nick := strings.ToLower(msg.Nick())
	switch msg.Command {
	case "JOIN":
		m[nick] = true
	case "PART":
		delete(m, nick)
	}
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestSubscribeAtomicWithLineStream: whatever moment a module subscribes at, its snapshot plus
// the events it gets afterwards must reproduce the final channel roster — no event between the
// snapshot and the registration lost, none counted in both.
func TestSubscribeAtomicWithLineStream(t *testing.T) {
	s := newBusSession(t)
	rng := rand.New(rand.NewSource(1))
	in := map[int]bool{}
	var lines []string
	for i := 0; i < 2000; i++ {
		n := rng.Intn(40)
		if in[n] {
			lines = append(lines, fmt.Sprintf(":p%d!u@h PART #c", n))
		} else {
			lines = append(lines, fmt.Sprintf(":p%d!u@h JOIN #c", n))
		}
		in[n] = !in[n]
	}

	type sub struct {
		snap map[string]bool
		sink *recSink
	}
	var subs []sub
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for id := uint64(1); id <= 300; id++ {
			select {
			case <-done:
				return
			default:
			}
			runtime.Gosched()
			sink := &recSink{}
			snap := s.Subscribe(id, sink, nil, func() any { return s.membersOf("#c") }).(map[string]bool)
			subs = append(subs, sub{snap: snap, sink: sink})
		}
	}()
	feed(s, lines...)
	close(done)
	wg.Wait()

	final := keys(s.membersOf("#c"))
	if len(subs) < 10 {
		t.Fatalf("only %d subscriptions raced the stream", len(subs))
	}
	for i, sb := range subs {
		m := sb.snap
		for _, ev := range sb.sink.lines() {
			applyJoinPart(m, ev.Raw)
		}
		if got := keys(m); strings.Join(got, ",") != strings.Join(final, ",") {
			t.Fatalf("subscription %d: snapshot + events = %v, final roster = %v", i, got, final)
		}
	}
}
