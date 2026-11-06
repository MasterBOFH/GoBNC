package session

import (
	"reflect"
	"testing"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// A2992: GoBNC tracks the server's SILENCE list per network for gobnc-retecloud's sync: it
// queries the list silently after registration and follows SILENCE changes.

func silenceEvents(sink *recSink) [][]string {
	var out [][]string
	for _, ev := range sink.events() {
		if st, ok := ev.(wire.StateEvent); ok && st.Kind == wire.StateSilence {
			out = append(out, append([]string{}, st.Silence...))
		}
	}
	return out
}

func TestSilenceListQueriedSilently(t *testing.T) {
	s, d, sink, w := newMetaSession(t)
	s.RefreshSilence()
	if got := w.lines(); !reflect.DeepEqual(got, []string{"SILENCE"}) {
		t.Fatalf("uplink writes = %q", got)
	}
	before := len(sink.lines())
	feed(s, ":srv 271 me me *!*@spam.example", ":srv 271 me me troll!*@*", ":srv 272 me :End of Silence List")
	if sent := d.snapshot(); len(sent) != 0 {
		t.Fatalf("an IRC client got the silent query's replies: %+v", sent)
	}
	if n := len(sink.lines()); n != before {
		t.Fatalf("modules got %d line events from the silent query", n-before)
	}
	want := []string{"*!*@spam.example", "troll!*@*"}
	if got := silenceEvents(sink); !reflect.DeepEqual(got, [][]string{want}) {
		t.Fatalf("silence events = %q", got)
	}
	snap := s.SubscribeWithSnapshot(9, &recSink{}, nil, nil)
	if !reflect.DeepEqual(snap.Silence, want) {
		t.Fatalf("snapshot silence = %q", snap.Silence)
	}
}

func TestSilenceEmptyListAndNoSILENCESupport(t *testing.T) {
	s, _, sink, w := newMetaSession(t)
	s.mu.Lock()
	s.silence = []string{"*!*@stale"}
	s.mu.Unlock()
	s.RefreshSilence()
	feed(s, ":srv 272 me :End of Silence List")
	if got := silenceEvents(sink); !reflect.DeepEqual(got, [][]string{{}}) {
		t.Fatalf("silence events = %q, want one empty list", got)
	}
	s.mu.Lock()
	delete(s.isupport.Raw, "SILENCE")
	s.mu.Unlock()
	w.reset()
	s.RefreshSilence()
	if got := w.lines(); len(got) != 0 {
		t.Fatalf("a network without SILENCE was queried: %q", got)
	}
	snap := s.SubscribeWithSnapshot(9, &recSink{}, nil, nil)
	if snap.Silence == nil || len(snap.Silence) != 0 {
		t.Fatalf("snapshot silence = %#v, want []", snap.Silence)
	}
}

// A live change (the server's SILENCE echo) updates the list and reaches modules as a state
// event ahead of the line itself.
func TestSilenceLiveChange(t *testing.T) {
	s, d, sink, _ := newMetaSession(t)
	feed(s, ":me!u@h SILENCE +*!*@a.example", ":me!u@h SILENCE +b!*@*,-*!*@a.example")
	if got := silenceEvents(sink); !reflect.DeepEqual(got, [][]string{{"*!*@a.example"}, {"b!*@*"}}) {
		t.Fatalf("silence events = %q", got)
	}
	evs := sink.events()
	var kinds []string
	for _, ev := range evs {
		switch e := ev.(type) {
		case wire.StateEvent:
			kinds = append(kinds, "state")
		case wire.LineEvent:
			if e.Kind == wire.LineOther {
				kinds = append(kinds, "line")
			}
		}
	}
	if !reflect.DeepEqual(kinds, []string{"state", "line", "state", "line"}) {
		t.Fatalf("event order = %v, want each state event before its line", kinds)
	}
	if n := len(d.snapshot()); n != 2 {
		t.Fatalf("the IRC client got %d lines, want both SILENCE lines", n)
	}
}

// A client's query of another nick's list passes through to it and leaves ours alone.
func TestClientSilenceQueryOfAnotherNickLeavesTheList(t *testing.T) {
	s, d, sink, _ := newMetaSession(t)
	s.mu.Lock()
	s.silence = []string{"*!*@keep"}
	s.mu.Unlock()
	s.tracker.Begin(BeginOpts{Client: d.ID(), Cmd: "SILENCE"})
	feed(s, ":srv 271 me other *!*@x", ":srv 272 me :End of Silence List")
	if got := silenceEvents(sink); len(got) != 0 {
		t.Fatalf("silence events = %q, want none", got)
	}
	if n := len(d.snapshot()); n != 2 {
		t.Fatalf("the requesting client got %d lines, want 2", n)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !reflect.DeepEqual(s.silence, []string{"*!*@keep"}) {
		t.Fatalf("silence = %q", s.silence)
	}
}
