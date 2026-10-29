package session

import (
	"slices"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

func TestLocalSelfEchoEmitsOneSelfLineWithID(t *testing.T) {
	s, sink := newEnvelopeSession(t)
	d := &fakeDL{id: "c1", caps: map[string]bool{"echo-message": true}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	before := len(sink.lines())
	msg, _ := irc.Parse("PRIVMSG #a :typed locally")
	s.echoSelfLocally(d, msg) // uplink without echo-message
	got := sink.lines()[before:]
	if len(got) != 1 {
		t.Fatalf("%d line events for one local echo: %+v", len(got), got)
	}
	ev := got[0]
	if !ev.Self || ev.Kind != wire.LineMessage || len(ev.Buffers) != 1 || ev.Buffers[0].Name != "#a" || ev.Buffers[0].ID == 0 {
		t.Fatalf("local echo envelope = %+v", ev)
	}
}

func TestMARKREADFromClientEmitsReadMarker(t *testing.T) {
	s, sink := newEnvelopeSession(t)
	feed(s, ":peer!p@h PRIVMSG #a :read me")
	last := sink.lines()[len(sink.lines())-1]
	d := &fakeDL{id: "c1", caps: map[string]bool{readMarkerCap: true}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	ts := time.Now().Add(time.Second).UTC().Format(time.RFC3339Nano)
	msg, _ := irc.Parse("MARKREAD #A timestamp=" + ts)
	if err := s.handleMARKREAD(d, msg); err != nil {
		t.Fatal(err)
	}
	var rm *wire.ReadMarkerEvent
	for _, ev := range sink.events() {
		if e, ok := ev.(wire.ReadMarkerEvent); ok {
			rm = &e
		}
	}
	if rm == nil || rm.Key != "#a" || rm.Buffer != "#A" || rm.Time != ts || rm.ID != last.Buffers[0].ID {
		t.Fatalf("read_marker = %+v, want key #a, time %s, id %d", rm, ts, last.Buffers[0].ID)
	}
}

func TestCAPAckVisibleInSnapshotRightAfter(t *testing.T) {
	s, _ := newEnvelopeSession(t)
	s.HandleMessage(irc.Message{Source: "srv.example", Command: "CAP", Params: []string{"me", "ACK", "away-notify"}})
	if snap := snapshotOf(s); !slices.Contains(snap.Caps, "away-notify") {
		t.Fatalf("caps = %v", snap.Caps)
	}
}
