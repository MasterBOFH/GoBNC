package session

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// A query peer's NICK renames their query on every device that has a row for it (buffer_rename,
// emitted before the line) and stores the NICK in the query's history under the new nick.

// renamesBefore returns the buffer_rename events and whether each came before the first line
// event mentioning text.
func renamesBefore(sink *recSink, text string) (renames []wire.BufferRenameEvent, allBefore bool) {
	lineIdx := -1
	var idx []int
	for i, ev := range sink.events() {
		switch e := ev.(type) {
		case wire.BufferRenameEvent:
			renames = append(renames, e)
			idx = append(idx, i)
		case wire.LineEvent:
			if lineIdx < 0 && strings.Contains(e.Raw, text) {
				lineIdx = i
			}
		}
	}
	allBefore = lineIdx >= 0
	for _, i := range idx {
		if i > lineIdx {
			allBefore = false
		}
	}
	return renames, allBefore
}

func TestPeerNickRenamesTheQueryOnEveryDevice(t *testing.T) {
	s, sink := newEnvelopeSession(t) // peer shares #a and #b, and has a query in history
	ctx := context.Background()
	nid := s.Network.ID
	_ = s.store.SetBufferOpen(ctx, "phone", nid, "peer", "peer", true, 0)
	_ = s.store.SetBufferOpen(ctx, "pad", nid, "peer", "Peer", false, 4)
	_ = s.store.SetBufferOpen(ctx, "tablet", nid, "other", "other", true, 0)

	feed(s, ":peer!p@h NICK :Peer[2]")

	renames, before := renamesBefore(sink, "NICK")
	if len(renames) != 2 || !before {
		t.Fatalf("buffer_rename events = %+v (before the line: %v), want one per device with the query, first", renames, before)
	}
	want := wire.BufferRenameEvent{Network: nid, ClientID: "pad", From: "peer", FromKey: "peer", To: "Peer[2]", ToKey: "peer{2}"}
	if renames[0] != want {
		t.Fatalf("rename = %+v, want %+v", renames[0], want)
	}
	if bufs, _ := s.store.ListOpenBuffers(ctx, "pad", nid); len(bufs) != 1 || bufs[0].Target != "peer{2}" || bufs[0].Open || bufs[0].ClosedAtID != 4 || bufs[0].RenamedFrom != "Peer" {
		t.Fatalf("pad rows = %+v", bufs)
	}

	// The query's copy of the NICK is stored under the new nick and the envelope says so.
	ev := lastLine(t, sink)
	if !sameSet(bufferNames(ev), []string{"#a", "#b", "Peer[2]"}) {
		t.Fatalf("NICK envelope buffers = %v", bufferNames(ev))
	}
	for _, b := range ev.Buffers {
		if b.ID <= 0 {
			t.Fatalf("buffer %s has no stored id: %+v", b.Name, ev.Buffers)
		}
		if b.Name == "Peer[2]" {
			m, err := s.store.MessageByID(ctx, nid, b.ID)
			if err != nil || m == nil || m.Target != "peer{2}" || m.Command != "NICK" {
				t.Fatalf("query copy = %+v (err %v), want a NICK under peer{2}", m, err)
			}
		}
	}
}

func TestPeerNickWithoutSharedChannelStillRenamesAndStores(t *testing.T) {
	s, sink := newEnvelopeSession(t)
	ctx := context.Background()
	nid := s.Network.ID
	feed(s, ":loner!l@h PRIVMSG me :hi") // a query, no shared channel
	_ = s.store.SetBufferOpen(ctx, "phone", nid, "loner", "loner", true, 0)

	feed(s, ":loner!l@h NICK :loner2")

	if renames, before := renamesBefore(sink, "NICK :loner2"); len(renames) != 1 || !before || renames[0].ToKey != "loner2" {
		t.Fatalf("renames = %+v before=%v", renames, before)
	}
	ev := lastLine(t, sink)
	if len(ev.Buffers) != 1 || ev.Buffers[0].Name != "loner2" || ev.Buffers[0].ID <= 0 {
		t.Fatalf("NICK envelope = %+v, want the query under its new nick with an id", ev.Buffers)
	}
}

func TestPeerNickWithoutQueryRenamesNothing(t *testing.T) {
	s, sink := newEnvelopeSession(t)
	feed(s, ":bob!b@h JOIN #a", ":bob!b@h NICK :bob2")
	if renames, _ := renamesBefore(sink, "NICK :bob2"); len(renames) != 0 {
		t.Fatalf("renames = %+v", renames)
	}
	ev := lastLine(t, sink)
	if !reflect.DeepEqual(bufferNames(ev), []string{"#a"}) {
		t.Fatalf("NICK envelope buffers = %v, want #a only", bufferNames(ev))
	}
	if has, _ := s.store.HasTarget(context.Background(), s.Network.ID, "bob2"); has {
		t.Fatal("a nick change with no query is stored under the new nick")
	}
}

func TestOwnNickRenamesNoQuery(t *testing.T) {
	s, sink := newEnvelopeSession(t)
	_ = s.store.SetBufferOpen(context.Background(), "phone", s.Network.ID, "me", "me", true, 0)
	feed(s, ":me!u@h NICK :me2")
	if renames, _ := renamesBefore(sink, "NICK :me2"); len(renames) != 0 {
		t.Fatalf("our own nick change renamed a buffer: %+v", renames)
	}
}
