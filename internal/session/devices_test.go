package session

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// eventsAround returns sink's buffer_open events for buffer key with their positions, and the
// position of the first line event mentioning text.
func eventsAround(sink *recSink, key, text string) (opens []wire.BufferOpenEvent, openIdx []int, lineIdx int) {
	lineIdx = -1
	for i, ev := range sink.events() {
		switch e := ev.(type) {
		case wire.BufferOpenEvent:
			if e.Key == key {
				opens = append(opens, e)
				openIdx = append(openIdx, i)
			}
		case wire.LineEvent:
			if lineIdx < 0 && strings.Contains(e.Raw, text) {
				lineIdx = i
			}
		}
	}
	return
}

func TestDMOpensQueryOnEveryDeviceBeforeItsLine(t *testing.T) {
	s, sink := newEnvelopeSession(t) // peer already has a query in history
	ctx := context.Background()
	_ = s.store.SetDeviceNetwork(ctx, "phone", s.Network.ID, true)
	_ = s.store.SetDeviceNetwork(ctx, "pad", s.Network.ID, true)
	_ = s.store.SetBufferOpen(ctx, "pad", s.Network.ID, "peer", "peer", false, 1) // closed earlier

	feed(s, ":peer!p@h PRIVMSG me :new dm")
	opens, openIdx, lineIdx := eventsAround(sink, "peer", "new dm")
	if len(opens) != 2 || opens[0].Reason != wire.OpenMessage {
		t.Fatalf("buffer_open events = %+v, want one per device", opens)
	}
	for _, i := range openIdx {
		if lineIdx < 0 || i > lineIdx {
			t.Fatalf("buffer_open at %d, line at %d: buffer_open must come first", i, lineIdx)
		}
	}
	line := sink.events()[lineIdx].(wire.LineEvent)
	if floor, _ := s.store.BufferClosedAt(ctx, "pad", s.Network.ID, "peer"); floor != line.Buffers[0].ID-1 {
		t.Fatalf("reopened floor = %d, want %d", floor, line.Buffers[0].ID-1)
	}
}

func TestNoticeAndCTCPDoNotOpenQueries(t *testing.T) {
	s, sink := newEnvelopeSession(t)
	_ = s.store.SetDeviceNetwork(context.Background(), "phone", s.Network.ID, true)
	feed(s, ":bob!b@h NOTICE me :a notice", ":carol!c@h PRIVMSG me :\x01VERSION\x01",
		":srv.example PRIVMSG me :from the server", ":peer!p@h PRIVMSG #a :channel line")
	for _, ev := range sink.events() {
		if e, ok := ev.(wire.BufferOpenEvent); ok {
			t.Fatalf("unexpected buffer_open %+v", e)
		}
	}
	feed(s, ":dave!d@h PRIVMSG me :\x01ACTION waves\x01")
	if opens, _, _ := eventsAround(sink, "dave", "waves"); len(opens) != 1 {
		t.Fatalf("an ACTION should open the query: %+v", opens)
	}
}

func TestSeedDeviceOpensQueriesWithUnreadLines(t *testing.T) {
	s, sink := newEnvelopeSession(t) // "peer" query has an unread line
	ctx := context.Background()
	feed(s, ":old!o@h PRIVMSG me :read already")
	if _, _, err := s.store.SetReadMarkerIfNewer(ctx, s.Network.ID, "old", time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	// Devices that existed before these lines got them opened; a brand-new device seeds.
	seeded, err := s.SeedDevice("fresh")
	if err != nil {
		t.Fatal(err)
	}
	if len(seeded) != 1 || seeded[0] != "peer" {
		t.Fatalf("seeded = %v, want [peer]", seeded)
	}
	if opens, _, _ := eventsAround(sink, "peer", "\x00"); len(opens) != 1 || opens[0].Reason != wire.OpenSeed || opens[0].ClientID != "fresh" {
		t.Fatalf("seed events = %+v", opens)
	}
	if again, _ := s.SeedDevice("fresh"); len(again) != 0 {
		t.Fatalf("second seed opened %v", again)
	}
}

func TestSeedSkipsStatusmsgMaskAndNoticeOnlyTargets(t *testing.T) {
	s, _ := newEnvelopeSession(t)
	feed(s, ":op!o@h PRIVMSG @#a :ops only", ":oper!o@h PRIVMSG $*.example :global",
		":NickServ!s@services. NOTICE me :identify", ":Carol!c@h PRIVMSG me :hi")
	_ = s.store.SetBufferOpen(context.Background(), "fresh", s.Network.ID, "carol", "Carol", true, 0) // a row first
	seeded, err := s.SeedDevice("fresh")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, b := range seeded {
		got[b] = true
	}
	if !got["peer"] || got["op"] || got["oper"] || got["NickServ"] || len(seeded) != 2 {
		t.Fatalf("seeded = %v, want peer and Carol only", seeded)
	}
}
