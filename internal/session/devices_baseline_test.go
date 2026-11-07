package session

import (
	"context"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// A new device's first seed with baseline: every buffer with history and no read marker gets
// its marker at its newest line (read_marker event, MARKREAD to IRC clients), so nothing old is
// unread and a never-read query is not opened; a buffer with a marker keeps it and seeds as before.
func TestSeedDeviceBaselineMarksNeverReadBuffersRead(t *testing.T) {
	s, sink := newEnvelopeSession(t) // "peer" query: one line, no marker
	ctx := context.Background()
	feed(s, ":peer!p@h PRIVMSG #a :channel line", ":bob!b@h PRIVMSG me :first")
	time.Sleep(5 * time.Millisecond) // the marker is a time: keep the second line strictly later
	feed(s, ":bob!b@h PRIVMSG me :second")
	// bob was read up to his first line: his second still counts and opens his query.
	bob, _ := s.store.MessagesLatest(ctx, s.Network.ID, "bob", 2)
	bobMark := bob[0].Time.UTC().Format(time.RFC3339Nano)
	if _, _, err := s.store.SetReadMarkerIfNewer(ctx, s.Network.ID, "bob", bobMark); err != nil {
		t.Fatal(err)
	}
	d := &fakeDL{id: "irc", caps: map[string]bool{readMarkerCap: true}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	d.clearSent()

	seeded, err := s.SeedDevice("fresh", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(seeded) != 1 || seeded[0] != "bob" {
		t.Fatalf("seeded = %v, want [bob] only (peer was never read)", seeded)
	}
	for _, key := range []string{"peer", "#a"} {
		newest, _ := s.store.MessagesLatest(ctx, s.Network.ID, key, 1)
		a, err := s.store.BufferActivity(ctx, s.Network.ID, key, markerTime(t, s, key), 0)
		if err != nil || len(a.Unread) != 0 {
			t.Fatalf("%s unread after the baseline = %d (%v)", key, len(a.Unread), err)
		}
		var ev *wire.ReadMarkerEvent
		for _, e := range sink.events() {
			if rm, ok := e.(wire.ReadMarkerEvent); ok && rm.Key == key {
				ev = &rm
			}
		}
		if ev == nil || ev.ID != newest[0].ID {
			t.Fatalf("%s read_marker = %+v, want id %d", key, ev, newest[0].ID)
		}
		found := false
		for _, m := range d.sent {
			found = found || (m.Command == "MARKREAD" && m.Param(0) == key)
		}
		if !found {
			t.Fatalf("no MARKREAD %s to the IRC client: %+v", key, d.sent)
		}
	}
	if ts, _, _ := s.store.GetReadMarker(ctx, s.Network.ID, "bob"); ts != bobMark {
		t.Fatalf("bob's marker moved to %s", ts)
	}

	// Later seeds (the device is seeded) set nothing: a new never-read query stays unread.
	feed(s, ":carol!c@h PRIVMSG me :after the first sync")
	if _, err := s.SeedDevice("fresh", true); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.store.GetReadMarker(ctx, s.Network.ID, "carol"); ok {
		t.Fatal("a later seed set carol's marker")
	}
}

// Without baseline, seeding leaves markers alone (the earlier behaviour).
func TestSeedDeviceWithoutBaselineSetsNoMarker(t *testing.T) {
	s, _ := newEnvelopeSession(t)
	seeded, err := s.SeedDevice("fresh", false)
	if err != nil || len(seeded) != 1 || seeded[0] != "peer" {
		t.Fatalf("seeded = %v, %v; want [peer]", seeded, err)
	}
	if _, ok, _ := s.store.GetReadMarker(context.Background(), s.Network.ID, "peer"); ok {
		t.Fatal("seed without baseline set a marker")
	}
}

func markerTime(t *testing.T, s *Session, key string) time.Time {
	t.Helper()
	ts, ok, err := s.store.GetReadMarker(context.Background(), s.Network.ID, key)
	if err != nil || !ok {
		t.Fatalf("%s has no marker (%v)", key, err)
	}
	m, err := parseMarkTime(ts)
	if err != nil {
		t.Fatal(err)
	}
	return m
}
