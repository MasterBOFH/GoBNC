package session

import (
	"context"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// insertLine stores a PRIVMSG from source under target directly (as an older brain, or a line
// that arrived before our nick was known, may have stored it), newest last.
func insertLine(t *testing.T, s *Session, target, source, text string) int64 {
	t.Helper()
	id, err := s.store.InsertMessage(context.Background(), store.Message{
		NetworkID: s.Network.ID, Target: target, Time: time.Now().UTC(), Command: "PRIVMSG",
		Source: source, Raw: ":" + source + " PRIVMSG " + target + " :" + text, Text: text,
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond) // keep times strictly increasing
	return id
}

func openRows(t *testing.T, s *Session, client string) map[string]store.OpenBuffer {
	t.Helper()
	rows, err := s.store.ListOpenBuffers(context.Background(), client, s.Network.ID)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]store.OpenBuffer{}
	for _, r := range rows {
		out[r.Target] = r
	}
	return out
}

// A3005: the seed opens a query only under the peer's own key, displayed with the peer's newest
// spelling. A target that is our own nick (lines addressed to us stored under it) and a target
// whose lines come from someone else (a service address) are not queries of that sender.
func TestSeedDeviceOpensOnlyQueriesOfTheTargetItself(t *testing.T) {
	s, sink := newEnvelopeSession(t) // "peer" query, one unread line
	insertLine(t, s, "me", "Melksedek!m@h", "addressed to us, stored under our nick")
	insertLine(t, s, "me", "Me!u@h", "a note to self: its sender folds to the target, still no query")
	insertLine(t, s, "x@channels.undernet.org", "norIrM!n@h", "a line under a service address")
	insertLine(t, s, "carol", "CAROL!c@h", "older spelling")
	insertLine(t, s, "carol", "Carol!c@h", "newest spelling")
	insertLine(t, s, "dave", "Dave!d@h", "dave's own line")
	insertLine(t, s, "dave", "eve!e@h", "someone else's line under dave, newest")

	seeded, err := s.SeedDevice("fresh", false)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, b := range seeded {
		got[b] = true
	}
	if len(seeded) != 3 || !got["peer"] || !got["Carol"] || !got["Dave"] {
		t.Fatalf("seeded = %v, want peer, Carol and Dave", seeded)
	}
	rows := openRows(t, s, "fresh")
	for _, key := range []string{"me", "x@channels.undernet.org", "melksedek", "norirm", "eve"} {
		if r, ok := rows[key]; ok {
			t.Fatalf("row %q opened: %+v", key, r)
		}
	}
	for key, display := range map[string]string{"peer": "peer", "carol": "Carol", "dave": "Dave"} {
		if r := rows[key]; !r.Open || r.Display != display {
			t.Fatalf("row %q = %+v, want open as %q", key, r, display)
		}
	}
	for _, ev := range sink.events() {
		if e, ok := ev.(wire.BufferOpenEvent); ok && s.Fold(e.Buffer) != e.Key {
			t.Fatalf("buffer_open %+v: display does not fold to its key", e)
		}
	}
}

// A3005 repair: a row an earlier seed opened under the wrong key (target "me", shown as
// "Melksedek") closes when the device closes "Melksedek", with a buffer_close of its own after
// the real key's; other open rows stay open.
func TestCloseBufferAlsoClosesOpenRowDisplayedAsIt(t *testing.T) {
	s, sink := newEnvelopeSession(t)
	ctx := context.Background()
	misID := insertLine(t, s, "me", "Melksedek!m@h", "stored under our nick")
	for _, r := range []struct{ target, display string }{{"me", "Melksedek"}, {"peer", "peer"}} {
		if err := s.store.SetBufferOpen(ctx, "phone", s.Network.ID, r.target, r.display, true, 0); err != nil {
			t.Fatal(err)
		}
	}
	// Another device's mis-keyed row is not this device's to close.
	_ = s.store.SetBufferOpen(ctx, "pad", s.Network.ID, "me", "Melksedek", true, 0)

	if _, err := s.CloseBufferForDevice("phone", "Melksedek"); err != nil {
		t.Fatal(err)
	}
	rows := openRows(t, s, "phone")
	if r := rows["me"]; r.Open || r.ClosedAtID != misID {
		t.Fatalf("mis-keyed row = %+v, want closed at %d", r, misID)
	}
	if r := rows["melksedek"]; r.Open {
		t.Fatalf("closed key's row = %+v, want closed", r)
	}
	if !rows["peer"].Open {
		t.Fatal("an unrelated open row was closed")
	}
	if !openRows(t, s, "pad")["me"].Open {
		t.Fatal("another device's row was closed")
	}
	var closes []wire.BufferCloseEvent
	for _, ev := range sink.events() {
		if e, ok := ev.(wire.BufferCloseEvent); ok {
			closes = append(closes, e)
		}
	}
	if len(closes) != 2 || closes[0].Key != "melksedek" || closes[0].Buffer != "Melksedek" ||
		closes[1].Key != "me" || closes[1].Buffer != "Melksedek" || closes[1].ClosedAtID != misID || closes[1].ClientID != "phone" {
		t.Fatalf("buffer_close events = %+v, want melksedek then the repaired me", closes)
	}
}

// A query renamed by the peer's NICK (A2982) keeps a key that folds to its display: closing
// another buffer leaves it open, and closing it closes that one row only.
func TestCloseBufferLeavesRenamedQueryAlone(t *testing.T) {
	s, sink := newEnvelopeSession(t)
	ctx := context.Background()
	_ = s.store.SetBufferOpen(ctx, "phone", s.Network.ID, "peer", "peer", true, 0)
	_ = s.store.SetBufferOpen(ctx, "phone", s.Network.ID, "bob", "bob", true, 0)
	feed(s, ":peer!p@h NICK Peer2")
	rows := openRows(t, s, "phone")
	if r := rows["peer2"]; !r.Open || r.Display != "Peer2" || r.RenamedFrom != "peer" {
		t.Fatalf("renamed row = %+v", r)
	}
	if _, err := s.CloseBufferForDevice("phone", "bob"); err != nil {
		t.Fatal(err)
	}
	if !openRows(t, s, "phone")["peer2"].Open {
		t.Fatal("closing bob closed the renamed query")
	}
	if _, err := s.CloseBufferForDevice("phone", "Peer2"); err != nil {
		t.Fatal(err)
	}
	if r := openRows(t, s, "phone")["peer2"]; r.Open {
		t.Fatalf("renamed query after its close = %+v", r)
	}
	n := 0
	for _, ev := range sink.events() {
		if _, ok := ev.(wire.BufferCloseEvent); ok {
			n++
		}
	}
	if n != 2 {
		t.Fatalf("%d buffer_close events, want 2 (one per close)", n)
	}
}
