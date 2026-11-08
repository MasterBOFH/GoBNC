package module

import (
	"context"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// stoppedFixture is a network the brain runs no Session for (as after StopNetwork), with its
// history, a read marker on #d, device "phone"'s open buffers #d and friend, and #e in the
// network's channel list (the channels it joins on connect), all in the store. ids are the
// stored lines: #d's four (the first read), friend's one, then #e's one.
func stoppedFixture(t *testing.T, features ...string) (tc *testConn, nid int64, ids []int64) {
	t.Helper()
	tc, _, nid, _ = historyFixtureWith(t, func(be *fakeBackend, _ *Server, db *store.Store, nid int64) {
		delete(be.sessions, nid) // stopped: the brain runs no Session for it
		ctx := context.Background()
		t0 := time.Now().Add(-time.Hour).UTC()
		for i, l := range []struct{ target, src, text string }{
			{"#d", "a!u@h", "read already"},
			{"#d", "a!u@h", "hello all"},
			{"#d", "a!u@h", "me: are you there?"},
			{"#d", "me!u@h", "my own line"},
			{"friend", "friend!u@h", "psst"},
			{"#e", "a!u@h", "elsewhere"},
		} {
			id, err := db.InsertMessage(ctx, store.Message{NetworkID: nid, Target: l.target,
				Time: t0.Add(time.Duration(i) * time.Second), Command: "PRIVMSG", Source: l.src, Raw: "x", Text: l.text})
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, id)
		}
		if _, _, err := db.SetReadMarkerIfNewer(ctx, nid, "#d", t0.Format(time.RFC3339Nano)); err != nil {
			t.Fatal(err)
		}
		if err := db.AddChannel(ctx, nid, "#E", ""); err != nil {
			t.Fatal(err)
		}
		for _, b := range []string{"#d", "friend"} {
			if err := db.SetBufferOpen(ctx, "phone", nid, b, b, true, 0); err != nil {
				t.Fatal(err)
			}
		}
	}, features...)
	return tc, nid, ids
}

// A stopped network's history.activity and read_markers.get answer from the store, with the
// counting rules of a live network over its channel list, the device's open buffers and
// Server: own lines are never unread, every unread query line is a highlight, a channel line
// is one when it names the network's nick.
func TestHistoryActivityReadsTheStoreForAStoppedNetwork(t *testing.T) {
	tc, nid, ids := stoppedFixture(t, "history", "read_markers")

	var act wire.HistoryActivityResult
	tc.call(wire.MethodHistoryActivity, wire.HistoryActivityParams{Network: nid, Device: "phone"}, &act)
	got := map[string]wire.BufferActivity{}
	for _, b := range act.Buffers {
		got[b.Key] = b
	}
	if len(act.Buffers) != 4 || got["#d"].Buffer != "#d" || got["friend"].Buffer != "friend" || got["#e"].Buffer != "#E" ||
		got[wire.ServerBufferKey].Buffer != wire.ServerBuffer {
		t.Fatalf("activity buffers = %+v, want #E (the channel list), #d, friend and Server", act.Buffers)
	}
	if d := got["#d"]; d.Unread != 2 || d.Highlights != 1 || d.LastID != ids[3] || d.LastTime == "" {
		t.Errorf("#d activity = %+v, want 2 unread, 1 highlight, last id %d", d, ids[3])
	}
	if f := got["friend"]; f.Unread != 1 || f.Highlights != 1 || f.LastID != ids[4] {
		t.Errorf("friend activity = %+v, want 1 unread, 1 highlight", f)
	}
	if e := got["#e"]; e.Unread != 1 || e.Highlights != 0 || e.LastID != ids[5] {
		t.Errorf("#e activity = %+v, want 1 unread, no highlight", e)
	}

	var mark wire.ReadMarkerGetResult
	tc.call(wire.MethodReadMarkersGet, wire.ReadMarkerGetParams{Network: nid, Buffer: "#D"}, &mark)
	if mark.ID != ids[0] || mark.Time == "" {
		t.Fatalf("read marker = %+v, want id %d", mark, ids[0])
	}

	// A network the store does not have is still not_found.
	if code := tc.callErr(wire.MethodHistoryActivity, wire.HistoryActivityParams{Network: 999, Device: "phone"}); code != wire.CodeNotFound {
		t.Errorf("activity for an unknown network: code %q, want not_found", code)
	}
	if code := tc.callErr(wire.MethodReadMarkersGet, wire.ReadMarkerGetParams{Network: 999, Buffer: "#d"}); code != wire.CodeNotFound {
		t.Errorf("read marker for an unknown network: code %q, want not_found", code)
	}
}

// Writes still need the Session: read_markers.set on a stopped network answers not_found.
func TestReadMarkerSetNeedsASession(t *testing.T) {
	tc, nid, ids := stoppedFixture(t, "read_markers")
	if code := tc.callErr(wire.MethodReadMarkersSet, wire.ReadMarkerSetParams{Network: nid, Buffer: "#d", ID: ids[1]}); code != wire.CodeNotFound {
		t.Fatalf("read_markers.set on a stopped network: code %q, want not_found", code)
	}
}

// A stopped network's history.messages pages its stored history, first_unread anchor included.
func TestHistoryMessagesReadsTheStoreForAStoppedNetwork(t *testing.T) {
	tc, nid, ids := stoppedFixture(t, "history")

	var msgs wire.HistoryMessagesResult
	tc.call(wire.MethodHistoryMessages, wire.HistoryMessagesParams{Network: nid, Buffer: "#D"}, &msgs)
	if len(msgs.Messages) != 4 || msgs.Messages[3].ID != ids[3] || msgs.Messages[0].Buffer != "#D" || msgs.HasBefore || msgs.HasAfter {
		t.Fatalf("history.messages = %+v, want #d's four lines", msgs)
	}
	tc.call(wire.MethodHistoryMessages, wire.HistoryMessagesParams{Network: nid, Buffer: "#d", BeforeID: ptr(ids[3]), Limit: 2}, &msgs)
	if len(msgs.Messages) != 2 || msgs.Messages[1].ID != ids[2] || !msgs.HasBefore || !msgs.HasAfter {
		t.Fatalf("before page = %+v, want two lines before %d", msgs, ids[3])
	}
	tc.call(wire.MethodHistoryMessages, wire.HistoryMessagesParams{Network: nid, Buffer: "#d", Device: "phone", Anchor: wire.AnchorFirstUnread}, &msgs)
	if len(msgs.Messages) != 3 || msgs.Messages[0].ID != ids[1] {
		t.Fatalf("first_unread page = %+v, want it to start at %d", msgs.Messages, ids[1])
	}
	if code := tc.callErr(wire.MethodHistoryMessages, wire.HistoryMessagesParams{Network: 999, Buffer: "#d"}); code != wire.CodeNotFound {
		t.Errorf("messages for an unknown network: code %q, want not_found", code)
	}
}
