package module

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// historyFixture: a store with network "net" and messages seeded by the caller, a Session for
// it, and a module "m" granted features.
func historyFixture(t *testing.T, features ...string) (*testConn, *store.Store, int64, *session.Session) {
	t.Helper()
	return historyFixtureWith(t, nil, features...)
}

// historyFixtureWith is historyFixture with setup applied to the backend and server before the
// server starts (so the server goroutines never race the test's writes).
func historyFixtureWith(t *testing.T, setup func(*fakeBackend, *Server, *store.Store, int64), features ...string) (*testConn, *store.Store, int64, *session.Session) {
	t.Helper()
	dir, err := os.MkdirTemp("", "gbh")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	db, err := store.Open(filepath.Join(dir, "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	nid, err := db.UpsertNetwork(context.Background(), store.Network{Name: "net", Host: "h", Port: 1, Nick: "me", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	sess := registeredSessionWithStore(t, nid, db)
	be := &fakeBackend{ready: true, db: db, sessions: map[int64]*session.Session{nid: sess}}
	_, path := startServerWith(t, []config.ModuleConfig{{Name: "m", Features: features}}, be, func(srv *Server) {
		if setup != nil {
			setup(be, srv, db, nid)
		}
	})
	tc := dialModule(t, path)
	tc.hello("m", features...)
	return tc, db, nid, sess
}

func seedHistory(t *testing.T, db *store.Store, nid int64, target string, n int) []int64 {
	t.Helper()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ids := make([]int64, n)
	for i := range ids {
		id, err := db.InsertMessage(context.Background(), store.Message{NetworkID: nid, Target: target,
			Time: t0.Add(time.Duration(i) * time.Second), Command: "PRIVMSG", Source: "a!b@c", Raw: ":a!b@c PRIVMSG #c :x", Text: "x"})
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
	}
	return ids
}

func ptr(v int64) *int64 { return &v }

func TestHistoryMessagesSelectorValidation(t *testing.T) {
	tc, _, nid, _ := historyFixture(t, "history")
	for name, p := range map[string]wire.HistoryMessagesParams{
		"two selectors":          {Network: nid, Buffer: "#c", BeforeID: ptr(5), AroundID: ptr(1)},
		"bounds and around":      {Network: nid, Buffer: "#c", BeforeID: ptr(5), AfterID: ptr(1), AroundID: ptr(3)},
		"after and anchor":       {Network: nid, Buffer: "#c", AfterID: ptr(1), Anchor: wire.AnchorLatest},
		"anchor and id":          {Network: nid, Buffer: "#c", AroundID: ptr(5), Anchor: wire.AnchorLatest},
		"limit too big":          {Network: nid, Buffer: "#c", Limit: 501},
		"negative limit":         {Network: nid, Buffer: "#c", Limit: -1},
		"first_unread no device": {Network: nid, Buffer: "#c", Anchor: wire.AnchorFirstUnread},
		"unknown anchor":         {Network: nid, Buffer: "#c", Anchor: "middle"},
		"no buffer":              {Network: nid},
	} {
		if code := tc.callErr(wire.MethodHistoryMessages, p); code != wire.CodeBadParams {
			t.Errorf("%s: code %q, want bad_params", name, code)
		}
	}
	if code := tc.callErr(wire.MethodHistoryMessages, wire.HistoryMessagesParams{Network: 999, Buffer: "#c"}); code != wire.CodeNotFound {
		t.Errorf("unknown network: code %q, want not_found", code)
	}
}

func TestHistoryMessagesPagesByID(t *testing.T) {
	tc, db, nid, _ := historyFixture(t, "history")
	ids := seedHistory(t, db, nid, "#c", 120)
	var res wire.HistoryMessagesResult
	tc.call(wire.MethodHistoryMessages, wire.HistoryMessagesParams{Network: nid, Buffer: "#C"}, &res)
	if len(res.Messages) != 50 || res.Messages[49].ID != ids[119] || !res.HasBefore || res.HasAfter {
		t.Fatalf("latest page: %d msgs, last %d, has_before %v has_after %v", len(res.Messages), res.Messages[len(res.Messages)-1].ID, res.HasBefore, res.HasAfter)
	}
	if res.Messages[0].Buffer != "#C" || res.Messages[0].Command != "PRIVMSG" || res.Messages[0].Time != "2026-01-01T00:01:10Z" {
		t.Fatalf("message = %+v", res.Messages[0])
	}
	tc.call(wire.MethodHistoryMessages, wire.HistoryMessagesParams{Network: nid, Buffer: "#c", AfterID: ptr(ids[0]), Limit: 500}, &res)
	if len(res.Messages) != 119 || !res.HasBefore || res.HasAfter {
		t.Fatalf("after page: %d msgs, has_before %v has_after %v", len(res.Messages), res.HasBefore, res.HasAfter)
	}
}

// Both bounds are the gap fill: the rows strictly between them, oldest first. has_after says
// rows remain inside the range (below before_id), not merely somewhere newer in the buffer;
// has_before keeps its meaning (older rows exist in the buffer).
func TestHistoryMessagesBetweenBounds(t *testing.T) {
	tc, db, nid, _ := historyFixture(t, "history")
	ids := seedHistory(t, db, nid, "#c", 30)
	call := func(after, before int64, limit int) wire.HistoryMessagesResult {
		t.Helper()
		var res wire.HistoryMessagesResult
		tc.call(wire.MethodHistoryMessages, wire.HistoryMessagesParams{Network: nid, Buffer: "#c",
			AfterID: ptr(after), BeforeID: ptr(before), Limit: limit}, &res)
		return res
	}
	// Empty range: adjacent rows.
	if res := call(ids[10], ids[11], 50); len(res.Messages) != 0 || res.HasAfter || res.HasBefore {
		t.Fatalf("empty: %d msgs, has_before %v has_after %v", len(res.Messages), res.HasBefore, res.HasAfter)
	}
	// Partial page: everything inside, and nothing left inside though newer rows exist.
	res := call(ids[10], ids[16], 50)
	if len(res.Messages) != 5 || res.Messages[0].ID != ids[11] || res.Messages[4].ID != ids[15] || res.HasAfter || !res.HasBefore {
		t.Fatalf("partial: %d msgs, has_before %v has_after %v", len(res.Messages), res.HasBefore, res.HasAfter)
	}
	// Full page: the oldest rows after after_id, more inside.
	res = call(ids[10], ids[25], 5)
	if len(res.Messages) != 5 || res.Messages[0].ID != ids[11] || res.Messages[4].ID != ids[15] || !res.HasAfter || !res.HasBefore {
		t.Fatalf("full: %d msgs, has_before %v has_after %v", len(res.Messages), res.HasBefore, res.HasAfter)
	}
	// Exactly the range's size: a full page with nothing left inside.
	if res = call(ids[10], ids[16], 5); len(res.Messages) != 5 || res.HasAfter {
		t.Fatalf("exact: %d msgs, has_after %v", len(res.Messages), res.HasAfter)
	}
	// From the buffer's start (the fixture's own JOIN, then ids 0..2): no older rows.
	if res = call(0, ids[3], 50); len(res.Messages) != 4 || res.Messages[3].ID != ids[2] || res.HasBefore || res.HasAfter {
		t.Fatalf("start: %d msgs, has_before %v has_after %v", len(res.Messages), res.HasBefore, res.HasAfter)
	}
}

func TestHistoryActivityCountsUnreadAndHighlights(t *testing.T) {
	tc, db, nid, _ := historyFixture(t, "history")
	t0 := time.Now().Add(-time.Hour).UTC()
	for i, l := range []struct{ src, text string }{
		{"a!u@h", "hello all"},
		{"a!u@h", "me: are you there?"},
		{"me!u@h", "my own line"},
		{"a!u@h", "\x01VERSION\x01"},
	} {
		if _, err := db.InsertMessage(context.Background(), store.Message{NetworkID: nid, Target: "#c",
			Time: t0.Add(time.Duration(i) * time.Second), Command: "PRIVMSG", Source: l.src, Raw: "x", Text: l.text}); err != nil {
			t.Fatal(err)
		}
	}
	var res wire.HistoryActivityResult
	tc.call(wire.MethodHistoryActivity, wire.HistoryActivityParams{Network: nid, Device: "phone"}, &res)
	var ch *wire.BufferActivity
	names := []string{}
	for i, b := range res.Buffers {
		names = append(names, b.Buffer)
		if b.Key == "#c" {
			ch = &res.Buffers[i]
		}
	}
	if ch == nil || len(res.Buffers) != 2 {
		t.Fatalf("buffers = %v, want #c and Server", names)
	}
	if ch.Unread != 2 || ch.Highlights != 1 || ch.LastID == 0 || ch.LastTime == "" {
		t.Fatalf("#c activity = %+v, want 2 unread, 1 highlight", *ch)
	}
	if code := tc.callErr(wire.MethodHistoryActivity, wire.HistoryActivityParams{Network: nid}); code != wire.CodeBadParams {
		t.Fatalf("missing device: code %q", code)
	}
}

// The Server buffer stores server notices, but they are status traffic, not unread: only a
// person's line there (a $mask notice) counts. In a channel a server notice counts like any.
func TestHistoryActivityServerNoticesAreNotUnreadInServer(t *testing.T) {
	tc, db, nid, _ := historyFixture(t, "history")
	t0 := time.Now().Add(-time.Hour).UTC()
	for i, l := range []struct{ target, src, text string }{
		{"srv.example", "srv.example", "*** Looking up your hostname"},
		{"irc.example", "", "*** no prefix"},
		{"srv.example", "srv.example", "*** Notice -- snotice"},
		{"irc.example", "oper!o@h", "global notice"},
		{"#c", "srv.example", "channel maintenance"},
	} {
		if _, err := db.InsertMessage(context.Background(), store.Message{NetworkID: nid, Target: l.target,
			Time: t0.Add(time.Duration(i) * time.Second), Command: "NOTICE", Source: l.src, Raw: "x", Text: l.text}); err != nil {
			t.Fatal(err)
		}
	}
	var res wire.HistoryActivityResult
	tc.call(wire.MethodHistoryActivity, wire.HistoryActivityParams{Network: nid, Device: "phone"}, &res)
	got := map[string]wire.BufferActivity{}
	for _, b := range res.Buffers {
		got[b.Buffer] = b
	}
	if srv := got[wire.ServerBuffer]; srv.Unread != 1 || srv.LastID == 0 {
		t.Fatalf("Server activity = %+v, want 1 unread (the $mask notice) and its last id", srv)
	}
	if ch := got["#c"]; ch.Unread != 1 {
		t.Fatalf("#c activity = %+v, want the server notice counted", ch)
	}
}
