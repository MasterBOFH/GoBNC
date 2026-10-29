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
	sess := registeredSession(t, nid)
	be := &fakeBackend{ready: true, db: db, sessions: map[int64]*session.Session{nid: sess}}
	_, path := startServer(t, []config.ModuleConfig{{Name: "m", Features: features}}, be)
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
		"two selectors":          {Network: nid, Buffer: "#c", BeforeID: ptr(5), AfterID: ptr(1)},
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
