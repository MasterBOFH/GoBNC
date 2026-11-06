package module

import (
	"context"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// A stopped network's "our nick" is the nick it last had (the alternate after a collision),
// not its configured one: a channel line naming the alternate is a highlight, and the
// alternate's own lines are never unread.
func TestHistoryActivityStoppedNetworkUsesLastNick(t *testing.T) {
	tc, _, nid, _ := historyFixtureWith(t, func(be *fakeBackend, _ *Server, db *store.Store, nid int64) {
		delete(be.sessions, nid)
		ctx := context.Background()
		if err := db.SetLastNick(ctx, nid, "me_"); err != nil { // configured nick "me"
			t.Fatal(err)
		}
		t0 := time.Now().Add(-time.Hour).UTC()
		for i, l := range []struct{ src, text string }{
			{"a!u@h", "me_: are you there?"},
			{"a!u@h", "hello all"},
			{"me_!u@h", "my own line"},
		} {
			if _, err := db.InsertMessage(ctx, store.Message{NetworkID: nid, Target: "#d",
				Time: t0.Add(time.Duration(i) * time.Second), Command: "PRIVMSG", Source: l.src, Raw: "x", Text: l.text}); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.SetBufferOpen(ctx, "phone", nid, "#d", "#d", true, 0); err != nil {
			t.Fatal(err)
		}
	}, "history")

	var act wire.HistoryActivityResult
	tc.call(wire.MethodHistoryActivity, wire.HistoryActivityParams{Network: nid, Device: "phone"}, &act)
	for _, b := range act.Buffers {
		if b.Key == "#d" {
			if b.Unread != 2 || b.Highlights != 1 {
				t.Fatalf("#d activity = %+v, want 2 unread (own line left out), 1 highlight", b)
			}
			return
		}
	}
	t.Fatalf("no #d in %+v", act.Buffers)
}

// With no nick recorded, a stopped network's nick is still its configured one.
func TestStoredViewFallsBackToConfiguredNick(t *testing.T) {
	var view bufferView
	historyFixtureWith(t, func(be *fakeBackend, srv *Server, db *store.Store, nid int64) {
		delete(be.sessions, nid)
		var err error
		if view, err = srv.view(context.Background(), nid); err != nil {
			t.Fatal(err)
		}
	})
	if _, live := view.(*session.Session); live || view.Nick() != "me" {
		t.Fatalf("view = %T nick %q, want the stored view with the configured nick me", view, view.Nick())
	}
}
