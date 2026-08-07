package history

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/store"
)

func TestQueryLegacyAfterFiltersEvents(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	id, err := db.UpsertNetwork(ctx, store.Network{Name: "n", Host: "h", Port: 1, Nick: "x", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	h := New(db)
	t0 := time.Now().UTC().Add(-time.Hour)
	for _, cmd := range []string{"JOIN", "PRIVMSG", "NOTICE", "PART", "TAGMSG", "TOPIC"} {
		ts := t0
		t0 = t0.Add(time.Minute)
		_ = h.Store(ctx, Record{
			NetworkID: id, Target: "#c", Time: ts, Command: cmd,
			Source: "a!b@c", Raw: ":" + "a!b@c " + cmd + " #c :x", Text: "x",
		})
	}
	msgs, err := h.QueryLegacyAfter(ctx, id, "#c", time.Now().UTC().Add(-2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 {
		t.Fatalf("want PRIVMSG+NOTICE only, got %d %#v", len(msgs), msgs)
	}
	for _, m := range msgs {
		if m.Command != "PRIVMSG" && m.Command != "NOTICE" {
			t.Fatalf("unexpected %s", m.Command)
		}
	}
}

func TestQueryLegacyAfterUnlimited(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	id, err := db.UpsertNetwork(ctx, store.Network{Name: "n", Host: "h", Port: 1, Nick: "x", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	h := NewWithLimits(db, 100, 0) // unlimited
	t0 := time.Now().UTC().Add(-time.Hour)
	for i := 0; i < 5; i++ {
		_ = h.Store(ctx, Record{
			NetworkID: id, Target: "#c", Time: t0.Add(time.Duration(i) * time.Minute),
			Command: "PRIVMSG", Source: "a!b@c", Raw: ":a!b@c PRIVMSG #c :x", Text: "x",
		})
	}
	h.SetLegacyPlaybackMax(2)
	capped, err := h.QueryLegacyAfter(ctx, id, "#c", t0.Add(-time.Minute))
	if err != nil || len(capped) != 2 {
		t.Fatalf("capped: %d %v", len(capped), err)
	}
	h.SetLegacyPlaybackMax(0)
	all, err := h.QueryLegacyAfter(ctx, id, "#c", t0.Add(-time.Minute))
	if err != nil || len(all) != 5 {
		t.Fatalf("unlimited: %d %v", len(all), err)
	}
}
