package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestMigrateIdempotent(t *testing.T) {
	p := filepath.Join(t.TempDir(), "t.db")
	s, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s2, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	s2.Close()
}

func TestNetworkCRUD(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	id, err := s.UpsertNetwork(ctx, Network{
		Name: "libera", Host: "irc.libera.chat", Port: 6697, TLS: true, Nick: "gobnc", Enabled: true,
	})
	if err != nil || id == 0 {
		t.Fatalf("id=%d err=%v", id, err)
	}
	n, err := s.NetworkByName(ctx, "libera")
	if err != nil || n.Host != "irc.libera.chat" {
		t.Fatalf("%+v %v", n, err)
	}
	if err := s.AddChannel(ctx, id, "#gobnc", "secret"); err != nil {
		t.Fatal(err)
	}
	chs, err := s.ListChannels(ctx, id)
	if err != nil || len(chs) != 1 || chs[0].Name != "#gobnc" || chs[0].Key != "secret" {
		t.Fatalf("%v %v", chs, err)
	}
	if err := s.RemoveChannel(ctx, id, "#gobnc"); err != nil {
		t.Fatal(err)
	}
	chs, err = s.ListChannels(ctx, id)
	if err != nil || len(chs) != 0 {
		t.Fatalf("after remove: %v %v", chs, err)
	}
	list, err := s.ListNetworks(ctx)
	if err != nil || len(list) != 1 {
		t.Fatal(list, err)
	}
	if err := s.DeleteNetwork(ctx, "libera"); err != nil {
		t.Fatal(err)
	}
}

func TestAuthFingerprint(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	if err := s.SetPasswordHash(ctx, "argon2id$..."); err != nil {
		t.Fatal(err)
	}
	h, err := s.PasswordHash(ctx)
	if err != nil || h != "argon2id$..." {
		t.Fatal(h, err)
	}
	fp := "aabbcc"
	if err := s.AddFingerprint(ctx, fp, "laptop"); err != nil {
		t.Fatal(err)
	}
	ok, err := s.HasFingerprint(ctx, fp)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	ok, _ = s.HasFingerprint(ctx, "nope")
	if ok {
		t.Fatal("expected miss")
	}
}

func TestMessagesQueryRetention(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	id, err := s.UpsertNetwork(ctx, Network{Name: "n", Host: "h", Port: 6667, Nick: "x", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		m := Message{
			NetworkID: id, Target: "#c", Time: t0.Add(time.Duration(i) * time.Minute),
			MsgID: "m" + string(rune('a'+i)), Command: "PRIVMSG", Source: "a!b@c",
			Raw: "x", Text: "msg",
		}
		if err := s.InsertMessage(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	before := t0.Add(3 * time.Minute)
	msgs, err := s.QueryMessages(ctx, HistoryQuery{NetworkID: id, Target: "#c", Before: &before, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 3 {
		t.Fatalf("got %d", len(msgs))
	}
	latest, err := s.QueryMessages(ctx, HistoryQuery{NetworkID: id, Target: "#c", Latest: true, Limit: 2})
	if err != nil || len(latest) != 2 {
		t.Fatalf("%v %v", latest, err)
	}
	if !latest[0].Time.Before(latest[1].Time) && !latest[0].Time.Equal(latest[1].Time) {
		t.Fatal("not ascending")
	}
	n, err := s.DeleteOlderThan(ctx, id, t0.Add(2*time.Minute))
	if err != nil || n != 2 {
		t.Fatalf("deleted=%d err=%v", n, err)
	}
}

func TestReadMarkerMonotonic(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	id, err := s.UpsertNetwork(ctx, Network{Name: "n", Host: "h", Port: 6667, Nick: "x", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	_, ok, err := s.GetReadMarker(ctx, id, "#c")
	if err != nil || ok {
		t.Fatalf("empty: ok=%v err=%v", ok, err)
	}
	t1 := "2019-01-04T14:33:26.123Z"
	t2 := "2019-01-04T14:34:00.000Z"
	stored, updated, err := s.SetReadMarkerIfNewer(ctx, id, "#c", t1)
	if err != nil || !updated || stored != t1 {
		t.Fatalf("first set: %q updated=%v err=%v", stored, updated, err)
	}
	stored, updated, err = s.SetReadMarkerIfNewer(ctx, id, "#c", t1)
	if err != nil || updated || stored != t1 {
		t.Fatalf("equal: %q updated=%v err=%v", stored, updated, err)
	}
	stored, updated, err = s.SetReadMarkerIfNewer(ctx, id, "#c", "2019-01-04T14:33:00.000Z")
	if err != nil || updated || stored != t1 {
		t.Fatalf("older: %q updated=%v err=%v", stored, updated, err)
	}
	stored, updated, err = s.SetReadMarkerIfNewer(ctx, id, "#c", t2)
	if err != nil || !updated || stored != t2 {
		t.Fatalf("newer: %q updated=%v err=%v", stored, updated, err)
	}
	got, ok, err := s.GetReadMarker(ctx, id, "#c")
	if err != nil || !ok || got != t2 {
		t.Fatalf("get: %q ok=%v err=%v", got, ok, err)
	}
}

func openTemp(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
