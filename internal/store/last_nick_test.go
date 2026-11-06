package store

import (
	"context"
	"path/filepath"
	"testing"
)

// A network's last-used nick is stored per network, replaced by the next one, survives a
// reopen, and goes with the network.
func TestLastNickPerNetwork(t *testing.T) {
	ctx := context.Background()
	p := filepath.Join(t.TempDir(), "t.db")
	s, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	a, err := s.UpsertNetwork(ctx, Network{Name: "a", Host: "h", Port: 1, Nick: "me"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.UpsertNetwork(ctx, Network{Name: "b", Host: "h", Port: 1, Nick: "me"})
	if err != nil {
		t.Fatal(err)
	}
	if nick, err := s.LastNick(ctx, a); err != nil || nick != "" {
		t.Fatalf("LastNick before any = %q, %v; want empty", nick, err)
	}
	if err := s.SetLastNick(ctx, a, "me_"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetLastNick(ctx, a, "me__"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetLastNick(ctx, b, "other"); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s, err = Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if nick, err := s.LastNick(ctx, a); err != nil || nick != "me__" {
		t.Fatalf("LastNick(a) = %q, %v; want me__", nick, err)
	}
	if nick, _ := s.LastNick(ctx, b); nick != "other" {
		t.Fatalf("LastNick(b) = %q, want other", nick)
	}
	if err := s.DeleteNetwork(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if nick, _ := s.LastNick(ctx, a); nick != "" {
		t.Fatalf("LastNick of a deleted network = %q, want empty", nick)
	}
}
