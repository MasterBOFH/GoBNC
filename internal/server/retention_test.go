package server

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/config"
	gobnclog "github.com/MasterBOFH/GoBNC/internal/log"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

func TestPruneHistoryRetention(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "t.db")
	cfg.HistoryRetentionDays = 1

	s, err := New(cfg, gobnclog.New("error", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	id, err := s.Store().UpsertNetwork(ctx, store.Network{
		Name: "n", Host: "h", Port: 1, Nick: "x", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().Add(-48 * time.Hour)
	recent := time.Now().UTC().Add(-time.Hour)
	if err := s.Store().InsertMessage(ctx, store.Message{
		NetworkID: id, Target: "#c", Time: old, Command: "PRIVMSG",
		Source: "a!b@c", Raw: ":a!b@c PRIVMSG #c :old", Text: "old",
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Store().InsertMessage(ctx, store.Message{
		NetworkID: id, Target: "#c", Time: recent, Command: "PRIVMSG",
		Source: "a!b@c", Raw: ":a!b@c PRIVMSG #c :new", Text: "new",
	}); err != nil {
		t.Fatal(err)
	}

	s.pruneHistory(ctx)

	msgs, err := s.Store().QueryMessages(ctx, store.HistoryQuery{
		NetworkID: id, Target: "#c", Latest: true, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].Text != "new" {
		t.Fatalf("want only recent message, got %#v", msgs)
	}
}
