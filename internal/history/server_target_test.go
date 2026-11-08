package history

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// The "Server" buffer (server notices and $mask lines, stored under its folded key for
// modules) is no chathistory target: IRCv3 TARGETS lists channels and nicks. It is never
// listed in TARGETS nor served to an IRC client, which gets INVALID_TARGET; its rows stay.
func TestCHATHISTORYServerBufferIsNoTarget(t *testing.T) {
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
	t0 := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	for i, target := range []string{"#a", "server", "bob"} {
		if _, err := h.Store(ctx, Record{NetworkID: id, Target: target, Time: t0.Add(time.Duration(i) * time.Minute),
			Command: "NOTICE", Source: "srv.example", Raw: ":srv.example NOTICE " + target + " :hi", Text: "hi"}); err != nil {
			t.Fatal(err)
		}
	}
	s := &fakeSender{caps: map[string]bool{"chathistory": true}}
	if err := h.HandleCHATHISTORY(s, id, irc.Message{Command: "CHATHISTORY", Params: []string{"TARGETS",
		t0.Add(-time.Minute).Format(time.RFC3339Nano), t0.Add(time.Hour).Format(time.RFC3339Nano), "2"}}); err != nil {
		t.Fatal(err)
	}
	var targets []string
	for _, m := range s.sent {
		if m.Command == "CHATHISTORY" && len(m.Params) >= 2 && m.Params[0] == "TARGETS" {
			targets = append(targets, m.Params[1])
		}
	}
	if len(targets) != 2 || targets[0] != "#a" || targets[1] != "bob" {
		t.Fatalf("TARGETS = %v, want [#a bob] (limit 2 counts real targets only)", targets)
	}

	for _, sub := range [][]string{{"LATEST", "server", "*", "10"}, {"BEFORE", "server", "timestamp=" + t0.Add(time.Hour).Format(time.RFC3339Nano), "10"}} {
		s := &fakeSender{caps: map[string]bool{"chathistory": true}}
		if err := h.HandleCHATHISTORY(s, id, irc.Message{Command: "CHATHISTORY", Params: sub}); err != nil {
			t.Fatal(err)
		}
		if len(s.sent) != 1 || s.sent[0].Command != "FAIL" || s.sent[0].Param(1) != "INVALID_TARGET" ||
			s.sent[0].Param(2) != sub[0] || s.sent[0].Param(3) != "server" {
			t.Fatalf("%s server: sent %+v", sub[0], s.sent)
		}
	}
	if ok, _ := db.HasTarget(ctx, id, "server"); !ok {
		t.Fatal("the Server buffer's rows are gone")
	}
}
