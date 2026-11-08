package history

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// serverTargetFixture stores one line each under #a, a server's name (where its notices are
// stored), a real nick Server's query "server" and bob, a minute apart.
func serverTargetFixture(t *testing.T) (*Store, int64, time.Time) {
	t.Helper()
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
	for i, l := range []struct{ target, command, source, raw string }{
		{"#a", "NOTICE", "srv.example", ":srv.example NOTICE #a :hi"},
		{"srv.example", "NOTICE", "srv.example", ":srv.example NOTICE x :*** Notice -- snotice"},
		{"server", "PRIVMSG", "Server!s@h", ":Server!s@h PRIVMSG x :hi from a nick"},
		{"bob", "PRIVMSG", "bob!b@h", ":bob!b@h PRIVMSG x :hi"},
	} {
		if _, err := h.Store(ctx, Record{NetworkID: id, Target: l.target, Time: t0.Add(time.Duration(i) * time.Minute),
			Command: l.command, Source: l.source, Raw: l.raw, Text: "hi"}); err != nil {
			t.Fatal(err)
		}
	}
	return h, id, t0
}

func targetsOf(sent []irc.Message) []string {
	var targets []string
	for _, m := range sent {
		if m.Command == "CHATHISTORY" && len(m.Params) >= 2 && m.Params[0] == "TARGETS" {
			targets = append(targets, m.Params[1])
		}
	}
	return targets
}

func countCommand(sent []irc.Message, cmd string) int {
	n := 0
	for _, m := range sent {
		if m.Command == cmd {
			n++
		}
	}
	return n
}

// A server's notices are stored under its own name, an ordinary chathistory target for every
// client: TARGETS lists it beside channels and nicks, and CHATHISTORY serves it. A real nick
// Server ("server") is an ordinary query again.
func TestCHATHISTORYServerNameIsAnOrdinaryTarget(t *testing.T) {
	h, id, t0 := serverTargetFixture(t)
	s := &fakeSender{caps: map[string]bool{"chathistory": true}}
	if err := h.HandleCHATHISTORY(s, id, irc.Message{Command: "CHATHISTORY", Params: []string{"TARGETS",
		t0.Add(-time.Minute).Format(time.RFC3339Nano), t0.Add(time.Hour).Format(time.RFC3339Nano), "10"}}); err != nil {
		t.Fatal(err)
	}
	if got := targetsOf(s.sent); len(got) != 4 || got[0] != "#a" || got[1] != "srv.example" || got[2] != "server" || got[3] != "bob" {
		t.Fatalf("TARGETS = %v, want [#a srv.example server bob]", got)
	}
	for _, tc := range []struct{ target, cmd string }{{"srv.example", "NOTICE"}, {"server", "PRIVMSG"}} {
		for _, sub := range [][]string{{"LATEST", tc.target, "*", "10"}, {"BEFORE", tc.target, "timestamp=" + t0.Add(time.Hour).Format(time.RFC3339Nano), "10"}} {
			q := &fakeSender{caps: map[string]bool{"chathistory": true, "batch": true}}
			if err := h.HandleCHATHISTORY(q, id, irc.Message{Command: "CHATHISTORY", Params: sub}); err != nil {
				t.Fatal(err)
			}
			if countCommand(q.sent, tc.cmd) != 1 || countCommand(q.sent, "FAIL") != 0 {
				t.Fatalf("%v: sent %+v, want its one line", sub, q.sent)
			}
		}
	}
}
