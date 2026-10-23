package session

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/history"
	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// TestCHATHISTORYTargetsThroughClientPath sends TARGETS through
// HandleClientMessage, not HandleCHATHISTORY directly: the client path
// casemaps Params[1] as a target, and for TARGETS that's a timestamp —
// lowercasing its "T"/"Z" made every TARGETS request FAIL INVALID_PARAMS.
func TestCHATHISTORYTargetsThroughClientPath(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	if _, err := db.UpsertNetwork(ctx, store.Network{Name: "n", Host: "h", Port: 1, Nick: "me", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	netw, err := db.NetworkByName(ctx, "n")
	if err != nil {
		t.Fatal(err)
	}
	s := New(netw, db, history.New(db), nil, nil)
	s.registered = true

	now := time.Now().UTC()
	s.HandleMessage(irc.Message{Source: "me!u@h", Command: "JOIN", Params: []string{"#Chan"}})
	s.HandleMessage(irc.Message{
		Tags:    map[string]string{"time": now.Format("2006-01-02T15:04:05.000Z"), "msgid": "m1"},
		Source:  "bob!u@h",
		Command: "PRIVMSG",
		Params:  []string{"#Chan", "hi"},
	})

	d := &fakeDL{id: "c1", caps: map[string]bool{"chathistory": true, "batch": true, "message-tags": true, "server-time": true}}
	if err := s.HandleClientMessage(d, irc.Message{
		Command: "CHATHISTORY",
		Params: []string{"TARGETS",
			"timestamp=" + now.Add(time.Minute).Format("2006-01-02T15:04:05.000Z"),
			"timestamp=" + now.Add(-time.Hour).Format("2006-01-02T15:04:05.000Z"),
			"10"},
	}); err != nil {
		t.Fatal(err)
	}
	var targets []string
	for _, m := range d.sent {
		if m.Command == "FAIL" {
			t.Fatalf("TARGETS with valid timestamps got FAIL: %+v", m.Params)
		}
		if m.Command == "CHATHISTORY" && m.Param(0) == "TARGETS" {
			targets = append(targets, m.Param(1))
		}
	}
	if len(targets) != 1 || s.isupport.CaseMapping.Canonical(targets[0]) != "#chan" {
		t.Fatalf("TARGETS=%v, want [#chan]; sent=%+v", targets, d.sent)
	}
}
