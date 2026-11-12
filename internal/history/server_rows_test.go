package history

import (
	"context"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// Rete A3131: the Server buffer's module-only rows (burst numerics, the uplink's ERROR, State
// rows) are stored under "*". An IRC client asking CHATHISTORY for "*", with event-playback
// (every stored command), gets an empty batch, and TARGETS does not list it.
func TestCHATHISTORYNeverServesTheServerRows(t *testing.T) {
	h, id, t0 := seedHistory(t)
	ctx := context.Background()
	for i, raw := range []string{":srv.example 001 x :Welcome", "ERROR :Closing Link"} {
		msg, _ := irc.Parse(raw)
		if _, err := h.Store(ctx, Record{NetworkID: id, Target: store.ServerRowsTarget, Time: t0.Add(time.Duration(i) * time.Second),
			Command: msg.Command, Source: msg.Source, Raw: raw, Text: msg.Trailing()}); err != nil {
			t.Fatal(err)
		}
	}
	caps := map[string]bool{"chathistory": true, "batch": true, "server-time": true, "event-playback": true, "message-tags": true}
	for _, params := range [][]string{
		{"LATEST", store.ServerRowsTarget, "*", "50"},
		{"AFTER", store.ServerRowsTarget, "timestamp=" + t0.Add(-time.Hour).Format(time.RFC3339Nano), "50"},
	} {
		s := &fakeSender{caps: caps}
		if err := h.HandleCHATHISTORY(s, id, irc.Message{Command: "CHATHISTORY", Params: params}); err != nil {
			t.Fatal(err)
		}
		for _, m := range s.sent {
			if m.Command != "BATCH" {
				t.Fatalf("CHATHISTORY %v sent %q", params, m.Encode())
			}
		}
	}
	s := &fakeSender{caps: caps}
	lo, hi := t0.Add(-time.Hour).Format(time.RFC3339Nano), t0.Add(time.Hour).Format(time.RFC3339Nano)
	if err := h.HandleCHATHISTORY(s, id, irc.Message{Command: "CHATHISTORY",
		Params: []string{"TARGETS", "timestamp=" + lo, "timestamp=" + hi, "50"}}); err != nil {
		t.Fatal(err)
	}
	for _, m := range s.sent {
		if m.Command == "CHATHISTORY" && m.Param(1) == store.ServerRowsTarget {
			t.Fatalf("TARGETS lists %q", m.Encode())
		}
	}
}
