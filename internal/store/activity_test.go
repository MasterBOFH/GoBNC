package store

import (
	"context"
	"testing"
	"time"
)

func TestBufferActivityUnread(t *testing.T) {
	s := openTemp(t)
	ctx := context.Background()
	nid, err := s.UpsertNetwork(ctx, Network{Name: "n", Host: "h", Port: 1, Nick: "me", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	add := func(i int, cmd, src, text string) int64 {
		id, err := s.InsertMessage(ctx, Message{NetworkID: nid, Target: "#c", Time: t0.Add(time.Duration(i) * time.Minute),
			Command: cmd, Source: src, Raw: "x", Text: text})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	add(0, "PRIVMSG", "a!u@h", "read already")
	add(2, "PRIVMSG", "a!u@h", "unread one")
	add(3, "NOTICE", "a!u@h", "\x01VERSION x\x01")     // CTCP reply: not a conversation line
	add(4, "PRIVMSG", "a!u@h", "\x01ACTION waves\x01") // ACTION counts
	add(5, "JOIN", "b!u@h", "")
	floorID := add(6, "PRIVMSG", "a!u@h", "below floor")
	last := add(7, "PRIVMSG", "a!u@h", "after floor")

	a, err := s.BufferActivity(ctx, nid, "#c", t0.Add(time.Minute), 0)
	if err != nil {
		t.Fatal(err)
	}
	if a.LastID != last || !a.LastTime.Equal(t0.Add(7*time.Minute)) {
		t.Fatalf("last = %d %v", a.LastID, a.LastTime)
	}
	texts := map[string]bool{}
	for _, l := range a.Unread {
		texts[l.Text] = true
	}
	if len(a.Unread) != 4 || !texts["unread one"] || !texts["\x01ACTION waves\x01"] || texts["\x01VERSION x\x01"] || texts["read already"] {
		t.Fatalf("unread = %+v", a.Unread)
	}
	a, _ = s.BufferActivity(ctx, nid, "#c", t0.Add(time.Minute), floorID)
	if len(a.Unread) != 1 || a.Unread[0].Text != "after floor" {
		t.Fatalf("unread above floor = %+v", a.Unread)
	}
	if a, _ := s.BufferActivity(ctx, nid, "#none", time.Time{}, 0); a.LastID != 0 || len(a.Unread) != 0 {
		t.Fatalf("empty buffer = %+v", a)
	}
}
