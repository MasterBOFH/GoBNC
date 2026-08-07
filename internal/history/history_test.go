package history

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

type fakeSender struct {
	caps map[string]bool
	sent []irc.Message
}

func (f *fakeSender) Send(m irc.Message) error {
	f.sent = append(f.sent, m)
	return nil
}
func (f *fakeSender) HasCap(n string) bool { return f.caps[n] }

func seedHistory(t *testing.T) (*Store, int64, time.Time) {
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
	for i := 0; i < 10; i++ {
		ts := t0.Add(time.Duration(i) * time.Minute)
		msg := irc.Message{
			Tags:    map[string]string{"time": ts.Format("2006-01-02T15:04:05.000Z")},
			Source:  "a!b@c",
			Command: "PRIVMSG",
			Params:  []string{"#c", "m" + string(rune('0'+i))},
		}
		if err := h.Store(ctx, Record{
			NetworkID: id, Target: "#c", Time: ts, Command: "PRIVMSG", Source: "a!b@c",
			Raw: msg.Encode(), Text: msg.Trailing(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	return h, id, t0
}

func countPRIVMSG(sent []irc.Message) int {
	n := 0
	for _, m := range sent {
		if m.Command == "PRIVMSG" {
			n++
		}
	}
	return n
}

func TestCHATHISTORYEventPlayback(t *testing.T) {
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

	storeLine := func(i int, cmd string, raw string) {
		ts := t0.Add(time.Duration(i) * time.Minute)
		if err := h.Store(ctx, Record{
			NetworkID: id, Target: "#c", Time: ts, Command: cmd, Source: "a!b@c",
			Raw: raw, Text: "",
		}); err != nil {
			t.Fatal(err)
		}
	}
	storeLine(0, "PRIVMSG", ":a!b@c PRIVMSG #c :hi")
	storeLine(1, "JOIN", ":bob!u@h JOIN #c")
	storeLine(2, "PRIVMSG", ":a!b@c PRIVMSG #c :there")
	storeLine(3, "PART", ":bob!u@h PART #c :bye")
	storeLine(4, "MODE", ":chan!serv@h MODE #c +o a")

	// Without event-playback: only PRIVMSG/NOTICE.
	plain := &fakeSender{caps: map[string]bool{"chathistory": true}}
	if err := h.HandleCHATHISTORY(plain, id, irc.Message{
		Command: "CHATHISTORY",
		Params:  []string{"LATEST", "#c", "*", "10"},
	}); err != nil {
		t.Fatal(err)
	}
	if countPRIVMSG(plain.sent) != 2 {
		t.Fatalf("plain privmsgs=%d sent=%v", countPRIVMSG(plain.sent), cmds(plain.sent))
	}
	for _, m := range plain.sent {
		if m.Command == "JOIN" || m.Command == "PART" || m.Command == "MODE" {
			t.Fatalf("plain client got event %s", m.Command)
		}
	}

	// With draft/event-playback: include JOIN/PART/MODE.
	full := &fakeSender{caps: map[string]bool{
		"chathistory": true, "draft/event-playback": true,
	}}
	if err := h.HandleCHATHISTORY(full, id, irc.Message{
		Command: "CHATHISTORY",
		Params:  []string{"LATEST", "#c", "*", "10"},
	}); err != nil {
		t.Fatal(err)
	}
	got := cmds(full.sent)
	if countPRIVMSG(full.sent) != 2 {
		t.Fatalf("full privmsgs=%d got=%v", countPRIVMSG(full.sent), got)
	}
	if !containsCmd(got, "JOIN") || !containsCmd(got, "PART") || !containsCmd(got, "MODE") {
		t.Fatalf("expected events in batch: %v", got)
	}
}

func cmds(sent []irc.Message) []string {
	var out []string
	for _, m := range sent {
		if m.Command == "BATCH" {
			continue
		}
		out = append(out, m.Command)
	}
	return out
}

func containsCmd(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}


func TestCHATHISTORYMsgIDMatchesStoredWire(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	netID, err := db.UpsertNetwork(ctx, store.Network{Name: "n", Host: "h", Port: 1, Nick: "x", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	h := New(db)
	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	wireID := "wire-msgid-abc123"
	// Same encoding path as live fan-out after ensureMessageID (Raw rebuilt with msgid).
	live := irc.Message{
		Tags: map[string]string{
			"time":  ts.Format("2006-01-02T15:04:05.000Z"),
			"msgid": wireID,
		},
		Source:  "bob!u@h",
		Command: "PRIVMSG",
		Params:  []string{"#c", "hello"},
	}
	wire := live.Encode()
	if !strings.Contains(wire, "msgid="+wireID) {
		t.Fatalf("wire form missing msgid: %q", wire)
	}
	if err := h.Store(ctx, Record{
		NetworkID: netID, Target: "#c", Time: ts, MsgID: wireID,
		Command: "PRIVMSG", Source: live.Source, Raw: wire, Text: "hello",
	}); err != nil {
		t.Fatal(err)
	}

	s := &fakeSender{caps: map[string]bool{"chathistory": true, "message-tags": true, "batch": true}}
	if err := h.HandleCHATHISTORY(s, netID, irc.Message{
		Command: "CHATHISTORY",
		Params:  []string{"LATEST", "#c", "*", "5"},
	}); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, m := range s.sent {
		if m.Command != "PRIVMSG" {
			continue
		}
		found = true
		got, ok := m.Tag("msgid")
		if !ok || got != wireID {
			t.Fatalf("CHATHISTORY msgid=%q want wire %q tags=%v", got, wireID, m.Tags)
		}
		if m.Wire() != "" && !strings.Contains(m.Wire(), "msgid="+wireID) {
			t.Fatalf("playback wire missing msgid: %q", m.Wire())
		}
	}
	if !found {
		t.Fatal("no PRIVMSG in CHATHISTORY reply")
	}
}

func TestCHATHISTORYMsgIDFromColumnWhenRawLacksTag(t *testing.T) {
	db, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	netID, err := db.UpsertNetwork(ctx, store.Network{Name: "n", Host: "h", Port: 1, Nick: "x", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	h := New(db)
	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	wireID := "column-only-id"
	// Raw without msgid tag, but column has the wire id (ingestion recorded it).
	if err := h.Store(ctx, Record{
		NetworkID: netID, Target: "#c", Time: ts, MsgID: wireID,
		Command: "PRIVMSG", Source: "a!b@c",
		Raw: ":a!b@c PRIVMSG #c :hi", Text: "hi",
	}); err != nil {
		t.Fatal(err)
	}
	s := &fakeSender{caps: map[string]bool{"chathistory": true}}
	_ = h.HandleCHATHISTORY(s, netID, irc.Message{
		Command: "CHATHISTORY", Params: []string{"LATEST", "#c", "*", "5"},
	})
	for _, m := range s.sent {
		if m.Command == "PRIVMSG" && m.Tags["msgid"] != wireID {
			t.Fatalf("got %q want %q", m.Tags["msgid"], wireID)
		}
	}
}

func TestCHATHISTORYLatestBefore(t *testing.T) {
	h, id, t0 := seedHistory(t)
	s := &fakeSender{caps: map[string]bool{"chathistory": true, "batch": true, "server-time": true}}
	before := t0.Add(3 * time.Minute).Format(time.RFC3339Nano)
	if err := h.HandleCHATHISTORY(s, id, irc.Message{
		Command: "CHATHISTORY",
		Params:  []string{"BEFORE", "#c", before, "10"},
	}); err != nil {
		t.Fatal(err)
	}
	if countPRIVMSG(s.sent) != 3 {
		t.Fatalf("before privmsgs=%d", countPRIVMSG(s.sent))
	}

	s2 := &fakeSender{caps: map[string]bool{"chathistory": true}}
	_ = h.HandleCHATHISTORY(s2, id, irc.Message{
		Command: "CHATHISTORY",
		Params:  []string{"LATEST", "#c", "*", "2"},
	})
	if countPRIVMSG(s2.sent) != 2 {
		t.Fatalf("latest=%d", countPRIVMSG(s2.sent))
	}
}

func TestCHATHISTORYAfterAroundBetween(t *testing.T) {
	h, id, t0 := seedHistory(t)

	s := &fakeSender{caps: map[string]bool{"chathistory": true}}
	after := t0.Add(7 * time.Minute).Format(time.RFC3339Nano)
	_ = h.HandleCHATHISTORY(s, id, irc.Message{
		Command: "CHATHISTORY",
		Params:  []string{"AFTER", "#c", after, "10"},
	})
	// messages at 8,9 → 2
	if countPRIVMSG(s.sent) != 2 {
		t.Fatalf("after=%d %+v", countPRIVMSG(s.sent), s.sent)
	}

	s2 := &fakeSender{caps: map[string]bool{"chathistory": true}}
	around := t0.Add(5 * time.Minute).Format(time.RFC3339Nano)
	_ = h.HandleCHATHISTORY(s2, id, irc.Message{
		Command: "CHATHISTORY",
		Params:  []string{"AROUND", "#c", around, "4"},
	})
	n := countPRIVMSG(s2.sent)
	if n < 3 || n > 4 {
		t.Fatalf("around=%d", n)
	}

	s3 := &fakeSender{caps: map[string]bool{"chathistory": true}}
	a := t0.Add(2 * time.Minute).Format(time.RFC3339Nano)
	b := t0.Add(6 * time.Minute).Format(time.RFC3339Nano)
	_ = h.HandleCHATHISTORY(s3, id, irc.Message{
		Command: "CHATHISTORY",
		Params:  []string{"BETWEEN", "#c", a, b, "10"},
	})
	// strictly between 2 and 6 → 3,4,5 → 3 msgs
	if countPRIVMSG(s3.sent) != 3 {
		t.Fatalf("between=%d", countPRIVMSG(s3.sent))
	}
}

func TestCHATHISTORYBeforeMsgID(t *testing.T) {
	h, netID := seedMsgIDHistory(t)

	s := &fakeSender{caps: map[string]bool{"chathistory": true, "batch": true, "message-tags": true}}
	if err := h.HandleCHATHISTORY(s, netID, irc.Message{
		Command: "CHATHISTORY",
		Params:  []string{"BEFORE", "#c", "msgid=m3", "10"},
	}); err != nil {
		t.Fatal(err)
	}
	got := privmsgIDs(s.sent)
	if len(got) != 3 || got[0] != "m0" || got[2] != "m2" {
		t.Fatalf("BEFORE msgid=m3 got %v", got)
	}
	for _, id := range got {
		if id == "m3" {
			t.Fatal("BEFORE must exclude the selector msgid")
		}
	}

	s2 := &fakeSender{caps: map[string]bool{"chathistory": true, "message-tags": true}}
	_ = h.HandleCHATHISTORY(s2, netID, irc.Message{
		Command: "CHATHISTORY",
		Params:  []string{"AFTER", "#c", "msgid=m2", "10"},
	})
	got = privmsgIDs(s2.sent)
	if len(got) != 2 || got[0] != "m3" || got[1] != "m4" {
		t.Fatalf("AFTER msgid=m2 got %v", got)
	}

	// Unknown msgid → empty batch, not FAIL.
	s3 := &fakeSender{caps: map[string]bool{"chathistory": true, "batch": true}}
	_ = h.HandleCHATHISTORY(s3, netID, irc.Message{
		Command: "CHATHISTORY",
		Params:  []string{"BEFORE", "#c", "msgid=does-not-exist", "10"},
	})
	if countPRIVMSG(s3.sent) != 0 {
		t.Fatalf("unknown msgid should be empty, got %d", countPRIVMSG(s3.sent))
	}
	var batches int
	for _, m := range s3.sent {
		if m.Command == "BATCH" {
			batches++
		}
		if m.Command == "FAIL" {
			t.Fatalf("unexpected FAIL: %+v", m)
		}
	}
	if batches != 2 {
		t.Fatalf("want empty +/- batch, got %d BATCH lines", batches)
	}
}

func TestCHATHISTORYMsgIDSelectorsComprehensive(t *testing.T) {
	h, netID := seedMsgIDHistory(t)

	t.Run("LATEST after msgid", func(t *testing.T) {
		s := &fakeSender{caps: map[string]bool{"chathistory": true, "message-tags": true}}
		_ = h.HandleCHATHISTORY(s, netID, irc.Message{
			Command: "CHATHISTORY",
			Params:  []string{"LATEST", "#c", "msgid=m1", "10"},
		})
		got := privmsgIDs(s.sent)
		// most recent among m2,m3,m4 (all after m1)
		if len(got) != 3 || got[0] != "m2" || got[2] != "m4" {
			t.Fatalf("LATEST msgid=m1 got %v", got)
		}
		for _, id := range got {
			if id == "m0" || id == "m1" {
				t.Fatalf("LATEST must exclude at/before selector: %v", got)
			}
		}
	})

	t.Run("AROUND msgid", func(t *testing.T) {
		s := &fakeSender{caps: map[string]bool{"chathistory": true, "message-tags": true}}
		_ = h.HandleCHATHISTORY(s, netID, irc.Message{
			Command: "CHATHISTORY",
			Params:  []string{"AROUND", "#c", "msgid=m2", "4"},
		})
		got := privmsgIDs(s.sent)
		if len(got) < 3 || len(got) > 4 {
			t.Fatalf("AROUND msgid=m2 count=%d ids=%v", len(got), got)
		}
		found := false
		for _, id := range got {
			if id == "m2" {
				found = true
			}
		}
		if !found {
			t.Fatalf("AROUND should include anchor msgid: %v", got)
		}
	})

	t.Run("BETWEEN msgids", func(t *testing.T) {
		s := &fakeSender{caps: map[string]bool{"chathistory": true, "message-tags": true}}
		_ = h.HandleCHATHISTORY(s, netID, irc.Message{
			Command: "CHATHISTORY",
			Params:  []string{"BETWEEN", "#c", "msgid=m1", "msgid=m4", "10"},
		})
		got := privmsgIDs(s.sent)
		if len(got) != 2 || got[0] != "m2" || got[1] != "m3" {
			t.Fatalf("BETWEEN m1..m4 got %v", got)
		}
	})

	t.Run("BETWEEN msgid order independent", func(t *testing.T) {
		s := &fakeSender{caps: map[string]bool{"chathistory": true, "message-tags": true}}
		_ = h.HandleCHATHISTORY(s, netID, irc.Message{
			Command: "CHATHISTORY",
			Params:  []string{"BETWEEN", "#c", "msgid=m4", "msgid=m1", "10"},
		})
		got := privmsgIDs(s.sent)
		if len(got) != 2 || got[0] != "m2" || got[1] != "m3" {
			t.Fatalf("BETWEEN reversed got %v", got)
		}
	})

	t.Run("timestamp= prefix still works", func(t *testing.T) {
		s := &fakeSender{caps: map[string]bool{"chathistory": true, "message-tags": true}}
		ts := time.Date(2024, 6, 1, 12, 3, 0, 0, time.UTC).Format(time.RFC3339Nano)
		_ = h.HandleCHATHISTORY(s, netID, irc.Message{
			Command: "CHATHISTORY",
			Params:  []string{"BEFORE", "#c", "timestamp="+ts, "10"},
		})
		got := privmsgIDs(s.sent)
		if len(got) != 3 || got[2] != "m2" {
			t.Fatalf("BEFORE timestamp= got %v", got)
		}
	})

	t.Run("invalid selector FAILs", func(t *testing.T) {
		s := &fakeSender{caps: map[string]bool{"chathistory": true}}
		_ = h.HandleCHATHISTORY(s, netID, irc.Message{
			Command: "CHATHISTORY",
			Params:  []string{"BEFORE", "#c", "not-a-selector", "10"},
		})
		found := false
		for _, m := range s.sent {
			if m.Command == "FAIL" && m.Param(1) == "INVALID_PARAMS" {
				found = true
			}
		}
		if !found {
			t.Fatalf("want FAIL INVALID_PARAMS, got %+v", s.sent)
		}
	})

	t.Run("empty msgid= FAILs", func(t *testing.T) {
		s := &fakeSender{caps: map[string]bool{"chathistory": true}}
		_ = h.HandleCHATHISTORY(s, netID, irc.Message{
			Command: "CHATHISTORY",
			Params:  []string{"BEFORE", "#c", "msgid=", "10"},
		})
		found := false
		for _, m := range s.sent {
			if m.Command == "FAIL" {
				found = true
			}
		}
		if !found {
			t.Fatalf("want FAIL for empty msgid, got %+v", s.sent)
		}
	})
}

func seedMsgIDHistory(t *testing.T) (*Store, int64) {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	netID, err := db.UpsertNetwork(ctx, store.Network{Name: "n", Host: "h", Port: 1, Nick: "x", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	h := New(db)
	t0 := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	ids := []string{"m0", "m1", "m2", "m3", "m4"}
	for i, mid := range ids {
		ts := t0.Add(time.Duration(i) * time.Minute)
		msg := irc.Message{
			Tags:    map[string]string{"time": ts.Format("2006-01-02T15:04:05.000Z"), "msgid": mid},
			Source:  "a!b@c",
			Command: "PRIVMSG",
			Params:  []string{"#c", "line" + string(rune('0'+i))},
		}
		if err := h.Store(ctx, Record{
			NetworkID: netID, Target: "#c", Time: ts, MsgID: mid,
			Command: "PRIVMSG", Source: msg.Source, Raw: msg.Encode(), Text: msg.Trailing(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	return h, netID
}

func privmsgIDs(sent []irc.Message) []string {
	var got []string
	for _, m := range sent {
		if m.Command == "PRIVMSG" {
			got = append(got, m.Tags["msgid"])
		}
	}
	return got
}
