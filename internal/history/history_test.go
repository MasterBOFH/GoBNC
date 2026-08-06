package history

import (
	"context"
	"path/filepath"
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
