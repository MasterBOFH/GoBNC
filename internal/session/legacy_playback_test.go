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

func openLegacyFixture(t *testing.T) (*store.Store, *history.Store, int64) {
	t.Helper()
	db, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	id, err := db.UpsertNetwork(context.Background(), store.Network{Name: "n", Host: "h", Port: 1, Nick: "me", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	return db, history.New(db), id
}

func storeLine(t *testing.T, hist *history.Store, id int64, ts time.Time, cmd, target, text string) {
	t.Helper()
	msg := irc.Message{
		Tags:    map[string]string{"time": ts.UTC().Format("2006-01-02T15:04:05.000Z")},
		Source:  "bob!u@h",
		Command: cmd,
		Params:  []string{target, text},
	}
	if cmd == "JOIN" || cmd == "PART" {
		msg.Params = []string{target}
		if text != "" && cmd == "PART" {
			msg.Params = []string{target, text}
		}
	}
	if err := hist.Store(context.Background(), history.Record{
		NetworkID: id, Target: target, Time: ts, Command: cmd,
		Source: msg.Source, Raw: msg.Encode(), Text: text,
	}); err != nil {
		t.Fatal(err)
	}
}

func sessionWithChan(t *testing.T, db *store.Store, hist *history.Store, id int64) *Session {
	t.Helper()
	s := New(store.Network{ID: id, Name: "n", Nick: "me"}, db, hist, nil, nil)
	s.registered = true
	s.mu.Lock()
	s.channels["#c"] = &ChannelState{Name: "#c", Members: map[string]struct{}{"me": {}, "bob": {}}}
	s.mu.Unlock()
	return s
}

func countCmds(sent []irc.Message, cmd string) int {
	n := 0
	for _, m := range sent {
		if m.Command == cmd {
			n++
		}
	}
	return n
}

func TestLegacyPlaybackOnAttach(t *testing.T) {
	db, hist, id := openLegacyFixture(t)
	t0 := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	for i := 0; i < 5; i++ {
		storeLine(t, hist, id, t0.Add(time.Duration(i)*time.Minute), "PRIVMSG", "#c", "line"+string(rune('0'+i)))
	}
	s := sessionWithChan(t, db, hist, id)

	legacy := &fakeDL{id: "legacy", caps: map[string]bool{}}
	if err := s.Attach(legacy); err != nil {
		t.Fatal(err)
	}
	if got := countCmds(legacy.sent, "PRIVMSG"); got != 5 {
		t.Fatalf("legacy attach expected 5 PRIVMSG, got %d", got)
	}

	legacy2 := &fakeDL{id: "legacy2", caps: map[string]bool{}}
	if err := s.Attach(legacy2); err != nil {
		t.Fatal(err)
	}
	if got := countCmds(legacy2.sent, "PRIVMSG"); got != 0 {
		t.Fatalf("second legacy attach should get 0 backlog, got %d", got)
	}
}

func TestLegacyPlaybackOnlyPRIVMSGandNOTICE(t *testing.T) {
	db, hist, id := openLegacyFixture(t)
	t0 := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	storeLine(t, hist, id, t0, "JOIN", "#c", "")
	storeLine(t, hist, id, t0.Add(time.Minute), "PRIVMSG", "#c", "hi")
	storeLine(t, hist, id, t0.Add(2*time.Minute), "NOTICE", "#c", "note")
	storeLine(t, hist, id, t0.Add(3*time.Minute), "PART", "#c", "bye")
	storeLine(t, hist, id, t0.Add(4*time.Minute), "TAGMSG", "#c", "")
	storeLine(t, hist, id, t0.Add(5*time.Minute), "TOPIC", "#c", "topic")
	storeLine(t, hist, id, t0.Add(6*time.Minute), "MODE", "#c", "+n")

	s := sessionWithChan(t, db, hist, id)
	d := &fakeDL{id: "legacy", caps: map[string]bool{"message-tags": true}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	for _, m := range d.sent {
		switch m.Command {
		case "PRIVMSG", "NOTICE", "001", "002", "003", "004", "005", "221", "375", "372", "376", "JOIN", "332", "353", "366", "CAP":
			// JOIN here is attach state JOIN, not history replay of stored JOIN.
			continue
		case "MODE":
			// Attach-burst own MODE (nick target), not history of channel MODE.
			if len(m.Params) > 0 && m.Params[0] != "" && m.Params[0][0] != '#' && m.Params[0][0] != '&' && m.Params[0][0] != '+' && m.Params[0][0] != '!' {
				continue
			}
			t.Fatalf("legacy replay must not send event/TAGMSG %s", m.Command)
		case "TAGMSG", "PART", "TOPIC", "KICK", "QUIT", "NICK":
			t.Fatalf("legacy replay must not send event/TAGMSG %s", m.Command)
		}
	}
	if countCmds(d.sent, "PRIVMSG") != 1 || countCmds(d.sent, "NOTICE") != 1 {
		t.Fatalf("priv=%d notice=%d in %#v", countCmds(d.sent, "PRIVMSG"), countCmds(d.sent, "NOTICE"), cmdsOf(d.sent))
	}
}

func cmdsOf(sent []irc.Message) []string {
	var out []string
	for _, m := range sent {
		out = append(out, m.Command)
	}
	return out
}

func TestChathistoryAttachSkipsLegacyPlayback(t *testing.T) {
	db, hist, id := openLegacyFixture(t)
	ts := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	storeLine(t, hist, id, ts, "PRIVMSG", "#c", "hi")
	s := sessionWithChan(t, db, hist, id)

	for _, caps := range []map[string]bool{
		{"chathistory": true, "batch": true, "server-time": true},
		{"draft/chathistory": true, "batch": true},
	} {
		d := &fakeDL{id: "ch", caps: caps}
		if err := s.Attach(d); err != nil {
			t.Fatal(err)
		}
		if countCmds(d.sent, "PRIVMSG") != 0 {
			t.Fatalf("caps=%v must not get legacy replay", caps)
		}
	}
	tsCur, err := s.getPlaybackCursor("#c")
	if err != nil {
		t.Fatal(err)
	}
	if tsCur != "" {
		t.Fatalf("CHATHISTORY attach must not advance cursor, got %q", tsCur)
	}
}

func TestCHATHISTORYDoesNotAdvancePlaybackCursor(t *testing.T) {
	db, hist, id := openLegacyFixture(t)
	t0 := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		storeLine(t, hist, id, t0.Add(time.Duration(i)*time.Minute), "PRIVMSG", "#c", "x")
	}
	s := New(store.Network{ID: id, Name: "n", Nick: "me"}, db, hist, nil, nil)
	sender := &fakeDL{id: "c", caps: map[string]bool{"chathistory": true, "batch": true, "server-time": true}}
	if err := hist.HandleCHATHISTORY(sender, id, irc.Message{
		Command: "CHATHISTORY",
		Params:  []string{"LATEST", "#c", "*", "10"},
	}); err != nil {
		t.Fatal(err)
	}
	if countCmds(sender.sent, "PRIVMSG") < 1 {
		t.Fatal("expected CHATHISTORY lines")
	}
	if tsCur, _ := s.getPlaybackCursor("#c"); tsCur != "" {
		t.Fatalf("CHATHISTORY must not set playback cursor: %q", tsCur)
	}
}

func TestLiveLegacyDeliveryAdvancesCursor(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	s.registered = true
	d := &fakeDL{id: "a", caps: map[string]bool{}}
	_ = s.Attach(d)
	d.sent = nil
	ts := "2024-06-01T12:00:00.000Z"
	s.HandleMessage(irc.Message{
		Tags:    map[string]string{"time": ts, "msgid": "1"},
		Source:  "bob!u@h",
		Command: "PRIVMSG",
		Params:  []string{"#c", "hi"},
	})
	got, _ := s.getPlaybackCursor("#c")
	if got != ts {
		t.Fatalf("cursor=%q want %q", got, ts)
	}
}

func TestChathistoryOnlyLiveDoesNotAdvanceCursor(t *testing.T) {
	db, hist, id := openLegacyFixture(t)
	s := sessionWithChan(t, db, hist, id)
	ch := &fakeDL{id: "ch", caps: map[string]bool{"chathistory": true, "echo-message": true}}
	_ = s.Attach(ch)
	ch.sent = nil

	ts := time.Now().UTC().Add(-time.Minute)
	storeLine(t, hist, id, ts, "PRIVMSG", "#c", "offline-for-legacy")
	s.HandleMessage(irc.Message{
		Tags:    map[string]string{"time": ts.Format("2006-01-02T15:04:05.000Z"), "msgid": "m1"},
		Source:  "bob!u@h",
		Command: "PRIVMSG",
		Params:  []string{"#c", "live"},
	})
	if cur, _ := s.getPlaybackCursor("#c"); cur != "" {
		t.Fatalf("chathistory-only live must not advance cursor: %q", cur)
	}

	legacy := &fakeDL{id: "legacy", caps: map[string]bool{}}
	if err := s.Attach(legacy); err != nil {
		t.Fatal(err)
	}
	if got := countCmds(legacy.sent, "PRIVMSG"); got < 1 {
		t.Fatalf("legacy should still get backlog after chathistory-only live, got %d", got)
	}
}

func TestLegacyPlaybackHorizonSkipsAncient(t *testing.T) {
	oldH := history.LegacyPlaybackHorizon
	history.LegacyPlaybackHorizon = 24 * time.Hour
	t.Cleanup(func() { history.LegacyPlaybackHorizon = oldH })

	db, hist, id := openLegacyFixture(t)
	storeLine(t, hist, id, time.Now().UTC().Add(-48*time.Hour), "PRIVMSG", "#c", "old")
	storeLine(t, hist, id, time.Now().UTC().Add(-time.Hour), "PRIVMSG", "#c", "new")
	s := sessionWithChan(t, db, hist, id)
	d := &fakeDL{id: "l", caps: map[string]bool{}}
	_ = s.Attach(d)
	if got := countCmds(d.sent, "PRIVMSG"); got != 1 {
		t.Fatalf("horizon: want 1 recent PRIVMSG, got %d", got)
	}
}

func TestLegacyPlaybackPartialBurstResumes(t *testing.T) {
	db, hist, id := openLegacyFixture(t)
	hist.SetLegacyPlaybackMax(2)
	t.Cleanup(func() { hist.SetLegacyPlaybackMax(history.DefaultLegacyPlaybackMax) })

	t0 := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	for i := 0; i < 5; i++ {
		storeLine(t, hist, id, t0.Add(time.Duration(i)*time.Minute), "PRIVMSG", "#c", "m"+string(rune('0'+i)))
	}
	s := sessionWithChan(t, db, hist, id)

	a := &fakeDL{id: "a", caps: map[string]bool{}}
	_ = s.Attach(a)
	if got := countCmds(a.sent, "PRIVMSG"); got != 2 {
		t.Fatalf("first burst want 2, got %d", got)
	}
	b := &fakeDL{id: "b", caps: map[string]bool{}}
	_ = s.Attach(b)
	if got := countCmds(b.sent, "PRIVMSG"); got != 2 {
		t.Fatalf("second burst want next 2, got %d", got)
	}
	c := &fakeDL{id: "c", caps: map[string]bool{}}
	_ = s.Attach(c)
	if got := countCmds(c.sent, "PRIVMSG"); got != 1 {
		t.Fatalf("third burst want last 1, got %d", got)
	}
}

func TestLegacyPlaybackSkipsEmptyRaw(t *testing.T) {
	db, hist, id := openLegacyFixture(t)
	ts := time.Now().UTC().Add(-time.Hour)
	if err := hist.Store(context.Background(), history.Record{
		NetworkID: id, Target: "#c", Time: ts, Command: "PRIVMSG",
		Source: "bob!u@h", Raw: "", Text: "x",
	}); err != nil {
		t.Fatal(err)
	}
	storeLine(t, hist, id, ts.Add(time.Minute), "PRIVMSG", "#c", "ok")
	s := sessionWithChan(t, db, hist, id)
	d := &fakeDL{id: "l", caps: map[string]bool{}}
	_ = s.Attach(d)
	if got := countCmds(d.sent, "PRIVMSG"); got != 1 {
		t.Fatalf("want 1 playable line, got %d", got)
	}
}

func TestLegacyPlaybackNOTICEIncluded(t *testing.T) {
	db, hist, id := openLegacyFixture(t)
	t0 := time.Now().UTC().Add(-time.Hour)
	storeLine(t, hist, id, t0, "NOTICE", "#c", "n1")
	storeLine(t, hist, id, t0.Add(time.Minute), "PRIVMSG", "#c", "p1")
	s := sessionWithChan(t, db, hist, id)
	d := &fakeDL{id: "l", caps: map[string]bool{}}
	_ = s.Attach(d)
	if countCmds(d.sent, "NOTICE") != 1 || countCmds(d.sent, "PRIVMSG") != 1 {
		t.Fatalf("notice=%d priv=%d", countCmds(d.sent, "NOTICE"), countCmds(d.sent, "PRIVMSG"))
	}
}

func TestLiveFanoutMsgIDMatchesCHATHISTORY(t *testing.T) {
	db, hist, id := openLegacyFixture(t)
	s := sessionWithChan(t, db, hist, id)
	// Clear pre-seeded channel so the live JOIN is the first history line for #join.
	s.mu.Lock()
	delete(s.channels, "#c")
	s.mu.Unlock()

	d := &fakeDL{id: "d", caps: map[string]bool{
		"message-tags": true, "chathistory": true, "batch": true,
		"event-playback": true, "server-time": true,
	}}
	_ = s.Attach(d)
	d.clearSent()

	s.HandleMessage(irc.Message{
		Source:  "me!u@h",
		Command: "JOIN",
		Params:  []string{"#join"},
	})
	var joinMsgID string
	for _, m := range d.snapshot() {
		if m.Command == "JOIN" && len(m.Params) > 0 && m.Params[0] == "#join" {
			joinMsgID = m.Tags["msgid"]
			break
		}
	}
	if joinMsgID == "" {
		t.Fatalf("self-JOIN missing msgid: %+v", d.snapshot())
	}

	ch := &fakeDL{id: "ch", caps: map[string]bool{
		"chathistory": true, "message-tags": true, "batch": true, "event-playback": true,
	}}
	if err := hist.HandleCHATHISTORY(ch, id, irc.Message{
		Command: "CHATHISTORY",
		Params:  []string{"LATEST", "#join", "*", "10"},
	}); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, m := range ch.snapshot() {
		if m.Command != "JOIN" {
			continue
		}
		found = true
		if m.Tags["msgid"] != joinMsgID {
			t.Fatalf("CHATHISTORY JOIN msgid=%q want live self-JOIN msgid %q", m.Tags["msgid"], joinMsgID)
		}
	}
	if !found {
		t.Fatal("CHATHISTORY returned no JOIN (need event-playback)")
	}
}
