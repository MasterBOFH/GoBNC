package session

import (
	"context"
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

func storedTargets(t *testing.T, s *Session) map[string]bool {
	t.Helper()
	targets, err := s.store.DistinctTargets(context.Background(), s.Network.ID)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, tg := range targets {
		out[tg] = true
	}
	return out
}

func assertOpensFold(t *testing.T, s *Session, sink *recSink) {
	t.Helper()
	for _, ev := range sink.events() {
		if e, ok := ev.(wire.BufferOpenEvent); ok && s.Fold(e.Buffer) != e.Key {
			t.Fatalf("buffer_open %+v: display does not fold to its key", e)
		}
	}
	rows, err := s.store.ListOpenBuffers(context.Background(), "phone", s.Network.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if s.Fold(r.Display) != r.Target {
			t.Fatalf("open row %+v: display does not fold to its key", r)
		}
	}
}

// A3005: a private message addressed to a nick the session does not know as its own (self not
// set yet, or still the configured nick while the server gave us another) is the sender's
// query: stored under the sender and reopening the sender's query, never under the nick it was
// addressed to.
func TestDMWhileSelfUnknownIsTheSendersQuery(t *testing.T) {
	for _, tc := range []struct {
		name  string
		unset func(s *Session)
	}{
		{"self unset", func(s *Session) { s.self = nil }},
		{"self still the configured nick", func(s *Session) { s.self.Nick = "configured" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, sink := newEnvelopeSession(t)
			_ = s.store.SetDeviceNetwork(context.Background(), "phone", s.Network.ID, true)
			s.mu.Lock()
			tc.unset(s)
			s.mu.Unlock()

			feed(s, ":Melksedek!m@h PRIVMSG MrIron :hello there")
			got := storedTargets(t, s)
			if got["mriron"] {
				t.Fatalf("stored under our own nick: targets %v", got)
			}
			if !got["melksedek"] {
				t.Fatalf("not stored under the sender: targets %v", got)
			}
			rows := openRows(t, s, "phone")
			if r := rows["melksedek"]; !r.Open || r.Display != "Melksedek" {
				t.Fatalf("melksedek row = %+v, want open as Melksedek (rows %v)", r, rows)
			}
			if _, ok := rows["mriron"]; ok {
				t.Fatalf("a row opened under our own nick: %v", rows)
			}
			line := lastLine(t, sink)
			if len(line.Buffers) != 1 || line.Buffers[0].Key != "melksedek" || line.Buffers[0].ID == 0 {
				t.Fatalf("line buffers = %+v, want melksedek with its stored id", line.Buffers)
			}
			assertOpensFold(t, s, sink)
		})
	}
}

// Once our nick is known, a DM to us and our own PRIVMSG to a nick (an echo-message line) are
// both the peer's query, as before.
func TestDMAndOwnLineWithSelfKnown(t *testing.T) {
	s, sink := newEnvelopeSession(t) // self "me"
	_ = s.store.SetDeviceNetwork(context.Background(), "phone", s.Network.ID, true)
	feed(s, ":Bob!b@h PRIVMSG me :to us", ":me!u@h PRIVMSG Carol :from us", ":me!u@h PRIVMSG #a :a channel line")
	got := storedTargets(t, s)
	for _, want := range []string{"bob", "carol", "#a"} {
		if !got[want] {
			t.Fatalf("missing target %q in %v", want, got)
		}
	}
	if got["me"] {
		t.Fatalf("stored under our own nick: %v", got)
	}
	rows := openRows(t, s, "phone")
	for key, display := range map[string]string{"bob": "Bob", "carol": "Carol"} {
		if r := rows[key]; !r.Open || r.Display != display {
			t.Fatalf("row %q = %+v, want open as %q", key, r, display)
		}
	}
	if _, ok := rows["#a"]; ok {
		t.Fatalf("a channel line opened a query row: %v", rows)
	}
	assertOpensFold(t, s, sink)
}

// A user's line to a channel of a network-specific CHANTYPES prefix stays the channel's.
func TestUserLineToCustomChantypeChannelStaysTheChannels(t *testing.T) {
	s, _ := newEnvelopeSession(t)
	s.mu.Lock()
	s.self = nil
	s.mu.Unlock()
	feed(s, ":srv.example 005 me CHANTYPES=#. :are supported", ":Bob!b@h PRIVMSG .chan :hi all")
	if got := storedTargets(t, s); !got[".chan"] || got["bob"] {
		t.Fatalf("targets = %v, want .chan and not bob", got)
	}
}

// reopenQueryLocked never writes a row whose display does not fold to its key, whatever target
// a caller hands it.
func TestReopenQueryRefusesMismatchedKey(t *testing.T) {
	s, sink := newEnvelopeSession(t)
	_ = s.store.SetDeviceNetwork(context.Background(), "phone", s.Network.ID, true)
	id := insertLine(t, s, "mriron", "Melksedek!m@h", "under the wrong key")
	msg, err := irc.Parse(":Melksedek!m@h PRIVMSG MrIron :under the wrong key")
	if err != nil {
		t.Fatal(err)
	}
	s.busMu.Lock()
	s.reopenQueryLocked(msg, "mriron", id)
	s.busMu.Unlock()
	if rows := openRows(t, s, "phone"); len(rows) != 0 {
		t.Fatalf("rows = %v, want none", rows)
	}
	assertOpensFold(t, s, sink)
}
