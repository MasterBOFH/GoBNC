package session

import (
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// newEnvelopeSession is a registered "me" with history, in #a and #b with "peer", STATUSMSG @+.
func newEnvelopeSession(t *testing.T) (*Session, *recSink) {
	t.Helper()
	db, hist, id := openLegacyFixture(t)
	s := New(store.Network{ID: id, Name: "n", Nick: "me"}, db, hist, nil, nil)
	for i, l := range []string{
		":srv.example 001 me :Welcome",
		":srv.example 005 me CHANTYPES=# STATUSMSG=@+ PREFIX=(ov)@+ :are supported",
		":srv.example 376 me :End of MOTD",
	} {
		s.HandleLine([]byte(l), uint64(i+1))
	}
	feed(s, ":me!u@h JOIN #a", ":me!u@h JOIN #b", ":peer!p@h JOIN #a", ":peer!p@h JOIN #b",
		":peer!p@h PRIVMSG me :earlier query line") // gives peer a query buffer in history
	sink := &recSink{}
	s.Subscribe(1, sink, nil, nil)
	return s, sink
}

func lastLine(t *testing.T, sink *recSink) wire.LineEvent {
	t.Helper()
	ls := sink.lines()
	if len(ls) == 0 {
		t.Fatal("no line event")
	}
	return ls[len(ls)-1]
}

func bufferNames(ev wire.LineEvent) []string {
	out := []string{}
	for _, b := range ev.Buffers {
		out = append(out, b.Name)
	}
	return out
}

func TestEnvelopeKindsAndBuffers(t *testing.T) {
	for _, tc := range []struct {
		name, line string
		kind       string
		buffers    []string
		self       bool
	}{
		{"channel privmsg", ":peer!p@h PRIVMSG #a :hi", wire.LineMessage, []string{"#a"}, false},
		{"query to us", ":peer!p@h PRIVMSG me :hi", wire.LineMessage, []string{"peer"}, false},
		{"statusmsg", ":peer!p@h PRIVMSG @#a :ops", wire.LineMessage, []string{"#a"}, false},
		{"ctcp version", ":peer!p@h PRIVMSG #a :\x01VERSION\x01", wire.LineCTCP, []string{"#a"}, false},
		{"ctcp action", ":peer!p@h PRIVMSG #a :\x01ACTION waves\x01", wire.LineMessage, []string{"#a"}, false},
		{"ctcp reply", ":peer!p@h NOTICE me :\x01VERSION x\x01", wire.LineCTCPReply, []string{"peer"}, false},
		{"server notice", ":srv.example NOTICE me :maintenance", wire.LineNotice, []string{wire.ServerBuffer}, false},
		{"mask notice", ":peer!p@h NOTICE $*.example :global", wire.LineNotice, []string{wire.ServerBuffer}, false},
		{"tagmsg", "@+typing=active :peer!p@h TAGMSG #a", wire.LineTagmsg, []string{"#a"}, false},
		{"join", ":new!n@h JOIN #a", wire.LineJoin, []string{"#a"}, false},
		{"part", ":peer!p@h PART #b :bye", wire.LinePart, []string{"#b"}, false},
		{"kick", ":me!u@h KICK #a peer :out", wire.LineKick, []string{"#a"}, true},
		{"channel mode", ":me!u@h MODE #a +o peer", wire.LineMode, []string{"#a"}, true},
		{"user mode", ":me MODE me +i", wire.LineMode, []string{}, true},
		{"topic", ":peer!p@h TOPIC #a :new", wire.LineTopic, []string{"#a"}, false},
		{"invite", ":peer!p@h INVITE me #c", wire.LineInvite, []string{}, false},
		{"away", ":peer!p@h AWAY :gone", wire.LineAway, []string{"#a", "#b", "peer"}, false},
		{"account", ":peer!p@h ACCOUNT acct", wire.LineAccount, []string{"#a", "#b", "peer"}, false},
		{"chghost", ":peer!p@h CHGHOST p new.host", wire.LineChghost, []string{"#a", "#b", "peer"}, false},
		{"setname", ":peer!p@h SETNAME :Real Name", wire.LineSetname, []string{"#a", "#b", "peer"}, false},
		{"channel numeric", ":srv.example 332 me #a :topic", wire.LineNumeric, []string{"#a"}, false},
		{"plain numeric", ":srv.example 251 me :users", wire.LineNumeric, []string{wire.ServerBuffer}, false},
		{"error", "ERROR :Closing link", wire.LineError, []string{wire.ServerBuffer}, false},
		{"other", ":peer!p@h WALLOPS :x", wire.LineOther, []string{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, sink := newEnvelopeSession(t)
			feed(s, tc.line)
			ev := lastLine(t, sink)
			if ev.Kind != tc.kind {
				t.Fatalf("kind = %q, want %q (%+v)", ev.Kind, tc.kind, ev)
			}
			got := bufferNames(ev)
			if !sameSet(got, tc.buffers) {
				t.Fatalf("buffers = %v, want %v", got, tc.buffers)
			}
			if ev.Self != tc.self {
				t.Fatalf("self = %v, want %v", ev.Self, tc.self)
			}
		})
	}
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := map[string]int{}
	for _, x := range a {
		m[x]++
	}
	for _, x := range b {
		m[x]--
	}
	for _, v := range m {
		if v != 0 {
			return false
		}
	}
	return true
}

func TestEnvelopeQuitListsSharedChannelsAndQuery(t *testing.T) {
	s, sink := newEnvelopeSession(t)
	feed(s, ":peer!p@h QUIT :gone")
	ev := lastLine(t, sink)
	if ev.Kind != wire.LineQuit || !sameSet(bufferNames(ev), []string{"#a", "#b", "peer"}) {
		t.Fatalf("quit envelope = %+v", ev)
	}
	for _, b := range ev.Buffers {
		if b.Name == "peer" && b.ID != 0 {
			t.Fatalf("query buffer has an id although QUIT is not stored there: %+v", b)
		}
		if b.Name != "peer" && b.ID == 0 {
			t.Fatalf("channel %s stored the QUIT but has no id: %+v", b.Name, b)
		}
	}
}

func TestEnvelopeIDsOnlyForStoredBuffers(t *testing.T) {
	s, sink := newEnvelopeSession(t)
	feed(s, ":peer!p@h PRIVMSG #a :stored")
	ev := lastLine(t, sink)
	if len(ev.Buffers) != 1 || ev.Buffers[0].ID == 0 || ev.Buffers[0].Key != "#a" {
		t.Fatalf("privmsg envelope = %+v", ev)
	}
	if ev.MsgID == "" || ev.Time == "" || ev.Source != "peer!p@h" || ev.Nick != "peer" {
		t.Fatalf("msgid/time/source/nick = %q/%q/%q/%q", ev.MsgID, ev.Time, ev.Source, ev.Nick)
	}
	feed(s, ":srv.example 251 me :not stored")
	if ev := lastLine(t, sink); len(ev.Buffers) != 1 || ev.Buffers[0].ID != 0 {
		t.Fatalf("numeric envelope = %+v", ev)
	}
}

func TestEnvelopeOwnNickChangeIsSelf(t *testing.T) {
	s, sink := newEnvelopeSession(t)
	feed(s, ":me!u@h NICK :me2")
	ev := lastLine(t, sink)
	if ev.Kind != wire.LineNick || !ev.Self || !sameSet(bufferNames(ev), []string{"#a", "#b"}) {
		t.Fatalf("own NICK envelope = %+v", ev)
	}
}
