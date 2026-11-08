package session

import (
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
)

// chathistoryServerSession is a registered "me" that got a server notice and a line from a real
// nick Server, plus an IRC client with chathistory.
func chathistoryServerSession(t *testing.T) (*Session, *fakeDL) {
	t.Helper()
	s, _ := newEnvelopeSession(t)
	feed(s, ":srv.example NOTICE me :*** Notice -- snotice", ":Server!s@h PRIVMSG me :hi from a nick")
	return s, &fakeDL{id: "c1", caps: map[string]bool{"chathistory": true, "batch": true, "message-tags": true, "server-time": true}}
}

func sentWith(sent []irc.Message, text string) int {
	n := 0
	for _, m := range sent {
		if strings.Contains(m.Wire(), text) {
			n++
		}
	}
	return n
}

// A real nick Server is an ordinary query again for an IRC client: CHATHISTORY Server (any
// case) pages its lines and never the server notices.
func TestCHATHISTORYNickServerIsAQuery(t *testing.T) {
	for _, target := range []string{"Server", "server"} {
		s, d := chathistoryServerSession(t)
		if err := s.HandleClientMessage(d, irc.Message{Command: "CHATHISTORY", Params: []string{"LATEST", target, "*", "10"}}); err != nil {
			t.Fatal(err)
		}
		if sentWith(d.sent, "hi from a nick") != 1 || sentWith(d.sent, "snotice") != 0 || sentWith(d.sent, "FAIL") != 0 {
			t.Fatalf("LATEST %s: sent %+v, want the nick's line only", target, d.sent)
		}
	}
}

// A server's notices are an ordinary chathistory target named after the server, for every
// client: TARGETS lists it (and the nick Server's query beside it), and CHATHISTORY pages it
// with the batch, server-time and msgid tags.
func TestCHATHISTORYServerNameIsATarget(t *testing.T) {
	s, d := chathistoryServerSession(t)
	now := time.Now().UTC()
	if err := s.HandleClientMessage(d, irc.Message{Command: "CHATHISTORY", Params: []string{"TARGETS",
		"timestamp=" + now.Add(time.Minute).Format("2006-01-02T15:04:05.000Z"),
		"timestamp=" + now.Add(-time.Hour).Format("2006-01-02T15:04:05.000Z"), "10"}}); err != nil {
		t.Fatal(err)
	}
	listed := map[string]bool{}
	for _, m := range d.sent {
		if m.Command == "CHATHISTORY" && m.Param(0) == "TARGETS" {
			listed[m.Param(1)] = true
		}
	}
	if !listed["srv.example"] || !listed["Server"] && !listed["server"] {
		t.Fatalf("TARGETS = %v, want srv.example and the query Server", listed)
	}
	for _, sub := range [][]string{{"LATEST", "srv.example", "*", "10"}, {"LATEST", "SRV.Example", "*", "10"},
		{"BEFORE", "srv.example", "timestamp=" + now.Add(time.Minute).Format("2006-01-02T15:04:05.000Z"), "10"}} {
		d.clearSent()
		if err := s.HandleClientMessage(d, irc.Message{Command: "CHATHISTORY", Params: sub}); err != nil {
			t.Fatal(err)
		}
		var notice *irc.Message
		for i, m := range d.sent {
			if m.Command == "NOTICE" {
				notice = &d.sent[i]
			}
		}
		if notice == nil || sentWith(d.sent, "hi from a nick") != 0 || d.sent[0].Command != "BATCH" {
			t.Fatalf("%v: sent %+v, want a batch with the server notice", sub, d.sent)
		}
		if _, ok := notice.Tag("time"); !ok {
			t.Fatalf("%v: notice without server-time: %+v", sub, notice)
		}
		if _, ok := notice.Tag("msgid"); !ok {
			t.Fatalf("%v: notice without msgid: %+v", sub, notice)
		}
		if _, ok := notice.Tag("batch"); !ok {
			t.Fatalf("%v: notice outside the batch: %+v", sub, notice)
		}
	}
}
