package session

import (
	"strings"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"sync"
	"testing"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// vcRecorder collects one virtual client's client_line events.
type vcRecorder struct {
	mu    sync.Mutex
	lines []wire.ClientLineEvent
}

func (r *vcRecorder) deliver(ev any) bool {
	if cl, ok := ev.(wire.ClientLineEvent); ok {
		r.mu.Lock()
		r.lines = append(r.lines, cl)
		r.mu.Unlock()
	}
	return true
}

func (r *vcRecorder) raws() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []string{}
	for _, l := range r.lines {
		out = append(out, l.Raw)
	}
	return out
}

func TestVirtualClientGetsOnlyItsSolicitedReplies(t *testing.T) {
	s, sink := newEnvelopeSession(t)
	recA, recB := &vcRecorder{}, &vcRecorder{}
	a := NewVirtualClient("rc", "ws1", s.Network.ID, recA.deliver)
	b := NewVirtualClient("rc", "ws2", s.Network.ID, recB.deliver)
	ircDL := &fakeDL{id: "irc1", caps: map[string]bool{}}
	if err := s.Attach(ircDL); err != nil {
		t.Fatal(err)
	}
	s.AttachVirtual(a)
	s.AttachVirtual(b)
	ircDL.clearSent()
	recA.lines, recB.lines = nil, nil
	if a.ID() != "mod:rc:ws1" {
		t.Fatalf("ID = %q", a.ID())
	}

	// A asked WHOIS peer.
	s.tracker.Begin(BeginOpts{Client: a.ID(), Cmd: "WHOIS", WhoisTargets: []string{"peer"}, CaseMapping: s.CaseMapping()})
	before := len(sink.lines())
	feed(s, ":srv.example 311 me peer p h * :Peer Real", ":srv.example 318 me peer :End of /WHOIS list.")
	if got := recA.raws(); len(got) != 2 || !strings.Contains(got[0], " 311 ") || !strings.Contains(got[1], " 318 ") {
		t.Fatalf("A got %v, want its 311 and 318", got)
	}
	if got := recB.raws(); len(got) != 0 {
		t.Fatalf("B got %v", got)
	}
	if got := ircDL.snapshot(); len(got) != 0 {
		t.Fatalf("IRC client got %+v", got)
	}
	if n := len(sink.lines()) - before; n != 0 {
		t.Fatalf("%d line events for a solicited reply", n)
	}

	// A channel line is an envelope, not a client_line.
	feed(s, ":peer!p@h PRIVMSG #a :hello")
	if n := len(sink.lines()) - before; n != 1 {
		t.Fatalf("%d line events for one channel PRIVMSG", n)
	}
	if got := append(recA.raws(), recB.raws()...); len(got) != 2 {
		t.Fatalf("virtual clients got client_lines for a broadcast line: %v", got)
	}
	if got := ircDL.snapshot(); len(got) != 1 {
		t.Fatalf("IRC client got %+v, want the PRIVMSG", got)
	}
	if s.DownlinkCount() != 1 {
		t.Fatalf("DownlinkCount = %d, want 1 (virtual clients not counted)", s.DownlinkCount())
	}
}

func TestVirtualClientSurvivesDisconnect(t *testing.T) {
	s, _ := newEnvelopeSession(t)
	rec := &vcRecorder{}
	vc := NewVirtualClient("rc", "ws1", s.Network.ID, rec.deliver)
	s.AttachVirtual(vc)
	s.HandleDisconnect(nil)
	s.mu.RLock()
	_, still := s.downlinks[vc.ID()]
	s.mu.RUnlock()
	if !still {
		t.Fatal("virtual client dropped by an uplink disconnect")
	}
	for _, r := range rec.raws() {
		if strings.HasPrefix(r, "ERROR") {
			t.Fatalf("virtual client was sent %q", r)
		}
	}
}

func TestStaleDetachKeepsTheReplacementClient(t *testing.T) {
	s, _ := newEnvelopeSession(t)
	old := NewVirtualClient("rc", "ws1", s.Network.ID, (&vcRecorder{}).deliver)
	repl := NewVirtualClient("rc", "ws1", s.Network.ID, (&vcRecorder{}).deliver)
	s.AttachVirtual(old)
	s.AttachVirtual(repl) // a second connection of the module, same client name
	s.DetachVirtual(old)  // the first connection goes away
	s.mu.RLock()
	cur := s.downlinks[repl.ID()]
	s.mu.RUnlock()
	if cur != Downlink(repl) {
		t.Fatal("detaching the old client removed its replacement")
	}
}

func TestReattachDropsThePreviousClientsPendingReplies(t *testing.T) {
	s, _ := newEnvelopeSession(t)
	oldRec, newRec := &vcRecorder{}, &vcRecorder{}
	old := NewVirtualClient("rc", "ws1", s.Network.ID, oldRec.deliver)
	s.AttachVirtual(old)
	s.tracker.Begin(BeginOpts{Client: old.ID(), Cmd: "WHOIS", WhoisTargets: []string{"peer"}, CaseMapping: s.CaseMapping()})
	s.AttachVirtual(NewVirtualClient("rc", "ws1", s.Network.ID, newRec.deliver))
	feed(s, ":srv.example 311 me peer p h * :Peer", ":srv.example 318 me peer :End")
	for _, r := range newRec.raws() {
		if strings.Contains(r, " 311 ") {
			t.Fatalf("the re-attached client got its predecessor's WHOIS reply: %v", newRec.raws())
		}
	}
}

func TestVirtualClientsSkipRelayedCTCPAndLocalEcho(t *testing.T) {
	s, _ := newEnvelopeSession(t)
	rec := &vcRecorder{}
	vc := NewVirtualClient("rc", "ws1", s.Network.ID, rec.deliver)
	s.AttachVirtual(vc)
	feed(s, ":peer!p@h PRIVMSG #a :\x01VERSION\x01") // relayed CTCP request
	msg, _ := irc.Parse("PRIVMSG #a :typed")
	s.echoSelfLocally(vc, msg)
	if got := rec.raws(); len(got) != 0 {
		t.Fatalf("virtual client got broadcast lines as client_line: %v", got)
	}
}
