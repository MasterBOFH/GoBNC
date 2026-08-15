package session

import (
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// TestFlushHeldAfterRegisterDoesNotBlockOnSerializedWHO: clients that attach
// before 376 pipeline several solicitous commands. Those are held, then
// flushed from completeRegistration on the demux goroutine. Begin must not
// block that goroutine on a later WHO's write — the second WHO is queued
// and sent when 315 arrives.
func TestFlushHeldAfterRegisterDoesNotBlockOnSerializedWHO(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	if err := s.HandleClientMessage(d, irc.Message{Command: "WHO", Params: []string{"#a"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.HandleClientMessage(d, irc.Message{Command: "WHO", Params: []string{"#b"}}); err != nil {
		t.Fatal(err)
	}
	s.mu.RLock()
	nHeld := len(s.heldUntilReg)
	s.mu.RUnlock()
	if nHeld != 2 {
		t.Fatalf("held=%d want 2", nHeld)
	}

	done := make(chan struct{})
	go func() {
		s.HandleLine([]byte(":irc.example 001 me :Welcome"), 1)
		s.HandleLine([]byte(":irc.example 376 me :End of MOTD"), 2)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("HandleLine(376) blocked flushing held WHO")
	}
	if !s.Registered() {
		t.Fatal("expected registration to complete")
	}
	s.Detach(d.ID())
}

func TestConcurrentSerializedWHODoesNotBlockCaller(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	s.mu.Lock()
	s.registered = true
	s.mu.Unlock()

	d1 := &fakeDL{id: "c1", caps: map[string]bool{}}
	d2 := &fakeDL{id: "c2", caps: map[string]bool{}}
	if err := s.Attach(d1); err != nil {
		t.Fatal(err)
	}
	if err := s.Attach(d2); err != nil {
		t.Fatal(err)
	}

	_ = s.HandleClientMessage(d1, irc.Message{Command: "WHO", Params: []string{"#a"}})
	if _, ok := s.tracker.ActiveClient(); !ok {
		t.Fatal("first WHO should be the serialized active request")
	}

	done := make(chan error, 1)
	go func() {
		done <- s.HandleClientMessage(d2, irc.Message{Command: "WHO", Params: []string{"#b"}})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second WHO blocked the caller")
	}
	s.Detach(d1.ID())
	s.Detach(d2.ID())
}

func TestRequestTrackerNAMESDemuxByChannel(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459

	_, _, w1 := rt.Begin(BeginOpts{Client: "c1", Cmd: "NAMES", EnquiryTarget: "#a"})
	_, _, w2 := rt.Begin(BeginOpts{Client: "c2", Cmd: "NAMES", EnquiryTarget: "#b"})
	if !w1 || !w2 {
		t.Fatal("different channels must both send", w1, w2)
	}
	c, only, _, _, _ := rt.RouteMessage(irc.Message{Command: "353", Params: []string{"me", "=", "#b", "n1"}}, cm)
	if !only || c != "c2" {
		t.Fatalf("#b 353 -> %s only=%v", c, only)
	}
	c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "353", Params: []string{"me", "=", "#a", "n2"}}, cm)
	if !only || c != "c1" {
		t.Fatalf("#a 353 -> %s only=%v", c, only)
	}
	c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "366", Params: []string{"me", "#a", "End"}}, cm)
	if !only || c != "c1" {
		t.Fatal(c, only)
	}
	c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "366", Params: []string{"me", "#b", "End"}}, cm)
	if !only || c != "c2" {
		t.Fatal(c, only)
	}
}

func TestRequestTrackerNAMESSameChannelCoalesce(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	_, _, w1 := rt.Begin(BeginOpts{Client: "c1", Cmd: "NAMES", EnquiryTarget: "#chan1"})
	_, _, w2 := rt.Begin(BeginOpts{Client: "c2", Cmd: "NAMES", EnquiryTarget: "#chan1"})
	if !w1 || w2 {
		t.Fatal("duplicate NAMES must skip the second uplink write", w1, w2)
	}
	got := rt.RouteAll(irc.Message{Command: "353", Params: []string{"me", "=", "#chan1", "a"}}, cm)
	requireDests(t, got, "c1", "c2")
	got = rt.RouteAll(irc.Message{Command: "366", Params: []string{"me", "#chan1", "End"}}, cm)
	requireDests(t, got, "c1", "c2")
	got = rt.RouteAll(irc.Message{Command: "353", Params: []string{"me", "=", "#chan1", "b"}}, cm)
	if len(got) != 0 {
		t.Fatalf("exchange finished, leftover dests %v", destIDs(got))
	}
}

func TestRequestTrackerLISTQueuesWrite(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	_, _, w1 := rt.Begin(BeginOpts{Client: "c1", Cmd: "LIST", Outbound: irc.Message{Command: "LIST", Params: []string{"1"}}})
	_, _, w2 := rt.Begin(BeginOpts{Client: "c2", Cmd: "LIST", Outbound: irc.Message{Command: "LIST", Params: []string{"2"}}})
	if !w1 || w2 {
		t.Fatal(w1, w2)
	}
	if got := rt.TakeReady(); len(got) != 0 {
		t.Fatal("second LIST must not write before 323")
	}
	rt.RouteMessage(irc.Message{Command: "323", Params: []string{"me", "End"}}, cm)
	got := rt.TakeReady()
	if len(got) != 1 || got[0].Param(0) != "2" {
		t.Fatalf("queued LIST: %+v", got)
	}
}

func TestRequestTrackerWHOAndLISTDoNotBlockEachOther(t *testing.T) {
	rt := NewRequestTracker()
	_, _, wWHO := rt.Begin(BeginOpts{Client: "c1", Cmd: "WHO", Outbound: irc.Message{Command: "WHO", Params: []string{"#a"}}})
	_, _, wLIST := rt.Begin(BeginOpts{Client: "c2", Cmd: "LIST", Outbound: irc.Message{Command: "LIST"}})
	if !wWHO || !wLIST {
		t.Fatal("WHO and LIST are different streams; both send", wWHO, wLIST)
	}
}
