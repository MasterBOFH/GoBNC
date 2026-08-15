package session

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/irc"
)

func TestRequestTrackerStressConcurrentWHOX(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	const n = 50
	type job struct {
		client ClientID
		tok    string
		mask   string
	}
	jobs := make([]job, n)
	for i := 0; i < n; i++ {
		mask := cm.Canonical(fmt.Sprintf("#c%d", i))
		id := ClientID(fmt.Sprintf("c%d", i))
		_, tok, wait := rt.Begin(BeginOpts{
			Client: id, Cmd: "WHO", PreferWHOX: true, WHOMask: mask, ClientLabel: fmt.Sprintf("L%d", i),
		})
		if !wait || tok == "" {
			t.Fatalf("begin %d: tok=%q writeNow=%v", i, tok, wait)
		}
		rt.SetWHOXClientFix(tok, true, "")
		jobs[i] = job{client: id, tok: tok, mask: mask}
	}

	var wg sync.WaitGroup
	errCh := make(chan string, n*2)
	for _, j := range jobs {
		j := j
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, only, _, strip, _ := rt.RouteMessage(irc.Message{
				Command: "354", Params: []string{"me", j.tok, j.mask, "nick"},
			}, cm)
			if !only || c != j.client || !strip {
				errCh <- fmt.Sprintf("354 %s: client=%s only=%v strip=%v", j.tok, c, only, strip)
			}
		}()
	}
	wg.Wait()

	for _, j := range jobs {
		j := j
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, only, _, _, _ := rt.RouteMessage(irc.Message{
				Command: "315", Params: []string{"me", j.mask, "End"},
			}, cm)
			if !only || c != j.client {
				errCh <- fmt.Sprintf("315 %s: client=%s only=%v", j.mask, c, only)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		t.Error(e)
	}
}

func TestRequestTrackerStressSameCommandHold(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	const n = 20
	for i := 0; i < n; i++ {
		_, _, writeNow := rt.Begin(BeginOpts{
			Client:   ClientID(fmt.Sprintf("c%d", i)),
			Cmd:      "LIST",
			Outbound: irc.Message{Command: "LIST", Params: []string{fmt.Sprintf("%d", i)}},
		})
		if i == 0 && !writeNow {
			t.Fatal("first LIST must send")
		}
		if i > 0 && writeNow {
			t.Fatalf("LIST %d must queue", i)
		}
	}

	for i := 0; i < n; i++ {
		if i > 0 {
			got := rt.TakeReady()
			if len(got) != 1 {
				t.Fatalf("LIST %d not released, got %d", i, len(got))
			}
		}
		c, only, _, _, _ := rt.RouteMessage(irc.Message{Command: "323", Params: []string{"me", "End"}}, cm)
		want := ClientID(fmt.Sprintf("c%d", i))
		if !only || c != want {
			t.Fatalf("end %d: got %s only=%v want %s", i, c, only, want)
		}
	}
	if _, ok := rt.ActiveClient(); ok {
		t.Fatal("queue should be empty")
	}
}

func TestRequestTrackerLagBeforeEndNumeric(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	_, _, w1 := rt.Begin(BeginOpts{Client: "c1", Cmd: "LIST", Outbound: irc.Message{Command: "LIST"}})
	_, _, w2 := rt.Begin(BeginOpts{Client: "c2", Cmd: "LIST", Outbound: irc.Message{Command: "LIST"}})
	if !w1 || w2 {
		t.Fatal(w1, w2)
	}
	if got := rt.TakeReady(); len(got) != 0 {
		t.Fatal("second must stay queued during lag")
	}
	rt.RouteMessage(irc.Message{Command: "323", Params: []string{"me", "End"}}, cm)
	if got := rt.TakeReady(); len(got) != 1 {
		t.Fatal("second LIST not released after end numeric")
	}
}

func TestRequestTrackerDropClientReleasesHold(t *testing.T) {
	rt := NewRequestTracker()
	_, _, w1 := rt.Begin(BeginOpts{Client: "c1", Cmd: "LIST", Outbound: irc.Message{Command: "LIST"}})
	_, _, w2 := rt.Begin(BeginOpts{Client: "c2", Cmd: "LIST", Outbound: irc.Message{Command: "LIST"}})
	if !w1 || w2 {
		t.Fatal(w1, w2)
	}
	rt.DropClient("c1")
	if got := rt.TakeReady(); len(got) != 1 {
		t.Fatal("dropping active client should release next LIST write")
	}
	active, ok := rt.ActiveClient()
	if !ok || active != "c2" {
		t.Fatalf("active=%s ok=%v", active, ok)
	}
}

func TestRequestTrackerDropClientReleasesSTATSHold(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	_, _, w1 := rt.Begin(BeginOpts{Client: "c1", Cmd: "STATS", StatsLetter: "y"})
	_, _, w2 := rt.Begin(BeginOpts{Client: "c2", Cmd: "STATS", StatsLetter: "y"})
	if !w1 || w2 {
		t.Fatal("duplicate STATS y coalesces; only first writes", w1, w2)
	}
	rt.DropClient("c1")
	c, only, _, _, _ := rt.RouteMessage(irc.Message{Command: "219", Params: []string{"me", "y", "End"}}, cm)
	if !only || c != "c2" {
		t.Fatal(c, only)
	}
}

func TestRequestTrackerSameWHOXMaskTwoClients(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	mask := cm.Canonical("#same")
	_, t1, w1 := rt.Begin(BeginOpts{Client: "c1", Cmd: "WHO", PreferWHOX: true, WHOMask: mask, WHOXFlags: "o", WHOXFields: "nuhs"})
	_, t2, w2 := rt.Begin(BeginOpts{Client: "c2", Cmd: "WHO", PreferWHOX: true, WHOMask: mask, WHOXFlags: "x", WHOXFields: "nuhs"})
	if !w1 || !w2 {
		t.Fatal("different WHOX flags must both write", w1, w2)
	}
	rt.SetWHOXClientFix(t1, true, "")
	rt.SetWHOXClientFix(t2, true, "")
	if t1 == t2 {
		t.Fatal("tokens must differ")
	}
	c, only, _, _, _ := rt.RouteMessage(irc.Message{Command: "354", Params: []string{"me", t2, "x"}}, cm)
	if !only || c != "c2" {
		t.Fatal(c, only)
	}
	c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "354", Params: []string{"me", t1, "x"}}, cm)
	if !only || c != "c1" {
		t.Fatal(c, only)
	}
	// Same mask on 315: oldest pending first.
	c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "315", Params: []string{"me", mask, "End"}}, cm)
	if !only || c != "c1" {
		t.Fatalf("first 315 -> %s", c)
	}
	c, only, _, _, _ = rt.RouteMessage(irc.Message{Command: "315", Params: []string{"me", mask, "End"}}, cm)
	if !only || c != "c2" {
		t.Fatalf("second 315 -> %s", c)
	}
}

func TestRequestTrackerTokenWraparound(t *testing.T) {
	rt := NewRequestTracker()
	rt.nextTok = 998
	_, t1, _ := rt.Begin(BeginOpts{Client: "c1", Cmd: "WHO", PreferWHOX: true, WHOMask: "#a"})
	_, t2, _ := rt.Begin(BeginOpts{Client: "c2", Cmd: "WHO", PreferWHOX: true, WHOMask: "#b"})
	_, t3, _ := rt.Begin(BeginOpts{Client: "c3", Cmd: "WHO", PreferWHOX: true, WHOMask: "#c"})
	if t1 != "999" || t2 != "1" || t3 != "2" {
		t.Fatalf("wrap: %s %s %s", t1, t2, t3)
	}
}

func TestRequestTrackerRaceHammer(t *testing.T) {
	rt := NewRequestTracker()
	cm := irc.CaseRFC1459
	var wg sync.WaitGroup
	var ops atomic.Int64
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				id := ClientID(fmt.Sprintf("g%d-%d", g, i%5))
				letter := string(rune('a' + (i % 4)))
				_, _, _ = rt.Begin(BeginOpts{Client: id, Cmd: "STATS", StatsLetter: letter})
				rt.RouteMessage(irc.Message{Command: "219", Params: []string{"me", letter, "End"}}, cm)
				ops.Add(1)
				if i%17 == 0 {
					rt.DropClient(id)
				}
			}
		}(g)
	}
	wg.Wait()
	if ops.Load() < 100 {
		t.Fatalf("too few ops: %d", ops.Load())
	}
}
