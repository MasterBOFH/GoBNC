package flood

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeClock drives a LinePacer without real sleeping: sleep advances now.
type fakeClock struct {
	t      time.Time
	slept  []time.Duration
	cancel error // returned by sleep when set
}

func newFakePacer() (*LinePacer, *fakeClock) {
	c := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	p := NewLinePacer()
	p.now = func() time.Time { return c.t }
	p.sleep = func(_ context.Context, d time.Duration) error {
		if c.cancel != nil {
			return c.cancel
		}
		c.slept = append(c.slept, d)
		c.t = c.t.Add(d)
		return nil
	}
	p.Configure(true)
	return p, c
}

func TestLinePacerDisabledNeverWaits(t *testing.T) {
	p, c := newFakePacer()
	p.Configure(false)
	for i := 0; i < 50; i++ {
		if err := p.Take(context.Background(), 400); err != nil {
			t.Fatal(err)
		}
	}
	if len(c.slept) != 0 {
		t.Fatalf("disabled pacer slept %v", c.slept)
	}
}

func TestLinePacerBurstThenOneLinePerBase(t *testing.T) {
	p, c := newFakePacer()
	// 64-byte lines cost 2s each; an 8s allowance lets 4 through at once.
	for i := 0; i < 4; i++ {
		if err := p.Take(context.Background(), 64); err != nil {
			t.Fatal(err)
		}
	}
	if len(c.slept) != 0 {
		t.Fatalf("burst of 4 slept %v", c.slept)
	}
	for i := 0; i < 3; i++ {
		if err := p.Take(context.Background(), 64); err != nil {
			t.Fatal(err)
		}
	}
	want := []time.Duration{2 * time.Second, 2 * time.Second, 2 * time.Second}
	if len(c.slept) != len(want) {
		t.Fatalf("slept %v, want %v", c.slept, want)
	}
	for i := range want {
		if c.slept[i] != want[i] {
			t.Fatalf("slept %v, want %v", c.slept, want)
		}
	}
}

func TestLinePacerChargesLongLinesMore(t *testing.T) {
	p, _ := newFakePacer()
	if got := p.Cost(64); got != 2*time.Second {
		t.Fatalf("Cost(64)=%v", got)
	}
	if got := p.Cost(400); got != 5*time.Second { // 2 + 400/120
		t.Fatalf("Cost(400)=%v", got)
	}
}

func TestLinePacerIdleTimeRestoresBurst(t *testing.T) {
	p, c := newFakePacer()
	for i := 0; i < 4; i++ {
		_ = p.Take(context.Background(), 64)
	}
	c.t = c.t.Add(time.Minute)
	for i := 0; i < 4; i++ {
		_ = p.Take(context.Background(), 64)
	}
	if len(c.slept) != 0 {
		t.Fatalf("after idling, burst slept %v", c.slept)
	}
}

func TestLinePacerOversizedLineWaitsForCatchUp(t *testing.T) {
	p, c := newFakePacer()
	p.Allowance = 8 * time.Second
	// 1200 bytes cost 12s, more than the whole allowance: the first goes
	// out at once, the second only once the clock has caught up again.
	_ = p.Take(context.Background(), 1200)
	_ = p.Take(context.Background(), 1200)
	if len(c.slept) != 1 || c.slept[0] != 12*time.Second {
		t.Fatalf("slept %v, want [12s]", c.slept)
	}
}

func TestLinePacerCancel(t *testing.T) {
	p, c := newFakePacer()
	for i := 0; i < 4; i++ {
		_ = p.Take(context.Background(), 64)
	}
	c.cancel = context.Canceled
	if err := p.Take(context.Background(), 64); !errors.Is(err, context.Canceled) {
		t.Fatalf("Take after cancel = %v", err)
	}
}

// TestLinePacerKeepsIrcuReading replays the pacer's send times through
// ircu's own rule (ircd/s_bsd.c read_packet + ircd/parse.c): a message is
// parsed only while the client's clock is less than 10s ahead, and each
// parsed message advances it by 2 + len/120 seconds. Every line the pacer
// releases must be parsed on arrival, so nothing ever waits in the recvq —
// that is what keeps the link clear of Excess Flood at any CLIENT_FLOOD.
func TestLinePacerKeepsIrcuReading(t *testing.T) {
	p, c := newFakePacer()
	sizes := []int{64, 64, 400, 64, 510, 64, 64, 64, 200, 64, 64, 64, 64, 510, 510, 64}
	var since time.Time // ircu's cli_since
	for i, n := range sizes {
		if err := p.Take(context.Background(), n); err != nil {
			t.Fatal(err)
		}
		now := c.t
		if since.Before(now) {
			since = now
		}
		if since.Sub(now) >= 10*time.Second {
			t.Fatalf("line %d (%d bytes) would sit in ircu's recvq: client clock %v ahead", i, n, since.Sub(now))
		}
		since = since.Add(time.Duration(2+n/120) * time.Second)
	}
}
