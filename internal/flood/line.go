package flood

import (
	"context"
	"sync"
	"time"
)

// LinePacer paces writes the way ircds meter a client, per line rather
// than per byte. ircu (ircd/parse.c) charges every message
// 2s + 1s per 120 bytes against the client's clock and stops reading once
// that clock runs 10s ahead of real time; what arrives meanwhile queues in
// the client's recvq, and past 1024 bytes (CLIENT_FLOOD) the server closes
// the link with Excess Flood. So a stock ircu sustains about one short
// line every two seconds after a few lines of burst, however few bytes
// they are — a rate no byte bucket can express without also throttling
// long lines far below what the server allows. Hybrid-family ircds and
// UnrealIRCd meter per line in the same spirit.
//
// LinePacer keeps its own copy of that clock and lets a line through only
// if charging it keeps the clock within Allowance of now. Allowance is
// short of ircu's 10s to absorb clock granularity, latency, and lines the
// driver sends unpaced (registration, PONG) that the server charges too.
type LinePacer struct {
	// Base is the fixed cost of one line, PerBytes the number of bytes
	// that cost one more second, Allowance how far ahead of real time the
	// charged clock may run.
	Base      time.Duration
	PerBytes  int
	Allowance time.Duration

	mu    sync.Mutex
	on    bool
	clock time.Time // the charged clock; never behind now once consulted

	now   func() time.Time
	sleep func(context.Context, time.Duration) error
}

// NewLinePacer returns a disabled pacer using ircu's cost model.
func NewLinePacer() *LinePacer {
	return &LinePacer{
		Base:      2 * time.Second,
		PerBytes:  120,
		Allowance: 8 * time.Second,
		now:       time.Now,
		sleep:     sleepCtx,
	}
}

// Configure turns pacing on or off. Turning it off forgets the charged clock.
func (p *LinePacer) Configure(on bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.on = on
	if !on {
		p.clock = time.Time{}
	}
}

// Enabled reports whether pacing is active.
func (p *LinePacer) Enabled() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.on
}

// Cost is what a line of n wire bytes is charged.
func (p *LinePacer) Cost(n int) time.Duration {
	if p.PerBytes <= 0 {
		return p.Base
	}
	return p.Base + time.Duration(n/p.PerBytes)*time.Second
}

// Take blocks until a line of n wire bytes may be sent, then charges it.
// A line whose own cost exceeds Allowance goes out once the charged clock
// has caught up with real time.
func (p *LinePacer) Take(ctx context.Context, n int) error {
	for {
		p.mu.Lock()
		if !p.on {
			p.mu.Unlock()
			return nil
		}
		now := p.now()
		if p.clock.Before(now) {
			p.clock = now
		}
		cost := p.Cost(n)
		ahead := p.clock.Sub(now)
		if ahead+cost <= p.Allowance || ahead == 0 {
			p.clock = p.clock.Add(cost)
			p.mu.Unlock()
			return nil
		}
		wait := ahead + cost - p.Allowance
		if wait > ahead {
			wait = ahead // oversized line: send once caught up
		}
		p.mu.Unlock()
		if err := p.sleep(ctx, wait); err != nil {
			return err
		}
	}
}
