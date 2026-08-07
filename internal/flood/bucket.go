// Package flood provides a byte token bucket for pacing IRC uplink writes.
package flood

import (
	"context"
	"sync"
	"time"
)

// ByteBucket is a token bucket metered in bytes.
// Rate <= 0 disables limiting (Take returns immediately).
type ByteBucket struct {
	mu     sync.Mutex
	burst  float64
	rate   float64 // bytes per second
	tokens float64
	last   time.Time
}

// NewByteBucket creates a bucket with the given burst (bytes) and refill rate (bytes/sec).
func NewByteBucket(burst int, rate float64) *ByteBucket {
	if burst < 0 {
		burst = 0
	}
	b := &ByteBucket{
		burst:  float64(burst),
		rate:   rate,
		tokens: float64(burst),
		last:   time.Now(),
	}
	if b.burst < 1 && rate > 0 {
		// Allow at least one byte of burst when rate is set so Take can progress.
		b.burst = 1
		b.tokens = 1
	}
	return b
}

// Configure updates burst/rate. Existing tokens are capped to the new burst.
func (b *ByteBucket) Configure(burst int, rate float64) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked(time.Now())
	if burst < 0 {
		burst = 0
	}
	b.burst = float64(burst)
	b.rate = rate
	if b.burst < 1 && rate > 0 {
		b.burst = 1
	}
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
}

// Enabled reports whether pacing is active.
func (b *ByteBucket) Enabled() bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.enabledLocked()
}

func (b *ByteBucket) enabledLocked() bool {
	return b.rate > 0 && b.burst > 0
}

// Take blocks until n bytes of tokens are available, then consumes them.
// Lines larger than burst wait until the bucket is full, then send (tokens → 0).
func (b *ByteBucket) Take(ctx context.Context, n int) error {
	if n <= 0 || b == nil {
		return nil
	}
	need := float64(n)
	for {
		b.mu.Lock()
		if !b.enabledLocked() {
			b.mu.Unlock()
			return nil
		}
		now := time.Now()
		b.refillLocked(now)
		if need <= b.burst {
			if b.tokens >= need {
				b.tokens -= need
				b.mu.Unlock()
				return nil
			}
			wait := time.Duration((need - b.tokens) / b.rate * float64(time.Second))
			b.mu.Unlock()
			if wait < time.Millisecond {
				wait = time.Millisecond
			}
			if err := sleepCtx(ctx, wait); err != nil {
				return err
			}
			continue
		}
		// Oversized line: wait for a full bucket, then drain it.
		if b.tokens >= b.burst {
			b.tokens = 0
			b.mu.Unlock()
			return nil
		}
		wait := time.Duration((b.burst - b.tokens) / b.rate * float64(time.Second))
		b.mu.Unlock()
		if wait < time.Millisecond {
			wait = time.Millisecond
		}
		if err := sleepCtx(ctx, wait); err != nil {
			return err
		}
	}
}

func (b *ByteBucket) refillLocked(now time.Time) {
	if b.rate <= 0 || b.burst <= 0 {
		b.last = now
		return
	}
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed * b.rate
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
		b.last = now
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
