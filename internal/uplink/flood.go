package uplink

import (
	"context"
	"fmt"
	"strings"

	"github.com/MasterBOFH/GoBNC/internal/flood"
)

// Flood pacing: client and bouncer application traffic share one byte bucket and
// FIFO queue. PONG and registration handshake bypass via writeImmediate.

func (u *Uplink) initFlood() {
	u.flood = flood.NewByteBucket(0, 0)
	u.floodWake = make(chan struct{}, 1)
	n := u.cfg.Network
	u.flood.Configure(n.FloodBurst, n.FloodRate)
}

func (u *Uplink) setFloodParams(burst int, rate float64) {
	if u.flood == nil {
		u.initFlood()
	}
	u.flood.Configure(burst, rate)
	u.kickFloodDrain()
}

func (u *Uplink) floodEnabled() bool {
	return u.flood != nil && u.flood.Enabled()
}

func (u *Uplink) startFloodDrain(ctx context.Context) {
	u.floodMu.Lock()
	if u.floodStop != nil {
		u.floodMu.Unlock()
		return
	}
	stop := make(chan struct{})
	u.floodStop = stop
	u.floodQ = nil
	u.floodMu.Unlock()
	go u.floodDrainLoop(ctx, stop)
}

func (u *Uplink) stopFloodDrain() {
	u.floodMu.Lock()
	stop := u.floodStop
	u.floodStop = nil
	u.floodQ = nil
	u.floodMu.Unlock()
	if stop != nil {
		close(stop)
	}
}

func (u *Uplink) kickFloodDrain() {
	if u.floodWake == nil {
		return
	}
	select {
	case u.floodWake <- struct{}{}:
	default:
	}
}

func (u *Uplink) enqueueFlood(line string) error {
	u.mu.RLock()
	c := u.conn
	u.mu.RUnlock()
	if c == nil {
		return errNotConnected
	}
	u.floodMu.Lock()
	max := u.cfg.MaxFloodQueue
	if max > 0 && len(u.floodQ) >= max {
		u.floodMu.Unlock()
		return fmt.Errorf("flood queue full (%d)", max)
	}
	u.floodQ = append(u.floodQ, line)
	u.floodMu.Unlock()
	u.kickFloodDrain()
	return nil
}

func (u *Uplink) floodDrainLoop(ctx context.Context, stop <-chan struct{}) {
	drainCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-stop:
			cancel()
		case <-ctx.Done():
			cancel()
		}
	}()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-u.floodWake:
		}
		for {
			u.floodMu.Lock()
			if len(u.floodQ) == 0 {
				u.floodMu.Unlock()
				break
			}
			line := u.floodQ[0]
			u.floodQ = u.floodQ[1:]
			u.floodMu.Unlock()

			n := wireBytes(line)
			if err := u.flood.Take(drainCtx, n); err != nil {
				return
			}
			if err := u.writeImmediate(line); err != nil {
				// Drop remaining queue on write failure; session will reconnect.
				u.floodMu.Lock()
				u.floodQ = nil
				u.floodMu.Unlock()
				return
			}
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			default:
			}
		}
	}
}

func wireBytes(line string) int {
	n := len(line)
	if !strings.HasSuffix(line, "\r\n") {
		n += 2
	}
	return n
}
