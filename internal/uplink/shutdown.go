package uplink

import (
	"context"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/version"
)

// GracefulQuit waits for the flood send queue to drain (bounded by ctx), then
// sends QUIT and returns. Does not close the connection; the caller should
// cancel the uplink Run context afterward.
func (u *Uplink) GracefulQuit(ctx context.Context, reason string) {
	if reason == "" {
		reason = version.QuitMessage()
	}
	u.waitFloodDrained(ctx)
	u.writeQuit(ctx, reason)
}

func (u *Uplink) waitFloodDrained(ctx context.Context) {
	if !u.floodEnabled() {
		return
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		u.floodMu.Lock()
		n := len(u.floodQ)
		u.floodMu.Unlock()
		if n == 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (u *Uplink) writeQuit(ctx context.Context, reason string) {
	u.writeMu.Lock()
	defer u.writeMu.Unlock()
	u.mu.RLock()
	c := u.conn
	u.mu.RUnlock()
	if c == nil {
		return
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.Underlying().SetWriteDeadline(deadline)
	} else {
		_ = c.Underlying().SetWriteDeadline(time.Now().Add(5 * time.Second))
	}
	_ = c.WriteLine("QUIT :" + reason)
	_ = c.Underlying().SetWriteDeadline(time.Time{})
}
