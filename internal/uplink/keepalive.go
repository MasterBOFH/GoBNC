package uplink

import (
	"context"
	"sync/atomic"
	"time"
)

// Keepalive timing (overridable in tests).
var (
	KeepaliveIdle  = 120 * time.Second // send PING after this much RX silence
	KeepaliveGrace = 60 * time.Second  // close if still silent after PING
)

func (u *Uplink) noteRX() {
	atomic.StoreInt64(&u.lastRXUnix, time.Now().UnixNano())
}

func (u *Uplink) lastRX() time.Time {
	ns := atomic.LoadInt64(&u.lastRXUnix)
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

func (u *Uplink) startKeepalive(ctx context.Context) {
	u.noteRX()
	go u.keepaliveLoop(ctx)
}

func (u *Uplink) keepaliveLoop(ctx context.Context) {
	idle := KeepaliveIdle
	grace := KeepaliveGrace
	if idle <= 0 {
		return
	}
	tick := idle / 4
	if tick < 50*time.Millisecond {
		tick = 50 * time.Millisecond
	}
	if tick > 10*time.Second {
		tick = 10 * time.Second
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	var pingAt time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		last := u.lastRX()
		if last.IsZero() {
			continue
		}
		silent := time.Since(last)
		if silent < idle {
			pingAt = time.Time{}
			continue
		}
		if pingAt.IsZero() {
			_ = u.writeImmediate("PING :gobnc")
			pingAt = time.Now()
			continue
		}
		if grace > 0 && time.Since(pingAt) >= grace && time.Since(last) >= idle {
			u.log.Info("uplink keepalive timeout; closing")
			u.mu.RLock()
			c := u.conn
			u.mu.RUnlock()
			if c != nil {
				_ = c.Close()
			}
			return
		}
	}
}
