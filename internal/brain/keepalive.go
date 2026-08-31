package brain

import (
	"time"

	"github.com/MasterBOFH/GoBNC/internal/keeper"
)

// KeepaliveIdle / KeepaliveGrace match internal/uplink/keepalive.go's
// original values. The keeper answers server-originated PING autonomously,
// but that is not a substitute for this loop: some ircds never send PING
// while the client is still writing (ISON reclaim is the case that showed
// this), and some don't treat those writes as resetting their own ping
// timer either. Without a client-originated PING the uplink then sits
// silent from the server's point of view until it is dropped. 120s idle /
// 60s grace is the old budget; downlink's wider 300s/120s budget is a
// different path (a client pinging us, not us pinging the ircd).
var (
	KeepaliveIdle  = 120 * time.Second
	KeepaliveGrace = 60 * time.Second
)

// WithKeepalive overrides KeepaliveIdle/KeepaliveGrace — mainly for tests,
// which need this well under a second. idle<=0 disables the loop entirely.
func WithKeepalive(idle, grace time.Duration) DriverOption {
	return func(drv *Driver) {
		drv.keepaliveIdle = idle
		drv.keepaliveGrace = grace
	}
}

// noteRX records that id's uplink delivered a line that counts as
// liveness. RPL_ISON (303) is deliberately excluded at the call site:
// nick-recovery's own 30s ISON poll produces a 303 even when the ircd
// has otherwise gone silent, and treating that reply as RX is what
// suppressed the keepalive PING (and then the connection) in the first
// place — see TestDriverKeepalivePINGsDuringISONReclaim.
func (d *Driver) noteRX(id keeper.NetworkID) {
	d.keepMu.Lock()
	d.lastRX[id] = time.Now().UnixNano()
	d.keepMu.Unlock()
}

func (d *Driver) lastRXTime(id keeper.NetworkID) time.Time {
	d.keepMu.Lock()
	ns := d.lastRX[id]
	d.keepMu.Unlock()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// startKeepaliveIfNeeded launches id's idle-PING loop if one isn't already
// running and keepalive isn't disabled. Called on EventConnected (a fresh
// dial) and from RegisterResumedNetwork (the keeper already held the
// socket; there will be no EventConnected for this brain to see).
func (d *Driver) startKeepaliveIfNeeded(id keeper.NetworkID) {
	if d.keepaliveIdle <= 0 {
		return
	}
	d.keepMu.Lock()
	if d.keepStops[id] != nil {
		d.keepMu.Unlock()
		return
	}
	stop := make(chan struct{})
	d.keepStops[id] = stop
	if d.lastRX[id] == 0 {
		d.lastRX[id] = time.Now().UnixNano()
	}
	d.keepMu.Unlock()
	go d.keepaliveLoop(id, stop)
}

func (d *Driver) stopKeepalive(id keeper.NetworkID) {
	d.keepMu.Lock()
	defer d.keepMu.Unlock()
	if stop, ok := d.keepStops[id]; ok {
		close(stop)
		delete(d.keepStops, id)
	}
	delete(d.lastRX, id)
}

func (d *Driver) keepaliveLoop(id keeper.NetworkID, stop <-chan struct{}) {
	idle := d.keepaliveIdle
	grace := d.keepaliveGrace
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
		case <-stop:
			return
		case <-t.C:
		}
		last := d.lastRXTime(id)
		if last.IsZero() {
			continue
		}
		silent := time.Since(last)
		if silent < idle {
			pingAt = time.Time{}
			continue
		}
		if pingAt.IsZero() {
			_ = d.sendLine(id, "PING :gobnc")
			pingAt = time.Now()
			continue
		}
		if grace > 0 && time.Since(pingAt) >= grace && time.Since(last) >= idle {
			if d.log != nil {
				d.log.Info("uplink keepalive timeout; closing", "network", d.peerLabel(id))
			}
			_ = d.client.SendClose(id)
			// A deliberate CloseRequest never produces a keeper
			// EventDisconnected (the keeper publishes one only when the
			// socket dies on its own — see Keeper.readLoop's cleanup), so
			// without help nothing downstream ever learns this uplink is
			// gone: internal/server's demux calls Session.HandleDisconnect
			// only on a NetworkEvent{Disconnected} or a PhaseFailed Result,
			// and this path used to produce neither — downlinks stayed
			// attached to a dead, silently re-registering network.
			// failRegistration solves the same problem for its own
			// deliberate Close by emitting the PhaseFailed Result; here the
			// network is registered, so synthesize the event the keeper
			// won't send and run it through the same handleNetworkEvent +
			// publish pair Run uses for a genuine disconnect — same teardown
			// (nick recovery, this keepalive loop via its stopKeepalive,
			// armReconnect), same demux routing to HandleDisconnect. If the
			// socket happens to die on its own in this same instant, the
			// keeper's genuine event doubles this one — HandleDisconnect is
			// documented benign when called twice for one disconnect.
			d.mu.Lock()
			epoch := d.epochs[id]
			d.mu.Unlock()
			ev := keeper.NetworkEventMsg{Network: id, Kind: keeper.EventDisconnected, Epoch: epoch, Error: "keepalive timeout"}
			d.handleNetworkEvent(ev)
			d.publishNetworkEvent(ev)
			return
		}
	}
}
