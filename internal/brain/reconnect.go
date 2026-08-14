package brain

import (
	"time"

	"github.com/MasterBOFH/GoBNC/internal/keeper"
)

// DefaultMinBackoff / DefaultMaxBackoff mirror internal/uplink.Config's old
// backoff defaults (1s / 60s) — automatic reconnect-with-backoff is new
// functionality at this layer (it did not exist anywhere in the keeper/
// brain split before this cutover: the keeper only ever reports that a
// socket died, it never redials on its own — see internal/keeper/keeper.go's
// package doc), but there's no reason a network should behave differently
// just because the process that owns the retry loop moved from
// internal/uplink to here.
const (
	DefaultMinBackoff = time.Second
	DefaultMaxBackoff = 60 * time.Second
)

// WithBackoff overrides DefaultMinBackoff/DefaultMaxBackoff — mainly for
// tests, which need sub-second bounds rather than waiting out real backoff
// delays.
func WithBackoff(min, max time.Duration) DriverOption {
	return func(d *Driver) { d.minBackoff = min; d.maxBackoff = max }
}

// StopNetwork closes id's connection (if any) and, unlike Reconnect, does
// not redial: it cancels any pending auto-reconnect for id and marks it
// stopped, so a disconnect event racing with this call cannot schedule one
// either (belt-and-suspenders — a deliberate Close, which this triggers,
// never itself publishes a NetworkEvent for armReconnect to react to; see
// Keeper.Close's doc comment). The network stays tracked — RegisterNetwork's
// config, SetChannels, and SetFloodParams all remain in effect — a later
// Dial or Reconnect through Driver resumes it, matching
// internal/server.ReconnectNetwork's existing "start if not running"
// behavior for a network the caller previously stopped.
func (d *Driver) StopNetwork(id keeper.NetworkID) error {
	d.stopNickRecovery(id)
	d.reconnMu.Lock()
	d.stopped[id] = true
	if t, ok := d.reconnectTimers[id]; ok {
		t.Stop()
		delete(d.reconnectTimers, id)
	}
	d.reconnMu.Unlock()
	return d.client.SendClose(id)
}

// armReconnect schedules a redial of id after its current backoff, using
// whatever DialConfig was last passed to Dial for it — the automatic
// counterpart to a caller-driven Reconnect, fired from Run on a failed
// DialResult and on a genuine (non-stale) EventDisconnected. Doubles id's
// backoff (capped at maxBackoff) for next time; a successful DialResult
// resets it back to minBackoff (see resetBackoff). A no-op if id was never
// Dial'd through Driver (nothing to redial with) or StopNetwork was called
// for it more recently than its last successful Dial/Reconnect.
func (d *Driver) armReconnect(id keeper.NetworkID) {
	d.mu.Lock()
	cfg, ok := d.dialConfigs[id]
	netCfg, tracked := d.configs[id]
	d.mu.Unlock()
	if !ok {
		return
	}
	d.reconnMu.Lock()
	if d.stopped[id] {
		d.reconnMu.Unlock()
		return
	}
	wait := d.backoff[id]
	if wait < d.minBackoff {
		wait = d.minBackoff
	}
	next := wait * 2
	if next > d.maxBackoff {
		next = d.maxBackoff
	}
	d.backoff[id] = next
	if t, ok := d.reconnectTimers[id]; ok {
		t.Stop()
	}
	d.reconnectTimers[id] = time.AfterFunc(wait, func() {
		d.reconnMu.Lock()
		delete(d.reconnectTimers, id)
		stopped := d.stopped[id]
		d.reconnMu.Unlock()
		if stopped {
			return
		}
		// A fresh dial needs a fresh registration.State — without this, a
		// network that already reached PhaseComplete once stays stuck
		// there forever: Step is a deliberate no-op past a terminal phase
		// (see registration.Step's own doc comment), so the new
		// connection's CAP/welcome traffic would be silently ignored and
		// no ActionRegistered would ever fire again. Same reset
		// Reconnect's own resetStateLocked call performs, just reached
		// from the auto-redial path instead of a caller-driven one.
		if tracked {
			d.mu.Lock()
			d.resetStateLocked(id, netCfg)
			d.mu.Unlock()
		}
		_ = d.client.SendDial(id, cfg, 0) // best-effort; a failure surfaces as another DialResult, re-arming
	})
	d.reconnMu.Unlock()
}

// resetBackoff returns id's backoff to minBackoff and clears any stopped/
// pending-timer state — called on every successful DialResult (see Run),
// whichever of Dial, Reconnect, or armReconnect's own redial produced it, so
// a network that's been stable doesn't inherit an elevated backoff from
// whatever attempt preceded it.
func (d *Driver) resetBackoff(id keeper.NetworkID) {
	d.reconnMu.Lock()
	d.backoff[id] = d.minBackoff
	delete(d.stopped, id)
	if t, ok := d.reconnectTimers[id]; ok {
		t.Stop()
		delete(d.reconnectTimers, id)
	}
	d.reconnMu.Unlock()
}

// clearStopped un-stops id — called at the start of Dial and Reconnect,
// both explicit "I want this connected" signals, so a network StopNetwork
// previously parked stays resumable through either entry point, and so a
// pending auto-reconnect timer from before this explicit call can't also
// fire and race it.
func (d *Driver) clearStopped(id keeper.NetworkID) {
	d.reconnMu.Lock()
	delete(d.stopped, id)
	if t, ok := d.reconnectTimers[id]; ok {
		t.Stop()
		delete(d.reconnectTimers, id)
	}
	d.reconnMu.Unlock()
}
