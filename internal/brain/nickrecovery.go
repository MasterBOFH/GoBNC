package brain

import (
	"strings"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/keeper"
	"github.com/MasterBOFH/GoBNC/internal/registration"
)

// DefaultNickRecoveryInterval matches internal/uplink/nick.go's original
// value — no reason for the new path to behave differently here.
const DefaultNickRecoveryInterval = 30 * time.Second

// WithNickRecoveryInterval overrides DefaultNickRecoveryInterval — mainly
// for tests, which need this well under a second rather than waiting out
// 30 real seconds per ISON tick.
func WithNickRecoveryInterval(d time.Duration) DriverOption {
	return func(drv *Driver) { drv.nickRecoveryInterval = d }
}

// ISON-based nick recovery, ported from internal/uplink/nick.go's
// nickRecoveryLoop/handleRecoveryISON/onSelfNickChange — same shape, same
// primary-then-alt preference logic, adapted to write via
// AttachClient.SendWrite instead of a raw socket and to react to
// registration.Step-parsed traffic Driver already has (via handleLine),
// rather than the old Uplink.handle's own dedicated hook. Deliberately
// not ported: internal/uplink.handleRecoveryNickError, which only ever
// decided whether a 432/433 caused by our own reclaim attempt should be
// hidden from downlink fan-out — Driver has no fan-out concept yet
// (that's stage 2b), so there is nothing here for it to suppress. It has
// no effect on recovery's own correctness; it's a UI-hygiene concern to
// pick back up when a fan-out consumer exists.
//
// State lives on Driver itself (nickRecMu/nickRecStops/isonPending/
// currentNick) rather than a separate type: this is Driver's own policy
// in the same sense registration.Step-driving and auto-join are, not an
// independent component with its own lifecycle.

// handleNickRecoveryTraffic dispatches the subset of traffic nick recovery
// cares about — called from handleLine for every parsed line on a tracked
// network, regardless of registration phase (recovery only ever acts once
// a loop is actually running, which only happens post-registration).
// Returns true when the line was a recovery ISON reply that must not be
// republished to Session (mirrors internal/uplink.handleRecoveryNumeric).
func (d *Driver) handleNickRecoveryTraffic(id keeper.NetworkID, msg irc.Message) bool {
	switch msg.Command {
	case "303":
		return d.handleRecoveryISON(id, msg)
	case "NICK":
		d.handleSelfNickChange(id, msg)
	}
	return false
}

// startNickRecoveryIfNeeded is called once a network reaches
// PhaseComplete (see handleLine) and again on every self-NICK-change
// (see handleSelfNickChange) — mirrors internal/uplink's
// maybeStartNickRecovery exactly: a no-op if recovery isn't configured,
// already on the primary nick, or already running.
func (d *Driver) startNickRecoveryIfNeeded(id keeper.NetworkID) {
	d.mu.Lock()
	cfg, ok := d.configs[id]
	state := d.states[id]
	d.mu.Unlock()
	if !ok || !cfg.NickRecovery {
		return
	}
	cm := caseMappingOf(state)
	if cm.Equal(d.getCurrentNick(id), cfg.PrimaryNick) {
		return
	}

	d.nickRecMu.Lock()
	if d.nickRecStops[id] != nil {
		d.nickRecMu.Unlock()
		return
	}
	stop := make(chan struct{})
	d.nickRecStops[id] = stop
	d.nickRecMu.Unlock()

	go d.nickRecoveryLoop(id, stop)
}

// StopNickRecovery cancels id's recovery loop on demand — the exported
// entry point for a client-driven NICK command (see
// internal/session.Session's downstream dispatch): a client changing nick
// manually should suppress automatic reclaim the same way
// internal/uplink.Uplink.StopNickRecovery did, so the two don't fight over
// what nick to hold.
func (d *Driver) StopNickRecovery(id keeper.NetworkID) { d.stopNickRecovery(id) }

// stopNickRecovery cancels id's recovery loop, if one is running —
// synchronous (the stop channel close happens before returning, same
// contract internal/uplink.StopNickRecovery had), called on a fresh
// registration attempt starting (resetStateLocked), a genuine disconnect
// (handleNetworkEvent), and reaching the primary nick again
// (handleSelfNickChange).
func (d *Driver) stopNickRecovery(id keeper.NetworkID) {
	d.nickRecMu.Lock()
	defer d.nickRecMu.Unlock()
	if stop, ok := d.nickRecStops[id]; ok {
		close(stop)
		delete(d.nickRecStops, id)
	}
	delete(d.isonPending, id)
}

func (d *Driver) nickRecoveryLoop(id keeper.NetworkID, stop <-chan struct{}) {
	t := time.NewTicker(d.nickRecoveryInterval)
	defer t.Stop()
	d.nickRecoveryTick(id)
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			d.nickRecoveryTick(id)
		}
	}
}

func (d *Driver) nickRecoveryTick(id keeper.NetworkID) {
	d.mu.Lock()
	cfg, ok := d.configs[id]
	state, tracked := d.states[id]
	d.mu.Unlock()
	if !ok || !tracked || !cfg.NickRecovery || state.Phase != registration.PhaseComplete {
		d.stopNickRecovery(id)
		return
	}
	cm := caseMappingOf(state)
	cur := d.getCurrentNick(id)
	if cm.Equal(cur, cfg.PrimaryNick) {
		d.stopNickRecovery(id)
		return
	}
	targets := isonTargets(cur, cfg.PrimaryNick, cfg.AltNick, cm)
	if len(targets) == 0 {
		d.stopNickRecovery(id)
		return
	}

	d.nickRecMu.Lock()
	if d.isonPending[id] || d.nickRecStops[id] == nil {
		d.nickRecMu.Unlock()
		return
	}
	d.isonPending[id] = true
	d.nickRecMu.Unlock()

	_ = d.sendLine(id, "ISON "+strings.Join(targets, " "))
}

// isonTargets is isonTargets from internal/uplink/nick.go, ported
// verbatim (parameters split out of store.Network since this package
// doesn't depend on internal/store).
func isonTargets(cur, primary, alt string, cm irc.CaseMapping) []string {
	if cm.Equal(cur, primary) {
		return nil
	}
	out := []string{primary}
	if alt != "" && !cm.Equal(alt, primary) && !cm.Equal(cur, alt) {
		out = append(out, alt)
	}
	return out
}

// handleRecoveryISON reacts to 303 (RPL_ISON) — only meaningful while
// this network has a pending ISON from nickRecoveryTick; otherwise it's
// ordinary traffic this package doesn't interpret and leaves alone.
func (d *Driver) handleRecoveryISON(id keeper.NetworkID, msg irc.Message) bool {
	d.nickRecMu.Lock()
	pending := d.isonPending[id]
	if pending {
		d.isonPending[id] = false
	}
	d.nickRecMu.Unlock()
	if !pending {
		return false
	}

	d.mu.Lock()
	cfg, ok := d.configs[id]
	state := d.states[id]
	d.mu.Unlock()
	if !ok {
		return true
	}
	cm := caseMappingOf(state)
	online := map[string]bool{}
	for _, nick := range strings.Fields(msg.Trailing()) {
		if nick != "" {
			online[cm.Canonical(nick)] = true
		}
	}

	cur := d.getCurrentNick(id)
	var want string
	if !cm.Equal(cur, cfg.PrimaryNick) && !online[cm.Canonical(cfg.PrimaryNick)] {
		want = cfg.PrimaryNick
	} else if cfg.AltNick != "" && !cm.Equal(cur, cfg.PrimaryNick) && !cm.Equal(cur, cfg.AltNick) &&
		!cm.Equal(cfg.AltNick, cfg.PrimaryNick) && !online[cm.Canonical(cfg.AltNick)] {
		want = cfg.AltNick
	}
	if want != "" {
		_ = d.sendLine(id, "NICK "+want)
	}
	return true
}

// handleSelfNickChange reacts to a NICK line whose source is our own
// current nick — mirrors internal/uplink.onSelfNickChange: stop recovery
// on reaching the primary nick, otherwise (re)start it (a no-op if
// recovery isn't configured or is already running).
func (d *Driver) handleSelfNickChange(id keeper.NetworkID, msg irc.Message) {
	if !irc.CaseRFC1459.Equal(msg.Nick(), d.getCurrentNick(id)) {
		return
	}
	newNick := msg.Trailing()
	if newNick == "" {
		newNick = msg.Param(0)
	}
	d.setCurrentNick(id, newNick)

	d.mu.Lock()
	cfg, ok := d.configs[id]
	state := d.states[id]
	d.mu.Unlock()
	if !ok {
		return
	}
	cm := caseMappingOf(state)
	if cm.Equal(newNick, cfg.PrimaryNick) {
		d.stopNickRecovery(id)
		return
	}
	d.startNickRecoveryIfNeeded(id)
}

func (d *Driver) getCurrentNick(id keeper.NetworkID) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if nick, ok := d.currentNick[id]; ok {
		return nick
	}
	return d.states[id].Nick
}

func (d *Driver) setCurrentNick(id keeper.NetworkID, nick string) {
	d.mu.Lock()
	d.currentNick[id] = nick
	d.mu.Unlock()
}

func caseMappingOf(state registration.State) irc.CaseMapping {
	if state.ISUPPORT != nil {
		return state.ISUPPORT.CaseMapping
	}
	return irc.CaseRFC1459
}
