package uplink

import (
	"strconv"
	"strings"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/connio"
	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

const (
	nickRecoveryInterval = 30 * time.Second
	defaultNickLen       = 30
	maxUnderscoreTries   = 20
)

// StopNickRecovery cancels ISON-based nick reclaim (e.g. intentional client NICK).
func (u *Uplink) StopNickRecovery() {
	u.mu.Lock()
	u.nickRecoveryUserStopped = true
	u.mu.Unlock()
	u.stopNickRecovery()
}

func (u *Uplink) stopNickRecovery() {
	u.nickRecMu.Lock()
	defer u.nickRecMu.Unlock()
	if u.nickRecStop != nil {
		close(u.nickRecStop)
		u.nickRecStop = nil
	}
	u.isonPending = false
}

func (u *Uplink) resetNickRecoveryState() {
	u.stopNickRecovery()
	u.mu.Lock()
	u.nickRecoveryUserStopped = false
	u.mu.Unlock()
}

func (u *Uplink) maybeStartNickRecovery() {
	u.mu.RLock()
	n := u.cfg.Network
	cur := u.nick
	stopped := u.nickRecoveryUserStopped
	cm := irc.CaseRFC1459
	if u.isupport != nil {
		cm = u.isupport.CaseMapping
	}
	u.mu.RUnlock()
	if !n.NickRecovery || stopped {
		return
	}
	if cm.Equal(cur, n.Nick) {
		return
	}
	u.nickRecMu.Lock()
	defer u.nickRecMu.Unlock()
	if u.nickRecStop != nil {
		return
	}
	stop := make(chan struct{})
	u.nickRecStop = stop
	go u.nickRecoveryLoop(stop)
}

func (u *Uplink) nickRecoveryLoop(stop <-chan struct{}) {
	t := time.NewTicker(nickRecoveryInterval)
	defer t.Stop()
	u.nickRecoveryTick()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			u.nickRecoveryTick()
		}
	}
}

func (u *Uplink) nickRecoveryTick() {
	u.mu.RLock()
	n := u.cfg.Network
	cur := u.nick
	stopped := u.nickRecoveryUserStopped
	reg := u.reg
	cm := irc.CaseRFC1459
	if u.isupport != nil {
		cm = u.isupport.CaseMapping
	}
	u.mu.RUnlock()
	if !reg || !n.NickRecovery || stopped {
		u.stopNickRecovery()
		return
	}
	if cm.Equal(cur, n.Nick) {
		u.stopNickRecovery()
		return
	}
	targets := isonTargets(cur, n, cm)
	if len(targets) == 0 {
		u.stopNickRecovery()
		return
	}
	u.nickRecMu.Lock()
	if u.isonPending || u.nickRecStop == nil {
		u.nickRecMu.Unlock()
		return
	}
	u.isonPending = true
	u.nickRecMu.Unlock()
	_ = u.WriteRaw("ISON " + strings.Join(targets, " "))
}

func isonTargets(cur string, n store.Network, cm irc.CaseMapping) []string {
	if cm.Equal(cur, n.Nick) {
		return nil
	}
	out := []string{n.Nick}
	// While on alt: primary only. While on nick_/__: primary and alt.
	if n.AltNick != "" && !cm.Equal(n.AltNick, n.Nick) && !cm.Equal(cur, n.AltNick) {
		out = append(out, n.AltNick)
	}
	return out
}

// handleRecoveryNumeric processes reclaim ISON/433 traffic.
// Returns true if the message must not be fanned out to downlinks.
func (u *Uplink) handleRecoveryNumeric(msg irc.Message) bool {
	switch msg.Command {
	case "303":
		return u.handleRecoveryISON(msg)
	case "432", "433":
		return u.handleRecoveryNickError(msg)
	default:
		return false
	}
}

func (u *Uplink) handleRecoveryISON(msg irc.Message) bool {
	u.nickRecMu.Lock()
	pending := u.isonPending
	if pending {
		u.isonPending = false
	}
	u.nickRecMu.Unlock()
	if !pending {
		return false
	}

	cm := u.caseMapping()
	online := map[string]bool{}
	for _, nick := range strings.Fields(msg.Trailing()) {
		if nick != "" {
			online[cm.Canonical(nick)] = true
		}
	}

	u.mu.RLock()
	n := u.cfg.Network
	cur := u.nick
	u.mu.RUnlock()

	// Prefer primary when free, then alt (only if not already on alt / primary).
	var want string
	if !cm.Equal(cur, n.Nick) && !online[cm.Canonical(n.Nick)] {
		want = n.Nick
	} else if n.AltNick != "" && !cm.Equal(cur, n.Nick) && !cm.Equal(cur, n.AltNick) &&
		!cm.Equal(n.AltNick, n.Nick) && !online[cm.Canonical(n.AltNick)] {
		want = n.AltNick
	}
	if want != "" {
		_ = u.WriteRaw("NICK " + want)
	}
	return true
}

func (u *Uplink) handleRecoveryNickError(msg irc.Message) bool {
	u.nickRecMu.Lock()
	active := u.nickRecStop != nil
	u.nickRecMu.Unlock()
	if !active {
		return false
	}
	bad := msg.Param(1)
	u.mu.RLock()
	n := u.cfg.Network
	u.mu.RUnlock()
	cm := u.caseMapping()
	if cm.Equal(bad, n.Nick) || (n.AltNick != "" && cm.Equal(bad, n.AltNick)) {
		return true
	}
	return false
}

func (u *Uplink) onSelfNickChange(newNick string) {
	cm := u.caseMapping()
	u.mu.RLock()
	primary := u.cfg.Network.Nick
	u.mu.RUnlock()
	if cm.Equal(newNick, primary) {
		u.stopNickRecovery()
		return
	}
	u.maybeStartNickRecovery()
}

func (u *Uplink) caseMapping() irc.CaseMapping {
	u.mu.RLock()
	defer u.mu.RUnlock()
	if u.isupport != nil {
		return u.isupport.CaseMapping
	}
	return irc.CaseRFC1459
}

// tryNextRegisterNick sends the next ladder nick after a collision.
// ok is false if recovery is off or the ladder is exhausted.
func (u *Uplink) tryNextRegisterNick(c *connio.Conn, badNick string) (ok bool, err error) {
	u.mu.RLock()
	n := u.cfg.Network
	last := u.nick
	maxLen := defaultNickLen
	cm := irc.CaseRFC1459
	if u.isupport != nil {
		cm = u.isupport.CaseMapping
		if v := u.isupport.Raw["NICKLEN"]; v != "" {
			if nl, err := strconv.Atoi(v); err == nil && nl > 0 {
				maxLen = nl
			}
		}
	}
	u.mu.RUnlock()

	if !n.NickRecovery {
		return false, nil
	}
	ladder := buildNickLadder(n.Nick, n.AltNick, maxLen)
	next := nextNickInLadder(ladder, last, badNick, cm)
	if next == "" {
		return false, nil
	}
	u.mu.Lock()
	u.nick = next
	u.mu.Unlock()
	u.log.Info("nick in use; trying next", "bad", badNick, "next", next)
	if err := c.WriteLine("NICK " + next); err != nil {
		return false, err
	}
	return true, nil
}

// buildNickLadder returns primary, optional alt, then nick_, nick__, … truncated to maxLen.
func buildNickLadder(primary, alt string, maxLen int) []string {
	if maxLen <= 0 {
		maxLen = defaultNickLen
	}
	seen := make(map[string]bool)
	var out []string
	add := func(nick string) {
		if nick == "" {
			return
		}
		if len(nick) > maxLen {
			nick = nick[:maxLen]
		}
		key := strings.ToLower(nick)
		if seen[key] {
			return
		}
		seen[key] = true
		out = append(out, nick)
	}
	add(primary)
	add(alt)
	for i := 1; i <= maxUnderscoreTries; i++ {
		if i >= maxLen {
			break
		}
		base := primary
		room := maxLen - i
		if room < 1 {
			break
		}
		if len(base) > room {
			base = base[:room]
		}
		add(base + strings.Repeat("_", i))
	}
	return out
}

func nextNickInLadder(ladder []string, last, bad string, cm irc.CaseMapping) string {
	idx := -1
	for i, n := range ladder {
		if cm.Equal(n, last) || cm.Equal(n, bad) {
			idx = i
		}
	}
	if idx < 0 {
		if len(ladder) > 1 {
			return ladder[1]
		}
		return ""
	}
	if idx+1 < len(ladder) {
		return ladder[idx+1]
	}
	return ""
}
