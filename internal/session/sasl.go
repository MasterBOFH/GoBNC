package session

import (
	"strings"

	"github.com/MasterBOFH/GoBNC/internal/caps"
	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/keeper"
)

// CapEnabler is implemented by downlinks that can enable a negotiated capability.
type CapEnabler interface {
	EnableCap(name string)
}

// OffersPassthroughSASL reports whether sasl is advertised to clients.
func (s *Session) OffersPassthroughSASL() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.saslOffer != ""
}

// SetSASLOfferForTest sets the passthrough SASL CAP token (tests only).
func (s *Session) SetSASLOfferForTest(offer string) {
	s.mu.Lock()
	s.saslOffer = offer
	s.mu.Unlock()
}

// bouncerOwnsSASLLocked reports whether the uplink's sasl belongs to the
// bouncer's own configured SASL (Network.SASL) right now. Configured and
// not known to have failed means the bouncer either has authenticated,
// is mid-exchange, or is about to be — and clients are held off: no
// passthrough offer, client AUTHENTICATE dropped, a client CAP REQ sasl
// NAK'd. Once the bouncer's attempt has failed (904/905/906, or no
// mechanism it could use), sasl is handed to clients exactly as if the
// bouncer had no SASL configured, until a login succeeds again. The
// rules, in order:
//
//  1. Network.SASL off: relay sasl to clients immediately (CAP LS/NEW).
//  2. Network.SASL on: hold — never relay a CAP NEW sasl while the
//     bouncer's exchange is pending; NAK client REQs meanwhile.
//  3. Bouncer succeeds (900/903): don't relay; withdraw (CAP DEL) an
//     offer left over from an earlier failure.
//  4. Bouncer fails: relay CAP NEW sasl=… and let passthrough work.
//  5. Bouncer never starts an exchange while a client's is in flight
//     (handleCAPLine's NEW case checks saslClient).
//
// Two AUTHENTICATE exchanges can't interleave on one uplink, which is
// what 2 and 5 exist for. Caller holds s.mu.
func (s *Session) bouncerOwnsSASLLocked() bool {
	return s.Network.SASL && !s.bouncerSASLFailed
}

// noteBouncerSASLOutcomeLocked records the end of a bouncer-owned
// exchange from its outcome numeric. Caller holds s.mu.
func (s *Session) noteBouncerSASLOutcomeLocked(cmd string) {
	switch cmd {
	case "900", "903", "907":
		s.bouncerSASLPending = false
		s.bouncerSASLFailed = false
	case "904", "905", "906":
		s.bouncerSASLPending = false
		s.bouncerSASLFailed = true
	}
}

// bouncerSASLMechUsableLocked mirrors registration.pickSASLMech's
// mechanism-list half: with credentials the bouncer needs SCRAM-SHA-256
// or PLAIN on offer, without a password EXTERNAL; an empty list (sasl
// advertised with no value) is taken as "try it", as pickSASLMech does.
// Whether a client certificate is actually configured for EXTERNAL isn't
// known here (that resolution is I/O, done once in internal/server) — a
// misconfigured EXTERNAL still gets REQ'd, and the resulting silent
// non-attempt leaves the bouncer pending until the next CAP DEL/NEW.
// Caller holds s.mu.
func (s *Session) bouncerSASLMechUsableLocked() bool {
	if len(s.upSASLMechs) == 0 {
		return true
	}
	want := []string{"EXTERNAL"}
	if s.Network.SASLUser != "" && s.Network.SASLPass != "" {
		want = []string{"SCRAM-SHA-256", "PLAIN"}
	}
	for _, m := range s.upSASLMechs {
		for _, w := range want {
			if strings.EqualFold(m, w) {
				return true
			}
		}
	}
	return false
}

// refreshSASLOffer recomputes the passthrough SASL offer from the uplink's
// current sasl availability — Session's own upSASLAvailable/upSASLMechs
// (kept current by HandleRegistered and HandleLine's CAP interpretation),
// replacing the old uplink.Uplink.SASLAvailable() query — and from who
// owns sasl right now (bouncerOwnsSASLLocked; this used to be the static
// Network.SASL, which left clients with no way to authenticate the uplink
// after the bouncer's own attempt had failed).
func (s *Session) refreshSASLOffer() (prev, now string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev = s.saslOffer
	s.saslOffer = ""
	if !s.bouncerOwnsSASLLocked() && s.upSASLAvailable {
		s.saslOffer = caps.FormatSASL(s.upSASLMechs)
	}
	now = s.saslOffer
	return prev, now
}

// syncSASLOffer recomputes the offer and tells cap-notify clients about
// any change — the post-registration path for a sasl-only change (the
// CAP ACK/DEL cases diff the whole offered set themselves).
func (s *Session) syncSASLOffer() {
	prev, now := s.refreshSASLOffer()
	s.notifySASLOfferChange(prev, now)
}

func (s *Session) notifySASLOfferChange(prev, now string) {
	if prev == now {
		return
	}
	if now != "" && prev == "" {
		s.broadcastCapNotify("NEW", []string{now})
		return
	}
	if now == "" && prev != "" {
		s.broadcastCapNotify("DEL", []string{"sasl"})
		return
	}
	if now != "" && prev != "" {
		s.broadcastCapNotify("DEL", []string{"sasl"})
		s.broadcastCapNotify("NEW", []string{now})
	}
}

// OnCapNAK reacts to a post-registration CAP NAK for something Session
// itself requested (see RequestClientSASL) — called from HandleLine's CAP
// interpretation. Dropped the *uplink.Uplink parameter the old
// uplink.Handler interface required: nothing in the body ever used it.
func (s *Session) OnCapNAK(names []string) {
	for _, n := range names {
		if n == "sasl" {
			s.finishSASLWaiters(false)
			return
		}
	}
}

// RequestClientSASL enables uplink sasl for a downlink (passthrough).
func (s *Session) RequestClientSASL(d Downlink) error {
	if !s.OffersPassthroughSASL() {
		return s.sendClientCapNAK(d, "sasl")
	}
	if s.HasUpCap("sasl") {
		return s.sendClientCapACK(d, "sasl")
	}
	s.mu.Lock()
	s.saslWaiters = append(s.saslWaiters, d.ID())
	needReq := !s.saslReqPending
	s.saslReqPending = true
	s.mu.Unlock()
	if !needReq {
		return nil
	}
	if s.driver == nil {
		s.mu.Lock()
		s.saslWaiters = nil
		s.saslReqPending = false
		s.mu.Unlock()
		return s.sendClientCapNAK(d, "sasl")
	}
	return s.driver.WriteRaw(s.netID, "CAP REQ :sasl")
}

func (s *Session) sendClientCapACK(d Downlink, name string) error {
	if enabler, ok := d.(CapEnabler); ok {
		enabler.EnableCap(name)
	}
	return d.Send(irc.Message{
		Source:  ServerName,
		Command: "CAP",
		Params:  []string{"*", "ACK", name},
	})
}

func (s *Session) sendClientCapNAK(d Downlink, name string) error {
	return d.Send(irc.Message{
		Source:  ServerName,
		Command: "CAP",
		Params:  []string{"*", "NAK", name},
	})
}

func (s *Session) finishSASLWaiters(ok bool) {
	s.mu.Lock()
	ids := append([]ClientID(nil), s.saslWaiters...)
	s.saslWaiters = nil
	s.saslReqPending = false
	downlinks := make([]Downlink, 0, len(ids))
	for _, id := range ids {
		if d, found := s.downlinks[id]; found {
			downlinks = append(downlinks, d)
		}
	}
	s.mu.Unlock()
	for _, d := range downlinks {
		if ok {
			_ = s.sendClientCapACK(d, "sasl")
		} else {
			_ = s.sendClientCapNAK(d, "sasl")
		}
	}
}

// routeSASLTraffic handles AUTHENTICATE and SASL numerics from the uplink.
// Always consumes them (do not fan out via the normal broadcast path).
//
// Bouncer-owned SASL: never emit AUTHENTICATE or outcome numerics; only
// RPL_LOGGEDIN / RPL_LOGGEDOUT (900/901) go to attached clients.
// Client-initiated SASL: AUTHENTICATE and 903–908 go only to saslClient;
// RPL_LOGGEDIN / RPL_LOGGEDOUT are broadcast so every client learns login state.
func (s *Session) routeSASLTraffic(msg irc.Message) {
	s.applyAccountFromSASL(msg)

	switch msg.Command {
	case "900", "901":
		if msg.Command == "900" {
			// Whoever drove the exchange, the uplink is logged in: the
			// bouncer's earlier failure (if any) no longer hands sasl to
			// clients. The offer itself is resynced on the 903 that
			// follows (both branches below) — see the client branch.
			s.mu.Lock()
			s.bouncerSASLFailed = false
			s.mu.Unlock()
		}
		// Login state is session-wide regardless of who drove AUTHENTICATE.
		s.broadcastToDownlinks(msg)
		return
	}

	s.mu.Lock()
	bouncer := s.Network.SASL && s.bouncerSASLPending
	if bouncer {
		s.noteBouncerSASLOutcomeLocked(msg.Command)
	}
	s.mu.Unlock()
	if bouncer {
		// AUTHENTICATE stays uplink-only; the outcome numerics are
		// swallowed too, except that a failure is worth a NOTICE — success
		// is already covered by the 900 broadcast above. The outcome also
		// decides the passthrough offer (rules 3 and 4 in
		// bouncerOwnsSASLLocked): a failure hands sasl to clients, a
		// success takes back an offer left from an earlier failure.
		s.syncSASLOffer()
		s.broadcastBouncerSASLFailure(msg)
		return
	}

	s.mu.RLock()
	id := s.saslClient
	d, ok := s.downlinks[id]
	s.mu.RUnlock()
	if !ok {
		// No initiating client — swallow (do not leak to other downlinks).
		return
	}
	_ = d.Send(msg)
	switch msg.Command {
	case "903", "904", "905", "906", "907":
		s.mu.Lock()
		if s.saslClient == d.ID() {
			s.saslClient = ""
		}
		s.mu.Unlock()
		// A client login clears bouncerSASLFailed (see the 900 case
		// above); after the outcome is delivered, withdraw the offer that
		// login made moot — done here rather than on the 900 itself so
		// the CAP DEL never lands between a client's 900 and 903.
		s.syncSASLOffer()
	}
}

// broadcastBouncerSASLFailure sends a NOTICE for a bouncer-owned SASL
// failure numeric (904/905/906) — called from both routeSASLTraffic
// (post-registration re-auth) and HandleRegistrationLine (the common case:
// the bouncer's own SASL attempt failing during initial registration,
// which never reaches routeSASLTraffic since registration.Step owns that
// traffic until PhaseComplete). Success needs no equivalent call: 900
// (RPL_LOGGEDIN) already broadcasts unconditionally above.
func (s *Session) broadcastBouncerSASLFailure(msg irc.Message) {
	switch msg.Command {
	case "904", "905", "906":
	default:
		return
	}
	reason := msg.Trailing()
	if reason == "" {
		reason = "SASL authentication failed"
	}
	s.Broadcast(reason)
}

func (s *Session) applyAccountFromSASL(msg irc.Message) {
	switch msg.Command {
	case "900":
		if len(msg.Params) < 3 {
			return
		}
		acct := msg.Params[2]
		if acct == "*" {
			acct = ""
		}
		s.mu.Lock()
		s.loggedIn = acct != ""
		var blobCloak []byte
		if s.self != nil {
			s.self.Account = acct
			if len(msg.Params) > 1 {
				prevHost := s.self.Host
				s.self.UpdateFromPrefix(msg.Params[1])
				if s.self.Host != "" && s.self.Host != prevHost {
					blobCloak = []byte(s.self.Host)
				}
			}
		}
		s.mu.Unlock()
		s.pushBlob("account", keeper.BlobModeReplace, []byte(acct))
		if blobCloak != nil {
			s.pushBlob("cloak", keeper.BlobModeReplace, blobCloak)
		}
	case "901":
		s.mu.Lock()
		s.loggedIn = false
		if s.self != nil {
			s.self.Account = ""
		}
		s.mu.Unlock()
		s.pushBlob("account", keeper.BlobModeReplace, nil)
	}
}

// Broadcast sends a bouncer-sourced NOTICE to every client currently
// attached to this network — used for the bouncer-initiated-SASL
// initiated/failed notices (see handleCAPLine's ACK case and
// routeSASLTraffic) and by Server.BroadcastAll for a rehash notice.
func (s *Session) Broadcast(text string) {
	s.mu.RLock()
	nick := s.liveNickLocked()
	s.mu.RUnlock()
	if nick == "" {
		nick = "*"
	}
	s.broadcastToDownlinks(irc.Message{
		Source:  ServerName,
		Command: "NOTICE",
		Params:  []string{nick, text},
	})
}

func (s *Session) broadcastToDownlinks(msg irc.Message) {
	// Snapshot then release before rewriteFor — see HandleMessage's own
	// comment on this exact pattern for why holding s.mu across a call
	// that nested-RLocks it again is a real deadlock hazard.
	s.mu.RLock()
	downlinks := make([]Downlink, 0, len(s.downlinks))
	for _, d := range s.downlinks {
		downlinks = append(downlinks, d)
	}
	s.mu.RUnlock()
	for _, d := range downlinks {
		_ = d.Send(s.rewriteFor(d, msg))
	}
}

// rplLoggedIn builds RPL_LOGGEDIN for attach replay when the uplink nick is
// still logged in (900 seen, no subsequent 901).
func (s *Session) rplLoggedInLocked() (irc.Message, bool) {
	if !s.loggedIn || s.self == nil || s.self.Account == "" {
		return irc.Message{}, false
	}
	nick := s.self.Nick
	prefix := s.self.Prefix()
	if prefix == "" {
		prefix = nick
	}
	acct := s.self.Account
	return irc.Message{
		Source:  s.serverPrefixLocked(),
		Command: "900",
		Params:  []string{nick, prefix, acct, "You are now logged in as " + acct},
	}, true
}

func (s *Session) forwardClientAuthenticate(d Downlink, msg irc.Message) error {
	if !d.HasCap("sasl") || s.driver == nil {
		return nil
	}
	s.mu.Lock()
	if s.bouncerOwnsSASLLocked() {
		// Bouncer handles SASL; clients don't drive AUTHENTICATE unless
		// the bouncer's own attempt has failed (rule 4).
		s.mu.Unlock()
		return nil
	}
	if s.saslClient != "" && s.saslClient != d.ID() {
		s.mu.Unlock()
		return nil
	}
	s.saslClient = d.ID()
	s.mu.Unlock()
	return s.WriteMessage(msg)
}
