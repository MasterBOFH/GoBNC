package session

import (
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

// refreshSASLOffer recomputes the passthrough SASL offer from the uplink's
// current sasl availability — Session's own upSASLAvailable/upSASLMechs
// (kept current by HandleRegistered and HandleLine's CAP interpretation),
// replacing the old uplink.Uplink.SASLAvailable() query. bouncerOwnsSASL is
// just s.Network.SASL now: internal/uplink.Uplink.OwnsSASL() only ever
// reduced to the same field (u.cfg.Network.SASL), so the indirection
// through a live Uplink instance added nothing worth keeping.
func (s *Session) refreshSASLOffer() (prev, now string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev = s.saslOffer
	s.saslOffer = ""
	if !s.Network.SASL && s.upSASLAvailable {
		s.saslOffer = caps.FormatSASL(s.upSASLMechs)
	}
	now = s.saslOffer
	return prev, now
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
		// Login state is session-wide regardless of who drove AUTHENTICATE.
		s.broadcastToDownlinks(msg)
		return
	}

	if s.Network.SASL {
		// AUTHENTICATE and 903–908 stay uplink-only.
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
	}
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
	if s.Network.SASL {
		// Bouncer handles SASL; clients never drive AUTHENTICATE.
		return nil
	}
	if !d.HasCap("sasl") || s.driver == nil {
		return nil
	}
	s.mu.Lock()
	if s.saslClient != "" && s.saslClient != d.ID() {
		s.mu.Unlock()
		return nil
	}
	s.saslClient = d.ID()
	s.mu.Unlock()
	return s.WriteMessage(msg)
}
