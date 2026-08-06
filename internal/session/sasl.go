package session

import (
	"github.com/MasterBOFH/GoBNC/internal/caps"
	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/internal/uplink"
)

// CapEnabler is implemented by downlinks that can enable a negotiated capability.
type CapEnabler interface {
	EnableCap(name string)
}

func bouncerOwnsSASL(n store.Network, u *uplink.Uplink) bool {
	if u != nil {
		return u.OwnsSASL()
	}
	return n.SASLUser != "" && n.SASLPass != ""
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

func (s *Session) refreshSASLOffer(u *uplink.Uplink) (prev, now string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev = s.saslOffer
	s.saslOffer = ""
	if !bouncerOwnsSASL(s.Network, u) && u != nil {
		if mechs, ok := u.SASLAvailable(); ok {
			s.saslOffer = caps.FormatSASL(mechs)
		}
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

// OnSASLOffer implements uplink.Handler — sasl available without bouncer REQ.
func (s *Session) OnSASLOffer(u *uplink.Uplink, available bool) {
	_ = available
	prev, now := s.refreshSASLOffer(u)
	s.notifySASLOfferChange(prev, now)
}

// OnCapNAK implements uplink.Handler.
func (s *Session) OnCapNAK(u *uplink.Uplink, names []string) {
	_ = u
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
	if s.uplink != nil && s.uplink.HasCap("sasl") {
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
	if s.uplink == nil {
		s.mu.Lock()
		s.saslWaiters = nil
		s.saslReqPending = false
		s.mu.Unlock()
		return s.sendClientCapNAK(d, "sasl")
	}
	return s.uplink.RequestCap("sasl")
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

func (s *Session) routeSASLPassthrough(msg irc.Message) bool {
	if bouncerOwnsSASL(s.Network, s.uplink) {
		return false
	}
	s.mu.RLock()
	id := s.saslClient
	d, ok := s.downlinks[id]
	if !ok {
		for _, dl := range s.downlinks {
			if dl.HasCap("sasl") {
				d = dl
				ok = true
				break
			}
		}
	}
	s.mu.RUnlock()
	if !ok {
		return true
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
	return true
}

func (s *Session) forwardClientAuthenticate(d Downlink, msg irc.Message) error {
	if !d.HasCap("sasl") || s.uplink == nil {
		return nil
	}
	s.mu.Lock()
	s.saslClient = d.ID()
	s.mu.Unlock()
	return s.uplink.WriteMessage(msg)
}
