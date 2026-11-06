package server

import (
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// recordLastNick stores the nick sess has on its uplink as the network's last nick, so the
// network keeps it once stopped (store.LastNick). Called when the uplink registers (the nick
// it got, the alternate after a collision) and when the network stops (any NICK since).
func (s *Server) recordLastNick(n store.Network, sess *session.Session) {
	nick := sess.Nick()
	if nick == "" {
		return
	}
	if err := s.store.SetLastNick(s.storeCtx(), n.ID, nick); err != nil {
		s.log.Warn("record last nick", "network", n.Name, "err", err)
	}
}

// stoppedNick is the nick of a network no Session runs: the one it last had, else its
// configured nick.
func (s *Server) stoppedNick(n store.Network) string {
	if nick, err := s.store.LastNick(s.storeCtx(), n.ID); err == nil && nick != "" {
		return nick
	}
	return n.Nick
}
