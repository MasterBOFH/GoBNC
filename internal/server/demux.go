package server

import (
	"context"
	"fmt"

	"github.com/MasterBOFH/GoBNC/internal/keeper"
	"github.com/MasterBOFH/GoBNC/internal/registration"
	"github.com/MasterBOFH/GoBNC/internal/session"
)

// runDemux is the one goroutine that reads the shared *brain.Driver's
// event stream and routes each event to the right *session.Session by the
// keeper.NetworkID every event carries — see docs/keeper-design.md's
// "one Driver per process, not one per network" note. Driver has no
// fan-out of its own (only one goroutine may read it, matching
// AttachClient's own single-reader contract) — this is that one goroutine
// for the whole server.
//
// Exits when ctx is done or Driver's channels close (Driver.Run returned,
// which closes every channel together — see Driver.Run's own defers), and
// treats the first closed channel it observes as authoritative rather than
// busy-looping on the others (a closed channel is always select-ready).
func (s *Server) runDemux(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case line, ok := <-s.driver.Lines():
			if !ok {
				return
			}
			if sess := s.sessionByNetwork(line.Network); sess != nil {
				sess.HandleLine(line.Raw)
			}
		case res, ok := <-s.driver.Results():
			if !ok {
				return
			}
			// Registration *success* is detected by Session itself,
			// directly from the raw line stream (see
			// session.Session.HandleLine's doc comment for why: it avoids
			// a real cross-channel ordering race this demux would
			// otherwise have to solve). Only the failure case — deadline
			// exceeded, nick ladder exhausted, SASL required but failed —
			// has no other observable signal, since Driver.failRegistration
			// closes the connection deliberately (no NetworkEvent follows).
			if res.State.Phase == registration.PhaseFailed {
				if sess := s.sessionByNetwork(res.Network); sess != nil {
					sess.HandleDisconnect(res.State.Err)
				}
			}
		case ev, ok := <-s.driver.NetworkEvents():
			if !ok {
				return
			}
			if ev.Kind == keeper.EventDisconnected {
				if sess := s.sessionByNetwork(ev.Network); sess != nil {
					sess.HandleDisconnect(networkEventErr(ev))
				}
			}
		case dr, ok := <-s.driver.DialResults():
			if !ok {
				return
			}
			if dr.OK {
				if err := s.driver.StartRegistration(dr.Network); err != nil {
					s.log.Warn("StartRegistration", "network", dr.Network, "err", err)
				}
			}
		}
	}
}

func networkEventErr(ev keeper.NetworkEventMsg) error {
	if ev.Error == "" {
		return nil
	}
	return fmt.Errorf("%s", ev.Error)
}

// sessionByNetwork looks up the session tracking keeper.NetworkID id.
func (s *Server) sessionByNetwork(id keeper.NetworkID) *session.Session {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sessByNetID[id]
}
