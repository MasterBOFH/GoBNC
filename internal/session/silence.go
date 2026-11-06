package session

import (
	"strings"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// The server's SILENCE list, per network, for modules (gobnc-retecloud's sync and its silence
// frame). GoBNC queries the list silently after registration (RefreshSilence; its 271/272
// replies route to internalClient) and follows the server's SILENCE lines after a change. A
// client's own query of our list (271s naming our nick) refreshes it as well; a query of
// another nick's list passes through and changes nothing.

// RefreshSilence queries the SILENCE list silently when the network advertises SILENCE. Call
// only where writing to the uplink is allowed: after a live registration completes, or after
// LiveReady for a resumed network.
func (s *Session) RefreshSilence() {
	s.mu.RLock()
	_, has := s.isupport.Raw["SILENCE"]
	ok := s.registered && has
	s.mu.RUnlock()
	if !ok {
		return
	}
	out := irc.Message{Command: "SILENCE"}
	_, _, writeNow := s.tracker.Begin(BeginOpts{Client: internalClient, Cmd: "SILENCE", Outbound: out})
	if writeNow {
		_ = s.WriteMessage(out)
	}
}

// stateSILENCELocked follows the server's SILENCE line for a change of our list
// ("+mask", "-mask", comma-separated). Caller holds s.mu.
func (s *Session) stateSILENCELocked(msg irc.Message) {
	if src := msg.Nick(); src != "" && s.self != nil && !s.isupport.CaseMapping.Equal(src, s.self.Nick) {
		return
	}
	for _, item := range strings.Split(msg.Param(0), ",") {
		switch {
		case item == "":
		case item[0] == '-':
			s.removeSilenceLocked(item[1:])
		case item[0] == '+':
			s.addSilenceLocked(item[1:])
		default:
			s.addSilenceLocked(item)
		}
	}
}

func (s *Session) addSilenceLocked(mask string) {
	if mask == "" {
		return
	}
	for _, m := range s.silence {
		if m == mask {
			return
		}
	}
	s.silence = append(s.silence, mask)
	s.silenceDirty = true
}

func (s *Session) removeSilenceLocked(mask string) {
	for i, m := range s.silence {
		if m == mask {
			s.silence = append(s.silence[:i:i], s.silence[i+1:]...)
			s.silenceDirty = true
			return
		}
	}
}

// state271Locked collects one RPL_SILELIST entry (271 me <nick> <mask>) of our own list.
// Caller holds s.mu.
func (s *Session) state271Locked(msg irc.Message) {
	if len(msg.Params) < 3 || s.self == nil || !s.isupport.CaseMapping.Equal(msg.Param(1), s.self.Nick) {
		return
	}
	s.silenceReply = append(s.silenceReply, msg.Param(2))
	s.silenceReplyOurs = true
}

// state272Locked ends a list reply: the collected entries replace the list when the reply was
// of our own list (an entry named us, or it answers GoBNC's own query, which an empty list
// answers with the 272 alone). Caller holds s.mu.
func (s *Session) state272Locked(ourQuery bool) {
	next := s.silenceReply
	ours := s.silenceReplyOurs || ourQuery
	s.silenceReply, s.silenceReplyOurs = nil, false
	if !ours {
		return
	}
	if next == nil {
		next = []string{}
	}
	same := len(next) == len(s.silence)
	for i := 0; same && i < len(next); i++ {
		same = next[i] == s.silence[i]
	}
	if !same {
		s.silence = next
		s.silenceDirty = true
	}
}

// emitSilenceLocked emits a state/silence event when the list changed. Caller holds busMu.
func (s *Session) emitSilenceLocked() {
	s.mu.Lock()
	dirty := s.silenceDirty
	s.silenceDirty = false
	list := append([]string{}, s.silence...)
	s.mu.Unlock()
	if !dirty || !s.hasSinksLocked() {
		return
	}
	s.emitLocked(wire.StateEvent{Network: s.Network.ID, Kind: wire.StateSilence, Silence: list})
}
