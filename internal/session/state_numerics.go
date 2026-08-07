package session

import (
	"strings"

	"github.com/MasterBOFH/GoBNC/internal/irc"
)

func (s *Session) stateWelcomeNumericLocked(msg irc.Message) {
	switch msg.Command {
	case "002":
		if len(msg.Params) > 1 {
			s.rpl002 = append([]string(nil), msg.Params[1:]...)
			s.detectIRCdLocked()
		}
	case "003":
		if len(msg.Params) > 1 {
			s.rpl003 = append([]string(nil), msg.Params[1:]...)
		}
	case "004":
		if len(msg.Params) > 1 {
			s.rpl004 = append([]string(nil), msg.Params[1:]...)
			// Fallback when 002 did not identify the IRCd.
			if s.ircd == "" && len(msg.Params) > 2 {
				if d := irc.DetectIRCdFrom004(msg.Params[2]); d != "" {
					s.ircd = d
					s.tracker.SetIRCd(d)
				}
			}
		}
	case "005":
		s.isupport.Parse005(msg.Params)
	}
}

// detectIRCdLocked sets s.ircd from 002 trailing (preferred) then 004 version.
// Caller must hold s.mu.
func (s *Session) detectIRCdLocked() {
	var text string
	if len(s.rpl002) > 0 {
		text = s.rpl002[len(s.rpl002)-1]
	}
	d := irc.DetectIRCd(text)
	if d == "" && len(s.rpl004) > 1 {
		d = irc.DetectIRCdFrom004(s.rpl004[1])
	}
	if d == "" {
		return
	}
	if d != s.ircd {
		s.ircd = d
		s.log.Info("detected ircd", "ircd", d)
	}
	s.tracker.SetIRCd(d)
}

// IRCd returns the detected uplink IRCd family, or empty if unknown.
func (s *Session) IRCd() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ircd
}

func (s *Session) state221Locked(msg irc.Message) {
	ms := msg.Param(1)
	if ms != "" && ms[0] != '+' && ms[0] != '-' {
		ms = "+" + ms
	}
	s.self.ApplyUModes(ms)
}

func (s *Session) state301Locked(msg irc.Message) {
	if u := s.ensureUserLocked(msg.Param(1)); u != nil {
		u.Away = true
		u.AwayMessage = msg.Trailing()
	}
}

func (s *Session) state305Locked(msg irc.Message) {
	_ = msg
	s.self.Away = false
	s.self.AwayMessage = ""
}

func (s *Session) state306Locked(msg irc.Message) {
	_ = msg
	s.self.Away = true
}

func (s *Session) state332Locked(msg irc.Message) {
	chName := msg.Param(1)
	if ch := s.channels[s.isupport.CaseMapping.Canonical(chName)]; ch != nil {
		ch.Topic = msg.Trailing()
	}
}

func (s *Session) state352Locked(msg irc.Message) {
	// RPL_WHOREPLY: ... user host server nick flags :hops realname
	if len(msg.Params) < 6 {
		return
	}
	u := s.ensureUserLocked(msg.Param(5))
	u.User = msg.Param(2)
	u.Host = msg.Param(3)
	u.ApplyWHOFlags(msg.Param(6))
}

func (s *Session) state353Locked(msg irc.Message) {
	// RPL_NAMREPLY: nick = #chan :@a +b c
	if len(msg.Params) < 3 {
		return
	}
	chName := msg.Param(2)
	key := s.isupport.CaseMapping.Canonical(chName)
	ch := s.channels[key]
	if ch == nil {
		ch = &ChannelState{Name: chName, Modes: irc.NewChannelModes(), Members: map[string]struct{}{}}
		s.channels[key] = ch
	}
	ms := s.isupport.Modes
	for _, ent := range strings.Fields(msg.Trailing()) {
		nick := ent
		var prefixes []byte
		for len(nick) > 0 {
			sym := nick[0]
			if ms != nil && ms.PrefixBySymbol[sym] != 0 {
				prefixes = append(prefixes, ms.PrefixBySymbol[sym])
				nick = nick[1:]
				continue
			}
			break
		}
		if nick == "" {
			continue
		}
		u := s.ensureUserLocked(nick)
		folded := s.isupport.CaseMapping.Canonical(u.Nick)
		ch.Members[folded] = struct{}{}
		if len(prefixes) > 0 && ch.Modes != nil {
			if ch.Modes.Nicks == nil {
				ch.Modes.Nicks = make(map[string]map[byte]bool)
			}
			if ch.Modes.Nicks[folded] == nil {
				ch.Modes.Nicks[folded] = make(map[byte]bool)
			}
			for _, m := range prefixes {
				ch.Modes.Nicks[folded][m] = true
			}
		}
	}
}

func (s *Session) state381Locked(msg irc.Message) {
	_ = msg
	s.self.Oper = true
}

func (s *Session) state396Locked(msg irc.Message) {
	if len(msg.Params) >= 2 && s.isupport.CaseMapping.Equal(msg.Param(0), s.self.Nick) {
		s.self.Host = msg.Param(1)
	}
}
