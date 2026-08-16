package session

import (
	"strings"

	"github.com/MasterBOFH/GoBNC/internal/irc"
)

// state001Locked records the uplink's 001 source prefix and the assigned
// nick. Returns the uplink-server blob value and the self-nick blob value
// to push once s.mu is released — see applyState. 001 is never replayed on
// a resumed attach (gap-only), so without these blob keys every later
// attach burst would fall back to ServerName and to Network.Nick (the nick
// we requested at registration, not the one the server assigned).
func (s *Session) state001Locked(msg irc.Message) (uplinkServer, selfNick []byte) {
	if src := strings.TrimSpace(msg.Source); src != "" {
		s.uplinkServer = src
		uplinkServer = []byte(src)
	}
	if len(msg.Params) > 0 && msg.Params[0] != "" {
		s.ensureSelfLocked(msg.Params[0])
		selfNick = []byte(msg.Params[0])
	}
	return uplinkServer, selfNick
}

// stateWelcomeNumericLocked returns the blob key and JSON-encoded value to
// push once s.mu is released (see applyState). 005 appends under "isupport";
// 002/003/004 replace under "rpl002"/"rpl003"/"rpl004" — those numerics
// are registration-only and cannot be re-queried, so a resumed Attach has
// nothing to replay them from unless the blob carried them.
func (s *Session) stateWelcomeNumericLocked(msg irc.Message) (key string, value []byte) {
	switch msg.Command {
	case "002":
		if len(msg.Params) > 1 {
			s.rpl002 = append([]string(nil), msg.Params[1:]...)
			s.detectIRCdLocked()
			return "rpl002", blobParams(s.rpl002)
		}
	case "003":
		if len(msg.Params) > 1 {
			s.rpl003 = append([]string(nil), msg.Params[1:]...)
			return "rpl003", blobParams(s.rpl003)
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
			return "rpl004", blobParams(s.rpl004)
		}
	case "005":
		s.isupport.Parse005(msg.Params)
		return "isupport", blobParams(msg.Params)
	}
	return "", nil
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
	if s.self == nil {
		return
	}
	ms := msg.Param(1)
	if ms == "" {
		ms = msg.Trailing()
	}
	if ms != "" && ms[0] != '+' && ms[0] != '-' {
		ms = "+" + ms
	}
	// RPL_UMODEIS is the full current set, not a delta — replace rather
	// than ApplyUModes onto leftovers from a previous life (empty after
	// resume, but a later 221 must not keep modes the server no longer
	// reports).
	s.self.UModes = make(map[byte]bool)
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
	// self's ident/host is only ever trusted from RPL_LOGGEDIN (900), CHGHOST,
	// or an observed nick!user@host prefix (touchUserFromPrefixLocked) — a WHO
	// reply's user/host fields for self are not one of those and have been
	// observed to diverge from the real cloak (e.g. a raw connecting address
	// instead of a cloak), so this only applies them to other nicks.
	if u != s.self {
		u.User = msg.Param(2)
		u.Host = msg.Param(3)
	}
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

// state366Locked handles RPL_ENDOFNAMES (366 <nick> <channel> :End of
// /NAMES list.), marking the channel's roster confirmed — see
// ChannelState.RosterKnown's doc comment for why Session.Attach depends on
// this rather than treating Members as trustworthy the moment a
// ChannelState exists. Fires for any 366, not just one
// RefreshResumedChannelNames solicited — an ordinary live self-JOIN's own
// automatic 353/366 burst confirms the roster exactly the same way, so
// this needs no special-casing for how the NAMES answer came about.
func (s *Session) state366Locked(msg irc.Message) {
	if len(msg.Params) < 2 {
		return
	}
	if ch := s.channels[s.isupport.CaseMapping.Canonical(msg.Param(1))]; ch != nil {
		ch.RosterKnown = true
	}
}

// state302Locked handles RPL_USERHOST (302 <nick> :entries...), each entry
// shaped "nick[*]=[+-]ident@host" (the optional "*" marks an IRC operator,
// the mandatory "+"/"-" marks away status) — used to learn self's own
// ident/host (see Session.RefreshSelfUserHost, the proactive query this
// answers) without depending on a self-JOIN echo, which a resumed session
// never gets (RegisterResumedNetwork never redrives registration, so no
// JOIN is ever sent to the uplink for an already-joined channel — see
// docs/keeper-design.md). Returns the new host as the cloak blob entry to
// push (once s.mu is released — see applyState) when it updates self,
// nil otherwise. Only self's own entry is applied; any other nick's in the
// same reply (a multi-nick USERHOST this brain didn't itself issue) is
// left alone — Session tracks identity for self and channel members via
// other paths, not a general USERHOST cache.
func (s *Session) state302Locked(msg irc.Message) []byte {
	if s.self == nil || len(msg.Params) < 2 {
		return nil
	}
	cm := s.isupport.CaseMapping
	for _, ent := range strings.Fields(msg.Trailing()) {
		eq := strings.IndexByte(ent, '=')
		if eq < 0 {
			continue
		}
		nick := strings.TrimSuffix(ent[:eq], "*")
		if !cm.Equal(nick, s.self.Nick) {
			continue
		}
		rest := ent[eq+1:]
		if len(rest) > 0 && (rest[0] == '+' || rest[0] == '-') {
			rest = rest[1:]
		}
		at := strings.IndexByte(rest, '@')
		if at < 0 {
			continue
		}
		s.self.User = rest[:at]
		host := rest[at+1:]
		s.self.Host = host
		return []byte(host)
	}
	return nil
}

func (s *Session) state381Locked(msg irc.Message) {
	_ = msg
	s.self.Oper = true
}

// state396Locked returns the new host as the cloak blob entry to push
// (once s.mu is released — see applyState) when it changes self's host,
// nil otherwise.
func (s *Session) state396Locked(msg irc.Message) []byte {
	if len(msg.Params) >= 2 && s.isupport.CaseMapping.Equal(msg.Param(0), s.self.Nick) {
		s.self.Host = msg.Param(1)
		return []byte(msg.Param(1))
	}
	return nil
}
