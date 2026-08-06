package session

import "github.com/MasterBOFH/GoBNC/internal/irc"

func (s *Session) stateJOINLocked(msg irc.Message) {
	cm := s.isupport.CaseMapping
	chName := msg.Param(0)
	if chName == "" {
		chName = msg.Trailing()
	}
	key := cm.Canonical(chName)
	ch := s.channels[key]
	if ch == nil {
		ch = &ChannelState{Name: chName, Modes: irc.NewChannelModes(), Members: map[string]struct{}{}}
		s.channels[key] = ch
	}
	u := s.touchUserFromPrefixLocked(msg.Source)
	if u == nil {
		u = s.ensureUserLocked(msg.Nick())
	}
	ch.Members[cm.Canonical(u.Nick)] = struct{}{}
	if u == s.self {
		ch.Name = chName
	}
	// extended-join: JOIN #chan account :Real Name
	if len(msg.Params) >= 2 {
		acct := msg.Param(1)
		if acct == "*" {
			u.Account = ""
		} else if acct != "" {
			u.Account = acct
		}
	}
}

func (s *Session) statePartKickLocked(msg irc.Message) (remove []string) {
	cm := s.isupport.CaseMapping
	chName := msg.Param(0)
	key := cm.Canonical(chName)
	nick := msg.Nick()
	if msg.Command == "KICK" {
		nick = msg.Param(1)
	}
	ch := s.channels[key]
	if ch == nil {
		return nil
	}
	delete(ch.Members, cm.Canonical(nick))
	if cm.Equal(nick, s.self.Nick) {
		delete(s.channels, key)
		return []string{chName}
	}
	s.maybeForgetUserLocked(nick)
	return nil
}

func (s *Session) stateQUITLocked(msg irc.Message) {
	cm := s.isupport.CaseMapping
	nick := msg.Nick()
	folded := cm.Canonical(nick)
	for _, ch := range s.channels {
		delete(ch.Members, folded)
		if ch.Modes != nil && ch.Modes.Nicks != nil {
			delete(ch.Modes.Nicks, folded)
		}
	}
	if s.self == nil || !cm.Equal(nick, s.self.Nick) {
		delete(s.users, folded)
	}
}

func (s *Session) stateTOPICLocked(msg irc.Message) {
	chName := msg.Param(0)
	if ch := s.channels[s.isupport.CaseMapping.Canonical(chName)]; ch != nil {
		ch.Topic = msg.Trailing()
	}
}

func (s *Session) stateNICKLocked(msg irc.Message) {
	old := msg.Nick()
	newNick := msg.Trailing()
	if newNick == "" {
		newNick = msg.Param(0)
	}
	s.renameUserLocked(old, newNick)
}

func (s *Session) stateAWAYLocked(msg irc.Message) {
	cm := s.isupport.CaseMapping
	// Client AWAY sets message; empty clears. Upstream may echo similarly.
	if cm.Equal(msg.Nick(), s.self.Nick) || msg.Source == "" {
		s.self.AwayMessage = msg.Trailing()
		s.self.Away = s.self.AwayMessage != ""
		return
	}
	if u := s.users[cm.Canonical(msg.Nick())]; u != nil {
		u.AwayMessage = msg.Trailing()
		u.Away = u.AwayMessage != ""
	}
}

// stateCHGHOSTLocked updates user/host from :nick!u@h CHGHOST newuser newhost.
func (s *Session) stateCHGHOSTLocked(msg irc.Message) {
	u := s.touchUserFromPrefixLocked(msg.Source)
	if u == nil {
		return
	}
	if nu := msg.Param(0); nu != "" {
		u.User = nu
	}
	if nh := msg.Param(1); nh != "" {
		u.Host = nh
	}
}

// stateACCOUNTLocked updates services account from :nick ACCOUNT name|* .
func (s *Session) stateACCOUNTLocked(msg irc.Message) {
	u := s.touchUserFromPrefixLocked(msg.Source)
	if u == nil {
		return
	}
	acct := msg.Param(0)
	if acct == "" {
		acct = msg.Trailing()
	}
	if acct == "*" {
		acct = ""
	}
	u.Account = acct
}

func (s *Session) stateMODELocked(msg irc.Message) (persist [][2]string) {
	cm := s.isupport.CaseMapping
	target := msg.Param(0)
	if len(target) > 0 && (target[0] == '#' || target[0] == '&' || target[0] == '+' || target[0] == '!') {
		ch := s.channels[cm.Canonical(target)]
		if ch == nil || s.isupport.Modes == nil || len(msg.Params) <= 1 {
			return nil
		}
		changes := s.isupport.Modes.ParseModeParams(msg.Params[1], msg.Params[2:])
		ch.Modes.Apply(s.isupport.Modes, cm, changes)
		for _, chg := range changes {
			if chg.Mode == 'k' {
				if chg.Set {
					ch.Key = chg.Arg
				} else {
					ch.Key = ""
				}
				persist = append(persist, [2]string{ch.Name, ch.Key})
			}
		}
		return persist
	}
	if cm.Equal(target, s.self.Nick) {
		modestring := msg.Param(1)
		if modestring == "" {
			modestring = msg.Trailing()
		}
		s.self.ApplyUModes(modestring)
	}
	return nil
}
