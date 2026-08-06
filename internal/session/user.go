package session

import (
	"strings"

	"github.com/MasterBOFH/GoBNC/internal/irc"
)

// User is cached identity for a nick on this network.
type User struct {
	Nick   string
	User   string // ident
	Host   string
	Away   bool
	Oper   bool
	Bot    bool
	Secure bool   // connected via TLS / secure link when known
	Account string // services account; empty if logged out / unknown

	// Self-only (meaningful when this is Session.self).
	UModes      map[byte]bool
	AwayMessage string
}

// Prefix returns nick!user@host from cached fields (best-effort if partial).
func (u *User) Prefix() string {
	if u == nil {
		return ""
	}
	switch {
	case u.User != "" && u.Host != "":
		return u.Nick + "!" + u.User + "@" + u.Host
	case u.User != "":
		return u.Nick + "!" + u.User
	default:
		return u.Nick
	}
}

// ApplyUModes updates UModes from a modestring like "+iw-x".
func (u *User) ApplyUModes(modestring string) {
	if u == nil {
		return
	}
	if u.UModes == nil {
		u.UModes = make(map[byte]bool)
	}
	irc.ApplyUserModes(u.UModes, modestring)
	// Common mappings from usermodes when present.
	if u.UModes['B'] || u.UModes['b'] {
		u.Bot = true
	}
	if u.UModes['Z'] || u.UModes['z'] {
		u.Secure = true
	}
}

// UModeString returns "+iw" or "".
func (u *User) UModeString() string {
	if u == nil {
		return ""
	}
	return irc.UserModeString(u.UModes)
}

// ApplyWHOFlags updates Away/Oper/Bot/Secure from a WHO/WHOX flags field (e.g. "H*@Bs").
func (u *User) ApplyWHOFlags(flags string) {
	if u == nil || flags == "" {
		return
	}
	u.Away = strings.ContainsAny(flags, "G") // gone; H = here
	if strings.Contains(flags, "H") && !strings.Contains(flags, "G") {
		u.Away = false
	}
	if strings.Contains(flags, "*") {
		u.Oper = true
	}
	if strings.ContainsAny(flags, "B") {
		u.Bot = true
	}
	if strings.ContainsAny(flags, "sS") {
		u.Secure = true
	}
}

// UpdateFromPrefix sets Nick/User/Host from "nick!user@host".
func (u *User) UpdateFromPrefix(source string) {
	if u == nil {
		return
	}
	nick, user, host := splitUserhost(source)
	if nick != "" {
		u.Nick = nick
	}
	if user != "" {
		u.User = user
	}
	if host != "" {
		u.Host = host
	}
}

func splitUserhost(source string) (nick, user, host string) {
	bang := strings.IndexByte(source, '!')
	if bang < 0 {
		return source, "", ""
	}
	nick = source[:bang]
	rest := source[bang+1:]
	at := strings.IndexByte(rest, '@')
	if at < 0 {
		return nick, rest, ""
	}
	return nick, rest[:at], rest[at+1:]
}

func (s *Session) ensureSelfLocked(nick string) {
	if s.self == nil {
		s.self = &User{Nick: nick, UModes: make(map[byte]bool)}
	}
	folded := s.isupport.CaseMapping.Canonical(nick)
	if existing := s.users[folded]; existing != nil && existing != s.self {
		s.self.Nick = nick
		if s.self.User == "" {
			s.self.User = existing.User
		}
		if s.self.Host == "" {
			s.self.Host = existing.Host
		}
	}
	s.users[folded] = s.self
	s.self.Nick = nick
}

func (s *Session) ensureUserLocked(nick string) *User {
	if nick == "" {
		return nil
	}
	cm := s.isupport.CaseMapping
	folded := cm.Canonical(nick)
	if u := s.users[folded]; u != nil {
		u.Nick = nick
		return u
	}
	if s.self != nil && cm.Equal(nick, s.self.Nick) {
		s.users[folded] = s.self
		s.self.Nick = nick
		return s.self
	}
	u := &User{Nick: nick}
	s.users[folded] = u
	return u
}

func (s *Session) touchUserFromPrefixLocked(source string) *User {
	nick, user, host := splitUserhost(source)
	if nick == "" || !strings.Contains(source, "!") {
		return nil
	}
	u := s.ensureUserLocked(nick)
	if user != "" {
		u.User = user
	}
	if host != "" {
		u.Host = host
	}
	return u
}

func (s *Session) renameUserLocked(oldNick, newNick string) {
	cm := s.isupport.CaseMapping
	oldKey := cm.Canonical(oldNick)
	newKey := cm.Canonical(newNick)
	u := s.users[oldKey]
	if u == nil {
		u = s.ensureUserLocked(newNick)
	} else {
		delete(s.users, oldKey)
		u.Nick = newNick
		s.users[newKey] = u
	}
	for _, ch := range s.channels {
		if _, ok := ch.Members[oldKey]; ok {
			delete(ch.Members, oldKey)
			ch.Members[newKey] = struct{}{}
		}
		if ch.Modes != nil && ch.Modes.Nicks != nil {
			if modes, ok := ch.Modes.Nicks[oldKey]; ok {
				delete(ch.Modes.Nicks, oldKey)
				ch.Modes.Nicks[newKey] = modes
			}
		}
	}
}

func (s *Session) maybeForgetUserLocked(nick string) {
	cm := s.isupport.CaseMapping
	folded := cm.Canonical(nick)
	if s.self != nil && cm.Equal(nick, s.self.Nick) {
		return
	}
	for _, ch := range s.channels {
		if _, ok := ch.Members[folded]; ok {
			return
		}
	}
	delete(s.users, folded)
}
