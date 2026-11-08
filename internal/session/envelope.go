package session

import (
	"context"
	"strings"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// routingState is what an envelope needs from before applyState: whether the line is ours, and
// for user-wide lines (QUIT, NICK, AWAY, ...) the channels the user shared with us.
type routingState struct {
	self     bool
	channels []string // display names
}

// userWideCommands are lines about a user rather than a target: they go to every channel the
// user shared with us, plus their query.
var userWideCommands = map[string]bool{
	"QUIT": true, "NICK": true, "AWAY": true, "ACCOUNT": true, "CHGHOST": true, "SETNAME": true,
}

// routingBefore captures msg's routingState. Call before applyState, holding busMu.
func (s *Session) routingBefore(msg irc.Message) routingState {
	if !s.hasSinksLocked() {
		return routingState{}
	}
	rs := routingState{self: s.isSelfNick(msg.Nick())}
	if !userWideCommands[msg.Command] {
		return rs
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	folded := s.isupport.CaseMapping.Canonical(msg.Nick())
	for _, ch := range s.channels {
		if _, ok := ch.Members[folded]; ok {
			rs.channels = append(rs.channels, ch.Name)
		}
	}
	return rs
}

// envelopeLocked builds the module line event for msg: kind, buffers (display name, casemapped
// key and the history id where that buffer stored the line), source and self. Caller holds busMu.
func (s *Session) envelopeLocked(msg irc.Message, ids map[string]int64, pre routingState, label string) wire.LineEvent {
	ev := wire.LineEvent{
		Network: s.Network.ID,
		Kind:    lineKind(msg),
		Raw:     msg.Wire(),
		Source:  msg.Source,
		Nick:    msg.Nick(),
		Self:    pre.self,
		Label:   label,
		Buffers: []wire.LineBuffer{},
	}
	ev.MsgID, _ = msg.Tag("msgid")
	ev.Time, _ = msg.Tag("time")

	s.mu.RLock()
	cm := s.isupport.CaseMapping
	chantypes := s.isupport.Raw["CHANTYPES"]
	statusmsg := s.isupport.Raw["STATUSMSG"]
	s.mu.RUnlock()
	isChan := func(name string) bool { return isChannelTarget(name, chantypes) }
	addKey := func(name, key string, storedAs ...string) {
		for _, b := range ev.Buffers {
			if b.Key == key {
				return
			}
		}
		b := wire.LineBuffer{Name: s.displayName(key, name), Key: key, ID: ids[key]}
		for _, alt := range storedAs {
			if b.ID == 0 {
				b.ID = ids[cm.Canonical(alt)]
			}
		}
		ev.Buffers = append(ev.Buffers, b)
	}
	add := func(name string, storedAs ...string) { addKey(name, cm.Canonical(name), storedAs...) }
	// The Server buffer is keyed "*" (wire.ServerBufferKey), not by its name's fold: a real nick
	// "Server" is an ordinary query. A stored line of it carries the id of its row under the
	// server's name (serverTargetOf).
	addServer := func(storedAs ...string) { addKey(wire.ServerBuffer, wire.ServerBufferKey, storedAs...) }

	switch msg.Command {
	case "PRIVMSG", "NOTICE", "TAGMSG":
		t := msg.Param(0)
		switch {
		case isChan(stripStatusMsg(t, statusmsg, isChan)):
			add(stripStatusMsg(t, statusmsg, isChan), t) // whatever its source, a server's included
		case s.inServerBuffer(msg):
			addServer(s.serverTargetOf(msg))
		case pre.self:
			add(t) // our own line to someone: their query
		default:
			add(msg.Nick()) // to us: the sender's query
		}
	case "JOIN":
		t := msg.Param(0)
		if t == "" {
			t = msg.Trailing()
		}
		if t != "" {
			add(t)
		}
	case "PART", "KICK", "TOPIC":
		if t := msg.Param(0); t != "" {
			add(t)
		}
	case "MODE":
		if t := msg.Param(0); isChan(t) {
			add(t)
		}
	case "ERROR":
		addServer()
	default:
		if userWideCommands[msg.Command] {
			for _, name := range pre.channels {
				add(name)
			}
			nick := msg.Nick()
			if newNick := msg.Param(0); msg.Command == "NICK" && newNick != "" && ids[cm.Canonical(newNick)] > 0 {
				add(newNick) // the query's copy, stored under the name the query now has
				if ids[cm.Canonical(nick)] > 0 {
					add(nick) // a device kept the old query too: its own copy
				}
			} else if nick != "" && !pre.self && s.hasQuery(nick) {
				add(nick)
			}
			break
		}
		if isNumeric(msg.Command) {
			if t := msg.Param(1); isChan(t) {
				add(t)
			} else {
				addServer()
			}
		}
	}
	return ev
}

// displayName prefers the spelling of a joined channel's own state over the line's.
func (s *Session) displayName(key, name string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if ch := s.channels[key]; ch != nil && ch.Name != "" {
		return ch.Name
	}
	return name
}

// hasQuery reports whether history holds a query buffer for nick.
func (s *Session) hasQuery(nick string) bool {
	if s.store == nil || s.Network.ID == 0 {
		return false
	}
	s.mu.RLock()
	key := s.isupport.CaseMapping.Canonical(nick)
	s.mu.RUnlock()
	ok, _ := s.store.HasTarget(context.Background(), s.Network.ID, key)
	return ok
}

// lineKind classifies msg into a wire line kind.
func lineKind(msg irc.Message) string {
	switch msg.Command {
	case "PRIVMSG":
		if verb, _, ok := parseCTCP(msg.Trailing()); ok && !strings.EqualFold(verb, "ACTION") {
			return wire.LineCTCP
		}
		return wire.LineMessage
	case "NOTICE":
		if _, _, ok := parseCTCP(msg.Trailing()); ok {
			return wire.LineCTCPReply
		}
		return wire.LineNotice
	case "TAGMSG":
		return wire.LineTagmsg
	case "JOIN", "PART", "QUIT", "KICK", "NICK", "MODE", "TOPIC", "INVITE", "AWAY", "ACCOUNT", "CHGHOST", "SETNAME":
		return strings.ToLower(msg.Command)
	case "ERROR":
		return wire.LineError
	}
	if isNumeric(msg.Command) {
		return wire.LineNumeric
	}
	return wire.LineOther
}

func isNumeric(cmd string) bool {
	return len(cmd) == 3 && cmd[0] >= '0' && cmd[0] <= '9' && cmd[1] >= '0' && cmd[1] <= '9' && cmd[2] >= '0' && cmd[2] <= '9'
}

// inServerBuffer reports whether a PRIVMSG/NOTICE/TAGMSG belongs to the Server buffer: server-
// sourced or to a $mask, and not to a channel (a channel's line is the channel's, whatever its
// source). envelopeLocked routes and historyTargets stores by this one rule, so the envelope's
// Server buffer carries the id the line was stored with.
func (s *Session) inServerBuffer(msg irc.Message) bool {
	t := msg.Param(0)
	if !serverSourced(msg) && !strings.HasPrefix(t, "$") {
		return false
	}
	s.mu.RLock()
	chantypes, statusmsg := s.isupport.Raw["CHANTYPES"], s.isupport.Raw["STATUSMSG"]
	s.mu.RUnlock()
	isChan := func(name string) bool { return isChannelTarget(name, chantypes) }
	return !isChan(stripStatusMsg(t, statusmsg, isChan))
}

// serverTargetOf is the history target a Server-buffer line (inServerBuffer) is stored under:
// its server's own name, folded — a notice from irc.example.net under "irc.example.net", an
// ordinary chathistory target that no nick can be. A line naming no server (no source, or a
// person's $mask notice) goes to the server we registered to (001's source), or before that is
// known to the network's configured host; "" (not stored) when neither is a server name.
func (s *Session) serverTargetOf(msg irc.Message) string {
	s.mu.RLock()
	cm, uplink := s.isupport.CaseMapping, s.uplinkServer
	s.mu.RUnlock()
	for _, name := range []string{msg.Source, uplink, s.Network.Host} {
		if name = strings.TrimSpace(name); irc.IsServerName(name) {
			return cm.Canonical(name)
		}
	}
	return ""
}

// statusMsgChannel returns the channel a STATUSMSG target (@#chan, +#chan) is addressed to;
// ok is false for any other target.
func (s *Session) statusMsgChannel(t string) (string, bool) {
	s.mu.RLock()
	chantypes, statusmsg := s.isupport.Raw["CHANTYPES"], s.isupport.Raw["STATUSMSG"]
	s.mu.RUnlock()
	isChan := func(name string) bool { return isChannelTarget(name, chantypes) }
	if ch := stripStatusMsg(t, statusmsg, isChan); ch != t && isChan(ch) {
		return ch, true
	}
	return "", false
}

// serverSourced: no source, or a server name (no "!", contains ".").
func serverSourced(msg irc.Message) bool {
	return ServerSource(msg.Source)
}

// ServerSource reports whether a line's source is a server rather than a person (irc.ServerSource).
func ServerSource(src string) bool { return irc.ServerSource(src) }

// stripStatusMsg removes ISUPPORT STATUSMSG prefixes (@#chan → #chan) from a channel target.
func stripStatusMsg(t, statusmsg string, isChan func(string) bool) string {
	for len(t) > 1 && strings.IndexByte(statusmsg, t[0]) >= 0 && !isChan(t) {
		t = t[1:]
	}
	return t
}
