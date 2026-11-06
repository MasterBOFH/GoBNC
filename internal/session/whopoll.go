package session

import (
	"sort"
	"strings"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
)

// Member metadata for modules (gobnc-retecloud's lite member lists): hosts, accounts and the
// away/oper/bot flags. GoBNC learns them without polling beyond what its clients do — from
// userhost-in-names, away-notify / account-notify / chghost, members' own lines, and every
// WHO/WHOX reply that passes through for a client — and polls WHOX itself only for a channel
// no reply has named within the interval. Its own polls are silent: the replies are routed to
// internalClient, which no downlink or module ever is.

// internalClient owns GoBNC's own enquiries (WHOX polls, the SILENCE list query). RouteAll
// returns it as the replies' destination, so they are neither broadcast nor emitted to modules,
// and no downlink has this id.
const internalClient ClientID = "\x00gobnc"

// whoPollFields is GoBNC's own WHOX: token, channel, user, host, nick, flags, account.
const whoPollFields = "tcuhnfa"

// whoxFieldOrder is the order WHOX reply fields come in, whatever order they were asked in.
const whoxFieldOrder = "tcuihsnfdlaor"

// PollWHO sends at most one silent WHOX poll: for the joined channel whose members a WHO reply
// named longest ago, provided that was at least interval before now. It polls only while a
// module is subscribed (nothing else reads the metadata), the network is registered and has
// WHOX, and interval > 0. It returns the polled channel's name, or "".
func (s *Session) PollWHO(now time.Time, interval time.Duration) string {
	if interval <= 0 {
		return ""
	}
	s.busMu.Lock()
	sinks := s.hasSinksLocked()
	s.busMu.Unlock()
	if !sinks {
		return ""
	}
	s.mu.Lock()
	if !s.registered || s.isupport == nil || !s.isupport.WHOX {
		s.mu.Unlock()
		return ""
	}
	keys := make([]string, 0, len(s.channels))
	for key, ch := range s.channels {
		if ch.RosterKnown {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	pick := ""
	var oldest time.Time
	for _, key := range keys {
		last := s.lastWHO[key]
		if now.Sub(last) < interval {
			continue
		}
		if pick == "" || last.Before(oldest) {
			pick, oldest = key, last
		}
	}
	if pick == "" {
		s.mu.Unlock()
		return ""
	}
	name := s.channels[pick].Name
	s.noteWHOAtLocked(pick, now)
	s.mu.Unlock()

	_, tok, writeNow := s.tracker.Begin(BeginOpts{Client: internalClient, Cmd: "WHO", PreferWHOX: true,
		WHOMask: pick, WHOXFields: whoPollFields})
	if !writeNow || tok == "" {
		return name // an identical client WHOX is in flight; its replies teach the same
	}
	_ = s.WriteMessage(irc.Message{Command: "WHO", Params: []string{name, "%" + whoPollFields + "," + tok}})
	return name
}

// noteWHOLocked records that a WHO reply just named channel. Caller holds s.mu.
func (s *Session) noteWHOLocked(channel string) {
	if channel == "" || !isChannelName(channel) {
		return
	}
	s.noteWHOAtLocked(s.isupport.CaseMapping.Canonical(channel), time.Now())
}

func (s *Session) noteWHOAtLocked(key string, at time.Time) {
	if s.lastWHO == nil {
		s.lastWHO = make(map[string]time.Time)
	}
	if at.After(s.lastWHO[key]) {
		s.lastWHO[key] = at
	}
}

// state354Locked learns from RPL_WHOSPCRPL (354), whose fields the request named (fields, from
// the tracker; 't' when the reply carries a token). Self's ident/host are not taken from it,
// as with 352. Caller holds s.mu.
func (s *Session) state354Locked(msg irc.Message, fields string) {
	vals := map[byte]string{}
	i := 1 // params[0] is our nick
	for j := 0; j < len(whoxFieldOrder); j++ {
		f := whoxFieldOrder[j]
		if strings.IndexByte(fields, f) < 0 {
			continue
		}
		if i >= len(msg.Params) {
			return
		}
		vals[f] = msg.Params[i]
		i++
	}
	nick := vals['n']
	if nick == "" {
		return
	}
	u := s.ensureUserLocked(nick)
	if u != s.self {
		if v, ok := vals['u']; ok && v != "" {
			u.User = v
		}
		if v, ok := vals['h']; ok && v != "" {
			u.Host = v
		}
	}
	if v, ok := vals['f']; ok {
		u.ApplyWHOFlags(v)
	}
	if v, ok := vals['a']; ok {
		if v == "0" { // WHOX: not logged in
			v = ""
		}
		u.Account = v
	}
	s.noteWHOLocked(vals['c'])
}

// stripNamesUserhost turns a userhost-in-names RPL_NAMREPLY back into the plain form.
func stripNamesUserhost(msg irc.Message) irc.Message {
	if len(msg.Params) == 0 {
		return msg
	}
	ents := strings.Fields(msg.Params[len(msg.Params)-1])
	changed := false
	for i, ent := range ents {
		if j := strings.IndexByte(ent, '!'); j >= 0 {
			ents[i] = ent[:j]
			changed = true
		}
	}
	if !changed {
		return msg
	}
	out := msg
	out.Params = append([]string(nil), msg.Params...)
	out.Params[len(out.Params)-1] = strings.Join(ents, " ")
	out.Raw = ""
	return out
}
