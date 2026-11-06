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

// WHOPoll is what PollWHO is allowed: Interval is how long a channel's metadata may go without a
// WHO reply naming it (0 disables GoBNC's own polls); SeedPace is the least time between two
// polls while a joined channel has not been named by any WHO reply on this connection (the
// seed, after a brain start, a module's first subscription or a reconnect); Pace is the least
// time between two polls of channels that are merely stale. A zero pace does not limit.
type WHOPoll struct {
	Interval, SeedPace, Pace time.Duration
}

// PollWHO sends at most one silent WHOX poll. A channel no WHO reply has named on this
// connection comes first (by key), at SeedPace; otherwise the channel a reply named longest ago,
// provided that was at least Interval before now, at Pace. A channel any client WHO'd within the
// interval is skipped. It polls only while a module is subscribed (nothing else reads the
// metadata), the network is registered and has WHOX, and Interval > 0. It returns the polled
// channel's name, or "".
func (s *Session) PollWHO(now time.Time, p WHOPoll) string {
	if p.Interval <= 0 {
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
	pick, seed := "", false
	var oldest time.Time
	for _, key := range keys {
		last, named := s.lastWHO[key]
		if !named {
			pick, seed = key, true
			break
		}
		if now.Sub(last) < p.Interval {
			continue
		}
		if pick == "" || last.Before(oldest) {
			pick, oldest = key, last
		}
	}
	pace := p.Pace
	if seed {
		pace = p.SeedPace
	}
	// A tenth of the pace is slack for the ticker's jitter: a tick a hair early is not a skip.
	if pick == "" || (!s.lastPollWHO.IsZero() && now.Sub(s.lastPollWHO) < pace-pace/10) {
		s.mu.Unlock()
		return ""
	}
	name := s.channels[pick].Name
	s.noteWHOAtLocked(pick, now)
	s.lastPollWHO = now
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
	key := s.isupport.CaseMapping.Canonical(channel)
	s.noteWHOAtLocked(key, time.Now())
	s.noteWHONamedLocked(key)
}

// noteWHONamedLocked remembers that a WHO reply named channel (folded), so the WHO's end sends
// its members, with what the replies taught, to modules. Caller holds s.mu.
func (s *Session) noteWHONamedLocked(key string) {
	for _, k := range s.whoNamed {
		if k == key {
			return
		}
	}
	s.whoNamed = append(s.whoNamed, key)
}

// state315Locked ends a WHO (RPL_ENDOFWHO): every joined channel a reply named, or the WHO's own
// channel mask, has its members sent to modules (emitRosterDoneLocked), so a lite client already
// subscribed sees the flags and hosts the replies taught. Caller holds s.mu.
func (s *Session) state315Locked(msg irc.Message) {
	if mask := msg.Param(1); isChannelName(mask) {
		s.noteWHONamedLocked(s.isupport.CaseMapping.Canonical(mask))
	}
	named := s.whoNamed
	s.whoNamed = nil
	for _, key := range named {
		ch := s.channels[key]
		if ch == nil || !ch.RosterKnown {
			continue // its 366 sends the roster
		}
		dup := false
		for _, k := range s.rosterDone {
			dup = dup || k == key
		}
		if !dup {
			s.rosterDone = append(s.rosterDone, key)
		}
	}
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
