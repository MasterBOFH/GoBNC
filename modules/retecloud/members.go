package retecloud

import (
	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// Channel membership as the highlight rules need it (sender_on_common_channels, its opped /
// voiced forms, sender_logged_in): seeded from every snapshot, replaced by members state
// events, and kept in step by the lines in between. Loop-owned.

type memberInfo struct {
	modes   string // mode letters ("ov")
	account string // "" when not logged in or unknown
}

// seedMembers replaces the membership with a snapshot's.
func (n *network) seedMembers(s wire.Snapshot) {
	n.modeSet = irc.DefaultModeSet()
	if v, ok := s.ISupport["CHANMODES"]; ok {
		_ = n.modeSet.ParseCHANMODES(v)
	}
	if v, ok := s.ISupport["PREFIX"]; ok {
		_ = n.modeSet.ParsePREFIX(v)
	}
	n.members = map[string]map[string]*memberInfo{}
	for _, ch := range s.Channels {
		n.setMembers(ch.Key, ch.Members)
	}
}

func (n *network) setMembers(chanKey string, members []wire.Member) {
	modes, symbols := prefixRanks(n.prefix())
	set := make(map[string]*memberInfo, len(members))
	for _, mem := range members {
		set[n.key(mem.Nick)] = &memberInfo{modes: modeLetters(mem.Prefixes, modes, symbols), account: mem.Account}
	}
	n.members[chanKey] = set
}

func (n *network) prefix() string {
	if n.isupport != nil {
		return n.isupport["PREFIX"]
	}
	return ""
}

// trackMembers applies one line's membership change.
func (n *network) trackMembers(line wire.LineEvent) {
	if n.members == nil {
		n.members = map[string]map[string]*memberInfo{}
	}
	msg, err := irc.Parse(line.Raw)
	if err != nil {
		return
	}
	nick := n.key(msg.Nick())
	switch line.Kind {
	case wire.LineJoin:
		ch := n.key(msg.Param(0))
		if line.Self {
			n.members[ch] = map[string]*memberInfo{}
		}
		set := n.members[ch]
		if set == nil {
			return
		}
		mi := &memberInfo{}
		if acc := msg.Param(1); len(msg.Params) >= 3 && acc != "*" { // extended-join
			mi.account = acc
		}
		set[nick] = mi
	case wire.LinePart:
		ch := n.key(msg.Param(0))
		if line.Self {
			delete(n.members, ch)
			return
		}
		delete(n.members[ch], nick)
	case wire.LineKick:
		ch, target := n.key(msg.Param(0)), msg.Param(1)
		if n.isMe(target) {
			delete(n.members, ch)
			return
		}
		delete(n.members[ch], n.key(target))
	case wire.LineQuit:
		for _, set := range n.members {
			delete(set, nick)
		}
	case wire.LineNick:
		to := n.key(msg.Param(0))
		for _, set := range n.members {
			if mi, ok := set[nick]; ok {
				delete(set, nick)
				set[to] = mi
			}
		}
	case wire.LineAccount:
		acc := msg.Param(0)
		if acc == "*" {
			acc = ""
		}
		for _, set := range n.members {
			if mi, ok := set[nick]; ok {
				mi.account = acc
			}
		}
	case wire.LineMode:
		set := n.members[n.key(msg.Param(0))]
		if set == nil || len(msg.Params) < 2 || n.modeSet == nil {
			return
		}
		for _, ch := range n.modeSet.ParseModeParams(msg.Params[1], msg.Params[2:]) {
			if n.modeSet.Classify(ch.Mode) != irc.ModePrefix || ch.Arg == "" {
				continue
			}
			mi := set[n.key(ch.Arg)]
			if mi == nil {
				continue
			}
			mi.modes = withMode(mi.modes, ch.Mode, ch.Set)
		}
	}
}

func withMode(modes string, mode byte, on bool) string {
	out := make([]byte, 0, len(modes)+1)
	for i := 0; i < len(modes); i++ {
		if modes[i] != mode {
			out = append(out, modes[i])
		}
	}
	if on {
		out = append(out, mode)
	}
	return string(out)
}

// senderContext is what the rules know of nick: its account (the line's account tag first) and
// the channels it shares with us, with its mode letters in each.
func (n *network) senderContext(nick, tagAccount string) (account string, channels map[string]string) {
	key := n.key(nick)
	account = tagAccount
	for ch, set := range n.members {
		if mi, ok := set[key]; ok {
			if channels == nil {
				channels = map[string]string{}
			}
			channels[ch] = mi.modes
			if account == "" {
				account = mi.account
			}
		}
	}
	return account, channels
}
