package retecloud

import (
	"strings"
	"time"
)

// Reply routing for lite connections, ported from retecloud-api websocket/replies.go. Replies the
// brain's RequestTracker routes to the asking WebSocket arrive as client_line; replies to queries
// it does not track arrive as broadcast numerics, which a lite connection only receives while it
// has such a query pending (a LIST on the desktop does not flood every phone).

// replyFamily is the set of numerics one query command produces, and the one ending it.
type replyFamily struct {
	name     string
	numerics map[int]bool
	end      int // 0 = no end numeric; the request expires instead
}

func family(name string, end int, numerics ...int) *replyFamily {
	f := &replyFamily{name: name, numerics: map[int]bool{}, end: end}
	for _, n := range numerics {
		f.numerics[n] = true
	}
	if end != 0 {
		f.numerics[end] = true
	}
	return f
}

var (
	familyWhois   = family("WHOIS", 318, 276, 301, 307, 310, 311, 312, 313, 317, 319, 320, 330, 335, 338, 378, 379, 671)
	familyWhowas  = family("WHOWAS", 369, 314, 312, 330)
	familyWho     = family("WHO", 315, 352, 354)
	familyList    = family("LIST", 323, 321, 322)
	familyNames   = family("NAMES", 366, 353)
	familyBans    = family("BANLIST", 368, 367)
	familyExcepts = family("EXCEPTLIST", 349, 348)
	familyInvex   = family("INVITELIST", 347, 346)
	familyQuiets  = family("QUIETLIST", 729, 728)
	familyLinks   = family("LINKS", 365, 364)
	familyMotd    = family("MOTD", 376, 375, 372, 422)
	familyLusers  = family("LUSERS", 266, 251, 252, 253, 254, 255, 265)
	familyInfo    = family("INFO", 374, 371)
	familyUserhst = family("USERHOST", 0, 302)
	familyIson    = family("ISON", 0, 303)
	familyAdmin   = family("ADMIN", 0, 256, 257, 258, 259)
	familyTime    = family("TIME", 0, 391)
	familyVersion = family("VERSION", 0, 351)
	familyStats   = family("STATS", 219, 211, 212, 213, 215, 216, 218, 240, 241, 242, 243, 244, 249, 250)

	allFamilies = []*replyFamily{familyWhois, familyWhowas, familyWho, familyList, familyNames, familyBans,
		familyExcepts, familyInvex, familyQuiets, familyLinks, familyMotd, familyLusers, familyInfo,
		familyUserhst, familyIson, familyAdmin, familyTime, familyVersion, familyStats}
)

// pendingReplyTTL bounds a request that never sees its end numeric.
var pendingReplyTTL = 30 * time.Second

// familyForCommand maps an outgoing raw command to the reply family it triggers, if any.
func familyForCommand(command string, isChannel func(string) bool) *replyFamily {
	f := strings.Fields(command)
	if len(f) == 0 {
		return nil
	}
	switch strings.ToUpper(f[0]) {
	case "WHOIS":
		return familyWhois
	case "WHOWAS":
		return familyWhowas
	case "WHO":
		return familyWho
	case "LIST":
		return familyList
	case "NAMES":
		return familyNames
	case "LINKS":
		return familyLinks
	case "MOTD":
		return familyMotd
	case "LUSERS":
		return familyLusers
	case "INFO":
		return familyInfo
	case "USERHOST":
		return familyUserhst
	case "ISON":
		return familyIson
	case "ADMIN":
		return familyAdmin
	case "TIME":
		return familyTime
	case "VERSION":
		return familyVersion
	case "STATS":
		return familyStats
	case "MODE":
		// "MODE #chan b" (list query, no mask) → that list.
		if len(f) == 3 && isChannel(f[1]) {
			switch strings.TrimPrefix(f[2], "+") {
			case "b":
				return familyBans
			case "e":
				return familyExcepts
			case "I":
				return familyInvex
			case "q":
				return familyQuiets
			}
		}
	}
	return nil
}

// replyFamilyOf returns the family a numeric belongs to (nil for everything else).
func replyFamilyOf(numeric int) *replyFamily {
	for _, f := range allFamilies {
		if f.numerics[numeric] {
			return f
		}
	}
	return nil
}

type pendingReply struct {
	family  *replyFamily
	expires time.Time
}

// notePending records a query this connection sent on a network.
func (ls *liteSession) notePending(command string, isChannel func(string) bool) {
	if f := familyForCommand(command, isChannel); f != nil {
		ls.pending = append(ls.pending, pendingReply{family: f, expires: time.Now().Add(pendingReplyTTL)})
	}
}

// replyAllowed decides a broadcast numeric for a lite connection; handled=false means it is
// not reply traffic and normal routing applies.
func (ls *liteSession) replyAllowed(numeric int, anyPendingElsewhere bool) (allow, handled bool) {
	now := time.Now()
	live := ls.pending[:0]
	for _, p := range ls.pending {
		if now.Before(p.expires) {
			live = append(live, p)
		}
	}
	ls.pending = live
	if numeric >= 400 && numeric < 600 {
		// An error answers whichever request is pending; with none pending anywhere it is about
		// something else (a failed JOIN, nick in use…) and goes to everyone.
		if len(ls.pending) > 0 {
			return true, true
		}
		return !anyPendingElsewhere, true
	}
	f := replyFamilyOf(numeric)
	if f == nil {
		return false, false
	}
	for i, p := range ls.pending {
		if p.family == f {
			if numeric == f.end {
				ls.pending = append(ls.pending[:i], ls.pending[i+1:]...)
			}
			return true, true
		}
	}
	return false, true
}

func (ls *liteSession) hasPending() bool {
	now := time.Now()
	for _, p := range ls.pending {
		if now.Before(p.expires) {
			return true
		}
	}
	return false
}
