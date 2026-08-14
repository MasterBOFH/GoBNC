package registration

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/MasterBOFH/GoBNC/internal/irc"
)

const (
	defaultNickLen     = 30
	maxUnderscoreTries = 20
)

// stepNickError handles 432 (erroneous nick), 433 (nick in use), and 437
// (nick/channel temporarily unavailable). Mirrors the register()-phase
// branches in internal/uplink/uplink.go — post-welcome nick errors aren't
// registration failures (session-level code owns those), and 437's
// pre-welcome form is only a nick collision when its first param is "*"
// (otherwise it's a channel-target 437, unrelated to registration).
func stepNickError(s State, in Input) (State, []Action) {
	msg := in.Msg
	if s.GotWelcome {
		return s, nil
	}
	if msg.Command == "437" && msg.Param(0) != "*" {
		return s, nil
	}

	bad := msg.Param(1)
	next, ok := nextLadderNick(s, bad)
	if !ok {
		s.Phase = PhaseFailed
		s.Err = fmt.Errorf("nick error: %s %v", msg.Command, msg.Params)
		return s, []Action{{Kind: ActionFailed, Err: s.Err, Replay: in.Replay}}
	}
	s.Nick = next
	return s, []Action{{Kind: ActionSend, Line: "NICK " + next, Replay: in.Replay}}
}

// nextLadderNick mirrors uplink.tryNextRegisterNick's decision (minus the
// actual write, which the caller turns into an ActionSend): ok is false if
// nick recovery is off or the ladder is exhausted.
func nextLadderNick(s State, bad string) (next string, ok bool) {
	if !s.nickRecovery {
		return "", false
	}
	maxLen := defaultNickLen
	if v := s.ISUPPORT.Raw["NICKLEN"]; v != "" {
		if nl, err := strconv.Atoi(v); err == nil && nl > 0 {
			maxLen = nl
		}
	}
	ladder := buildNickLadder(s.PrimaryNick, s.AltNick, maxLen)
	next = nextNickInLadder(ladder, s.Nick, bad, s.ISUPPORT.CaseMapping)
	return next, next != ""
}

// buildNickLadder returns primary, optional alt, then nick_, nick__, …
// truncated to maxLen. Ported verbatim from internal/uplink/nick.go — pure
// already, no I/O, nothing to change.
func buildNickLadder(primary, alt string, maxLen int) []string {
	if maxLen <= 0 {
		maxLen = defaultNickLen
	}
	seen := make(map[string]bool)
	var out []string
	add := func(nick string) {
		if nick == "" {
			return
		}
		if len(nick) > maxLen {
			nick = nick[:maxLen]
		}
		key := strings.ToLower(nick)
		if seen[key] {
			return
		}
		seen[key] = true
		out = append(out, nick)
	}
	add(primary)
	add(alt)
	for i := 1; i <= maxUnderscoreTries; i++ {
		if i >= maxLen {
			break
		}
		base := primary
		room := maxLen - i
		if room < 1 {
			break
		}
		if len(base) > room {
			base = base[:room]
		}
		add(base + strings.Repeat("_", i))
	}
	return out
}

// nextNickInLadder is ported verbatim from internal/uplink/nick.go.
func nextNickInLadder(ladder []string, last, bad string, cm irc.CaseMapping) string {
	idx := -1
	for i, n := range ladder {
		if cm.Equal(n, last) || cm.Equal(n, bad) {
			idx = i
		}
	}
	if idx < 0 {
		if len(ladder) > 1 {
			return ladder[1]
		}
		return ""
	}
	if idx+1 < len(ladder) {
		return ladder[idx+1]
	}
	return ""
}
