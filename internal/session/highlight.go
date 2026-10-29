package session

import (
	"strings"

	"github.com/MasterBOFH/GoBNC/internal/irc"
)

// isNickWordChar reports whether b can be part of a nick for highlight matching.
func isNickWordChar(b byte) bool {
	switch {
	case b >= 'a' && b <= 'z', b >= 'A' && b <= 'Z', b >= '0' && b <= '9':
		return true
	}
	return strings.IndexByte("_-[]\\`^{}|", b) >= 0
}

// IsHighlight reports whether text mentions nick as a whole word under cm (so "[me]" matches
// "{ME}" under rfc1459, and "mention" does not highlight "men").
func IsHighlight(text, nick string, cm irc.CaseMapping) bool {
	if nick == "" {
		return false
	}
	t, n := cm.Canonical(text), cm.Canonical(nick)
	for i := 0; ; {
		j := strings.Index(t[i:], n)
		if j < 0 {
			return false
		}
		start, end := i+j, i+j+len(n)
		if (start == 0 || !isNickWordChar(t[start-1])) && (end == len(t) || !isNickWordChar(t[end])) {
			return true
		}
		i = start + 1
	}
}
