package session

import (
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/irc"
)

func TestIsHighlight(t *testing.T) {
	for _, tc := range []struct {
		text, nick string
		cm         irc.CaseMapping
		want       bool
	}{
		{"hey men, look", "men", irc.CaseRFC1459, true},
		{"a mention", "men", irc.CaseRFC1459, false},
		{"women", "men", irc.CaseRFC1459, false},
		{"men2 is someone else", "men", irc.CaseRFC1459, false},
		{"MEN: ping", "men", irc.CaseRFC1459, true},
		{"[nick]", "{NICK}", irc.CaseRFC1459, true},
		{"[nick]", "{NICK}", irc.CaseASCII, false},
		{"x_men", "men", irc.CaseRFC1459, false},
		{"", "men", irc.CaseRFC1459, false},
	} {
		if got := IsHighlight(tc.text, tc.nick, tc.cm); got != tc.want {
			t.Errorf("IsHighlight(%q, %q) = %v, want %v", tc.text, tc.nick, got, tc.want)
		}
	}
}
