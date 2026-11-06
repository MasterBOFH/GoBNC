// Package highlight evaluates a session's highlight rules (the config key highlight_rules of
// retecloud-api's WEBSOCKET_PROTOCOL.md, "Highlight rules") the way Rete.app does
// (IRCStore+Notifications.isHighlightByRules): the reference for every backend and frontend.
package highlight

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/session"
)

// Rule is one highlight rule as it travels on the wire (without a client's local id).
type Rule struct {
	IsEnabled bool      `json:"isEnabled"`
	RuleType  string    `json:"ruleType"`
	MatchType MatchType `json:"matchType"`
}

// MatchType carries the text, nick or channel a rule names.
type MatchType struct {
	Value string `json:"value"`
}

// Limits of set_highlight_rules.
const (
	MaxRules = 200
	MaxBytes = 64 << 10
)

// ErrInvalidRule is a rule that is not an object with a string ruleType.
var ErrInvalidRule = errors.New("Invalid rule")

// ParseRules reads a rules array (raw JSON, not an encoded string). A rule whose ruleType is
// unknown is kept: it never matches.
func ParseRules(raw []byte) ([]Rule, error) {
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, ErrInvalidRule
	}
	rules := make([]Rule, 0, len(items))
	for _, it := range items {
		var r Rule
		if err := json.Unmarshal(it, &r); err != nil || strings.TrimSpace(r.RuleType) == "" {
			return nil, ErrInvalidRule
		}
		rules = append(rules, r)
	}
	return rules, nil
}

// Positive reports whether a rule type is positive (a match makes a highlight); every other
// type, unknown ones included, is negative.
func Positive(ruleType string) bool {
	switch ruleType {
	case "contains_my_nick", "contains_text", "sender_is", "sender_logged_in", "sender_on_common_channels",
		"sender_opped_on_common_channels", "sender_voiced_on_common_channels", "message_on_channel":
		return true
	}
	return false
}

// Line is one PRIVMSG / NOTICE as the evaluation sees it.
type Line struct {
	Off     bool   // the buffer's highlight switch is off
	DM      bool   // the buffer is a query
	Sender  string // the sender's nick
	Text    string // the message text (a CTCP ACTION's text without its wrapping)
	Buffer  string // the channel, or the peer for a DM
	Account string // the sender's account; "" when not logged in or unknown
	// Channels maps each channel the sender shares with us (folded) to the sender's mode
	// letters there ("ov").
	Channels map[string]string
}

// Eval is the evaluation order of WEBSOCKET_PROTOCOL.md (the first step that decides wins):
// switch off, our own line, a DM, a negative rule, the positive rules, then the default nick
// mention. rules nil (never written) or without enabled rules uses the default.
func Eval(rules []Rule, l Line, myNick string, cm irc.CaseMapping) bool {
	if l.Off {
		return false
	}
	if myNick != "" && l.Sender != "" && cm.Equal(l.Sender, myNick) {
		return false
	}
	if l.DM {
		return true
	}
	positives := 0
	for _, r := range rules {
		if !r.IsEnabled {
			continue
		}
		if Positive(r.RuleType) {
			positives++
			continue
		}
		if match(r, l, myNick, cm) {
			return false
		}
	}
	if positives > 0 {
		for _, r := range rules {
			if r.IsEnabled && Positive(r.RuleType) && match(r, l, myNick, cm) {
				return true
			}
		}
		return false
	}
	return session.IsHighlight(l.Text, myNick, cm)
}

// match is Rete's evaluateRule for one rule.
func match(r Rule, l Line, myNick string, cm irc.CaseMapping) bool {
	v := r.MatchType.Value
	switch r.RuleType {
	case "contains_my_nick":
		return myNick != "" && strings.Contains(cm.Canonical(l.Text), cm.Canonical(myNick))
	case "contains_text":
		return v != "" && strings.Contains(strings.ToLower(l.Text), strings.ToLower(v))
	case "sender_is":
		return v != "" && cm.Equal(l.Sender, v)
	case "sender_is_not":
		return v != "" && !cm.Equal(l.Sender, v)
	case "sender_logged_in":
		return l.Sender != "" && l.Account != ""
	case "sender_not_logged_in":
		return l.Sender == "" || l.Account == ""
	case "sender_on_common_channels":
		return l.Sender != "" && len(l.Channels) > 0
	case "sender_not_on_common_channels":
		return l.Sender == "" || len(l.Channels) == 0
	case "sender_opped_on_common_channels":
		return l.Sender != "" && hasMode(l.Channels, 'o')
	case "sender_voiced_on_common_channels":
		return l.Sender != "" && hasMode(l.Channels, 'v')
	case "message_on_channel":
		return v != "" && cm.Equal(l.Buffer, v)
	case "message_not_on_channel":
		return v != "" && !cm.Equal(l.Buffer, v)
	}
	return false
}

func hasMode(channels map[string]string, mode byte) bool {
	for _, modes := range channels {
		if strings.IndexByte(modes, mode) >= 0 {
			return true
		}
	}
	return false
}
