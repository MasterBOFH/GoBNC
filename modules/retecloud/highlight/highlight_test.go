package highlight

import (
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/irc"
)

func rule(typ, value string) Rule {
	return Rule{IsEnabled: true, RuleType: typ, MatchType: MatchType{Value: value}}
}

func TestEvalOrder(t *testing.T) {
	cm := irc.ParseCaseMapping("rfc1459")
	base := Line{Sender: "alice", Text: "hello me", Buffer: "#c", Channels: map[string]string{"#c": ""}}
	cases := []struct {
		name  string
		rules []Rule
		mod   func(*Line)
		want  bool
	}{
		{"default mention", nil, nil, true},
		{"default no mention", nil, func(l *Line) { l.Text = "hello meh" }, false},
		{"switch off beats everything", nil, func(l *Line) { l.Off = true }, false},
		{"switch off beats a DM", nil, func(l *Line) { l.Off, l.DM = true, true }, false},
		{"own line", nil, func(l *Line) { l.Sender = "ME" }, false},
		{"own line in a DM", nil, func(l *Line) { l.Sender, l.DM = "me", true }, false},
		{"DM always", []Rule{rule("sender_is_not", "alice")}, func(l *Line) { l.DM, l.Text = true, "x" }, true},
		{"not rule holds", []Rule{rule("contains_text", "hello"), rule("message_not_on_channel", "#other")}, nil, true},
		{"not rule fails", []Rule{rule("contains_text", "hello"), rule("message_not_on_channel", "#C")}, nil, false},
		{"owner: nick, not from chief (alice)", []Rule{rule("contains_my_nick", ""), rule("sender_is_not", "chief")}, nil, true},
		{"owner: nick, not from chief (chief)", []Rule{rule("contains_my_nick", ""), rule("sender_is_not", "CHIEF")}, func(l *Line) { l.Sender = "chief" }, false},
		{"owner: nick, not from chief (no mention)", []Rule{rule("contains_my_nick", ""), rule("sender_is_not", "chief")}, func(l *Line) { l.Text = "hello" }, false},
		{"not rule alone filters the default", []Rule{rule("sender_is_not", "alice")}, nil, false},
		{"not rule alone keeps the default", []Rule{rule("sender_is_not", "bob")}, nil, true},
		{"empty not rules hold", []Rule{rule("sender_is_not", ""), rule("message_not_on_channel", "")}, nil, true},
		{"positive replaces the default", []Rule{rule("contains_text", "zzz")}, nil, false},
		{"positive match", []Rule{rule("contains_text", "HELLO")}, func(l *Line) { l.Text = "hello" }, true},
		{"disabled rules are ignored", []Rule{{IsEnabled: false, RuleType: "contains_text", MatchType: MatchType{Value: "zzz"}}}, nil, true},
		{"contains_my_nick casemapped", []Rule{rule("contains_my_nick", "")}, func(l *Line) { l.Text = "x{ME}x" }, true},
		{"unknown type never matches", []Rule{rule("frobnicate", "")}, nil, true},
		{"sender_is", []Rule{rule("sender_is", "ALICE")}, func(l *Line) { l.Text = "x" }, true},
		{"sender_logged_in no", []Rule{rule("sender_logged_in", "")}, nil, false},
		{"sender_logged_in yes", []Rule{rule("sender_logged_in", "")}, func(l *Line) { l.Account = "acc" }, true},
		{"sender_not_logged_in holds", []Rule{rule("sender_not_logged_in", "")}, nil, true},
		{"sender_not_logged_in fails", []Rule{rule("sender_not_logged_in", "")}, func(l *Line) { l.Account = "acc" }, false},
		{"sender_not_on_common_channels holds", []Rule{rule("sender_not_on_common_channels", "")}, func(l *Line) { l.Channels = nil }, true},
		{"sender_not_on_common_channels fails", []Rule{rule("sender_not_on_common_channels", "")}, nil, false},
		{"opped", []Rule{rule("sender_opped_on_common_channels", "")}, func(l *Line) { l.Channels = map[string]string{"#c": "o"} }, true},
		{"not opped", []Rule{rule("sender_opped_on_common_channels", "")}, nil, false},
		{"voiced", []Rule{rule("sender_voiced_on_common_channels", "")}, func(l *Line) { l.Channels = map[string]string{"#x": "v"} }, true},
		{"message_on_channel", []Rule{rule("message_on_channel", "#C")}, func(l *Line) { l.Text = "x" }, true},
	}
	for _, tc := range cases {
		l := base
		if tc.mod != nil {
			tc.mod(&l)
		}
		if got := Eval(tc.rules, l, "me", cm); got != tc.want {
			t.Errorf("%s: got %v", tc.name, got)
		}
	}
}

func TestParseRules(t *testing.T) {
	rules, err := ParseRules([]byte(`[{"isEnabled":true,"ruleType":"contains_text","matchType":{"value":"rete"}},{"isEnabled":false,"ruleType":"new_kind","matchType":{"value":""},"extra":1}]`))
	if err != nil || len(rules) != 2 || rules[0].MatchType.Value != "rete" || rules[1].RuleType != "new_kind" {
		t.Fatalf("%v %+v", err, rules)
	}
	for _, bad := range []string{`{}`, `"[]"`, `[1]`, `[{"isEnabled":true}]`, `[{"ruleType":5}]`} {
		if _, err := ParseRules([]byte(bad)); err != ErrInvalidRule {
			t.Errorf("%s: %v", bad, err)
		}
	}
	if rules, err := ParseRules([]byte(`[]`)); err != nil || len(rules) != 0 {
		t.Fatalf("empty: %v %v", rules, err)
	}
}
