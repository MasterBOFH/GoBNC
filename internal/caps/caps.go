package caps

import "strings"

// Package caps lists IRCv3 capabilities GoBNC offers to clients and requests upstream.

// AlwaysOffer is always included in CAP LS and is never removed via CAP DEL
// because of uplink changes (bouncer-local or always synthesizable).
var AlwaysOffer = []string{
	"cap-notify",
	"message-tags",
	"server-time",
	"batch",
	"labeled-response",
	"echo-message",
	"chathistory",
	"draft/chathistory",
	"event-playback",
	"draft/event-playback",
}

// UplinkOffer is advertised to clients only while the uplink has the capability enabled.
var UplinkOffer = []string{
	"away-notify",
	"chghost",
	"invite-notify",
	"account-notify",
	"extended-join",
}

// AllOffer is AlwaysOffer ∪ UplinkOffer (full potential set).
func AllOffer() []string {
	out := make([]string, 0, len(AlwaysOffer)+len(UplinkOffer))
	out = append(out, AlwaysOffer...)
	out = append(out, UplinkOffer...)
	return out
}

// Offered returns caps available to clients given which uplink caps are enabled.
func Offered(uplinkHas map[string]bool) []string {
	out := make([]string, 0, len(AlwaysOffer)+len(UplinkOffer))
	out = append(out, AlwaysOffer...)
	for _, c := range UplinkOffer {
		if uplinkHas != nil && uplinkHas[c] {
			out = append(out, c)
		}
	}
	return out
}

// IsUplinkOffer reports whether name is an uplink-backed advertised capability.
func IsUplinkOffer(name string) bool {
	name = CapName(name)
	for _, c := range UplinkOffer {
		if c == name {
			return true
		}
	}
	return false
}

// CapName strips a CAP 302 value (name=value → name).
func CapName(raw string) string {
	name, _, _ := strings.Cut(raw, "=")
	return name
}

// FormatSASL returns "sasl" or "sasl=PLAIN,EXTERNAL" for CAP LS/NEW.
func FormatSASL(mechs []string) string {
	if len(mechs) == 0 {
		return "sasl"
	}
	return "sasl=" + strings.Join(mechs, ",")
}

// Diff returns names in after but not in before (order preserved from after).
// Comparison is by capability name (CAP 302 values ignored).
func Diff(before, after []string) []string {
	set := make(map[string]bool, len(before))
	for _, c := range before {
		set[CapName(c)] = true
	}
	var out []string
	for _, c := range after {
		if !set[CapName(c)] {
			out = append(out, c)
		}
	}
	return out
}

// WithoutValues returns capability names only (strip CAP 302 values).
func WithoutValues(capsList []string) []string {
	out := make([]string, len(capsList))
	for i, c := range capsList {
		out[i] = CapName(c)
	}
	return out
}
