// Package irc — IRCd family detection from registration text.
package irc

import "strings"

// Detected IRCd family identifiers (aligned with retecloud-irc numeric_handlers).
const (
	IRCdUnknown    = ""
	IRCdInspIRCd   = "inspircd"
	IRCdUnreal     = "unrealircd"
	IRCdSolanum    = "solanum"
	IRCdCharybdis  = "charybdis"
	IRCdSeven      = "ircd-seven"
	IRCdRatbox     = "ratbox"
	IRCdHybrid     = "hybrid"
	IRCdBahamut    = "bahamut"
	IRCdNgIRCd     = "ngircd"
	IRCdOragono    = "oragono"
	IRCdErgo       = "ergo"
	IRCdSnircd     = "snircd"
	IRCdIrcu       = "ircu"
	IRCdIrc2       = "ircd-irc2" // classic ircd 2.11.x
)

// DetectIRCd guesses the IRCd family from RPL_YOURHOST (002) trailing text
// (or similar registration banner). More specific matches first; snircd before ircu.
func DetectIRCd(text string) string {
	s := strings.ToLower(text)
	switch {
	case strings.Contains(s, "inspircd"):
		return IRCdInspIRCd
	case strings.Contains(s, "unrealircd"), strings.Contains(s, "unreal-"):
		return IRCdUnreal
	case strings.Contains(s, "solanum"):
		return IRCdSolanum
	case strings.Contains(s, "charybdis"):
		return IRCdCharybdis
	case strings.Contains(s, "ircd-seven"), strings.Contains(s, "ircd seven"), strings.Contains(s, "ircdseven"):
		return IRCdSeven
	case strings.Contains(s, "ircd-ratbox"), strings.Contains(s, "ratbox"):
		return IRCdRatbox
	case strings.Contains(s, "ircd-hybrid"), strings.Contains(s, "hybrid"):
		return IRCdHybrid
	case strings.Contains(s, "bahamut"):
		return IRCdBahamut
	case strings.Contains(s, "ngircd"):
		return IRCdNgIRCd
	case strings.Contains(s, "oragono"):
		return IRCdOragono
	case strings.Contains(s, "ergo"):
		return IRCdErgo
	case strings.Contains(s, "snircd"):
		return IRCdSnircd
	case strings.Contains(s, "u2.10."), strings.Contains(s, "ircu"), strings.Contains(s, "undernet"):
		// u2.10.12.x today; u2.10.13+ next (WHOISSECURE 671).
		return IRCdIrcu
	case strings.Contains(s, "ircd-irc2"), strings.Contains(s, "2.11."):
		// Classic ircd2 banners: "running version 2.11.2p3"
		return IRCdIrc2
	default:
		return IRCdUnknown
	}
}

// DetectIRCdFrom004 uses RPL_MYINFO version token (004 params after nick: server version ...).
func DetectIRCdFrom004(version string) string {
	if version == "" {
		return IRCdUnknown
	}
	return DetectIRCd(version)
}

// MAPEndCodes returns end-of-MAP numerics for this IRCd family.
func MAPEndCodes(ircd string) map[string]bool {
	switch ircd {
	case IRCdIrcu, IRCdSnircd:
		return map[string]bool{"017": true}
	case IRCdUnreal, IRCdInspIRCd:
		return map[string]bool{"007": true}
	case IRCdBahamut:
		// Bahamut historically varies; treat Unreal-style if present.
		return map[string]bool{"007": true}
	default:
		// Unknown: accept common ends but prefer not to steal unrelated traffic —
		// still need *some* terminator for hold-until-end.
		return map[string]bool{"017": true, "007": true, "359": true}
	}
}

// MAPReplyCodes returns MAP body numerics for this IRCd family.
func MAPReplyCodes(ircd string) map[string]bool {
	ends := MAPEndCodes(ircd)
	out := make(map[string]bool, len(ends)+4)
	for k, v := range ends {
		out[k] = v
	}
	switch ircd {
	case IRCdIrcu, IRCdSnircd:
		out["015"] = true
		out["016"] = true
	case IRCdUnreal:
		out["006"] = true
	case IRCdInspIRCd:
		out["006"] = true
		out["018"] = true // RPL_MAPUSERS
	default:
		out["006"] = true
		out["015"] = true
		out["016"] = true
		out["018"] = true
		out["357"] = true
		out["358"] = true
	}
	return out
}

// IsWHOISReply reports whether numeric belongs to a WHOIS exchange on this IRCd.
// RFC-core replies are always accepted; extension numerics are family-scoped so
// conflicting uses (e.g. 307 USERIP vs WHOISREGNICK) are not stolen from other traffic.
func IsWHOISReply(numeric, ircd string) bool {
	switch numeric {
	case "301", // RPL_AWAY (also unsolicited; only routed when WHOIS pending)
		"311", "312", "313", "317", "318", "319",
		"401": // ERR_NOSUCHNICK
		return true
	}

	switch ircd {
	case IRCdIrcu, IRCdSnircd:
		switch numeric {
		case "330", // RPL_WHOISACCOUNT
			"338", // RPL_WHOISACTUALLY
			"671": // RPL_WHOISSECURE (ircu2 u2.10.13+)
			return true
		}
	case IRCdUnreal:
		switch numeric {
		case "276", // RPL_WHOISCERTFP (some)
			"307", // RPL_WHOISREGNICK
			"310", // RPL_WHOISHELPOP
			"320", // RPL_WHOISSPECIAL
			"335", // RPL_WHOISBOT
			"378", // RPL_WHOISHOST
			"379", // RPL_WHOISMODES
			"671": // RPL_WHOISSECURE
			return true
		}
	case IRCdBahamut:
		switch numeric {
		case "307", // RPL_WHOISREGNICK
			"338": // RPL_WHOISACTUALLY
			return true
		}
	case IRCdInspIRCd, IRCdErgo, IRCdOragono:
		switch numeric {
		case "276",
			"330",
			"671",
			"672", "673", "674":
			return true
		}
	case IRCdRatbox, IRCdCharybdis, IRCdSolanum, IRCdSeven, IRCdHybrid:
		switch numeric {
		case "276",
			"330", // account / logged-in
			"335", // WHOISTEXT (hybrid) / similar
			"337", // solanum RPL_WHOISTEXT — idle time hidden (+I); charybdis lineage
			"338", // WHOISACTUALLY
			"671":
			return true
		}
	case IRCdNgIRCd, IRCdIrc2:
		// Mostly RFC-core only.
		return false
	default:
		// Unknown IRCd: accept common extensions (legacy union) so we do not drop replies.
		switch numeric {
		case "276", "307", "310", "320", "325",
			"330", "335", "336", "337", "338",
			"378", "379",
			"671", "672", "673", "674":
			return true
		}
	}
	return false
}
