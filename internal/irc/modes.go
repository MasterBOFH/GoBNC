package irc

import (
	"strings"
)

// ModeKind classifies channel modes per CHANMODES / PREFIX.
type ModeKind int

const (
	ModeUnknown ModeKind = iota
	ModeList             // A: list modes, always take arg when set/unset
	ModeArgAlways        // B: always take arg
	ModeArgSet           // C: take arg when setting
	ModeFlag             // D: never take arg
	ModePrefix           // PREFIX modes (o,v,…): always take nick arg
)

// ModeSet describes modes supported via ISUPPORT.
type ModeSet struct {
	// Kind maps mode char -> kind
	Kind map[byte]ModeKind
	// Prefix maps mode char -> prefix symbol (@,+)
	Prefix map[byte]byte
	// PrefixBySymbol maps @,+ -> mode char
	PrefixBySymbol map[byte]byte
	// PrefixOrder high to low privilege mode chars
	PrefixOrder []byte
}

// DefaultModeSet returns a common freenode/libera-like set.
func DefaultModeSet() *ModeSet {
	ms := &ModeSet{
		Kind:           make(map[byte]ModeKind),
		Prefix:         make(map[byte]byte),
		PrefixBySymbol: make(map[byte]byte),
	}
	_ = ms.ParseCHANMODES("eIbq,k,flj,CFLMPQScgimnprstuz")
	_ = ms.ParsePREFIX("(ov)@+")
	return ms
}

// ParseCHANMODES parses CHANMODES=A,B,C,D.
func (ms *ModeSet) ParseCHANMODES(v string) error {
	if ms.Kind == nil {
		ms.Kind = make(map[byte]ModeKind)
	}
	parts := strings.Split(v, ",")
	kinds := []ModeKind{ModeList, ModeArgAlways, ModeArgSet, ModeFlag}
	for i, part := range parts {
		if i >= len(kinds) {
			break
		}
		for j := 0; j < len(part); j++ {
			ms.Kind[part[j]] = kinds[i]
		}
	}
	return nil
}

// ParsePREFIX parses PREFIX=(ov)@+.
func (ms *ModeSet) ParsePREFIX(v string) error {
	if ms.Prefix == nil {
		ms.Prefix = make(map[byte]byte)
	}
	if ms.PrefixBySymbol == nil {
		ms.PrefixBySymbol = make(map[byte]byte)
	}
	ms.PrefixOrder = nil
	l := strings.IndexByte(v, '(')
	r := strings.IndexByte(v, ')')
	if l < 0 || r < 0 || r <= l {
		return nil
	}
	modes := v[l+1 : r]
	syms := v[r+1:]
	n := len(modes)
	if len(syms) < n {
		n = len(syms)
	}
	for i := 0; i < n; i++ {
		ms.Prefix[modes[i]] = syms[i]
		ms.PrefixBySymbol[syms[i]] = modes[i]
		ms.Kind[modes[i]] = ModePrefix
		ms.PrefixOrder = append(ms.PrefixOrder, modes[i])
	}
	return nil
}

// Classify returns the kind of mode char.
func (ms *ModeSet) Classify(mode byte) ModeKind {
	if ms == nil || ms.Kind == nil {
		return ModeUnknown
	}
	k, ok := ms.Kind[mode]
	if !ok {
		return ModeUnknown
	}
	return k
}

// ModeChange is a single +/- mode applied to a channel.
type ModeChange struct {
	Set  bool
	Mode byte
	Arg  string // may be empty
}

// ParseModeParams parses MODE parameters given modestring and args.
// modestring is like "+o-v", args are remaining params.
func (ms *ModeSet) ParseModeParams(modestring string, args []string) []ModeChange {
	var out []ModeChange
	argI := 0
	set := true
	for i := 0; i < len(modestring); i++ {
		c := modestring[i]
		switch c {
		case '+':
			set = true
			continue
		case '-':
			set = false
			continue
		}
		kind := ms.Classify(c)
		needArg := false
		switch kind {
		case ModeList, ModeArgAlways, ModePrefix:
			needArg = true
		case ModeArgSet:
			needArg = set
		case ModeUnknown:
			// Conservative: if args remain, consume one when setting; else none.
			needArg = set && argI < len(args)
		}
		ch := ModeChange{Set: set, Mode: c}
		if needArg && argI < len(args) {
			ch.Arg = args[argI]
			argI++
		}
		out = append(out, ch)
	}
	return out
}

// ChannelModes holds applied channel mode state.
type ChannelModes struct {
	Flags map[byte]bool            // D modes and similar
	Args  map[byte]string          // B/C single-arg modes (e.g. k, l)
	Lists map[byte]map[string]bool // A list modes
	// Nicks maps folded nick -> set of prefix mode chars
	Nicks map[string]map[byte]bool
}

// NewChannelModes creates empty state.
func NewChannelModes() *ChannelModes {
	return &ChannelModes{
		Flags: make(map[byte]bool),
		Args:  make(map[byte]string),
		Lists: make(map[byte]map[string]bool),
		Nicks: make(map[string]map[byte]bool),
	}
}

// Apply applies mode changes using casemap for nick keys.
func (cm *ChannelModes) Apply(ms *ModeSet, cmapping CaseMapping, changes []ModeChange) {
	for _, ch := range changes {
		kind := ModeUnknown
		if ms != nil {
			kind = ms.Classify(ch.Mode)
		}
		switch kind {
		case ModePrefix:
			nick := cmapping.Canonical(ch.Arg)
			if nick == "" {
				continue
			}
			if ch.Set {
				if cm.Nicks[nick] == nil {
					cm.Nicks[nick] = make(map[byte]bool)
				}
				cm.Nicks[nick][ch.Mode] = true
			} else if cm.Nicks[nick] != nil {
				delete(cm.Nicks[nick], ch.Mode)
				if len(cm.Nicks[nick]) == 0 {
					delete(cm.Nicks, nick)
				}
			}
		case ModeList:
			if cm.Lists[ch.Mode] == nil {
				cm.Lists[ch.Mode] = make(map[string]bool)
			}
			key := ch.Arg
			if ch.Set {
				cm.Lists[ch.Mode][key] = true
			} else {
				delete(cm.Lists[ch.Mode], key)
			}
		case ModeArgAlways, ModeArgSet:
			if ch.Set {
				cm.Args[ch.Mode] = ch.Arg
				delete(cm.Flags, ch.Mode)
			} else {
				delete(cm.Args, ch.Mode)
			}
		default: // Flag or Unknown as flag
			if ch.Set {
				cm.Flags[ch.Mode] = true
			} else {
				delete(cm.Flags, ch.Mode)
			}
		}
	}
}

// ModeString returns a modestring and args suitable for RPL_CHANNELMODEIS-ish display.
func (cm *ChannelModes) ModeString() (string, []string) {
	var modes []byte
	var args []string
	var flags []byte
	for m := range cm.Flags {
		flags = append(flags, m)
	}
	sortBytes(flags)
	modes = append(modes, flags...)
	var argModes []byte
	for m := range cm.Args {
		argModes = append(argModes, m)
	}
	sortBytes(argModes)
	for _, m := range argModes {
		modes = append(modes, m)
		args = append(args, cm.Args[m])
	}
	return "+" + string(modes), args
}

func sortBytes(a []byte) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j] < a[j-1]; j-- {
			a[j], a[j-1] = a[j-1], a[j]
		}
	}
}

// ApplyUserModes applies a usermode string like "+iw-x" to a mode set.
func ApplyUserModes(modes map[byte]bool, modestring string) {
	if modes == nil {
		return
	}
	set := true
	for i := 0; i < len(modestring); i++ {
		c := modestring[i]
		switch c {
		case '+':
			set = true
		case '-':
			set = false
		default:
			if set {
				modes[c] = true
			} else {
				delete(modes, c)
			}
		}
	}
}

// UserModeString formats usermodes as "+iw" (empty string if none).
func UserModeString(modes map[byte]bool) string {
	if len(modes) == 0 {
		return ""
	}
	chars := make([]byte, 0, len(modes))
	for m := range modes {
		chars = append(chars, m)
	}
	sortBytes(chars)
	return "+" + string(chars)
}
