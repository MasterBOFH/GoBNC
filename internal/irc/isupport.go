package irc

import "sort"

// ISUPPORT holds parsed 005 tokens.
type ISUPPORT struct {
	Raw         map[string]string
	CaseMapping CaseMapping
	Modes       *ModeSet
	WHOX        bool
	Prefix      string
	ChanModes   string
}

// NewISUPPORT returns defaults.
func NewISUPPORT() *ISUPPORT {
	return &ISUPPORT{
		Raw:         make(map[string]string),
		CaseMapping: CaseRFC1459,
		Modes:       DefaultModeSet(),
	}
}

// Parse005 merges tokens from an RPL_ISUPPORT (005) message params
// (skip nick at params[0]; trailing "are supported..." ignored if present).
func (is *ISUPPORT) Parse005(params []string) {
	if is.Raw == nil {
		is.Raw = make(map[string]string)
	}
	if is.Modes == nil {
		is.Modes = DefaultModeSet()
	}
	for i, p := range params {
		if i == 0 {
			continue // nick
		}
		if i == len(params)-1 && (p == "are supported by this server" || p == "is supported by this server") {
			continue
		}
		key, val, _ := splitKV(p)
		is.Raw[key] = val
		switch key {
		case "CASEMAPPING":
			is.CaseMapping = ParseCaseMapping(val)
		case "CHANMODES":
			is.ChanModes = val
			_ = is.Modes.ParseCHANMODES(val)
		case "PREFIX":
			is.Prefix = val
			_ = is.Modes.ParsePREFIX(val)
		case "WHOX":
			is.WHOX = true
		}
	}
}

func splitKV(p string) (key, val string, ok bool) {
	eq := indexByte(p, '=')
	if eq < 0 {
		return p, "", false
	}
	return p[:eq], p[eq+1:], true
}

func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

const isupportTrailing = "are supported by this server"

// Tokens returns sorted KEY or KEY=VALUE tokens from Raw.
func (is *ISUPPORT) Tokens() []string {
	if is == nil || len(is.Raw) == 0 {
		return nil
	}
	keys := make([]string, 0, len(is.Raw))
	for k := range is.Raw {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		v := is.Raw[k]
		if v == "" {
			out = append(out, k)
		} else {
			out = append(out, k+"="+v)
		}
	}
	return out
}

// CloneRaw returns a copy of Raw, optionally applying overrides (empty value deletes).
func (is *ISUPPORT) CloneRaw(overrides map[string]string) map[string]string {
	out := make(map[string]string)
	if is != nil {
		for k, v := range is.Raw {
			out[k] = v
		}
	}
	for k, v := range overrides {
		if v == "" {
			delete(out, k)
		} else {
			out[k] = v
		}
	}
	return out
}

// RPL005 builds one or more RPL_ISUPPORT messages for nick from the given token map.
// Lines stay under maxLen bytes when encoded (IRC 512 including CRLF → use ~500).
func RPL005(nick string, raw map[string]string, maxLen int) []Message {
	if maxLen <= 0 {
		maxLen = 500
	}
	tmp := &ISUPPORT{Raw: raw}
	tokens := tmp.Tokens()
	if len(tokens) == 0 {
		return []Message{{Command: "005", Params: []string{nick, isupportTrailing}}}
	}
	prefixLen := len("005 ") + len(nick) + 1 + len(isupportTrailing) + 2 // rough + spaces/colon
	var msgs []Message
	var batch []string
	batchLen := prefixLen
	flush := func() {
		if len(batch) == 0 {
			return
		}
		params := make([]string, 0, len(batch)+2)
		params = append(params, nick)
		params = append(params, batch...)
		params = append(params, isupportTrailing)
		msgs = append(msgs, Message{Command: "005", Params: params})
		batch = nil
		batchLen = prefixLen
	}
	for _, tok := range tokens {
		need := len(tok) + 1
		if len(batch) > 0 && batchLen+need > maxLen {
			flush()
		}
		batch = append(batch, tok)
		batchLen += need
	}
	flush()
	return msgs
}
