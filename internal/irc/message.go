// Package irc implements IRC message parsing/encoding, casemapping, and modes.
package irc

import (
	"bytes"
	"fmt"
	"strings"
)

// Message is a parsed IRC protocol message.
type Message struct {
	Tags    map[string]string // IRCv3 message tags (nil if none)
	Source  string            // nick!user@host or server name (no leading :)
	Command string
	Params  []string
}

// Parse parses a single IRC line (without trailing CRLF).
func Parse(line string) (Message, error) {
	var msg Message
	s := line
	if len(s) == 0 {
		return msg, fmt.Errorf("empty message")
	}

	// Tags
	if s[0] == '@' {
		sp := strings.IndexByte(s, ' ')
		if sp < 0 {
			return msg, fmt.Errorf("tags without command")
		}
		tagStr := s[1:sp]
		s = strings.TrimLeft(s[sp+1:], " ")
		tags, err := parseTags(tagStr)
		if err != nil {
			return msg, err
		}
		msg.Tags = tags
	}

	if len(s) == 0 {
		return msg, fmt.Errorf("missing command")
	}

	// Prefix
	if s[0] == ':' {
		sp := strings.IndexByte(s, ' ')
		if sp < 0 {
			return msg, fmt.Errorf("prefix without command")
		}
		msg.Source = s[1:sp]
		s = strings.TrimLeft(s[sp+1:], " ")
	}

	if len(s) == 0 {
		return msg, fmt.Errorf("missing command")
	}

	// Command
	sp := strings.IndexByte(s, ' ')
	if sp < 0 {
		msg.Command = s
		return msg, nil
	}
	msg.Command = s[:sp]
	s = strings.TrimLeft(s[sp+1:], " ")

	// Params
	for len(s) > 0 {
		if s[0] == ':' {
			msg.Params = append(msg.Params, s[1:])
			break
		}
		sp := strings.IndexByte(s, ' ')
		if sp < 0 {
			msg.Params = append(msg.Params, s)
			break
		}
		msg.Params = append(msg.Params, s[:sp])
		s = strings.TrimLeft(s[sp+1:], " ")
	}
	return msg, nil
}

func parseTags(tagStr string) (map[string]string, error) {
	tags := make(map[string]string)
	for _, part := range strings.Split(tagStr, ";") {
		if part == "" {
			continue
		}
		eq := strings.IndexByte(part, '=')
		var key, val string
		if eq < 0 {
			key = part
			val = ""
		} else {
			key = part[:eq]
			var err error
			val, err = UnescapeTagValue(part[eq+1:])
			if err != nil {
				return nil, err
			}
		}
		if key == "" {
			return nil, fmt.Errorf("empty tag key")
		}
		tags[key] = val
	}
	return tags, nil
}

// UnescapeTagValue decodes IRCv3 tag escapes.
func UnescapeTagValue(v string) (string, error) {
	if !strings.ContainsRune(v, '\\') {
		return v, nil
	}
	var b strings.Builder
	b.Grow(len(v))
	for i := 0; i < len(v); i++ {
		if v[i] != '\\' {
			b.WriteByte(v[i])
			continue
		}
		i++
		if i >= len(v) {
			return "", fmt.Errorf("trailing escape")
		}
		switch v[i] {
		case ':':
			b.WriteByte(';')
		case 's':
			b.WriteByte(' ')
		case '\\':
			b.WriteByte('\\')
		case 'r':
			b.WriteByte('\r')
		case 'n':
			b.WriteByte('\n')
		default:
			// Unknown escape: drop backslash, keep char (per common practice)
			b.WriteByte(v[i])
		}
	}
	return b.String(), nil
}

// EscapeTagValue encodes a tag value for the wire.
func EscapeTagValue(v string) string {
	var b strings.Builder
	b.Grow(len(v))
	for i := 0; i < len(v); i++ {
		switch v[i] {
		case ';':
			b.WriteString(`\:`)
		case ' ':
			b.WriteString(`\s`)
		case '\\':
			b.WriteString(`\\`)
		case '\r':
			b.WriteString(`\r`)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteByte(v[i])
		}
	}
	return b.String()
}

// Encode serializes the message to a wire line without CRLF.
func (m Message) Encode() string {
	var b bytes.Buffer
	if len(m.Tags) > 0 {
		b.WriteByte('@')
		first := true
		// Stable-ish: iterate keys sorted for determinism in tests
		keys := make([]string, 0, len(m.Tags))
		for k := range m.Tags {
			keys = append(keys, k)
		}
		sortStrings(keys)
		for _, k := range keys {
			if !first {
				b.WriteByte(';')
			}
			first = false
			b.WriteString(k)
			v := m.Tags[k]
			if v != "" {
				b.WriteByte('=')
				b.WriteString(EscapeTagValue(v))
			}
		}
		b.WriteByte(' ')
	}
	if m.Source != "" {
		b.WriteByte(':')
		b.WriteString(m.Source)
		b.WriteByte(' ')
	}
	b.WriteString(m.Command)
	for i, p := range m.Params {
		b.WriteByte(' ')
		// Always colon-encode the final parameter. Required when it is empty, has
		// spaces, or starts with ':'; also the expected form for PRIVMSG/NOTICE text.
		if i == len(m.Params)-1 {
			b.WriteByte(':')
			b.WriteString(p)
		} else {
			b.WriteString(p)
		}
	}
	return b.String()
}

func sortStrings(a []string) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j] < a[j-1]; j-- {
			a[j], a[j-1] = a[j-1], a[j]
		}
	}
}

// Nick returns the nick portion of Source, or Source if no '!'.
func (m Message) Nick() string {
	if i := strings.IndexByte(m.Source, '!'); i >= 0 {
		return m.Source[:i]
	}
	return m.Source
}

// Tag returns a tag value and whether it was present.
func (m Message) Tag(key string) (string, bool) {
	if m.Tags == nil {
		return "", false
	}
	v, ok := m.Tags[key]
	return v, ok
}

// Param returns params[i] or "".
func (m Message) Param(i int) string {
	if i < 0 || i >= len(m.Params) {
		return ""
	}
	return m.Params[i]
}

// Trailing returns the last param.
func (m Message) Trailing() string {
	if len(m.Params) == 0 {
		return ""
	}
	return m.Params[len(m.Params)-1]
}

// CopyTags returns a shallow copy of tags.
func (m Message) CopyTags() map[string]string {
	if m.Tags == nil {
		return nil
	}
	out := make(map[string]string, len(m.Tags))
	for k, v := range m.Tags {
		out[k] = v
	}
	return out
}
