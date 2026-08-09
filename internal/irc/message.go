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
	// Raw is the original wire line (no CRLF) when parsed from the network.
	// When set and the message body is unchanged, Wire preserves it exactly
	// (only the tag prefix may be rewritten for per-client caps).
	Raw string
}

// Parse parses a single IRC line (without trailing CRLF).
func Parse(line string) (Message, error) {
	var msg Message
	msg.Raw = line
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

// Encode serializes a message we originate (attach burst, keepalive, errors).
// The last parameter is always trailing-encoded (":…") so callers do not need
// command-specific colon heuristics. Do not use Encode to forward network lines —
// keep Raw and use Wire so uplink/client bodies stay verbatim.
func (m Message) Encode() string {
	var b bytes.Buffer
	if len(m.Tags) > 0 {
		b.WriteString(formatTagPrefix(m.Tags))
		b.WriteByte(' ')
	}
	if m.Source != "" {
		b.WriteByte(':')
		b.WriteString(m.Source)
		b.WriteByte(' ')
	}
	b.WriteString(m.Command)
	n := len(m.Params)
	for i, p := range m.Params {
		b.WriteByte(' ')
		if i == n-1 {
			b.WriteByte(':')
		}
		b.WriteString(p)
	}
	return b.String()
}

// Wire returns the line to send on the wire.
//
// If Raw is set (parsed from the network and the body was not rewritten), the
// original prefix/command/params are preserved and only the tag prefix is
// replaced for capability compatibility (server-time, message-tags, …).
// Cap-driven body edits must clear or replace Raw (see session rewriteFor).
func (m Message) Wire() string {
	if m.Raw == "" {
		return m.Encode()
	}
	body := stripTagPrefix(m.Raw)
	if len(m.Tags) == 0 {
		return body
	}
	return formatTagPrefix(m.Tags) + " " + body
}

// InvalidateRaw clears Raw so the next Wire/Encode rebuilds from fields.
func (m *Message) InvalidateRaw() {
	m.Raw = ""
}

func stripTagPrefix(line string) string {
	if line == "" || line[0] != '@' {
		return line
	}
	sp := strings.IndexByte(line, ' ')
	if sp < 0 {
		return line
	}
	return strings.TrimLeft(line[sp+1:], " ")
}

func formatTagPrefix(tags map[string]string) string {
	var b bytes.Buffer
	b.WriteByte('@')
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sortStrings(keys)
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(';')
		}
		b.WriteString(k)
		v := tags[k]
		if v != "" {
			b.WriteByte('=')
			b.WriteString(EscapeTagValue(v))
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

// ParamsText returns the parameter payload after the command as a single string.
//
// For opaque single-argument commands (PASS, AUTHENTICATE, …) clients may send
// either "CMD :payload" or "CMD payload". Without a leading colon, spaces in the
// payload become multiple IRC params; ParamsText reconstructs the original text
// from Raw when available, otherwise joins Params with spaces.
func (m Message) ParamsText() string {
	if m.Raw != "" {
		if text, ok := paramsTextFromRaw(m.Raw, m.Command); ok {
			return text
		}
	}
	if len(m.Params) == 0 {
		return ""
	}
	return strings.Join(m.Params, " ")
}

// paramsTextFromRaw extracts the text after command in a wire line.
func paramsTextFromRaw(line, command string) (string, bool) {
	s := stripTagPrefix(line)
	if len(s) > 0 && s[0] == ':' {
		sp := strings.IndexByte(s, ' ')
		if sp < 0 {
			return "", false
		}
		s = strings.TrimLeft(s[sp+1:], " ")
	}
	if command == "" {
		return "", false
	}
	if len(s) < len(command) || !strings.EqualFold(s[:len(command)], command) {
		return "", false
	}
	rest := s[len(command):]
	if rest == "" {
		return "", true
	}
	if rest[0] != ' ' && rest[0] != '\t' {
		return "", false
	}
	rest = strings.TrimLeft(rest, " \t")
	if rest == "" {
		return "", true
	}
	if rest[0] == ':' {
		return rest[1:], true
	}
	return rest, true
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
