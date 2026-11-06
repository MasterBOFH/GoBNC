// Package push builds, seals and relays Rete push notifications (push v1): the plaintext of
// retecloud-api's documentation/PUSH_ENCRYPTION.md, sealed with HPKE to the phone's X25519 key,
// posted to ReteCloud's relay (PUBLIC_API.md, POST /api/push/relay).
package push

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"
)

// Content types.
const (
	TypeMessage = "message"
	TypeClear   = "clear"
)

// Message kinds.
const (
	KindHighlight = "highlight"
	KindDM        = "dm"
)

// Size budget (PUSH_ENCRYPTION.md, Size budget).
const (
	MaxText      = 1024 // bytes of UTF-8 in text
	MaxPlaintext = 2048 // bytes of padded plaintext
	padBlock     = 256
)

// SourceGoBNC is src for every push this bouncer sends.
const SourceGoBNC = "gobnc"

// Message is a message push's plaintext; field order is the document's.
type Message struct {
	V         int    `json:"v"`
	Type      string `json:"type"`
	EID       string `json:"eid"`
	Src       string `json:"src"`
	Profile   string `json:"profile,omitempty"`
	SessionID string `json:"session_id"`
	Network   string `json:"network"`
	Buffer    string `json:"buffer"`
	Kind      string `json:"kind"`
	Sender    string `json:"sender"`
	Text      string `json:"text"`
	Action    bool   `json:"action,omitempty"`
	Time      int64  `json:"time"`
	ID        int64  `json:"id"`
}

// Clear is a clear push's plaintext.
type Clear struct {
	V         int    `json:"v"`
	Type      string `json:"type"`
	EID       string `json:"eid"`
	Src       string `json:"src"`
	Profile   string `json:"profile,omitempty"`
	SessionID string `json:"session_id"`
	Buffer    string `json:"buffer"`
	ReadID    int64  `json:"read_id"`
	Time      int64  `json:"time"`
}

// ErrTooLarge is returned when a message does not fit the budget even with an empty text.
var ErrTooLarge = errors.New("push: content does not fit 2048 bytes")

// NewEventID mints an event_id: 128 random bits, hex.
func NewEventID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// MessagePlaintext encodes m (V and Type filled in) with its text stripped of IRC formatting
// and capped, and pads it. A text that still does not fit is shortened by the excess.
func MessagePlaintext(m Message) ([]byte, error) {
	m.V, m.Type = 1, TypeMessage
	text := CapText(StripFormatting(strings.ToValidUTF8(m.Text, "�")), MaxText)
	encode := func(limit int) ([]byte, error) {
		m.Text = CapText(text, limit)
		return marshal(m)
	}
	b, err := encode(len(text))
	if err != nil || len(b) <= MaxPlaintext {
		return Pad(b), err
	}
	// Shorten the text by the excess: the longest cut that fits (escaping can make the encoded
	// text longer than its bytes, so the cut is searched rather than computed).
	lo, hi := -1, len(text) // encode(lo) fits (lo = -1: not known yet), encode(hi) does not
	for hi-lo > 1 {
		mid := (lo + hi) / 2
		if mid < 0 {
			mid = 0
		}
		if b, err = encode(mid); err != nil {
			return nil, err
		}
		if len(b) <= MaxPlaintext {
			lo = mid
		} else {
			hi = mid
		}
	}
	if lo < 0 {
		return nil, ErrTooLarge
	}
	if b, err = encode(lo); err != nil {
		return nil, err
	}
	return Pad(b), nil
}

// ClearPlaintext encodes c (V and Type filled in) and pads it.
func ClearPlaintext(c Clear) ([]byte, error) {
	c.V, c.Type = 1, TypeClear
	b, err := marshal(c)
	if err != nil {
		return nil, err
	}
	if len(b) > MaxPlaintext {
		return nil, ErrTooLarge
	}
	return Pad(b), nil
}

// marshal is the plaintext's byte form (PUSH_ENCRYPTION.md, Test vectors): compact, keys in
// the document's order (the structs'), "<", ">" and "&" not escaped, no trailing newline.
func marshal(v any) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(b.Bytes(), []byte("\n")), nil
}

// Pad appends ASCII spaces up to the next multiple of 256 bytes.
func Pad(b []byte) []byte {
	n := (len(b) + padBlock - 1) / padBlock * padBlock
	if n == 0 {
		n = padBlock
	}
	out := make([]byte, n)
	copy(out, b)
	for i := len(b); i < n; i++ {
		out[i] = ' '
	}
	return out
}

// ellipsis ends a cut text.
const ellipsis = "…"

// CapText returns s when it is at most limit bytes; otherwise s cut at a code-point boundary so
// that it ends in "…" and is at most limit bytes ("" when even "…" does not fit).
func CapText(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	if limit < len(ellipsis) {
		return ""
	}
	cut := limit - len(ellipsis)
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + ellipsis
}

// StripFormatting removes IRC formatting codes: bold, colour (with its digits), hex colour
// (with its digits), reset, monospace, reverse, italic, strikethrough and underline.
func StripFormatting(s string) string {
	if strings.IndexFunc(s, isFormat) < 0 {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); {
		switch c := s[i]; c {
		case 0x02, 0x0F, 0x11, 0x16, 0x1D, 0x1E, 0x1F:
			i++
		case 0x03:
			i = skipColour(s, i+1, 2, isDigit)
		case 0x04:
			i = skipColour(s, i+1, 6, isHex)
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String()
}

func isFormat(r rune) bool {
	switch r {
	case 0x02, 0x03, 0x04, 0x0F, 0x11, 0x16, 0x1D, 0x1E, 0x1F:
		return true
	}
	return false
}

// skipColour skips a colour code's "fg[,bg]" (each up to width chars of class ok) from i.
func skipColour(s string, i, width int, ok func(byte) bool) int {
	n := 0
	for n < width && i < len(s) && ok(s[i]) {
		i++
		n++
	}
	if n > 0 && i+1 < len(s) && s[i] == ',' && ok(s[i+1]) {
		i++
		for n = 0; n < width && i < len(s) && ok(s[i]); n++ {
			i++
		}
	}
	return i
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func isHex(c byte) bool { return isDigit(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') }
