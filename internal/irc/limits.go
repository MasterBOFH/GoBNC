package irc

import (
	"errors"
	"strings"
)

// IRCv3 message-tags size limits (https://ircv3.net/specs/extensions/message-tags.html)
// plus RFC 1459/2812 message length of 512 bytes including CRLF.
const (
	// MaxClientTagSection is '@' + 4094 tag data + trailing space.
	MaxClientTagSection = 4096
	// MaxCombinedTagSection is '@' + 4094 + ';' + 4094 + space.
	MaxCombinedTagSection = 8191
	// MaxMessage is the classic IRC message size including CRLF.
	MaxMessage = 512

	// MaxClientLine is the max downlink wire line (client tags + message), including CRLF.
	MaxClientLine = MaxClientTagSection + MaxMessage // 4608
	// MaxServerLine is the max uplink wire line (combined tags + message), including CRLF.
	MaxServerLine = MaxCombinedTagSection + MaxMessage // 8703

	// ErrInputTooLong is the ERR_INPUTTOOLONG numeric.
	ErrInputTooLong = "417"
)

// ErrLineTooLong is returned when a peer sends a line exceeding the configured max.
var ErrLineTooLong = errors.New("irc: line too long")

// InputTooLong builds ERR_INPUTTOOLONG (417) for a downlink client.
func InputTooLong(nick string) Message {
	if nick == "" {
		nick = "*"
	}
	return Message{
		Source:  "gobnc",
		Command: ErrInputTooLong,
		Params:  []string{nick, "Input line was too long"},
	}
}

// HasRawCRLF reports whether the message contains raw CR or LF in source, params, or tags.
func HasRawCRLF(msg Message) bool {
	if strings.ContainsAny(msg.Source, "\r\n") || strings.ContainsAny(msg.Command, "\r\n") {
		return true
	}
	for _, p := range msg.Params {
		if strings.ContainsAny(p, "\r\n") {
			return true
		}
	}
	for k, v := range msg.Tags {
		if strings.ContainsAny(k, "\r\n") || strings.ContainsAny(v, "\r\n") {
			return true
		}
	}
	return false
}
