package session

import (
	"strings"

	"github.com/MasterBOFH/GoBNC/internal/irc"
)

// handleBNC runs the in-band management command and replies with NOTICE from gobnc.
func (s *Session) handleBNC(d Downlink, args []string) error {
	// debug is intercepted here, before s.admin: it needs the live Session
	// and the specific requesting Downlink (to know who to relay to and
	// which network's registry to subscribe on), neither of which
	// internal/admin's stateless command set (network/auth/rehash/status)
	// has access to — see handleBNCDebug's own doc comment.
	if len(args) > 0 && strings.EqualFold(args[0], "debug") {
		return s.handleBNCDebug(d, args[1:])
	}

	nick := s.Nick()
	if nick == "" {
		nick = "*"
	}
	sendNotice := func(text string) error {
		return d.Send(irc.Message{
			Source:  ServerName,
			Command: "NOTICE",
			Params:  []string{nick, text},
		})
	}
	if s.admin == nil {
		return sendNotice("BNC unavailable")
	}
	lines, err := s.admin(args)
	for _, line := range lines {
		if line == "" {
			line = " "
		}
		if e := sendNotice(line); e != nil {
			return e
		}
	}
	if err != nil {
		return sendNotice(err.Error())
	}
	return nil
}
