package session

import (
	"github.com/MasterBOFH/GoBNC/internal/irc"
)

// handleBNC runs the in-band management command and replies with NOTICE from gobnc.
func (s *Session) handleBNC(d Downlink, args []string) error {
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
