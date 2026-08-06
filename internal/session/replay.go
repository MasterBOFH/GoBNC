package session

import (
	"strings"

	"github.com/MasterBOFH/GoBNC/internal/caps"
	"github.com/MasterBOFH/GoBNC/internal/irc"
)

// ServerName is the source prefix on bouncer-generated numerics and notices.
const ServerName = "gobnc"

// Attach registers a downlink and replays welcome + channel state.
func (s *Session) Attach(d Downlink) error {
	s.mu.Lock()
	s.downlinks[d.ID()] = d
	nick := s.self.Nick
	rpl002 := append([]string(nil), s.rpl002...)
	rpl003 := append([]string(nil), s.rpl003...)
	rpl004 := append([]string(nil), s.rpl004...)
	umode := s.self.UModeString()
	prefix := s.self.Prefix()
	isupportRaw := s.isupport.CloneRaw(nil)
	chans := make([]*ChannelState, 0, len(s.channels))
	for _, ch := range s.channels {
		chans = append(chans, ch)
	}
	namesFor := make(map[string][]string, len(chans))
	for _, ch := range chans {
		namesFor[ch.Name] = s.namesListLocked(ch)
	}
	s.mu.Unlock()

	send := func(msg irc.Message) {
		_ = d.Send(s.rewriteFor(d, msg))
	}

	send(irc.Message{Source: ServerName, Command: "001", Params: []string{nick, "Welcome to GoBNC"}})
	if len(rpl002) > 0 {
		send(irc.Message{Source: ServerName, Command: "002", Params: append([]string{nick}, rpl002...)})
	}
	if len(rpl003) > 0 {
		send(irc.Message{Source: ServerName, Command: "003", Params: append([]string{nick}, rpl003...)})
	}
	if len(rpl004) > 0 {
		send(irc.Message{Source: ServerName, Command: "004", Params: append([]string{nick}, rpl004...)})
	}
	for _, m := range irc.RPL005(nick, isupportRaw, 500) {
		m.Source = ServerName
		send(m)
	}
	if umode != "" {
		send(irc.Message{Source: ServerName, Command: "221", Params: []string{nick, umode}})
	}
	// Clients often wait for end-of-MOTD to finish registration; we don't burst the
	// uplink MOTD, but tell them how to fetch it and close with 376.
	send(irc.Message{Source: ServerName, Command: "375", Params: []string{nick, "- GoBNC Message of the Day -"}})
	send(irc.Message{Source: ServerName, Command: "372", Params: []string{nick, "- MOTD can be requested by typing /MOTD"}})
	send(irc.Message{Source: ServerName, Command: "376", Params: []string{nick, "End of /MOTD command."}})

	for _, ch := range chans {
		send(irc.Message{Source: prefix, Command: "JOIN", Params: []string{ch.Name}})
		if ch.Topic != "" {
			send(irc.Message{Source: ServerName, Command: "332", Params: []string{nick, ch.Name, ch.Topic}})
		}
		if names := namesFor[ch.Name]; len(names) > 0 {
			send(irc.Message{Source: ServerName, Command: "353", Params: []string{nick, "=", ch.Name, strings.Join(names, " ")}})
		}
		send(irc.Message{Source: ServerName, Command: "366", Params: []string{nick, ch.Name, "End of /NAMES list."}})
	}
	s.notifyAttachCaps(d)
	return nil
}

// notifyAttachCaps tells cap-notify clients about uplink-backed caps available now
// (auth-time CAP LS only had AlwaysOffer because the session was not attached yet).
func (s *Session) notifyAttachCaps(d Downlink) {
	if !d.HasCap("cap-notify") {
		return
	}
	var names []string
	for _, c := range s.OfferedCaps() {
		if caps.IsUplinkOffer(c) || caps.CapName(c) == "sasl" {
			names = append(names, c)
		}
	}
	if len(names) == 0 {
		return
	}
	_ = d.Send(irc.Message{
		Source:  ServerName,
		Command: "CAP",
		Params:  []string{"*", "NEW", strings.Join(names, " ")},
	})
}

// namesListLocked builds NAMES entries (@nick …) for channel replay. Caller holds s.mu.
func (s *Session) namesListLocked(ch *ChannelState) []string {
	if ch == nil {
		return nil
	}
	ms := s.isupport.Modes
	out := make([]string, 0, len(ch.Members))
	for folded := range ch.Members {
		nick := folded
		if u := s.users[folded]; u != nil {
			nick = u.Nick
		}
		var pref []byte
		if ch.Modes != nil && ch.Modes.Nicks[folded] != nil && ms != nil {
			for _, m := range ms.PrefixOrder {
				if ch.Modes.Nicks[folded][m] {
					if sym := ms.Prefix[m]; sym != 0 {
						pref = append(pref, sym)
					}
				}
			}
		}
		out = append(out, string(pref)+nick)
	}
	return out
}
