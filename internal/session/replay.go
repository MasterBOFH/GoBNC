package session

import (
	"strconv"
	"strings"

	"github.com/MasterBOFH/GoBNC/internal/caps"
	"github.com/MasterBOFH/GoBNC/internal/irc"
)

// ServerName is the fallback source prefix on bouncer-generated numerics and notices
// when the uplink's 001 server name is not yet known.
const ServerName = "gobnc"

// serverPrefixLocked returns the source for attach-burst server messages.
// Prefers the uplink 001 prefix when known. Caller must hold s.mu.
func (s *Session) serverPrefixLocked() string {
	if s.uplinkServer != "" {
		return s.uplinkServer
	}
	return ServerName
}

// Attach registers a downlink and replays welcome + channel state.
//
// If the uplink is already registered, sends the full cached burst using the
// live uplink nick (modern IRC: 001's first parameter is the assigned nick).
//
// If the uplink is not registered yet, send a synthetic 001 with the configured
// nick so the client can finish local registration, then relay live/buffered
// uplink traffic. When a later real 001 has a different nick, OnRegistrationLine
// sends self-NICK then the real 001. Mid-attach with a buffered 001 already
// present replays the buffer, then self-NICK if the nick differs.
func (s *Session) Attach(d Downlink) error {
	s.mu.Lock()
	s.downlinks[d.ID()] = d
	registered := s.registered
	if !registered {
		cfgNick := s.Network.Nick
		if s.self != nil && s.self.Nick != "" {
			cfgNick = s.self.Nick
		}
		if cfgNick == "" {
			cfgNick = "*"
		}
		buf := append([]irc.Message(nil), s.regBuffer...)
		s.awaitingUplink[d.ID()] = true
		s.mu.Unlock()

		has001 := false
		actualNick := ""
		for _, m := range buf {
			if m.Command == "001" && len(m.Params) > 0 {
				has001 = true
				actualNick = m.Params[0]
				break
			}
		}
		if !has001 {
			_ = d.Send(s.rewriteFor(d, irc.Message{
				Source:  ServerName,
				Command: "001",
				Params:  []string{cfgNick, "Welcome pending uplink registration"},
			}))
		}
		for _, m := range buf {
			_ = d.Send(s.rewriteFor(d, m))
		}
		// If client/config nick ≠ IRC nick, notify with self-NICK.
		if has001 && actualNick != "" && !irc.CaseRFC1459.Equal(cfgNick, actualNick) {
			_ = d.Send(s.rewriteFor(d, irc.Message{
				Source:  cfgNick + "!" + s.networkIdent() + "@" + ServerName,
				Command: "NICK",
				Params:  []string{actualNick},
			}))
		}
		return nil
	}

	nick := s.liveNickLocked()
	src := s.serverPrefixLocked()
	rpl002 := append([]string(nil), s.rpl002...)
	rpl003 := append([]string(nil), s.rpl003...)
	rpl004 := append([]string(nil), s.rpl004...)
	umode := s.self.UModeString()
	prefix := s.self.Prefix()
	isupportRaw := s.isupport.CloneRaw(nil)
	if s.hist != nil {
		isupportRaw["CHATHISTORY"] = strconv.Itoa(s.hist.MaxLimit())
		isupportRaw["MSGREFTYPES"] = "msgid,timestamp"
	}
	chans := make([]*ChannelState, 0, len(s.channels))
	for _, ch := range s.channels {
		chans = append(chans, ch)
	}
	namesFor := make(map[string][]string, len(chans))
	for _, ch := range chans {
		namesFor[ch.Name] = s.namesListLocked(ch)
	}
	loggedIn, haveLogin := s.rplLoggedInLocked()
	s.mu.Unlock()

	send := func(msg irc.Message) {
		_ = d.Send(s.rewriteFor(d, msg))
	}

	send(irc.Message{Source: src, Command: "001", Params: []string{nick, "Welcome to GoBNC"}})
	if len(rpl002) > 0 {
		send(irc.Message{Source: src, Command: "002", Params: append([]string{nick}, rpl002...)})
	}
	if len(rpl003) > 0 {
		send(irc.Message{Source: src, Command: "003", Params: append([]string{nick}, rpl003...)})
	}
	if len(rpl004) > 0 {
		send(irc.Message{Source: src, Command: "004", Params: append([]string{nick}, rpl004...)})
	}
	for _, m := range irc.RPL005(nick, isupportRaw, 500) {
		m.Source = src
		send(m)
	}
	if umode != "" {
		// RPL_UMODEIS modes are a middle param (no leading ':'); set Raw so Wire
		// does not Encode them as trailing.
		send(irc.Message{
			Source:  src,
			Command: "221",
			Params:  []string{nick, umode},
			Raw:     ":" + src + " 221 " + nick + " " + umode,
		})
	}
	// Clients often wait for end-of-MOTD to finish registration; we don't burst the
	// uplink MOTD, but tell them how to fetch it and close with 376.
	send(irc.Message{Source: src, Command: "375", Params: []string{nick, "- GoBNC Message of the Day -"}})
	send(irc.Message{Source: src, Command: "372", Params: []string{nick, "- MOTD can be requested by typing /MOTD"}})
	send(irc.Message{Source: src, Command: "376", Params: []string{nick, "End of /MOTD command."}})

	if haveLogin {
		send(loggedIn)
	}

	for _, ch := range chans {
		send(irc.Message{Source: prefix, Command: "JOIN", Params: []string{ch.Name}})
		if d.HasCap("draft/read-marker") {
			s.sendMarkReadAfterJoin(d, ch.Name)
		}
		if ch.Topic != "" {
			send(irc.Message{Source: src, Command: "332", Params: []string{nick, ch.Name, ch.Topic}})
		}
		if names := namesFor[ch.Name]; len(names) > 0 {
			send(irc.Message{Source: src, Command: "353", Params: []string{nick, "=", ch.Name, strings.Join(names, " ")}})
		}
		send(irc.Message{Source: src, Command: "366", Params: []string{nick, ch.Name, "End of /NAMES list."}})
		if !hasChathistory(d) {
			s.playLegacyHistory(d, ch.Name)
		}
	}
	s.notifyAttachCaps(d)
	return nil
}

func (s *Session) networkIdent() string {
	if s.Network.Username != "" {
		return s.Network.Username
	}
	return "gobnc"
}

// liveNickLocked returns the nick to advertise to clients. Prefers the uplink's
// live nick when registered so attach 001 matches reality after nick collisions.
// Caller must hold s.mu.
func (s *Session) liveNickLocked() string {
	if s.uplink != nil {
		if un := s.uplink.Nick(); un != "" {
			if s.self == nil || s.self.Nick != un {
				s.ensureSelfLocked(un)
			}
			return un
		}
	}
	if s.self != nil && s.self.Nick != "" {
		return s.self.Nick
	}
	return s.Network.Nick
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
