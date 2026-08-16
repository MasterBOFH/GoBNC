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

// DebugSource is the pseudo-nick /bnc debug relay messages are sent from
// (see sessionDebugTarget.deliver in debug.go) — exported so
// internal/downlink can recognize and skip logging them as ordinary
// outgoing traffic. Without that exclusion, a debug relay message sent to
// a client with raw/all mode active would itself get logged as downlink
// traffic, get picked back up by the same subscription, and relay again —
// an exponential feedback loop, confirmed live (found the hard way: a
// single "/bnc debug all" turned into a multi-gigabyte runaway stream
// within seconds).
const DebugSource = ">debug"

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
// uplink traffic. A later real 001 (possibly with a different assigned nick)
// is the nick source of truth — the client sees two 001s and no self-NICK.
// Mid-attach with a buffered 001 already present replays the buffer as-is
// (no synthetic 001, no NICK).
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
		for _, m := range buf {
			if m.Command == "001" {
				has001 = true
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
	rosterKnownFor := make(map[string]bool, len(chans))
	for _, ch := range chans {
		namesFor[ch.Name] = s.namesListLocked(ch)
		rosterKnownFor[ch.Name] = ch.RosterKnown
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
	// Clients often wait for end-of-MOTD to finish registration; we don't burst the
	// uplink MOTD, but tell them how to fetch it and close with 376.
	send(irc.Message{Source: src, Command: "375", Params: []string{nick, "- GoBNC Message of the Day -"}})
	send(irc.Message{Source: src, Command: "372", Params: []string{nick, "- MOTD can be requested by typing /MOTD"}})
	send(irc.Message{Source: src, Command: "376", Params: []string{nick, "End of /MOTD command."}})

	if haveLogin {
		send(loggedIn)
	}

	// Own usermodes as `:prefix MODE nick +modes`, the same form a live
	// uplink sends during registration. 221 RPL_UMODEIS is a query reply
	// (MODE nick) and clients connecting to the bouncer typically ignore
	// it as part of a burst — they watch this MODE instead. Withheld when
	// umodes are unknown (empty after a resume, before RefreshSelfUModes'
	// 221 lands); the synthesized MODE reaches already-attached clients
	// through broadcastSelfUMode once it does, matching NAMES/RosterKnown.
	if umode != "" {
		send(irc.Message{Source: prefix, Command: "MODE", Params: []string{nick, umode}})
	}

	for _, ch := range chans {
		send(irc.Message{Source: prefix, Command: "JOIN", Params: []string{ch.Name}})
		if d.HasCap("draft/read-marker") {
			s.sendMarkReadAfterJoin(d, ch.Name)
		}
		if ch.Topic != "" {
			send(irc.Message{Source: src, Command: "332", Params: []string{nick, ch.Name, ch.Topic}})
		}
		// Withhold NAMES entirely until the roster is actually confirmed
		// (RosterKnown — see its own doc comment) rather than handing this
		// client a self-only 353/366 that looks real but isn't, e.g. right
		// after a resume, before RefreshResumedChannelNames' live NAMES
		// query has answered. The real 353/366 reaches this client the
		// ordinary way once it arrives — it's already attached and
		// downlinks by then, so no special catch-up delivery is needed.
		if rosterKnownFor[ch.Name] {
			if names := namesFor[ch.Name]; len(names) > 0 {
				send(irc.Message{Source: src, Command: "353", Params: []string{nick, "=", ch.Name, strings.Join(names, " ")}})
			}
			send(irc.Message{Source: src, Command: "366", Params: []string{nick, ch.Name, "End of /NAMES list."}})
		}
		if !hasChathistory(d) {
			s.playLegacyHistory(d, ch.Name)
		}
	}
	s.notifyAttachCaps(d)
	return nil
}

// selfUModeMessageLocked is the `:own-prefix MODE own-nick +modes` line
// Attach bursts and broadcastSelfUMode send. Zero Message when umodes
// aren't known yet (UModeString empty). Caller must hold s.mu.
func (s *Session) selfUModeMessageLocked() irc.Message {
	if s.self == nil {
		return irc.Message{}
	}
	umode := s.self.UModeString()
	if umode == "" {
		return irc.Message{}
	}
	return irc.Message{
		Source:  s.self.Prefix(),
		Command: "MODE",
		Params:  []string{s.self.Nick, umode},
	}
}

// broadcastSelfUMode sends the attach-burst own-MODE to every currently
// attached downlink. Used when an unsolicited 221 arrives (the answer to
// RefreshSelfUModes, or a server that emits RPL_UMODEIS without a client
// query) so a client that attached after a resume, before umodes were
// known, still learns them — the NAMES analogue of forwarding the live
// 353/366 rather than waiting for the next Attach.
func (s *Session) broadcastSelfUMode() {
	s.mu.RLock()
	msg := s.selfUModeMessageLocked()
	downlinks := make([]Downlink, 0, len(s.downlinks))
	for _, d := range s.downlinks {
		downlinks = append(downlinks, d)
	}
	s.mu.RUnlock()
	if msg.Command == "" {
		return
	}
	for _, d := range downlinks {
		_ = d.Send(s.rewriteFor(d, msg))
	}
}

func (s *Session) networkIdent() string {
	if s.Network.Username != "" {
		return s.Network.Username
	}
	return "gobnc"
}

// liveNickLocked returns the nick to advertise to clients — s.self.Nick is
// already the live nick (kept current by HandleRegistered/applyState/
// HandleRegistrationLine directly, unlike the old design where it lagged
// behind uplink.Uplink's own nick field until the next explicit sync).
// Caller must hold s.mu.
func (s *Session) liveNickLocked() string {
	if s.self != nil && s.self.Nick != "" {
		return s.self.Nick
	}
	return s.Network.Nick
}

// notifyAttachCaps tells cap-notify clients about uplink-backed caps available
// now that were not already included in the client's own CAP LS reply (e.g.
// auth-time CAP LS only had AlwaysOffer because the network/uplink was not
// yet known, or wasn't registered yet, when the client answered CAP LS).
func (s *Session) notifyAttachCaps(d Downlink) {
	if !d.HasCap("cap-notify") {
		return
	}
	var names []string
	for _, c := range s.OfferedCaps() {
		name := caps.CapName(c)
		if d.HasSeenCap(name) {
			continue
		}
		if caps.IsUplinkOffer(c) || name == "sasl" {
			names = append(names, c)
		}
	}
	if len(names) == 0 {
		return
	}
	for _, c := range names {
		d.MarkSeenCap(caps.CapName(c))
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
