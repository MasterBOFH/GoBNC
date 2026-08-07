package session

import (
	"context"
	"strings"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/caps"
	"github.com/MasterBOFH/GoBNC/internal/history"
	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/uplink"
)

// OnRegistered implements uplink.Handler.
func (s *Session) OnRegistered(u *uplink.Uplink) {
	s.mu.Lock()
	prevOffer := caps.Offered(s.upCaps)
	if s.saslOffer != "" {
		prevOffer = append(prevOffer, s.saslOffer)
	}
	s.ensureSelfLocked(u.Nick())
	s.self.Nick = u.Nick()
	s.isupport = u.ISUPPORT()
	s.upCaps = u.Caps()
	s.rpl002, s.rpl003, s.rpl004 = u.Welcome()
	s.detectIRCdLocked()
	s.self.UModes = make(map[byte]bool)
	if um := u.UserModes(); um != "" {
		s.self.ApplyUModes(um)
	}
	s.mu.Unlock()
	_, nowSASL := s.refreshSASLOffer(u)
	s.mu.Lock()
	nowOffer := caps.Offered(s.upCaps)
	if nowSASL != "" {
		nowOffer = append(nowOffer, nowSASL)
	}
	s.mu.Unlock()
	s.log.Info("uplink registered", "nick", u.Nick())
	if added := caps.Diff(prevOffer, nowOffer); len(added) > 0 {
		s.broadcastCapNotify("NEW", added)
	}
}

// OnCapsChanged implements uplink.Handler — uplink CAP ACK/DEL after registration.
func (s *Session) OnCapsChanged(u *uplink.Uplink, added, removed []string) {
	s.mu.Lock()
	prevOffer := caps.Offered(s.upCaps)
	if s.saslOffer != "" {
		prevOffer = append(prevOffer, s.saslOffer)
	}
	s.upCaps = u.Caps()
	s.mu.Unlock()
	_, nowSASL := s.refreshSASLOffer(u)
	s.mu.Lock()
	nowOffer := caps.Offered(s.upCaps)
	if nowSASL != "" {
		nowOffer = append(nowOffer, nowSASL)
	}
	s.mu.Unlock()
	if gained := caps.Diff(prevOffer, nowOffer); len(gained) > 0 {
		s.broadcastCapNotify("NEW", gained)
	}
	if lost := caps.Diff(nowOffer, prevOffer); len(lost) > 0 {
		s.broadcastCapNotify("DEL", lost)
	}
	for _, name := range added {
		if name == "sasl" {
			s.finishSASLWaiters(true)
			break
		}
	}
	for _, name := range removed {
		if name == "sasl" {
			s.finishSASLWaiters(false)
			break
		}
	}
}

// broadcastCapNotify sends CAP NEW/DEL to clients that negotiated cap-notify.
func (s *Session) broadcastCapNotify(sub string, names []string) {
	if len(names) == 0 {
		return
	}
	msg := irc.Message{
		Source:  ServerName,
		Command: "CAP",
		Params:  []string{"*", sub, strings.Join(names, " ")},
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, d := range s.downlinks {
		if !d.HasCap("cap-notify") {
			continue
		}
		if sub == "DEL" {
			for _, n := range names {
				d.ClearCap(caps.CapName(n))
			}
		}
		_ = d.Send(msg)
	}
}

// OnDisconnect implements uplink.Handler.
func (s *Session) OnDisconnect(u *uplink.Uplink, err error) {
	_ = u
	s.log.Info("uplink down", "err", err)
	s.mu.Lock()
	prevOffer := caps.Offered(s.upCaps)
	if s.saslOffer != "" {
		prevOffer = append(prevOffer, s.saslOffer)
	}
	s.upCaps = make(map[string]bool)
	s.saslOffer = ""
	s.saslWaiters = nil
	s.saslReqPending = false
	s.saslClient = ""
	nowOffer := caps.Offered(s.upCaps)
	s.mu.Unlock()
	if lost := caps.Diff(nowOffer, prevOffer); len(lost) > 0 {
		s.broadcastCapNotify("DEL", lost)
	}
}

// OnMessage implements uplink.Handler — update state, history, fan-out to downlinks.
func (s *Session) OnMessage(u *uplink.Uplink, msg irc.Message) {
	_ = u
	if msg.Command == "CAP" {
		return
	}
	if msg.Command == "AUTHENTICATE" ||
		msg.Command == "900" || msg.Command == "903" || msg.Command == "904" ||
		msg.Command == "905" || msg.Command == "906" || msg.Command == "907" {
		_ = s.routeSASLPassthrough(msg)
		return
	}
	msg = ensureMessageTime(msg)
	s.maybeStoreHistory(msg) // before applyState so QUIT/NICK still see channel membership
	s.applyState(msg)

	s.mu.RLock()
	cm := s.isupport.CaseMapping
	s.mu.RUnlock()
	client, only, echoLabel, stripWHOX, restoreWHOX := s.tracker.RouteMessage(msg, cm)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if only {
		if d, ok := s.downlinks[client]; ok {
			out := s.rewriteFor(d, msg)
			if out.Command == "354" && len(out.Params) > 1 {
				if stripWHOX {
					// Remove injected querytype token (params[1]).
					out.Params = append([]string{out.Params[0]}, out.Params[2:]...)
					out.Raw = ""
				} else if restoreWHOX != "" {
					out.Params[1] = restoreWHOX
					out.Raw = ""
				}
			}
			if echoLabel != "" && d.HasCap("labeled-response") {
				if out.Tags == nil {
					out.Tags = map[string]string{}
				}
				out.Tags["label"] = echoLabel
			}
			_ = d.Send(out)
		}
		return
	}
	for _, d := range s.downlinks {
		out := s.rewriteFor(d, msg)
		if out.Command == "" {
			continue
		}
		_ = d.Send(out)
	}
}

// ensureMessageTime adds @time= when the uplink did not provide server-time.
// Used for live fan-out and for history Raw stored in the DB.
func ensureMessageTime(msg irc.Message) irc.Message {
	if _, ok := msg.Tag("time"); ok {
		return msg
	}
	msg.Tags = msg.CopyTags()
	if msg.Tags == nil {
		msg.Tags = map[string]string{}
	}
	msg.Tags["time"] = time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	return msg
}

func (s *Session) maybeStoreHistory(msg irc.Message) {
	if s.hist == nil {
		return
	}
	targets := s.historyTargets(msg)
	if len(targets) == 0 {
		return
	}
	ts := time.Now().UTC()
	if t, ok := msg.Tag("time"); ok {
		if parsed, err := time.Parse(time.RFC3339Nano, t); err == nil {
			ts = parsed
		} else if parsed, err := time.Parse(time.RFC3339, t); err == nil {
			ts = parsed
		}
	}
	msgid, _ := msg.Tag("msgid")
	raw := msg.Raw
	if raw == "" {
		raw = msg.Encode()
	}
	text := msg.Trailing()
	for _, target := range targets {
		_ = s.hist.Store(context.Background(), history.Record{
			NetworkID: s.Network.ID,
			Target:    target,
			Time:      ts,
			MsgID:     msgid,
			Command:   msg.Command,
			Source:    msg.Source,
			Raw:       raw,
			Text:      text,
		})
	}
}

// historyTargets returns folded channel/query names this line should be stored under.
// Call before applyState so QUIT/NICK still see membership.
func (s *Session) historyTargets(msg irc.Message) []string {
	cm := s.isupport.CaseMapping
	switch msg.Command {
	case "PRIVMSG", "NOTICE", "TAGMSG":
		t := msg.Param(0)
		if t == "" {
			return nil
		}
		return []string{cm.Canonical(t)}
	case "JOIN":
		t := msg.Param(0)
		if t == "" {
			t = msg.Trailing()
		}
		if t == "" {
			return nil
		}
		return []string{cm.Canonical(t)}
	case "PART", "KICK", "TOPIC":
		t := msg.Param(0)
		if t == "" {
			return nil
		}
		return []string{cm.Canonical(t)}
	case "MODE":
		t := msg.Param(0)
		if t == "" || !isChannelName(t) {
			return nil // ignore user modes
		}
		return []string{cm.Canonical(t)}
	case "QUIT", "NICK":
		s.mu.RLock()
		defer s.mu.RUnlock()
		nick := msg.Nick()
		folded := cm.Canonical(nick)
		var out []string
		for key, ch := range s.channels {
			if _, ok := ch.Members[folded]; ok {
				out = append(out, key)
			}
		}
		return out
	default:
		return nil
	}
}

func isChannelName(s string) bool {
	return len(s) > 0 && (s[0] == '#' || s[0] == '&' || s[0] == '+' || s[0] == '!')
}

// rewriteFor strips/adjusts an upstream message for a client's negotiated caps.
// Empty Command means the caller should not deliver the message to this client.
func (s *Session) rewriteFor(d Downlink, msg irc.Message) irc.Message {
	return s.rewriteMessage(d, msg, false)
}

// rewriteMessage is rewriteFor with forceSelfEcho for locally synthesized echoes
// when the uplink lacks echo-message (deliver to every client).
func (s *Session) rewriteMessage(d Downlink, msg irc.Message, forceSelfEcho bool) irc.Message {
	out := msg
	out.Tags = msg.CopyTags()
	if !s.clientAccepts(d, out, forceSelfEcho) {
		out.Command = ""
		return out
	}
	// extended-join: strip account/GECOS for clients that did not negotiate it.
	if out.Command == "JOIN" && !d.HasCap("extended-join") && len(out.Params) > 1 {
		out.Params = []string{out.Params[0]}
		out.Raw = "" // body changed; cannot reuse uplink wire form
	}
	wantTime := d.HasCap("server-time")
	wantTags := d.HasCap("message-tags")
	if !d.HasCap("batch") && out.Tags != nil {
		delete(out.Tags, "batch")
	}
	if !wantTags && !wantTime {
		out.Tags = nil
		return out
	}
	if !wantTags {
		// server-time alone: only the time tag
		t, ok := out.Tag("time")
		if !ok {
			t = time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
		}
		out.Tags = map[string]string{"time": t}
		return out
	}
	if wantTime {
		if _, ok := out.Tag("time"); !ok {
			if out.Tags == nil {
				out.Tags = map[string]string{}
			}
			out.Tags["time"] = time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
		}
	} else if out.Tags != nil {
		delete(out.Tags, "time")
	}
	if out.Tags != nil && len(out.Tags) == 0 {
		out.Tags = nil
	}
	return out
}

// clientAccepts reports whether this downlink should receive msg given its caps.
func (s *Session) clientAccepts(d Downlink, msg irc.Message, forceSelfEcho bool) bool {
	switch msg.Command {
	case "TAGMSG":
		if !d.HasCap("message-tags") {
			return false
		}
		if s.isSelfNick(msg.Nick()) && !forceSelfEcho {
			return d.HasCap("echo-message")
		}
		return true
	case "PRIVMSG", "NOTICE":
		if s.isSelfNick(msg.Nick()) && !forceSelfEcho {
			return d.HasCap("echo-message")
		}
		return true
	case "BATCH":
		return d.HasCap("batch")
	case "AWAY":
		// away-notify; never echo our own AWAY (use 305/306 instead).
		if !d.HasCap("away-notify") {
			return false
		}
		return !s.isSelfNick(msg.Nick())
	case "CHGHOST":
		return d.HasCap("chghost")
	case "ACCOUNT":
		return d.HasCap("account-notify")
	case "INVITE":
		// Traditional INVITE targets us; invite-notify also sends INVITEs for others.
		if s.isSelfNick(msg.Param(0)) {
			return true
		}
		return d.HasCap("invite-notify")
	default:
		return true
	}
}

func (s *Session) isSelfNick(nick string) bool {
	if nick == "" {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.self == nil {
		return false
	}
	return s.isupport.CaseMapping.Equal(nick, s.self.Nick)
}
