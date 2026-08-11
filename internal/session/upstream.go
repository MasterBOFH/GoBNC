package session

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/caps"
	"github.com/MasterBOFH/GoBNC/internal/history"
	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/uplink"
	"github.com/google/uuid"
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
	if acct := u.Account(); acct != "" {
		s.self.Account = acct
		s.loggedIn = true
	}
	s.registered = true
	// Clients that watched live registration are done awaiting.
	awaiting := make([]Downlink, 0, len(s.awaitingUplink))
	for id := range s.awaitingUplink {
		if d, ok := s.downlinks[id]; ok {
			awaiting = append(awaiting, d)
		}
	}
	s.awaitingUplink = make(map[ClientID]bool)
	s.regBuffer = nil
	loggedIn, haveLogin := s.rplLoggedInLocked()
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
	for _, d := range awaiting {
		if haveLogin {
			_ = d.Send(s.rewriteFor(d, loggedIn))
		}
		s.notifyAttachCaps(d)
	}
	s.flushHeldAfterRegister()
}

// OnRegistrationLine implements uplink.Handler — relay pre-registration traffic
// to clients that attached before uplink welcome completed.
func (s *Session) OnRegistrationLine(u *uplink.Uplink, msg irc.Message) {
	_ = u
	s.mu.Lock()
	cfgNick := s.Network.Nick
	var nickChangeFrom string
	if msg.Command == "001" && len(msg.Params) > 0 {
		actual := msg.Params[0]
		s.ensureSelfLocked(actual)
		s.self.Nick = actual
		if src := strings.TrimSpace(msg.Source); src != "" {
			s.uplinkServer = src
		}
		// Clients may already have a synthetic 001 with the configured nick.
		// Send self-NICK before the real 001 when the live nick differs.
		if cfgNick != "" && !s.isupport.CaseMapping.Equal(cfgNick, actual) {
			nickChangeFrom = cfgNick
		}
	}
	s.regBuffer = append(s.regBuffer, msg)
	targets := make([]Downlink, 0, len(s.awaitingUplink))
	for id := range s.awaitingUplink {
		if d, ok := s.downlinks[id]; ok {
			targets = append(targets, d)
		}
	}
	ident := s.Network.Username
	s.mu.Unlock()
	if ident == "" {
		ident = "gobnc"
	}
	for _, d := range targets {
		if nickChangeFrom != "" {
			_ = d.Send(s.rewriteFor(d, irc.Message{
				Source:  nickChangeFrom + "!" + ident + "@" + ServerName,
				Command: "NICK",
				Params:  []string{msg.Params[0]},
			}))
		}
		_ = d.Send(s.rewriteFor(d, msg))
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
//
// If the uplink had finished registration, clients get ERROR and are closed so
// they reconnect with a clean attach burst. If registration never completed
// (including reconnect attempts while clients are already attached and waiting),
// keep downlinks open, clear ephemeral registration state, and NOTICE the
// failure so /bnc remains usable.
func (s *Session) OnDisconnect(u *uplink.Uplink, err error) {
	_ = u
	s.log.Info("uplink down", "err", err)
	reason := disconnectReason(err)

	s.mu.Lock()
	wasRegistered := s.registered
	clients := make([]Downlink, 0, len(s.downlinks))
	for _, d := range s.downlinks {
		clients = append(clients, d)
	}
	if wasRegistered {
		s.downlinks = make(map[ClientID]Downlink)
		s.awaitingUplink = make(map[ClientID]bool)
	} else {
		// Stay attached and keep waiting for the next successful registration.
		awaiting := make(map[ClientID]bool, len(s.downlinks))
		for id := range s.downlinks {
			awaiting[id] = true
		}
		s.awaitingUplink = awaiting
	}
	s.regBuffer = nil
	s.registered = false
	s.upCaps = make(map[string]bool)
	s.saslOffer = ""
	s.saslWaiters = nil
	s.saslReqPending = false
	s.saslClient = ""
	s.loggedIn = false
	s.rpl002, s.rpl003, s.rpl004 = nil, nil, nil
	s.uplinkServer = ""
	s.ircd = ""
	s.channels = make(map[string]*ChannelState)
	s.pendingJoinKeys = make(map[string]string)
	s.selfEcho = nil
	s.selfEchoSeq = 0
	if wasRegistered {
		s.heldUntilReg = nil
	}
	s.heldFlushCancel = nil
	s.heldFlushSent = nil
	nick := s.Network.Nick
	user := s.Network.Username
	s.self = &User{Nick: nick, User: user, UModes: make(map[byte]bool)}
	s.users = map[string]*User{
		irc.CaseRFC1459.Canonical(nick): s.self,
	}
	s.isupport = irc.NewISUPPORT()
	s.tracker = NewRequestTracker()
	s.mu.Unlock()

	if wasRegistered {
		errMsg := irc.Message{Command: "ERROR", Params: []string{reason}}
		for _, d := range clients {
			_ = d.Send(errMsg)
			_ = d.Close()
		}
		return
	}
	for _, d := range clients {
		_ = d.Send(s.rewriteFor(d, irc.Message{
			Source:  ServerName,
			Command: "NOTICE",
			Params:  []string{nickOrStar(nick), "Uplink connection failed: " + reason + ". Retrying..."},
		}))
	}
}

func nickOrStar(nick string) string {
	if nick == "" {
		return "*"
	}
	return nick
}

func disconnectReason(err error) string {
	if err == nil {
		return "connection to the server was lost"
	}
	if errors.Is(err, context.Canceled) {
		return "connection to the server was lost"
	}
	msg := strings.TrimSpace(err.Error())
	if msg == "" {
		return "connection to the server was lost"
	}
	// Prefer the human text from "server ERROR: …".
	const prefix = "server ERROR: "
	if strings.HasPrefix(msg, prefix) {
		msg = strings.TrimSpace(msg[len(prefix):])
		if msg != "" {
			return msg
		}
	}
	return msg
}

// OnMessage implements uplink.Handler — update state, history, fan-out to downlinks.
func (s *Session) OnMessage(u *uplink.Uplink, msg irc.Message) {
	_ = u
	// Belt-and-suspenders: uplink keepalive must never reach clients.
	if msg.Command == "PING" || msg.Command == "PONG" {
		return
	}
	if msg.Command == "CAP" {
		return
	}
	if msg.Command == "AUTHENTICATE" ||
		msg.Command == "900" || msg.Command == "901" ||
		msg.Command == "903" || msg.Command == "904" ||
		msg.Command == "905" || msg.Command == "906" || msg.Command == "907" ||
		msg.Command == "908" {
		s.routeSASLTraffic(msg)
		return
	}
	msg = ensureMessageTime(msg)
	msg = ensureMessageID(msg)
	pending, msg, selfEcho := s.consumeSelfEcho(msg)
	if selfEcho {
		s.maybeStoreHistory(msg)
		s.applyState(msg)
		if strings.EqualFold(msg.Command, "ACK") {
			// Labeled ACK only goes to the originating client.
			s.mu.RLock()
			d, have := s.downlinks[pending.Client]
			s.mu.RUnlock()
			if have {
				out := s.rewriteFor(d, msg)
				if pending.Label != "" && d.HasCap("labeled-response") {
					if out.Tags == nil {
						out.Tags = map[string]string{}
					}
					out.Tags["label"] = pending.Label
				}
				_ = d.Send(out)
			}
			return
		}
		s.fanoutSelfEcho(msg, pending)
		return
	}
	s.maybeStoreHistory(msg) // before applyState so QUIT/NICK still see channel membership
	s.applyState(msg)

	s.mu.RLock()
	cm := s.isupport.CaseMapping
	s.mu.RUnlock()
	client, only, echoLabel, stripWHOX, restoreWHOX := s.tracker.RouteMessage(msg, cm)
	s.mu.RLock()
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
			if out.Command != "" && legacyPlaybackCommands(msg) && !hasChathistory(d) {
				s.advanceLegacyPlaybackIfDelivered(msg, true)
			}
		}
		s.mu.RUnlock()
		return
	}
	legacyHit := false
	for _, d := range s.downlinks {
		out := s.rewriteFor(d, msg)
		if out.Command == "" {
			continue
		}
		_ = d.Send(out)
		if legacyPlaybackCommands(msg) && !hasChathistory(d) {
			legacyHit = true
		}
	}
	s.mu.RUnlock()
	s.advanceLegacyPlaybackIfDelivered(msg, legacyHit)
	s.maybeSendMarkReadOnSelfJOIN(msg)
	s.maybeJoinOnInvite(msg)
}

// maybeJoinOnInvite retries JOIN when we are invited to a channel that is still
// on the auto-join list but we are not currently in (e.g. invite-only failure
// after reconnect). Intentional PART removes the channel from the store, so
// invites after PART do not rejoin.
func (s *Session) maybeJoinOnInvite(msg irc.Message) {
	if msg.Command != "INVITE" {
		return
	}
	line := s.inviteAutoJoinLine(msg)
	if line == "" || s.uplink == nil {
		return
	}
	_ = s.uplink.WriteRaw(line)
}

// inviteAutoJoinLine returns a JOIN line when INVITE targets us for a remembered
// but unjoined channel; otherwise "".
func (s *Session) inviteAutoJoinLine(msg irc.Message) string {
	if !s.isSelfNick(msg.Param(0)) {
		return ""
	}
	chName := msg.Param(1)
	if chName == "" || s.store == nil || s.Network.ID == 0 {
		return ""
	}

	s.mu.RLock()
	cm := s.isupport.CaseMapping
	_, joined := s.channels[cm.Canonical(chName)]
	s.mu.RUnlock()
	if joined {
		return ""
	}

	chs, err := s.store.ListChannels(context.Background(), s.Network.ID)
	if err != nil {
		return ""
	}
	for _, ch := range chs {
		if !cm.Equal(ch.Name, chName) {
			continue
		}
		if ch.Key != "" {
			return "JOIN " + ch.Name + " " + ch.Key
		}
		return "JOIN " + ch.Name
	}
	return ""
}

// ensureMessageTime adds @time= when the uplink did not provide server-time.
// Used for live fan-out and for history Raw stored in the DB.
// Keeps Raw so Wire() can reuse the uplink body (colonation, spacing) and only
// replace the tag prefix.
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

// ensureMessageID adds @msgid= when the uplink did not supply one.
// IDs are RFC 4122 UUIDs so they stay unique across restarts and never collide
// with typical ircd formats. Spec MAY attach on any event (SHOULD on PRIVMSG/
// NOTICE); we assign on all live traffic so history and clients stay consistent.
// See https://ircv3.net/specs/extensions/message-ids
// Keeps Raw so Wire() preserves the uplink body verbatim.
func ensureMessageID(msg irc.Message) irc.Message {
	if msg.Command == "" {
		return msg
	}
	if _, ok := msg.Tag("msgid"); ok {
		return msg
	}
	msg.Tags = msg.CopyTags()
	if msg.Tags == nil {
		msg.Tags = map[string]string{}
	}
	msg.Tags["msgid"] = uuid.NewString()
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
	// Wire keeps the uplink body (from Raw) and applies our tags — verbatim relay.
	raw := msg.Wire()
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

// rewriteFor adjusts an upstream message for a client's negotiated caps.
// Empty Command means the caller should not deliver the message to this client.
//
// Policy: the IRC body (prefix/command/params) is passed through via msg.Raw /
// Wire. Capability handling only replaces the tag prefix, except:
//   - extended-join: strip account/GECOS from JOIN (body edit → replace Raw)
//   - WHOX token fixups in OnMessage (body edit → clear Raw)
func (s *Session) rewriteFor(d Downlink, msg irc.Message) irc.Message {
	out := msg
	out.Tags = msg.CopyTags()
	if !s.clientAccepts(d, out) {
		out.Command = ""
		return out
	}
	// extended-join: clients without the cap must not see account/GECOS params.
	if out.Command == "JOIN" && !d.HasCap("extended-join") && len(out.Params) > 1 {
		ch := out.Params[0]
		out.Params = []string{ch}
		// Body edit: set an explicit JOIN wire form (do not re-Encode the uplink line).
		if out.Source != "" {
			out.Raw = ":" + out.Source + " JOIN " + ch
		} else {
			out.Raw = "JOIN " + ch
		}
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
func (s *Session) clientAccepts(d Downlink, msg irc.Message) bool {
	switch msg.Command {
	case "TAGMSG":
		if !d.HasCap("message-tags") {
			return false
		}
		if s.isSelfNick(msg.Nick()) {
			return d.HasCap("echo-message")
		}
		return true
	case "PRIVMSG", "NOTICE":
		if s.isSelfNick(msg.Nick()) {
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
