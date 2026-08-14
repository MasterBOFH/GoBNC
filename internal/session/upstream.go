package session

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/caps"
	"github.com/MasterBOFH/GoBNC/internal/history"
	"github.com/MasterBOFH/GoBNC/internal/irc"
	gobnclog "github.com/MasterBOFH/GoBNC/internal/log"
	"github.com/google/uuid"
)

// DesiredCaps requested when available, post-registration (CAP NEW). This
// is Session's own copy of internal/uplink.DesiredCaps' list — during
// registration itself, internal/registration.Step owns the identical
// decision independently (it has its own copy, for the same
// small-package-no-shared-dependency reason keeper.NetworkID and
// brain.ChannelJoin already do); Session only needs this for caps a server
// advertises *after* registration completes, which registration.Step never
// sees (Step is a deliberate no-op past a terminal Phase).
var DesiredCaps = []string{
	"cap-notify",
	"message-tags",
	"server-time",
	"batch",
	"labeled-response",
	"account-tag",
	"account-notify",
	"extended-join",
	"echo-message",
	"away-notify",
	"chghost",
	"invite-notify",
	"sasl",
	"chathistory",
	"draft/chathistory",
}

// HandleLine is the single entry point for every line the uplink says,
// delivered by internal/server's demux from brain.Driver.Lines() — before,
// during, and forever after registration (see Driver.Lines' doc comment).
// This replaces the old uplink.Handler split (OnRegistrationLine/
// OnRegistered/OnMessage, each invoked by internal/uplink.Uplink itself at
// the right moment): there's no Uplink instance left to decide "the right
// moment" on Session's behalf, so Session decides it here, from the raw
// line stream directly, the same way registration.Step does.
func (s *Session) HandleLine(raw []byte) {
	msg, err := irc.Parse(string(raw))
	if err != nil {
		s.log.Warn("parse error", "line", gobnclog.RedactIRC(string(raw)), "err", err)
		return
	}
	// Belt-and-suspenders: uplink keepalive must never reach clients, and
	// the keeper answers PING autonomously — see internal/keeper's package
	// doc — so this is never itself an action Session needs to take.
	if msg.Command == "PING" || msg.Command == "PONG" {
		return
	}
	// Safe to run unconditionally, pre- or post-registration: applyState is
	// a pure state-cache updater with no fan-out of its own (see state.go).
	// Feeding it every line as it arrives — rather than only snapshotting
	// once at registration completion, the way OnRegistered used to copy
	// from *uplink.Uplink's own incrementally-built fields — is what lets
	// HandleRegistered below not need a registration.State parameter at
	// all: by the time completeRegistration runs, s.isupport/s.rpl002-4/
	// s.ircd/s.self.UModes are already exactly right.
	s.applyState(msg)

	if !s.Registered() {
		s.HandleRegistrationLine(msg)
		return
	}
	s.HandleMessage(msg)
}

// HandleRegistrationLine relays pre-registration traffic to clients
// that attached before uplink welcome completed, and detects registration
// completion directly from the wire (376/422 after 001) rather than
// waiting on a separate signal from Driver.Results() — see HandleLine's
// doc comment. Using the same 376/422-after-001 signal
// registration.Step itself uses (see registration.go's Step) means this
// can never race a companion "registration complete" notification arriving
// on a different channel in the wrong order, because there is no companion
// notification for the success path anymore; internal/server's demux only
// needs Driver.Results() for the *failure* path (deadline, nick ladder
// exhausted, SASL required — none of which are otherwise observable in the
// raw line stream), which HandleDisconnect already covers.
func (s *Session) HandleRegistrationLine(msg irc.Message) {
	switch msg.Command {
	case "CAP":
		s.handleCAPLine(msg, false)
		return
	case "AUTHENTICATE":
		return
	case "900", "901", "903", "904", "905", "906", "907", "908":
		s.applyAccountFromSASL(msg)
		return
	case "432", "433", "437":
		// Not relayed here unconditionally: whether a given nick error is
		// the terminal one (ladder exhausted) or gets silently swallowed
		// (ladder tries the next candidate) is a registration.Step-owned
		// decision Session doesn't replicate — see nick.go's
		// nextLadderNick. Only the terminal one should ever reach a
		// client (matches internal/uplink's old behavior: emitRegistrationLine
		// only ran on the ladder-exhausted return path, never on a
		// swallowed continue). Stashed here and surfaced by
		// HandleDisconnect exactly when the failure it's reporting really
		// is this nick error — see its own doc comment.
		s.mu.Lock()
		s.lastNickErrorLine = msg
		s.hasLastNickErrorLine = true
		s.mu.Unlock()
		return
	}

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
		s.gotWelcome = true
		// Clients may already have a synthetic 001 with the configured nick.
		// Send self-NICK before the real 001 when the live nick differs.
		if cfgNick != "" && !s.isupport.CaseMapping.Equal(cfgNick, actual) {
			nickChangeFrom = cfgNick
		}
	}
	visible := isRegistrationVisible(msg.Command)
	if visible {
		s.regBuffer = append(s.regBuffer, msg)
	}
	var targets []Downlink
	if visible {
		targets = make([]Downlink, 0, len(s.awaitingUplink))
		for id := range s.awaitingUplink {
			if d, ok := s.downlinks[id]; ok {
				targets = append(targets, d)
			}
		}
	}
	ident := s.Network.Username
	gotWelcome := s.gotWelcome
	s.mu.Unlock()

	if visible {
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

	if (msg.Command == "376" || msg.Command == "422") && gotWelcome {
		s.completeRegistration()
	}
}

// isRegistrationVisible reports whether a command should be relayed to
// clients waiting on uplink registration. Mirrors internal/uplink.go's
// register() switch: NOTICE and MODE were always relayed via their own
// explicit case (never gated by the old isRegistrationVisible function at
// all — that function only ever guarded register()'s default branch, i.e.
// numerics with no case of their own), and 432/433/437 are deliberately
// excluded here even though they're 3-digit numerics — see
// HandleRegistrationLine's own case for why those need different handling
// entirely, not just a "relay or don't" decision made from the command
// name alone.
func isRegistrationVisible(cmd string) bool {
	switch strings.ToUpper(cmd) {
	case "NOTICE", "MODE":
		return true
	case "CAP", "AUTHENTICATE", "PING", "PONG", "ERROR",
		"432", "433", "437",
		"900", "901", "902", "903", "904", "905", "906", "907", "908":
		return false
	}
	if len(cmd) == 3 {
		for i := 0; i < 3; i++ {
			if cmd[i] < '0' || cmd[i] > '9' {
				return false
			}
		}
		return true
	}
	return false
}

// completeRegistration replaces OnRegistered's body — see HandleLine's doc
// comment for why it no longer takes a registration.State: every field the
// old snapshot copied is already current on Session by the time this runs,
// having been kept that way incrementally as each line arrived. Guarded so
// a stray extra 376/422 (or, in principle, a duplicate call from anywhere
// else) can't re-fire the completion side effects.
func (s *Session) completeRegistration() {
	s.mu.Lock()
	if s.registered {
		s.mu.Unlock()
		return
	}
	prevOffer := caps.Offered(nil) // a fresh registration always starts from an empty upCaps (see HandleDisconnect)
	s.registered = true
	nick := s.self.Nick
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

	_, nowSASL := s.refreshSASLOffer()
	s.mu.Lock()
	nowOffer := caps.Offered(s.upCaps)
	if nowSASL != "" {
		nowOffer = append(nowOffer, nowSASL)
	}
	s.mu.Unlock()

	s.log.Info("uplink registered", "nick", nick)
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

// handleCAPLine interprets one CAP line from the uplink and updates
// Session's own caps/SASL-availability bookkeeping — the merger of what
// internal/uplink.Uplink.handleCAP (raw CAP parsing) and
// session.Session.OnCapsChanged/OnSASLOffer (client-facing notification)
// used to split across two types, now that there's only Session left to do
// either. Called for every CAP line, pre- and post-registration:
// pre-registration, registration.Step already owns the CAP LS/REQ/END
// *protocol* (deciding what to request, when to end negotiation) — the
// only thing left for Session to do is track what got ACK'd, so it's
// accurate the moment registration completes. registered controls the
// client-facing side (broadcastCapNotify, OnCapNAK, SASL-offer-change,
// and — new in this cutover, since nothing else does it anymore now that
// Uplink itself is gone — auto-REQing newly desired caps advertised via a
// post-registration CAP NEW, which registration.Step can never see because
// Step no-ops past a terminal Phase).
func (s *Session) handleCAPLine(msg irc.Message, registered bool) {
	if len(msg.Params) < 2 {
		return
	}
	sub := strings.ToUpper(msg.Params[1])
	trailing := msg.Trailing()
	switch sub {
	case "LS":
		available := parseCapList(trailing)
		if v, ok := available["sasl"]; ok {
			s.mu.Lock()
			s.noteSASLOfferLocked(v)
			s.mu.Unlock()
		}
	case "ACK":
		s.mu.Lock()
		prevOffer := caps.Offered(s.upCaps)
		if s.saslOffer != "" {
			prevOffer = append(prevOffer, s.saslOffer)
		}
		var added []string
		for _, raw := range strings.Fields(trailing) {
			raw = strings.TrimPrefix(raw, "-")
			name, val, _ := strings.Cut(raw, "=")
			if name == "sasl" && val != "" {
				s.noteSASLOfferLocked(val)
			}
			if !s.upCaps[name] {
				added = append(added, name)
			}
			s.upCaps[name] = true
		}
		s.mu.Unlock()
		if !registered {
			return
		}
		_, nowSASL := s.refreshSASLOffer()
		s.mu.Lock()
		nowOffer := caps.Offered(s.upCaps)
		if nowSASL != "" {
			nowOffer = append(nowOffer, nowSASL)
		}
		s.mu.Unlock()
		if gained := caps.Diff(prevOffer, nowOffer); len(gained) > 0 {
			s.broadcastCapNotify("NEW", gained)
		}
		for _, name := range added {
			if name == "sasl" {
				s.finishSASLWaiters(true)
				break
			}
		}
	case "NAK":
		if !registered {
			return
		}
		var names []string
		for _, raw := range strings.Fields(trailing) {
			raw = strings.TrimPrefix(raw, "-")
			name, _, _ := strings.Cut(raw, "=")
			names = append(names, name)
		}
		s.OnCapNAK(names)
	case "NEW":
		if !registered {
			return
		}
		available := parseCapList(trailing)
		saslWanted := s.Network.SASL
		if v, ok := available["sasl"]; ok {
			s.mu.Lock()
			s.noteSASLOfferLocked(v)
			s.mu.Unlock()
			if !saslWanted {
				prev, now := s.refreshSASLOffer()
				s.notifySASLOfferChange(prev, now)
			}
		}
		var req []string
		for _, want := range DesiredCaps {
			if want == "sasl" && !saslWanted {
				continue
			}
			if _, ok := available[want]; ok && !s.HasUpCap(want) {
				req = append(req, want)
			}
		}
		if len(req) > 0 && s.driver != nil {
			_ = s.driver.WriteRaw(s.netID, "CAP REQ :"+strings.Join(req, " "))
		}
	case "DEL":
		if !registered {
			return
		}
		s.mu.Lock()
		prevOffer := caps.Offered(s.upCaps)
		if s.saslOffer != "" {
			prevOffer = append(prevOffer, s.saslOffer)
		}
		var removed []string
		for _, name := range strings.Fields(trailing) {
			name = strings.TrimPrefix(name, "-")
			name, _, _ = strings.Cut(name, "=")
			if s.upCaps[name] {
				delete(s.upCaps, name)
				removed = append(removed, name)
			}
			if name == "sasl" {
				s.upSASLAvailable = false
				s.upSASLMechs = nil
			}
		}
		s.mu.Unlock()
		// sasl's own DEL notice, if any, falls out of this diff naturally:
		// refreshSASLOffer recomputes saslOffer from the fields just
		// cleared above, so a lost sasl offer is already reflected in
		// nowOffer below, same as any other lost cap.
		_, nowSASL := s.refreshSASLOffer()
		s.mu.Lock()
		nowOffer := caps.Offered(s.upCaps)
		if nowSASL != "" {
			nowOffer = append(nowOffer, nowSASL)
		}
		s.mu.Unlock()
		if lost := caps.Diff(nowOffer, prevOffer); len(lost) > 0 {
			s.broadcastCapNotify("DEL", lost)
		}
		for _, name := range removed {
			if name == "sasl" {
				s.finishSASLWaiters(false)
				break
			}
		}
	}
}

// noteSASLOfferLocked records sasl availability/mechs from a CAP LS/NEW/ACK
// value — ported from internal/uplink.Uplink.noteSASLOffer's "present"
// branch (the "not present" branch is handleCAPLine's DEL case, which
// clears these fields directly instead). Caller must hold s.mu.
func (s *Session) noteSASLOfferLocked(val string) {
	s.upSASLAvailable = true
	if val == "" {
		return
	}
	var mechs []string
	for _, m := range strings.Split(val, ",") {
		m = strings.TrimSpace(m)
		if m != "" {
			mechs = append(mechs, m)
		}
	}
	if len(mechs) > 0 {
		s.upSASLMechs = mechs
	}
}

// parseCapList parses a CAP 302 list into name → value (value may be
// empty) — ported verbatim from internal/uplink/uplink.go.
func parseCapList(s string) map[string]string {
	out := make(map[string]string)
	for _, p := range strings.Fields(s) {
		name, val, _ := strings.Cut(p, "=")
		out[name] = val
	}
	return out
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
		} else if sub == "NEW" {
			// Record as seen so a later attach-time sync (notifyAttachCaps)
			// does not re-announce it.
			for _, n := range names {
				d.MarkSeenCap(caps.CapName(n))
			}
		}
		_ = d.Send(msg)
	}
}

// HandleDisconnect replaces OnDisconnect's body — see internal/server's
// demux for the two signals that call it: a NetworkEventMsg{Disconnected}
// for a connection that died (registered or not), and a Driver Result with
// Phase == PhaseFailed for a registration attempt that failed without ever
// losing the connection on its own (deadline, nick ladder exhausted, SASL
// required) — see brain.Driver.failRegistration's doc comment for why that
// case has no corresponding NetworkEvent to react to instead. The two can
// both fire for a single mid-registration server-initiated disconnect
// (failRegistration is what turns *that* case into a Result, but the
// disconnect itself also produces its own NetworkEvent); calling this
// twice in that narrow case is benign — the same ephemeral state gets
// reset twice, and attached-but-still-registering clients may see the
// "Retrying..." NOTICE twice — not treated as worth a dedup mechanism.
//
// If the uplink had finished registration, clients get ERROR and are closed so
// they reconnect with a clean attach burst. If registration never completed
// (including reconnect attempts while clients are already attached and waiting),
// keep downlinks open, clear ephemeral registration state, and NOTICE the
// failure so /bnc remains usable.
func (s *Session) HandleDisconnect(err error) {
	s.log.Info("uplink down", "err", err)
	reason := disconnectReason(err)

	s.mu.Lock()
	wasRegistered := s.registered
	nickErrLine := s.lastNickErrorLine
	hadNickErr := s.hasLastNickErrorLine
	s.lastNickErrorLine = irc.Message{}
	s.hasLastNickErrorLine = false
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
	s.gotWelcome = false
	s.upCaps = make(map[string]bool)
	s.upSASLAvailable = false
	s.upSASLMechs = nil
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
	// Surface the triggering nick error itself, but only when this
	// disconnect genuinely IS that nick ladder exhausting — registration's
	// own Err text says so (see registration/nick.go's stepNickError,
	// the only producer of this exact wording). A stale hadNickErr from an
	// earlier, successfully-swallowed 433 (ladder moved on, then some
	// unrelated failure happened later) fails this check and is correctly
	// never surfaced — matches internal/uplink's old behavior, where the
	// numeric was relayed synchronously, only on the ladder-exhausted path.
	if hadNickErr && strings.Contains(reason, "nick error:") {
		for _, d := range clients {
			_ = d.Send(s.rewriteFor(d, nickErrLine))
		}
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

// HandleMessage replaces OnMessage's body — update state, history, fan-out
// to downlinks, called from HandleLine once the uplink is registered. Also
// now folds in CAP interpretation and SASL-outcome logging, both of which
// internal/uplink.Uplink used to own internally (handleCAP,
// handleSASLOutcome) and hand Session only the already-decided result.
func (s *Session) HandleMessage(msg irc.Message) {
	// Belt-and-suspenders: uplink keepalive must never reach clients.
	// HandleLine already drops PING/PONG before ever calling HandleMessage
	// for traffic arriving the normal way (see its own doc comment), but
	// this method is also called directly by a couple of tests exercising
	// fan-out in isolation — kept here too, matching
	// internal/uplink.Uplink.OnMessage's own redundant guard, which this
	// replaces.
	if msg.Command == "PING" || msg.Command == "PONG" {
		return
	}
	if msg.Command == "CAP" {
		s.handleCAPLine(msg, true)
		return
	}
	if msg.Command == "AUTHENTICATE" ||
		msg.Command == "900" || msg.Command == "901" ||
		msg.Command == "903" || msg.Command == "904" ||
		msg.Command == "905" || msg.Command == "906" || msg.Command == "907" ||
		msg.Command == "908" {
		s.logSASLOutcome(msg)
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
	if only {
		// Snapshot the one downlink, then release s.mu before rewriteFor:
		// rewriteFor (via clientAccepts/isSelfNick) takes its own RLock,
		// and holding s.mu across that nested call is a real deadlock
		// hazard under concurrent writers (see sync.RWMutex's own docs on
		// recursive RLock) — found live, by hand, via a concurrency stress
		// test; not something this cutover introduced (the old
		// internal/uplink.Handler-driven OnMessage had the identical
		// shape), but worth closing now that it's been isolated.
		s.mu.RLock()
		d, ok := s.downlinks[client]
		s.mu.RUnlock()
		if ok {
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
		return
	}
	s.mu.RLock()
	downlinks := make([]Downlink, 0, len(s.downlinks))
	for _, d := range s.downlinks {
		downlinks = append(downlinks, d)
	}
	s.mu.RUnlock()
	legacyHit := false
	for _, d := range downlinks {
		out := s.rewriteFor(d, msg)
		if out.Command == "" {
			continue
		}
		_ = d.Send(out)
		if legacyPlaybackCommands(msg) && !hasChathistory(d) {
			legacyHit = true
		}
	}
	s.advanceLegacyPlaybackIfDelivered(msg, legacyHit)
	s.maybeSendMarkReadOnSelfJOIN(msg)
	s.maybeJoinOnInvite(msg)
}

// logSASLOutcome replaces internal/uplink.Uplink.handleSASLOutcome's
// logging half — ported as observability only. The old version also
// disconnected the uplink outright when SASLRequired was set and a
// post-registration, client-initiated (passthrough) SASL attempt failed;
// Session has no way to ask the shared Driver to hang up over an
// application-level policy failure like that (only QuitNetwork exists, for
// a deliberate bouncer-initiated disconnect), so that enforcement is not
// reproduced here. Narrow gap, flagged rather than silently dropped: it
// only matters for a network with SASLRequired set that also runs
// passthrough (not bouncer-owned) SASL, an unusual combination.
func (s *Session) logSASLOutcome(msg irc.Message) {
	if !s.Registered() {
		return
	}
	switch msg.Command {
	case "900", "903":
		s.log.Info("SASL authentication successful")
	case "901":
		s.log.Info("logged out of services account")
	case "907":
		s.log.Debug("SASL already authenticated")
	case "904", "905", "906", "908":
		s.log.Info("SASL authentication failed", "numeric", msg.Command, "text", msg.Trailing())
	}
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
	if line == "" || s.driver == nil {
		return
	}
	_ = s.WriteMessage(irc.Message{Raw: line})
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
//   - WHOX token fixups in HandleMessage (body edit → clear Raw)
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
