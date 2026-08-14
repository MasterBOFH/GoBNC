package session

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/MasterBOFH/GoBNC/internal/brain"
	"github.com/MasterBOFH/GoBNC/internal/irc"
)

// Detach removes a downlink.
func (s *Session) Detach(id ClientID) {
	s.mu.Lock()
	if d, ok := s.downlinks[id]; ok {
		delete(s.downlinks, id)
		_ = d.Close()
	}
	delete(s.awaitingUplink, id)
	if s.saslClient == id {
		s.saslClient = ""
	}
	if len(s.heldUntilReg) > 0 {
		kept := s.heldUntilReg[:0]
		for _, h := range s.heldUntilReg {
			if h.Client != id {
				kept = append(kept, h)
			}
		}
		s.heldUntilReg = kept
	}
	delete(s.heldFlushCancel, id)
	delete(s.heldFlushSent, id)
	s.mu.Unlock()
	s.tracker.DropClient(id)
	s.unsubscribeDebug(id)
}

// HandleClientMessage processes a message from a downlink toward the uplink.
func (s *Session) HandleClientMessage(d Downlink, msg irc.Message) error {
	if irc.HasRawCRLF(msg) {
		s.log.Warn("client message contains raw CR/LF; rejected",
			"client", d.ID(), "command", msg.Command)
		nick := s.Nick()
		if nick == "" {
			nick = "*"
		}
		return d.Send(irc.InputTooLong(nick))
	}
	// IRCv3 UTF8ONLY: once advertised by the network, do not send non-UTF-8 upstream.
	// Check Wire(), not Encode() — Wire() is what actually reaches the uplink
	// (it preserves Raw verbatim when the body is unrewritten).
	if s.isupport != nil && s.isupport.UTF8Only && !utf8.ValidString(msg.Wire()) {
		return s.failInvalidUTF8(d, msg.Command)
	}
	cmd := strings.ToUpper(msg.Command)
	switch cmd {
	case "PING":
		return d.Send(irc.Message{Command: "PONG", Params: msg.Params})
	case "PONG":
		// Reply to bouncer keepalive (or client stray PONG) — never forward upstream.
		return nil
	case "BNC":
		return s.handleBNC(d, msg.Params)
	case "AUTHENTICATE":
		return s.forwardClientAuthenticate(d, msg)
	case "MARKREAD":
		return s.handleMARKREAD(d, msg)
	case "CHATHISTORY":
		if s.hist != nil {
			if len(msg.Params) >= 2 {
				msg.Params[1] = s.isupport.CaseMapping.Canonical(msg.Params[1])
			}
			return s.hist.HandleCHATHISTORY(d, s.Network.ID, msg)
		}
		return d.Send(irc.Message{Source: ServerName, Command: "FAIL", Params: []string{"CHATHISTORY", "TEMPORARILY_UNAVAILABLE", "history unavailable"}})
	case "QUIT":
		s.Detach(d.ID())
		return nil
	case "JOIN":
		s.rememberJoinKeys(msg)
		if s.driver == nil {
			return fmt.Errorf("uplink not ready")
		}
		return s.WriteMessage(s.toUplink(msg))
	case "PART":
		// Removal from DB happens when uplink echoes our PART (applyState).
		if s.driver == nil {
			return fmt.Errorf("uplink not ready")
		}
		return s.WriteMessage(s.toUplink(msg))
	case "PRIVMSG", "NOTICE", "TAGMSG":
		if s.driver == nil {
			return fmt.Errorf("uplink not ready")
		}
		if cmd == "TAGMSG" && !s.HasUpCap("message-tags") {
			return nil
		}
		clientLabel, _ := msg.Tag("label")
		out := s.toUplink(msg)
		// Uplink will echo: remap client label so we can restore it for the sender only.
		if clientLabel != "" && s.HasUpCap("labeled-response") && s.HasUpCap("echo-message") {
			upLabel := s.registerSelfEcho(d.ID(), clientLabel)
			if out.Tags == nil {
				out.Tags = map[string]string{}
			}
			out.Tags["label"] = upLabel
			return s.WriteMessage(out)
		}
		if err := s.WriteMessage(out); err != nil {
			return err
		}
		// No server echo: fan out locally so every attached client sees the line.
		if !s.HasUpCap("echo-message") {
			s.echoSelfLocally(d, msg)
		}
		return nil
	case "MODE":
		if s.holdUntilRegistered(d, msg) {
			return nil
		}
		enquiry := isMODEEnquiryWith(msg.Params, s.isupport.Modes)
		if enquiry && s.skipDuplicateAfterHold(d, msg) {
			return nil
		}
		if s.driver == nil {
			return fmt.Errorf("uplink not ready")
		}
		if enquiry {
			return s.forwardSolicitous(d, msg)
		}
		return s.WriteMessage(s.toUplink(msg))
	case "TOPIC":
		if s.holdUntilRegistered(d, msg) {
			return nil
		}
		enquiry := isTOPICEnquiry(msg.Params)
		if enquiry && s.skipDuplicateAfterHold(d, msg) {
			return nil
		}
		if s.driver == nil {
			return fmt.Errorf("uplink not ready")
		}
		if enquiry {
			return s.forwardSolicitous(d, msg)
		}
		return s.WriteMessage(s.toUplink(msg))
	case "SILENCE":
		if s.holdUntilRegistered(d, msg) {
			return nil
		}
		enquiry := isSILENCEEnquiry(msg.Params)
		if enquiry && s.skipDuplicateAfterHold(d, msg) {
			return nil
		}
		if s.driver == nil {
			return fmt.Errorf("uplink not ready")
		}
		if enquiry {
			return s.forwardSolicitous(d, msg)
		}
		return s.WriteMessage(s.toUplink(msg))
	case "INVITE", "KICK":
		if s.driver == nil {
			return fmt.Errorf("uplink not ready")
		}
		return s.WriteMessage(s.toUplink(msg))
	case "NICK":
		if s.driver == nil {
			return fmt.Errorf("uplink not ready")
		}
		// Avoid racing the registration nick ladder / reclaim.
		if !s.Registered() {
			return nil
		}
		s.driver.StopNickRecovery(s.netID)
		return s.WriteMessage(s.toUplink(msg))
	default:
		if IsSolicitous(cmd) {
			if s.holdUntilRegistered(d, msg) {
				return nil
			}
			if s.skipDuplicateAfterHold(d, msg) {
				return nil
			}
			return s.forwardSolicitous(d, msg)
		}
		if s.driver == nil {
			return fmt.Errorf("uplink not ready")
		}
		return s.WriteMessage(s.toUplink(msg))
	}
}

const (
	maxHeldUntilReg  = 64
	heldFlushSentTTL = 5 * time.Second
)

func heldFingerprint(msg irc.Message) string {
	return strings.ToUpper(msg.Command) + " " + strings.Join(msg.Params, " ")
}

func (s *Session) isHoldableUntilReg(msg irc.Message) bool {
	cmd := strings.ToUpper(msg.Command)
	if IsSolicitous(cmd) {
		return true
	}
	switch cmd {
	case "MODE":
		return isMODEEnquiryWith(msg.Params, s.isupport.Modes)
	case "TOPIC":
		return isTOPICEnquiry(msg.Params)
	case "SILENCE":
		return isSILENCEEnquiry(msg.Params)
	default:
		return false
	}
}

// holdUntilRegistered queues solicitous/enquiry commands while the uplink is
// still registering (clients may already have a synthetic 001).
// Repeated identical commands replace the earlier hold (no queue duplicates).
func (s *Session) holdUntilRegistered(d Downlink, msg irc.Message) bool {
	if s.Registered() {
		return false
	}
	if !s.isHoldableUntilReg(msg) {
		return false
	}
	key := heldFingerprint(msg)
	s.mu.Lock()
	replaced := false
	for i := range s.heldUntilReg {
		if s.heldUntilReg[i].Client == d.ID() && heldFingerprint(s.heldUntilReg[i].Msg) == key {
			s.heldUntilReg[i].Msg = msg
			replaced = true
			break
		}
	}
	if !replaced {
		s.heldUntilReg = append(s.heldUntilReg, heldClientMsg{Client: d.ID(), Msg: msg})
		if len(s.heldUntilReg) > maxHeldUntilReg {
			s.heldUntilReg = s.heldUntilReg[len(s.heldUntilReg)-maxHeldUntilReg:]
		}
	}
	s.mu.Unlock()
	return true
}

// skipDuplicateAfterHold prefers a live post-001 re-send over a held flush.
// Returns true when this client message should not be forwarded (flush already did).
func (s *Session) skipDuplicateAfterHold(d Downlink, msg irc.Message) bool {
	if !s.isHoldableUntilReg(msg) {
		return false
	}
	key := heldFingerprint(msg)
	id := d.ID()
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	// Drop matching holds still waiting to flush.
	if len(s.heldUntilReg) > 0 {
		kept := s.heldUntilReg[:0]
		for _, h := range s.heldUntilReg {
			if h.Client == id && heldFingerprint(h.Msg) == key {
				continue
			}
			kept = append(kept, h)
		}
		s.heldUntilReg = kept
	}

	// Flush already forwarded this — drop the immediate client repeat.
	if m := s.heldFlushSent[id]; m != nil {
		if at, ok := m[key]; ok {
			delete(m, key)
			if len(m) == 0 {
				delete(s.heldFlushSent, id)
			}
			if now.Sub(at) <= heldFlushSentTTL {
				return true
			}
		}
	}

	// Cancel an in-flight flush of the same command; this live send wins.
	if s.heldFlushing {
		if s.heldFlushCancel == nil {
			s.heldFlushCancel = make(map[ClientID]map[string]struct{})
		}
		if s.heldFlushCancel[id] == nil {
			s.heldFlushCancel[id] = make(map[string]struct{})
		}
		s.heldFlushCancel[id][key] = struct{}{}
	}
	return false
}

func (s *Session) flushHeldAfterRegister() {
	s.mu.Lock()
	held := s.heldUntilReg
	s.heldUntilReg = nil
	s.heldFlushing = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.heldFlushing = false
		s.heldFlushCancel = nil
		s.mu.Unlock()
	}()

	for _, h := range held {
		key := heldFingerprint(h.Msg)
		s.mu.Lock()
		if m := s.heldFlushCancel[h.Client]; m != nil {
			if _, ok := m[key]; ok {
				delete(m, key)
				if len(m) == 0 {
					delete(s.heldFlushCancel, h.Client)
				}
				s.mu.Unlock()
				continue
			}
		}
		if s.heldFlushSent == nil {
			s.heldFlushSent = make(map[ClientID]map[string]time.Time)
		}
		if s.heldFlushSent[h.Client] == nil {
			s.heldFlushSent[h.Client] = make(map[string]time.Time)
		}
		s.heldFlushSent[h.Client][key] = time.Now()
		d, ok := s.downlinks[h.Client]
		s.mu.Unlock()
		if !ok {
			s.mu.Lock()
			if m := s.heldFlushSent[h.Client]; m != nil {
				delete(m, key)
				if len(m) == 0 {
					delete(s.heldFlushSent, h.Client)
				}
			}
			s.mu.Unlock()
			continue
		}
		_ = s.forwardHeldMessage(d, h.Msg)
	}
}

// forwardHeldMessage sends a previously held command without re-entering hold/dedup.
func (s *Session) forwardHeldMessage(d Downlink, msg irc.Message) error {
	cmd := strings.ToUpper(msg.Command)
	switch cmd {
	case "MODE", "TOPIC", "SILENCE":
		return s.forwardSolicitous(d, msg)
	default:
		if IsSolicitous(cmd) {
			return s.forwardSolicitous(d, msg)
		}
		if s.driver == nil {
			return fmt.Errorf("uplink not ready")
		}
		return s.WriteMessage(s.toUplink(msg))
	}
}

// toUplink strips client tags the uplink cannot handle (old ircds treat @tags as the command).
func (s *Session) toUplink(msg irc.Message) irc.Message {
	out := msg
	out.Tags = nil
	if s.driver == nil || !s.HasUpCap("message-tags") {
		return out
	}
	out.Tags = msg.CopyTags()
	if out.Tags != nil {
		// Uplink labels are owned by the bouncer, never forward the client's.
		delete(out.Tags, "label")
		if len(out.Tags) == 0 {
			out.Tags = nil
		}
	}
	return out
}

func (s *Session) forwardSolicitous(d Downlink, msg irc.Message) error {
	preferLabel := s.driver != nil && s.HasUpCap("labeled-response")
	preferWHOX := s.driver != nil && s.isupport != nil && s.isupport.WHOX
	cmd := strings.ToUpper(msg.Command)
	clientLabel, _ := msg.Tag("label")
	// WHOX tokens only when the client already used WHOX syntax — never upgrade plain
	// WHO (that would turn 352 replies into 354).
	clientWHOX := cmd == "WHO" && isWHOXParam(msg.Param(1))
	cm := s.isupport.CaseMapping
	var whoisTargets []string
	if cmd == "WHOIS" && !preferLabel {
		for _, n := range ParseWHOISTargets(msg.Params) {
			whoisTargets = append(whoisTargets, cm.Canonical(n))
		}
	}
	whoMask := ""
	if cmd == "WHO" || cmd == "WHOX" {
		whoMask = cm.Canonical(msg.Param(0))
	}
	statsLetter := ""
	if cmd == "STATS" && !preferLabel {
		statsLetter = ParseStatsLetter(msg.Params)
	}
	enquiryTarget := ""
	if cmd == "MODE" || cmd == "TOPIC" {
		enquiryTarget = cm.Canonical(msg.Param(0))
	}
	label, token, wait := s.tracker.Begin(BeginOpts{
		Client:        d.ID(),
		Cmd:           cmd,
		ClientLabel:   clientLabel,
		PreferLabel:   preferLabel,
		PreferWHOX:    preferWHOX && clientWHOX,
		WhoisTargets:  whoisTargets,
		WHOMask:       whoMask,
		StatsLetter:   statsLetter,
		EnquiryTarget: enquiryTarget,
	})
	out := s.toUplink(msg)
	if label != "" {
		if out.Tags == nil {
			out.Tags = map[string]string{}
		}
		out.Tags["label"] = label
	}
	if token != "" && cmd == "WHO" {
		spec, injectedT, clientTok := injectWHOXToken(msg.Param(1), token)
		s.tracker.SetWHOXClientFix(token, injectedT, clientTok)
		out = irc.Message{
			Tags:    out.Tags,
			Command: "WHO",
			Params:  []string{msg.Param(0), spec},
		}
	}
	if wait != nil {
		<-wait // hold until prior exchange's end-numeric
	}
	return s.WriteMessage(out)
}

// echoSelfLocally synthesizes a self PRIVMSG/NOTICE/TAGMSG when the uplink
// lacks echo-message. Only downlinks that negotiated echo-message receive it
// (clients without the cap already display their own sends locally).
// The originating client's label is preserved so labeled-response correlation works.
func (s *Session) echoSelfLocally(origin Downlink, msg irc.Message) {
	clientLabel, _ := msg.Tag("label")
	echo := irc.Message{
		Source:  s.SelfPrefix(),
		Command: strings.ToUpper(msg.Command),
		Params:  append([]string(nil), msg.Params...),
		Tags:    msg.CopyTags(),
	}
	if echo.Tags != nil {
		delete(echo.Tags, "label")
		if len(echo.Tags) == 0 {
			echo.Tags = nil
		}
	}
	echo = ensureMessageTime(echo)
	echo = ensureMessageID(echo)
	// keeperSeq=0: this echo is synthesized locally for clients when the
	// network doesn't support echo-message, not derived from any keeper
	// line — see maybeStoreHistory's own doc comment.
	s.maybeStoreHistory(echo, 0)

	// Snapshot then release before rewriteFor — see HandleMessage's own
	// comment on this pattern for why holding s.mu across a call that
	// nested-RLocks it again is a real deadlock hazard.
	s.mu.RLock()
	downlinks := make([]Downlink, 0, len(s.downlinks))
	for _, d := range s.downlinks {
		downlinks = append(downlinks, d)
	}
	s.mu.RUnlock()
	legacyHit := false
	for _, d := range downlinks {
		out := s.rewriteFor(d, echo)
		if out.Command == "" {
			continue
		}
		if d.ID() == origin.ID() && clientLabel != "" && d.HasCap("labeled-response") {
			if out.Tags == nil {
				out.Tags = map[string]string{}
			}
			out.Tags["label"] = clientLabel
		}
		_ = d.Send(out)
		if !hasChathistory(d) {
			legacyHit = true
		}
	}
	s.advanceLegacyPlaybackIfDelivered(echo, legacyHit)
}

func (s *Session) registerSelfEcho(client ClientID, clientLabel string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.selfEchoSeq++
	up := formatID("E", s.selfEchoSeq)
	if s.selfEcho == nil {
		s.selfEcho = make(map[string]pendingSelfEcho)
	}
	s.selfEcho[up] = pendingSelfEcho{Client: client, Label: clientLabel}
	return up
}

// consumeSelfEcho strips a tracked uplink self-echo label and returns the pending
// client mapping. msg is returned with the uplink label removed for history/fan-out.
func (s *Session) consumeSelfEcho(msg irc.Message) (pendingSelfEcho, irc.Message, bool) {
	lbl, ok := msg.Tag("label")
	if !ok || lbl == "" {
		return pendingSelfEcho{}, msg, false
	}
	cmd := strings.ToUpper(msg.Command)
	if cmd != "PRIVMSG" && cmd != "NOTICE" && cmd != "TAGMSG" && cmd != "ACK" {
		return pendingSelfEcho{}, msg, false
	}
	s.mu.Lock()
	p, ok := s.selfEcho[lbl]
	if ok {
		delete(s.selfEcho, lbl)
	}
	s.mu.Unlock()
	if !ok {
		return pendingSelfEcho{}, msg, false
	}
	out := msg
	out.Tags = msg.CopyTags()
	if out.Tags != nil {
		delete(out.Tags, "label")
		if len(out.Tags) == 0 {
			out.Tags = nil
		}
	}
	out.Raw = "" // tag prefix changed
	if cmd != "ACK" && !s.isSelfNick(msg.Nick()) {
		// Stale/mismatched label mapping; continue as a normal unlabeled line.
		return pendingSelfEcho{}, out, false
	}
	return p, out, true
}

func (s *Session) fanoutSelfEcho(msg irc.Message, pending pendingSelfEcho) {
	// Snapshot then release before rewriteFor — see HandleMessage's own
	// comment on this pattern for why holding s.mu across a call that
	// nested-RLocks it again is a real deadlock hazard.
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
		if d.ID() == pending.Client && pending.Label != "" && d.HasCap("labeled-response") {
			if out.Tags == nil {
				out.Tags = map[string]string{}
			}
			out.Tags["label"] = pending.Label
		}
		_ = d.Send(out)
		if legacyPlaybackCommands(msg) && !hasChathistory(d) {
			legacyHit = true
		}
	}
	s.advanceLegacyPlaybackIfDelivered(msg, legacyHit)
}

// isWHOXParam reports whether a WHO second argument uses WHOX (%fields) syntax.
func isWHOXParam(p string) bool {
	return strings.Contains(p, "%")
}

// isMODEEnquiry reports whether MODE expects numeric replies (view/list) rather
// than a mode change that should fan out as a MODE command to all clients.
// List queries include both "MODE #c b" and "MODE #c +b" (no mode arguments).
func isMODEEnquiry(params []string) bool {
	return isMODEEnquiryWith(params, nil)
}

func isMODEEnquiryWith(params []string, ms *irc.ModeSet) bool {
	if len(params) == 0 || params[0] == "" {
		return false
	}
	if len(params) == 1 {
		return true // MODE #chan | MODE nick
	}
	if len(params) > 2 {
		// Mode arguments present → set/unset (e.g. +b *!*@host), not a list query.
		return false
	}
	modes := params[1]
	if modes == "" {
		return true
	}
	// Bare list letters, or +/- list letters with no args: MODE #c b | MODE #c +b
	return isListModesOnly(modes, ms)
}

// isListModesOnly reports whether modestring is only +/− and type-A list mode letters.
func isListModesOnly(modes string, ms *irc.ModeSet) bool {
	saw := false
	for i := 0; i < len(modes); i++ {
		c := modes[i]
		if c == '+' || c == '-' {
			continue
		}
		if !isListModeChar(c, ms) {
			return false
		}
		saw = true
	}
	return saw
}

func isListModeChar(c byte, ms *irc.ModeSet) bool {
	if ms != nil {
		switch ms.Classify(c) {
		case irc.ModeList:
			return true
		case irc.ModeUnknown:
			// fall through to common defaults (e/I/q may be absent from ircu CHANMODES)
		default:
			return false
		}
	}
	switch c {
	case 'b', 'e', 'I', 'q':
		return true
	default:
		return false
	}
}

// isTOPICEnquiry reports TOPIC #chan (fetch) vs TOPIC #chan :text (set).
func isTOPICEnquiry(params []string) bool {
	return len(params) == 1 && params[0] != ""
}

// isSILENCEEnquiry reports a silence list query vs an add/remove update.
// ircu2: omitted/empty param or a nick → 271* + 272; +mask/-mask (or hostmasks) → change.
func isSILENCEEnquiry(params []string) bool {
	if len(params) == 0 || params[0] == "" {
		return true
	}
	for _, part := range strings.Split(params[0], ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if strings.HasPrefix(part, "+") || strings.HasPrefix(part, "-") {
			return false
		}
		if strings.ContainsAny(part, "!@") {
			return false
		}
	}
	return true
}

func (s *Session) failInvalidUTF8(d Downlink, cmd string) error {
	cmd = strings.ToUpper(cmd)
	if cmd == "" {
		cmd = "*"
	}
	return d.Send(irc.Message{
		Source:  ServerName,
		Command: "FAIL",
		Params:  []string{cmd, "INVALID_UTF8", "Message rejected, your IRC software MUST use UTF-8 encoding on this network"},
	})
}

// injectWHOXToken keeps the client's WHOX flags/fields intact, ensures querytype 't'
// is present (appended last before the comma), and sets our routing token.
// injectedT is true when we added 't' (354 querytype must be stripped for the client).
// clientTok is the token the client sent, if any (restored on 354 when !injectedT).
func injectWHOXToken(param, token string) (spec string, injectedT bool, clientTok string) {
	flags, fields, clientTok := parseWHOXParam(param)
	injectedT = !strings.Contains(strings.ToLower(fields), "t")
	if injectedT {
		// ircu: 't' must be last among the field letters, immediately before ,token.
		fields += "t"
	}
	return flags + "%" + fields + "," + token, injectedT, clientTok
}

// parseWHOXParam splits [flags]%[fields][,token] (ircu / IRCv3 WHOX).
func parseWHOXParam(p string) (flags, fields, token string) {
	p = strings.TrimSpace(p)
	if p == "" {
		return "", "", ""
	}
	pct := strings.Index(p, "%")
	if pct < 0 {
		return p, "", ""
	}
	flags = p[:pct]
	rest := p[pct+1:]
	if i := strings.Index(rest, ","); i >= 0 {
		fields = rest[:i]
		token = rest[i+1:]
	} else {
		fields = rest
	}
	return flags, fields, token
}

// rememberJoinKeys records channel keys from a client JOIN until the uplink
// echoes our self-JOIN (refused joins must not be persisted for auto-rejoin).
func (s *Session) rememberJoinKeys(msg irc.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingJoinKeys == nil {
		s.pendingJoinKeys = make(map[string]string)
	}
	for _, jk := range ParseJoin(msg.Param(0), msg.Param(1)) {
		ck := s.isupport.CaseMapping.Canonical(jk.Name)
		s.pendingJoinKeys[ck] = jk.Key
	}
}

// JoinTarget is one channel from a JOIN command, with its optional key.
type JoinTarget struct {
	Name string
	Key  string
}

// ParseJoin splits IRC multi-JOIN syntax.
// Channels and keys are comma-separated and paired by index; extra channels are key-less.
func ParseJoin(channels, keys string) []JoinTarget {
	if channels == "" {
		return nil
	}
	chans := strings.Split(channels, ",")
	var keyList []string
	if keys != "" {
		keyList = strings.Split(keys, ",")
	}
	out := make([]JoinTarget, 0, len(chans))
	for i, name := range chans {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		jt := JoinTarget{Name: name}
		if i < len(keyList) {
			jt.Key = keyList[i]
		}
		out = append(out, jt)
	}
	return out
}

func (s *Session) persistChannel(name, key string) {
	if s.store == nil || s.Network.ID == 0 {
		return
	}
	if err := s.store.AddChannel(context.Background(), s.Network.ID, name, key); err != nil {
		s.log.Error("persist channel", "channel", name, "err", err)
	}
	s.syncUplinkChannels()
}

func (s *Session) persistRemoveChannel(name string) {
	if s.store == nil || s.Network.ID == 0 {
		return
	}
	if err := s.store.RemoveChannel(context.Background(), s.Network.ID, name); err != nil {
		s.log.Error("remove channel", "channel", name, "err", err)
	}
	s.syncUplinkChannels()
}

func (s *Session) syncUplinkChannels() {
	if s.driver == nil || s.store == nil || s.Network.ID == 0 {
		return
	}
	chs, err := s.store.ListChannels(context.Background(), s.Network.ID)
	if err != nil {
		return
	}
	joins := make([]brain.ChannelJoin, 0, len(chs))
	for _, ch := range chs {
		joins = append(joins, brain.ChannelJoin{Name: ch.Name, Key: ch.Key})
	}
	s.driver.SetChannels(s.netID, joins)
}
