package session

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

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
	s.mu.Unlock()
	s.tracker.DropClient(id)
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
	if s.isupport != nil && s.isupport.UTF8Only && !utf8.ValidString(msg.Encode()) {
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
		if s.uplink == nil {
			return fmt.Errorf("uplink not ready")
		}
		return s.uplink.WriteMessage(s.toUplink(msg))
	case "PART":
		// Removal from DB happens when uplink echoes our PART (applyState).
		if s.uplink == nil {
			return fmt.Errorf("uplink not ready")
		}
		return s.uplink.WriteMessage(s.toUplink(msg))
	case "PRIVMSG", "NOTICE", "TAGMSG":
		if s.uplink == nil {
			return fmt.Errorf("uplink not ready")
		}
		if cmd == "TAGMSG" && !s.uplink.HasCap("message-tags") {
			return nil
		}
		clientLabel, _ := msg.Tag("label")
		out := s.toUplink(msg)
		// Uplink will echo: remap client label so we can restore it for the sender only.
		if clientLabel != "" && s.uplink.HasCap("labeled-response") && s.uplink.HasCap("echo-message") {
			upLabel := s.registerSelfEcho(d.ID(), clientLabel)
			if out.Tags == nil {
				out.Tags = map[string]string{}
			}
			out.Tags["label"] = upLabel
			return s.uplink.WriteMessage(out)
		}
		if err := s.uplink.WriteMessage(out); err != nil {
			return err
		}
		// No server echo: fan out locally so every attached client sees the line.
		if !s.uplink.HasCap("echo-message") {
			s.echoSelfLocally(d, msg)
		}
		return nil
	case "MODE":
		if s.uplink == nil {
			return fmt.Errorf("uplink not ready")
		}
		if isMODEEnquiryWith(msg.Params, s.isupport.Modes) {
			return s.forwardSolicitous(d, msg)
		}
		return s.uplink.WriteMessage(s.toUplink(msg))
	case "TOPIC":
		if s.uplink == nil {
			return fmt.Errorf("uplink not ready")
		}
		if isTOPICEnquiry(msg.Params) {
			return s.forwardSolicitous(d, msg)
		}
		return s.uplink.WriteMessage(s.toUplink(msg))
	case "SILENCE":
		if s.uplink == nil {
			return fmt.Errorf("uplink not ready")
		}
		if isSILENCEEnquiry(msg.Params) {
			return s.forwardSolicitous(d, msg)
		}
		return s.uplink.WriteMessage(s.toUplink(msg))
	case "INVITE", "KICK":
		if s.uplink == nil {
			return fmt.Errorf("uplink not ready")
		}
		return s.uplink.WriteMessage(s.toUplink(msg))
	case "NICK":
		if s.uplink == nil {
			return fmt.Errorf("uplink not ready")
		}
		// Avoid racing the registration nick ladder / reclaim.
		if !s.uplink.Registered() {
			return nil
		}
		s.uplink.StopNickRecovery()
		return s.uplink.WriteMessage(s.toUplink(msg))
	default:
		if IsSolicitous(cmd) {
			return s.forwardSolicitous(d, msg)
		}
		if s.uplink == nil {
			return fmt.Errorf("uplink not ready")
		}
		return s.uplink.WriteMessage(s.toUplink(msg))
	}
}

// toUplink strips client tags the uplink cannot handle (old ircds treat @tags as the command).
func (s *Session) toUplink(msg irc.Message) irc.Message {
	out := msg
	out.Tags = nil
	if s.uplink == nil || !s.uplink.HasCap("message-tags") {
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
	preferLabel := s.uplink != nil && s.uplink.HasCap("labeled-response")
	preferWHOX := s.uplink != nil && s.uplink.ISUPPORT() != nil && s.uplink.ISUPPORT().WHOX
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
	return s.uplink.WriteMessage(out)
}

// echoSelfLocally synthesizes a self PRIVMSG/NOTICE/TAGMSG when the uplink
// lacks echo-message, and delivers it to every downlink (multi-client sync).
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
	s.maybeStoreHistory(echo)

	s.mu.RLock()
	legacyHit := false
	for _, d := range s.downlinks {
		out := s.rewriteMessage(d, echo, true)
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
	s.mu.RUnlock()
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
	s.mu.RLock()
	legacyHit := false
	for _, d := range s.downlinks {
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
	s.mu.RUnlock()
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
	if s.uplink == nil || s.store == nil || s.Network.ID == 0 {
		return
	}
	chs, err := s.store.ListChannels(context.Background(), s.Network.ID)
	if err != nil {
		return
	}
	s.uplink.SetChannels(chs)
}
