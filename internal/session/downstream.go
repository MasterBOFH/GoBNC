package session

import (
	"context"
	"fmt"
	"strings"

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
	cmd := strings.ToUpper(msg.Command)
	switch cmd {
	case "PING":
		return d.Send(irc.Message{Command: "PONG", Params: msg.Params})
	case "PONG":
		// Reply to bouncer keepalive (or client stray PONG) — never forward upstream.
		return nil
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
		s.persistClientJoin(msg)
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
		if err := s.uplink.WriteMessage(s.toUplink(msg)); err != nil {
			return err
		}
		// No server echo: fan out locally so every attached client sees the line.
		if !s.uplink.HasCap("echo-message") {
			s.echoSelfLocally(msg)
		}
		return nil
	case "MODE", "TOPIC", "INVITE", "KICK", "NICK":
		if s.uplink == nil {
			return fmt.Errorf("uplink not ready")
		}
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
	label, token, wait := s.tracker.Begin(BeginOpts{
		Client:       d.ID(),
		Cmd:          cmd,
		ClientLabel:  clientLabel,
		PreferLabel:  preferLabel,
		PreferWHOX:   preferWHOX && clientWHOX,
		WhoisTargets: whoisTargets,
		WHOMask:      whoMask,
		StatsLetter:  statsLetter,
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
func (s *Session) echoSelfLocally(msg irc.Message) {
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
		_ = d.Send(out)
		if !hasChathistory(d) {
			legacyHit = true
		}
	}
	s.mu.RUnlock()
	s.advanceLegacyPlaybackIfDelivered(echo, legacyHit)
}

// isWHOXParam reports whether a WHO second argument uses WHOX (%fields) syntax.
func isWHOXParam(p string) bool {
	return strings.Contains(p, "%")
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


// persistClientJoin stores channels (+ keys) from a client JOIN before uplink forward.
func (s *Session) persistClientJoin(msg irc.Message) {
	for _, jk := range ParseJoin(msg.Param(0), msg.Param(1)) {
		s.mu.Lock()
		ck := s.isupport.CaseMapping.Canonical(jk.Name)
		ch := s.channels[ck]
		if ch == nil {
			ch = &ChannelState{Name: jk.Name, Modes: irc.NewChannelModes(), Members: map[string]struct{}{}}
			s.channels[ck] = ch
		}
		ch.Key = jk.Key
		s.mu.Unlock()
		s.persistChannel(jk.Name, jk.Key)
	}
	s.syncUplinkChannels()
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
