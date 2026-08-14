package session

import (
	"sort"
	"strings"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	gobnclog "github.com/MasterBOFH/GoBNC/internal/log"
)

// sessionDebugTarget is one client's live /bnc debug subscription —
// implements gobnclog.DebugTarget, called only from
// gobnclog.DebugRegistry.Run (a goroutine internal/server.Server.Run
// starts), never synchronously from whatever logged the record. Looks up
// the downlink fresh on every delivery rather than capturing it at
// subscribe time: the client may have been replaced (reconnect) or
// detached since.
type sessionDebugTarget struct {
	sess *Session
	id   ClientID
}

func (t *sessionDebugTarget) DeliverRaw(dir, line string) {
	t.deliver(dir + " " + line)
}

func (t *sessionDebugTarget) DeliverLog(level, msg string, attrs map[string]string) {
	var b strings.Builder
	b.WriteByte('[')
	b.WriteString(level)
	b.WriteString("] ")
	b.WriteString(msg)
	if len(attrs) > 0 {
		keys := make([]string, 0, len(attrs))
		for k := range attrs {
			keys = append(keys, k)
		}
		sort.Strings(keys) // deterministic order — map iteration isn't
		for _, k := range keys {
			b.WriteByte(' ')
			b.WriteString(k)
			b.WriteByte('=')
			b.WriteString(attrs[k])
		}
	}
	t.deliver(b.String())
}

// deliver sends text as a PRIVMSG from the pseudo-user ">debug" to the
// client's own current nick — the target has to be the client's own nick
// for any client to render an incoming PRIVMSG at all; the fake nick-style
// source is what makes clients open a separate query/DM window for it,
// keeping debug output out of real conversations.
func (t *sessionDebugTarget) deliver(text string) {
	t.sess.mu.RLock()
	d, ok := t.sess.downlinks[t.id]
	nick := ""
	if t.sess.self != nil {
		nick = t.sess.self.Nick
	}
	t.sess.mu.RUnlock()
	if !ok {
		return
	}
	if nick == "" {
		nick = "*"
	}
	_ = d.Send(irc.Message{
		Source:  DebugSource,
		Command: "PRIVMSG",
		Params:  []string{nick, text},
	})
}

// handleBNCDebug implements /bnc debug raw|log|all|off for the requesting
// client only (not broadcast to every client attached to this network) —
// intercepted in handleBNC before falling through to s.admin, since it
// needs the live Session and the specific requesting Downlink, neither of
// which internal/admin's stateless command set has access to.
func (s *Session) handleBNCDebug(d Downlink, args []string) error {
	nick := s.Nick()
	if nick == "" {
		nick = "*"
	}
	sendNotice := func(text string) error {
		return d.Send(irc.Message{Source: ServerName, Command: "NOTICE", Params: []string{nick, text}})
	}
	if s.debugRegistry == nil {
		return sendNotice("debug unavailable")
	}
	if len(args) != 1 {
		return sendNotice("usage: /bnc debug raw|log|all|off")
	}
	switch strings.ToLower(args[0]) {
	case "raw":
		s.subscribeDebug(d, gobnclog.DebugRaw)
		return sendNotice("debug: raw traffic enabled (this network only)")
	case "log":
		s.subscribeDebug(d, gobnclog.DebugLog)
		return sendNotice("debug: log output enabled (this network only)")
	case "all":
		s.subscribeDebug(d, gobnclog.DebugAll)
		return sendNotice("debug: raw traffic + log output enabled (this network only)")
	case "off":
		s.unsubscribeDebug(d.ID())
		return sendNotice("debug: disabled")
	default:
		return sendNotice("usage: /bnc debug raw|log|all|off")
	}
}

func (s *Session) subscribeDebug(d Downlink, mode gobnclog.DebugMode) {
	id := d.ID()
	s.mu.Lock()
	target, ok := s.debugTargets[id]
	if !ok {
		target = &sessionDebugTarget{sess: s, id: id}
		s.debugTargets[id] = target
	}
	s.mu.Unlock()
	s.debugRegistry.Subscribe(s.Network.Name, target, mode)
}

// unsubscribeDebug is safe to call even when id was never subscribed
// (handles both /bnc debug off with no active subscription and Detach
// cleanup for every departing client, not just ones that used /bnc debug).
// Caller must not hold s.mu.
func (s *Session) unsubscribeDebug(id ClientID) {
	s.mu.Lock()
	target, ok := s.debugTargets[id]
	if ok {
		delete(s.debugTargets, id)
	}
	s.mu.Unlock()
	if ok && s.debugRegistry != nil {
		s.debugRegistry.Unsubscribe(s.Network.Name, target)
	}
}
