package session

import (
	"context"
	"strings"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// opensQuery reports whether msg, stored under a query target, opens that query on every
// device: a PRIVMSG from or to a user (our own outgoing lines count), CTCP ACTION included,
// other CTCP requests, server-sourced lines and $mask targets not — matching ReteCloud's rule
// (notices never open a query).
func opensQuery(msg irc.Message) bool {
	if msg.Command != "PRIVMSG" || serverSourced(msg) || strings.HasPrefix(msg.Param(0), "$") {
		return false
	}
	if verb, _, ok := parseCTCP(msg.Trailing()); ok && !strings.EqualFold(verb, "ACTION") {
		return false
	}
	return true
}

// reopenQueryLocked applies the device rule for a line stored with id under query target: the
// query opens on every device where it is not open, and each such device's buffer_open (reason
// "message") is emitted now — before the line's own event. Caller holds busMu.
func (s *Session) reopenQueryLocked(msg irc.Message, target string, id int64) {
	if s.store == nil || s.Network.ID == 0 || id == 0 || !opensQuery(msg) {
		return
	}
	s.mu.RLock()
	chantypes, statusmsg := s.isupport.Raw["CHANTYPES"], s.isupport.Raw["STATUSMSG"]
	s.mu.RUnlock()
	isChan := func(n string) bool { return isChannelTarget(n, chantypes) }
	if isChan(stripStatusMsg(msg.Param(0), statusmsg, isChan)) {
		return // a channel line
	}
	display := msg.Nick()
	if s.isSelfNick(display) {
		display = msg.Param(0) // our own line: the query is the target's
	}
	changed, err := s.store.ReopenForMessage(context.Background(), s.Network.ID, target, display, id)
	if err != nil {
		s.log.Warn("reopen query for devices", "target", target, "err", err)
		return
	}
	for _, c := range changed {
		s.emitLocked(wire.BufferOpenEvent{Network: s.Network.ID, ClientID: c, Buffer: display, Key: target, Reason: wire.OpenMessage})
	}
}

// SeedDevice gives a device its first open set on this network: when it has no buffer rows
// here yet, every query with a conversation line after the shared read marker (not a CTCP
// request, not server traffic) is opened and announced with buffer_open reason "seed". It
// returns the buffers opened (none when the device already had rows).
func (s *Session) SeedDevice(clientID string) ([]string, error) {
	if s.store == nil || s.Network.ID == 0 {
		return nil, nil
	}
	ctx := context.Background()
	s.busMu.Lock()
	defer s.busMu.Unlock()
	has, err := s.store.DeviceHasBuffers(ctx, clientID, s.Network.ID)
	if err != nil || has {
		return nil, err
	}
	targets, err := s.store.DistinctTargets(ctx, s.Network.ID)
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	chantypes := s.isupport.Raw["CHANTYPES"]
	s.mu.RUnlock()
	var seeded []string
	for _, target := range targets {
		if target == s.Fold(wire.ServerBuffer) || isChannelTarget(target, chantypes) {
			continue
		}
		var marker time.Time
		if ts, ok, err := s.store.GetReadMarker(ctx, s.Network.ID, target); err != nil {
			return seeded, err
		} else if ok {
			marker, _ = parseMarkTime(ts)
		}
		a, err := s.store.BufferActivity(ctx, s.Network.ID, target, marker, 0)
		if err != nil {
			return seeded, err
		}
		display := ""
		for _, l := range a.Unread {
			if !strings.Contains(l.Source, "!") {
				continue // server-sourced
			}
			if nick := sourceNickOf(l.Source); !s.isSelfNick(nick) {
				display = nick
			} else if display == "" {
				display = target
			}
		}
		if display == "" {
			continue
		}
		if err := s.store.SetBufferOpen(ctx, clientID, s.Network.ID, target, display, true, 0); err != nil {
			return seeded, err
		}
		seeded = append(seeded, display)
		s.emitLocked(wire.BufferOpenEvent{Network: s.Network.ID, ClientID: clientID, Buffer: display, Key: target, Reason: wire.OpenSeed})
	}
	return seeded, nil
}

func isChannelTarget(name, chantypes string) bool {
	if name == "" {
		return false
	}
	if chantypes != "" {
		return strings.IndexByte(chantypes, name[0]) >= 0
	}
	return isChannelName(name)
}

func sourceNickOf(src string) string {
	if i := strings.IndexByte(src, '!'); i >= 0 {
		return src[:i]
	}
	return src
}
