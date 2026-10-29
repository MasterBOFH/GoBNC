package session

import (
	"context"
	"errors"
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

// IsChannel reports whether name is a channel on this network (ISUPPORT CHANTYPES).
func (s *Session) IsChannel(name string) bool {
	s.mu.RLock()
	chantypes := s.isupport.Raw["CHANTYPES"]
	s.mu.RUnlock()
	return isChannelTarget(name, chantypes)
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

// OpenBufferForDevice opens buffer on one device (a user action) and emits buffer_open reason
// "user".
func (s *Session) OpenBufferForDevice(clientID, buffer string) error {
	if s.store == nil || s.Network.ID == 0 {
		return errNoStore
	}
	key := s.Fold(buffer)
	s.busMu.Lock()
	defer s.busMu.Unlock()
	if err := s.store.SetBufferOpen(context.Background(), clientID, s.Network.ID, key, buffer, true, 0); err != nil {
		return err
	}
	s.emitLocked(wire.BufferOpenEvent{Network: s.Network.ID, ClientID: clientID, Buffer: buffer, Key: key, Reason: wire.OpenUser})
	return nil
}

// CloseBufferForDevice closes buffer on one device: its floor becomes the buffer's newest
// message id (what is unread now stays dismissed there) and buffer_close is emitted. It returns
// that floor.
func (s *Session) CloseBufferForDevice(clientID, buffer string) (int64, error) {
	if s.store == nil || s.Network.ID == 0 {
		return 0, errNoStore
	}
	ctx := context.Background()
	key := s.Fold(buffer)
	s.busMu.Lock()
	defer s.busMu.Unlock()
	latest, err := s.store.MessagesLatest(ctx, s.Network.ID, key, 1)
	if err != nil {
		return 0, err
	}
	var closedAt int64
	if len(latest) == 1 {
		closedAt = latest[0].ID
	}
	if err := s.store.SetBufferOpen(ctx, clientID, s.Network.ID, key, buffer, false, closedAt); err != nil {
		return 0, err
	}
	if closedAt, err = s.store.BufferClosedAt(ctx, clientID, s.Network.ID, key); err != nil {
		return 0, err
	}
	s.emitLocked(wire.BufferCloseEvent{Network: s.Network.ID, ClientID: clientID, Buffer: buffer, Key: key, ClosedAtID: closedAt})
	return closedAt, nil
}

var errNoStore = errors.New("session: no store")
