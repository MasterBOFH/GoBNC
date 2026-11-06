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

// SeedDevice gives a device its first open set on this network, once (DeviceSeeded): every
// query with an unread PRIVMSG from the peer after the shared read marker (not a CTCP request,
// not server traffic — the live rule's lines) is opened and announced with buffer_open reason
// "seed". It returns the buffers opened.
func (s *Session) SeedDevice(clientID string) ([]string, error) {
	if s.store == nil || s.Network.ID == 0 {
		return nil, nil
	}
	ctx := context.Background()
	s.busMu.Lock()
	defer s.busMu.Unlock()
	seeded, err := s.store.DeviceSeeded(ctx, clientID, s.Network.ID)
	if err != nil || seeded {
		return nil, err
	}
	targets, err := s.store.DistinctTargets(ctx, s.Network.ID)
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	chantypes, statusmsg := s.isupport.Raw["CHANTYPES"], s.isupport.Raw["STATUSMSG"]
	s.mu.RUnlock()
	var seededBufs []string
	for _, target := range targets {
		if target == "" || target == s.Fold(wire.ServerBuffer) || target[0] == '$' ||
			strings.IndexByte(statusmsg, target[0]) >= 0 || isChannelTarget(target, chantypes) {
			continue
		}
		var marker time.Time
		if ts, ok, err := s.store.GetReadMarker(ctx, s.Network.ID, target); err != nil {
			return seededBufs, err
		} else if ok {
			marker, _ = parseMarkTime(ts)
		}
		a, err := s.store.BufferActivity(ctx, s.Network.ID, target, marker, 0)
		if err != nil {
			return seededBufs, err
		}
		display := ""
		for _, l := range a.Unread { // newest first: the peer's current spelling
			if l.Command != "PRIVMSG" || !strings.Contains(l.Source, "!") {
				continue
			}
			if verb, _, ok := parseCTCP(l.Text); ok && !strings.EqualFold(verb, "ACTION") {
				continue
			}
			if nick := sourceNickOf(l.Source); !s.isSelfNick(nick) {
				display = nick
				break
			}
		}
		if display == "" {
			continue
		}
		if err := s.store.SetBufferOpen(ctx, clientID, s.Network.ID, target, display, true, 0); err != nil {
			return seededBufs, err
		}
		seededBufs = append(seededBufs, display)
		s.emitLocked(wire.BufferOpenEvent{Network: s.Network.ID, ClientID: clientID, Buffer: display, Key: target, Reason: wire.OpenSeed})
	}
	return seededBufs, s.store.MarkDeviceSeeded(ctx, clientID, s.Network.ID)
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

// queryExists reports whether we have a query with the folded nick: stored lines under it, or a
// device row (open or closed) for it.
func (s *Session) queryExists(key string) bool {
	if s.store == nil || s.Network.ID == 0 {
		return false
	}
	ctx := context.Background()
	if ok, err := s.store.HasTarget(ctx, s.Network.ID, key); err == nil && ok {
		return true
	}
	ok, err := s.store.HasBufferRows(ctx, s.Network.ID, key)
	return err == nil && ok
}

// queryKeptOld reports whether some device holds queries for both nicks: a peer's nick change
// keeps both there (owner ruling), so the old query's history gets the change too.
func (s *Session) queryKeptOld(fromKey, toKey string) bool {
	if s.store == nil || s.Network.ID == 0 {
		return false
	}
	ok, err := s.store.DeviceHoldsBoth(context.Background(), s.Network.ID, fromKey, toKey)
	return err == nil && ok
}

// renameQueryLocked follows a peer's NICK in every device's buffer rows (store.RenameBuffer:
// open/closed and the floor kept; a device that already has the new nick's row keeps both) and
// emits buffer_rename for each device whose rows were renamed — before the line's own event. Our
// own NICK renames nothing. Call before applyState, holding busMu.
func (s *Session) renameQueryLocked(msg irc.Message) {
	if msg.Command != "NICK" || s.store == nil || s.Network.ID == 0 {
		return
	}
	from, to := msg.Nick(), msg.Param(0)
	if from == "" || to == "" || s.isSelfNick(from) {
		return
	}
	fromKey, toKey := s.Fold(from), s.Fold(to)
	changed, _, err := s.store.RenameBuffer(context.Background(), s.Network.ID, fromKey, toKey, to)
	if err != nil {
		s.log.Warn("rename query for devices", "from", fromKey, "to", toKey, "err", err)
		return
	}
	for _, c := range changed {
		s.emitLocked(wire.BufferRenameEvent{Network: s.Network.ID, ClientID: c, From: from, FromKey: fromKey, To: to, ToKey: toKey})
	}
}

var errNoStore = errors.New("session: no store")
