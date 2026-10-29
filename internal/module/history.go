package module

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

func init() {
	registerMethod(wire.MethodHistoryMessages, handleHistoryMessages, wire.FeatureHistory)
	registerMethod(wire.MethodHistoryActivity, handleHistoryActivity, wire.FeatureHistory)
}

const (
	defaultHistoryLimit = 50
	maxHistoryLimit     = 500
)

// handleHistoryMessages pages one buffer's history by message id: before_id, after_id,
// around_id, anchor (first_unread / latest) or, with none of them, the latest page.
func handleHistoryMessages(ctx context.Context, c *Conn, params json.RawMessage) (any, error) {
	p, err := decodeParams[wire.HistoryMessagesParams](params)
	if err != nil {
		return nil, err
	}
	selectors := 0
	for _, set := range []bool{p.BeforeID != nil, p.AfterID != nil, p.AroundID != nil, p.Anchor != ""} {
		if set {
			selectors++
		}
	}
	if selectors > 1 {
		return nil, wire.Errorf(wire.CodeBadParams, "at most one of before_id, after_id, around_id, anchor")
	}
	if p.Limit == 0 {
		p.Limit = defaultHistoryLimit
	}
	if p.Limit < 1 || p.Limit > maxHistoryLimit {
		return nil, wire.Errorf(wire.CodeBadParams, "limit must be 1..%d", maxHistoryLimit)
	}
	if p.Buffer == "" {
		return nil, wire.Errorf(wire.CodeBadParams, "buffer is required")
	}
	switch p.Anchor {
	case "", wire.AnchorLatest:
	case wire.AnchorFirstUnread:
		if p.Device == "" {
			return nil, wire.Errorf(wire.CodeBadParams, "anchor first_unread needs device")
		}
	default:
		return nil, wire.Errorf(wire.CodeBadParams, "unknown anchor %q", p.Anchor)
	}
	sess, err := c.srv.session(p.Network)
	if err != nil {
		return nil, err
	}
	db := c.srv.backend.Store()
	if db == nil {
		return nil, wire.Errorf(wire.CodeInternal, "no history store")
	}
	key := sess.Fold(p.Buffer)
	var msgs []store.Message
	switch {
	case p.BeforeID != nil:
		msgs, err = db.MessagesBefore(ctx, p.Network, key, *p.BeforeID, p.Limit)
	case p.AfterID != nil:
		msgs, err = db.MessagesAfter(ctx, p.Network, key, *p.AfterID, p.Limit)
	case p.AroundID != nil:
		msgs, err = db.MessagesAround(ctx, p.Network, key, *p.AroundID, p.Limit)
	case p.Anchor == wire.AnchorFirstUnread:
		msgs, err = firstUnreadPage(ctx, c, db, p, key)
	default:
		msgs, err = db.MessagesLatest(ctx, p.Network, key, p.Limit)
	}
	if err != nil {
		return nil, err
	}
	res := wire.HistoryMessagesResult{Messages: make([]wire.HistoryMessage, 0, len(msgs))}
	for _, m := range msgs {
		res.Messages = append(res.Messages, historyMessage(m, p.Buffer))
	}
	if len(msgs) > 0 {
		if res.HasBefore, err = db.HasMessageBefore(ctx, p.Network, key, msgs[0].ID); err != nil {
			return nil, err
		}
		if res.HasAfter, err = db.HasMessageAfter(ctx, p.Network, key, msgs[len(msgs)-1].ID); err != nil {
			return nil, err
		}
	}
	return res, nil
}

// firstUnreadPage starts at the first line after the shared read marker and the device's
// closed-at floor; with nothing unread it is the latest page.
func firstUnreadPage(ctx context.Context, c *Conn, db *store.Store, p wire.HistoryMessagesParams, key string) ([]store.Message, error) {
	var marker time.Time
	if ts, ok, err := db.GetReadMarker(ctx, p.Network, key); err != nil {
		return nil, err
	} else if ok {
		marker, _ = time.Parse(time.RFC3339Nano, ts)
	}
	floor, err := c.srv.closedAtFloor(ctx, p.Device, p.Network, key)
	if err != nil {
		return nil, err
	}
	first, err := db.FirstUnreadID(ctx, p.Network, key, marker, floor)
	if err != nil {
		return nil, err
	}
	if first == 0 {
		return db.MessagesLatest(ctx, p.Network, key, p.Limit)
	}
	return db.MessagesAfter(ctx, p.Network, key, first-1, p.Limit)
}

// closedAtFloor is the device's closed_at_id for a buffer: nothing at or below it counts as
// unread on that device. Device state arrives with the devices feature; until then none.
func (s *Server) closedAtFloor(ctx context.Context, device string, network int64, key string) (int64, error) {
	return 0, nil
}

// handleHistoryActivity reports, for every buffer the device shows (joined channels, its open
// queries, Server), the newest message and the unread / highlight counts.
func handleHistoryActivity(ctx context.Context, c *Conn, params json.RawMessage) (any, error) {
	p, err := decodeParams[wire.HistoryActivityParams](params)
	if err != nil {
		return nil, err
	}
	if p.Device == "" {
		return nil, wire.Errorf(wire.CodeBadParams, "device is required")
	}
	sess, err := c.srv.session(p.Network)
	if err != nil {
		return nil, err
	}
	db := c.srv.backend.Store()
	if db == nil {
		return nil, wire.Errorf(wire.CodeInternal, "no history store")
	}
	queries, err := c.srv.openQueries(ctx, p.Device, p.Network)
	if err != nil {
		return nil, err
	}
	cm := sess.CaseMapping()
	self := sess.Nick()
	selfKey := cm.Canonical(self)
	type shown struct {
		name  string
		query bool
	}
	var buffers []shown
	seen := map[string]bool{}
	add := func(name string, query bool) {
		if k := cm.Canonical(name); !seen[k] {
			seen[k] = true
			buffers = append(buffers, shown{name, query})
		}
	}
	for _, ch := range sess.JoinedChannels() {
		add(ch, false)
	}
	for _, q := range queries {
		add(q, true)
	}
	add(wire.ServerBuffer, false)

	res := wire.HistoryActivityResult{Buffers: make([]wire.BufferActivity, 0, len(buffers))}
	for _, b := range buffers {
		key := cm.Canonical(b.name)
		var marker time.Time
		if ts, ok, err := db.GetReadMarker(ctx, p.Network, key); err != nil {
			return nil, err
		} else if ok {
			marker, _ = time.Parse(time.RFC3339Nano, ts)
		}
		floor, err := c.srv.closedAtFloor(ctx, p.Device, p.Network, key)
		if err != nil {
			return nil, err
		}
		a, err := db.BufferActivity(ctx, p.Network, key, marker, floor)
		if err != nil {
			return nil, err
		}
		ba := wire.BufferActivity{Buffer: b.name, Key: key, LastID: a.LastID}
		if a.LastID != 0 {
			ba.LastTime = a.LastTime.UTC().Format(time.RFC3339Nano)
		}
		for _, l := range a.Unread {
			if cm.Canonical(sourceNick(l.Source)) == selfKey {
				continue // our own lines are never unread
			}
			ba.Unread++
			if b.query || session.IsHighlight(l.Text, self, cm) {
				ba.Highlights++
			}
		}
		res.Buffers = append(res.Buffers, ba)
	}
	return res, nil
}

func sourceNick(src string) string {
	if i := strings.IndexByte(src, '!'); i >= 0 {
		return src[:i]
	}
	return src
}

// openQueries lists the device's open query buffers on a network. Device state arrives with
// the devices feature; until then none.
func (s *Server) openQueries(ctx context.Context, device string, network int64) ([]string, error) {
	return nil, nil
}

func historyMessage(m store.Message, buffer string) wire.HistoryMessage {
	return wire.HistoryMessage{ID: m.ID, Time: m.Time.UTC().Format(time.RFC3339Nano), MsgID: m.MsgID,
		Command: m.Command, Source: m.Source, Buffer: buffer, Raw: m.Raw, Text: m.Text}
}
