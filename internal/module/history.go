package module

import (
	"context"
	"encoding/json"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

func init() {
	registerMethod(wire.MethodHistoryMessages, handleHistoryMessages, wire.FeatureHistory)
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

func historyMessage(m store.Message, buffer string) wire.HistoryMessage {
	return wire.HistoryMessage{ID: m.ID, Time: m.Time.UTC().Format(time.RFC3339Nano), MsgID: m.MsgID,
		Command: m.Command, Source: m.Source, Buffer: buffer, Raw: m.Raw, Text: m.Text}
}
