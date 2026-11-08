package module

import (
	"context"
	"encoding/json"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

func init() {
	registerMethod(wire.MethodReadMarkersGet, handleReadMarkersGet, wire.FeatureReadMarkers)
	registerMethod(wire.MethodReadMarkersSet, handleReadMarkersSet, wire.FeatureReadMarkers)
}

// handleReadMarkersGet returns a buffer's shared read marker: its time and the newest message
// id at or before it (0 / "" when unset), from the store for a stopped network too.
func handleReadMarkersGet(ctx context.Context, c *Conn, params json.RawMessage) (any, error) {
	p, err := decodeParams[wire.ReadMarkerGetParams](params)
	if err != nil {
		return nil, err
	}
	sess, err := c.srv.view(ctx, p.Network)
	if err != nil {
		return nil, err
	}
	db := c.srv.backend.Store()
	if db == nil {
		return nil, wire.Errorf(wire.CodeInternal, "no store")
	}
	key := wire.BufferKey(sess.Fold, p.Buffer)
	ts, ok, err := db.GetReadMarker(ctx, p.Network, key)
	if err != nil || !ok {
		return wire.ReadMarkerGetResult{}, err
	}
	return wire.ReadMarkerGetResult{ID: c.srv.idAtMarker(ctx, p.Network, key, ts), Time: ts}, nil
}

// handleReadMarkersSet moves a buffer's read marker forward to message id's time.
func handleReadMarkersSet(ctx context.Context, c *Conn, params json.RawMessage) (any, error) {
	p, err := decodeParams[wire.ReadMarkerSetParams](params)
	if err != nil {
		return nil, err
	}
	sess, err := c.srv.session(p.Network)
	if err != nil {
		return nil, err
	}
	db := c.srv.backend.Store()
	if db == nil {
		return nil, wire.Errorf(wire.CodeInternal, "no store")
	}
	m, err := db.MessageByID(ctx, p.Network, p.ID)
	if err != nil {
		return nil, err
	}
	key := wire.BufferKey(sess.Fold, p.Buffer)
	if m == nil || !store.InBuffer(m.Target, key) {
		return nil, wire.Errorf(wire.CodeNotFound, "no message %d in %s", p.ID, p.Buffer)
	}
	stored, updated, err := sess.SetReadMarkerFromModule(p.Buffer, m.Time.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}
	return wire.ReadMarkerSetResult{StoredID: c.srv.idAtMarker(ctx, p.Network, key, stored), Updated: updated}, nil
}

// idAtMarker is the newest message id at or before a stored marker time.
func (s *Server) idAtMarker(ctx context.Context, network int64, key, ts string) int64 {
	t, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return 0
	}
	id, _ := s.backend.Store().IDAtOrBefore(ctx, network, key, t)
	return id
}
