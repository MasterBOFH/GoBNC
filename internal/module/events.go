package module

import (
	"context"
	"encoding/json"

	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

func init() {
	registerMethod(wire.MethodEventsSubscribe, handleEventsSubscribe, wire.FeatureEvents)
	registerMethod(wire.MethodEventsUnsubscribe, handleEventsUnsubscribe, wire.FeatureEvents)
}

// connSink delivers a Session's bus events to one module connection.
type connSink struct{ c *Conn }

func (k connSink) Deliver(ev any) bool {
	name := eventName(ev)
	if name == "" {
		return true
	}
	return k.c.EnqueueEvent(name, ev)
}

func eventName(ev any) string {
	switch ev.(type) {
	case wire.LineEvent:
		return wire.EventLine
	case wire.StateEvent:
		return wire.EventState
	case wire.ReadMarkerEvent:
		return wire.EventReadMarker
	case wire.BufferOpenEvent:
		return wire.EventBufferOpen
	case wire.BufferCloseEvent:
		return wire.EventBufferClose
	case wire.ClientLineEvent:
		return wire.EventClientLine
	}
	return ""
}

func (s *Server) session(network int64) (*session.Session, error) {
	sess, ok := s.backend.Session(network)
	if !ok || sess == nil {
		return nil, wire.Errorf(wire.CodeNotFound, "no network %d", network)
	}
	return sess, nil
}

// handleEventsSubscribe registers the connection on the network's bus. The response carrying
// the snapshot is queued under the bus lock, ahead of the first event.
func handleEventsSubscribe(ctx context.Context, c *Conn, params json.RawMessage) (any, error) {
	p, err := decodeParams[wire.SubscribeParams](params)
	if err != nil {
		return nil, err
	}
	sess, err := c.srv.session(p.Network)
	if err != nil {
		return nil, err
	}
	sess.SubscribeWithSnapshot(c.ID(), connSink{c}, p.Kinds, func(snap wire.Snapshot) {
		c.Respond(ctx, wire.SubscribeResult{Snapshot: snap})
	})
	c.mu.Lock()
	first := c.subs == nil
	if first {
		c.subs = map[int64]bool{}
	}
	c.subs[p.Network] = true
	c.mu.Unlock()
	if first {
		c.OnClose(func() { c.unsubscribeAll() })
	}
	return nil, errResponded
}

func handleEventsUnsubscribe(ctx context.Context, c *Conn, params json.RawMessage) (any, error) {
	p, err := decodeParams[wire.NetworkParams](params)
	if err != nil {
		return nil, err
	}
	sess, err := c.srv.session(p.Network)
	if err != nil {
		return nil, err
	}
	sess.Unsubscribe(c.ID())
	c.mu.Lock()
	delete(c.subs, p.Network)
	c.mu.Unlock()
	return nil, nil
}

// unsubscribeAll removes the connection from every network bus it joined.
func (c *Conn) unsubscribeAll() {
	c.mu.Lock()
	nets := make([]int64, 0, len(c.subs))
	for n := range c.subs {
		nets = append(nets, n)
	}
	c.subs = nil
	c.mu.Unlock()
	for _, n := range nets {
		if sess, ok := c.srv.backend.Session(n); ok && sess != nil {
			sess.Unsubscribe(c.ID())
		}
	}
}
