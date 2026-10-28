package session

import (
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// The event bus feeds modules (internal/module) a per-network stream of envelopes that lines up
// exactly with a snapshot of Session state.
//
// Lock order: busMu is taken before s.mu, never the other way round. Every path that changes
// state a snapshot reports (history store → applyState → emit for an uplink line, and the
// downlink-side paths that change state on their own) holds busMu across the change and its
// emit, and Subscribe builds the snapshot and registers the sink under busMu too. So for any
// subscriber, snapshot + the events delivered after it = the state that follows, with nothing
// missed or applied twice. Never acquire busMu while holding s.mu.
//
// Emitting is a non-blocking hand-off to each sink (EventSink.Deliver); the bus never waits on
// a module.

// EventSink receives bus events (wire.LineEvent, wire.StateEvent, ...). Deliver must not block;
// returning false means the sink is dead and is removed.
type EventSink interface {
	Deliver(ev any) bool
}

type sinkEntry struct {
	sink  EventSink
	kinds map[string]bool // nil = every line kind
}

// Subscribe registers sink under id (replacing an earlier registration with the same id). build
// runs under busMu before registration — the snapshot, plus anything that must reach the
// subscriber ahead of the first event — and its result is returned. kinds limits line events
// to those kinds (empty = all); other events are always delivered.
func (s *Session) Subscribe(id uint64, sink EventSink, kinds []string, build func() any) any {
	s.busMu.Lock()
	defer s.busMu.Unlock()
	var snap any
	if build != nil {
		snap = build()
	}
	var km map[string]bool
	if len(kinds) > 0 {
		km = make(map[string]bool, len(kinds))
		for _, k := range kinds {
			km[k] = true
		}
	}
	if s.sinks == nil {
		s.sinks = make(map[uint64]sinkEntry)
	}
	s.sinks[id] = sinkEntry{sink: sink, kinds: km}
	return snap
}

// Unsubscribe removes the sink registered under id.
func (s *Session) Unsubscribe(id uint64) {
	s.busMu.Lock()
	delete(s.sinks, id)
	s.busMu.Unlock()
}

// emitLineLocked assigns the next per-network seq and delivers ev to every sink whose kinds
// match. Caller holds busMu.
func (s *Session) emitLineLocked(ev wire.LineEvent) {
	if len(s.sinks) == 0 {
		return
	}
	s.busSeq++
	ev.Seq = s.busSeq
	for id, e := range s.sinks {
		if e.kinds != nil && !e.kinds[ev.Kind] {
			continue
		}
		if !e.sink.Deliver(ev) {
			delete(s.sinks, id)
		}
	}
}

// emitLocked delivers a non-line event to every sink. Caller holds busMu.
func (s *Session) emitLocked(ev any) {
	for id, e := range s.sinks {
		if !e.sink.Deliver(ev) {
			delete(s.sinks, id)
		}
	}
}

// hasSinksLocked reports whether anything listens (callers skip building envelopes otherwise).
// Caller holds busMu.
func (s *Session) hasSinksLocked() bool { return len(s.sinks) > 0 }
