package session

import (
	"context"
	"sort"
	"strings"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// SubscribeWithSnapshot registers sink like Subscribe and hands the snapshot to deliver while
// still under busMu, so whatever deliver queues (the subscribe response) precedes the sink's
// first event.
func (s *Session) SubscribeWithSnapshot(id uint64, sink EventSink, kinds []string, deliver func(wire.Snapshot)) wire.Snapshot {
	return s.Subscribe(id, sink, kinds, func() any {
		snap := s.snapshotLocked()
		if deliver != nil {
			deliver(snap)
		}
		return snap
	}).(wire.Snapshot)
}

// snapshotLocked is the module view of the network right now. Caller holds busMu; it takes
// s.mu itself.
func (s *Session) snapshotLocked() wire.Snapshot {
	var lastID int64
	if s.store != nil && s.Network.ID != 0 {
		lastID, _ = s.store.MaxMessageID(context.Background(), s.Network.ID)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := wire.Snapshot{
		Network:  s.Network.ID,
		State:    s.networkStateLocked(),
		LastID:   lastID,
		ISupport: make(map[string]string, len(s.isupport.Raw)),
		Caps:     []string{},
		Channels: []wire.Channel{},
	}
	if u := s.self; u != nil {
		snap.Self = wire.Self{Nick: s.liveNickLocked(), User: u.User, Host: u.Host, Account: u.Account,
			UModes: u.UModeString(), Away: u.Away}
	}
	for k, v := range s.isupport.Raw {
		snap.ISupport[k] = v
	}
	for c, on := range s.upCaps {
		if on {
			snap.Caps = append(snap.Caps, c)
		}
	}
	sort.Strings(snap.Caps)
	cm := s.isupport.CaseMapping
	for folded, ch := range s.channels {
		c := wire.Channel{Name: ch.Name, Key: folded, Topic: wire.Topic{Text: ch.Topic},
			ChanKey: ch.Key, MembersComplete: ch.RosterKnown, Members: s.membersLocked(ch)}
		if ch.Modes != nil {
			modes, args := ch.Modes.ModeString()
			c.Modes = strings.TrimSpace(strings.Join(append([]string{modes}, args...), " "))
			if k := ch.Modes.Args['k']; k != "" {
				c.ChanKey = k
			}
		}
		if c.Key == "" {
			c.Key = cm.Canonical(ch.Name)
		}
		snap.Channels = append(snap.Channels, c)
	}
	sort.Slice(snap.Channels, func(i, j int) bool { return snap.Channels[i].Key < snap.Channels[j].Key })
	return snap
}

// networkStateLocked is the snapshot's network state. Caller holds s.mu.
func (s *Session) networkStateLocked() string {
	if s.registered {
		return wire.NetRegistered
	}
	return wire.NetConnecting
}

// membersLocked lists a channel's members with prefixes in PREFIX order. Caller holds s.mu.
func (s *Session) membersLocked(ch *ChannelState) []wire.Member {
	out := make([]wire.Member, 0, len(ch.Members))
	ms := s.isupport.Modes
	for folded := range ch.Members {
		m := wire.Member{Nick: folded}
		if u := s.users[folded]; u != nil {
			m.Nick, m.User, m.Host, m.Account, m.Away = u.Nick, u.User, u.Host, u.Account, u.Away
		}
		if ch.Modes != nil && ch.Modes.Nicks[folded] != nil && ms != nil {
			var pref []byte
			for _, mode := range ms.PrefixOrder {
				if ch.Modes.Nicks[folded][mode] {
					if sym := ms.Prefix[mode]; sym != 0 {
						pref = append(pref, sym)
					}
				}
			}
			m.Prefixes = string(pref)
		}
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Nick < out[j].Nick })
	return out
}

// emitRosterDoneLocked emits a state/members event for every channel whose roster a NAMES
// reply (366) just confirmed, and forgets them. Caller holds busMu.
func (s *Session) emitRosterDoneLocked() {
	s.mu.Lock()
	done := s.rosterDone
	s.rosterDone = nil
	s.mu.Unlock()
	if len(done) == 0 || !s.hasSinksLocked() {
		return
	}
	for _, folded := range done {
		s.mu.RLock()
		ch := s.channels[folded]
		var ev wire.StateEvent
		if ch != nil {
			ev = wire.StateEvent{Network: s.Network.ID, Kind: wire.StateMembers, Buffer: ch.Name, Key: folded,
				Members: s.membersLocked(ch), Complete: true}
		}
		s.mu.RUnlock()
		if ch != nil {
			s.emitLocked(ev)
		}
	}
}
