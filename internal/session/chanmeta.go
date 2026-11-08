package session

import (
	"strconv"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// Channel metadata (A3031): a channel's modes, topic, topic setter/time and creation time, with
// whether each is known (ChannelState.ModesKnown/TopicKnown). The replies are parsed whoever
// asked (state324Locked and friends: no queries, no standing cost), so a snapshot can tell
// "unknown" from "empty" — gobnc-retecloud's lite state omits what is unknown, where "" would
// read as cleared. A change a reply makes is a module state event of kind "channel".
//
// With the channel_meta feature (StateChannelMeta), GoBNC also asks: after a brain reload every
// resumed channel's MODE and TOPIC (the server never resends the JOIN burst to a resumed
// connection), and after our own JOIN the channel's MODE (the burst carries the topic, not the
// modes). The questions are GoBNC's own (internalClient: no client or module sees the replies
// as lines) and go out one channel per RefreshChannelMeta call, which internal/server makes once
// per WHOX seed tick.

// channelMetaFetch is one channel's queued channel_meta question: MODE, TOPIC or both.
type channelMetaFetch struct {
	name        string
	mode, topic bool
}

// noteChannelMetaLocked marks a channel's metadata changed, for the state/channel event.
// Caller holds s.mu.
func (s *Session) noteChannelMetaLocked(folded string) {
	for _, k := range s.metaDone {
		if k == folded {
			return
		}
	}
	s.metaDone = append(s.metaDone, folded)
}

// metaChannelLocked returns the joined channel a numeric names in param i, and its key.
// Caller holds s.mu.
func (s *Session) metaChannelLocked(msg irc.Message, i int) (*ChannelState, string) {
	folded := s.isupport.CaseMapping.Canonical(msg.Param(i))
	return s.channels[folded], folded
}

// state324Locked handles RPL_CHANNELMODEIS (324 me #chan +modes args...): the channel's whole
// mode set, so flags and arguments are replaced (lists and member prefixes are not part of it).
// Caller holds s.mu.
func (s *Session) state324Locked(msg irc.Message) {
	ch, folded := s.metaChannelLocked(msg, 1)
	if ch == nil || len(msg.Params) < 3 {
		return
	}
	if ch.Modes == nil {
		ch.Modes = irc.NewChannelModes()
	}
	ch.Modes.Flags, ch.Modes.Args = make(map[byte]bool), make(map[byte]string)
	if ms := s.isupport.Modes; ms != nil {
		ch.Modes.Apply(ms, s.isupport.CaseMapping, ms.ParseModeParams(msg.Params[2], msg.Params[3:]))
	}
	ch.ModesKnown = true
	s.noteChannelMetaLocked(folded)
}

// state329Locked handles RPL_CREATIONTIME (329 me #chan ts). Caller holds s.mu.
func (s *Session) state329Locked(msg irc.Message) {
	if ch, _ := s.metaChannelLocked(msg, 1); ch != nil {
		if ts, err := strconv.ParseInt(msg.Param(2), 10, 64); err == nil {
			ch.Created = ts
		}
	}
}

// state331Locked handles RPL_NOTOPIC: known to have no topic. Caller holds s.mu.
func (s *Session) state331Locked(msg irc.Message) {
	if ch, folded := s.metaChannelLocked(msg, 1); ch != nil {
		ch.Topic, ch.TopicKnown, ch.TopicSetter, ch.TopicTime = "", true, "", 0
		s.noteChannelMetaLocked(folded)
	}
}

// state332Locked handles RPL_TOPIC (332 me #chan :text); 333 follows with setter and time.
// Caller holds s.mu.
func (s *Session) state332Locked(msg irc.Message) {
	if ch, folded := s.metaChannelLocked(msg, 1); ch != nil {
		ch.Topic, ch.TopicKnown, ch.TopicSetter, ch.TopicTime = msg.Trailing(), true, "", 0
		s.noteChannelMetaLocked(folded)
	}
}

// state333Locked handles RPL_TOPICWHOTIME (333 me #chan setter ts). Caller holds s.mu.
func (s *Session) state333Locked(msg irc.Message) {
	if ch, folded := s.metaChannelLocked(msg, 1); ch != nil {
		ch.TopicSetter = msg.Param(2)
		ch.TopicTime, _ = strconv.ParseInt(msg.Param(3), 10, 64)
		s.noteChannelMetaLocked(folded)
	}
}

// stateTOPICLocked handles a live TOPIC: the topic, its source the setter and the line's time
// the time. Caller holds s.mu.
func (s *Session) stateTOPICLocked(msg irc.Message) {
	if ch, _ := s.metaChannelLocked(msg, 0); ch != nil {
		ch.Topic, ch.TopicKnown, ch.TopicSetter, ch.TopicTime = msg.Trailing(), true, msg.Source, messageUnix(msg)
	}
}

// endJoinBurstLocked ends our JOIN's burst at its 366: no 332 in it means no topic.
// Caller holds s.mu.
func (s *Session) endJoinBurstLocked(ch *ChannelState, folded string) {
	if !ch.joinBurst {
		return
	}
	ch.joinBurst = false
	if !ch.TopicKnown {
		ch.Topic, ch.TopicKnown = "", true
		s.noteChannelMetaLocked(folded)
	}
}

// messageUnix is a line's server-time (or now) in unix seconds.
func messageUnix(msg irc.Message) int64 {
	if ts, ok := msg.Tag("time"); ok {
		if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
			return t.Unix()
		}
	}
	return time.Now().Unix()
}

// emitChannelMetaLocked emits a state/channel event for every channel whose metadata a reply
// just changed. Caller holds busMu (not s.mu).
func (s *Session) emitChannelMetaLocked() {
	s.mu.Lock()
	done := s.metaDone
	s.metaDone = nil
	var evs []wire.StateEvent
	if len(done) > 0 && s.hasSinksLocked() {
		for _, folded := range done {
			if ch := s.channels[folded]; ch != nil {
				evs = append(evs, wire.StateEvent{Network: s.Network.ID, Kind: wire.StateChannel, Buffer: ch.Name, Key: folded})
			}
		}
	}
	s.mu.Unlock()
	for _, ev := range evs {
		s.emitLocked(ev)
	}
}

// queueChannelMetaLocked queues a channel_meta question (only with that feature). Caller holds
// s.mu.
func (s *Session) queueChannelMetaLocked(name string, mode, topic bool) {
	if !s.StateFeatures().Has(StateChannelMeta) {
		return
	}
	s.pendingChannelMeta = append(s.pendingChannelMeta, channelMetaFetch{name: name, mode: mode, topic: topic})
}

// RefreshChannelMeta asks the uplink one queued channel's MODE and/or TOPIC (the halves still
// unknown; a channel left, or already known, is skipped), as GoBNC's own enquiry. It returns the
// channel asked about, or "". Call where writing to the uplink is allowed (internal/server's
// WHOX seed tick, after LiveReady); the queue is filled only after LiveReady (a resumed
// network's, by RefreshResumedChannelNames) or by a live JOIN.
func (s *Session) RefreshChannelMeta() string {
	if !s.StateFeatures().Has(StateChannelMeta) {
		return ""
	}
	s.mu.Lock()
	var next channelMetaFetch
	var folded string
	for len(s.pendingChannelMeta) > 0 {
		f := s.pendingChannelMeta[0]
		s.pendingChannelMeta = s.pendingChannelMeta[1:]
		key := s.isupport.CaseMapping.Canonical(f.name)
		ch := s.channels[key]
		if ch == nil {
			continue
		}
		f.name = ch.Name
		f.mode = f.mode && !ch.ModesKnown
		f.topic = f.topic && !ch.TopicKnown
		if f.mode || f.topic {
			next, folded = f, key
			break
		}
	}
	s.mu.Unlock()
	if folded == "" {
		return ""
	}
	if next.mode {
		out := irc.Message{Command: "MODE", Params: []string{next.name}}
		if _, _, writeNow := s.tracker.Begin(BeginOpts{Client: internalClient, Cmd: "MODE", EnquiryTarget: folded, Outbound: out}); writeNow {
			_ = s.WriteMessage(out)
		}
	}
	if next.topic {
		out := irc.Message{Command: "TOPIC", Params: []string{next.name}}
		if _, _, writeNow := s.tracker.Begin(BeginOpts{Client: internalClient, Cmd: "TOPIC", EnquiryTarget: folded, Outbound: out}); writeNow {
			_ = s.WriteMessage(out)
		}
	}
	return next.name
}
