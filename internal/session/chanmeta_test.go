package session

import (
	"slices"
	"strings"
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/keeper"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// A3031: channel metadata (modes, topic, setter/time, creation) and whether each is known.

func snapChan(t *testing.T, s *Session, name string) wire.Channel {
	t.Helper()
	s.busMu.Lock()
	snap := s.snapshotLocked()
	s.busMu.Unlock()
	for _, ch := range snap.Channels {
		if ch.Name == name {
			return ch
		}
	}
	t.Fatalf("no %s in snapshot", name)
	return wire.Channel{}
}

func channelStateEvents(sink *recSink) []string {
	var out []string
	for _, ev := range sink.events() {
		if st, ok := ev.(wire.StateEvent); ok && st.Kind == wire.StateChannel {
			out = append(out, st.Buffer)
		}
	}
	return out
}

// The replies are parsed whoever asked, with no feature: 324 is the whole mode set, 332/333 the
// topic with its setter and time, 331 no topic, 329 the creation time; each makes its half
// known, and each change is a state/channel event for modules.
func TestChannelMetaParsedFromReplies(t *testing.T) {
	s, _, sink, _ := newMetaSession(t)
	s.SetStateFeatures(0)
	if c := snapChan(t, s, "#c"); c.ModesKnown {
		t.Fatalf("#c modes known before any 324: %+v", c)
	}
	feed(s, ":srv 005 me CHANMODES=beI,k,l,imnpst :are supported",
		":srv 324 me #c +ntk sekrit", ":srv 329 me #c 1700000000",
		":srv 332 me #c :hello world", ":srv 333 me #c op!o@h 1700000001",
		":srv 324 me #d +s", ":srv 331 me #d :No topic is set.")
	c := snapChan(t, s, "#c")
	if !c.ModesKnown || c.Modes != "+ntk sekrit" || !c.TopicKnown || c.Topic.Text != "hello world" ||
		c.Topic.Setter != "op!o@h" || c.Topic.Time != "2023-11-14T22:13:21Z" {
		t.Fatalf("#c = %+v", c)
	}
	s.mu.RLock()
	created := s.channels["#c"].Created
	s.mu.RUnlock()
	if created != 1700000000 {
		t.Fatalf("created = %d", created)
	}
	if d := snapChan(t, s, "#d"); !d.ModesKnown || d.Modes != "+s" || !d.TopicKnown || d.Topic.Text != "" || d.Topic.Setter != "" {
		t.Fatalf("#d = %+v", d)
	}
	if ev := channelStateEvents(sink); !slices.Contains(ev, "#c") || !slices.Contains(ev, "#d") {
		t.Fatalf("channel events = %q", ev)
	}
	// A later 324 replaces the set; it is not a delta.
	feed(s, ":srv 324 me #c +m")
	if c := snapChan(t, s, "#c"); c.Modes != "+m" {
		t.Fatalf("#c after a second 324 = %q", c.Modes)
	}
}

// A live TOPIC is the topic, its source the setter and its time the time; our JOIN's burst
// without 332 means no topic.
func TestChannelMetaLiveTopicAndJoinBurst(t *testing.T) {
	s, _, _, _ := newMetaSession(t)
	feed(s, "@time=2026-11-08T10:00:00.000Z :alice!a@h TOPIC #c :fresh")
	if c := snapChan(t, s, "#c"); !c.TopicKnown || c.Topic.Text != "fresh" || c.Topic.Setter != "alice!a@h" || c.Topic.Time != "2026-11-08T10:00:00Z" {
		t.Fatalf("#c = %+v", c)
	}
	feed(s, ":me!u@h JOIN #e")
	if e := snapChan(t, s, "#e"); e.TopicKnown {
		t.Fatalf("#e topic known mid-burst: %+v", e)
	}
	feed(s, ":srv 353 me = #e :@me", ":srv 366 me #e :End of /NAMES list.")
	if e := snapChan(t, s, "#e"); !e.TopicKnown || e.Topic.Text != "" || e.ModesKnown {
		t.Fatalf("#e after its burst = %+v", e)
	}
}

// resumedSession is a session resumed from the blob (a brain reload) in #a and #b, with an IRC
// client attached and a module subscribed; uplink writes are recorded.
func resumedSession(t *testing.T, features StateFeatures) (*Session, *fakeDL, *recSink, *writeRec) {
	t.Helper()
	s := New(store.Network{ID: 1, Name: "n", Nick: "me"}, nil, nil, nil, nil)
	s.SetStateFeatures(features)
	w := &writeRec{}
	s.writeHook = w.write
	s.SeedFromBlob([]keeper.BlobEntry{
		{Key: "self-nick", Values: [][]byte{[]byte("me")}},
		{Key: "channel:#a", Values: [][]byte{[]byte("")}},
		{Key: "channel:#b", Values: [][]byte{[]byte("")}},
	})
	d := &fakeDL{id: "d1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	d.clearSent()
	sink := &recSink{}
	s.Subscribe(1, sink, nil, nil)
	return s, d, sink, w
}

// After a brain reload, with channel_meta: every resumed channel starts unknown; its MODE and
// TOPIC are asked one channel per RefreshChannelMeta call (the server's WHOX seed tick), as
// GoBNC's own enquiries, so the replies reach neither the IRC client nor modules as lines; the
// answers make the channel known and are state/channel events.
func TestChannelMetaReloadRefetchPacedAndAbsorbed(t *testing.T) {
	s, d, sink, w := resumedSession(t, StateChannelMeta)
	if a := snapChan(t, s, "#a"); a.ModesKnown || a.TopicKnown {
		t.Fatalf("resumed #a known: %+v", a)
	}
	s.RefreshResumedChannelNames()
	if got := strings.Join(w.lines(), "|"); got != "NAMES #a|NAMES #b" {
		t.Fatalf("after the reload's refresh: %q (MODE/TOPIC wait for the tick)", got)
	}
	w.reset()
	if got := s.RefreshChannelMeta(); got != "#a" {
		t.Fatalf("first tick asked %q", got)
	}
	if got := strings.Join(w.lines(), "|"); got != "MODE #a|TOPIC #a" {
		t.Fatalf("first tick wrote %q", got)
	}
	lines := len(sink.lines())
	feed(s, ":srv 324 me #a +nt", ":srv 329 me #a 1700000000", ":srv 332 me #a :hello", ":srv 333 me #a op 1700000001")
	if sent := sentTo(d); len(sent) != 0 {
		t.Fatalf("the fetch's replies reached the IRC client: %q", sent)
	}
	if n := len(sink.lines()); n != lines {
		t.Fatalf("the fetch's replies reached modules as %d lines", n-lines)
	}
	if a := snapChan(t, s, "#a"); !a.ModesKnown || a.Modes != "+nt" || !a.TopicKnown || a.Topic.Text != "hello" || a.Topic.Setter != "op" {
		t.Fatalf("#a after the answer = %+v", a)
	}
	if ev := channelStateEvents(sink); !slices.Contains(ev, "#a") {
		t.Fatalf("no channel event for #a: %q", ev)
	}
	w.reset()
	if got := s.RefreshChannelMeta(); got != "#b" || strings.Join(w.lines(), "|") != "MODE #b|TOPIC #b" {
		t.Fatalf("second tick: %q wrote %q", got, w.lines())
	}
	if got := s.RefreshChannelMeta(); got != "" {
		t.Fatalf("third tick asked %q", got)
	}
}

// Without channel_meta a reload asks nothing beyond NAMES (core alone).
func TestChannelMetaReloadWithoutFeatureAsksNothing(t *testing.T) {
	s, _, _, w := resumedSession(t, 0)
	s.RefreshResumedChannelNames()
	if got := s.RefreshChannelMeta(); got != "" {
		t.Fatalf("asked %q without channel_meta", got)
	}
	if got := strings.Join(w.lines(), "|"); got != "NAMES #a|NAMES #b" {
		t.Fatalf("writes = %q", got)
	}
	feed(s, ":me!u@h JOIN #e")
	if got := s.RefreshChannelMeta(); got != "" {
		t.Fatalf("JOIN queued %q without channel_meta", got)
	}
}

// A half already known by the time its turn comes (an IRC client asked meanwhile) is not asked
// again; our JOIN queues only MODE (its burst carries the topic).
func TestChannelMetaSkipsKnownHalvesAndJoinAsksMode(t *testing.T) {
	s, _, _, w := resumedSession(t, StateChannelMeta)
	s.RefreshResumedChannelNames()
	feed(s, ":srv 324 me #a +n") // a client's MODE #a answered first
	w.reset()
	if got := s.RefreshChannelMeta(); got != "#a" || strings.Join(w.lines(), "|") != "TOPIC #a" {
		t.Fatalf("tick asked %q, wrote %q", got, w.lines())
	}
	feed(s, ":srv 324 me #b +n", ":srv 331 me #b :No topic is set.")
	feed(s, ":me!u@h JOIN #e")
	w.reset()
	if got := s.RefreshChannelMeta(); got != "#e" || strings.Join(w.lines(), "|") != "MODE #e" {
		t.Fatalf("tick after JOIN asked %q, wrote %q", got, w.lines())
	}
}

func sentTo(d *fakeDL) []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]string, 0, len(d.sent))
	for _, m := range d.sent {
		out = append(out, m.Command+" "+strings.Join(m.Params, " "))
	}
	return out
}
