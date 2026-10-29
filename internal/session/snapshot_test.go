package session

import (
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/keeper"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

func snapshotOf(s *Session) wire.Snapshot {
	var snap wire.Snapshot
	s.SubscribeWithSnapshot(99, &recSink{}, nil, func(sn wire.Snapshot) { snap = sn })
	s.Unsubscribe(99)
	return snap
}

func TestSnapshotAfterRegistrationJoinNamesTopic(t *testing.T) {
	s := New(store.Network{ID: 3, Name: "n", Nick: "me"}, nil, nil, nil, nil)
	for i, l := range []string{
		":srv 001 me :Welcome",
		":srv 005 me CASEMAPPING=ascii PREFIX=(ov)@+ CHANTYPES=# :are supported",
		":srv 376 me :End of MOTD",
	} {
		s.HandleLine([]byte(l), uint64(i+1))
	}
	if !s.Registered() {
		t.Fatal("not registered after 376")
	}
	feed(s,
		":me!ident@cloak JOIN #Chan",
		":srv 353 me = #Chan :@me +Voiced plain",
		":srv 366 me #Chan :End of NAMES",
		":op!o@h TOPIC #Chan :hello world",
	)
	snap := snapshotOf(s)
	if snap.Network != 3 || snap.State != wire.NetRegistered {
		t.Fatalf("network/state = %d/%q", snap.Network, snap.State)
	}
	if snap.Self.Nick != "me" || snap.Self.User != "ident" || snap.Self.Host != "cloak" {
		t.Fatalf("self = %+v", snap.Self)
	}
	if snap.ISupport["CASEMAPPING"] != "ascii" {
		t.Fatalf("isupport = %v", snap.ISupport)
	}
	if len(snap.Channels) != 1 {
		t.Fatalf("channels = %+v", snap.Channels)
	}
	ch := snap.Channels[0]
	if ch.Name != "#Chan" || ch.Key != "#chan" || ch.Topic.Text != "hello world" || !ch.MembersComplete {
		t.Fatalf("channel = %+v", ch)
	}
	got := map[string]string{}
	for _, m := range ch.Members {
		got[m.Nick] = m.Prefixes
	}
	want := map[string]string{"me": "@", "Voiced": "+", "plain": ""}
	if len(got) != len(want) {
		t.Fatalf("members = %+v", ch.Members)
	}
	for n, p := range want {
		if got[n] != p {
			t.Fatalf("member %s prefixes %q, want %q (members %+v)", n, got[n], p, ch.Members)
		}
	}
}

func TestSnapshotBlobSeededChannelCompletesOnNames(t *testing.T) {
	s := New(store.Network{ID: 4, Name: "n", Nick: "me"}, nil, nil, nil, nil)
	s.SeedFromBlob([]keeper.BlobEntry{{Key: "channel:#res", Values: [][]byte{[]byte("")}}})
	s.mu.Lock()
	s.registered = true
	s.mu.Unlock()
	snap := snapshotOf(s)
	if len(snap.Channels) != 1 || snap.Channels[0].MembersComplete {
		t.Fatalf("blob-seeded channel should be incomplete: %+v", snap.Channels)
	}
	sink := &recSink{}
	s.Subscribe(1, sink, nil, nil)
	feed(s, ":srv 353 me = #res :me other", ":srv 366 me #res :End of NAMES")
	var st *wire.StateEvent
	for _, ev := range sink.events() {
		if e, ok := ev.(wire.StateEvent); ok {
			st = &e
		}
	}
	if st == nil || st.Kind != wire.StateMembers || st.Buffer != "#res" || !st.Complete || len(st.Members) != 2 {
		t.Fatalf("state event = %+v (events %+v)", st, sink.events())
	}
	if snap := snapshotOf(s); !snap.Channels[0].MembersComplete {
		t.Fatal("snapshot after 366 still incomplete")
	}
}

func TestSnapshotSeqPlacesItInTheStream(t *testing.T) {
	s := newBusSession(t)
	sink := &recSink{}
	s.SubscribeWithSnapshot(1, sink, nil, nil)
	feed(s, ":a!u@h JOIN #c", ":b!u@h JOIN #c")
	lines := sink.lines()
	var snap wire.Snapshot
	s.SubscribeWithSnapshot(1, sink, nil, func(sn wire.Snapshot) { snap = sn }) // re-subscribe: fresh snapshot
	if snap.Seq != lines[len(lines)-1].Seq {
		t.Fatalf("snapshot seq = %d, want the last emitted line's seq %d", snap.Seq, lines[len(lines)-1].Seq)
	}
	feed(s, ":c!u@h JOIN #c")
	if after := sink.lines(); after[len(after)-1].Seq != snap.Seq+1 {
		t.Fatalf("next line seq = %d, want %d", after[len(after)-1].Seq, snap.Seq+1)
	}
}
