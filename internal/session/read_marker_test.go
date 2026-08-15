package session

import (
	"context"
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/internal/testutil"
)

func TestMARKREADGetSetBroadcast(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	s.registered = true
	a := &fakeDL{id: "a", caps: map[string]bool{"draft/read-marker": true}}
	b := &fakeDL{id: "b", caps: map[string]bool{"draft/read-marker": true}}
	c := &fakeDL{id: "c", caps: map[string]bool{}} // no cap
	_ = s.Attach(a)
	_ = s.Attach(b)
	_ = s.Attach(c)
	a.clearSent()
	b.clearSent()
	c.clearSent()

	if err := s.HandleClientMessage(a, irc.Message{Command: "MARKREAD", Params: []string{"#chan"}}); err != nil {
		t.Fatal(err)
	}
	if len(a.sent) != 1 || a.sent[0].Command != "MARKREAD" || a.sent[0].Param(1) != "*" {
		t.Fatalf("get *: %+v", a.sent)
	}
	a.clearSent()

	ts := "2019-01-04T14:33:26.123Z"
	if err := s.HandleClientMessage(a, irc.Message{Command: "MARKREAD", Params: []string{"#chan", "timestamp=" + ts}}); err != nil {
		t.Fatal(err)
	}
	if len(a.sent) != 1 || a.sent[0].Param(1) != "timestamp="+ts {
		t.Fatalf("set reply: %+v", a.sent)
	}
	if len(b.sent) != 1 || b.sent[0].Command != "MARKREAD" || b.sent[0].Param(1) != "timestamp="+ts {
		t.Fatalf("broadcast to b: %+v", b.sent)
	}
	if len(c.sent) != 0 {
		t.Fatalf("no-cap client: %+v", c.sent)
	}
	a.clearSent()
	b.clearSent()

	older := "2019-01-04T14:33:00.000Z"
	if err := s.HandleClientMessage(a, irc.Message{Command: "MARKREAD", Params: []string{"#chan", "timestamp=" + older}}); err != nil {
		t.Fatal(err)
	}
	if a.sent[0].Param(1) != "timestamp="+ts {
		t.Fatalf("older should echo stored: %+v", a.sent)
	}
	if len(b.sent) != 0 {
		t.Fatalf("no broadcast on non-update: %+v", b.sent)
	}
}

func TestMARKREADFailCodes(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	d := &fakeDL{id: "c1", caps: map[string]bool{"draft/read-marker": true}}
	s.registered = true
	_ = s.Attach(d)
	d.clearSent()

	_ = s.HandleClientMessage(d, irc.Message{Command: "MARKREAD"})
	if d.sent[0].Command != "FAIL" || d.sent[0].Param(1) != "NEED_MORE_PARAMS" {
		t.Fatalf("%+v", d.sent)
	}
	d.clearSent()
	_ = s.HandleClientMessage(d, irc.Message{Command: "MARKREAD", Params: []string{"#c", "*"}})
	if d.sent[0].Command != "FAIL" || d.sent[0].Param(1) != "INVALID_PARAMS" {
		t.Fatalf("%+v", d.sent)
	}
	d.clearSent()
	_ = s.HandleClientMessage(d, irc.Message{Command: "MARKREAD", Params: []string{"#c", "not-a-time"}})
	if d.sent[0].Command != "FAIL" || d.sent[0].Param(1) != "INVALID_PARAMS" {
		t.Fatalf("%+v", d.sent)
	}
}

func TestAttachMARKREADBefore366(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	s.registered = true
	s.channels["#c"] = &ChannelState{Name: "#c", Members: map[string]struct{}{"me": {}}, RosterKnown: true}
	if _, _, err := s.setReadMarkerIfNewer("#c", "2019-01-04T14:33:26.123Z"); err != nil {
		t.Fatal(err)
	}

	d := &fakeDL{id: "c1", caps: map[string]bool{"draft/read-marker": true}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	var sawJOIN, sawMARK, saw366 bool
	for _, m := range d.sent {
		switch m.Command {
		case "JOIN":
			if m.Param(0) == "#c" {
				sawJOIN = true
				if sawMARK || saw366 {
					t.Fatal("JOIN must come first")
				}
			}
		case "MARKREAD":
			if !sawJOIN {
				t.Fatal("MARKREAD before JOIN")
			}
			if saw366 {
				t.Fatal("MARKREAD after 366")
			}
			sawMARK = true
			if m.Param(0) != "#c" || m.Param(1) != "timestamp=2019-01-04T14:33:26.123Z" {
				t.Fatalf("%+v", m)
			}
		case "366":
			if m.Param(1) == "#c" {
				if !sawMARK {
					t.Fatal("366 without MARKREAD")
				}
				saw366 = true
			}
		}
	}
	if !sawJOIN || !sawMARK || !saw366 {
		t.Fatalf("join=%v mark=%v 366=%v", sawJOIN, sawMARK, saw366)
	}
}

func TestAttachMARKREADStarWhenUnset(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	s.registered = true
	s.channels["#c"] = &ChannelState{Name: "#c", Members: map[string]struct{}{}}
	d := &fakeDL{id: "c1", caps: map[string]bool{"draft/read-marker": true}}
	_ = s.Attach(d)
	found := false
	for _, m := range d.sent {
		if m.Command == "MARKREAD" && m.Param(0) == "#c" {
			found = true
			if m.Param(1) != "*" {
				t.Fatalf("%+v", m)
			}
		}
	}
	if !found {
		t.Fatal("missing MARKREAD *")
	}
}

func TestLiveSelfJOINInjectsMARKREAD(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	s.registered = true
	d := &fakeDL{id: "c1", caps: map[string]bool{"draft/read-marker": true, "message-tags": true, "server-time": true}}
	_ = s.Attach(d)
	d.clearSent()
	s.HandleMessage(irc.Message{Source: "me!u@h", Command: "JOIN", Params: []string{"#live"}})
	var joinIdx, markIdx = -1, -1
	for i, m := range d.sent {
		if m.Command == "JOIN" {
			joinIdx = i
		}
		if m.Command == "MARKREAD" && m.Param(0) == "#live" {
			markIdx = i
		}
	}
	if joinIdx < 0 || markIdx < 0 || markIdx <= joinIdx {
		t.Fatalf("order join=%d mark=%d msgs=%+v", joinIdx, markIdx, d.sent)
	}
}

func TestMARKREADPersisted(t *testing.T) {
	db := testutil.TempStore(t)
	ctx := context.Background()
	id, err := db.UpsertNetwork(ctx, store.Network{Name: "n", Host: "h", Port: 1, Nick: "me", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	netw, _ := db.NetworkByName(ctx, "n")
	s := New(netw, db, nil, nil, nil)
	s.registered = true
	d := &fakeDL{id: "c1", caps: map[string]bool{"draft/read-marker": true}}
	_ = s.Attach(d)
	d.clearSent()
	ts := "2020-02-02T02:02:02.000Z"
	_ = s.HandleClientMessage(d, irc.Message{Command: "MARKREAD", Params: []string{"bob", "timestamp=" + ts}})
	got, ok, err := db.GetReadMarker(ctx, id, "bob")
	if err != nil || !ok || got != ts {
		t.Fatalf("persist: %q ok=%v err=%v", got, ok, err)
	}
}
