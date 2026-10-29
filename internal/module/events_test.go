package module

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/history"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// registeredSession is a Session registered as "me" in #c, fed lines directly (no uplink).
func registeredSession(t *testing.T, id int64) *session.Session {
	t.Helper()
	return registeredSessionWithStore(t, id, nil)
}

// registeredSessionWithStore is registeredSession keeping its history and markers in db.
func registeredSessionWithStore(t *testing.T, id int64, db *store.Store) *session.Session {
	t.Helper()
	var hist *history.Store
	if db != nil {
		hist = history.New(db)
	}
	s := session.New(store.Network{ID: id, Name: "net", Nick: "me"}, db, hist, nil, nil)
	for i, l := range []string{":srv.example 001 me :Welcome", ":srv.example 376 me :End", ":me!u@h JOIN #c"} {
		s.HandleLine([]byte(l), uint64(i+1))
	}
	if !s.Registered() {
		t.Fatal("session not registered")
	}
	return s
}

// nextEvent returns the next event frame named name.
func (tc *testConn) nextEvent(name string) json.RawMessage {
	tc.t.Helper()
	for {
		f, err := tc.read()
		if err != nil {
			tc.t.Fatalf("waiting for %s event: %v", name, err)
		}
		if f.Kind == wire.KindEvent && f.Event.Event == name {
			return f.Event.Data
		}
	}
}

func TestSubscribeSnapshotThenLineEvents(t *testing.T) {
	sess := registeredSession(t, 5)
	be := &fakeBackend{ready: true, nets: []wire.NetworkInfo{{ID: 5, Name: "net", State: wire.NetRegistered, Nick: "me"}},
		sessions: map[int64]*session.Session{5: sess}}
	_, path := startServer(t, []config.ModuleConfig{{Name: "m", Features: []string{"networks", "events"}}}, be)
	tc := dialModule(t, path)
	tc.hello("m", "networks", "events")

	var nets wire.NetworksListResult
	tc.call(wire.MethodNetworksList, wire.Empty{}, &nets)
	if len(nets.Networks) != 1 || nets.Networks[0].ID != 5 {
		t.Fatalf("networks.list = %+v", nets)
	}
	var sub wire.SubscribeResult
	tc.call(wire.MethodEventsSubscribe, wire.SubscribeParams{Network: 5}, &sub)
	if sub.Snapshot.Network != 5 || len(sub.Snapshot.Channels) != 1 || sub.Snapshot.Channels[0].Name != "#c" {
		t.Fatalf("snapshot = %+v", sub.Snapshot)
	}
	sess.HandleLine([]byte(":peer!p@h PRIVMSG #c :hello module"), 10)
	var ev wire.LineEvent
	if err := json.Unmarshal(tc.nextEvent(wire.EventLine), &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Kind != wire.LineMessage || !strings.Contains(ev.Raw, "hello module") || ev.Network != 5 {
		t.Fatalf("line event = %+v", ev)
	}
	if code := tc.callErr(wire.MethodEventsSubscribe, wire.SubscribeParams{Network: 99}); code != wire.CodeNotFound {
		t.Fatalf("unknown network: code %q", code)
	}

	tc.c.Close()
	deadline := time.Now().Add(2 * time.Second)
	for sess.Subscribers() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("sink still registered after the connection closed (%d)", sess.Subscribers())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestUnsubscribeStopsEvents(t *testing.T) {
	sess := registeredSession(t, 6)
	be := &fakeBackend{ready: true, sessions: map[int64]*session.Session{6: sess}}
	_, path := startServer(t, []config.ModuleConfig{{Name: "m", Features: []string{"events"}}}, be)
	tc := dialModule(t, path)
	tc.hello("m", "events")
	tc.call(wire.MethodEventsSubscribe, wire.SubscribeParams{Network: 6}, nil)
	tc.call(wire.MethodEventsUnsubscribe, wire.NetworkParams{Network: 6}, nil)
	if n := sess.Subscribers(); n != 0 {
		t.Fatalf("%d sinks after unsubscribe", n)
	}
}
