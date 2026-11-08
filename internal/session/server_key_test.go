package session

import (
	"context"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// Each server's notices are stored under that server's own name: a services server's under its
// name, beside the one we registered to.
func TestServerNoticesAreStoredUnderEachServersName(t *testing.T) {
	s, _ := newEnvelopeSession(t)
	feed(s, ":srv.example NOTICE me :*** Notice -- snotice", ":Services.Example NOTICE me :services notice")
	ctx := context.Background()
	for target, text := range map[string]string{"srv.example": "*** Notice -- snotice", "services.example": "services notice"} {
		rows, err := s.store.MessagesLatest(ctx, s.Network.ID, target, 10)
		if err != nil || len(rows) != 1 || rows[0].Text != text {
			t.Fatalf("%s rows = %+v (err %v), want %q", target, rows, err, text)
		}
	}
	if ok, _ := s.store.HasTarget(ctx, s.Network.ID, "server"); ok {
		t.Fatal("a server notice was stored under server")
	}
}

// A line naming no server while the server we registered to is unknown (a resumed brain whose
// blob had no 001 source) goes to the network's configured host when that is a server name,
// and is not stored when it is not.
func TestSourcelessNoticeWithoutUplinkNameGoesToTheHost(t *testing.T) {
	for _, tc := range []struct{ host, want string }{{"IRC.Example.ORG", "irc.example.org"}, {"localhost", ""}} {
		db, hist, id := openLegacyFixture(t)
		s := New(store.Network{ID: id, Name: "n", Nick: "me", Host: tc.host}, db, hist, nil, nil)
		s.registered = true
		s.HandleLine([]byte("NOTICE me :*** no prefix"), 1)
		targets, err := db.DistinctTargets(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if tc.want == "" {
			if len(targets) != 0 {
				t.Fatalf("host %s: stored under %v, want nothing", tc.host, targets)
			}
			continue
		}
		if len(targets) != 1 || targets[0] != tc.want {
			t.Fatalf("host %s: stored under %v, want [%s]", tc.host, targets, tc.want)
		}
	}
}

// A module names the Server buffer "Server" (in any case); its read marker and device rows are
// keyed "*", the Server buffer's own key, never "server" (a real nick Server's query), and IRC
// clients get no MARKREAD for it ("Server" would name that query).
func TestModuleServerBufferIsKeyedStar(t *testing.T) {
	s, sink := newEnvelopeSession(t)
	ctx := context.Background()
	d := &fakeDL{id: "irc", caps: map[string]bool{readMarkerCap: true}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	feed(s, ":srv.example NOTICE me :*** Notice -- snotice")
	d.clearSent()
	ts := time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano)
	if _, updated, err := s.SetReadMarkerFromModule(wire.ServerBuffer, ts); err != nil || !updated {
		t.Fatalf("SetReadMarkerFromModule: updated %v err %v", updated, err)
	}
	if _, ok, _ := s.store.GetReadMarker(ctx, s.Network.ID, wire.ServerBufferKey); !ok {
		t.Fatal("no read marker under *")
	}
	if _, ok, _ := s.store.GetReadMarker(ctx, s.Network.ID, "server"); ok {
		t.Fatal("the Server buffer's marker landed on the query server")
	}
	var marker *wire.ReadMarkerEvent
	for _, ev := range sink.events() {
		if e, ok := ev.(wire.ReadMarkerEvent); ok {
			marker = &e
		}
	}
	if marker == nil || marker.Key != wire.ServerBufferKey || marker.Buffer != wire.ServerBuffer {
		t.Fatalf("read_marker event = %+v, want buffer Server key *", marker)
	}
	for _, m := range d.sent {
		if m.Command == "MARKREAD" {
			t.Fatalf("IRC client got %+v for the Server buffer", m)
		}
	}

	if err := s.OpenBufferForDevice("phone", "server"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CloseBufferForDevice("phone", wire.ServerBuffer); err != nil {
		t.Fatal(err)
	}
	rows, err := s.store.ListOpenBuffers(ctx, "phone", s.Network.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Target != wire.ServerBufferKey || rows[0].ClosedAtID == 0 {
		t.Fatalf("device rows = %+v, want one closed row keyed * with the notice's id as floor", rows)
	}
}

// A new device's baseline marks the server-name targets read for IRC clients (MARKREAD with the
// server's name) and the Server buffer for modules (read_marker buffer Server, key "*"); a
// server's name is never a module buffer of its own.
func TestBaselineMarksServerTargetsAndTheServerBuffer(t *testing.T) {
	s, sink := newEnvelopeSession(t)
	d := &fakeDL{id: "irc", caps: map[string]bool{readMarkerCap: true}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	feed(s, ":srv.example NOTICE me :*** Notice -- snotice")
	d.clearSent()
	if _, err := s.SeedDevice("fresh", true); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, ev := range sink.events() {
		if e, ok := ev.(wire.ReadMarkerEvent); ok {
			keys = append(keys, e.Key)
			if irc.IsServerName(e.Key) || (e.Key == wire.ServerBufferKey && e.Buffer != wire.ServerBuffer) {
				t.Fatalf("read_marker %+v: modules get the Server buffer, not a server's name", e)
			}
		}
	}
	found := false
	for _, k := range keys {
		found = found || k == wire.ServerBufferKey
	}
	if !found {
		t.Fatalf("read_marker keys = %v, want the Server buffer's *", keys)
	}
	marked := false
	for _, m := range d.sent {
		if m.Command == "MARKREAD" && m.Param(0) == wire.ServerBufferKey {
			t.Fatalf("MARKREAD * to an IRC client: %+v", m)
		}
		marked = marked || (m.Command == "MARKREAD" && m.Param(0) == "srv.example")
	}
	if !marked {
		t.Fatalf("no MARKREAD srv.example to the IRC client: %+v", d.sent)
	}
}
