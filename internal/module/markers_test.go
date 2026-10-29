package module

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// ircDownlink is an attached IRC client stand-in recording what Session sends it.
type ircDownlink struct {
	id   session.ClientID
	caps map[string]bool
	mu   sync.Mutex
	sent []irc.Message
}

func (d *ircDownlink) ID() session.ClientID     { return d.id }
func (d *ircDownlink) Caps() map[string]bool    { return d.caps }
func (d *ircDownlink) HasCap(n string) bool     { return d.caps[n] }
func (d *ircDownlink) ClearCap(n string)        {}
func (d *ircDownlink) HasSeenCap(n string) bool { return true }
func (d *ircDownlink) MarkSeenCap(n string)     {}
func (d *ircDownlink) Close() error             { return nil }
func (d *ircDownlink) RemoteAddr() string       { return "127.0.0.1" }
func (d *ircDownlink) Send(m irc.Message) error {
	d.mu.Lock()
	d.sent = append(d.sent, m)
	d.mu.Unlock()
	return nil
}

func (d *ircDownlink) commands(cmd string) []irc.Message {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []irc.Message
	for _, m := range d.sent {
		if m.Command == cmd {
			out = append(out, m)
		}
	}
	return out
}

func TestReadMarkerSetFromModule(t *testing.T) {
	tc, db, nid, sess := historyFixture(t, "read_markers", "events")
	ids := seedHistory(t, db, nid, "#c", 3)
	dl := &ircDownlink{id: "irc1", caps: map[string]bool{"draft/read-marker": true}}
	if err := sess.Attach(dl); err != nil {
		t.Fatal(err)
	}

	before := len(dl.commands("MARKREAD")) // Attach's own per-channel MARKREAD
	var res wire.ReadMarkerSetResult
	tc.call(wire.MethodReadMarkersSet, wire.ReadMarkerSetParams{Network: nid, Buffer: "#c", ID: ids[1]}, &res)
	if !res.Updated || res.StoredID != ids[1] {
		t.Fatalf("set = %+v, want updated to %d", res, ids[1])
	}
	marks := dl.commands("MARKREAD")[before:]
	if len(marks) != 1 || marks[0].Param(0) != "#c" || marks[0].Param(1) == "*" {
		t.Fatalf("IRC client got MARKREAD %+v after the module's set", marks)
	}
	tc.call(wire.MethodReadMarkersSet, wire.ReadMarkerSetParams{Network: nid, Buffer: "#c", ID: ids[0]}, &res)
	if res.Updated || res.StoredID != ids[1] {
		t.Fatalf("lower id moved the marker: %+v", res)
	}
	var got wire.ReadMarkerGetResult
	tc.call(wire.MethodReadMarkersGet, wire.ReadMarkerGetParams{Network: nid, Buffer: "#C"}, &got)
	if got.ID != ids[1] || got.Time == "" {
		t.Fatalf("get = %+v, want id %d", got, ids[1])
	}
}

func TestMARKREADFromIRCReachesModule(t *testing.T) {
	tc, db, nid, sess := historyFixture(t, "read_markers", "events")
	ids := seedHistory(t, db, nid, "#c", 2)
	tc.call(wire.MethodEventsSubscribe, wire.SubscribeParams{Network: nid}, nil)
	dl := &ircDownlink{id: "irc1", caps: map[string]bool{"draft/read-marker": true}}
	if err := sess.Attach(dl); err != nil {
		t.Fatal(err)
	}
	ts := time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC).Format(time.RFC3339Nano) // message 2's time
	msg, _ := irc.Parse("MARKREAD #c timestamp=" + ts)
	if err := sess.HandleClientMessage(dl, msg); err != nil {
		t.Fatal(err)
	}
	var ev wire.ReadMarkerEvent
	if err := json.Unmarshal(tc.nextEvent(wire.EventReadMarker), &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Network != nid || ev.Key != "#c" || ev.ID != ids[1] {
		t.Fatalf("read_marker event = %+v, want id %d", ev, ids[1])
	}
}
