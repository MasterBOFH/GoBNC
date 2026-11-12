package module

import (
	"context"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// A module reads the Server buffer by the name "Server" (any case), as before; its rows are
// those stored under every server's name, together by id. A real nick Server's query rows
// ("server") are not the Server buffer's.
func TestModuleReadsTheServerBufferFromServerTargets(t *testing.T) {
	tc, db, nid, _ := historyFixture(t, "history", "read_markers")
	ctx := context.Background()
	t0 := time.Now().Add(-time.Hour).UTC()
	var srvIDs []int64
	for i, l := range []struct{ target, src, text string }{
		{"srv.example", "srv.example", "*** Notice -- snotice"},
		{"server", "Server!s@h", "hi from a nick"},
		{"services.example", "services.example", "*** services notice"},
	} {
		id, err := db.InsertMessage(ctx, store.Message{NetworkID: nid, Target: l.target,
			Time: t0.Add(time.Duration(i) * time.Second), Command: "NOTICE", Source: l.src, Raw: "x", Text: l.text})
		if err != nil {
			t.Fatal(err)
		}
		if l.target != "server" {
			srvIDs = append(srvIDs, id)
		}
	}
	for _, name := range []string{wire.ServerBuffer, "server"} {
		var res wire.HistoryMessagesResult
		tc.call(wire.MethodHistoryMessages, wire.HistoryMessagesParams{Network: nid, Buffer: name}, &res)
		res.Messages = notices(res.Messages) // the Server buffer also holds the fixture's registration rows (A3131)
		if len(res.Messages) != 2 || res.Messages[0].ID != srvIDs[0] || res.Messages[1].ID != srvIDs[1] ||
			res.Messages[0].Buffer != name {
			t.Fatalf("history.messages %q = %+v, want the two server notices", name, res.Messages)
		}
	}
	if _, _, err := db.SetReadMarkerIfNewer(ctx, nid, wire.ServerBufferKey, t0.Add(time.Minute).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	var mark wire.ReadMarkerGetResult
	tc.call(wire.MethodReadMarkersGet, wire.ReadMarkerGetParams{Network: nid, Buffer: wire.ServerBuffer}, &mark)
	if mark.ID != srvIDs[1] {
		t.Fatalf("read_markers.get Server = %+v, want id %d", mark, srvIDs[1])
	}
	var set wire.ReadMarkerSetResult
	tc.call(wire.MethodReadMarkersSet, wire.ReadMarkerSetParams{Network: nid, Buffer: wire.ServerBuffer, ID: srvIDs[1]}, &set)
	if set.StoredID != srvIDs[1] {
		t.Fatalf("read_markers.set Server = %+v", set)
	}

	var act wire.HistoryActivityResult
	tc.call(wire.MethodHistoryActivity, wire.HistoryActivityParams{Network: nid, Device: "phone"}, &act)
	for _, b := range act.Buffers {
		if b.Buffer == wire.ServerBuffer {
			if b.Key != wire.ServerBufferKey || b.LastID != srvIDs[1] {
				t.Fatalf("Server activity = %+v, want key * and the last notice's id", b)
			}
			return
		}
	}
	t.Fatalf("activity = %+v, no Server buffer", act.Buffers)
}

// notices keeps a page's NOTICE rows: the fixture's network registered, so the Server buffer
// also holds its burst numerics and State row (Rete A3131).
func notices(msgs []wire.HistoryMessage) []wire.HistoryMessage {
	var out []wire.HistoryMessage
	for _, m := range msgs {
		if m.Command == "NOTICE" {
			out = append(out, m)
		}
	}
	return out
}
