package session

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/history"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// The per-device query rules cost an IRC-only bouncer nothing: while the store has no device
// (HasDevices, cached), a query line opens nothing and a peer's NICK renames nothing and adds
// no query copy. The rows a device would hold are put in the database behind the session's
// store (a second handle on the same file), so its cache still says no device: had the rules
// run, they would have found and changed them.
func TestDeviceRulesSkippedWithoutDevices(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.db")
	db, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	nid, err := db.UpsertNetwork(context.Background(), store.Network{Name: "n", Host: "h", Port: 1, Nick: "me", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	s := New(store.Network{ID: nid, Name: "n", Nick: "me"}, db, history.New(db), nil, nil)
	feed(s, ":srv.example 001 me :Welcome", ":srv.example 005 me CHANTYPES=# PREFIX=(ov)@+ :are supported",
		":srv.example 376 me :End of MOTD", ":peer!p@h PRIVMSG me :earlier query line")
	sink := &recSink{}
	s.Subscribe(1, sink, nil, nil)

	behind, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = behind.Close() })
	ctx := context.Background()
	if err := behind.SetBufferOpen(ctx, "phone", nid, "peer", "peer", false, 1); err != nil { // closed
		t.Fatal(err)
	}
	if db.HasDevices() {
		t.Fatal("the session's store saw the device")
	}

	feed(s, ":peer!p@h PRIVMSG me :new dm", ":peer!p@h NICK :peer2")
	for _, ev := range sink.events() {
		switch e := ev.(type) {
		case wire.BufferOpenEvent, wire.BufferRenameEvent:
			t.Fatalf("device rule ran without devices: %+v", e)
		}
	}
	rows, err := behind.ListOpenBuffers(ctx, "phone", nid)
	if err != nil || len(rows) != 1 || rows[0].Target != "peer" || rows[0].Open {
		t.Fatalf("device rows changed: %+v, %v", rows, err)
	}
	if ok, _ := db.HasTarget(ctx, nid, "peer2"); ok {
		t.Fatal("the NICK was stored as a query copy without devices")
	}
	msgs, err := db.MessagesLatest(ctx, nid, "peer", 10)
	if err != nil || len(msgs) != 2 || msgs[1].Text != "new dm" {
		t.Fatalf("query rows = %+v, %v", msgs, err)
	}
}
