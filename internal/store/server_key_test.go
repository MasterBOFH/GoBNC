package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// The Server buffer was stored under its name's fold, "server", which a real nick Server's
// query shares. Open moves the rows from a server to that server's name, and rows with no
// source or to a $mask to the network's host when it is a server name, keeping their ids; the
// query's rows stay. Running it again changes nothing.
func TestOpenMovesServerRowsToTheServersName(t *testing.T) {
	for _, tc := range []struct {
		host, unattributed string
	}{
		{"irc.example.net", "irc.example.net"},
		{"localhost", "server"}, // no server name to give them: they stay
	} {
		t.Run(tc.host, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "t.db")
			db, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			nid, err := db.UpsertNetwork(ctx, Network{Name: "n", Host: tc.host, Port: 1, Nick: "me", Enabled: true})
			if err != nil {
				t.Fatal(err)
			}
			t0 := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
			rows := []struct{ source, raw, want string }{
				{"Hub.Example.NET", ":Hub.Example.NET NOTICE me :*** Notice -- snotice", "hub.example.net"},
				{"services.example.net", "@time=2026-11-01T12:00:00Z :services.example.net NOTICE * :*** Looking up your hostname", "services.example.net"},
				{"", "NOTICE AUTH :*** no prefix", tc.unattributed},
				{"oper!o@h", ":oper!o@h NOTICE $*.example :global notice", tc.unattributed},
				{"Server!s@h", ":Server!s@h PRIVMSG me :hi from a nick", "server"},
				{"me!u@h", ":me!u@h PRIVMSG Server :hi back", "server"},
				{"me", ":me PRIVMSG Server :bare own echo", "server"},
				{"Server!s@h", ":Server!s@h NOTICE me :a notice from the nick", "server"},
			}
			ids := make([]int64, len(rows))
			for i, r := range rows {
				if ids[i], err = db.InsertMessage(ctx, Message{NetworkID: nid, Target: "server", Time: t0.Add(time.Duration(i) * time.Second),
					Command: "NOTICE", Source: r.source, Raw: r.raw, Text: "x"}); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			for run := 1; run <= 2; run++ {
				db, err = Open(path)
				if err != nil {
					t.Fatalf("open %d: %v", run, err)
				}
				for i, r := range rows {
					m, err := db.MessageByID(ctx, nid, ids[i])
					if err != nil || m == nil {
						t.Fatalf("run %d: row %d gone (err %v)", run, ids[i], err)
					}
					if m.Target != r.want {
						t.Errorf("run %d: %q stored under %q, want %q", run, r.raw, m.Target, r.want)
					}
				}
				if run == 2 {
					got, err := db.ServerTargets(ctx, nid)
					want := []string{"hub.example.net", "services.example.net"}
					if tc.unattributed != "server" {
						want = []string{"hub.example.net", "irc.example.net", "services.example.net"}
					}
					if err != nil || len(got) != len(want) {
						t.Fatalf("ServerTargets = %v (err %v), want %v", got, err, want)
					}
					for i := range want {
						if got[i] != want[i] {
							t.Fatalf("ServerTargets = %v, want %v", got, want)
						}
					}
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// The Server buffer's read marker and device rows follow its rows to the Server buffer's
// module key "*" when nothing is left under "server" (no query of a nick Server shares the
// key); when a query remains they stay, as nothing tells whose they are. A row already stored
// under the server's name for the same keeper line wins over the moved duplicate.
func TestOpenMovesServerBufferMarkersOnlyWhenUnambiguous(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	only, err := db.UpsertNetwork(ctx, Network{Name: "only", Host: "h", Port: 1, Nick: "me", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	shared, err := db.UpsertNetwork(ctx, Network{Name: "shared", Host: "h", Port: 1, Nick: "me", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	insert := func(nid int64, target, source, raw string, seq uint64, at int) int64 {
		t.Helper()
		id, err := db.InsertMessage(ctx, Message{NetworkID: nid, Target: target, Time: t0.Add(time.Duration(at) * time.Second),
			Command: "NOTICE", Source: source, Raw: raw, Text: "x", KeeperSeq: seq})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	keptID := insert(only, "srv.example", "srv.example", ":srv.example NOTICE me :dup", 7, 0)
	insert(only, "server", "srv.example", ":srv.example NOTICE me :dup", 7, 0) // the same keeper line
	moved := insert(only, "server", "srv.example", ":srv.example NOTICE me :snotice", 8, 1)
	insert(shared, "server", "srv.example", ":srv.example NOTICE me :snotice", 1, 0)
	query := insert(shared, "server", "Server!s@h", ":Server!s@h PRIVMSG me :hi", 2, 1)
	ts := t0.Add(time.Minute).Format(time.RFC3339Nano)
	for _, nid := range []int64{only, shared} {
		if _, _, err := db.SetReadMarkerIfNewer(ctx, nid, "server", ts); err != nil {
			t.Fatal(err)
		}
		if _, _, err := db.SetPlaybackCursorIfNewer(ctx, nid, "server", ts); err != nil {
			t.Fatal(err)
		}
		if err := db.SetBufferOpen(ctx, "phone", nid, "server", "Server", false, moved); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if db, err = Open(path); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	srv, err := db.MessagesLatest(ctx, only, "srv.example", 10)
	if err != nil || len(srv) != 2 || srv[0].ID != keptID || srv[1].ID != moved {
		t.Fatalf("only: srv.example rows = %+v (err %v), want the existing row and the moved one", srv, err)
	}
	if ok, _ := db.HasTarget(ctx, only, "server"); ok {
		t.Fatal("only: rows left under server")
	}
	check := func(nid int64, want, gone string, cursor bool) {
		t.Helper()
		if _, ok, _ := db.GetReadMarker(ctx, nid, want); !ok {
			t.Errorf("network %d: no read marker under %q", nid, want)
		}
		if _, ok, _ := db.GetReadMarker(ctx, nid, gone); ok {
			t.Errorf("network %d: read marker left under %q", nid, gone)
		}
		if _, ok, _ := db.GetPlaybackCursor(ctx, nid, "server"); ok != cursor {
			t.Errorf("network %d: playback cursor for server present %v, want %v", nid, ok, cursor)
		}
		rows, err := db.ListOpenBuffers(ctx, "phone", nid)
		if err != nil || len(rows) != 1 || rows[0].Target != want || rows[0].ClosedAtID != moved {
			t.Errorf("network %d: device rows = %+v (err %v), want one under %q", nid, rows, err, want)
		}
	}
	check(only, wire.ServerBufferKey, "server", false)
	check(shared, "server", wire.ServerBufferKey, true)
	if m, _ := db.MessageByID(ctx, shared, query); m == nil || m.Target != "server" {
		t.Fatalf("shared: the query's row = %+v, want it under server", m)
	}
}

// The Server buffer ("*") reads every server-name target of a network together, by id; other
// targets (a nick Server's "server", channels, user@host addressing) are not in it.
func TestServerBufferReadsEveryServerTarget(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	nid, err := db.UpsertNetwork(ctx, Network{Name: "n", Host: "h", Port: 1, Nick: "me", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := db.MessagesLatest(ctx, nid, wire.ServerBufferKey, 10); len(got) != 0 {
		t.Fatalf("empty Server buffer = %+v", got)
	}
	t0 := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	var want []int64
	for i, target := range []string{"hub.example.net", "server", "#rete.dev", "services.example.net", "x@channels.example.net", "hub.example.net"} {
		id, err := db.InsertMessage(ctx, Message{NetworkID: nid, Target: target, Time: t0.Add(time.Duration(i) * time.Second),
			Command: "NOTICE", Source: "x", Raw: "x", Text: "x"})
		if err != nil {
			t.Fatal(err)
		}
		if InBuffer(target, wire.ServerBufferKey) {
			want = append(want, id)
		}
	}
	got, err := db.MessagesLatest(ctx, nid, wire.ServerBufferKey, 10)
	if err != nil || len(got) != 3 || got[0].ID != want[0] || got[1].ID != want[1] || got[2].ID != want[2] {
		t.Fatalf("Server buffer = %+v (err %v), want ids %v", got, err, want)
	}
	if before, _ := db.MessagesBefore(ctx, nid, wire.ServerBufferKey, want[2], 10); len(before) != 2 {
		t.Fatalf("MessagesBefore = %+v, want 2", before)
	}
	if ok, _ := db.HasMessageAfter(ctx, nid, wire.ServerBufferKey, want[2]); ok {
		t.Fatal("HasMessageAfter the newest")
	}
	if ok, _ := db.HasMessageBefore(ctx, nid, wire.ServerBufferKey, want[1]); !ok {
		t.Fatal("no HasMessageBefore the second")
	}
	a, err := db.BufferActivity(ctx, nid, wire.ServerBufferKey, time.Time{}, 0)
	if err != nil || a.LastID != want[2] || len(a.Unread) != 3 {
		t.Fatalf("activity = %+v (err %v)", a, err)
	}
	if id, _ := db.IDAtOrBefore(ctx, nid, wire.ServerBufferKey, t0.Add(4*time.Second)); id != want[1] {
		t.Fatalf("IDAtOrBefore = %d, want %d", id, want[1])
	}
	if id, _ := db.FirstUnreadID(ctx, nid, wire.ServerBufferKey, t0.Add(time.Second), 0); id != want[1] {
		t.Fatalf("FirstUnreadID = %d, want %d", id, want[1])
	}
}
