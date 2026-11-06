package store

import (
	"context"
	"reflect"
	"testing"
)

// A query renamed by its peer's NICK: every device's row follows, keeping open/closed and the
// floor, merging into a row for the new nick, and remembering the old name until a sync clears it.

func bufRows(t *testing.T, s *Store, device string, nid int64) []OpenBuffer {
	t.Helper()
	rows, err := s.ListOpenBuffers(context.Background(), device, nid)
	if err != nil {
		t.Fatal(err)
	}
	if rows == nil {
		rows = []OpenBuffer{}
	}
	return rows
}

func TestRenameBufferAcrossDevices(t *testing.T) {
	s, nid := deviceStore(t)
	ctx := context.Background()
	_ = s.SetBufferOpen(ctx, "phone", nid, "alice", "Alice", true, 0)
	_ = s.SetBufferOpen(ctx, "desktop", nid, "alice", "alice", false, 30)
	_ = s.SetBufferOpen(ctx, "desktop", nid, "bob", "bob", true, 10)
	_ = s.SetBufferOpen(ctx, "tablet", nid, "carol", "carol", true, 0)

	changed, err := s.RenameBuffer(ctx, nid, "alice", "bob", "Bob")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(changed, []string{"desktop", "phone"}) {
		t.Fatalf("changed = %v", changed)
	}
	if got, want := bufRows(t, s, "phone", nid), []OpenBuffer{{Target: "bob", Display: "Bob", Open: true, RenamedFrom: "Alice"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("phone = %+v, want %+v", got, want)
	}
	// Merged: the closed alice into the open bob, the higher floor kept.
	if got, want := bufRows(t, s, "desktop", nid), []OpenBuffer{{Target: "bob", Display: "Bob", Open: true, ClosedAtID: 30, RenamedFrom: "alice"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("desktop = %+v, want %+v", got, want)
	}
	if got := bufRows(t, s, "tablet", nid); len(got) != 1 || got[0].Target != "carol" || got[0].RenamedFrom != "" {
		t.Fatalf("tablet = %+v", got)
	}
}

func TestRenameBufferKeepsAClosedRowClosed(t *testing.T) {
	s, nid := deviceStore(t)
	ctx := context.Background()
	_ = s.SetBufferOpen(ctx, "phone", nid, "alice", "alice", false, 9)
	if _, err := s.RenameBuffer(ctx, nid, "alice", "bob", "bob"); err != nil {
		t.Fatal(err)
	}
	if got, want := bufRows(t, s, "phone", nid), []OpenBuffer{{Target: "bob", Display: "bob", ClosedAtID: 9, RenamedFrom: "alice"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("phone = %+v, want %+v", got, want)
	}
}

func TestRenameBufferTwiceKeepsTheFirstNameAndClearIsExact(t *testing.T) {
	s, nid := deviceStore(t)
	ctx := context.Background()
	_ = s.SetBufferOpen(ctx, "phone", nid, "alice", "alice", true, 0)
	_, _ = s.RenameBuffer(ctx, nid, "alice", "bob", "bob")
	_, _ = s.RenameBuffer(ctx, nid, "bob", "rob", "rob")
	if got := bufRows(t, s, "phone", nid); len(got) != 1 || got[0].Target != "rob" || got[0].RenamedFrom != "alice" {
		t.Fatalf("phone = %+v, want rob renamed from alice", got)
	}
	// A stale clear (the row was renamed again since the sync read it) leaves it.
	if err := s.ClearRenamedFrom(ctx, "phone", nid, "rob", "someone-else"); err != nil {
		t.Fatal(err)
	}
	if got := bufRows(t, s, "phone", nid); got[0].RenamedFrom != "alice" {
		t.Fatalf("a stale clear cleared: %+v", got)
	}
	if err := s.ClearRenamedFrom(ctx, "phone", nid, "rob", "alice"); err != nil {
		t.Fatal(err)
	}
	if got := bufRows(t, s, "phone", nid); got[0].RenamedFrom != "" || !got[0].Open {
		t.Fatalf("after the sync = %+v", got)
	}
}

func TestRenameBufferCaseOnlyAndNoRow(t *testing.T) {
	s, nid := deviceStore(t)
	ctx := context.Background()
	_ = s.SetBufferOpen(ctx, "phone", nid, "alice", "alice", true, 0)
	changed, err := s.RenameBuffer(ctx, nid, "alice", "alice", "Alice")
	if err != nil || !reflect.DeepEqual(changed, []string{"phone"}) {
		t.Fatalf("case-only: changed = %v err = %v", changed, err)
	}
	if got, want := bufRows(t, s, "phone", nid), []OpenBuffer{{Target: "alice", Display: "Alice", Open: true}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("phone = %+v, want %+v", got, want)
	}
	if changed, _ := s.RenameBuffer(ctx, nid, "nobody", "carol", "carol"); len(changed) != 0 {
		t.Fatalf("no row: changed = %v", changed)
	}
	if has, _ := s.HasBufferRows(ctx, nid, "alice"); !has {
		t.Fatal("HasBufferRows(alice) = false")
	}
	if has, _ := s.HasBufferRows(ctx, nid, "nobody"); has {
		t.Fatal("HasBufferRows(nobody) = true")
	}
}
