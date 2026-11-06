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

	_, _, _ = s.SetReadMarkerIfNewer(ctx, nid, "alice", "2026-11-06T10:00:00Z")
	if both, _ := s.DeviceHoldsBoth(ctx, nid, "alice", "bob"); !both {
		t.Fatal("DeviceHoldsBoth = false with the desktop holding both")
	}

	changed, kept, err := s.RenameBuffer(ctx, nid, "alice", "bob", "Bob")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(changed, []string{"phone"}) || !kept {
		t.Fatalf("changed = %v kept = %v, want phone renamed and the desktop keeping both", changed, kept)
	}
	if got, want := bufRows(t, s, "phone", nid), []OpenBuffer{{Target: "bob", Display: "Bob", Open: true, RenamedFrom: "Alice"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("phone = %+v, want %+v", got, want)
	}
	// Keep both (owner ruling): the desktop already had bob, so both rows stay as they were.
	if got, want := bufRows(t, s, "desktop", nid), []OpenBuffer{{Target: "alice", Display: "alice", ClosedAtID: 30}, {Target: "bob", Display: "bob", Open: true, ClosedAtID: 10}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("desktop = %+v, want %+v", got, want)
	}
	if got := bufRows(t, s, "tablet", nid); len(got) != 1 || got[0].Target != "carol" || got[0].RenamedFrom != "" {
		t.Fatalf("tablet = %+v", got)
	}
	// A device kept both: the read marker stays with alice.
	if ts, ok, _ := s.GetReadMarker(ctx, nid, "alice"); !ok || ts == "" {
		t.Fatal("alice's read marker moved although a device kept both")
	}
}

func TestPlainRenameMovesTheReadMarker(t *testing.T) {
	s, nid := deviceStore(t)
	ctx := context.Background()
	_ = s.SetBufferOpen(ctx, "phone", nid, "alice", "alice", true, 0)
	_, _, _ = s.SetReadMarkerIfNewer(ctx, nid, "alice", "2026-11-06T10:00:00Z")
	if _, kept, err := s.RenameBuffer(ctx, nid, "alice", "bob", "bob"); err != nil || kept {
		t.Fatalf("kept = %v err = %v", kept, err)
	}
	if _, ok, _ := s.GetReadMarker(ctx, nid, "bob"); !ok {
		t.Fatal("bob has no read marker after a plain rename")
	}
	if _, ok, _ := s.GetReadMarker(ctx, nid, "alice"); ok {
		t.Fatal("alice kept its read marker after it moved")
	}
}

func TestRenameBufferKeepsAClosedRowClosed(t *testing.T) {
	s, nid := deviceStore(t)
	ctx := context.Background()
	_ = s.SetBufferOpen(ctx, "phone", nid, "alice", "alice", false, 9)
	if _, _, err := s.RenameBuffer(ctx, nid, "alice", "bob", "bob"); err != nil {
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
	_, _, _ = s.RenameBuffer(ctx, nid, "alice", "bob", "bob")
	_, _, _ = s.RenameBuffer(ctx, nid, "bob", "rob", "rob")
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
	changed, _, err := s.RenameBuffer(ctx, nid, "alice", "alice", "Alice")
	if err != nil || !reflect.DeepEqual(changed, []string{"phone"}) {
		t.Fatalf("case-only: changed = %v err = %v", changed, err)
	}
	if got, want := bufRows(t, s, "phone", nid), []OpenBuffer{{Target: "alice", Display: "Alice", Open: true}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("phone = %+v, want %+v", got, want)
	}
	if changed, _, _ := s.RenameBuffer(ctx, nid, "nobody", "carol", "carol"); len(changed) != 0 {
		t.Fatalf("no row: changed = %v", changed)
	}
	if has, _ := s.HasBufferRows(ctx, nid, "alice"); !has {
		t.Fatal("HasBufferRows(alice) = false")
	}
	if has, _ := s.HasBufferRows(ctx, nid, "nobody"); has {
		t.Fatal("HasBufferRows(nobody) = true")
	}
}
