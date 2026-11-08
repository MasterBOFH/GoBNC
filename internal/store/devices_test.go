package store

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
)

func deviceStore(t *testing.T) (*Store, int64) {
	t.Helper()
	s := openTemp(t)
	nid, err := s.UpsertNetwork(context.Background(), Network{Name: "n", Host: "h", Port: 1, Nick: "me", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	return s, nid
}

// HasDevices is false until a device row exists (TouchDevice, or a row that needs one), and is
// read back from the database when it is opened again.
func TestHasDevices(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if s.HasDevices() {
		t.Fatal("a new store has devices")
	}
	if _, err := s.TouchDevice(context.Background(), "phone", "Phone"); err != nil {
		t.Fatal(err)
	}
	if !s.HasDevices() {
		t.Fatal("no devices after TouchDevice")
	}
	_ = s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if !s.HasDevices() {
		t.Fatal("reopened store forgot its devices")
	}

	other, nid := deviceStore(t)
	if err := other.SetDeviceNetwork(context.Background(), "pad", nid, true); err != nil {
		t.Fatal(err)
	}
	if !other.HasDevices() {
		t.Fatal("no devices after a row that needs one")
	}
}

func TestTouchDeviceAndToggles(t *testing.T) {
	s, nid := deviceStore(t)
	ctx := context.Background()
	if created, err := s.TouchDevice(ctx, "phone", "iPhone"); err != nil || !created {
		t.Fatalf("first touch: created=%v err=%v", created, err)
	}
	if created, _ := s.TouchDevice(ctx, "phone", ""); created {
		t.Fatal("second touch reported created")
	}
	if m, _ := s.ListDeviceNetworks(ctx, "phone"); m[nid] {
		t.Fatal("never-set toggle is on")
	}
	_ = s.SetDeviceNetwork(ctx, "phone", nid, true)
	if m, _ := s.ListDeviceNetworks(ctx, "phone"); !m[nid] {
		t.Fatal("toggle not stored")
	}
	_ = s.SetDeviceNetwork(ctx, "phone", nid, false)
	if m, _ := s.ListDeviceNetworks(ctx, "phone"); m[nid] {
		t.Fatal("toggle not cleared")
	}
}

func TestBufferFloorOnlyRises(t *testing.T) {
	s, nid := deviceStore(t)
	ctx := context.Background()
	_ = s.SetBufferOpen(ctx, "phone", nid, "bob", "Bob", false, 50)
	_ = s.SetBufferOpen(ctx, "phone", nid, "bob", "Bob", false, 20)
	if id, _ := s.BufferClosedAt(ctx, "phone", nid, "bob"); id != 50 {
		t.Fatalf("floor = %d after a lower close, want 50", id)
	}
	_ = s.SetBufferOpen(ctx, "phone", nid, "bob", "Bob", true, 0)
	bufs, _ := s.ListOpenBuffers(ctx, "phone", nid)
	if len(bufs) != 1 || !bufs[0].Open || bufs[0].ClosedAtID != 50 {
		t.Fatalf("buffers = %+v", bufs)
	}
}

func TestReopenForMessageChangesOnlyNotOpenDevices(t *testing.T) {
	s, nid := deviceStore(t)
	ctx := context.Background()
	for _, d := range []string{"open", "closed", "none"} {
		_ = s.SetDeviceNetwork(ctx, d, nid, true)
	}
	_ = s.SetDeviceNetwork(ctx, "elsewhere", nid, false) // does not follow this network
	_ = s.SetBufferOpen(ctx, "open", nid, "bob", "bob", true, 0)
	_ = s.SetBufferOpen(ctx, "closed", nid, "bob", "bob", false, 10)

	changed, err := s.ReopenForMessage(ctx, nid, "bob", "Bob", 42)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(changed, []string{"closed", "none"}) {
		t.Fatalf("changed = %v, want [closed none]", changed)
	}
	if id, _ := s.BufferClosedAt(ctx, "closed", nid, "bob"); id != 41 {
		t.Fatalf("reopened floor = %d, want 41 (id-1)", id)
	}
	if again, _ := s.ReopenForMessage(ctx, nid, "bob", "Bob", 43); len(again) != 0 {
		t.Fatalf("second message changed %v", again)
	}
	if has, _ := s.DeviceHasBuffers(ctx, "none", nid); !has {
		t.Fatal("device with no row got none")
	}
}

func TestDeletingNetworkCascadesDeviceRows(t *testing.T) {
	s, nid := deviceStore(t)
	ctx := context.Background()
	_ = s.SetDeviceNetwork(ctx, "phone", nid, true)
	_ = s.SetBufferOpen(ctx, "phone", nid, "bob", "bob", true, 0)
	if err := s.DeleteNetwork(ctx, "n"); err != nil {
		t.Fatal(err)
	}
	if m, _ := s.ListDeviceNetworks(ctx, "phone"); len(m) != 0 {
		t.Fatalf("device_networks left: %v", m)
	}
	if bufs, _ := s.ListOpenBuffers(ctx, "phone", nid); len(bufs) != 0 {
		t.Fatalf("open_buffers left: %v", bufs)
	}
	if devs, _ := s.ListDevices(ctx); len(devs) != 1 {
		t.Fatalf("device itself should stay: %v", devs)
	}
	_ = s.DeleteDevice(ctx, "phone")
	if devs, _ := s.ListDevices(ctx); len(devs) != 0 {
		t.Fatalf("device not deleted: %v", devs)
	}
}

func TestReopenSkipsAReplayOfALineClosedAfter(t *testing.T) {
	s, nid := deviceStore(t)
	ctx := context.Background()
	_ = s.SetDeviceNetwork(ctx, "phone", nid, true)
	_ = s.SetBufferOpen(ctx, "phone", nid, "bob", "bob", false, 100) // closed after line 100
	if changed, _ := s.ReopenForMessage(ctx, nid, "bob", "Bob", 100); len(changed) != 0 {
		t.Fatalf("replay of line 100 reopened %v", changed)
	}
	if changed, _ := s.ReopenForMessage(ctx, nid, "bob", "Bob", 101); len(changed) != 1 {
		t.Fatalf("a new line did not reopen: %v", changed)
	}
}

func TestDeviceSeededFlag(t *testing.T) {
	s, nid := deviceStore(t)
	ctx := context.Background()
	if ok, _ := s.DeviceSeeded(ctx, "phone", nid); ok {
		t.Fatal("seeded before marking")
	}
	_ = s.MarkDeviceSeeded(ctx, "phone", nid)
	if ok, _ := s.DeviceSeeded(ctx, "phone", nid); !ok {
		t.Fatal("not seeded after marking")
	}
}
