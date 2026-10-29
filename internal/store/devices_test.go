package store

import (
	"context"
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
		_, _ = s.TouchDevice(ctx, d, "")
	}
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
