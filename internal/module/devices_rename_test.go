package module

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// A query renamed by its peer's NICK: devices.buffers lists renamed_from until
// devices.clear_renames clears what a sync reported; buffer_rename reaches the module.

func TestDeviceBuffersListAndClearRenames(t *testing.T) {
	tc, db, nid, _ := historyFixture(t, "devices")
	ctx := context.Background()
	if err := db.SetBufferOpen(ctx, "phone", nid, "alice", "alice", true, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RenameBuffer(ctx, nid, "alice", "bob", "Bob"); err != nil {
		t.Fatal(err)
	}
	var bufs wire.DeviceBuffersResult
	tc.call(wire.MethodDevicesBuffers, wire.DeviceNetworkParams{ClientID: "phone", Network: nid}, &bufs)
	want := wire.DeviceBuffer{Buffer: "Bob", Key: "bob", Open: true, RenamedFrom: "alice"}
	if len(bufs.Buffers) != 1 || bufs.Buffers[0] != want {
		t.Fatalf("buffers = %+v, want %+v", bufs.Buffers, want)
	}

	tc.call(wire.MethodDevicesClearRenames, wire.DeviceClearRenamesParams{ClientID: "phone", Network: nid, Buffers: bufs.Buffers}, nil)
	bufs = wire.DeviceBuffersResult{}
	tc.call(wire.MethodDevicesBuffers, wire.DeviceNetworkParams{ClientID: "phone", Network: nid}, &bufs)
	if len(bufs.Buffers) != 1 || bufs.Buffers[0].RenamedFrom != "" {
		t.Fatalf("after clear_renames = %+v", bufs.Buffers)
	}
	if code := tc.callErr(wire.MethodDevicesClearRenames, wire.DeviceClearRenamesParams{Network: nid}); code != wire.CodeBadParams {
		t.Fatalf("no client_id: code %q", code)
	}
}

func TestBufferRenameEventName(t *testing.T) {
	if got := eventName(wire.BufferRenameEvent{}); got != wire.EventBufferRename {
		t.Fatalf("eventName = %q", got)
	}
	b, _ := json.Marshal(wire.BufferRenameEvent{Network: 1, ClientID: "phone", From: "alice", FromKey: "alice", To: "Bob", ToKey: "bob"})
	if string(b) != `{"network":1,"client_id":"phone","from":"alice","from_key":"alice","to":"Bob","to_key":"bob"}` {
		t.Fatalf("buffer_rename = %s", b)
	}
}
