package module

import (
	"context"
	"testing"

	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

// devices.seed with baseline sets the read marker of a buffer nobody has read (the device's
// first seed only) and needs read_markers; without baseline it sets none.
func TestDevicesSeedBaseline(t *testing.T) {
	tc, db, nid, _ := historyFixture(t, wire.FeatureDevices, wire.FeatureReadMarkers)
	ids := seedHistory(t, db, nid, "#c", 3)
	seedHistory(t, db, nid, "#d", 1)

	var res wire.DeviceSeedResult
	tc.call(wire.MethodDevicesSeed, wire.DeviceSeedParams{ClientID: "plain", Network: nid}, &res)
	if _, ok, _ := db.GetReadMarker(context.Background(), nid, "#c"); ok {
		t.Fatal("a seed without baseline set a marker")
	}

	tc.call(wire.MethodDevicesSeed, wire.DeviceSeedParams{ClientID: "phone", Network: nid, Baseline: true}, &res)
	var mark wire.ReadMarkerGetResult
	tc.call(wire.MethodReadMarkersGet, wire.ReadMarkerGetParams{Network: nid, Buffer: "#c"}, &mark)
	if mark.ID != ids[2] {
		t.Fatalf("#c marker id = %d, want the newest line %d", mark.ID, ids[2])
	}
}

func TestDevicesSeedBaselineNeedsReadMarkers(t *testing.T) {
	tc, db, nid, _ := historyFixture(t, wire.FeatureDevices)
	seedHistory(t, db, nid, "#c", 1)
	if code := tc.callErr(wire.MethodDevicesSeed, wire.DeviceSeedParams{ClientID: "phone", Network: nid, Baseline: true}); code != wire.CodeNotGranted {
		t.Fatalf("code = %q, want not_granted", code)
	}
	if _, ok, _ := db.GetReadMarker(context.Background(), nid, "#c"); ok {
		t.Fatal("a refused baseline set a marker")
	}
}
