package module

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/auth"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

func TestDeviceToggleRoundTrip(t *testing.T) {
	tc, _, nid, _ := historyFixture(t, "devices")
	var touched wire.DeviceTouchResult
	tc.call(wire.MethodDevicesTouch, wire.DeviceTouchParams{ClientID: "phone", Name: "iPhone"}, &touched)
	if !touched.Created {
		t.Fatal("first touch not created")
	}
	var nets wire.DeviceNetworksResult
	tc.call(wire.MethodDevicesNetworks, wire.DeviceParams{ClientID: "phone"}, &nets)
	if len(nets.Networks) != 0 { // the fixture's fakeBackend lists no networks
		t.Fatalf("networks = %+v", nets)
	}
	if code := tc.callErr(wire.MethodDevicesSetNetwork, wire.DeviceSetNetworkParams{ClientID: "phone", Network: nid, Subscribed: true}); code != wire.CodeNotFound {
		t.Fatalf("unlisted network: code %q", code)
	}
}

func TestDeviceToggleListedNetworkAndEvent(t *testing.T) {
	tc, _, nid, _ := historyFixtureWith(t, func(b *fakeBackend, _ *Server, _ *store.Store, nid int64) {
		b.nets = []wire.NetworkInfo{{ID: nid, Name: "net"}}
	}, "devices")
	tc.call(wire.MethodDevicesSetNetwork, wire.DeviceSetNetworkParams{ClientID: "phone", Network: nid, Subscribed: true}, nil)
	var ev wire.DeviceNetworkEvent
	if err := json.Unmarshal(tc.nextEvent(wire.EventDeviceNetwork), &ev); err != nil {
		t.Fatal(err)
	}
	if ev.ClientID != "phone" || ev.Network != nid || !ev.Subscribed {
		t.Fatalf("device_network = %+v", ev)
	}
	var nets wire.DeviceNetworksResult
	tc.call(wire.MethodDevicesNetworks, wire.DeviceParams{ClientID: "phone"}, &nets)
	if len(nets.Networks) != 1 || !nets.Networks[0].Subscribed {
		t.Fatalf("networks = %+v", nets)
	}
}

func TestDeviceCloseReturnsNewestID(t *testing.T) {
	tc, db, nid, _ := historyFixture(t, "devices", "events")
	ids := seedHistory(t, db, nid, "bob", 3)
	tc.call(wire.MethodEventsSubscribe, wire.SubscribeParams{Network: nid}, nil)
	tc.call(wire.MethodDevicesOpen, wire.DeviceBufferParams{ClientID: "phone", Network: nid, Buffer: "Bob"}, nil)
	var open wire.BufferOpenEvent
	_ = json.Unmarshal(tc.nextEvent(wire.EventBufferOpen), &open)
	if open.Reason != wire.OpenUser || open.Key != "bob" {
		t.Fatalf("buffer_open = %+v", open)
	}
	var closed wire.DeviceCloseResult
	tc.call(wire.MethodDevicesClose, wire.DeviceBufferParams{ClientID: "phone", Network: nid, Buffer: "Bob"}, &closed)
	if closed.ClosedAtID != ids[2] {
		t.Fatalf("closed_at_id = %d, want %d", closed.ClosedAtID, ids[2])
	}
	var bufs wire.DeviceBuffersResult
	tc.call(wire.MethodDevicesBuffers, wire.DeviceNetworkParams{ClientID: "phone", Network: nid}, &bufs)
	if len(bufs.Buffers) != 1 || bufs.Buffers[0].Open || bufs.Buffers[0].ClosedAtID != ids[2] {
		t.Fatalf("buffers = %+v", bufs)
	}
}

// authFixture: password "s3cret" and fingerprint fp registered; the server clock is *now.
func authFixture(t *testing.T, allowPassword, allowCert bool, now *time.Time) (*testConn, string) {
	t.Helper()
	fp := strings.Repeat("ab", 32)
	tc, _, _, _ := historyFixtureWith(t, func(b *fakeBackend, srv *Server, db *store.Store, _ int64) {
		b.cfg.AllowPasswordAuth, b.cfg.AllowCertAuth = allowPassword, allowCert
		hash, err := auth.HashPassword("s3cret")
		if err != nil {
			t.Fatal(err)
		}
		if err := db.SetPasswordHash(context.Background(), hash); err != nil {
			t.Fatal(err)
		}
		if err := db.AddFingerprint(context.Background(), fp, "phone"); err != nil {
			t.Fatal(err)
		}
		if now != nil {
			srv.now = func() time.Time { return *now }
		}
	}, "auth")
	return tc, fp
}

func authOK(t *testing.T, tc *testConn, p wire.AuthCheckParams) bool {
	t.Helper()
	var res wire.AuthCheckResult
	tc.call(wire.MethodAuthCheck, p, &res)
	return res.OK
}

func TestAuthCheckPasswordAndCert(t *testing.T) {
	tc, fp := authFixture(t, true, true, nil)
	if !authOK(t, tc, wire.AuthCheckParams{Password: "s3cret", RemoteIP: "10.0.0.1"}) {
		t.Fatal("right password refused")
	}
	if authOK(t, tc, wire.AuthCheckParams{Password: "wrong", RemoteIP: "10.0.0.1"}) {
		t.Fatal("wrong password accepted")
	}
	colon := ""
	for i := 0; i < len(fp); i += 2 {
		if i > 0 {
			colon += ":"
		}
		colon += strings.ToUpper(fp[i : i+2])
	}
	if !authOK(t, tc, wire.AuthCheckParams{CertSHA256: colon, RemoteIP: "10.0.0.1"}) {
		t.Fatal("registered fingerprint (colon form) refused")
	}
	if authOK(t, tc, wire.AuthCheckParams{CertSHA256: strings.Repeat("cd", 32), RemoteIP: "10.0.0.1"}) {
		t.Fatal("unregistered fingerprint accepted")
	}
}

func TestAuthCheckPasswordDisabled(t *testing.T) {
	tc, _ := authFixture(t, false, true, nil)
	if authOK(t, tc, wire.AuthCheckParams{Password: "s3cret", RemoteIP: "10.0.0.1"}) {
		t.Fatal("password accepted with allow_password_auth=false")
	}
}

func TestAuthCheckRateLimit(t *testing.T) {
	now := time.Now()
	tc, _ := authFixture(t, true, true, &now)
	for i := 0; i < 5; i++ {
		authOK(t, tc, wire.AuthCheckParams{Password: "wrong", RemoteIP: "10.0.0.9"})
	}
	if authOK(t, tc, wire.AuthCheckParams{Password: "s3cret", RemoteIP: "10.0.0.9"}) {
		t.Fatal("right password accepted after 5 failures within the window")
	}
	if !authOK(t, tc, wire.AuthCheckParams{Password: "s3cret", RemoteIP: "10.0.0.10"}) {
		t.Fatal("another IP was limited too")
	}
	now = now.Add(61 * time.Second)
	if !authOK(t, tc, wire.AuthCheckParams{Password: "s3cret", RemoteIP: "10.0.0.9"}) {
		t.Fatal("still limited after the window passed")
	}
}

func authCheck(t *testing.T, tc *testConn, p wire.AuthCheckParams) wire.AuthCheckResult {
	t.Helper()
	var res wire.AuthCheckResult
	tc.call(wire.MethodAuthCheck, p, &res)
	return res
}

func TestAuthCheckReasonInvalid(t *testing.T) {
	tc, _ := authFixture(t, true, false, nil)
	if res := authCheck(t, tc, wire.AuthCheckParams{Password: "s3cret", RemoteIP: "10.0.0.1"}); !res.OK || res.Reason != "" {
		t.Fatalf("right password: %+v", res)
	}
	if res := authCheck(t, tc, wire.AuthCheckParams{Password: "wrong", RemoteIP: "10.0.0.1"}); res.OK || res.Reason != wire.AuthInvalid || res.RetryAfter != 0 {
		t.Fatalf("wrong password: %+v, want reason invalid", res)
	}
	if res := authCheck(t, tc, wire.AuthCheckParams{CertSHA256: strings.Repeat("ab", 32), RemoteIP: "10.0.0.1"}); res.OK || res.Reason != wire.AuthInvalid {
		t.Fatalf("cert with allow_cert_auth=false: %+v, want reason invalid", res)
	}
}

func TestAuthCheckReasonAddressNotAllowed(t *testing.T) {
	tc, _, _, _ := historyFixtureWith(t, func(b *fakeBackend, _ *Server, db *store.Store, _ int64) {
		b.cfg.AllowPasswordAuth, b.cfg.AllowedIPs = true, []string{"192.168.0.0/16"}
		hash, _ := auth.HashPassword("s3cret")
		if err := db.SetPasswordHash(context.Background(), hash); err != nil {
			t.Fatal(err)
		}
	}, "auth")
	if res := authCheck(t, tc, wire.AuthCheckParams{Password: "s3cret", RemoteIP: "10.0.0.1"}); res.OK || res.Reason != wire.AuthInvalid {
		t.Fatalf("address outside allowed_ips: %+v, want reason invalid", res)
	}
}

func TestAuthCheckReasonRateLimitedWithRetryAfter(t *testing.T) {
	now := time.Now()
	tc, _ := authFixture(t, true, true, &now)
	for i := 0; i < 5; i++ {
		if res := authCheck(t, tc, wire.AuthCheckParams{Password: "wrong", RemoteIP: "10.0.0.9"}); res.Reason != wire.AuthInvalid {
			t.Fatalf("failure %d: %+v, want reason invalid", i+1, res)
		}
		if i == 0 {
			now = now.Add(20 * time.Second) // the oldest failure frees the window 40 s from here
		}
	}
	res := authCheck(t, tc, wire.AuthCheckParams{Password: "s3cret", RemoteIP: "10.0.0.9"})
	if res.OK || res.Reason != wire.AuthRateLimited || res.RetryAfter != 40 {
		t.Fatalf("locked out: %+v, want rate_limited, retry_after 40", res)
	}
	now = now.Add(39*time.Second + 500*time.Millisecond)
	if res := authCheck(t, tc, wire.AuthCheckParams{Password: "s3cret", RemoteIP: "10.0.0.9"}); res.Reason != wire.AuthRateLimited || res.RetryAfter != 1 {
		t.Fatalf("half a second left: %+v, want rate_limited, retry_after 1 (rounded up)", res)
	}
	now = now.Add(time.Second)
	if res := authCheck(t, tc, wire.AuthCheckParams{Password: "s3cret", RemoteIP: "10.0.0.9"}); !res.OK {
		t.Fatalf("after the window: %+v", res)
	}
}
