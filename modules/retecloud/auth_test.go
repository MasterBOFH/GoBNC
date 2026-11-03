package retecloud

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestAuthenticatePassword(t *testing.T) {
	e := newTestEnv(t, "undernet", "libera")
	c := e.dial(false)
	data := c.authenticate("ios-1")
	if data["client_id"] != "ios-1" || data["user_id"] != float64(1) {
		t.Fatalf("authenticate_success data = %v", data)
	}
	if ips, ok := data["assigned_ips"].([]any); !ok || len(ips) != 0 {
		t.Fatalf("assigned_ips = %v, want []", data["assigned_ips"])
	}
	sessions := data["sessions"].([]any)
	if len(sessions) != 2 {
		t.Fatalf("sessions = %v", sessions)
	}
	var un map[string]any
	for _, s := range sessions {
		if s.(map[string]any)["session_id"] == e.sid("undernet") {
			un = s.(map[string]any)
		}
	}
	if un == nil || un["name"] != "undernet" || un["state"] != "connected" || un["subscribed"] != false {
		t.Fatalf("sessions = %v", sessions)
	}
}

func TestAuthenticateWrongPasswordAndMissingClientID(t *testing.T) {
	e := newTestEnv(t, "undernet")
	c := e.dial(false)
	rid := c.request("authenticate", map[string]any{"password": "nope", "client_id": "ios-1"}, "")
	if f := c.waitFor(0, 5*time.Second, reply(rid, "authenticate_success", "authenticate_error")); f["error"] != "Authentication failed" {
		t.Fatalf("wrong password: %v", f)
	}
	rid = c.request("authenticate", map[string]any{"password": testPassword}, "")
	if f := c.waitFor(0, 5*time.Second, reply(rid, "authenticate_success", "authenticate_error")); f["error"] != "Invalid request data" {
		t.Fatalf("missing client_id: %v", f)
	}
}

func TestAuthenticateClientCert(t *testing.T) {
	e := newTestEnv(t, "undernet")
	if err := e.be.db.AddFingerprint(context.Background(), e.fx.ClientSHA256, "phone"); err != nil {
		t.Fatal(err)
	}
	c := e.dial(true)
	rid := c.request("authenticate", map[string]any{"client_id": "ios-cert"}, "")
	if f := c.waitFor(0, 5*time.Second, reply(rid, "authenticate_success", "authenticate_error")); f["type"] != "authenticate_success" {
		t.Fatalf("cert auth: %v", f)
	}
	noCert := e.dial(false)
	rid = noCert.request("authenticate", map[string]any{"client_id": "ios-nocert"}, "")
	if f := noCert.waitFor(0, 5*time.Second, reply(rid, "authenticate_success", "authenticate_error")); f["error"] != "Authentication failed" {
		t.Fatalf("no cert, no password: %v", f)
	}
}

func TestSessionsListShowsToggle(t *testing.T) {
	e := newTestEnv(t, "undernet")
	if err := e.be.db.SetDeviceNetwork(context.Background(), "ios-1", e.nets["undernet"], true); err != nil {
		t.Fatal(err)
	}
	c := e.dial(false)
	s := c.authenticate("ios-1")["sessions"].([]any)[0].(map[string]any)
	if s["subscribed"] != true {
		t.Fatalf("session = %v, want subscribed", s)
	}
}

func TestFrameBeforeAuthenticateRejected(t *testing.T) {
	e := newTestEnv(t, "undernet")
	c := e.dial(false)
	rid := c.request("sync_session", map[string]any{"client_id": "x", "sync_mode": "lite"}, e.sid("undernet"))
	f := c.waitFor(0, 5*time.Second, reply(rid, "sync_session_error"))
	if f["error"] != "Not authenticated" {
		t.Fatalf("pre-auth frame: %v", f)
	}
}

func TestUnauthenticatedConnectionClosedAfterTimeout(t *testing.T) {
	old := authTimeout
	authTimeout = 200 * time.Millisecond
	defer func() { authTimeout = old }()
	e := newTestEnv(t, "undernet")
	c := e.dial(false)
	if ce := c.closedWith(3 * time.Second); ce.Code != 1008 || ce.Text != "authentication timeout" {
		t.Fatalf("close = %d %q, want 1008 authentication timeout", ce.Code, ce.Text)
	}
	if n := c.count(); n != 0 {
		t.Fatalf("got %d frames before the close", n)
	}
}

func TestPasswordLoginWithUnregisteredCertIsNotThrottled(t *testing.T) {
	e := newTestEnv(t, "undernet")
	for i := 0; i < 7; i++ { // more than the 5 failures per address the brain allows
		c := e.dial(true) // presents a certificate the bouncer does not know
		c.authenticate(fmt.Sprintf("ios-%d", i))
	}
}
