package module

import (
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

func TestClientsAttachDetach(t *testing.T) {
	sess := registeredSession(t, 7)
	be := &fakeBackend{ready: true, sessions: map[int64]*session.Session{7: sess}}
	_, path := startServer(t, []config.ModuleConfig{{Name: "rc", Features: []string{"clients"}}}, be)
	tc := dialModule(t, path)
	tc.hello("rc", "clients")

	var res wire.ClientsAttachResult
	tc.call(wire.MethodClientsAttach, wire.ClientParams{Network: 7, Client: "ws-1"}, &res)
	if res.ClientID != "mod:rc:ws-1" {
		t.Fatalf("client_id = %q", res.ClientID)
	}
	if code := tc.callErr(wire.MethodClientsAttach, wire.ClientParams{Network: 7, Client: "bad name"}); code != wire.CodeBadParams {
		t.Fatalf("bad name: code %q", code)
	}
	if code := tc.callErr(wire.MethodClientsDetach, wire.ClientParams{Network: 7, Client: "nope"}); code != wire.CodeNotFound {
		t.Fatalf("unknown client: code %q", code)
	}
	tc.call(wire.MethodClientsAttach, wire.ClientParams{Network: 7, Client: "ws-2"}, nil)
	tc.call(wire.MethodClientsDetach, wire.ClientParams{Network: 7, Client: "ws-1"}, nil)
	if sess.HasDownlink("mod:rc:ws-1") || !sess.HasDownlink("mod:rc:ws-2") {
		t.Fatal("detach removed the wrong client")
	}
	tc.c.Close()
	deadline := time.Now().Add(2 * time.Second)
	for sess.HasDownlink("mod:rc:ws-2") {
		if time.Now().After(deadline) {
			t.Fatal("virtual client still attached after the module disconnected")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
