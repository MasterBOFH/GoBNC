package module

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

func sendFixture(t *testing.T, echo, ready bool) (*testConn, *fakeIRCd, *session.Session) {
	t.Helper()
	ircd := newFakeIRCd(t, echo)
	sess := uplinkSession(t, 9, ircd, nil)
	be := &fakeBackend{ready: ready, sessions: map[int64]*session.Session{9: sess}}
	_, path := startServer(t, []config.ModuleConfig{{Name: "rc", Features: []string{"events", "clients", "send"}}}, be)
	tc := dialModule(t, path)
	tc.hello("rc", "events", "clients", "send")
	tc.call(wire.MethodClientsAttach, wire.ClientParams{Network: 9, Client: "ws1"}, nil)
	return tc, ircd, sess
}

func waitReceived(t *testing.T, ircd *fakeIRCd, cmd, substr string) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, l := range ircd.received(cmd) {
			if strings.Contains(l, substr) {
				return l
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("ircd never got %s containing %q (got %v)", cmd, substr, ircd.received(cmd))
	return ""
}

func TestSendLineEchoCarriesLabelAndReachesIRCClients(t *testing.T) {
	tc, ircd, sess := sendFixture(t, true, true)
	if !sess.HasUpCap("echo-message") || !sess.HasUpCap("labeled-response") {
		t.Fatal("fake ircd caps not negotiated")
	}
	dl := &ircDownlink{id: "irc1", caps: map[string]bool{"echo-message": true}}
	if err := sess.Attach(dl); err != nil {
		t.Fatal(err)
	}
	tc.call(wire.MethodEventsSubscribe, wire.SubscribeParams{Network: 9}, nil)
	tc.call(wire.MethodSendLine, wire.SendLineParams{Network: 9, Client: "ws1", Line: "PRIVMSG #c :from the phone", Label: "L7"}, nil)
	waitReceived(t, ircd, "PRIVMSG", "from the phone")

	var ev wire.LineEvent
	for {
		if err := json.Unmarshal(tc.nextEvent(wire.EventLine), &ev); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(ev.Raw, "from the phone") {
			break
		}
	}
	if !ev.Self || ev.Label != "L7" || ev.Kind != wire.LineMessage {
		t.Fatalf("echo envelope = %+v, want self with label L7", ev)
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(dl.commands("PRIVMSG")) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("attached IRC client never saw the module's line")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSendLineNotReadyBeforeLiveReady(t *testing.T) {
	tc, _, _ := sendFixture(t, false, false)
	code := tc.callErr(wire.MethodSendLine, wire.SendLineParams{Network: 9, Client: "ws1", Line: "PRIVMSG #c :early"})
	if code != wire.CodeNotReady {
		t.Fatalf("code = %q, want not_ready", code)
	}
}

func TestSendLineUnattachedClient(t *testing.T) {
	tc, _, _ := sendFixture(t, false, true)
	if code := tc.callErr(wire.MethodSendLine, wire.SendLineParams{Network: 9, Client: "nobody", Line: "PRIVMSG #c :x"}); code != wire.CodeNotFound {
		t.Fatalf("unattached client: code %q", code)
	}
}

func TestSendLineNeedsClientsFeature(t *testing.T) {
	sess := registeredSession(t, 10)
	be := &fakeBackend{ready: true, sessions: map[int64]*session.Session{10: sess}}
	_, path := startServer(t, []config.ModuleConfig{{Name: "rc", Features: []string{"events", "send"}}}, be)
	tc := dialModule(t, path)
	tc.hello("rc", "events", "send")
	if code := tc.callErr(wire.MethodSendLine, wire.SendLineParams{Network: 10, Client: "ws1", Line: "PRIVMSG #c :x"}); code != wire.CodeNotGranted {
		t.Fatalf("send without clients: code %q, want not_granted", code)
	}
}
