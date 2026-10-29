package module

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/pkg/moduleapi/wire"
)

func commandsFixture(t *testing.T) (*Server, *testConn) {
	t.Helper()
	srv, path := startServer(t, []config.ModuleConfig{{Name: "rc", Features: []string{"commands"}}}, &fakeBackend{ready: true})
	tc := dialModule(t, path)
	tc.hello("rc", "commands")
	tc.call(wire.MethodCommandsRegister, wire.CommandRegisterParams{Name: "devices", Help: "list devices"}, nil)
	return srv, tc
}

func TestModuleCommandRoundTrip(t *testing.T) {
	srv, tc := commandsFixture(t)
	type result struct {
		lines   []string
		handled bool
		err     error
	}
	done := make(chan result, 1)
	go func() {
		lines, handled, err := srv.RunCommand("devices", []string{"list", "-v"}, wire.OriginBNC)
		done <- result{lines, handled, err}
	}()
	var ev wire.CommandEvent
	if err := json.Unmarshal(tc.nextEvent(wire.EventCommand), &ev); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ev.Args, []string{"list", "-v"}) || ev.Origin != wire.OriginBNC {
		t.Fatalf("command event = %+v", ev)
	}
	tc.call(wire.MethodCommandsReply, wire.CommandReplyParams{InvocationID: ev.InvocationID, Lines: []string{"phone", "pad"}}, nil)
	r := <-done
	if !r.handled || r.err != nil || !reflect.DeepEqual(r.lines, []string{"phone", "pad"}) {
		t.Fatalf("RunCommand = %+v", r)
	}
	if _, handled, _ := srv.RunCommand("nope", nil, wire.OriginCLI); handled {
		t.Fatal("unregistered command handled")
	}
}

func TestModuleCommandTimeout(t *testing.T) {
	srv, _ := commandsFixture(t)
	old := CommandTimeout
	CommandTimeout = 100 * time.Millisecond
	defer func() { CommandTimeout = old }()
	_, handled, err := srv.RunCommand("devices", nil, wire.OriginCLI)
	if !handled || err == nil || err.Error() != "module rc did not answer" {
		t.Fatalf("handled=%v err=%v, want \"module rc did not answer\"", handled, err)
	}
}

func TestModuleCommandNames(t *testing.T) {
	_, tc := commandsFixture(t)
	for _, name := range []string{"status", "network", "Bad", "x y"} {
		if code := tc.callErr(wire.MethodCommandsRegister, wire.CommandRegisterParams{Name: name}); code != wire.CodeBadParams {
			t.Errorf("register %q: code %q, want bad_params", name, code)
		}
	}
}
