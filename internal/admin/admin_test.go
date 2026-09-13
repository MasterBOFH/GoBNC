package admin

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/store"
)

type memRuntime struct {
	started, stopped, reloaded []string
	reconnected                []string
	rehashN, reloadN, dieN     int
	startOK, stopOK, reloadOK  bool
	reconnectErr, rehashErr    error
	reloadErr, dieErr          error
	status                     Status
	statusOK                   bool
	statusErr                  error
}

func (m *memRuntime) StartNetwork(name string) (bool, error) {
	m.started = append(m.started, name)
	return m.startOK, nil
}
func (m *memRuntime) StopNetwork(name string) (bool, error) {
	m.stopped = append(m.stopped, name)
	return m.stopOK, nil
}
func (m *memRuntime) ReloadNetwork(name string) (bool, error) {
	m.reloaded = append(m.reloaded, name)
	return m.reloadOK, nil
}
func (m *memRuntime) ReconnectNetwork(name string) error {
	m.reconnected = append(m.reconnected, name)
	return m.reconnectErr
}
func (m *memRuntime) Rehash() error {
	m.rehashN++
	return m.rehashErr
}
func (m *memRuntime) Reload() error {
	m.reloadN++
	return m.reloadErr
}
func (m *memRuntime) Die() error {
	m.dieN++
	return m.dieErr
}
func (m *memRuntime) Status() (Status, bool, error) {
	return m.status, m.statusOK, m.statusErr
}

func testDeps(t *testing.T, rt Runtime) Deps {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return Deps{
		Store: st, Nick: "defnick", Username: "u", Realname: "r", AltNick: "alt",
		Runtime: rt,
	}
}

func TestNetworkDisconnectReconnectCurrent(t *testing.T) {
	rt := &memRuntime{startOK: true, stopOK: true}
	deps := testDeps(t, rt)
	deps.CurrentNetwork = "libera"

	lines, err := Run(context.Background(), deps, Options{}, []string{"disconnect"})
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 1 || lines[0] != "disconnected libera" {
		t.Fatalf("disconnect: %v", lines)
	}
	if len(rt.stopped) != 1 || rt.stopped[0] != "libera" {
		t.Fatalf("stopped: %v", rt.stopped)
	}

	lines, err = Run(context.Background(), deps, Options{}, []string{"reconnect"})
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 1 || lines[0] != "reconnect requested for libera" {
		t.Fatalf("reconnect: %v", lines)
	}
	if len(rt.reconnected) != 1 || rt.reconnected[0] != "libera" {
		t.Fatalf("reconnected: %v", rt.reconnected)
	}

	lines, err = Run(context.Background(), deps, Options{}, []string{"network", "disconnect", "oftc"})
	if err != nil {
		t.Fatal(err)
	}
	if lines[0] != "disconnected oftc" {
		t.Fatalf("named disconnect: %v", lines)
	}

	_, err = Run(context.Background(), testDeps(t, rt), Options{}, []string{"reconnect"})
	if err == nil || !strings.Contains(err.Error(), "usage:") {
		t.Fatalf("CLI without current network: %v", err)
	}
}

func TestRunHelpAndRejects(t *testing.T) {
	deps := testDeps(t, &memRuntime{startOK: true})
	lines, err := Run(context.Background(), deps, Options{AllowInlineSASLPass: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(lines, "\n")
	if !strings.Contains(joined, "BNC commands:") || !strings.Contains(joined, "network list") || !strings.Contains(joined, "status") {
		t.Fatalf("help: %q", joined)
	}
	if !strings.Contains(joined, "reload") || !strings.Contains(joined, "die") {
		t.Fatalf("help missing reload/die: %q", joined)
	}
	if !strings.Contains(joined, "reconnect") || !strings.Contains(joined, "disconnect") {
		t.Fatalf("help missing reconnect/disconnect: %q", joined)
	}
	for _, bad := range []string{"serve", "auth", "stop"} {
		_, err := Run(context.Background(), deps, Options{}, []string{bad})
		if err == nil || !strings.Contains(err.Error(), "not available via BNC") {
			t.Fatalf("%s: want BNC reject, got %v", bad, err)
		}
	}
}

func TestInlineSASLPass(t *testing.T) {
	rt := &memRuntime{startOK: true}
	deps := testDeps(t, rt)
	opts := Options{AllowInlineSASLPass: true}
	lines, err := Run(context.Background(), deps, opts, []string{
		"network", "add", "n1", "irc.example", "6697", "--nick=nick",
		"--sasl-user=me", "--sasl-pass=s3cret",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 1 || !strings.Contains(lines[0], "added and started") {
		t.Fatalf("%v", lines)
	}
	n, err := deps.Store.NetworkByName(context.Background(), "n1")
	if err != nil {
		t.Fatal(err)
	}
	if n.SASLUser != "me" || n.SASLPass != "s3cret" {
		t.Fatalf("sasl: %+v", n)
	}
	if !n.SASL {
		t.Fatal("expected sasl=true implied by user+pass")
	}

	_, err = Run(context.Background(), deps, opts, []string{
		"network", "add", "n1ext", "irc.example", "6697", "--nick=nick",
		"--sasl-user=MrIron",
	})
	if err != nil {
		t.Fatal(err)
	}
	n, err = deps.Store.NetworkByName(context.Background(), "n1ext")
	if err != nil {
		t.Fatal(err)
	}
	if n.SASLUser != "MrIron" || n.SASLPass != "" || !n.SASL {
		t.Fatalf("EXTERNAL authzid add: %+v", n)
	}

	_, err = Run(context.Background(), deps, opts, []string{
		"network", "add", "n2", "irc.example", "6697", "--nick=nick", "--sasl-pass",
	})
	if err == nil || !strings.Contains(err.Error(), "--sasl-pass=secret") {
		t.Fatalf("bare --sasl-pass: %v", err)
	}

	_, err = Run(context.Background(), deps, Options{}, []string{
		"network", "add", "n3", "irc.example", "6697", "--nick=nick", "--sasl-pass=x",
	})
	if err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("CLI inline reject: %v", err)
	}
}

func TestNetworkTLSCertFlags(t *testing.T) {
	rt := &memRuntime{startOK: true, reloadOK: true}
	deps := testDeps(t, rt)
	opts := Options{AllowInlineSASLPass: true}
	_, err := Run(context.Background(), deps, opts, []string{
		"network", "add", "n1", "irc.example", "6697", "--nick=nick",
		"--tls-cert=certs/a.crt", "--tls-key=certs/a.key",
	})
	if err != nil {
		t.Fatal(err)
	}
	n, err := deps.Store.NetworkByName(context.Background(), "n1")
	if err != nil {
		t.Fatal(err)
	}
	if n.TLSCert != "certs/a.crt" || n.TLSKey != "certs/a.key" {
		t.Fatalf("add: %+v", n)
	}
	_, err = Run(context.Background(), deps, opts, []string{
		"network", "mod", "n1", "--tls-cert=none",
	})
	if err != nil {
		t.Fatal(err)
	}
	n, err = deps.Store.NetworkByName(context.Background(), "n1")
	if err != nil {
		t.Fatal(err)
	}
	if n.TLSCert != "none" {
		t.Fatalf("mod none: %+v", n)
	}
}

func TestNetworkBindHostFlags(t *testing.T) {
	rt := &memRuntime{startOK: true, reloadOK: true}
	deps := testDeps(t, rt)
	opts := Options{AllowInlineSASLPass: true}
	_, err := Run(context.Background(), deps, opts, []string{
		"network", "add", "n1", "irc.example", "6697", "--nick=nick",
		"--bind-host=198.51.100.2",
	})
	if err != nil {
		t.Fatal(err)
	}
	n, err := deps.Store.NetworkByName(context.Background(), "n1")
	if err != nil {
		t.Fatal(err)
	}
	if n.BindHost != "198.51.100.2" {
		t.Fatalf("add: %+v", n)
	}
	_, err = Run(context.Background(), deps, opts, []string{
		"network", "mod", "n1", "--bind-host=none",
	})
	if err != nil {
		t.Fatal(err)
	}
	n, err = deps.Store.NetworkByName(context.Background(), "n1")
	if err != nil {
		t.Fatal(err)
	}
	if n.BindHost != "none" {
		t.Fatalf("mod none: %+v", n)
	}
}

func TestNetworkSASLFlag(t *testing.T) {
	rt := &memRuntime{startOK: true, reloadOK: true}
	deps := testDeps(t, rt)
	opts := Options{AllowInlineSASLPass: true}
	_, err := Run(context.Background(), deps, opts, []string{
		"network", "add", "ext", "irc.example", "6697", "--nick=nick",
		"--sasl=true", "--tls-cert=certs/c.crt", "--tls-key=certs/c.key",
	})
	if err != nil {
		t.Fatal(err)
	}
	n, err := deps.Store.NetworkByName(context.Background(), "ext")
	if err != nil {
		t.Fatal(err)
	}
	if !n.SASL || n.SASLUser != "" || n.SASLPass != "" {
		t.Fatalf("external-style: %+v", n)
	}
	_, err = Run(context.Background(), deps, opts, []string{
		"network", "mod", "ext", "--sasl=false",
	})
	if err != nil {
		t.Fatal(err)
	}
	n, err = deps.Store.NetworkByName(context.Background(), "ext")
	if err != nil {
		t.Fatal(err)
	}
	if n.SASL {
		t.Fatal("expected sasl=false after mod")
	}
}

func TestNetworkListRehash(t *testing.T) {
	rt := &memRuntime{startOK: true}
	deps := testDeps(t, rt)
	_, err := Run(context.Background(), deps, Options{AllowInlineSASLPass: true}, []string{
		"network", "add", "n1", "h", "6697", "--nick=nick",
	})
	if err != nil {
		t.Fatal(err)
	}
	lines, err := Run(context.Background(), deps, Options{}, []string{"network", "list"})
	if err != nil || len(lines) != 1 || !strings.HasPrefix(lines[0], "n1\t") {
		t.Fatalf("list: %v %v", lines, err)
	}
	lines, err = Run(context.Background(), deps, Options{}, []string{"rehash"})
	if err != nil || len(lines) != 1 || lines[0] != "Rehash complete." {
		t.Fatalf("rehash: %v %v", lines, err)
	}
	if rt.rehashN != 1 {
		t.Fatalf("rehashN=%d", rt.rehashN)
	}

	lines, err = Run(context.Background(), deps, Options{}, []string{"reload"})
	if err != nil || len(lines) != 1 || lines[0] != "Reloading brain (keeper stays)." {
		t.Fatalf("reload: %v %v", lines, err)
	}
	if rt.reloadN != 1 {
		t.Fatalf("reloadN=%d", rt.reloadN)
	}
	lines, err = Run(context.Background(), deps, Options{}, []string{"die"})
	if err != nil || len(lines) != 1 || lines[0] != "Stopping brain and keeper." {
		t.Fatalf("die: %v %v", lines, err)
	}
	if rt.dieN != 1 {
		t.Fatalf("dieN=%d", rt.dieN)
	}
}

func TestStatusLiveAndOffline(t *testing.T) {
	rt := &memRuntime{
		statusOK: true,
		status: Status{
			Running: true, ListenAddr: "127.0.0.1:6697", Clients: 1,
			Networks: []NetworkStatus{{
				Name: "libera", Host: "irc.libera.chat", Port: 6697, TLS: true, Enabled: true,
				ConfigNick: "cfg", Nick: "live", Running: true, Registered: true, Clients: 1,
			}},
		},
	}
	deps := testDeps(t, rt)
	deps.ListenAddr = "127.0.0.1:6697"
	lines, err := Run(context.Background(), deps, Options{}, []string{"status"})
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(lines, "\n")
	for _, want := range []string{"daemon running", "listen 127.0.0.1:6697", "clients 1", "network libera connected nick=live"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing %q in %q", want, joined)
		}
	}

	// Offline path: runtime reports daemon down; use DB rows.
	rt.statusOK = false
	_, err = deps.Store.UpsertNetwork(context.Background(), store.Network{
		Name: "oftc", Host: "irc.oftc.net", Port: 6697, TLS: true, Nick: "bob", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	lines, err = Run(context.Background(), deps, Options{}, []string{"status"})
	if err != nil {
		t.Fatal(err)
	}
	joined = strings.Join(lines, "\n")
	if !strings.Contains(joined, "daemon not running") || !strings.Contains(joined, "network oftc stopped nick=bob") {
		t.Fatalf("offline status: %q", joined)
	}
}

func TestFormatStatusStates(t *testing.T) {
	lines := FormatStatus(Status{
		Running: true, ListenAddr: "0.0.0.0:1", Clients: 0,
		Networks: []NetworkStatus{
			{Name: "a", Host: "h", Port: 1, Enabled: false, ConfigNick: "n"},
			{Name: "b", Host: "h", Port: 1, Enabled: true, Running: true, ConfigNick: "n"},
			{Name: "c", Host: "h", Port: 1, Enabled: true, Running: true, Registered: true, Nick: "x", ConfigNick: "n"},
		},
	})
	joined := strings.Join(lines, "\n")
	for _, want := range []string{"daemon running", "listen 0.0.0.0:1", "clients 0", "network a disabled", "network b connecting", "network c connected nick=x"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing %q in %q", want, joined)
		}
	}
}

func TestFormatStatusVersions(t *testing.T) {
	lines := FormatStatus(Status{
		Running: true, Version: "0.1.1", BrainVersion: 1, KeeperVersion: 1, KeeperRelease: "0.1.1", KeeperUpgrade: "none",
	})
	joined := strings.Join(lines, "\n")
	for _, want := range []string{"version 0.1.1", "brain 1", "keeper 1 (0.1.1)", "keeper-upgrade none"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing %q in %q", want, joined)
		}
	}
	lines = FormatStatus(Status{Running: true, BrainVersion: 2, KeeperVersion: 1, KeeperUpgrade: "should"})
	joined = strings.Join(lines, "\n")
	if !strings.Contains(joined, "keeper-upgrade should") {
		t.Fatalf("should: %q", joined)
	}
	lines = FormatStatus(Status{Running: false})
	joined = strings.Join(lines, "\n")
	if strings.Contains(joined, "brain") || strings.Contains(joined, "keeper") {
		t.Fatalf("offline status should omit versions: %q", joined)
	}
}

// network add: the port is optional and defaults by transport; the nick
// is --nick= only, never a positional.
func TestNetworkAddPortDefaultsAndNickFlag(t *testing.T) {
	for _, tc := range []struct {
		name     string
		args     []string
		wantPort int
		wantWS   bool
		wantTLS  bool
	}{
		{"tcp tls", []string{"irc.example"}, 6697, false, true},
		{"tcp plain", []string{"irc.example", "--tls=false"}, 6667, false, false},
		{"wss", []string{"wss://irc.example"}, 443, true, true},
		{"ws", []string{"ws://irc.example"}, 80, true, false},
		{"explicit port", []string{"irc.example", "7000"}, 7000, false, true},
		{"url port", []string{"wss://irc.example:8443/irc"}, 8443, true, true},
		{"explicit port beats url port", []string{"wss://irc.example:8443", "9443"}, 9443, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deps := testDeps(t, &memRuntime{startOK: true})
			args := append([]string{"network", "add", "n"}, tc.args...)
			args = append(args, "--nick=me")
			if _, err := Run(context.Background(), deps, Options{}, args); err != nil {
				t.Fatalf("%v: %v", args, err)
			}
			n, err := deps.Store.NetworkByName(context.Background(), "n")
			if err != nil {
				t.Fatal(err)
			}
			if n.Port != tc.wantPort || n.WebSocket != tc.wantWS || n.TLS != tc.wantTLS || n.Nick != "me" {
				t.Fatalf("got port=%d ws=%v tls=%v nick=%q, want port=%d ws=%v tls=%v nick=me",
					n.Port, n.WebSocket, n.TLS, n.Nick, tc.wantPort, tc.wantWS, tc.wantTLS)
			}
		})
	}

	// A bare word where the port would go is not a nick.
	deps := testDeps(t, &memRuntime{startOK: true})
	if _, err := Run(context.Background(), deps, Options{}, []string{"network", "add", "n", "irc.example", "somenick"}); err == nil || !strings.Contains(err.Error(), "--nick=") {
		t.Fatalf("positional nick accepted or wrong error: %v", err)
	}
	// Nor after the port.
	if _, err := Run(context.Background(), deps, Options{}, []string{"network", "add", "n", "irc.example", "6697", "somenick"}); err == nil || !strings.Contains(err.Error(), "--nick=") {
		t.Fatalf("positional nick after port accepted or wrong error: %v", err)
	}
	// Without --nick= and no default_nick, it's an error that names the flag.
	deps.Nick = ""
	if _, err := Run(context.Background(), deps, Options{}, []string{"network", "add", "n", "irc.example"}); err == nil || !strings.Contains(err.Error(), "--nick=") {
		t.Fatalf("missing nick: %v", err)
	}
}
