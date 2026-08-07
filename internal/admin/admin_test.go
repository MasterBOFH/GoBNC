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
	rehashN                    int
	startOK, stopOK, reloadOK  bool
	reconnectErr, rehashErr    error
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
		"network", "add", "n1", "irc.example", "6697", "nick",
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
		"network", "add", "n2", "irc.example", "6697", "nick", "--sasl-pass",
	})
	if err == nil || !strings.Contains(err.Error(), "--sasl-pass=secret") {
		t.Fatalf("bare --sasl-pass: %v", err)
	}

	_, err = Run(context.Background(), deps, Options{}, []string{
		"network", "add", "n3", "irc.example", "6697", "nick", "--sasl-pass=x",
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
		"network", "add", "n1", "irc.example", "6697", "nick",
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

func TestNetworkSASLFlag(t *testing.T) {
	rt := &memRuntime{startOK: true, reloadOK: true}
	deps := testDeps(t, rt)
	opts := Options{AllowInlineSASLPass: true}
	_, err := Run(context.Background(), deps, opts, []string{
		"network", "add", "ext", "irc.example", "6697", "nick",
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
		"network", "add", "n1", "h", "6697", "nick",
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
	for _, want := range []string{"network a disabled", "network b connecting", "network c connected nick=x"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing %q in %q", want, joined)
		}
	}
}
