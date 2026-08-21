package config

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/version"
)

func TestLoadJSONMissing(t *testing.T) {
	cfg, err := LoadJSON(filepath.Join(t.TempDir(), "nope.json"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ListenAddr == "" {
		t.Fatal("expected defaults")
	}
}

func TestLoadJSON(t *testing.T) {
	p := filepath.Join(t.TempDir(), "cfg.json")
	if err := os.WriteFile(p, []byte(`{"listen_addr":"0.0.0.0:7000","log_level":"debug"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadJSON(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ListenAddr != "0.0.0.0:7000" || cfg.LogLevel != "debug" {
		t.Fatalf("%+v", cfg)
	}
}

func TestLoadJSONWithComments(t *testing.T) {
	p := filepath.Join(t.TempDir(), "cfg.json")
	content := `{
  // listen address
  "listen_addr": "127.0.0.1:6697", /* TLS */
  "log_level": "info",
  "db_path": "x.db"
}`
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadJSON(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ListenAddr != "127.0.0.1:6697" || cfg.DBPath != "x.db" {
		t.Fatalf("%+v", cfg)
	}
}

func TestLoadJSONExampleFile(t *testing.T) {
	// Repo-root example (annotated JSONC).
	p := filepath.Join("..", "..", "gobnc.json.example")
	cfg, err := LoadJSON(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ListenAddr != "127.0.0.1:6697" || cfg.MaxFloodQueue != 16384 {
		t.Fatalf("%+v", cfg)
	}
}

func TestValidateFailClosed(t *testing.T) {
	cfg := Default()
	cfg.AllowPasswordAuth = false
	cfg.AllowCertAuth = false
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error")
	}
}

func TestParseAllowedIPsCIDRAndBareIP(t *testing.T) {
	nets, err := ParseAllowedIPs([]string{"10.0.0.0/8", "192.168.1.5", "::1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(nets) != 3 {
		t.Fatalf("got %d nets, want 3: %+v", len(nets), nets)
	}
	if !nets[0].Contains(mustParseIP(t, "10.1.2.3")) {
		t.Fatal("10.0.0.0/8 should contain 10.1.2.3")
	}
	if !nets[1].Contains(mustParseIP(t, "192.168.1.5")) {
		t.Fatal("bare IPv4 should compile to an exact /32")
	}
	if nets[1].Contains(mustParseIP(t, "192.168.1.6")) {
		t.Fatal("bare IPv4 /32 must not match a different host")
	}
	if !nets[2].Contains(mustParseIP(t, "::1")) {
		t.Fatal("bare IPv6 should compile to an exact /128")
	}
}

func TestParseAllowedIPsEmptyMeansUnrestricted(t *testing.T) {
	nets, err := ParseAllowedIPs(nil)
	if err != nil || nets != nil {
		t.Fatalf("nets=%v err=%v, want nil, nil", nets, err)
	}
}

func TestParseAllowedIPsRejectsGarbage(t *testing.T) {
	for _, bad := range []string{"not-an-ip", "10.0.0.0/99", "", "1.2.3.4/"} {
		if _, err := ParseAllowedIPs([]string{bad}); err == nil {
			t.Fatalf("entry %q: expected error, got nil", bad)
		}
	}
}

// TestValidateRejectsBadAllowedIPs proves Validate fails config load/rehash
// outright on a malformed entry, rather than silently ignoring it and
// leaving the bouncer either wide open or wrongly locked down at runtime.
func TestValidateRejectsBadAllowedIPs(t *testing.T) {
	cfg := Default()
	cfg.AllowedIPs = []string{"not-an-ip"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for malformed allowed_ips entry")
	}
}

func TestDefaultCTCPModes(t *testing.T) {
	cfg := Default()
	if cfg.CTCPPing != "relay" || cfg.CTCPVersion != "relay" || cfg.CTCPOther != "relay" {
		t.Fatalf("%+v", cfg)
	}
}

func TestLoadJSONCTCPEmptyFallsBackToRelay(t *testing.T) {
	p := filepath.Join(t.TempDir(), "cfg.json")
	if err := os.WriteFile(p, []byte(`{"listen_addr":"0.0.0.0:7000","ctcp_ping":""}`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadJSON(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CTCPPing != "relay" {
		t.Fatalf("ctcp_ping = %q, want relay", cfg.CTCPPing)
	}
}

func TestLoadJSONCTCPExplicitValues(t *testing.T) {
	p := filepath.Join(t.TempDir(), "cfg.json")
	content := `{"listen_addr":"0.0.0.0:7000","ctcp_ping":"edge","ctcp_version":"disable","ctcp_other":"disable"}`
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadJSON(p)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CTCPPing != "edge" || cfg.CTCPVersion != "disable" || cfg.CTCPOther != "disable" {
		t.Fatalf("%+v", cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected Validate error: %v", err)
	}
}

func TestValidateRejectsBadCTCPMode(t *testing.T) {
	for _, tt := range []struct {
		name string
		mut  func(*Config)
	}{
		{"ctcp_ping garbage", func(c *Config) { c.CTCPPing = "sometimes" }},
		{"ctcp_version garbage", func(c *Config) { c.CTCPVersion = "sometimes" }},
		{"ctcp_other garbage", func(c *Config) { c.CTCPOther = "sometimes" }},
		{"ctcp_other edge not allowed", func(c *Config) { c.CTCPOther = "edge" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			tt.mut(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func mustParseIP(t *testing.T, s string) net.IP {
	t.Helper()
	ip := net.ParseIP(s)
	if ip == nil {
		t.Fatalf("net.ParseIP(%q) failed", s)
	}
	return ip
}

func TestDefaultQuitMessage(t *testing.T) {
	want := version.QuitMessage()
	if want != "GoBNC "+version.Version {
		t.Fatalf("version.QuitMessage=%q", want)
	}
	cfg := Default()
	if cfg.QuitMessage != want {
		t.Fatalf("Default QuitMessage=%q want %q", cfg.QuitMessage, want)
	}
	cfg.QuitMessage = ""
	if cfg.QuitReason() != want {
		t.Fatalf("empty QuitReason=%q", cfg.QuitReason())
	}
	cfg.QuitMessage = "custom bye"
	if cfg.QuitReason() != "custom bye" {
		t.Fatal(cfg.QuitReason())
	}
	if ShutdownTimeout != 5*time.Second {
		t.Fatalf("ShutdownTimeout=%v", ShutdownTimeout)
	}
}

func TestNetworkIdentityDefaults(t *testing.T) {
	cfg := Default()
	nick, user, real, alt := cfg.NetworkIdentityDefaults()
	if nick != "" || user != "gobnc" || real != "GoBNC" || alt != "" {
		t.Fatalf("defaults: nick=%q user=%q real=%q alt=%q", nick, user, real, alt)
	}
	cfg.DefaultNick = "alice"
	cfg.DefaultUsername = "auser"
	cfg.DefaultRealname = "Alice"
	cfg.DefaultAltNick = "alice_"
	nick, user, real, alt = cfg.NetworkIdentityDefaults()
	if nick != "alice" || user != "auser" || real != "Alice" || alt != "alice_" {
		t.Fatalf("custom: nick=%q user=%q real=%q alt=%q", nick, user, real, alt)
	}
	cfg.DefaultUsername = ""
	cfg.DefaultRealname = ""
	_, user, real, _ = cfg.NetworkIdentityDefaults()
	if user != DefaultUsernameFallback || real != DefaultRealnameFallback {
		t.Fatalf("empty fallbacks: user=%q real=%q", user, real)
	}
}

func TestResolveTLSClientCert(t *testing.T) {
	cert, key, ok := ResolveTLSClientCert("", "", "", "")
	if ok || cert != "" || key != "" {
		t.Fatalf("empty: %q %q %v", cert, key, ok)
	}
	cert, key, ok = ResolveTLSClientCert("", "", "g.crt", "g.key")
	if !ok || cert != "g.crt" || key != "g.key" {
		t.Fatalf("inherit global: %q %q %v", cert, key, ok)
	}
	cert, key, ok = ResolveTLSClientCert("n.crt", "n.key", "g.crt", "g.key")
	if !ok || cert != "n.crt" || key != "n.key" {
		t.Fatalf("network override: %q %q %v", cert, key, ok)
	}
	cert, key, ok = ResolveTLSClientCert("none", "", "g.crt", "g.key")
	if ok {
		t.Fatalf("none should disable: %q %q", cert, key)
	}
	cert, key, ok = ResolveTLSClientCert("-", "-", "g.crt", "g.key")
	if ok {
		t.Fatalf("dash should disable: %q %q", cert, key)
	}
	_, _, ok = ResolveTLSClientCert("only.crt", "", "g.crt", "g.key")
	if ok {
		t.Fatal("incomplete network pair must not fall back partially")
	}
}

func TestResolveBindHost(t *testing.T) {
	if got := ResolveBindHost("", ""); got != "" {
		t.Fatalf("empty: %q", got)
	}
	if got := ResolveBindHost("", "203.0.113.10"); got != "203.0.113.10" {
		t.Fatalf("inherit global: %q", got)
	}
	if got := ResolveBindHost("198.51.100.2", "203.0.113.10"); got != "198.51.100.2" {
		t.Fatalf("network override: %q", got)
	}
	if got := ResolveBindHost("none", "203.0.113.10"); got != "" {
		t.Fatalf("none disables: %q", got)
	}
	if got := ResolveBindHost("-", "203.0.113.10"); got != "" {
		t.Fatalf("dash disables: %q", got)
	}
}

func TestDialLocalAddr(t *testing.T) {
	addr, err := DialLocalAddr("")
	if err != nil || addr != nil {
		t.Fatalf("empty: %v %v", addr, err)
	}
	addr, err = DialLocalAddr("127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	tcp, ok := addr.(*net.TCPAddr)
	if !ok || !tcp.IP.Equal(net.ParseIP("127.0.0.1")) || tcp.Port != 0 {
		t.Fatalf("%#v", addr)
	}
	addr, err = DialLocalAddr("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tcp, ok = addr.(*net.TCPAddr)
	if !ok || !tcp.IP.Equal(net.ParseIP("127.0.0.1")) {
		t.Fatalf("%#v", addr)
	}
}

func TestResolvedControlSocket(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "/run/user/1000")
	cfg := Default()
	got := cfg.ResolvedControlSocket()
	want := filepath.Join("/run/user/1000", "gobnc", "gobnc.sock")
	if got != want {
		t.Fatalf("empty default: got %q want %q", got, want)
	}
	cfg.ControlSocket = "gobnc.sock"
	if cfg.ResolvedControlSocket() != want {
		t.Fatalf("legacy default: got %q want %q", cfg.ResolvedControlSocket(), want)
	}
	cfg.ControlSocket = "/tmp/custom.sock"
	if cfg.ResolvedControlSocket() != "/tmp/custom.sock" {
		t.Fatalf("explicit: %q", cfg.ResolvedControlSocket())
	}

	t.Setenv("XDG_RUNTIME_DIR", "")
	home := t.TempDir()
	t.Setenv("HOME", home)
	cfg = Default()
	got = cfg.ResolvedControlSocket()
	want = filepath.Join(home, ".gobnc", "gobnc.sock")
	if got != want {
		t.Fatalf("home fallback: got %q want %q", got, want)
	}
}
