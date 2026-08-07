package config

import (
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
