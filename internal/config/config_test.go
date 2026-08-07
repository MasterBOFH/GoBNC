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
