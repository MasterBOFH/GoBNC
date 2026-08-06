package config

import (
	"os"
	"path/filepath"
	"testing"
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
