package server

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/config"
	gobnclog "github.com/MasterBOFH/GoBNC/internal/log"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// The stored draft/resume-0.5 token has to reach brain.NetworkConfig for
// every registration attempt this server drives — boot, ReloadNetworkConfig,
// ReconnectNetwork all build the config here.
func TestNetworkConfigCarriesStoredResumeToken(t *testing.T) {
	cfg := config.Default()
	cfg.DBPath = filepath.Join(t.TempDir(), "t.db")
	cfg.TLSCert, cfg.TLSKey = "missing", "missing"
	s, err := New(cfg, gobnclog.New("error", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	id, err := s.Store().UpsertNetwork(ctx, store.Network{Name: "n", Host: "h", Port: 6697, TLS: true, Nick: "me", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	n, err := s.Store().NetworkByName(ctx, "n")
	if err != nil {
		t.Fatal(err)
	}
	if got := s.networkConfigForLocked(n).ResumeToken; got != "" {
		t.Fatalf("no token stored, config has %q", got)
	}
	if err := s.Store().SetResumeToken(ctx, id, "JKbAypzFiovffzuD8VEfcs6bOLrXsSenxsyZNt8"); err != nil {
		t.Fatal(err)
	}
	if got := s.networkConfigForLocked(n).ResumeToken; got != "JKbAypzFiovffzuD8VEfcs6bOLrXsSenxsyZNt8" {
		t.Fatalf("config token=%q, want the stored one", got)
	}
}
