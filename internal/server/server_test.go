package server

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/MasterBOFH/GoBNC/internal/auth"
	"github.com/MasterBOFH/GoBNC/internal/config"
	gobnclog "github.com/MasterBOFH/GoBNC/internal/log"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

func TestCLIStyleOps(t *testing.T) {
	cfg := config.Default()
	cfg.DBPath = filepath.Join(t.TempDir(), "t.db")
	cfg.TLSCert = "missing"
	cfg.TLSKey = "missing"
	s, err := New(cfg, gobnclog.New("error", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	h, err := auth.HashPassword("pw")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Store().SetPasswordHash(ctx, h); err != nil {
		t.Fatal(err)
	}
	id, err := s.Store().UpsertNetwork(ctx, store.Network{
		Name: "libera", Host: "irc.libera.chat", Port: 6697, TLS: true, Nick: "gobnc", Enabled: true,
	})
	if err != nil || id == 0 {
		t.Fatal(err)
	}
	if err := s.Store().AddChannel(ctx, id, "#test", ""); err != nil {
		t.Fatal(err)
	}
	if err := s.Store().AddFingerprint(ctx, "abcd", "x"); err != nil {
		t.Fatal(err)
	}
	nets, err := s.Store().ListNetworks(ctx)
	if err != nil || len(nets) != 1 {
		t.Fatal(nets, err)
	}
}

func TestShutdownCancel(t *testing.T) {
	cfg := config.Default()
	cfg.DBPath = filepath.Join(t.TempDir(), "t.db")
	s, err := New(cfg, gobnclog.New("error", nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}
