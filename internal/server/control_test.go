package server

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/control"
	gobnclog "github.com/MasterBOFH/GoBNC/internal/log"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

func TestControlStartStopNetwork(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "gobnc.sock")
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "t.db")
	cfg.ControlSocket = sock
	cfg.TLSCert = filepath.Join(dir, "missing.crt")
	cfg.TLSKey = filepath.Join(dir, "missing.key")

	s, err := New(cfg, gobnclog.New("error", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.runCtx = ctx
	s.cancel = cancel

	if err := s.serveControl(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)

	_, err = s.Store().UpsertNetwork(ctx, store.Network{
		Name: "net1", Host: "127.0.0.1", Port: 1, Nick: "n", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := control.Client(sock, control.CmdStartNetwork+" net1")
	if err != nil || resp != "OK" {
		t.Fatalf("start: %q %v", resp, err)
	}
	if _, err := s.Session("net1"); err != nil {
		t.Fatal(err)
	}

	resp, err = control.Client(sock, control.CmdStopNetwork+" net1")
	if err != nil || resp != "OK" {
		t.Fatalf("stop: %q %v", resp, err)
	}
	if _, err := s.Session("net1"); err == nil {
		t.Fatal("expected not running")
	}
}

func TestControlReloadNetworkConfig(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "s.sock")
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "t.db")
	cfg.ControlSocket = sock

	s, err := New(cfg, gobnclog.New("error", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.runCtx = ctx
	s.cancel = cancel
	if err := s.serveControl(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)

	_, err = s.Store().UpsertNetwork(ctx, store.Network{
		Name: "net1", Host: "127.0.0.1", Port: 6697, Nick: "n", TLS: true, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp, err := control.Client(sock, control.CmdStartNetwork+" net1"); err != nil || resp != "OK" {
		t.Fatalf("start: %q %v", resp, err)
	}

	n, err := s.Store().NetworkByName(ctx, "net1")
	if err != nil {
		t.Fatal(err)
	}
	n.TLS = false
	n.Port = 4443
	if _, err := s.Store().UpsertNetwork(ctx, n); err != nil {
		t.Fatal(err)
	}
	if resp, err := control.Client(sock, control.CmdReloadNetwork+" net1"); err != nil || resp != "OK" {
		t.Fatalf("reload: %q %v", resp, err)
	}
	sess, err := s.Session("net1")
	if err != nil {
		t.Fatal(err)
	}
	if sess.Network.TLS || sess.Network.Port != 4443 {
		t.Fatalf("session network not updated: tls=%v port=%d", sess.Network.TLS, sess.Network.Port)
	}
}
