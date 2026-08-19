package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
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
	attachTestKeeper(t, s)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.runCtx = ctx
	s.cancel = cancel

	if err := s.serveControl(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)

	st, err := os.Stat(sock)
	if err != nil {
		t.Fatal(err)
	}
	if perm := st.Mode().Perm(); perm&0o077 != 0 {
		t.Fatalf("socket mode=%04o want owner-only", perm)
	}

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
	attachTestKeeper(t, s)

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

func TestControlReconnectNetwork(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "r.sock")
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "t.db")
	cfg.ControlSocket = sock

	s, err := New(cfg, gobnclog.New("error", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	attachTestKeeper(t, s)

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
	if resp, err := control.Client(sock, control.CmdReconnectNetwork+" net1"); err != nil || resp != "OK" {
		t.Fatalf("reconnect while stopped should start: %q %v", resp, err)
	}
	if _, err := s.Session("net1"); err != nil {
		t.Fatalf("expected network running after reconnect-start: %v", err)
	}

	n, err := s.Store().NetworkByName(ctx, "net1")
	if err != nil {
		t.Fatal(err)
	}
	n.Port = 9999
	if _, err := s.Store().UpsertNetwork(ctx, n); err != nil {
		t.Fatal(err)
	}
	if resp, err := control.Client(sock, control.CmdReconnectNetwork+" net1"); err != nil || resp != "OK" {
		t.Fatalf("reconnect: %q %v", resp, err)
	}
	sess, err := s.Session("net1")
	if err != nil {
		t.Fatal(err)
	}
	if sess.Network.Port != 9999 {
		t.Fatalf("config not reloaded before reconnect: port=%d", sess.Network.Port)
	}
}

func TestControlStatus(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "status.sock")
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "t.db")
	cfg.ControlSocket = sock
	cfg.ListenAddr = "127.0.0.1:6697"

	s, err := New(cfg, gobnclog.New("error", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	attachTestKeeper(t, s)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.runCtx = ctx
	s.cancel = cancel
	if err := s.serveControl(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)

	_, err = s.Store().UpsertNetwork(ctx, store.Network{
		Name: "net1", Host: "irc.example", Port: 6697, Nick: "alice", TLS: true, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp, err := control.Client(sock, control.CmdStartNetwork+" net1"); err != nil || resp != "OK" {
		t.Fatalf("start: %q %v", resp, err)
	}

	payload, ok, err := control.TryQuery(sock, control.CmdStatus)
	if err != nil || !ok || payload == "" {
		t.Fatalf("status: ok=%v payload=%q err=%v", ok, payload, err)
	}
	if !strings.Contains(payload, `"listen_addr":"127.0.0.1:6697"`) {
		t.Fatalf("listen: %s", payload)
	}
	if !strings.Contains(payload, `"name":"net1"`) || !strings.Contains(payload, `"running":true`) {
		t.Fatalf("network: %s", payload)
	}
	if !strings.Contains(payload, `"brain_version":`) || !strings.Contains(payload, `"keeper_upgrade":"none"`) {
		t.Fatalf("versions: %s", payload)
	}
	if !strings.Contains(payload, `"version":`) {
		t.Fatalf("release version: %s", payload)
	}
}

// Reload's own regression suite (spawn failure, full handoff, timeout) now
// lives in reload_handoff_test.go — RequestReload/WantReload's old
// "cancels immediately on RELOAD" behavior no longer holds by design: the
// old brain must not detach/cancel until a replacement has confirmed it's
// ready (see runReloadHandoff's own doc comment).
