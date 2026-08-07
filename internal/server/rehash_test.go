package server

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/auth"
	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/downlink"
	gobnclog "github.com/MasterBOFH/GoBNC/internal/log"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/internal/testutil"
)

func TestCertHolderSwap(t *testing.T) {
	fx1 := testutil.NewTLSFixture(t)
	fx2 := testutil.NewTLSFixture(t)
	h := &certHolder{}
	if err := h.Load(filepath.Join(fx1.Dir, "server.crt"), filepath.Join(fx1.Dir, "server.key")); err != nil {
		t.Fatal(err)
	}
	c1, err := h.GetCertificate(nil)
	if err != nil || c1 == nil {
		t.Fatal(err)
	}
	fp1 := sha256.Sum256(c1.Certificate[0])

	if err := h.Load(filepath.Join(fx2.Dir, "server.crt"), filepath.Join(fx2.Dir, "server.key")); err != nil {
		t.Fatal(err)
	}
	c2, err := h.GetCertificate(nil)
	if err != nil || c2 == nil {
		t.Fatal(err)
	}
	fp2 := sha256.Sum256(c2.Certificate[0])
	if fp1 == fp2 {
		t.Fatal("expected different cert after Load")
	}
}

func TestRehashTLSAndConfig(t *testing.T) {
	dir := t.TempDir()
	fxA := testutil.NewTLSFixture(t)
	fxB := testutil.NewTLSFixture(t)

	certPath := filepath.Join(dir, "server.crt")
	keyPath := filepath.Join(dir, "server.key")
	copyFile(t, filepath.Join(fxA.Dir, "server.crt"), certPath)
	copyFile(t, filepath.Join(fxA.Dir, "server.key"), keyPath)

	cfgPath := filepath.Join(dir, "gobnc.json")
	cfg := config.Default()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.DBPath = filepath.Join(dir, "t.db")
	cfg.ControlSocket = filepath.Join(dir, "gobnc.sock")
	cfg.TLSCert = certPath
	cfg.TLSKey = keyPath
	cfg.AllowPasswordAuth = true
	cfg.AllowCertAuth = true
	cfg.MaxClients = 32
	writeConfig(t, cfgPath, cfg)

	s, err := New(cfg, gobnclog.New("error", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	h, err := auth.HashPassword("s3cret")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Store().SetPasswordHash(ctx, h); err != nil {
		t.Fatal(err)
	}
	_, err = s.Store().UpsertNetwork(ctx, store.Network{
		Name: "libera", Host: "127.0.0.1", Port: 1, Nick: "n", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Bind an ephemeral port for the test without calling full Run (avoids uplink dial).
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.runCtx = runCtx
	s.cancel = cancel
	if err := s.certs.Load(cfg.TLSCert, cfg.TLSKey); err != nil {
		t.Fatal(err)
	}
	tlsCfg := &tls.Config{
		GetCertificate: s.certs.GetCertificate,
		ClientAuth:     tls.RequestClientCert,
		MinVersion:     tls.VersionTLS12,
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", tlsCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := ln.Addr().String()
	cfg.ListenAddr = addr
	s.cfg = cfg
	writeConfig(t, cfgPath, cfg)

	dl := downlink.NewListener(cfg, s.store, s, tlsCfg, s.log)
	s.dl = dl
	go func() { _ = dl.Serve(runCtx, ln) }()

	fpBefore := peerFingerprint(t, addr, fxA)
	wantA := serverLeafFP(t, filepath.Join(fxA.Dir, "server.crt"), filepath.Join(fxA.Dir, "server.key"))
	if fpBefore != wantA {
		t.Fatalf("before rehash fp=%s want %s", fpBefore, wantA)
	}

	// Replace cert files and tighten config.
	copyFile(t, filepath.Join(fxB.Dir, "server.crt"), certPath)
	copyFile(t, filepath.Join(fxB.Dir, "server.key"), keyPath)
	cfg.AllowCertAuth = false
	cfg.MaxClients = 7
	cfg.MaxFloodQueue = 42
	cfg.LegacyPlaybackMax = 99
	cfg.HistoryRetentionDays = 3
	cfg.QuitMessage = "rehashed"
	// Immutable-looking change should be ignored.
	cfg.ListenAddr = "0.0.0.0:9999"
	cfg.DBPath = filepath.Join(dir, "other.db")
	cfg.LogLevel = "debug"
	writeConfig(t, cfgPath, cfg)

	if err := s.Rehash(cfgPath); err != nil {
		t.Fatal(err)
	}

	s.mu.RLock()
	got := s.cfg
	s.mu.RUnlock()
	if got.ListenAddr != addr {
		t.Fatalf("listen_addr mutated: %q", got.ListenAddr)
	}
	if got.DBPath != filepath.Join(dir, "t.db") {
		t.Fatalf("db_path mutated: %q", got.DBPath)
	}
	if got.LogLevel != "info" {
		t.Fatalf("log_level should stay info, got %q", got.LogLevel)
	}
	if got.AllowCertAuth {
		t.Fatal("AllowCertAuth should be false after rehash")
	}
	if got.MaxClients != 7 {
		t.Fatalf("MaxClients=%d", got.MaxClients)
	}
	if got.MaxFloodQueue != 42 || got.LegacyPlaybackMax != 99 || got.HistoryRetentionDays != 3 {
		t.Fatalf("limits not applied: %+v", got)
	}
	if got.QuitMessage != "rehashed" {
		t.Fatalf("QuitMessage=%q", got.QuitMessage)
	}

	fpAfter := peerFingerprint(t, addr, fxB)
	wantB := serverLeafFP(t, filepath.Join(fxB.Dir, "server.crt"), filepath.Join(fxB.Dir, "server.key"))
	if fpAfter != wantB {
		t.Fatalf("after rehash fp=%s want %s", fpAfter, wantB)
	}
	if fpAfter == fpBefore {
		t.Fatal("server cert fingerprint unchanged after rehash")
	}
}

func TestRehashReloadsNetworkConfig(t *testing.T) {
	dir := t.TempDir()
	fx := testutil.NewTLSFixture(t)
	cfgPath := filepath.Join(dir, "gobnc.json")
	cfg := config.Default()
	cfg.DBPath = filepath.Join(dir, "t.db")
	cfg.ControlSocket = filepath.Join(dir, "c.sock")
	cfg.TLSCert = filepath.Join(fx.Dir, "server.crt")
	cfg.TLSKey = filepath.Join(fx.Dir, "server.key")
	cfg.ListenAddr = "127.0.0.1:0"
	writeConfig(t, cfgPath, cfg)

	s, err := New(cfg, gobnclog.New("error", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.runCtx = ctx
	s.cancel = cancel
	if err := s.certs.Load(cfg.TLSCert, cfg.TLSKey); err != nil {
		t.Fatal(err)
	}

	_, err = s.Store().UpsertNetwork(ctx, store.Network{
		Name: "net1", Host: "127.0.0.1", Port: 6697, Nick: "n", TLS: true, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.StartNetworkByName("net1"); err != nil {
		t.Fatal(err)
	}
	sess, err := s.Session("net1")
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.Store().UpsertNetwork(ctx, store.Network{
		Name: "net1", Host: "10.0.0.2", Port: 4443, Nick: "n", TLS: false, Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Rehash(cfgPath); err != nil {
		t.Fatal(err)
	}
	if sess.Network.Host != "10.0.0.2" || sess.Network.Port != 4443 || sess.Network.TLS {
		t.Fatalf("session network not updated: %+v", sess.Network)
	}
}

func writeConfig(t *testing.T, path string, cfg config.Config) {
	t.Helper()
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	b, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

func serverLeafFP(t *testing.T, certPath, keyPath string) string {
	t.Helper()
	pair, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(pair.Certificate[0])
	return hex.EncodeToString(sum[:])
}

func peerFingerprint(t *testing.T, addr string, fx *testutil.TLSFixture) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		c, err := tls.Dial("tcp", addr, &tls.Config{
			RootCAs:            fx.ClientTLS.RootCAs,
			ServerName:         "localhost",
			InsecureSkipVerify: false,
			MinVersion:         tls.VersionTLS12,
		})
		if err != nil {
			last = err
			time.Sleep(20 * time.Millisecond)
			continue
		}
		st := c.ConnectionState()
		_ = c.Close()
		if len(st.PeerCertificates) == 0 {
			t.Fatal("no peer cert")
		}
		sum := sha256.Sum256(st.PeerCertificates[0].Raw)
		return hex.EncodeToString(sum[:])
	}
	t.Fatalf("dial: %v", last)
	return ""
}
