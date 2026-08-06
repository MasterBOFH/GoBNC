package downlink

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/auth"
	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/internal/testutil"
)

type memMgr struct {
	s *session.Session
}

func (m *memMgr) Session(network string) (*session.Session, error) {
	if network == m.s.Name() {
		return m.s, nil
	}
	return nil, context.Canceled
}

func TestAuthPasswordAndCert(t *testing.T) {
	fx := testutil.NewTLSFixture(t)
	db, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	h, err := auth.HashPassword("s3cret")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetPasswordHash(ctx, h); err != nil {
		t.Fatal(err)
	}
	if err := db.AddFingerprint(ctx, fx.ClientSHA256, "test"); err != nil {
		t.Fatal(err)
	}
	_, err = db.UpsertNetwork(ctx, store.Network{Name: "libera", Host: "x", Port: 1, Nick: "n", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	netw, _ := db.NetworkByName(ctx, "libera")
	sess := session.New(netw, db, nil, nil)
	mgr := &memMgr{s: sess}

	cfg := config.Default()
	cfg.AllowPasswordAuth = true
	cfg.AllowCertAuth = true

	ln, err := tls.Listen("tcp", "127.0.0.1:0", fx.ServerTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	l := NewListener(cfg, db, mgr, fx.ServerTLS, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = l.Serve(ctx, ln) }()

	// Password auth without client cert
	clientTLS := &tls.Config{RootCAs: fx.ClientTLS.RootCAs, ServerName: "localhost", MinVersion: tls.VersionTLS12}
	c, err := tls.Dial("tcp", ln.Addr().String(), clientTLS)
	if err != nil {
		t.Fatal(err)
	}
	write := func(s string) { _, _ = c.Write([]byte(s + "\r\n")) }
	write("PASS libera/s3cret")
	write("NICK me")
	write("USER me 0 * :me")
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 4096)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(string(buf[:n]), "001") {
		t.Fatalf("got %q", buf[:n])
	}
	_ = c.Close()

	// Cert auth without password: PASS network/ or bare network
	c2, err := tls.Dial("tcp", ln.Addr().String(), fx.ClientTLS)
	if err != nil {
		t.Fatal(err)
	}
	write2 := func(s string) { _, _ = c2.Write([]byte(s + "\r\n")) }
	write2("PASS libera/")
	write2("NICK me")
	write2("USER me 0 * :me")
	_ = c2.SetReadDeadline(time.Now().Add(3 * time.Second))
	n, err = c2.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(string(buf[:n]), "001") {
		t.Fatalf("cert auth got %q", buf[:n])
	}
	_ = c2.Close()
}

func TestAuthFailClosed(t *testing.T) {
	fx := testutil.NewTLSFixture(t)
	db, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cfg := config.Default()
	cfg.AllowPasswordAuth = true
	cfg.AllowCertAuth = false
	_ = db.SetPasswordHash(context.Background(), mustHash(t, "right"))

	ln, err := tls.Listen("tcp", "127.0.0.1:0", fx.ServerTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	sess := session.New(store.Network{Name: "n", Nick: "x"}, db, nil, nil)
	l := NewListener(cfg, db, &memMgr{s: sess}, fx.ServerTLS, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = l.Serve(ctx, ln) }()

	clientTLS := &tls.Config{RootCAs: fx.ClientTLS.RootCAs, ServerName: "localhost"}
	c, err := tls.Dial("tcp", ln.Addr().String(), clientTLS)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = c.Write([]byte("PASS n/wrong\r\nNICK me\r\nUSER me 0 * :me\r\n"))
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1024)
	n, _ := c.Read(buf)
	if contains(string(buf[:n]), "001") {
		t.Fatal("should not auth")
	}
}

func TestPlainTCPRejected(t *testing.T) {
	fx := testutil.NewTLSFixture(t)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", fx.ServerTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	db, _ := store.Open(filepath.Join(t.TempDir(), "t.db"))
	defer db.Close()
	l := NewListener(config.Default(), db, &memMgr{s: session.New(store.Network{Name: "n"}, db, nil, nil)}, fx.ServerTLS, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = l.Serve(ctx, ln) }()

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, _ = c.Write([]byte("NICK x\r\n"))
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 64)
	_, err = c.Read(buf)
	// Should fail or get TLS alert — not a successful IRC 001
	if err == nil && contains(string(buf), "001") {
		t.Fatal("plain should not work")
	}
}

func TestRegistrationReady(t *testing.T) {
	cases := []struct {
		name                       string
		nick                       string
		gotUser, capStarted, capEnded bool
		want                       bool
	}{
		{"empty", "", false, false, false, false},
		{"nick only", "me", false, false, false, false},
		{"user only", "", true, false, false, false},
		{"nick+user no cap", "me", true, false, false, true},
		{"nick+user cap pending", "me", true, true, false, false},
		{"nick+user cap end", "me", true, true, true, true},
		{"cap end before nick/user", "", false, true, true, false},
		{"cap end + nick only", "me", false, true, true, false},
		{"cap end + user only", "", true, true, true, false},
		{"cap end then nick+user", "me", true, true, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := registrationReady(tc.nick, tc.gotUser, tc.capStarted, tc.capEnded)
			if got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}

func TestAuthCAPEndAfterNickUser(t *testing.T) {
	c := dialAuthClient(t, false)
	defer c.Close()
	write := func(s string) { _, _ = c.Write([]byte(s + "\r\n")) }
	// CAP LS first, then NICK/USER, then CAP END — common IRCv3 order.
	write("CAP LS")
	write("PASS libera/s3cret")
	write("NICK me")
	write("USER me 0 * :me")
	write("CAP END")
	if err := readUntil(c, "001", 3*time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestAuthCAPEndBeforeNickUser(t *testing.T) {
	c := dialAuthClient(t, false)
	defer c.Close()
	write := func(s string) { _, _ = c.Write([]byte(s + "\r\n")) }
	// CAP END before NICK/USER.
	write("CAP LS")
	write("PASS libera/s3cret")
	write("CAP END")
	write("NICK me")
	write("USER me 0 * :me")
	if err := readUntil(c, "001", 3*time.Second); err != nil {
		t.Fatal(err)
	}
}

// dialAuthClient starts a listener with password auth for network "libera".
func dialAuthClient(t *testing.T, withClientCert bool) *tls.Conn {
	t.Helper()
	fx := testutil.NewTLSFixture(t)
	db, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	if err := db.SetPasswordHash(ctx, mustHash(t, "s3cret")); err != nil {
		t.Fatal(err)
	}
	_, err = db.UpsertNetwork(ctx, store.Network{Name: "libera", Host: "x", Port: 1, Nick: "n", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	netw, _ := db.NetworkByName(ctx, "libera")
	sess := session.New(netw, db, nil, nil)

	cfg := config.Default()
	cfg.AllowPasswordAuth = true
	cfg.AllowCertAuth = false

	ln, err := tls.Listen("tcp", "127.0.0.1:0", fx.ServerTLS)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	l := NewListener(cfg, db, &memMgr{s: sess}, fx.ServerTLS, nil)
	serveCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = l.Serve(serveCtx, ln) }()

	clientTLS := &tls.Config{RootCAs: fx.ClientTLS.RootCAs, ServerName: "localhost", MinVersion: tls.VersionTLS12}
	if withClientCert {
		clientTLS = fx.ClientTLS
	}
	c, err := tls.Dial("tcp", ln.Addr().String(), clientTLS)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func readUntil(c net.Conn, want string, timeout time.Duration) error {
	_ = c.SetReadDeadline(time.Now().Add(timeout))
	var got string
	buf := make([]byte, 4096)
	for {
		n, err := c.Read(buf)
		if n > 0 {
			got += string(buf[:n])
			if contains(got, want) {
				return nil
			}
		}
		if err != nil {
			return fmt.Errorf("read until %q: %w (got %q)", want, err, got)
		}
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || stringIndex(s, sub) >= 0)
}
func stringIndex(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
func mustHash(t *testing.T, p string) string {
	t.Helper()
	h, err := auth.HashPassword(p)
	if err != nil {
		t.Fatal(err)
	}
	return h
}
