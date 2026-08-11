package downlink

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/auth"
	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/irc"
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

func TestPassParamWithAndWithoutColon(t *testing.T) {
	cases := []struct {
		line, network, secret string
	}{
		{"PASS Undernet/s3cret", "Undernet", "s3cret"},
		{"PASS :Undernet/s3cret", "Undernet", "s3cret"},
		{"PASS Undernet/s3cret 99", "Undernet", "s3cret 99"},
		{"PASS :Undernet/s3cret 99", "Undernet", "s3cret 99"},
		{"PASS Undernet/", "Undernet", ""},
		{"PASS :Undernet/", "Undernet", ""},
		{"PASS Undernet", "Undernet", ""},
	}
	for _, tc := range cases {
		msg, err := irc.Parse(tc.line)
		if err != nil {
			t.Fatalf("%q: %v", tc.line, err)
		}
		pass := msg.ParamsText()
		if got := networkFromPass(pass); got != tc.network {
			t.Fatalf("%q network=%q want %q (pass=%q)", tc.line, got, tc.network, pass)
		}
		if got := stripNetworkFromPass(pass); got != tc.secret {
			t.Fatalf("%q secret=%q want %q (pass=%q)", tc.line, got, tc.secret, pass)
		}
	}
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

type logCapture struct {
	mu      sync.Mutex
	records []string
}

func (h *logCapture) Enabled(context.Context, slog.Level) bool { return true }
func (h *logCapture) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder
	b.WriteString(r.Message)
	r.Attrs(func(a slog.Attr) bool {
		b.WriteByte(' ')
		b.WriteString(a.String())
		return true
	})
	h.mu.Lock()
	h.records = append(h.records, b.String())
	h.mu.Unlock()
	return nil
}
func (h *logCapture) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *logCapture) WithGroup(string) slog.Handler      { return h }
func (h *logCapture) has(substr string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if strings.Contains(r, substr) {
			return true
		}
	}
	return false
}

func TestAuthFailedLoggedUnknownNetwork(t *testing.T) {
	fx := testutil.NewTLSFixture(t)
	db, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cfg := config.Default()
	cfg.AllowPasswordAuth = true
	cfg.AllowCertAuth = false
	_ = db.SetPasswordHash(context.Background(), mustHash(t, "s3cret"))

	cap := &logCapture{}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", fx.ServerTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	sess := session.New(store.Network{Name: "libera", Nick: "x"}, db, nil, nil)
	l := NewListener(cfg, db, &memMgr{s: sess}, fx.ServerTLS, slog.New(cap))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = l.Serve(ctx, ln) }()

	clientTLS := &tls.Config{RootCAs: fx.ClientTLS.RootCAs, ServerName: "localhost"}
	c, err := tls.Dial("tcp", ln.Addr().String(), clientTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, _ = c.Write([]byte("PASS nosuch/s3cret\r\nNICK me\r\nUSER me 0 * :me\r\n"))
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1024)
	_, _ = c.Read(buf)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cap.has("auth failed") && cap.has("unknown network") && cap.has("nosuch") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("missing auth failed log for unknown network: %#v", cap.records)
}

func TestAuthFailedLoggedInvalidPassword(t *testing.T) {
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

	cap := &logCapture{}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", fx.ServerTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	sess := session.New(store.Network{Name: "n", Nick: "x"}, db, nil, nil)
	l := NewListener(cfg, db, &memMgr{s: sess}, fx.ServerTLS, slog.New(cap))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = l.Serve(ctx, ln) }()

	clientTLS := &tls.Config{RootCAs: fx.ClientTLS.RootCAs, ServerName: "localhost"}
	c, err := tls.Dial("tcp", ln.Addr().String(), clientTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, _ = c.Write([]byte("PASS n/wrong\r\nNICK me\r\nUSER me 0 * :me\r\n"))
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1024)
	_, _ = c.Read(buf)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cap.has("auth failed") && cap.has("invalid password") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("missing auth failed log for invalid password: %#v", cap.records)
}

func TestAuthFailedLoggedInvalidFingerprint(t *testing.T) {
	fx := testutil.NewTLSFixture(t)
	db, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cfg := config.Default()
	cfg.AllowPasswordAuth = false
	cfg.AllowCertAuth = true
	// No fingerprints registered — client cert must fail before IRC auth.

	cap := &logCapture{}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", fx.ServerTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	sess := session.New(store.Network{Name: "libera", Nick: "x"}, db, nil, nil)
	l := NewListener(cfg, db, &memMgr{s: sess}, fx.ServerTLS, slog.New(cap))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = l.Serve(ctx, ln) }()

	c, err := tls.Dial("tcp", ln.Addr().String(), fx.ClientTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// CAP LS would hang waiting for a reply if we entered authenticate(); early reject must ERROR first.
	_, _ = c.Write([]byte("CAP LS 302\r\nPASS libera/\r\nNICK me\r\nUSER me 0 * :me\r\n"))
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1024)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	got := string(buf[:n])
	if contains(got, "CAP") {
		t.Fatalf("cert-only reject must not answer CAP before auth: %q", got)
	}
	wantErr := "Authentication failed (cert fingerprint: " + fx.ClientSHA256 + ")"
	if !contains(got, wantErr) {
		t.Fatalf("ERROR missing fingerprint: got %q want substring %q", got, wantErr)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cap.has("auth failed") && cap.has("invalid fingerprint") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("missing auth failed log for invalid fingerprint: %#v", cap.records)
}

func TestAuthFailedCertOnlyMissingCert(t *testing.T) {
	fx := testutil.NewTLSFixture(t)
	db, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cfg := config.Default()
	cfg.AllowPasswordAuth = false
	cfg.AllowCertAuth = true

	cap := &logCapture{}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", fx.ServerTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	sess := session.New(store.Network{Name: "libera", Nick: "x"}, db, nil, nil)
	l := NewListener(cfg, db, &memMgr{s: sess}, fx.ServerTLS, slog.New(cap))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = l.Serve(ctx, ln) }()

	clientTLS := &tls.Config{RootCAs: fx.ClientTLS.RootCAs, ServerName: "localhost", MinVersion: tls.VersionTLS12}
	c, err := tls.Dial("tcp", ln.Addr().String(), clientTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, _ = c.Write([]byte("PASS libera/\r\nNICK me\r\nUSER me 0 * :me\r\n"))
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1024)
	n, _ := c.Read(buf)
	got := string(buf[:n])
	if !contains(got, "Authentication failed") {
		t.Fatalf("want Authentication failed, got %q", got)
	}
	if contains(got, "cert fingerprint:") {
		t.Fatalf("no cert presented; ERROR must not invent a fingerprint: %q", got)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cap.has("auth failed") && cap.has("client certificate required") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("missing auth failed log for missing cert: %#v", cap.records)
}

func TestAuthCertOnlySuccess(t *testing.T) {
	fx := testutil.NewTLSFixture(t)
	db, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if err := db.AddFingerprint(ctx, fx.ClientSHA256, "test"); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.AllowPasswordAuth = false
	cfg.AllowCertAuth = true

	ln, err := tls.Listen("tcp", "127.0.0.1:0", fx.ServerTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	sess := session.New(store.Network{Name: "libera", Nick: "x"}, db, nil, nil)
	l := NewListener(cfg, db, &memMgr{s: sess}, fx.ServerTLS, nil)
	serveCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = l.Serve(serveCtx, ln) }()

	c, err := tls.Dial("tcp", ln.Addr().String(), fx.ClientTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	write := func(s string) { _, _ = c.Write([]byte(s + "\r\n")) }
	write("PASS libera/")
	write("NICK me")
	write("USER me 0 * :me")
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 4096)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(string(buf[:n]), "001") {
		t.Fatalf("cert-only auth got %q", buf[:n])
	}
}

func TestAuthFailedText(t *testing.T) {
	if got := authFailedText(""); got != "Authentication failed" {
		t.Fatalf("empty fp: %q", got)
	}
	if got := authFailedText("abc"); got != "Authentication failed (cert fingerprint: abc)" {
		t.Fatalf("with fp: %q", got)
	}
}

func TestPeerIP(t *testing.T) {
	if got := peerIP(nil); got != "" {
		t.Fatalf("nil conn: %q", got)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	done := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			done <- nil
			return
		}
		done <- c
	}()
	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server := <-done
	if server == nil {
		t.Fatal("accept failed")
	}
	defer server.Close()
	if got := peerIP(server); got != "127.0.0.1" {
		t.Fatalf("peerIP = %q, want 127.0.0.1", got)
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

func TestReplaceListenerAcceptsOnNewAddr(t *testing.T) {
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
	cfg := config.Default()
	cfg.AllowPasswordAuth = true
	cfg.AllowCertAuth = false
	sess := session.New(store.Network{Name: "libera", Nick: "x"}, db, nil, nil)
	l := NewListener(cfg, db, &memMgr{s: sess}, fx.ServerTLS, nil)

	ln1, err := tls.Listen("tcp", "127.0.0.1:0", fx.ServerTLS)
	if err != nil {
		t.Fatal(err)
	}
	oldAddr := ln1.Addr().String()
	serveCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- l.Serve(serveCtx, ln1) }()

	// Wait until the first listener is accepting.
	waitDial := func(addr string) error {
		deadline := time.Now().Add(2 * time.Second)
		var last error
		for time.Now().Before(deadline) {
			c, err := tls.Dial("tcp", addr, &tls.Config{
				RootCAs: fx.ClientTLS.RootCAs, ServerName: "localhost", MinVersion: tls.VersionTLS12,
			})
			if err != nil {
				last = err
				time.Sleep(20 * time.Millisecond)
				continue
			}
			_ = c.Close()
			return nil
		}
		return last
	}
	if err := waitDial(oldAddr); err != nil {
		t.Fatalf("first listener: %v", err)
	}

	ln2, err := tls.Listen("tcp", "127.0.0.1:0", fx.ServerTLS)
	if err != nil {
		t.Fatal(err)
	}
	newAddr := ln2.Addr().String()
	l.ReplaceListener(ln2)

	deadline := time.Now().Add(2 * time.Second)
	var dialErr error
	for time.Now().Before(deadline) {
		c, err := tls.Dial("tcp", newAddr, &tls.Config{
			RootCAs: fx.ClientTLS.RootCAs, ServerName: "localhost", MinVersion: tls.VersionTLS12,
		})
		if err != nil {
			dialErr = err
			time.Sleep(20 * time.Millisecond)
			continue
		}
		_, _ = c.Write([]byte("PASS libera/s3cret\r\nNICK me\r\nUSER me 0 * :me\r\n"))
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 2048)
		n, err := c.Read(buf)
		_ = c.Close()
		if err == nil && contains(string(buf[:n]), "001") {
			select {
			case err := <-errCh:
				t.Fatalf("Serve exited early: %v", err)
			default:
			}
			return
		}
		dialErr = fmt.Errorf("auth reply: %v %q", err, buf[:n])
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("dial new addr: %v", dialErr)
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

func TestMaxClientsRejectsExcess(t *testing.T) {
	fx := testutil.NewTLSFixture(t)
	db, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
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
	mgr := &memMgr{s: sess}

	cfg := config.Default()
	cfg.MaxClients = 1
	cfg.AllowCertAuth = false

	ln, err := tls.Listen("tcp", "127.0.0.1:0", fx.ServerTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	l := NewListener(cfg, db, mgr, fx.ServerTLS, nil)
	serveCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = l.Serve(serveCtx, ln) }()

	clientTLS := &tls.Config{RootCAs: fx.ClientTLS.RootCAs, ServerName: "localhost", MinVersion: tls.VersionTLS12}
	c1, err := tls.Dial("tcp", ln.Addr().String(), clientTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	// Hold the single slot in auth (slow registration).
	time.Sleep(50 * time.Millisecond)

	c2, err := tls.Dial("tcp", ln.Addr().String(), clientTLS)
	if err != nil {
		// Closed during handshake is fine — slot was refused.
		return
	}
	defer c2.Close()
	_ = c2.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 64)
	_, err = c2.Read(buf)
	// Second connection should be closed promptly (EOF / reset), not serve IRC.
	if err == nil {
		t.Fatalf("expected second connection closed, read %q", buf)
	}
}

func TestDownlinkLineTooLongSends417(t *testing.T) {
	fx := testutil.NewTLSFixture(t)
	db, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
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
	mgr := &memMgr{s: sess}

	cfg := config.Default()
	cfg.AllowCertAuth = false
	ln, err := tls.Listen("tcp", "127.0.0.1:0", fx.ServerTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	l := NewListener(cfg, db, mgr, fx.ServerTLS, nil)
	serveCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = l.Serve(serveCtx, ln) }()

	clientTLS := &tls.Config{RootCAs: fx.ClientTLS.RootCAs, ServerName: "localhost", MinVersion: tls.VersionTLS12}
	c, err := tls.Dial("tcp", ln.Addr().String(), clientTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	write := func(s string) { _, _ = c.Write([]byte(s + "\r\n")) }
	write("PASS libera/s3cret")
	write("NICK me")
	write("USER me 0 * :me")
	if err := readUntil(c, "001", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	huge := strings.Repeat("A", irc.MaxClientLine) + "\r\nPING gobnc\r\n"
	if _, err := c.Write([]byte(huge)); err != nil {
		t.Fatal(err)
	}
	if err := readUntil(c, " 417 ", 5*time.Second); err != nil {
		t.Fatal(err)
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
