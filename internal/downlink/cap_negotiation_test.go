package downlink

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/internal/testutil"
)

// Bug 1: CAP LS 302 implicitly enables cap-notify without an explicit CAP REQ.

func TestClientCAPLS302EnablesCapNotifyImplicitly(t *testing.T) {
	server, client := net.Pipe()
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })

	cl := &Client{
		id:       "c1",
		conn:     client,
		r:        bufio.NewReader(client),
		caps:     map[string]bool{},
		capsSeen: map[string]bool{},
		sess:     session.New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil),
	}

	_ = readCAPReply(t, server, func() {
		if err := handleClientCAP(cl, irc.Message{Command: "CAP", Params: []string{"LS", "302"}}); err != nil {
			t.Fatal(err)
		}
	})
	if !cl.HasCap("cap-notify") {
		t.Fatal("CAP LS 302 must implicitly enable cap-notify without CAP REQ")
	}
}

func TestClientCAPLSWithoutVersionDoesNotEnableCapNotify(t *testing.T) {
	server, client := net.Pipe()
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })

	cl := &Client{
		id:       "c1",
		conn:     client,
		r:        bufio.NewReader(client),
		caps:     map[string]bool{},
		capsSeen: map[string]bool{},
		sess:     session.New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil),
	}

	_ = readCAPReply(t, server, func() {
		if err := handleClientCAP(cl, irc.Message{Command: "CAP", Params: []string{"LS"}}); err != nil {
			t.Fatal(err)
		}
	})
	if cl.HasCap("cap-notify") {
		t.Fatal("legacy CAP LS (no 302) must not implicitly enable cap-notify")
	}
}

// Bug 3: CAP LIST returns the capabilities actually negotiated (enabled).

func TestClientCAPLIST(t *testing.T) {
	server, client := net.Pipe()
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })

	cl := &Client{
		id:       "c1",
		conn:     client,
		r:        bufio.NewReader(client),
		caps:     map[string]bool{},
		capsSeen: map[string]bool{},
	}

	_ = readCAPReply(t, server, func() {
		if err := handleClientCAP(cl, irc.Message{
			Command: "CAP",
			Params:  []string{"REQ", "server-time batch"},
		}); err != nil {
			t.Fatal(err)
		}
	})

	line := readCAPReply(t, server, func() {
		if err := handleClientCAP(cl, irc.Message{Command: "CAP", Params: []string{"LIST"}}); err != nil {
			t.Fatal(err)
		}
	})
	if !strings.Contains(line, "LIST") {
		t.Fatalf("want CAP LIST reply: %q", line)
	}
	if !strings.Contains(line, "server-time") || !strings.Contains(line, "batch") {
		t.Fatalf("CAP LIST missing negotiated caps: %q", line)
	}
	if strings.Contains(line, "cap-notify") {
		t.Fatalf("CAP LIST must only report negotiated caps, not just-offered ones: %q", line)
	}
}

func TestClientCAPLISTEmptyWhenNothingNegotiated(t *testing.T) {
	server, client := net.Pipe()
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })

	cl := &Client{
		id:       "c1",
		conn:     client,
		r:        bufio.NewReader(client),
		caps:     map[string]bool{},
		capsSeen: map[string]bool{},
	}

	line := readCAPReply(t, server, func() {
		if err := handleClientCAP(cl, irc.Message{Command: "CAP", Params: []string{"LIST"}}); err != nil {
			t.Fatal(err)
		}
	})
	if !strings.HasSuffix(line, "LIST :") {
		t.Fatalf("want empty CAP LIST reply: %q", line)
	}
}

// Bug 2: a client connecting after the bouncer has already registered with
// the upstream must get ONE collated CAP LS (including uplink-backed caps),
// not CAP LS followed later by a redundant CAP NEW.

func TestCAPLSCollatedWhenUplinkAlreadyRegistered(t *testing.T) {
	for _, order := range []string{"pass-first", "cap-first"} {
		t.Run(order, func(t *testing.T) {
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
			if _, err := db.UpsertNetwork(ctx, store.Network{Name: "libera", Host: "x", Port: 1, Nick: "n", Enabled: true}); err != nil {
				t.Fatal(err)
			}
			netw, _ := db.NetworkByName(ctx, "libera")
			sess := session.New(netw, db, nil, nil, nil)
			sess.SetRegisteredForTest(true)
			sess.SetUpCapsForTest(map[string]bool{"away-notify": true, "chghost": true})

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
			c, err := tls.Dial("tcp", ln.Addr().String(), clientTLS)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = c.Close() })

			write := func(s string) { _, _ = c.Write([]byte(s + "\r\n")) }
			if order == "pass-first" {
				write("PASS libera/s3cret")
				write("CAP LS 302")
			} else {
				write("CAP LS 302")
				write("PASS libera/s3cret")
			}
			write("NICK me")
			write("USER me 0 * :me")
			write("CAP END")

			br := bufio.NewReader(c)
			capLS, err := readMatchingLine(c, br, func(l string) bool {
				return strings.Contains(l, "CAP") && strings.Contains(l, "LS")
			}, 3*time.Second)
			if err != nil {
				t.Fatalf("CAP LS reply: %v", err)
			}
			if !strings.Contains(capLS, "away-notify") || !strings.Contains(capLS, "chghost") {
				t.Fatalf("collated CAP LS missing uplink-backed caps: %q", capLS)
			}

			if _, err := readMatchingLine(c, br, func(l string) bool {
				return strings.Contains(l, "376")
			}, 3*time.Second); err != nil {
				t.Fatalf("end of registration burst: %v", err)
			}

			// No further CAP NEW should follow: the collated LS already told
			// the client about the uplink-backed caps.
			_ = c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
			line, err := br.ReadString('\n')
			if err == nil && strings.Contains(line, "CAP") && strings.Contains(line, "NEW") {
				t.Fatalf("unexpected duplicate CAP NEW after collated CAP LS: %q", line)
			}
		})
	}
}

func readMatchingLine(c net.Conn, br *bufio.Reader, match func(string) bool, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_ = c.SetReadDeadline(time.Now().Add(timeout))
		line, err := br.ReadString('\n')
		if err != nil {
			return "", err
		}
		line = strings.TrimRight(line, "\r\n")
		if match(line) {
			return line, nil
		}
	}
	return "", fmt.Errorf("timeout waiting for matching line")
}

// A CAP LS that arrives after a PASS whose network did not resolve (no
// network/ prefix, or an unknown name) must be answered immediately, not
// deferred: PASS has already gone by, so nothing later in registration can
// resolve the network, and a deferred reply would never be sent — the
// client waits for CAP LS before CAP END, and the login sits silent until
// the 30s auth timeout instead of failing at once.
func TestCAPLSAfterUnresolvedPASSIsAnsweredImmediately(t *testing.T) {
	for _, pass := range []string{"PASS justthepassword", "PASS nosuchnet/pw"} {
		t.Run(pass, func(t *testing.T) {
			fx := testutil.NewTLSFixture(t)
			db, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			cfg := config.Default()
			cfg.AllowPasswordAuth = true
			cfg.AllowCertAuth = true
			_ = db.SetPasswordHash(context.Background(), mustHash(t, "pw"))

			ln, err := tls.Listen("tcp", "127.0.0.1:0", fx.ServerTLS)
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			sess := session.New(store.Network{Name: "net", Nick: "x"}, db, nil, nil, nil)
			l := NewListener(cfg, db, &memMgr{s: sess}, fx.ServerTLS, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() { _ = l.Serve(ctx, ln) }()

			c, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{RootCAs: fx.ClientTLS.RootCAs, ServerName: "localhost"})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			r := bufio.NewReader(c)
			_, _ = c.Write([]byte(pass + "\r\nCAP LS 302\r\nNICK me\r\nUSER me 0 * :me\r\n"))
			_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
			line, err := r.ReadString('\n')
			if err != nil || !strings.Contains(line, "CAP * LS") {
				t.Fatalf("want an immediate CAP LS reply, got %q err=%v", line, err)
			}
			// Client finishes negotiation; auth must now fail right away.
			_, _ = c.Write([]byte("CAP END\r\n"))
			line, err = r.ReadString('\n')
			if err != nil || !strings.HasPrefix(line, "ERROR ") {
				t.Fatalf("want an immediate ERROR, got %q err=%v", line, err)
			}
			if _, err = r.ReadString('\n'); err == nil {
				t.Fatal("connection must be closed after ERROR")
			}
		})
	}
}
