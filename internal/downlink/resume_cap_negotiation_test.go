package downlink

import (
	"bufio"
	"context"
	"crypto/tls"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/keeper"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/internal/testutil"
)

// TestCAPLSCollatedAfterResume is TestCAPLSCollatedWhenUplinkAlreadyRegistered's
// missing counterpart: that test proves collated CAP LS works, but seeds the
// session's state via SetRegisteredForTest/SetUpCapsForTest — test-only
// shortcuts that bypass the actual resume path entirely. A brain reattaching
// to a keeper (docs/keeper-design.md's "Blob store and gap-only resume")
// restores registered state and the enabled cap set through
// Session.SeedFromBlob instead, and nothing in this repo had driven a real
// client's CAP LS/REQ/END wire exchange against a session built that way
// until this test. Also covers the second half of the same question: that a
// client mid-CAP-negotiation receives no live traffic at all, resumed
// session or not — internal/downlink.authenticate only calls sess.Attach
// (which is what adds a client to the broadcast fan-out) after CAP END, so
// this pins that as an observed behavior, not just an assumption about the
// code's structure.
func TestCAPLSCollatedAfterResume(t *testing.T) {
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

	// The real resume path, not a test shortcut: mirrors exactly what a
	// resumed brain's HelloAck delivers (see Session.SeedFromBlob's own doc
	// comment) — a resolved "caps" entry (replace mode, the enabled set),
	// nothing else needed for this test.
	sess.SeedFromBlob([]keeper.BlobEntry{
		{Key: "self-nick", Values: [][]byte{[]byte("n")}},
		{Key: "caps", Values: [][]byte{[]byte(`["away-notify","chghost"]`)}},
	})
	if !sess.Registered() {
		t.Fatal("SeedFromBlob must leave the session registered")
	}

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
	write("PASS libera/s3cret")
	write("CAP LS 302")
	write("NICK me")
	write("USER me 0 * :me")
	write("CAP REQ :away-notify")

	br := bufio.NewReader(c)
	capLS, err := readMatchingLine(c, br, func(l string) bool {
		return strings.Contains(l, "CAP") && strings.Contains(l, "LS")
	}, 3*time.Second)
	if err != nil {
		t.Fatalf("CAP LS reply: %v", err)
	}
	if !strings.Contains(capLS, "away-notify") || !strings.Contains(capLS, "chghost") {
		t.Fatalf("collated CAP LS after resume missing blob-restored caps: %q", capLS)
	}
	if _, err := readMatchingLine(c, br, func(l string) bool {
		return strings.Contains(l, "CAP") && strings.Contains(l, "ACK") && strings.Contains(l, "away-notify")
	}, 3*time.Second); err != nil {
		t.Fatalf("CAP ACK reply: %v", err)
	}

	// Still mid-negotiation (no CAP END yet) — a live message must not
	// reach this client. sess.HandleMessage is the same entry point real
	// uplink traffic arrives through; nothing about it knows or cares that
	// this particular downlink hasn't finished CAP negotiation, so the only
	// thing that can be stopping delivery is that Attach hasn't added this
	// client to the broadcast set yet.
	sess.HandleMessage(irc.Message{Source: "irc.example", Command: "NOTICE", Params: []string{"me", "should not arrive before CAP END"}})
	_ = c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if line, err := br.ReadString('\n'); err == nil {
		t.Fatalf("message delivered before CAP END: %q", line)
	}

	write("CAP END")

	if _, err := readMatchingLine(c, br, func(l string) bool {
		return strings.Contains(l, "376")
	}, 3*time.Second); err != nil {
		t.Fatalf("end of registration burst after CAP END: %v", err)
	}

	// Now attached: the same message shape must arrive.
	sess.HandleMessage(irc.Message{Source: "irc.example", Command: "NOTICE", Params: []string{"me", "should arrive after CAP END"}})
	if _, err := readMatchingLine(c, br, func(l string) bool {
		return strings.Contains(l, "should arrive after CAP END")
	}, 3*time.Second); err != nil {
		t.Fatalf("message never delivered after CAP END: %v", err)
	}
}
