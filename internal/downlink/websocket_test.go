package downlink

import (
	"bufio"
	"context"
	"crypto/tls"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/auth"
	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/session"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/internal/testutil"
	"github.com/MasterBOFH/GoBNC/internal/wsconn"
)

// A WebSocket IRC client (IRCv3 websocket extension) reaches the same
// listener, on the same TLS port, as a plain IRC client: the listener peeks
// the opening "GET ", upgrades, and runs the identical authenticate/attach
// session over the framed transport. Uses wsconn.Client as the client side,
// so both ends of the adapter are exercised end to end.
func TestDownlinkAcceptsWebSocketClient(t *testing.T) {
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
	if _, err := db.UpsertNetwork(ctx, store.Network{Name: "libera", Host: "x", Port: 1, Nick: "n", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	netw, _ := db.NetworkByName(ctx, "libera")
	sess := session.New(netw, db, nil, nil, nil)

	cfg := config.Default()
	cfg.AllowPasswordAuth = true

	ln, err := tls.Listen("tcp", "127.0.0.1:0", fx.ServerTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	l := NewListener(cfg, db, &memMgr{s: sess}, fx.ServerTLS, nil)
	serveCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = l.Serve(serveCtx, ln) }()

	// TLS to the listener, then the IRCv3 WebSocket client handshake over it.
	clientTLS := &tls.Config{RootCAs: fx.ClientTLS.RootCAs, ServerName: "localhost", MinVersion: tls.VersionTLS12}
	tc, err := tls.Dial("tcp", ln.Addr().String(), clientTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.Close()
	dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dcancel()
	ws, sub, err := wsconn.Client(dctx, tc, "wss://localhost/", 4096)
	if err != nil {
		t.Fatalf("websocket client handshake against the downlink: %v", err)
	}
	defer ws.Close()
	if sub != "binary.ircv3.net" {
		t.Fatalf("negotiated subprotocol %q, want binary.ircv3.net", sub)
	}

	write := func(s string) { _, _ = ws.Write([]byte(s + "\r\n")) }
	write("PASS libera/s3cret")
	write("NICK me")
	write("USER me 0 * :me")

	_ = ws.SetReadDeadline(time.Now().Add(3 * time.Second))
	br := bufio.NewReader(ws)
	got001 := false
	for i := 0; i < 20 && !got001; i++ {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("read: %v (so far no 001)", err)
		}
		if strings.Contains(line, " 001 ") {
			got001 = true
		}
	}
	if !got001 {
		t.Fatal("WebSocket client never received 001 (registration did not complete over WS)")
	}
}
