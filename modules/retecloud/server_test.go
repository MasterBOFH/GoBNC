package retecloud

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/MasterBOFH/GoBNC/internal/testutil"
)

// startListener serves the WSS listener on a free port; accept gets each WebSocket.
func startListener(t *testing.T, fx *testutil.TLSFixture, maxConns int, accept func(*websocket.Conn, string, string)) string {
	t.Helper()
	cfg := Config{ListenAddr: "127.0.0.1:0", TLSCert: filepath.Join(fx.Dir, "server.crt"),
		TLSKey: filepath.Join(fx.Dir, "server.key"), MaxConnections: maxConns}
	l, err := NewListener(cfg, nil, accept)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = l.ServeListener(ctx, ln); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return "wss://" + ln.Addr().String() + "/ws"
}

func dialWS(url string, tlsCfg *tls.Config) (*websocket.Conn, *http.Response, error) {
	d := websocket.Dialer{TLSClientConfig: tlsCfg, HandshakeTimeout: 5 * time.Second}
	return d.Dial(url, nil)
}

func TestTLSWithAndWithoutClientCert(t *testing.T) {
	fx := testutil.NewTLSFixture(t)
	got := make(chan string, 2)
	url := startListener(t, fx, 16, func(ws *websocket.Conn, fp, ip string) {
		got <- fp
		ws.Close()
	})
	withCert, _, err := dialWS(url, fx.ClientTLS)
	if err != nil {
		t.Fatalf("dial with client cert: %v", err)
	}
	withCert.Close()
	if fp := <-got; fp != fx.ClientSHA256 {
		t.Fatalf("fingerprint = %q, want %q", fp, fx.ClientSHA256)
	}
	noCert := fx.ClientTLS.Clone()
	noCert.Certificates = nil
	ws, _, err := dialWS(url, noCert)
	if err != nil {
		t.Fatalf("dial without a client cert: %v", err)
	}
	ws.Close()
	if fp := <-got; fp != "" {
		t.Fatalf("fingerprint without a cert = %q", fp)
	}
}

func TestSeventeenthConnectionGets503(t *testing.T) {
	fx := testutil.NewTLSFixture(t)
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })
	url := startListener(t, fx, 16, func(ws *websocket.Conn, fp, ip string) {
		<-hold
		ws.Close()
	})
	for i := 0; i < 16; i++ {
		ws, _, err := dialWS(url, fx.ClientTLS)
		if err != nil {
			t.Fatalf("connection %d: %v", i+1, err)
		}
		t.Cleanup(func() { ws.Close() })
	}
	_, resp, err := dialWS(url, fx.ClientTLS)
	if err == nil || resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		code := 0
		if resp != nil {
			code = resp.StatusCode
		}
		t.Fatalf("17th connection: err=%v status=%d, want 503", err, code)
	}
}

func TestCrossOriginBrowserRefused(t *testing.T) {
	fx := testutil.NewTLSFixture(t)
	url := startListener(t, fx, 16, func(ws *websocket.Conn, fp, ip string) { ws.Close() })
	d := websocket.Dialer{TLSClientConfig: fx.ClientTLS, HandshakeTimeout: 5 * time.Second}
	_, resp, err := d.Dial(url, http.Header{"Origin": []string{"https://evil.example"}})
	if err == nil || resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-origin upgrade: err=%v resp=%v, want 403", err, resp)
	}
	ws, _, err := dialWS(url, fx.ClientTLS) // native client: no Origin
	if err != nil {
		t.Fatalf("no-Origin client refused: %v", err)
	}
	ws.Close()
}
