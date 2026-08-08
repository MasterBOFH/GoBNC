package downlink

import (
	"context"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
)

func TestDownlinkKeepalivePING(t *testing.T) {
	oldIdle, oldGrace := KeepaliveIdle, KeepaliveGrace
	KeepaliveIdle = 80 * time.Millisecond
	KeepaliveGrace = time.Hour
	t.Cleanup(func() {
		KeepaliveIdle, KeepaliveGrace = oldIdle, oldGrace
	})

	a, b := net.Pipe()
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })

	cl := &Client{
		id:   "c1",
		conn: a,
		caps: make(map[string]bool),
		log:  slog.Default(),
	}
	atomic.StoreInt64(&cl.lastRXUnix, time.Now().Add(-200*time.Millisecond).UnixNano())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go cl.keepaliveLoop(ctx)

	_ = b.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 256)
	n, err := b.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	got := string(buf[:n])
	if !strings.Contains(got, "PING :gobnc") {
		t.Fatalf("expected PING :gobnc, got %q", got)
	}
}

func TestDownlinkKeepaliveTimeoutCloses(t *testing.T) {
	oldIdle, oldGrace := KeepaliveIdle, KeepaliveGrace
	KeepaliveIdle = 50 * time.Millisecond
	KeepaliveGrace = 50 * time.Millisecond
	t.Cleanup(func() {
		KeepaliveIdle, KeepaliveGrace = oldIdle, oldGrace
	})

	a, b := net.Pipe()
	t.Cleanup(func() { _ = b.Close() })

	cl := &Client{
		id:   "c1",
		conn: a,
		caps: make(map[string]bool),
		log:  slog.Default(),
	}
	atomic.StoreInt64(&cl.lastRXUnix, time.Now().Add(-time.Second).UnixNano())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go cl.keepaliveLoop(ctx)

	// Drain PINGs until close.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_ = b.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		buf := make([]byte, 256)
		_, err := b.Read(buf)
		if err != nil {
			if err == io.EOF || strings.Contains(err.Error(), "closed") {
				return
			}
			// timeout: keep waiting for close
			continue
		}
	}
	t.Fatal("expected downlink close after keepalive grace")
}

func TestClientSendPINGWire(t *testing.T) {
	a, b := net.Pipe()
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	cl := &Client{conn: a, caps: map[string]bool{}, log: slog.Default()}
	go func() {
		_ = cl.Send(irc.Message{Command: "PING", Params: []string{"gobnc"}})
	}()
	_ = b.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 64)
	n, err := b.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(buf[:n]), "PING :gobnc") {
		t.Fatalf("%q", buf[:n])
	}
}
