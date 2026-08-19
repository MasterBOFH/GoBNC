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

	"github.com/MasterBOFH/GoBNC/internal/config"
	"github.com/MasterBOFH/GoBNC/internal/irc"
)

func TestDownlinkKeepalivePING(t *testing.T) {
	idle := 80 * time.Millisecond
	grace := time.Hour

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
	go cl.keepaliveLoop(ctx, idle, grace)

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
	idle := 50 * time.Millisecond
	grace := 50 * time.Millisecond

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
	go cl.keepaliveLoop(ctx, idle, grace)

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

// TestRuntimeReadTimeoutExceedsKeepaliveBudget guards against the raw
// per-line read deadline racing keepaliveLoop's own PING+grace close: a
// client that answers every one of its own pings should never get silently
// dropped by a shorter, unlogged read timeout underneath keepaliveLoop.
func TestRuntimeReadTimeoutExceedsKeepaliveBudget(t *testing.T) {
	idle := 200 * time.Second
	grace := 200 * time.Second

	budget := idle + grace
	if got := runtimeReadTimeout(idle, grace); got <= budget {
		t.Fatalf("runtime read timeout %v must exceed keepalive budget %v", got, budget)
	}
}

// TestKeepaliveConfigOverride confirms ping_idle_seconds/ping_grace_seconds
// from gobnc.json take priority over the package defaults.
func TestKeepaliveConfigOverride(t *testing.T) {
	l := &Listener{}
	l.SetConfig(config.Config{PingIdleSeconds: 45, PingGraceSeconds: 15})

	if got := l.keepaliveIdle(); got != 45*time.Second {
		t.Fatalf("keepaliveIdle = %v, want 45s", got)
	}
	if got := l.keepaliveGrace(); got != 15*time.Second {
		t.Fatalf("keepaliveGrace = %v, want 15s", got)
	}
}

// TestKeepaliveConfigDefaultsWhenUnset confirms a zero/omitted config value
// falls back to the package defaults rather than disabling keepalive.
func TestKeepaliveConfigDefaultsWhenUnset(t *testing.T) {
	l := &Listener{}
	l.SetConfig(config.Config{})

	if got := l.keepaliveIdle(); got != KeepaliveIdle {
		t.Fatalf("keepaliveIdle = %v, want default %v", got, KeepaliveIdle)
	}
	if got := l.keepaliveGrace(); got != KeepaliveGrace {
		t.Fatalf("keepaliveGrace = %v, want default %v", got, KeepaliveGrace)
	}
}
