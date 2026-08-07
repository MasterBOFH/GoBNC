package uplink

import (
	"bufio"
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/connio"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

func TestUplinkKeepalivePING(t *testing.T) {
	oldIdle, oldGrace := KeepaliveIdle, KeepaliveGrace
	KeepaliveIdle = 80 * time.Millisecond
	KeepaliveGrace = time.Hour
	t.Cleanup(func() {
		KeepaliveIdle, KeepaliveGrace = oldIdle, oldGrace
	})

	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	u := New(Config{Network: store.Network{Nick: "n"}}, nil)
	u.mu.Lock()
	u.conn = connio.New(client, 0)
	u.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	atomic.StoreInt64(&u.lastRXUnix, time.Now().Add(-200*time.Millisecond).UnixNano())
	go u.keepaliveLoop(ctx)

	br := bufio.NewReader(server)
	_ = server.SetReadDeadline(time.Now().Add(3 * time.Second))
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(line, "PING") {
		t.Fatalf("expected keepalive PING, got %q", line)
	}
}

func TestUplinkKeepaliveClearedByTraffic(t *testing.T) {
	oldIdle, oldGrace := KeepaliveIdle, KeepaliveGrace
	KeepaliveIdle = 100 * time.Millisecond
	KeepaliveGrace = 50 * time.Millisecond
	t.Cleanup(func() {
		KeepaliveIdle, KeepaliveGrace = oldIdle, oldGrace
	})

	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	u := New(Config{Network: store.Network{Nick: "n"}}, nil)
	u.mu.Lock()
	u.conn = connio.New(client, 0)
	u.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	u.noteRX()
	go u.keepaliveLoop(ctx)

	// Keep feeding RX so no PING should be sent.
	done := make(chan struct{})
	go func() {
		defer close(done)
		br := bufio.NewReader(server)
		_ = server.SetReadDeadline(time.Now().Add(400 * time.Millisecond))
		_, err := br.ReadString('\n')
		if err == nil {
			t.Error("unexpected PING while traffic flowing")
		}
	}()
	deadline := time.Now().Add(350 * time.Millisecond)
	for time.Now().Before(deadline) {
		u.noteRX()
		time.Sleep(20 * time.Millisecond)
	}
	<-done
}
