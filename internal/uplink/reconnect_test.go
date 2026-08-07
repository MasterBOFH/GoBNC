package uplink

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/store"
)

func TestForceReconnectSkipsBackoff(t *testing.T) {
	var dials atomic.Int32
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_, _ = io.Copy(io.Discard, c)
			}(c)
		}
	}()

	_, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatal(err)
	}

	u := New(Config{
		Network: store.Network{
			Name: "t", Host: "127.0.0.1", Port: port, Nick: "n", TLS: false,
		},
		MinBackoff: 30 * time.Second,
		MaxBackoff: 30 * time.Second,
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dials.Add(1)
			var d net.Dialer
			return d.DialContext(ctx, network, addr)
		},
	}, &regHandler{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		_ = u.Run(ctx)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for dials.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if dials.Load() < 1 {
		t.Fatal("first dial never happened")
	}

	u.ForceReconnect()

	deadline = time.Now().Add(2 * time.Second)
	for dials.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if dials.Load() < 2 {
		t.Fatalf("expected immediate second dial after ForceReconnect, got %d", dials.Load())
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit")
	}
}
