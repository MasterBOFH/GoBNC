package uplink

import (
	"bufio"
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/connio"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

func TestFloodQueueAndPONGBypass(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	u := New(Config{
		Network: store.Network{
			Nick: "n", FloodBurst: 20, FloodRate: 20, // 20 B/s
		},
	}, nil)
	u.mu.Lock()
	u.conn = connio.New(client, 0)
	u.mu.Unlock()
	u.setFloodParams(20, 20)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	u.startFloodDrain(ctx)
	defer u.stopFloodDrain()

	lines := make(chan string, 8)
	go func() {
		defer close(lines)
		br := bufio.NewReader(server)
		for {
			_ = server.SetReadDeadline(time.Now().Add(5 * time.Second))
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			lines <- strings.TrimRight(line, "\r\n")
		}
	}()

	line1 := "PRIVMSG #c :a" // 14+2=16 bytes
	line2 := "PRIVMSG #c :b"
	if err := u.WriteRaw(line1); err != nil {
		t.Fatal(err)
	}
	if err := u.WriteRaw(line2); err != nil {
		t.Fatal(err)
	}

	got := <-lines
	if got != line1 {
		t.Fatalf("first=%q", got)
	}

	// While line2 waits for tokens, PONG must still go out ahead of it.
	if err := u.writeImmediate("PONG :xyz"); err != nil {
		t.Fatal(err)
	}
	got = <-lines
	if got != "PONG :xyz" {
		t.Fatalf("pong=%q (expected before paced line2)", got)
	}

	start := time.Now()
	got = <-lines
	if got != line2 {
		t.Fatalf("second=%q", got)
	}
	if time.Since(start) < 200*time.Millisecond {
		t.Fatalf("line2 arrived too fast; flood not applied")
	}
}

func TestFloodDisabledImmediate(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	u := New(Config{Network: store.Network{Nick: "n"}}, nil)
	u.mu.Lock()
	u.conn = connio.New(client, 0)
	u.mu.Unlock()

	done := make(chan string, 1)
	go func() {
		br := bufio.NewReader(server)
		line, _ := br.ReadString('\n')
		done <- strings.TrimRight(line, "\r\n")
	}()
	if err := u.WriteRaw("PRIVMSG #c :fast"); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-done:
		if got != "PRIVMSG #c :fast" {
			t.Fatal(got)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestFloodNeverDropsQueued(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	u := New(Config{
		Network: store.Network{Nick: "n", FloodBurst: 50, FloodRate: 500},
	}, nil)
	u.mu.Lock()
	u.conn = connio.New(client, 0)
	u.mu.Unlock()
	u.setFloodParams(50, 500)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	u.startFloodDrain(ctx)
	defer u.stopFloodDrain()

	const n = 20
	errc := make(chan error, 1)
	go func() {
		br := bufio.NewReader(server)
		for i := 0; i < n; i++ {
			_ = server.SetReadDeadline(time.Now().Add(5 * time.Second))
			line, err := br.ReadString('\n')
			if err != nil {
				errc <- err
				return
			}
			if !strings.Contains(line, "PRIVMSG") {
				errc <- io.ErrUnexpectedEOF
				return
			}
		}
		errc <- nil
	}()

	for i := 0; i < n; i++ {
		if err := u.WriteRaw("PRIVMSG #c :x"); err != nil {
			t.Fatal(err)
		}
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestFloodQueueCap(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	u := New(Config{
		Network:       store.Network{Nick: "n", FloodBurst: 100, FloodRate: 100},
		MaxFloodQueue: 2,
	}, nil)
	u.mu.Lock()
	u.conn = connio.New(client, 0)
	u.mu.Unlock()
	u.setFloodParams(100, 100)
	// Do not start drain — queue should fill and stay full.

	if err := u.enqueueFlood("a"); err != nil {
		t.Fatal(err)
	}
	if err := u.enqueueFlood("b"); err != nil {
		t.Fatal(err)
	}
	err := u.enqueueFlood("c")
	if err == nil || !strings.Contains(err.Error(), "flood queue full") {
		t.Fatalf("want queue full, got %v", err)
	}

	u.cfg.MaxFloodQueue = 0
	if err := u.enqueueFlood("d"); err != nil {
		t.Fatal(err)
	}
	_ = server
}
