package uplink

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/connio"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/internal/version"
)

func TestGracefulQuitSendsQUIT(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	u := New(Config{Network: store.Network{Nick: "n"}}, nil)
	u.mu.Lock()
	u.conn = connio.New(client)
	u.mu.Unlock()

	done := make(chan string, 1)
	go func() {
		br := bufio.NewReader(server)
		_ = server.SetReadDeadline(time.Now().Add(2 * time.Second))
		line, err := br.ReadString('\n')
		if err != nil {
			done <- err.Error()
			return
		}
		done <- strings.TrimRight(line, "\r\n")
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	u.GracefulQuit(ctx, "") // default version quit message

	got := <-done
	want := "QUIT :" + version.QuitMessage()
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestGracefulQuitDrainsFloodThenQUIT(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	u := New(Config{Network: store.Network{Nick: "n", FloodBurst: 1000, FloodRate: 100000}}, nil)
	u.mu.Lock()
	u.conn = connio.New(client)
	u.mu.Unlock()
	u.setFloodParams(1000, 100000)

	ctxDrain, cancelDrain := context.WithCancel(context.Background())
	defer cancelDrain()
	u.startFloodDrain(ctxDrain)
	defer u.stopFloodDrain()

	_ = u.WriteRaw("PRIVMSG #c :one")
	_ = u.WriteRaw("PRIVMSG #c :two")

	lines := make(chan string, 8)
	go func() {
		br := bufio.NewReader(server)
		for {
			_ = server.SetReadDeadline(time.Now().Add(3 * time.Second))
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			lines <- strings.TrimRight(line, "\r\n")
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	u.GracefulQuit(ctx, "shutdown")

	var got []string
	deadline := time.After(2 * time.Second)
collect:
	for len(got) < 3 {
		select {
		case line := <-lines:
			got = append(got, line)
			if strings.HasPrefix(line, "QUIT") {
				break collect
			}
		case <-deadline:
			break collect
		}
	}
	if len(got) < 3 || got[len(got)-1] != "QUIT :shutdown" {
		t.Fatalf("expected PRIVMSGs then QUIT, got %#v", got)
	}
}

func TestGracefulQuitRespectsTimeout(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	// No reader on server → write may block; deadline must unblock.

	u := New(Config{Network: store.Network{Nick: "n"}}, nil)
	u.mu.Lock()
	u.conn = connio.New(client)
	u.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	u.GracefulQuit(ctx, "x")
	if time.Since(start) > time.Second {
		t.Fatal("GracefulQuit hung past timeout")
	}
}
