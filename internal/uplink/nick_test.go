package uplink

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

func TestBuildNickLadder(t *testing.T) {
	got := buildNickLadder("alice", "bob", 9)
	if len(got) < 5 {
		t.Fatalf("ladder too short: %v", got)
	}
	if got[0] != "alice" || got[1] != "bob" || got[2] != "alice_" {
		t.Fatalf("got %v", got)
	}
	for _, n := range got {
		if len(n) > 9 {
			t.Fatalf("overlong %q", n)
		}
	}
	got2 := buildNickLadder("me", "", 30)
	if got2[0] != "me" || got2[1] != "me_" {
		t.Fatalf("%v", got2)
	}
}

func TestNextNickInLadder(t *testing.T) {
	cm := irc.CaseRFC1459
	ladder := buildNickLadder("nick", "alt", 30)
	if next := nextNickInLadder(ladder, "nick", "nick", cm); next != "alt" {
		t.Fatalf("got %q", next)
	}
	if next := nextNickInLadder(ladder, "alt", "alt", cm); next != "nick_" {
		t.Fatalf("got %q", next)
	}
	last := ladder[len(ladder)-1]
	if next := nextNickInLadder(ladder, last, last, cm); next != "" {
		t.Fatalf("expected exhausted, got %q", next)
	}
}

func TestIsonTargets(t *testing.T) {
	cm := irc.CaseRFC1459
	n := store.Network{Nick: "prim", AltNick: "alt", NickRecovery: true}
	if got := isonTargets("alt", n, cm); len(got) != 1 || got[0] != "prim" {
		t.Fatalf("on alt: %v", got)
	}
	if got := isonTargets("prim_", n, cm); len(got) != 2 || got[0] != "prim" || got[1] != "alt" {
		t.Fatalf("on underscore: %v", got)
	}
	if got := isonTargets("prim", n, cm); len(got) != 0 {
		t.Fatalf("on primary: %v", got)
	}
}

func TestNickLadderDuringRegister(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	netCfg := store.Network{
		Name: "test", Host: "pipe", Port: 1, Nick: "taken", AltNick: "taken2", NickRecovery: true,
	}
	u := New(Config{
		Network:    netCfg,
		MinBackoff: time.Hour,
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return client, nil
		},
	}, nil)

	scriptDone := make(chan error, 1)
	go func() {
		scriptDone <- runNickLadderServer(server, time.Now().Add(6*time.Second))
	}()
	runDone := make(chan error, 1)
	go func() { runDone <- u.session(ctx) }()

	waitUntil(t, 5*time.Second, func() bool { return u.Registered() })
	if got := u.Nick(); got != "taken_" {
		t.Fatalf("nick=%q want taken_", got)
	}

	cancel()
	<-runDone
	if err := <-scriptDone; err != nil {
		t.Fatal(err)
	}
}

func runNickLadderServer(server net.Conn, deadline time.Time) error {
	br := bufio.NewReader(server)
	read := func() (string, error) {
		_ = server.SetReadDeadline(deadline)
		line, err := br.ReadString('\n')
		return strings.TrimRight(line, "\r\n"), err
	}
	write := func(s string) error {
		_, err := io.WriteString(server, s+"\r\n")
		return err
	}
	for _, want := range []string{"CAP LS", "NICK taken", "USER"} {
		line, err := read()
		if err != nil {
			return fmt.Errorf("%s: %w", want, err)
		}
		if want == "NICK taken" {
			if line != "NICK taken" {
				return fmt.Errorf("want NICK taken, got %q", line)
			}
		} else if !strings.Contains(line, want) {
			return fmt.Errorf("want %s, got %q", want, line)
		}
	}
	_ = write(":server 433 * taken :Nickname is already in use.")
	line, err := read()
	if err != nil || line != "NICK taken2" {
		return fmt.Errorf("alt nick: %q %v", line, err)
	}
	_ = write(":server 433 * taken2 :Nickname is already in use.")
	line, err = read()
	if err != nil || line != "NICK taken_" {
		return fmt.Errorf("underscore nick: %q %v", line, err)
	}
	_ = write(":server 001 taken_ :Welcome")
	_ = write(":server 376 taken_ :End of /MOTD command.")
	// Nick recovery may send ISON immediately; drain so WriteRaw does not block.
	_ = server.SetReadDeadline(time.Now().Add(2 * time.Second))
	if line, err := read(); err == nil && strings.HasPrefix(line, "ISON ") {
		_ = write(":server 303 taken_ :taken taken2")
	}
	return nil
}

func waitUntil(t *testing.T, d time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timeout waiting")
}
