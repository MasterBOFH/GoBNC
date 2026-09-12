package session

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/internal/testutil"
)

// A client attached while the uplink is still registering — a fresh
// connect, or a client held across a resume, which has no idea it's in
// that window — must have every uplink-bound command it sends queued
// until registration completes, then delivered in order. Before this,
// only enquiries were held: a PRIVMSG or JOIN went straight to the
// unregistered uplink, bounced with 451, and was lost (seen live during
// a held resume).
func TestClientCommandsHeldUntilUplinkRegistered(t *testing.T) {
	db := testutil.TempStore(t)
	ctx := context.Background()
	if _, err := db.UpsertNetwork(ctx, store.Network{Name: "n", Host: "h", Port: 1, Nick: "me", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	netCfg, err := db.NetworkByName(ctx, "n")
	if err != nil {
		t.Fatal(err)
	}
	s := New(netCfg, db, nil, nil, nil)

	ln, host, port := newFakeIRCListener(t)
	capEndSeen := make(chan struct{})
	release := make(chan struct{})
	after := make(chan []string, 1)
	scriptDone := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			scriptDone <- err
			return
		}
		t.Cleanup(func() { _ = conn.Close() })
		scriptDone <- runHoldRegServer(conn, capEndSeen, release, after)
	}()

	newTestUplink(t, s, netCfg, host, port)
	<-capEndSeen

	// Uplink is mid-registration. A client sends what a user would.
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{"PRIVMSG #chan :hello", "JOIN #late", "PRIVMSG bob :hello again"} {
		msg, _ := irc.Parse(line)
		if err := s.HandleClientMessage(d, msg); err != nil {
			t.Fatalf("HandleClientMessage(%q): %v", line, err)
		}
	}
	close(release)

	select {
	case err := <-scriptDone:
		if err != nil {
			t.Fatal("script:", err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("script timeout")
	}
	got := <-after
	want := []string{"PRIVMSG #chan :hello", "JOIN #late", "PRIVMSG bob :hello again"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("after registration uplink got %q, want %q in order", got, want)
	}
}

// runHoldRegServer drives registration up to CAP END, signals, then holds
// there until released — the window under test. Any line arriving in that
// window is a failure (it reached an unregistered uplink). After 001..376
// it reads three lines and reports them back.
func runHoldRegServer(server net.Conn, capEndSeen chan<- struct{}, release <-chan struct{}, after chan<- []string) error {
	br := newLineBuf(server)
	read := func(d time.Duration) (string, error) {
		_ = server.SetReadDeadline(time.Now().Add(d))
		return br.readLine()
	}
	write := func(s string) error {
		_, err := io.WriteString(server, s+"\r\n")
		return err
	}
	for _, want := range []string{"CAP LS", "NICK", "USER"} {
		line, err := read(5 * time.Second)
		if err != nil || !strings.Contains(line, want) {
			return fmt.Errorf("%s: %q %v", want, line, err)
		}
	}
	if err := write("CAP * LS :message-tags cap-notify"); err != nil {
		return err
	}
	if line, err := read(5 * time.Second); err != nil || !strings.Contains(line, "CAP REQ") {
		return fmt.Errorf("CAP REQ: %q %v", line, err)
	}
	if err := write("CAP * ACK :message-tags cap-notify"); err != nil {
		return err
	}
	if line, err := read(5 * time.Second); err != nil || line != "CAP END" {
		return fmt.Errorf("CAP END: %q %v", line, err)
	}
	close(capEndSeen)
	<-release
	// The window: the client has sent its commands; none may be here.
	if line, err := read(400 * time.Millisecond); err == nil {
		return fmt.Errorf("sent to the unregistered uplink: %q", line)
	} else if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
		return fmt.Errorf("read in window: %v", err)
	}
	for _, l := range []string{
		":server 001 me :Welcome",
		":server 376 me :End of /MOTD command.",
	} {
		if err := write(l); err != nil {
			return err
		}
	}
	var got []string
	for i := 0; i < 3; i++ {
		line, err := read(5 * time.Second)
		if err != nil {
			return fmt.Errorf("after registration, line %d: %v (got %q so far)", i+1, err, got)
		}
		got = append(got, line)
	}
	after <- got
	return nil
}
