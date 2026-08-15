package session

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// TestWHOISRoutingThroughRealKeeperBrainWireMultiDownlink is
// TestRequestTrackerWHOISByNick's missing counterpart: that test (and the
// rest of session_test.go's RequestTracker.RouteMessage suite) proves the
// routing decision in isolation — a bare RequestTracker fed hand-built
// irc.Message values, no Session, no Downlink, no keeper, no brain, no
// wire. It has never been true that this logic actually behaves correctly
// once driven through the real architecture: three real downlinks each
// issuing their own WHOIS, a real fake ircd answering over a real TCP
// connection, the reply crossing the real keeper<->brain unix-socket wire
// (newTestUplink's full Manager+Listener+AttachClient+Driver stack, the
// same one internal/server's demux uses in production) before
// Session.HandleLine ever sees it. This drives exactly that, with the
// three targets' replies deliberately interleaved on the wire (not sent
// one client's burst at a time) to stress demux under the same
// interleaving order the routing tests already exercise in isolation —
// and confirms no downlink ever sees a reply meant for another one.
func TestWHOISRoutingThroughRealKeeperBrainWireMultiDownlink(t *testing.T) {
	ln, host, port := newFakeIRCListener(t)

	netCfg := store.Network{Name: "test", Host: "pipe", Port: 1, Nick: "me", Username: "u", Realname: "r"}
	s := New(netCfg, nil, nil, nil, nil)

	whoisSeen := make(chan string, 8)
	scriptDone := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			scriptDone <- err
			return
		}
		t.Cleanup(func() { _ = conn.Close() })
		scriptDone <- runWHOISRoutingServer(conn, time.Now().Add(10*time.Second), whoisSeen)
	}()
	newTestUplink(t, s, netCfg, host, port)

	waitUntil(t, 5*time.Second, s.Registered)

	da := &fakeDL{id: "da", caps: map[string]bool{}}
	db := &fakeDL{id: "db", caps: map[string]bool{}}
	dc := &fakeDL{id: "dc", caps: map[string]bool{}}
	for _, d := range []*fakeDL{da, db, dc} {
		if err := s.Attach(d); err != nil {
			t.Fatal(err)
		}
		d.clearSent()
	}

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		_ = s.HandleClientMessage(da, irc.Message{Command: "WHOIS", Params: []string{"alice"}})
	}()
	go func() {
		defer wg.Done()
		_ = s.HandleClientMessage(db, irc.Message{Command: "WHOIS", Params: []string{"bob"}})
	}()
	go func() {
		defer wg.Done()
		_ = s.HandleClientMessage(dc, irc.Message{Command: "WHOIS", Params: []string{"carol"}})
	}()
	wg.Wait()

	waitUntil(t, 5*time.Second, func() bool {
		return countCmd(da, "318") >= 1 && countCmd(db, "318") >= 1 && countCmd(dc, "318") >= 1
	})

	assertOnlyWhoisFor(t, da, "alice")
	assertOnlyWhoisFor(t, db, "bob")
	assertOnlyWhoisFor(t, dc, "carol")

	select {
	case err := <-scriptDone:
		if err != nil {
			t.Fatal("script:", err)
		}
	case <-time.After(2 * time.Second):
	}
}

func assertOnlyWhoisFor(t *testing.T, d *fakeDL, want string) {
	t.Helper()
	for _, m := range d.snapshot() {
		if !irc.IsWHOISReply(m.Command, "") || len(m.Params) < 2 {
			continue
		}
		if target := m.Param(1); target != want {
			t.Fatalf("downlink %s got a WHOIS reply for %q, want only %q: %+v", d.id, target, want, d.snapshot())
		}
	}
}

// runWHOISRoutingServer completes minimal registration, then waits for
// three WHOIS requests (one per nick) and answers all three with their
// numerics deliberately interleaved on the wire — round-robin across
// targets rather than one client's full burst at a time — the ordering
// most likely to expose a demux bug that a serial reply order would hide.
func runWHOISRoutingServer(server net.Conn, deadline time.Time, seen chan<- string) error {
	br := newLineBuf(server)
	read := func() (string, error) {
		_ = server.SetReadDeadline(deadline)
		return br.readLine()
	}
	write := func(s string) error {
		_, err := io.WriteString(server, s+"\r\n")
		return err
	}
	for _, want := range []string{"CAP LS", "NICK", "USER"} {
		line, err := read()
		if err != nil || !strings.Contains(line, want) {
			return fmt.Errorf("%s: %q %v", want, line, err)
		}
	}
	if err := write("CAP * LS :"); err != nil {
		return err
	}
	line, err := read()
	if err != nil || line != "CAP END" {
		return fmt.Errorf("CAP END: %q %v", line, err)
	}
	for _, l := range []string{
		":server 001 me :Welcome",
		":server 376 me :End of /MOTD command.",
	} {
		if err := write(l); err != nil {
			return err
		}
	}

	want := map[string]bool{"alice": true, "bob": true, "carol": true}
	for len(want) > 0 {
		_ = server.SetReadDeadline(deadline)
		line, err := br.readLine()
		if err != nil {
			return fmt.Errorf("reading WHOIS: %w", err)
		}
		if !strings.HasPrefix(line, "WHOIS ") {
			continue
		}
		nick := strings.TrimPrefix(strings.TrimSpace(strings.TrimPrefix(line, "WHOIS ")), ":")
		if !want[nick] {
			return fmt.Errorf("unexpected WHOIS target %q", nick)
		}
		delete(want, nick)
		select {
		case seen <- nick:
		default:
		}
	}

	targets := []string{"alice", "bob", "carol"}
	for _, tmpl := range []string{
		":server 311 me %s user host * :Real Name",
		":server 312 me %s irc.example :Some Server",
		":server 318 me %s :End of /WHOIS list.",
	} {
		for _, nick := range targets {
			if err := write(fmt.Sprintf(tmpl, nick)); err != nil {
				return err
			}
		}
	}

	return nil
}
