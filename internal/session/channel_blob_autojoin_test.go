package session

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/internal/testutil"
)

// TestAutoJoinRepopulatesChannelBlob is the regression test for the bug
// where a self-JOIN that was not preceded by a client-issued JOIN command
// (auto-join at connect, uplink-driven rejoin after a netsplit, invite
// auto-join) never pushed a "channel:#foo" blob entry — because
// persistChannel/pushBlob only ran behind the pendingJoinKeys check meant to
// guard the *SQL* row, and the blob has no equivalent fallback of its own
// (it's cleared unconditionally on every disconnect). The observable
// symptom: after a brain restart resumes from the keeper's blob, a newly
// attaching client gets no JOIN/332/353/366 burst for channels it was
// actually on, because Session.SeedFromBlob had nothing to seed them from.
//
// This drives a real fake ircd through the full keeper/brain wire (same
// harness as reg_relay_test.go's E2E tests) so the assertion below reads
// the blob back from the real keeper.Manager, not a mock.
func TestAutoJoinRepopulatesChannelBlob(t *testing.T) {
	db := testutil.TempStore(t)
	ctx := context.Background()
	id, err := db.UpsertNetwork(ctx, store.Network{Name: "n", Host: "h", Port: 1, Nick: "me", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	// The channel is already known to SQL with a real key, as if a client
	// had JOIN'd it in some earlier session — exactly the "uplink auto-rejoin
	// already has a DB row" case the pendingJoinKeys guard was written for.
	if err := db.AddChannel(ctx, id, "#auto", "topsecret"); err != nil {
		t.Fatal(err)
	}
	netCfg, err := db.NetworkByName(ctx, "n")
	if err != nil {
		t.Fatal(err)
	}

	s := New(netCfg, db, nil, nil, nil)

	ln, host, port := newFakeIRCListener(t)
	scriptDone := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			scriptDone <- err
			return
		}
		t.Cleanup(func() { _ = conn.Close() })
		scriptDone <- runAutoJoinRegServer(conn, time.Now().Add(8*time.Second))
	}()

	tu := newTestUplink(t, s, netCfg, host, port)

	waitUntil(t, 5*time.Second, func() bool {
		return s.Registered()
	})
	waitUntil(t, 5*time.Second, func() bool {
		s.mu.RLock()
		_, joined := s.channels[s.isupport.CaseMapping.Canonical("#auto")]
		s.mu.RUnlock()
		return joined
	})

	select {
	case err := <-scriptDone:
		if err != nil {
			t.Fatal("script:", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("script timeout")
	}

	// The whole point: no client ever sent JOIN #auto, so pendingJoinKeys
	// never had an entry for it — this must not mean the blob stays empty.
	waitUntil(t, 2*time.Second, func() bool {
		v, ok := tu.blobValue("channel:#auto")
		return ok && v == "topsecret"
	})

	// The SQL row must be untouched (still the real key, never clobbered
	// with an empty one) — the guard this test's fix must not break.
	chs, err := db.ListChannels(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range chs {
		if c.Name == "#auto" {
			found = true
			if c.Key != "topsecret" {
				t.Fatalf("SQL key clobbered: %q", c.Key)
			}
		}
	}
	if !found {
		t.Fatal("expected #auto to remain in SQL store")
	}
}

// runAutoJoinRegServer completes registration like reg_relay_test.go's
// runEarlyRegServer, then sends an unsolicited self-JOIN for #auto — the
// uplink's own auto-join echo, not a reply to any client-issued JOIN.
func runAutoJoinRegServer(server net.Conn, deadline time.Time) error {
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
	if err := write("CAP * LS :message-tags cap-notify"); err != nil {
		return err
	}
	line, err := read()
	if err != nil || !strings.Contains(line, "CAP REQ") {
		return fmt.Errorf("CAP REQ: %q %v", line, err)
	}
	if err := write("CAP * ACK :message-tags cap-notify"); err != nil {
		return err
	}
	line, err = read()
	if err != nil || line != "CAP END" {
		return fmt.Errorf("CAP END: %q %v", line, err)
	}
	for _, l := range []string{
		":server 001 me :Welcome to the network",
		":server 002 me :Your host is server",
		":server 003 me :This server was created once",
		":server 004 me server ircd iow nt",
		":server 375 me :- server Message of the Day -",
		":server 372 me :- hello",
		":server 376 me :End of /MOTD command.",
		// Unsolicited self-JOIN — nothing a client asked for.
		":me!u@h JOIN #auto",
	} {
		if err := write(l); err != nil {
			return err
		}
	}
	return nil
}
