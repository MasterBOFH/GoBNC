package session

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/registration"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/internal/testutil"
)

// TestResumeTokenPersistedAndNeverRelayed drives a fake ircd that speaks
// draft/resume-0.5 the way Oragono ≤ 2.5.1 did: offers the cap, ACKs it,
// issues "RESUME TOKEN" during registration, then issues a fresh one
// post-registration (the CAP NEW → REQ → ACK shape, minus the CAP dance).
// Both must land in the store's resume_token column, and neither RESUME
// line may reach a downlink — the early-attached client here sees the
// registration relay (regBuffer) and every post-registration broadcast,
// so it's positioned to catch a leak on either path.
func TestResumeTokenPersistedAndNeverRelayed(t *testing.T) {
	db := testutil.TempStore(t)
	ctx := context.Background()
	if _, err := db.UpsertNetwork(ctx, store.Network{
		Name: "n", Host: "irc.example", Port: 1, Nick: "testnick", Enabled: true,
		Username: "u", Realname: "r",
	}); err != nil {
		t.Fatal(err)
	}
	netCfg, err := db.NetworkByName(ctx, "n")
	if err != nil {
		t.Fatal(err)
	}
	s := New(netCfg, db, nil, nil, nil)
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}

	ln, host, port := newFakeIRCListener(t)
	scriptDone := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			scriptDone <- err
			return
		}
		t.Cleanup(func() { _ = conn.Close() })
		scriptDone <- runResumeTokenServer(conn, time.Now().Add(8*time.Second))
	}()

	tu := newTestUplink(t, s, netCfg, host, port)
	if err := tu.driver.StartRegistration(tu.netID); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 5*time.Second, func() bool { return s.Registered() })
	select {
	case err := <-scriptDone:
		if err != nil {
			t.Fatal("script:", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("script did not finish")
	}

	// The post-registration token supersedes the registration-time one.
	waitUntil(t, 3*time.Second, func() bool {
		tok, _ := db.ResumeToken(ctx, netCfg.ID)
		return tok == "secondtoken"
	})

	for _, m := range d.snapshot() {
		if strings.EqualFold(m.Command, "RESUME") {
			t.Fatalf("RESUME line relayed to a downlink: %+v", m)
		}
	}
	if d.snapshot()[len(d.snapshot())-1].Command == "" {
		t.Fatal("sanity: client got nothing at all")
	}
}

// runResumeTokenServer is the fake ircd for the test above.
func runResumeTokenServer(server net.Conn, deadline time.Time) error {
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
	if err := write("CAP * LS :message-tags " + registration.ResumeCap); err != nil {
		return err
	}
	line, err := read()
	if err != nil || !strings.HasPrefix(line, "CAP REQ") || !strings.Contains(line, registration.ResumeCap) {
		return fmt.Errorf("CAP REQ with %s: %q %v", registration.ResumeCap, line, err)
	}
	if err := write("CAP * ACK :message-tags " + registration.ResumeCap); err != nil {
		return err
	}
	if err := write(":server RESUME TOKEN firsttoken"); err != nil {
		return err
	}
	line, err = read()
	if err != nil || line != "CAP END" {
		return fmt.Errorf("CAP END (no token to present, so no RESUME): %q %v", line, err)
	}
	for _, l := range []string{
		":server 001 testnick :Welcome to the network",
		":server 002 testnick :Your host is server",
		":server 003 testnick :This server was created once",
		":server 004 testnick server ircd iow nt",
		":server 375 testnick :- server Message of the Day -",
		":server 372 testnick :- hello",
		":server 376 testnick :End of /MOTD command.",
		// A fresh token issued after registration (e.g. following a
		// CAP NEW re-negotiation) — must supersede and must not relay.
		":server RESUME TOKEN secondtoken",
		":server NOTICE testnick :marker",
	} {
		if err := write(l); err != nil {
			return err
		}
	}
	return nil
}

func TestGracefulQuitClearsResumeToken(t *testing.T) {
	db := testutil.TempStore(t)
	ctx := context.Background()
	id, err := db.UpsertNetwork(ctx, store.Network{Name: "n", Host: "h", Port: 1, Nick: "me", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetResumeToken(ctx, id, "tok"); err != nil {
		t.Fatal(err)
	}
	netCfg, err := db.NetworkByName(ctx, "n")
	if err != nil {
		t.Fatal(err)
	}
	s := New(netCfg, db, nil, nil, nil) // no driver: nothing to QUIT, but the token still dies
	s.GracefulQuit(ctx, "bye")
	if tok, err := db.ResumeToken(ctx, id); err != nil || tok != "" {
		t.Fatalf("after GracefulQuit: tok=%q err=%v, want cleared", tok, err)
	}
}

// TestResumableFlagFollowsCapACKAndDEL: the keeper's BlobKeyResumable
// (what turns its shutdown QUIT into BRB) is set once the uplink ACKs
// draft/resume-0.5 and removed again if the ircd DELs it.
func TestResumableFlagFollowsCapACKAndDEL(t *testing.T) {
	db := testutil.TempStore(t)
	ctx := context.Background()
	if _, err := db.UpsertNetwork(ctx, store.Network{
		Name: "n", Host: "irc.example", Port: 1, Nick: "testnick", Enabled: true,
		Username: "u", Realname: "r",
	}); err != nil {
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
		scriptDone <- runResumeTokenServer(conn, time.Now().Add(8*time.Second))
	}()
	tu := newTestUplink(t, s, netCfg, host, port)
	if err := tu.driver.StartRegistration(tu.netID); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 5*time.Second, func() bool { return s.Registered() })
	if err := <-scriptDone; err != nil {
		t.Fatal("script:", err)
	}
	k := tu.mgr.All()[tu.netID]
	waitUntil(t, 3*time.Second, k.Resumable)

	// Deliver a DEL the way the demux would — HandleLine is the same
	// entry point; the blob push it triggers goes over the live driver.
	s.HandleLine([]byte(":server CAP testnick DEL :"+registration.ResumeCap), 1<<20)
	waitUntil(t, 3*time.Second, func() bool { return !k.Resumable() })
}
