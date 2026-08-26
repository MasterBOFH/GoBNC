package session

import (
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/store"
)

// TestCapNewSASLWhileLoggedInDoesNotReauth: an ircd that re-advertises
// sasl after registration (typically CAP DEL :sasl / CAP NEW :sasl=… as
// services restart) must not make an already-authenticated bouncer-owned
// SASL session start over. Nothing can complete a post-registration SASL
// exchange anyway — registration.Step is terminal past PhaseComplete and
// nothing else ever sends AUTHENTICATE — so before this fix the CAP NEW
// re-REQ'd sasl, the ACK broadcast a second "SASL authentication
// starting" NOTICE, and then nothing happened. The client must see
// exactly one "starting" NOTICE (the real one, during registration) and
// the server must see no sasl CAP REQ after 001.
func TestCapNewSASLWhileLoggedInDoesNotReauth(t *testing.T) {
	ln, host, port := newFakeIRCListener(t)

	netCfg := store.Network{
		Name: "test", Host: "pipe", Port: 1, Nick: "testnick",
		Username: "u", Realname: "r", SASL: true, SASLUser: "acct", SASLPass: "secret",
	}
	s := New(netCfg, nil, nil, nil, nil)

	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}

	scriptDone := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			scriptDone <- err
			return
		}
		t.Cleanup(func() { _ = conn.Close() })
		deadline := time.Now().Add(8 * time.Second)
		if err := runBouncerSASLServer(conn, deadline); err != nil {
			scriptDone <- err
			return
		}
		waitUntilPoll(2*time.Second, func() bool { return s.Registered() })
		scriptDone <- runCapNewSASLAfterRegistration(conn, deadline)
	}()
	newTestUplink(t, s, netCfg, host, port)

	select {
	case err := <-scriptDone:
		if err != nil {
			t.Fatal("script:", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("script timeout")
	}

	s.mu.RLock()
	loggedIn := s.loggedIn
	s.mu.RUnlock()
	if !loggedIn {
		t.Fatal("session must still be logged in after the DEL/NEW round-trip")
	}
	sent := d.snapshot()
	var starts int
	for _, m := range sent {
		if m.Command == "NOTICE" && strings.Contains(m.Trailing(), "SASL authentication starting") {
			starts++
		}
	}
	if starts != 1 {
		t.Fatalf("want exactly one SASL-starting NOTICE (the real one, during registration), got %d: %+v", starts, sent)
	}
}

// runCapNewSASLAfterRegistration plays the services-restart sequence on an
// already-registered, already-logged-in uplink: CAP DEL :sasl, then CAP
// NEW :sasl=PLAIN. It then reads until a short quiet period and fails on
// any sasl CAP REQ or AUTHENTICATE from the bouncer — that would be the
// start of a re-auth that can never finish. A CAP REQ for other caps is
// tolerated (none are advertised here, so none is expected), and a REQ
// for sasl that the script answers with ACK is exactly the pre-fix
// symptom: it triggers the spurious second "starting" NOTICE.
func runCapNewSASLAfterRegistration(server net.Conn, deadline time.Time) error {
	br := newLineBuf(server)
	write := func(s string) error {
		_, err := io.WriteString(server, s+"\r\n")
		return err
	}
	if err := write("CAP * DEL :sasl"); err != nil {
		return err
	}
	if err := write("CAP * NEW :sasl=PLAIN"); err != nil {
		return err
	}
	quiet := time.Now().Add(700 * time.Millisecond)
	for {
		if quiet.After(deadline) {
			quiet = deadline
		}
		_ = server.SetReadDeadline(quiet)
		line, err := br.readLine()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				return nil
			}
			return err
		}
		switch {
		case strings.HasPrefix(line, "PING"):
			_ = write("PONG" + strings.TrimPrefix(line, "PING"))
		case strings.HasPrefix(line, "AUTHENTICATE"):
			return fmt.Errorf("bouncer started a post-registration re-auth: %q", line)
		case strings.HasPrefix(line, "CAP REQ") && strings.Contains(line, "sasl"):
			// Answer like a real ircd would, so the pre-fix path produces
			// its spurious NOTICE and the test fails on that too.
			_ = write("CAP * ACK :sasl")
			return fmt.Errorf("bouncer re-requested sasl while already logged in: %q", line)
		}
	}
}
