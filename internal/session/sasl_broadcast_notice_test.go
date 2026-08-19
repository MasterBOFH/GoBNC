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

// TestBouncerSASLNoticesReachAttachedClient is the live proof for the
// bouncer-initiated-SASL broadcast: a client attached before the uplink
// finishes registering (the common case for the bouncer's own SASL
// attempt, which always happens pre-001) sees a "starting" NOTICE the
// moment CAP ACKs sasl, and a failure NOTICE carrying the ircd's own
// reason text when the attempt fails with a non-required 904 — while
// never seeing the raw 904 itself (that stays uplink-only, matching
// TestMultiDownstreamBouncerSASL's assertNoSASLPrivate contract).
// Registration is not Required here, so it must still complete normally
// after the failure (mirrors registration.TestSASLNotRequiredNoMechanism
// ContinuesUnauthenticated's non-fatal expectation).
func TestBouncerSASLNoticesReachAttachedClient(t *testing.T) {
	ln, host, port := newFakeIRCListener(t)

	netCfg := store.Network{
		Name: "test", Host: "pipe", Port: 1, Nick: "testnick",
		Username: "u", Realname: "r", SASL: true, SASLUser: "acct", SASLPass: "secret",
	}
	s := New(netCfg, nil, nil, nil, nil)

	// Attach before the uplink is registered — awaitingUplink path — so
	// this client is live for the whole SASL exchange, not just the
	// post-registration burst.
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
		scriptDone <- runBouncerSASLFailureServer(conn, time.Now().Add(8*time.Second))
	}()
	newTestUplink(t, s, netCfg, host, port)

	waitUntil(t, 5*time.Second, func() bool { return s.Registered() })
	select {
	case err := <-scriptDone:
		if err != nil {
			t.Fatal("script:", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("script timeout")
	}

	sent := d.snapshot()
	var sawStart, sawFail bool
	for _, m := range sent {
		if m.Command == "904" {
			t.Fatalf("client must never see raw 904: %+v", sent)
		}
		if m.Command != "NOTICE" {
			continue
		}
		if strings.Contains(m.Trailing(), "SASL authentication starting") {
			sawStart = true
		}
		if strings.Contains(m.Trailing(), "SASL authentication failed") {
			sawFail = true
		}
	}
	if !sawStart {
		t.Fatalf("missing SASL-starting NOTICE: %+v", sent)
	}
	if !sawFail {
		t.Fatalf("missing SASL-failed NOTICE: %+v", sent)
	}
}

// runBouncerSASLFailureServer is runBouncerSASLServer's failure twin: same
// CAP LS/REQ/ACK and AUTHENTICATE PLAIN handshake, but answers with 904
// instead of 900/903, then expects the bouncer to send CAP END anyway
// (SASL not Required — see registration.TestSASLNotRequiredNoMechanism
// ContinuesUnauthenticated) and completes registration normally.
func runBouncerSASLFailureServer(server net.Conn, deadline time.Time) error {
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
	if err := write("CAP * LS :sasl=PLAIN message-tags cap-notify"); err != nil {
		return err
	}
	line, err := read()
	if err != nil || !strings.Contains(line, "CAP REQ") || !strings.Contains(line, "sasl") {
		return fmt.Errorf("bouncer must REQ sasl: %q %v", line, err)
	}
	if err := write("CAP * ACK :sasl message-tags cap-notify"); err != nil {
		return err
	}
	line, err = read()
	if err != nil || (line != "AUTHENTICATE PLAIN" && line != "AUTHENTICATE :PLAIN") {
		return fmt.Errorf("AUTHENTICATE PLAIN: %q %v", line, err)
	}
	if err := write("AUTHENTICATE +"); err != nil {
		return err
	}
	line, err = read()
	if err != nil || !strings.HasPrefix(line, "AUTHENTICATE ") || strings.HasSuffix(line, " +") {
		return fmt.Errorf("AUTHENTICATE payload: %q %v", line, err)
	}
	if err := write(":server 904 testnick :SASL authentication failed"); err != nil {
		return err
	}
	line, err = read()
	if err != nil || line != "CAP END" {
		return fmt.Errorf("CAP END: %q %v", line, err)
	}
	if err := write(":server 001 testnick :Welcome"); err != nil {
		return err
	}
	return write(":server 376 testnick :End of MOTD")
}
