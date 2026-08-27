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

// TestCapNewSASLAfterUnauthenticatedRegistrationAuths is the live twin of
// TestCapNewSASLWhileLoggedInDoesNotReauth: services were down when we
// connected (no sasl in CAP LS), so bouncer-owned SASL never happened;
// when the ircd later advertises CAP NEW :sasl=…, the bouncer must REQ
// it, the client must get the "SASL authentication starting" NOTICE,
// and — the part that used to be missing entirely — brain.Driver must
// then run the AUTHENTICATE exchange (registration.StepPost) through to
// a 900/903, after which the session is logged in and the client has
// seen RPL_LOGGEDIN. Full stack: fake ircd ↔ keeper ↔ Driver ↔ Session.
func TestCapNewSASLAfterUnauthenticatedRegistrationAuths(t *testing.T) {
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
		scriptDone <- runCapNewSASLReauthServer(conn, s, time.Now().Add(8*time.Second))
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

	waitUntil(t, 2*time.Second, func() bool {
		s.mu.RLock()
		defer s.mu.RUnlock()
		return s.loggedIn
	})
	sent := d.snapshot()
	var starts, loggedIn int
	for _, m := range sent {
		if m.Command == "NOTICE" && strings.Contains(m.Trailing(), "SASL authentication starting") {
			starts++
		}
		if m.Command == "900" {
			loggedIn++
		}
	}
	if starts != 1 {
		t.Fatalf("want exactly one SASL-starting NOTICE (for the post-registration auth), got %d: %+v", starts, sent)
	}
	if loggedIn != 1 {
		t.Fatalf("want RPL_LOGGEDIN once, got %d: %+v", loggedIn, sent)
	}
}

// runCapNewSASLReauthServer registers the bouncer with no sasl on offer,
// then advertises it via CAP NEW and expects the whole re-auth: CAP REQ
// sasl → ACK → AUTHENTICATE PLAIN → + → payload → 900/903.
func runCapNewSASLReauthServer(server net.Conn, s *Session, deadline time.Time) error {
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
	if err != nil || !strings.Contains(line, "CAP REQ") || strings.Contains(line, "sasl") {
		return fmt.Errorf("CAP REQ without sasl: %q %v", line, err)
	}
	if err := write("CAP * ACK :message-tags cap-notify"); err != nil {
		return err
	}
	line, err = read()
	if err != nil || line != "CAP END" {
		return fmt.Errorf("CAP END: %q %v", line, err)
	}
	if err := write(":server 001 testnick :Welcome"); err != nil {
		return err
	}
	if err := write(":server 376 testnick :End of MOTD"); err != nil {
		return err
	}
	waitUntilPoll(2*time.Second, func() bool { return s.Registered() })

	// Services back: sasl appears post-registration.
	if err := write("CAP * NEW :sasl=PLAIN"); err != nil {
		return err
	}
	line, err = read()
	if err != nil || !strings.HasPrefix(line, "CAP REQ") || !strings.Contains(line, "sasl") {
		return fmt.Errorf("bouncer must REQ sasl after CAP NEW: %q %v", line, err)
	}
	if err := write("CAP * ACK :sasl"); err != nil {
		return err
	}
	line, err = read()
	if err != nil || (line != "AUTHENTICATE PLAIN" && line != "AUTHENTICATE :PLAIN") {
		return fmt.Errorf("post-registration AUTHENTICATE PLAIN: %q %v", line, err)
	}
	if err := write("AUTHENTICATE +"); err != nil {
		return err
	}
	line, err = read()
	if err != nil || !strings.HasPrefix(line, "AUTHENTICATE ") || strings.HasSuffix(line, " +") {
		return fmt.Errorf("AUTHENTICATE payload: %q %v", line, err)
	}
	if err := write(":server 900 testnick testnick!u@h acct :You are now logged in as acct"); err != nil {
		return err
	}
	return write(":server 903 testnick :SASL authentication successful")
}
