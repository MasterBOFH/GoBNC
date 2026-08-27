package session

import (
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// TestBouncerSASLFailureHandsSASLToClient is the wire-level proof of
// rule 4 (bouncerOwnsSASLLocked): bouncer SASL is configured and fails
// during registration (904), after which a client must be able to
// authenticate the uplink itself — CAP REQ sasl ACK'd, its AUTHENTICATE
// forwarded upstream through the real Driver, the 900/903 routed back to
// it — with an attached bystander seeing only the 900; and once logged
// in, the offer is withdrawn from both (rule 3). Before this change
// every one of those steps was blocked by a static Network.SASL check.
func TestBouncerSASLFailureHandsSASLToClient(t *testing.T) {
	ln, host, port := newFakeIRCListener(t)

	netCfg := store.Network{
		Name: "test", Host: "pipe", Port: 1, Nick: "testnick",
		Username: "u", Realname: "r", SASL: true, SASLUser: "acct", SASLPass: "wrong",
	}
	s := New(netCfg, nil, nil, nil, nil)

	scriptDone := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			scriptDone <- err
			return
		}
		t.Cleanup(func() { _ = conn.Close() })
		deadline := time.Now().Add(10 * time.Second)
		if err := runBouncerSASLFailureServer(conn, deadline); err != nil {
			scriptDone <- err
			return
		}
		scriptDone <- runClientReauthAfterBouncerFailure(conn, deadline)
	}()
	newTestUplink(t, s, netCfg, host, port)

	waitUntil(t, 5*time.Second, func() bool { return s.Registered() })
	waitUntil(t, 2*time.Second, func() bool { return s.OffersPassthroughSASL() })

	initiator := &fakeDL{id: "init", caps: map[string]bool{"cap-notify": true}}
	other := &fakeDL{id: "other", caps: map[string]bool{"cap-notify": true}}
	for _, d := range []*fakeDL{initiator, other} {
		if err := s.Attach(d); err != nil {
			t.Fatal(err)
		}
		d.clearSent()
	}

	if err := s.RequestClientSASL(initiator); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, func() bool { return initiator.HasCap("sasl") })
	initiator.clearSent()

	if err := s.HandleClientMessage(initiator, irc.Message{Command: "AUTHENTICATE", Params: []string{"PLAIN"}}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, func() bool { return countMsgCmd(initiator.snapshot(), "AUTHENTICATE") > 0 })
	payload := base64.StdEncoding.EncodeToString([]byte("\x00acct\x00right"))
	if err := s.HandleClientMessage(initiator, irc.Message{Command: "AUTHENTICATE", Params: []string{payload}}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, func() bool {
		snap := initiator.snapshot()
		return countMsgCmd(snap, "903") > 0 && len(capNotices(snap, "DEL")) > 0
	})

	select {
	case err := <-scriptDone:
		if err != nil {
			t.Fatal("script:", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("script timeout")
	}

	snap := initiator.snapshot()
	if countMsgCmd(snap, "900") != 1 || countMsgCmd(snap, "903") != 1 {
		t.Fatalf("initiator wants 900+903: %+v", snap)
	}
	if got := capNotices(snap, "DEL"); len(got) != 1 || got[0] != "sasl" {
		t.Fatalf("initiator: offer must be withdrawn after login: %+v", snap)
	}
	waitUntil(t, 2*time.Second, func() bool { return len(capNotices(other.snapshot(), "DEL")) > 0 })
	osnap := other.snapshot()
	assertNoSASLPrivate(t, "other", osnap)
	if countMsgCmd(osnap, "900") != 1 {
		t.Fatalf("other wants exactly the 900: %+v", osnap)
	}
	if s.OffersPassthroughSASL() {
		t.Fatal("nothing offered once logged in")
	}
}

// runClientReauthAfterBouncerFailure continues after
// runBouncerSASLFailureServer left the uplink registered with sasl
// ACK'd but unauthenticated: it expects a client-driven PLAIN exchange
// (no new CAP REQ — sasl is already enabled) and answers 900/903.
func runClientReauthAfterBouncerFailure(server net.Conn, deadline time.Time) error {
	br := newLineBuf(server)
	read := func() (string, error) {
		_ = server.SetReadDeadline(deadline)
		return br.readLine()
	}
	write := func(s string) error {
		_, err := io.WriteString(server, s+"\r\n")
		return err
	}
	for {
		line, err := read()
		if err != nil {
			return fmt.Errorf("waiting for client AUTHENTICATE: %v", err)
		}
		if strings.HasPrefix(line, "PING") {
			_ = write("PONG" + strings.TrimPrefix(line, "PING"))
			continue
		}
		if strings.HasPrefix(line, "CAP REQ") {
			return fmt.Errorf("sasl is already enabled; no CAP REQ expected: %q", line)
		}
		if line != "AUTHENTICATE PLAIN" && line != "AUTHENTICATE :PLAIN" {
			return fmt.Errorf("AUTHENTICATE PLAIN: %q", line)
		}
		break
	}
	if err := write("AUTHENTICATE +"); err != nil {
		return err
	}
	line, err := read()
	if err != nil || !strings.HasPrefix(line, "AUTHENTICATE ") || strings.HasSuffix(line, " +") {
		return fmt.Errorf("AUTHENTICATE payload: %q %v", line, err)
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(strings.TrimPrefix(line, "AUTHENTICATE "), ":"))
	if err != nil || string(raw) != "\x00acct\x00right" {
		return fmt.Errorf("payload must be the client's credentials: %q %v", raw, err)
	}
	if err := write(":server 900 testnick testnick!u@h acct :You are now logged in as acct"); err != nil {
		return err
	}
	return write(":server 903 testnick :SASL authentication successful")
}
