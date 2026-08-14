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

func TestSASLPassthroughE2E(t *testing.T) {
	ln, host, port := newFakeIRCListener(t)

	netCfg := store.Network{Name: "test", Host: "pipe", Port: 1, Nick: "testnick"}
	s := New(netCfg, nil, nil, nil, nil)

	scriptDone := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			scriptDone <- err
			return
		}
		t.Cleanup(func() { _ = conn.Close() })
		scriptDone <- runPassthroughServer(conn, time.Now().Add(6*time.Second))
	}()
	newTestUplink(t, s, netCfg, host, port)

	waitUntil(t, 3*time.Second, func() bool { return s.Registered() && s.OffersPassthroughSASL() })

	d := &fakeDL{id: "c1", caps: map[string]bool{"cap-notify": true}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	if !findCapSub(d.snapshot(), "NEW", "sasl") {
		t.Fatalf("attach CAP NEW missing sasl: %+v", capMsgs(d.snapshot()))
	}
	d.clearSent()

	if err := s.RequestClientSASL(d); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, func() bool { return d.HasCap("sasl") })
	if !findCapSub(d.snapshot(), "ACK", "sasl") {
		t.Fatalf("want CAP ACK sasl: %+v", capMsgs(d.snapshot()))
	}
	d.clearSent()

	if err := s.HandleClientMessage(d, irc.Message{Command: "AUTHENTICATE", Params: []string{"PLAIN"}}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, func() bool {
		sent := d.snapshot()
		return len(sent) > 0 && sent[len(sent)-1].Command == "AUTHENTICATE"
	})
	sent := d.snapshot()
	if got := sent[len(sent)-1].Param(0); got != "+" {
		t.Fatalf("want AUTHENTICATE +, got %q in %+v", got, sent)
	}
	d.clearSent()

	payload := base64.StdEncoding.EncodeToString([]byte("\x00user\x00pass"))
	if err := s.HandleClientMessage(d, irc.Message{Command: "AUTHENTICATE", Params: []string{payload}}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, func() bool {
		for _, m := range d.snapshot() {
			if m.Command == "903" {
				return true
			}
		}
		return false
	})

	select {
	case err := <-scriptDone:
		if err != nil {
			t.Fatal("script:", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("script timeout")
	}
}

func runPassthroughServer(server net.Conn, deadline time.Time) error {
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
	if err := write("CAP * LS :sasl=PLAIN,EXTERNAL message-tags cap-notify"); err != nil {
		return err
	}
	line, err := read()
	if err != nil || !strings.Contains(line, "CAP REQ") {
		return fmt.Errorf("CAP REQ: %q %v", line, err)
	}
	if strings.Contains(line, "sasl") {
		return fmt.Errorf("registration REQ must not include sasl: %q", line)
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

	line, err = read()
	if err != nil || !strings.Contains(line, "CAP REQ") || !strings.Contains(line, "sasl") {
		return fmt.Errorf("client-driven CAP REQ sasl: %q %v", line, err)
	}
	if err := write("CAP * ACK :sasl"); err != nil {
		return err
	}

	line, err = read()
	if err != nil || !(line == "AUTHENTICATE PLAIN" || line == "AUTHENTICATE :PLAIN") {
		return fmt.Errorf("AUTHENTICATE PLAIN: %q %v", line, err)
	}
	if err := write("AUTHENTICATE +"); err != nil {
		return err
	}
	line, err = read()
	if err != nil || !strings.HasPrefix(line, "AUTHENTICATE ") || line == "AUTHENTICATE +" || line == "AUTHENTICATE :+" {
		return fmt.Errorf("AUTHENTICATE payload: %q %v", line, err)
	}
	return write(":server 903 testnick :SASL authentication successful")
}

func TestSASLCAPNewDelPassthrough(t *testing.T) {
	ln, host, port := newFakeIRCListener(t)

	netCfg := store.Network{Name: "test", Host: "pipe", Port: 1, Nick: "testnick"}
	s := New(netCfg, nil, nil, nil, nil)

	d := &fakeDL{id: "c1", caps: map[string]bool{"cap-notify": true}}
	s.mu.Lock()
	s.downlinks[d.id] = d
	s.mu.Unlock()

	scriptDone := make(chan error, 1)
	go func() {
		server, err := ln.Accept()
		if err != nil {
			scriptDone <- err
			return
		}
		t.Cleanup(func() { _ = server.Close() })
		br := newLineBuf(server)
		deadline := time.Now().Add(6 * time.Second)
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
				scriptDone <- fmt.Errorf("%s: %q %v", want, line, err)
				return
			}
		}
		if err := write("CAP * LS :message-tags cap-notify"); err != nil {
			scriptDone <- err
			return
		}
		line, err := read()
		if err != nil || !strings.Contains(line, "CAP REQ") {
			scriptDone <- fmt.Errorf("CAP REQ: %q %v", line, err)
			return
		}
		if err := write("CAP * ACK :message-tags cap-notify"); err != nil {
			scriptDone <- err
			return
		}
		line, err = read()
		if err != nil || line != "CAP END" {
			scriptDone <- fmt.Errorf("CAP END: %q %v", line, err)
			return
		}
		_ = write(":server 001 testnick :Welcome")
		_ = write(":server 376 testnick :End of MOTD")

		waitUntilPoll(2*time.Second, func() bool { return s.Registered() })
		if err := write("CAP * NEW :sasl=PLAIN,SCRAM-SHA-256"); err != nil {
			scriptDone <- err
			return
		}
		waitUntilPoll(2*time.Second, func() bool { return s.OffersPassthroughSASL() })
		if err := write("CAP * DEL :sasl"); err != nil {
			scriptDone <- err
			return
		}
		waitUntilPoll(2*time.Second, func() bool { return !s.OffersPassthroughSASL() })
		scriptDone <- nil
	}()

	newTestUplink(t, s, netCfg, host, port)

	select {
	case err := <-scriptDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(7 * time.Second):
		t.Fatal("timeout")
	}

	sent := d.snapshot()
	if !findCapSub(sent, "NEW", "sasl") {
		t.Fatalf("expected CAP NEW sasl: %+v", capMsgs(sent))
	}
	if !findCapSub(sent, "DEL", "sasl") {
		t.Fatalf("expected CAP DEL sasl: %+v", capMsgs(sent))
	}
}

func TestSASLMultiClientRouting(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	a := &fakeDL{id: "a", caps: map[string]bool{"sasl": true}}
	b := &fakeDL{id: "b", caps: map[string]bool{"sasl": true}}
	s.mu.Lock()
	s.downlinks[a.id] = a
	s.downlinks[b.id] = b
	s.saslOffer = "sasl=PLAIN"
	s.saslClient = a.id
	s.mu.Unlock()

	s.routeSASLTraffic(irc.Message{Command: "AUTHENTICATE", Params: []string{"+"}})
	if len(a.sent) != 1 || len(b.sent) != 0 {
		t.Fatalf("a=%+v b=%+v", a.sent, b.sent)
	}
	a.clearSent()
	b.clearSent()
	s.routeSASLTraffic(irc.Message{
		Command: "900",
		Params:  []string{"me", "me!u@h", "acct", "You are now logged in as acct"},
	})
	if len(a.sent) != 1 || a.sent[0].Command != "900" {
		t.Fatalf("initiator should get 900: %+v", a.sent)
	}
	if len(b.sent) != 1 || b.sent[0].Command != "900" {
		t.Fatalf("other client should get 900 only: %+v", b.sent)
	}
	a.clearSent()
	b.clearSent()
	s.routeSASLTraffic(irc.Message{Command: "903", Params: []string{"me", "ok"}})
	if len(a.sent) != 1 || a.sent[0].Command != "903" {
		t.Fatalf("initiator should get 903: %+v", a.sent)
	}
	if len(b.sent) != 0 {
		t.Fatalf("b should not get 903: %+v", b.sent)
	}
	s.mu.RLock()
	cleared := s.saslClient == ""
	s.mu.RUnlock()
	if !cleared {
		t.Fatal("saslClient not cleared")
	}
}

func TestSASLNoFallbackWithoutClient(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	a := &fakeDL{id: "a", caps: map[string]bool{"sasl": true}}
	s.mu.Lock()
	s.downlinks[a.id] = a
	s.saslOffer = "sasl=PLAIN"
	s.saslClient = "" // no initiator
	s.mu.Unlock()

	s.routeSASLTraffic(irc.Message{Command: "AUTHENTICATE", Params: []string{"+"}})
	s.routeSASLTraffic(irc.Message{Command: "903", Params: []string{"me", "ok"}})
	if len(a.sent) != 0 {
		t.Fatalf("must not leak SASL without saslClient: %+v", a.sent)
	}
}

func TestBouncerOwnedSASLOnlyEmitsLoggedIn(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me", SASL: true, SASLUser: "u", SASLPass: "p"}, nil, nil, nil, nil)
	a := &fakeDL{id: "a", caps: map[string]bool{}}
	b := &fakeDL{id: "b", caps: map[string]bool{}}
	s.mu.Lock()
	s.downlinks[a.id] = a
	s.downlinks[b.id] = b
	s.mu.Unlock()

	s.routeSASLTraffic(irc.Message{Command: "AUTHENTICATE", Params: []string{"+"}})
	s.routeSASLTraffic(irc.Message{Command: "903", Params: []string{"me", "ok"}})
	if len(a.sent) != 0 || len(b.sent) != 0 {
		t.Fatalf("AUTHENTICATE/903 must not reach clients: a=%+v b=%+v", a.sent, b.sent)
	}

	s.routeSASLTraffic(irc.Message{
		Command: "900",
		Params:  []string{"me", "me!u@h", "acct", "You are now logged in as acct"},
	})
	if len(a.sent) != 1 || a.sent[0].Command != "900" {
		t.Fatalf("want 900 to a: %+v", a.sent)
	}
	if len(b.sent) != 1 || b.sent[0].Command != "900" {
		t.Fatalf("want 900 to b: %+v", b.sent)
	}
	if s.self.Account != "acct" {
		t.Fatalf("account=%q", s.self.Account)
	}
}

func TestAttachReplaysLoggedIn(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me", Username: "u"}, nil, nil, nil, nil)
	s.mu.Lock()
	s.registered = true
	s.self.Account = "acct"
	s.self.Host = "h"
	s.loggedIn = true
	s.mu.Unlock()
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	var saw376, saw900 bool
	for _, m := range d.sent {
		switch m.Command {
		case "376":
			saw376 = true
			if saw900 {
				t.Fatal("900 must come after registration (376)")
			}
		case "900":
			if !saw376 {
				t.Fatal("900 before 376")
			}
			saw900 = true
			if m.Param(2) != "acct" {
				t.Fatalf("%+v", m)
			}
		}
	}
	if !saw900 {
		t.Fatalf("want 900 after 376: %+v", d.sent)
	}
}

func TestAttachOmitsLoggedInAfterLogout(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	s.registered = true
	s.routeSASLTraffic(irc.Message{
		Command: "900",
		Params:  []string{"me", "me!u@h", "acct", "You are now logged in as acct"},
	})
	s.routeSASLTraffic(irc.Message{
		Command: "901",
		Params:  []string{"me", "me!u@h", "You are now logged out"},
	})
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	for _, m := range d.sent {
		if m.Command == "900" {
			t.Fatal("must not replay 900 after RPL_LOGGEDOUT")
		}
	}
}

func TestSASLCapNAK(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	s.mu.Lock()
	s.saslOffer = "sasl=PLAIN"
	s.downlinks[d.id] = d
	s.saslWaiters = []ClientID{d.id}
	s.saslReqPending = true
	s.mu.Unlock()

	s.OnCapNAK([]string{"sasl"})
	if len(d.sent) != 1 || d.sent[0].Param(1) != "NAK" {
		t.Fatalf("want NAK: %+v", d.sent)
	}
	if d.HasCap("sasl") {
		t.Fatal("should not enable sasl on NAK")
	}
}

type lineBuf struct {
	c   net.Conn
	buf []byte
}

func newLineBuf(c net.Conn) *lineBuf {
	return &lineBuf{c: c, buf: make([]byte, 0, 4096)}
}

func (l *lineBuf) readLine() (string, error) {
	for {
		if i := indexByte(l.buf, '\n'); i >= 0 {
			line := string(l.buf[:i])
			l.buf = l.buf[i+1:]
			return strings.TrimRight(line, "\r"), nil
		}
		tmp := make([]byte, 1024)
		n, err := l.c.Read(tmp)
		if n > 0 {
			l.buf = append(l.buf, tmp[:n]...)
		}
		if err != nil {
			return "", err
		}
	}
}

func indexByte(b []byte, c byte) int {
	for i, x := range b {
		if x == c {
			return i
		}
	}
	return -1
}

func waitUntil(t *testing.T, d time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

func waitUntilPoll(d time.Duration, ok func() bool) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func findCapSub(msgs []irc.Message, sub, name string) bool {
	for _, m := range msgs {
		if m.Command != "CAP" {
			continue
		}
		if !strings.EqualFold(m.Param(1), sub) {
			continue
		}
		trail := m.Trailing()
		if trail == "" && len(m.Params) > 2 {
			trail = m.Params[2]
		}
		if strings.Contains(trail, name) {
			return true
		}
	}
	return false
}

func capMsgs(msgs []irc.Message) []string {
	var out []string
	for _, m := range msgs {
		if m.Command == "CAP" {
			out = append(out, m.Encode())
		}
	}
	return out
}
