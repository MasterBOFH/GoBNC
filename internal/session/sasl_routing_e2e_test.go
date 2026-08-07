package session

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
	"github.com/MasterBOFH/GoBNC/internal/uplink"
)

// saslCmds are AUTHENTICATE and SASL outcome/error numerics that must not leak
// to clients that did not initiate the exchange (or to any client when the
// bouncer owns SASL).
var saslPrivateCmds = map[string]bool{
	"AUTHENTICATE": true,
	"903":          true,
	"904":          true,
	"905":          true,
	"906":          true,
	"907":          true,
	"908":          true,
}

func countMsgCmd(msgs []irc.Message, cmd string) int {
	n := 0
	for _, m := range msgs {
		if m.Command == cmd {
			n++
		}
	}
	return n
}

func assertNoSASLPrivate(t *testing.T, label string, msgs []irc.Message) {
	t.Helper()
	for _, m := range msgs {
		if saslPrivateCmds[m.Command] {
			t.Fatalf("%s must not receive %s: %+v", label, m.Command, msgs)
		}
	}
}

func waitRegistered(t *testing.T, u *uplink.Uplink, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if u.Registered() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("uplink not registered")
}

// TestMultiDownstreamBouncerSASL: bouncer owns credentials; two attached clients
// see only RPL_LOGGEDIN — never AUTHENTICATE or outcome/error numerics.
func TestMultiDownstreamBouncerSASL(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	netCfg := store.Network{
		Name: "test", Host: "pipe", Port: 1, Nick: "testnick",
		Username: "u", Realname: "r", SASLUser: "acct", SASLPass: "secret",
	}
	s := New(netCfg, nil, nil, nil)
	u := uplink.New(uplink.Config{
		Network:    netCfg,
		MinBackoff: time.Hour,
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return client, nil
		},
	}, s)
	s.SetUplink(u)

	scriptDone := make(chan error, 1)
	go func() {
		scriptDone <- runBouncerSASLServer(server, time.Now().Add(8*time.Second))
	}()
	runDone := make(chan error, 1)
	go func() { runDone <- u.Run(ctx) }()

	waitRegistered(t, u, 5*time.Second)
	waitUntil(t, 2*time.Second, func() bool {
		s.mu.RLock()
		defer s.mu.RUnlock()
		return s.loggedIn && s.self != nil && s.self.Account == "acct"
	})

	a := &fakeDL{id: "a", caps: map[string]bool{}}
	b := &fakeDL{id: "b", caps: map[string]bool{}}
	if err := s.Attach(a); err != nil {
		t.Fatal(err)
	}
	if err := s.Attach(b); err != nil {
		t.Fatal(err)
	}

	for _, d := range []*fakeDL{a, b} {
		assertNoSASLPrivate(t, string(d.id)+" attach", d.snapshot())
		if countMsgCmd(d.snapshot(), "900") != 1 {
			t.Fatalf("%s: want exactly one 900 after registration, got %+v", d.id, d.snapshot())
		}
		if !afterCmd(d.snapshot(), "376", "900") {
			t.Fatalf("%s: 900 must follow 376", d.id)
		}
	}

	// Live post-registration SASL chatter from the uplink must not leak either.
	a.clearSent()
	b.clearSent()
	s.OnMessage(u, irc.Message{Command: "AUTHENTICATE", Params: []string{"+"}})
	s.OnMessage(u, irc.Message{Command: "904", Params: []string{"testnick", "SASL authentication failed"}})
	s.OnMessage(u, irc.Message{Command: "903", Params: []string{"testnick", "ok"}})
	assertNoSASLPrivate(t, "a live", a.snapshot())
	assertNoSASLPrivate(t, "b live", b.snapshot())

	// A third client attaching later still gets RPL_LOGGEDIN only.
	c := &fakeDL{id: "c", caps: map[string]bool{}}
	if err := s.Attach(c); err != nil {
		t.Fatal(err)
	}
	assertNoSASLPrivate(t, "c attach", c.snapshot())
	if countMsgCmd(c.snapshot(), "900") != 1 || !afterCmd(c.snapshot(), "376", "900") {
		t.Fatalf("late attach missing post-reg 900: %+v", c.snapshot())
	}

	select {
	case err := <-scriptDone:
		if err != nil {
			t.Fatal("script:", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("script timeout")
	}
	cancel()
	<-runDone
}

func runBouncerSASLServer(server net.Conn, deadline time.Time) error {
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
	if err := write(":server 900 testnick testnick!u@h acct :You are now logged in as acct"); err != nil {
		return err
	}
	if err := write(":server 903 testnick :SASL authentication successful"); err != nil {
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

// TestMultiDownstreamClientSASL: one client drives AUTHENTICATE; the other only
// sees RPL_LOGGEDIN; the bouncer never starts its own SASL negotiation.
func TestMultiDownstreamClientSASL(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()

	netCfg := store.Network{Name: "test", Host: "pipe", Port: 1, Nick: "testnick"}
	s := New(netCfg, nil, nil, nil)
	u := uplink.New(uplink.Config{
		Network:    netCfg,
		MinBackoff: time.Hour,
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return client, nil
		},
	}, s)
	s.SetUplink(u)

	scriptDone := make(chan error, 1)
	go func() {
		scriptDone <- runClientSASLMultiServer(server, time.Now().Add(10*time.Second))
	}()
	runDone := make(chan error, 1)
	go func() { runDone <- u.Run(ctx) }()

	waitRegistered(t, u, 5*time.Second)
	waitUntil(t, 2*time.Second, func() bool { return s.OffersPassthroughSASL() })
	if u.OwnsSASL() {
		t.Fatal("bouncer must not own SASL without credentials")
	}

	initiator := &fakeDL{id: "init", caps: map[string]bool{"cap-notify": true}}
	other := &fakeDL{id: "other", caps: map[string]bool{"cap-notify": true}}
	if err := s.Attach(initiator); err != nil {
		t.Fatal(err)
	}
	if err := s.Attach(other); err != nil {
		t.Fatal(err)
	}
	initiator.clearSent()
	other.clearSent()

	if err := s.RequestClientSASL(initiator); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, func() bool { return initiator.HasCap("sasl") })
	initiator.clearSent()
	other.clearSent()

	if err := s.HandleClientMessage(initiator, irc.Message{Command: "AUTHENTICATE", Params: []string{"PLAIN"}}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, func() bool {
		return countMsgCmd(initiator.snapshot(), "AUTHENTICATE") > 0
	})
	assertNoSASLPrivate(t, "other during challenge", other.snapshot())
	if countMsgCmd(other.snapshot(), "900") != 0 {
		t.Fatalf("other premature 900: %+v", other.snapshot())
	}

	payload := base64.StdEncoding.EncodeToString([]byte("\x00user\x00pass"))
	if err := s.HandleClientMessage(initiator, irc.Message{Command: "AUTHENTICATE", Params: []string{payload}}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, 3*time.Second, func() bool {
		return countMsgCmd(initiator.snapshot(), "903") > 0 && countMsgCmd(initiator.snapshot(), "900") > 0
	})
	waitUntil(t, 2*time.Second, func() bool {
		return countMsgCmd(other.snapshot(), "900") > 0
	})

	assertNoSASLPrivate(t, "other after success", other.snapshot())
	if countMsgCmd(other.snapshot(), "900") != 1 {
		t.Fatalf("other should see only 900 among SASL: %+v", other.snapshot())
	}
	if countMsgCmd(initiator.snapshot(), "AUTHENTICATE") < 1 {
		t.Fatalf("initiator missing AUTHENTICATE: %+v", initiator.snapshot())
	}
	if countMsgCmd(initiator.snapshot(), "900") != 1 || countMsgCmd(initiator.snapshot(), "903") != 1 {
		t.Fatalf("initiator want 900+903: %+v", initiator.snapshot())
	}

	// Late attach after successful client SASL.
	late := &fakeDL{id: "late", caps: map[string]bool{}}
	if err := s.Attach(late); err != nil {
		t.Fatal(err)
	}
	assertNoSASLPrivate(t, "late", late.snapshot())
	if countMsgCmd(late.snapshot(), "900") != 1 || !afterCmd(late.snapshot(), "376", "900") {
		t.Fatalf("late attach want post-reg 900: %+v", late.snapshot())
	}

	select {
	case err := <-scriptDone:
		if err != nil {
			t.Fatal("script:", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("script timeout")
	}
	cancel()
	<-runDone
}

func runClientSASLMultiServer(server net.Conn, deadline time.Time) error {
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
	if err != nil || !strings.Contains(line, "CAP REQ") {
		return fmt.Errorf("CAP REQ: %q %v", line, err)
	}
	if strings.Contains(line, "sasl") {
		return fmt.Errorf("bouncer must not REQ sasl without creds: %q", line)
	}
	if err := write("CAP * ACK :message-tags cap-notify"); err != nil {
		return err
	}
	line, err = read()
	if err != nil || line != "CAP END" {
		return fmt.Errorf("CAP END: %q %v", line, err)
	}
	// No AUTHENTICATE from bouncer before welcome.
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
		return fmt.Errorf("client AUTHENTICATE PLAIN: %q %v", line, err)
	}
	if err := write("AUTHENTICATE +"); err != nil {
		return err
	}
	line, err = read()
	if err != nil || !strings.HasPrefix(line, "AUTHENTICATE ") || line == "AUTHENTICATE +" || line == "AUTHENTICATE :+" {
		return fmt.Errorf("AUTHENTICATE payload: %q %v", line, err)
	}
	if err := write(":server 900 testnick testnick!u@h acct :You are now logged in as acct"); err != nil {
		return err
	}
	return write(":server 903 testnick :SASL authentication successful")
}

func afterCmd(msgs []irc.Message, before, after string) bool {
	sawBefore := false
	for _, m := range msgs {
		if m.Command == before {
			sawBefore = true
		}
		if m.Command == after {
			return sawBefore
		}
	}
	return false
}
