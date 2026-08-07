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
	"github.com/MasterBOFH/GoBNC/internal/uplink"
)

func TestAttachPendingBeforeUplink(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	if len(d.sent) != 1 || d.sent[0].Command != "001" {
		t.Fatalf("want only pending 001, got %+v", d.sent)
	}
	if !strings.Contains(d.sent[0].Trailing(), "pending uplink") {
		t.Fatalf("pending text: %+v", d.sent[0])
	}
	for _, m := range d.sent {
		if m.Command == "002" || m.Command == "376" {
			t.Fatalf("must not send fake registration: %+v", d.sent)
		}
	}
	s.mu.RLock()
	awaiting := s.awaitingUplink[d.id]
	s.mu.RUnlock()
	if !awaiting {
		t.Fatal("should be awaiting uplink")
	}
}

func TestAttachMidRegistrationCatchUp(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	s.OnRegistrationLine(nil, irc.Message{
		Source: "irc.example", Command: "NOTICE", Params: []string{"AUTH", "*** Looking up your hostname"},
	})
	s.OnRegistrationLine(nil, irc.Message{
		Source: "irc.example", Command: "001", Params: []string{"me", "Welcome"},
	})

	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	// pending 001 + buffered NOTICE + buffered 001
	if countMsgCmd(d.snapshot(), "001") != 2 {
		t.Fatalf("want stub+real 001: %+v", d.sent)
	}
	if countMsgCmd(d.snapshot(), "NOTICE") != 1 {
		t.Fatalf("want buffered NOTICE: %+v", d.sent)
	}

	s.OnRegistrationLine(nil, irc.Message{
		Source: "irc.example", Command: "376", Params: []string{"me", "End of MOTD"},
	})
	if countMsgCmd(d.snapshot(), "376") != 1 {
		t.Fatalf("live 376: %+v", d.sent)
	}
}

func TestOnDisconnectERRORClosesClients(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	s.registered = true
	s.channels["#c"] = &ChannelState{Name: "#c", Members: map[string]struct{}{}}
	a := &fakeDL{id: "a", caps: map[string]bool{}}
	b := &fakeDL{id: "b", caps: map[string]bool{}}
	_ = s.Attach(a)
	_ = s.Attach(b)
	a.clearSent()
	b.clearSent()

	s.OnDisconnect(nil, fmt.Errorf("server ERROR: Closing Link: me (Ping timeout)"))

	for _, d := range []*fakeDL{a, b} {
		if len(d.sent) != 1 || d.sent[0].Command != "ERROR" {
			t.Fatalf("%s: %+v", d.id, d.sent)
		}
		if d.sent[0].Trailing() != "Closing Link: me (Ping timeout)" && d.sent[0].Param(0) != "Closing Link: me (Ping timeout)" {
			t.Fatalf("%s ERROR text: %+v", d.id, d.sent[0])
		}
	}
	s.mu.RLock()
	nDown := len(s.downlinks)
	nChan := len(s.channels)
	reg := s.registered
	s.mu.RUnlock()
	if nDown != 0 || nChan != 0 || reg {
		t.Fatalf("downlinks=%d channels=%d registered=%v", nDown, nChan, reg)
	}
}

func TestOnDisconnectGenericReason(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	s.registered = true
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	_ = s.Attach(d)
	d.clearSent()
	s.OnDisconnect(nil, nil)
	if len(d.sent) != 1 || d.sent[0].Command != "ERROR" {
		t.Fatalf("%+v", d.sent)
	}
	got := d.sent[0].Trailing()
	if got == "" {
		got = d.sent[0].Param(0)
	}
	if got != "connection to the server was lost" {
		t.Fatalf("%q", got)
	}
}

func TestEarlyAttachRelaysRegistrationE2E(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

	d := &fakeDL{id: "early", caps: map[string]bool{"cap-notify": true}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	if countMsgCmd(d.snapshot(), "001") != 1 || countMsgCmd(d.snapshot(), "002") != 0 {
		t.Fatalf("pending attach: %+v", d.snapshot())
	}

	scriptDone := make(chan error, 1)
	go func() {
		scriptDone <- runEarlyRegServer(server, time.Now().Add(8*time.Second))
	}()
	runDone := make(chan error, 1)
	go func() { runDone <- u.Run(ctx) }()

	waitUntil(t, 5*time.Second, func() bool {
		return countMsgCmd(d.snapshot(), "NOTICE") >= 1 &&
			countMsgCmd(d.snapshot(), "376") >= 1 &&
			s.registered
	})

	sent := d.snapshot()
	if countMsgCmd(sent, "NOTICE") < 1 {
		t.Fatalf("want NOTICE AUTH relay: %+v", sent)
	}
	// stub + real 001
	if countMsgCmd(sent, "001") < 2 {
		t.Fatalf("want stub and uplink 001: %+v", sent)
	}
	if countMsgCmd(sent, "002") < 1 {
		t.Fatalf("want real 002: %+v", sent)
	}
	assertNoEmptyFakeMOTD(t, sent)

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

func assertNoEmptyFakeMOTD(t *testing.T, msgs []irc.Message) {
	t.Helper()
	for _, m := range msgs {
		if m.Command == "372" && strings.Contains(m.Trailing(), "MOTD can be requested") {
			t.Fatalf("fake MOTD should not appear during live relay: %+v", m)
		}
	}
}

func runEarlyRegServer(server net.Conn, deadline time.Time) error {
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
	if err := write("NOTICE AUTH :*** Looking up your hostname..."); err != nil {
		return err
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
	if err := write(":server 001 testnick :Welcome to the network"); err != nil {
		return err
	}
	if err := write(":server 002 testnick :Your host is server"); err != nil {
		return err
	}
	if err := write(":server 003 testnick :This server was created once"); err != nil {
		return err
	}
	if err := write(":server 004 testnick server ircd iow nt"); err != nil {
		return err
	}
	if err := write(":server 375 testnick :- server Message of the Day -"); err != nil {
		return err
	}
	if err := write(":server 372 testnick :- hello"); err != nil {
		return err
	}
	return write(":server 376 testnick :End of /MOTD command.")
}

func TestOnDisconnectWhileAwaitingUplink(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	d := &fakeDL{id: "early", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	s.OnRegistrationLine(nil, irc.Message{
		Source: "irc.example", Command: "NOTICE", Params: []string{"AUTH", "*** Looking up your hostname"},
	})
	d.clearSent()

	s.OnDisconnect(nil, fmt.Errorf("connection reset by peer"))

	if len(d.sent) != 1 || d.sent[0].Command != "ERROR" {
		t.Fatalf("want ERROR while awaiting: %+v", d.sent)
	}
	got := d.sent[0].Trailing()
	if got == "" {
		got = d.sent[0].Param(0)
	}
	if got != "connection reset by peer" {
		t.Fatalf("ERROR text=%q", got)
	}
	s.mu.RLock()
	nAwait := len(s.awaitingUplink)
	nDown := len(s.downlinks)
	nBuf := len(s.regBuffer)
	s.mu.RUnlock()
	if nAwait != 0 || nDown != 0 || nBuf != 0 {
		t.Fatalf("await=%d down=%d buf=%d", nAwait, nDown, nBuf)
	}
}

func TestTwoConcurrentEarlyAwaitingClients(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil)
	a := &fakeDL{id: "a", caps: map[string]bool{"cap-notify": true}}
	b := &fakeDL{id: "b", caps: map[string]bool{"cap-notify": true}}
	if err := s.Attach(a); err != nil {
		t.Fatal(err)
	}
	if err := s.Attach(b); err != nil {
		t.Fatal(err)
	}
	for _, d := range []*fakeDL{a, b} {
		if countMsgCmd(d.snapshot(), "001") != 1 {
			t.Fatalf("%s pending 001: %+v", d.id, d.sent)
		}
	}
	a.clearSent()
	b.clearSent()

	notice := irc.Message{Source: "irc.example", Command: "NOTICE", Params: []string{"AUTH", "*** Checking ident"}}
	welcome := irc.Message{Source: "irc.example", Command: "001", Params: []string{"me", "Welcome"}}
	motdEnd := irc.Message{Source: "irc.example", Command: "376", Params: []string{"me", "End of MOTD"}}
	s.OnRegistrationLine(nil, notice)
	s.OnRegistrationLine(nil, welcome)
	s.OnRegistrationLine(nil, motdEnd)

	for _, d := range []*fakeDL{a, b} {
		if countMsgCmd(d.snapshot(), "NOTICE") != 1 || countMsgCmd(d.snapshot(), "001") != 1 || countMsgCmd(d.snapshot(), "376") != 1 {
			t.Fatalf("%s missing live relay: %+v", d.id, d.snapshot())
		}
	}

	s.mu.RLock()
	if len(s.awaitingUplink) != 2 {
		t.Fatalf("both should still await until OnRegistered: %d", len(s.awaitingUplink))
	}
	s.mu.RUnlock()

	// Finish the way OnRegistered does for awaiters (without a live uplink).
	s.mu.Lock()
	s.registered = true
	s.awaitingUplink = make(map[ClientID]bool)
	s.regBuffer = nil
	s.mu.Unlock()

	s.mu.RLock()
	nAwait := len(s.awaitingUplink)
	s.mu.RUnlock()
	if nAwait != 0 {
		t.Fatalf("awaiting still set: %d", nAwait)
	}
}

func TestNickInUseRelayedThenDisconnect(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	netCfg := store.Network{Name: "test", Host: "pipe", Port: 1, Nick: "taken"}
	s := New(netCfg, nil, nil, nil)
	u := uplink.New(uplink.Config{
		Network:    netCfg,
		MinBackoff: time.Hour,
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return client, nil
		},
	}, s)
	s.SetUplink(u)

	a := &fakeDL{id: "a", caps: map[string]bool{}}
	b := &fakeDL{id: "b", caps: map[string]bool{}}
	if err := s.Attach(a); err != nil {
		t.Fatal(err)
	}
	if err := s.Attach(b); err != nil {
		t.Fatal(err)
	}
	a.clearSent()
	b.clearSent()

	scriptDone := make(chan error, 1)
	go func() {
		scriptDone <- runNickInUseServer(server, time.Now().Add(6*time.Second))
	}()
	runDone := make(chan error, 1)
	go func() { runDone <- u.Run(ctx) }()

	waitUntil(t, 5*time.Second, func() bool {
		return countMsgCmd(a.snapshot(), "433") >= 1 && countMsgCmd(a.snapshot(), "ERROR") >= 1
	})

	for _, d := range []*fakeDL{a, b} {
		if countMsgCmd(d.snapshot(), "433") < 1 {
			t.Fatalf("%s missing 433: %+v", d.id, d.snapshot())
		}
		if countMsgCmd(d.snapshot(), "ERROR") < 1 {
			t.Fatalf("%s missing ERROR after nick failure: %+v", d.id, d.snapshot())
		}
		// 433 should appear before ERROR.
		if !afterCmd(d.snapshot(), "433", "ERROR") {
			t.Fatalf("%s 433 must precede ERROR: %+v", d.id, d.snapshot())
		}
	}
	s.mu.RLock()
	nDown := len(s.downlinks)
	nAwait := len(s.awaitingUplink)
	s.mu.RUnlock()
	if nDown != 0 || nAwait != 0 {
		t.Fatalf("down=%d await=%d", nDown, nAwait)
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

func runNickInUseServer(server net.Conn, deadline time.Time) error {
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
	if err := write("CAP * LS :cap-notify"); err != nil {
		return err
	}
	line, err := read()
	if err != nil || !strings.Contains(line, "CAP REQ") {
		return fmt.Errorf("CAP REQ: %q %v", line, err)
	}
	if err := write("CAP * ACK :cap-notify"); err != nil {
		return err
	}
	line, err = read()
	if err != nil || line != "CAP END" {
		return fmt.Errorf("CAP END: %q %v", line, err)
	}
	return write(":server 433 * taken :Nickname is already in use.")
}
