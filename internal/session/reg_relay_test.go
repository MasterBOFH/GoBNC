package session

import (
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

func TestAttachPendingBeforeUplink(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	if len(d.sent) != 1 || d.sent[0].Command != "001" {
		t.Fatalf("want only pending 001, got %+v", d.sent)
	}
	if d.sent[0].Param(0) != "me" {
		t.Fatalf("pending 001 nick=%q want configured nick", d.sent[0].Param(0))
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

func TestHoldSolicitousUntilRegistered(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	msg := irc.Message{Command: "USERHOST", Params: []string{"me"}}
	if err := s.HandleClientMessage(d, msg); err != nil {
		t.Fatal(err)
	}
	if err := s.HandleClientMessage(d, msg); err != nil {
		t.Fatal(err)
	}
	s.mu.RLock()
	n := len(s.heldUntilReg)
	s.mu.RUnlock()
	if n != 1 {
		t.Fatalf("identical holds should coalesce, got %d", n)
	}
	if _, ok := s.tracker.ActiveClient(); ok {
		t.Fatal("must not begin solicitous while unregistered")
	}

	// Local commands still work.
	d.clearSent()
	if err := s.HandleClientMessage(d, irc.Message{Command: "PING", Params: []string{"x"}}); err != nil {
		t.Fatal(err)
	}
	if len(d.sent) != 1 || d.sent[0].Command != "PONG" {
		t.Fatalf("PING: %+v", d.sent)
	}

	s.Detach(d.id)
	s.mu.RLock()
	n = len(s.heldUntilReg)
	s.mu.RUnlock()
	if n != 0 {
		t.Fatalf("detach should drop held: %d", n)
	}
}

func TestSkipDuplicateUSERHOSTAfterFlush(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	msg := irc.Message{Command: "USERHOST", Params: []string{"me"}}
	if err := s.HandleClientMessage(d, msg); err != nil {
		t.Fatal(err)
	}

	// Simulate register + flush without a live uplink write path by marking sent.
	s.mu.Lock()
	s.registered = true
	s.heldUntilReg = nil
	s.heldFlushSent = map[ClientID]map[string]time.Time{
		d.id: {heldFingerprint(msg): time.Now()},
	}
	s.mu.Unlock()

	if err := s.HandleClientMessage(d, msg); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.tracker.ActiveClient(); ok {
		t.Fatal("post-001 repeat should be dropped, not forwarded")
	}
	s.mu.RLock()
	_, still := s.heldFlushSent[d.id]
	s.mu.RUnlock()
	if still {
		t.Fatal("dedup marker should be consumed")
	}

	// A later intentional USERHOST must still go through (no marker). A real
	// (if never-connected) Driver is enough — forwardSolicitous only needs
	// to reach WriteMessage, not have it succeed. Port 1 is a real address
	// nothing listens on, so the dial itself fails fast without needing a
	// fake server at all.
	newTestUplink(t, s, store.Network{Name: "n", Nick: "me"}, "127.0.0.1", 1)
	_ = s.HandleClientMessage(d, msg)
	if _, ok := s.tracker.ActiveClient(); !ok {
		t.Fatal("second USERHOST after dedup window should forward")
	}
}

func TestAttachMidRegistrationCatchUp(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	s.HandleRegistrationLine(irc.Message{
		Source: "irc.example", Command: "NOTICE", Params: []string{"AUTH", "*** Looking up your hostname"},
	})
	s.HandleRegistrationLine(irc.Message{
		Source: "irc.example", Command: "001", Params: []string{"me", "Welcome"},
	})

	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	// Buffer already has the real 001 — do not prepend a stub with the wrong nick.
	if countMsgCmd(d.snapshot(), "001") != 1 {
		t.Fatalf("want only buffered 001: %+v", d.sent)
	}
	if d.snapshot()[1].Command != "001" || d.snapshot()[1].Param(0) != "me" {
		t.Fatalf("buffered 001: %+v", d.sent)
	}
	if countMsgCmd(d.snapshot(), "NOTICE") != 1 {
		t.Fatalf("want buffered NOTICE: %+v", d.sent)
	}

	s.HandleRegistrationLine(irc.Message{
		Source: "irc.example", Command: "376", Params: []string{"me", "End of MOTD"},
	})
	if countMsgCmd(d.snapshot(), "376") != 1 {
		t.Fatalf("live 376: %+v", d.sent)
	}
}

func TestOnDisconnectERRORClosesClients(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	s.registered = true
	s.channels["#c"] = &ChannelState{Name: "#c", Members: map[string]struct{}{}}
	a := &fakeDL{id: "a", caps: map[string]bool{}}
	b := &fakeDL{id: "b", caps: map[string]bool{}}
	_ = s.Attach(a)
	_ = s.Attach(b)
	a.clearSent()
	b.clearSent()

	s.HandleDisconnect(fmt.Errorf("server ERROR: Closing Link: me (Ping timeout)"))

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
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	s.registered = true
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	_ = s.Attach(d)
	d.clearSent()
	s.HandleDisconnect(nil)
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
	ln, host, port := newFakeIRCListener(t)

	netCfg := store.Network{Name: "test", Host: "pipe", Port: 1, Nick: "testnick"}
	s := New(netCfg, nil, nil, nil, nil)

	d := &fakeDL{id: "early", caps: map[string]bool{"cap-notify": true}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	if countMsgCmd(d.snapshot(), "001") != 1 || countMsgCmd(d.snapshot(), "002") != 0 {
		t.Fatalf("pending attach: %+v", d.snapshot())
	}

	scriptDone := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			scriptDone <- err
			return
		}
		t.Cleanup(func() { _ = conn.Close() })
		scriptDone <- runEarlyRegServer(conn, time.Now().Add(8*time.Second))
	}()
	newTestUplink(t, s, netCfg, host, port)

	waitUntil(t, 5*time.Second, func() bool {
		return countMsgCmd(d.snapshot(), "NOTICE") >= 1 &&
			countMsgCmd(d.snapshot(), "376") >= 1 &&
			s.Registered()
	})

	sent := d.snapshot()
	if countMsgCmd(sent, "NOTICE") < 1 {
		t.Fatalf("want NOTICE AUTH relay: %+v", sent)
	}
	if got := welcomeNicks(sent); len(got) < 2 || got[0] != "testnick" || got[1] != "testnick" {
		t.Fatalf("want stub 001 then uplink 001, both testnick, got %v in %+v", got, sent)
	}
	assertNoSelfNICK(t, sent)
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

// TestAwaitingClientGetsCapNewOnceWhenUplinkRegisters covers the "client
// attached while the bouncer was still connecting to the upstream" case: the
// client's own CAP LS only had AlwaysOffer (session/uplink not known yet), so
// once the uplink registers with an uplink-backed cap the client must be told
// via CAP NEW — but exactly once. completeRegistration both broadcasts a
// NEW/DEL diff to all downlinks and separately syncs each newly-unblocked
// awaiting client via notifyAttachCaps; both paths could otherwise announce
// the same cap.
func TestAwaitingClientGetsCapNewOnceWhenUplinkRegisters(t *testing.T) {
	ln, host, port := newFakeIRCListener(t)

	netCfg := store.Network{Name: "test", Host: "pipe", Port: 1, Nick: "testnick"}
	s := New(netCfg, nil, nil, nil, nil)

	d := &fakeDL{id: "early", caps: map[string]bool{"cap-notify": true}}
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
		scriptDone <- runAwayNotifyRegServer(conn, time.Now().Add(8*time.Second))
	}()
	newTestUplink(t, s, netCfg, host, port)

	waitUntil(t, 5*time.Second, func() bool { return s.Registered() })
	// Give any async CAP NEW sends from completeRegistration a moment to land.
	time.Sleep(50 * time.Millisecond)

	sent := d.snapshot()
	var newCount int
	for _, m := range sent {
		if m.Command == "CAP" && m.Param(1) == "NEW" && strings.Contains(m.Trailing(), "away-notify") {
			newCount++
		}
	}
	if newCount != 1 {
		t.Fatalf("want exactly one CAP NEW away-notify, got %d: %+v", newCount, sent)
	}

	select {
	case err := <-scriptDone:
		if err != nil {
			t.Fatal("script:", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("script timeout")
	}
}

func runAwayNotifyRegServer(server net.Conn, deadline time.Time) error {
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
	if err := write("CAP * LS :message-tags cap-notify away-notify"); err != nil {
		return err
	}
	line, err := read()
	if err != nil || !strings.Contains(line, "CAP REQ") {
		return fmt.Errorf("CAP REQ: %q %v", line, err)
	}
	if !strings.Contains(line, "away-notify") {
		return fmt.Errorf("uplink should have requested away-notify: %q", line)
	}
	if err := write("CAP * ACK :message-tags cap-notify away-notify"); err != nil {
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
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	d := &fakeDL{id: "early", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	s.HandleRegistrationLine(irc.Message{
		Source: "irc.example", Command: "NOTICE", Params: []string{"AUTH", "*** Looking up your hostname"},
	})
	d.clearSent()

	s.HandleDisconnect(fmt.Errorf("connection reset by peer"))

	if len(d.sent) != 1 || d.sent[0].Command != "NOTICE" {
		t.Fatalf("want NOTICE while awaiting (keep client): %+v", d.sent)
	}
	got := d.sent[0].Trailing()
	if got == "" {
		got = d.sent[0].Param(1)
	}
	if !strings.Contains(got, "connection reset by peer") || !strings.Contains(got, "Retrying") {
		t.Fatalf("NOTICE text=%q", got)
	}
	s.mu.RLock()
	nAwait := len(s.awaitingUplink)
	nDown := len(s.downlinks)
	nBuf := len(s.regBuffer)
	awaiting := s.awaitingUplink[d.id]
	s.mu.RUnlock()
	if nAwait != 1 || nDown != 1 || nBuf != 0 || !awaiting {
		t.Fatalf("await=%d down=%d buf=%d awaiting=%v", nAwait, nDown, nBuf, awaiting)
	}
}

func TestOnDisconnectUnregisteredKeepsBNCUsable(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	s.SetAdmin(func(args []string) ([]string, error) {
		return []string{"ok " + strings.Join(args, " ")}, nil
	})
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	s.HandleDisconnect(fmt.Errorf("dial tcp: connection refused"))
	d.clearSent()
	if err := s.HandleClientMessage(d, irc.Message{Command: "BNC", Params: []string{"status"}}); err != nil {
		t.Fatal(err)
	}
	if len(d.sent) != 1 || d.sent[0].Command != "NOTICE" || d.sent[0].Trailing() != "ok status" {
		t.Fatalf("BNC while uplink down: %+v", d.sent)
	}
}

func TestTwoConcurrentEarlyAwaitingClients(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
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
	s.HandleRegistrationLine(notice)
	s.HandleRegistrationLine(welcome)
	s.HandleRegistrationLine(motdEnd)

	for _, d := range []*fakeDL{a, b} {
		if countMsgCmd(d.snapshot(), "NOTICE") != 1 || countMsgCmd(d.snapshot(), "001") != 1 || countMsgCmd(d.snapshot(), "376") != 1 {
			t.Fatalf("%s missing live relay: %+v", d.id, d.snapshot())
		}
	}

	s.mu.RLock()
	nAwait := len(s.awaitingUplink)
	s.mu.RUnlock()
	if nAwait != 0 {
		t.Fatalf("both should already be flushed by the live 376 (see completeRegistration): %d", nAwait)
	}
}

func TestNickErrorFallbackParsesRegistrationErr(t *testing.T) {
	got := nickErrorFallback("nick error: 432 [* badnick Erroneous Nickname]", "cfg")
	if got.Command != "432" || got.Param(1) != "badnick" || got.Trailing() != "Erroneous Nickname" {
		t.Fatalf("432: %+v", got)
	}
	got = nickErrorFallback("nick error: 437 [* hold Nick/channel is temporarily unavailable]", "cfg")
	if got.Command != "437" || got.Param(1) != "hold" {
		t.Fatalf("437: %+v", got)
	}
}

func TestNickErrorDisconnectRelaysWithoutStash(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "taken"}, nil, nil, nil, nil)
	a := &fakeDL{id: "a", caps: map[string]bool{}}
	b := &fakeDL{id: "b", caps: map[string]bool{}}
	_ = s.Attach(a)
	_ = s.Attach(b)
	a.clearSent()
	b.clearSent()

	// Result delivered before the 433 line — the demux race this covers.
	s.HandleDisconnect(fmt.Errorf("nick error: 433 [* taken Nickname is already in use.]"))

	for _, d := range []*fakeDL{a, b} {
		if countMsgCmd(d.snapshot(), "433") != 1 {
			t.Fatalf("%s missing synthesized 433: %+v", d.id, d.snapshot())
		}
		if countMsgCmd(d.snapshot(), "NOTICE") != 1 {
			t.Fatalf("%s missing NOTICE: %+v", d.id, d.snapshot())
		}
		if !afterCmd(d.snapshot(), "433", "NOTICE") {
			t.Fatalf("%s 433 must precede NOTICE: %+v", d.id, d.snapshot())
		}
	}
}

func TestNickErrorDisconnectDoesNotRelayStaleNumeric(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "taken"}, nil, nil, nil, nil)
	d := &fakeDL{id: "a", caps: map[string]bool{}}
	_ = s.Attach(d)
	d.clearSent()

	s.HandleRegistrationLine(irc.Message{
		Source: "irc.example", Command: "433", Params: []string{"*", "taken", "Nickname is already in use."},
	})
	s.HandleDisconnect(fmt.Errorf("registration deadline exceeded (10s)"))
	if countMsgCmd(d.snapshot(), "433") != 0 {
		t.Fatalf("swallowed 433 must not surface on an unrelated failure: %+v", d.snapshot())
	}
	if countMsgCmd(d.snapshot(), "NOTICE") != 1 {
		t.Fatalf("want reconnect NOTICE: %+v", d.snapshot())
	}
}

func TestNickErrorDisconnectUsesStashedLine(t *testing.T) {
	s := New(store.Network{Name: "n", Nick: "taken"}, nil, nil, nil, nil)
	d := &fakeDL{id: "a", caps: map[string]bool{}}
	_ = s.Attach(d)
	d.clearSent()

	s.HandleRegistrationLine(irc.Message{
		Source: "irc.example", Command: "433", Params: []string{"*", "taken", "Nickname is already in use."},
	})
	if countMsgCmd(d.snapshot(), "433") != 0 {
		t.Fatalf("433 must not relay until disconnect: %+v", d.snapshot())
	}
	s.HandleDisconnect(fmt.Errorf("nick error: 433 [* taken Nickname is already in use.]"))
	snap := d.snapshot()
	if countMsgCmd(snap, "433") != 1 {
		t.Fatalf("missing stashed 433: %+v", snap)
	}
	if snap[0].Source != "irc.example" {
		t.Fatalf("want uplink source, got %+v", snap[0])
	}
}

func TestNickInUseRelayedThenDisconnect(t *testing.T) {
	ln, host, port := newFakeIRCListener(t)

	netCfg := store.Network{Name: "test", Host: "pipe", Port: 1, Nick: "taken", NickRecovery: false}
	s := New(netCfg, nil, nil, nil, nil)

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
		conn, err := ln.Accept()
		if err != nil {
			scriptDone <- err
			return
		}
		t.Cleanup(func() { _ = conn.Close() })
		scriptDone <- runNickInUseServer(conn, time.Now().Add(6*time.Second))
	}()
	newTestUplink(t, s, netCfg, host, port)

	waitUntil(t, 5*time.Second, func() bool {
		return countMsgCmd(a.snapshot(), "433") >= 1 && countMsgCmd(a.snapshot(), "NOTICE") >= 1
	})

	for _, d := range []*fakeDL{a, b} {
		if countMsgCmd(d.snapshot(), "433") < 1 {
			t.Fatalf("%s missing 433: %+v", d.id, d.snapshot())
		}
		if countMsgCmd(d.snapshot(), "ERROR") != 0 {
			t.Fatalf("%s unexpected ERROR while unregistered: %+v", d.id, d.snapshot())
		}
		noticeOK := false
		for _, m := range d.snapshot() {
			if m.Command == "NOTICE" && strings.Contains(m.Trailing(), "Retrying") {
				noticeOK = true
				break
			}
		}
		if !noticeOK {
			t.Fatalf("%s missing reconnect NOTICE: %+v", d.id, d.snapshot())
		}
		// 433 should appear before the reconnect NOTICE.
		if !afterCmd(d.snapshot(), "433", "NOTICE") {
			t.Fatalf("%s 433 must precede NOTICE: %+v", d.id, d.snapshot())
		}
	}
	s.mu.RLock()
	nDown := len(s.downlinks)
	nAwait := len(s.awaitingUplink)
	s.mu.RUnlock()
	if nDown != 2 || nAwait != 2 {
		t.Fatalf("want clients kept: down=%d await=%d", nDown, nAwait)
	}

	select {
	case err := <-scriptDone:
		if err != nil {
			t.Fatal("script:", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("script timeout")
	}
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

func TestNickLadderSwallowsMid433(t *testing.T) {
	ln, host, port := newFakeIRCListener(t)

	netCfg := store.Network{
		Name: "test", Host: "pipe", Port: 1, Nick: "taken", AltNick: "alt", NickRecovery: true,
	}
	s := New(netCfg, nil, nil, nil, nil)

	d := &fakeDL{id: "a", caps: map[string]bool{}}
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
		scriptDone <- runNickLadderAcceptServer(conn, time.Now().Add(6*time.Second))
	}()
	newTestUplink(t, s, netCfg, host, port)

	waitUntil(t, 5*time.Second, func() bool { return s.Registered() })
	snap := d.snapshot()
	if countMsgCmd(snap, "433") != 0 {
		t.Fatalf("mid-ladder 433 must not reach client: %+v", snap)
	}
	if got := welcomeNicks(snap); len(got) != 2 || got[0] != "taken" || got[1] != "alt" {
		t.Fatalf("want pending 001 taken then real 001 alt, got %v in %+v", got, snap)
	}
	assertNoSelfNICK(t, snap)
	if got := s.Nick(); got != "alt" {
		t.Fatalf("session nick after ladder: %q want alt", got)
	}
	// Late attach must advertise the live nick, not the configured primary.
	late := &fakeDL{id: "late", caps: map[string]bool{}}
	if err := s.Attach(late); err != nil {
		t.Fatal(err)
	}
	lateSnap := late.snapshot()
	assertNoSelfNICK(t, lateSnap)
	var welcome irc.Message
	for _, m := range lateSnap {
		if m.Command == "001" {
			welcome = m
			break
		}
	}
	if welcome.Command != "001" || welcome.Param(0) != "alt" || !strings.Contains(welcome.Trailing(), "Welcome to GoBNC") {
		t.Fatalf("late attach 001: %+v (session nick=%q)", welcome, s.Nick())
	}

	if err := <-scriptDone; err != nil {
		t.Fatal(err)
	}
}

func TestAttachPendingSeesTwo001sWhenAssignedNickDiffers(t *testing.T) {
	// Case 1: downlink attached while the uplink is still connecting.
	// Synthetic 001 uses the configured nick; the real 001 uses the
	// assigned nick. 001 is the source of truth — no self-NICK.
	s := New(store.Network{Name: "n", Nick: "MrIron", Username: "u"}, nil, nil, nil, nil)
	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}

	s.HandleRegistrationLine(irc.Message{
		Source: "irc.example", Command: "001", Params: []string{"MrIron_", "Welcome"},
	})
	snap := d.snapshot()
	if got := welcomeNicks(snap); len(got) != 2 || got[0] != "MrIron" || got[1] != "MrIron_" {
		t.Fatalf("want pending 001 MrIron then real 001 MrIron_, got %v in %+v", got, snap)
	}
	if !strings.Contains(snap[0].Trailing(), "pending uplink") {
		t.Fatalf("first 001 should be the synthetic pending welcome: %+v", snap[0])
	}
	assertNoSelfNICK(t, snap)
}

func TestAttachMidRegistrationAssignedNickNoNICK(t *testing.T) {
	// Case 1 variant: attach after the real 001 is already buffered.
	// Replay the buffer as-is — no synthetic 001, no self-NICK — even
	// when the assigned nick differs from the configured one.
	s := New(store.Network{Name: "n", Nick: "me"}, nil, nil, nil, nil)
	s.HandleRegistrationLine(irc.Message{
		Source: "irc.example", Command: "NOTICE", Params: []string{"AUTH", "*** Looking up your hostname"},
	})
	s.HandleRegistrationLine(irc.Message{
		Source: "irc.example", Command: "001", Params: []string{"me_", "Welcome"},
	})

	d := &fakeDL{id: "c1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	snap := d.snapshot()
	if got := welcomeNicks(snap); len(got) != 1 || got[0] != "me_" {
		t.Fatalf("want only the buffered 001 with assigned nick, got %v in %+v", got, snap)
	}
	assertNoSelfNICK(t, snap)
}

func runNickLadderAcceptServer(server net.Conn, deadline time.Time) error {
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
	_ = write(":server CAP * LS :")
	line, err := read()
	if err != nil || line != "CAP END" {
		return fmt.Errorf("CAP END: %q %v", line, err)
	}
	_ = write(":server 433 * taken :Nickname is already in use.")
	line, err = read()
	if err != nil || line != "NICK alt" {
		return fmt.Errorf("alt: %q %v", line, err)
	}
	_ = write(":server 001 alt :Welcome")
	_ = write(":server 376 alt :End of /MOTD command.")
	// Drain possible ISON from recovery.
	_ = server.SetReadDeadline(time.Now().Add(2 * time.Second))
	if line, err := read(); err == nil && strings.HasPrefix(line, "ISON ") {
		_ = write(":server 303 alt :taken")
	}
	return nil
}
