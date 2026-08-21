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
)

func TestParseCTCP(t *testing.T) {
	tests := []struct {
		name       string
		text       string
		wantVerb   string
		wantParams string
		wantOK     bool
	}{
		{"well-formed with params", "\x01PING 12345\x01", "PING", "12345", true},
		{"well-formed no params", "\x01VERSION\x01", "VERSION", "", true},
		{"missing trailing delim", "\x01PING 12345", "", "", false},
		{"missing leading delim", "PING 12345\x01", "", "", false},
		{"empty payload", "\x01\x01", "", "", false},
		{"plain text", "hello", "", "", false},
		{"too short", "\x01", "", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			verb, params, ok := parseCTCP(tt.text)
			if ok != tt.wantOK || verb != tt.wantVerb || params != tt.wantParams {
				t.Fatalf("parseCTCP(%q) = %q, %q, %v; want %q, %q, %v",
					tt.text, verb, params, ok, tt.wantVerb, tt.wantParams, tt.wantOK)
			}
		})
	}
}

// TestHandleUplinkCTCP_RelayIsLiveOnlyNotStored covers the default (no
// SetCTCPConfig call, matching pre-CTCP-feature behavior): the request
// still reaches an attached downlink unchanged, but — unlike an ordinary
// PRIVMSG — is never stored for chathistory/legacy replay.
func TestHandleUplinkCTCP_RelayIsLiveOnlyNotStored(t *testing.T) {
	db, hist, id := openLegacyFixture(t)
	s := sessionWithChan(t, db, hist, id)

	d := &fakeDL{id: "d1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	d.clearSent()

	before := time.Now().UTC().Add(-time.Minute)
	s.HandleMessage(irc.Message{
		Source:  "bob!u@h",
		Command: "PRIVMSG",
		Params:  []string{"#c", "\x01VERSION\x01"},
	})

	sent := d.snapshot()
	if len(sent) != 1 || sent[0].Command != "PRIVMSG" || sent[0].Trailing() != "\x01VERSION\x01" {
		t.Fatalf("downlink did not receive relayed CTCP unchanged: %+v", sent)
	}

	msgs, err := hist.QueryLegacyAfter(context.Background(), id, "#c", before)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 0 {
		t.Fatalf("relay-mode CTCP must not be stored for legacy replay: %+v", msgs)
	}
}

func TestHandleUplinkCTCP_DisableDropsRequest(t *testing.T) {
	db, hist, id := openLegacyFixture(t)
	s := sessionWithChan(t, db, hist, id)
	s.SetCTCPConfig(NewCTCPConfig(CTCPModeDisable, CTCPModeRelay, CTCPModeRelay))

	d := &fakeDL{id: "d1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	d.clearSent()

	before := time.Now().UTC().Add(-time.Minute)
	s.HandleMessage(irc.Message{
		Source:  "bob!u@h",
		Command: "PRIVMSG",
		Params:  []string{"#c", "\x01PING 42\x01"},
	})

	if sent := d.snapshot(); len(sent) != 0 {
		t.Fatalf("disabled CTCP must not reach any downlink: %+v", sent)
	}
	msgs, err := hist.QueryLegacyAfter(context.Background(), id, "#c", before)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 0 {
		t.Fatalf("disabled CTCP must not be stored: %+v", msgs)
	}
}

// TestHandleUplinkCTCP_OtherVerbUsesOtherMode confirms a CTCP verb besides
// PING/VERSION/ACTION is governed by ctcp_other, independent of the
// ping/version settings.
func TestHandleUplinkCTCP_OtherVerbUsesOtherMode(t *testing.T) {
	db, hist, id := openLegacyFixture(t)
	s := sessionWithChan(t, db, hist, id)
	s.SetCTCPConfig(NewCTCPConfig(CTCPModeRelay, CTCPModeRelay, CTCPModeDisable))

	d := &fakeDL{id: "d1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	d.clearSent()

	s.HandleMessage(irc.Message{
		Source:  "bob!u@h",
		Command: "PRIVMSG",
		Params:  []string{"#c", "\x01CLIENTINFO\x01"},
	})

	if sent := d.snapshot(); len(sent) != 0 {
		t.Fatalf("ctcp_other=disable must drop CLIENTINFO: %+v", sent)
	}
}

// TestHandleUplinkCTCP_ActionAlwaysRelaysAndStores confirms /me formatting
// is excluded from ctcp_other entirely — it behaves like ordinary chat
// even when every CTCP mode is set to disable.
func TestHandleUplinkCTCP_ActionAlwaysRelaysAndStores(t *testing.T) {
	db, hist, id := openLegacyFixture(t)
	s := sessionWithChan(t, db, hist, id)
	s.SetCTCPConfig(NewCTCPConfig(CTCPModeDisable, CTCPModeDisable, CTCPModeDisable))

	d := &fakeDL{id: "d1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	d.clearSent()

	before := time.Now().UTC().Add(-time.Minute)
	s.HandleMessage(irc.Message{
		Source:  "bob!u@h",
		Command: "PRIVMSG",
		Params:  []string{"#c", "\x01ACTION waves\x01"},
	})

	if sent := d.snapshot(); len(sent) != 1 {
		t.Fatalf("ACTION must always relay regardless of ctcp_other=disable: %+v", sent)
	}
	msgs, err := hist.QueryLegacyAfter(context.Background(), id, "#c", before)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("ACTION should be stored like ordinary chat: %+v", msgs)
	}
}

// TestHandleUplinkCTCP_NoticeAlwaysRelays confirms a CTCP-framed NOTICE
// (a reply, not a request) is left alone regardless of configured mode.
func TestHandleUplinkCTCP_NoticeAlwaysRelays(t *testing.T) {
	db, hist, id := openLegacyFixture(t)
	s := sessionWithChan(t, db, hist, id)
	s.SetCTCPConfig(NewCTCPConfig(CTCPModeDisable, CTCPModeDisable, CTCPModeDisable))

	d := &fakeDL{id: "d1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	d.clearSent()

	s.HandleMessage(irc.Message{
		Source:  "bob!u@h",
		Command: "NOTICE",
		Params:  []string{"#c", "\x01VERSION bob-client 1.0\x01"},
	})

	if sent := d.snapshot(); len(sent) != 1 {
		t.Fatalf("CTCP NOTICE (reply) must always relay unchanged: %+v", sent)
	}
}

// TestCTCPEdgeReplyWritesToUplinkNotDownlink drives a real
// keeper/brain/wire stack (newTestUplink) to prove edge mode answers CTCP
// PING/VERSION directly over the uplink — "CTCP stops at the bouncer" —
// and that the original request never reaches any attached downlink.
func TestCTCPEdgeReplyWritesToUplinkNotDownlink(t *testing.T) {
	ln, host, port := newFakeIRCListener(t)

	netCfg := store.Network{Name: "test", Host: "pipe", Port: 1, Nick: "me", Username: "u", Realname: "r"}
	s := New(netCfg, nil, nil, nil, nil)
	s.SetCTCPConfig(NewCTCPConfig(CTCPModeEdge, CTCPModeEdge, CTCPModeRelay))

	replyLine := make(chan string, 2)
	scriptDone := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			scriptDone <- err
			return
		}
		t.Cleanup(func() { _ = conn.Close() })
		scriptDone <- runCTCPEdgeServer(conn, time.Now().Add(10*time.Second), replyLine)
	}()

	newTestUplink(t, s, netCfg, host, port)
	waitUntil(t, 5*time.Second, s.Registered)

	d := &fakeDL{id: "d1", caps: map[string]bool{}}
	if err := s.Attach(d); err != nil {
		t.Fatal(err)
	}
	d.clearSent()

	gotPing, gotVersion := false, false
	for i := 0; i < 2; i++ {
		select {
		case line := <-replyLine:
			if strings.Contains(line, "NOTICE attacker :\x01PING 123456789\x01") {
				gotPing = true
			}
			if strings.Contains(line, "NOTICE attacker :\x01VERSION GoBNC ") {
				gotVersion = true
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for edge-mode uplink replies (%d/2 received)", i)
		}
	}
	if !gotPing || !gotVersion {
		t.Fatalf("missing expected edge replies: ping=%v version=%v", gotPing, gotVersion)
	}

	if sent := d.snapshot(); len(sent) != 0 {
		t.Fatalf("edge mode must not relay the request to any downlink: %+v", sent)
	}

	select {
	case err := <-scriptDone:
		if err != nil {
			t.Fatal("script:", err)
		}
	case <-time.After(2 * time.Second):
	}
}

// runCTCPEdgeServer completes minimal registration, then plays a remote
// user's CTCP PING and VERSION requests targeted at our own nick, and
// forwards the bouncer's own uplink-bound replies back to the test via
// replyLine.
func runCTCPEdgeServer(server net.Conn, deadline time.Time, replyLine chan<- string) error {
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
	if err := write("CAP * LS :"); err != nil {
		return err
	}
	line, err := read()
	if err != nil || line != "CAP END" {
		return fmt.Errorf("CAP END: %q %v", line, err)
	}
	for _, l := range []string{
		":server 001 me :Welcome",
		":server 376 me :End of /MOTD command.",
	} {
		if err := write(l); err != nil {
			return err
		}
	}
	if err := write(":attacker!u@h PRIVMSG me :\x01PING 123456789\x01"); err != nil {
		return err
	}
	if err := write(":attacker!u@h PRIVMSG me :\x01VERSION\x01"); err != nil {
		return err
	}
	for i := 0; i < 2; i++ {
		reply, err := read()
		if err != nil {
			return err
		}
		replyLine <- reply
	}
	return nil
}
