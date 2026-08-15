package session

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MasterBOFH/GoBNC/internal/irc"
	"github.com/MasterBOFH/GoBNC/internal/store"
)

// TestWHOXSameChannelDifferentFlagsNoCrossTalk answers a specific question
// left open by TestSolicitousE2EConcurrentWHOXAndHold and
// TestReplyRoutingStressLagsAndConcurrentPolls: both of those keep two
// concurrent WHOX pollers on *different* channels. WHOX routes purely by
// token (RouteMessage keys off msg.Params[1], never the channel — see
// tracker.go), so two pollers sharing a channel should be no different in
// theory, but that's exactly the kind of "should be fine" that's worth
// actually driving through the real wire rather than trusting the theory.
//
// This also exercises the one genuinely different-per-client behavior
// "different flags" implies: whether the *client's own* WHOX syntax
// requested the 't' (querytype) field. d1 requests it explicitly (and
// supplies its own correlation token, "111", after the comma) — the
// bouncer must restore that exact value into the reply, untouched. d2
// omits 't' entirely — the bouncer injects its own internal token to
// route by (see injectWHOXToken), then must strip it back out of the
// reply before d2 ever sees it, since d2 never asked for a token field at
// all. Both requests target the same channel, wired through the real
// keeper<->brain stack, with lag between every reply line.
func TestWHOXSameChannelDifferentFlagsNoCrossTalk(t *testing.T) {
	ln, host, port := newFakeIRCListener(t)

	netCfg := store.Network{Name: "test", Host: "pipe", Port: 1, Nick: "me", Username: "u", Realname: "r"}
	s := New(netCfg, nil, nil, nil, nil)

	whoLines := make(chan string, 4)
	scriptDone := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			scriptDone <- err
			return
		}
		t.Cleanup(func() { _ = conn.Close() })
		scriptDone <- runWHOXSameChannelServer(conn, time.Now().Add(10*time.Second), whoLines)
	}()
	newTestUplink(t, s, netCfg, host, port)

	waitUntil(t, 5*time.Second, func() bool {
		if !s.Registered() {
			return false
		}
		s.mu.RLock()
		defer s.mu.RUnlock()
		return s.isupport != nil && s.isupport.WHOX
	})

	d1 := &fakeDL{id: "d1", caps: map[string]bool{}}
	d2 := &fakeDL{id: "d2", caps: map[string]bool{}}
	for _, d := range []*fakeDL{d1, d2} {
		if err := s.Attach(d); err != nil {
			t.Fatal(err)
		}
		d.clearSent()
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		// Requests 't' itself, own correlation token "111".
		_ = s.HandleClientMessage(d1, irc.Message{Command: "WHO", Params: []string{"#chan", "%tun,111"}})
	}()
	go func() {
		defer wg.Done()
		// No 't' — the bouncer must inject and later strip its own.
		_ = s.HandleClientMessage(d2, irc.Message{Command: "WHO", Params: []string{"#chan", "%fh,222"}})
	}()

	var line1, line2 string
	for i := 0; i < 2; i++ {
		select {
		case l := <-whoLines:
			if strings.Contains(l, "un") {
				line1 = l
			} else {
				line2 = l
			}
		case <-time.After(3 * time.Second):
			t.Fatal("timeout waiting for uplink WHO lines")
		}
	}
	wg.Wait()

	tok1 := whoToken(line1)
	tok2 := whoToken(line2)
	if tok1 == "" || tok2 == "" || tok1 == tok2 {
		t.Fatalf("bad/duplicate tokens: %q(%q) %q(%q)", tok1, line1, tok2, line2)
	}

	waitUntil(t, 3*time.Second, func() bool {
		return countCmd(d1, "354") >= 1 && countCmd(d2, "354") >= 1
	})
	if countCmd(d1, "354") != 1 || countCmd(d2, "354") != 1 {
		t.Fatalf("WHOX demux duplicated on shared channel: d1=%d d2=%d", countCmd(d1, "354"), countCmd(d2, "354"))
	}

	m1 := firstCmd(d1, "354")
	m2 := firstCmd(d2, "354")
	if m1 == nil || m2 == nil {
		t.Fatalf("missing 354: d1=%v d2=%v", d1.snapshot(), d2.snapshot())
	}

	// d1 asked for 't' and its own token "111" — must come back verbatim,
	// nothing stripped.
	if m1.Param(1) != "111" {
		t.Fatalf("d1 own correlation token not restored: %+v", m1.Params)
	}
	if !strings.Contains(m1.Trailing(), "MARKER-D1") {
		t.Fatalf("d1 got wrong content: %+v", m1.Params)
	}

	// d2 never requested 't' — the bouncer's own injected token must be
	// gone entirely, not merely different from d1's.
	if m2.Param(1) == tok2 {
		t.Fatalf("d2's injected WHOX token was not stripped: %+v", m2.Params)
	}
	if !strings.Contains(m2.Trailing(), "MARKER-D2") {
		t.Fatalf("d2 got wrong content: %+v", m2.Params)
	}

	if strings.Contains(m1.Trailing(), "MARKER-D2") || strings.Contains(m2.Trailing(), "MARKER-D1") {
		t.Fatalf("cross-talk between same-channel WHOX pollers: d1=%+v d2=%+v", m1.Params, m2.Params)
	}

	select {
	case err := <-scriptDone:
		if err != nil {
			t.Fatal("script:", err)
		}
	case <-time.After(2 * time.Second):
	}
}

func runWHOXSameChannelServer(server net.Conn, deadline time.Time, whoLines chan<- string) error {
	br := newLineBuf(server)
	read := func() (string, error) {
		_ = server.SetReadDeadline(deadline)
		return br.readLine()
	}
	write := func(s string) error {
		lag()
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
		":server 005 me CHANMODES=b,k,l,imnpst PREFIX=(ov)@+ CASEMAPPING=rfc1459 WHOX :are supported by this server",
		":server 001 me :Welcome",
		":server 376 me :End of /MOTD command.",
	} {
		if err := write(l); err != nil {
			return err
		}
	}

	var tok1, tok2 string
	for tok1 == "" || tok2 == "" {
		_ = server.SetReadDeadline(deadline)
		l, err := br.readLine()
		if err != nil {
			return fmt.Errorf("reading WHO: %w", err)
		}
		if !strings.HasPrefix(l, "WHO ") {
			continue
		}
		select {
		case whoLines <- l:
		default:
		}
		if strings.Contains(l, "un") {
			tok1 = whoToken(l)
		} else {
			tok2 = whoToken(l)
		}
	}

	// Interleaved with lag; d1's burst carries the 't' field it asked
	// for (its own token echoed at the fixed position), d2's carries no
	// token field at all — the bouncer is responsible for stripping the
	// internal one this script sends on the wire.
	schedule := []string{
		":server 354 me " + tok1 + " userval :MARKER-D1",
		":server 354 me " + tok2 + " hostval :MARKER-D2",
		":server 315 me #chan :End",
		":server 315 me #chan :End",
	}
	for _, l := range schedule {
		if err := write(l); err != nil {
			return err
		}
	}
	return nil
}
